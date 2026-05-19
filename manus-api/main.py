"""
/manus-api — DraftKings Data Nexus Codex FastAPI Service
=========================================================
Manus Directive v1.0, Section 2 & 5.3

Endpoints:
  POST /datasets/register          — Register a new dataset
  GET  /datasets/{dataset_id}      — Get dataset status and lineage
  GET  /datasets                   — List all registered datasets
  POST /graph/query                — Execute a Cypher-backed graph query
  GET  /graph/node/{node_id}       — Get a NEXUS node by ID
  POST /agents/orchestrate         — Orchestrate a Claude/Gemini agent call
  POST /graphrag/query             — GraphRAG query (Claude + Cypher + Gemini)
  GET  /health                     — Health check
  GET  /detonator/status           — Detonator Board activation status

Author  : Manus Directive v1.0
Version : 1.0.0
"""

import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from nexus_governance.evidence_gates.evidence_confidence_gate import (
    DeploymentStatus, DKRelevance, NexusNode, Tier,
    get_gate, get_detonator,
)

log = logging.getLogger("manus_api")
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Manus DraftKings Data Nexus Codex API",
    description=(
        "Multi-agent orchestration API for the DraftKings Data Nexus Codex. "
        "Anchored to MasterBrief_v54 (EV-001–EV-291, SB-01–SB-66). "
        "Manus Directive v1.0."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory stores (replace with PostgreSQL in production)
# ---------------------------------------------------------------------------

_datasets: dict[str, dict] = {}
_mutation_log: list[dict] = []

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class DatasetRegistration(BaseModel):
    dataset_id: str = Field(..., description="Unique dataset ID")
    name: str
    source_file: str
    tier: str = Field(..., description="T1 / T2 / T3")
    dk_domain: str = Field(..., description="michigan_core / asc_606 / cftc / etc.")
    deployment_status: str = Field(default="quarantine")
    description: str = ""
    nexus_node_id: Optional[str] = None

class GraphQueryRequest(BaseModel):
    cypher: str = Field(..., description="Cypher query string")
    params: dict = Field(default_factory=dict)
    tier_filter: Optional[str] = Field(None, description="Filter results to this tier")
    actor: str = Field(default="manus_api")

class AgentOrchestrationRequest(BaseModel):
    goal: str = Field(..., description="High-level goal for the agent")
    agent_id: str = Field(..., description="NEXUS agent ID (e.g., NEXUS-030)")
    model: str = Field(default="claude", description="claude / gemini / openai")
    allowed_tiers: list[str] = Field(default=["T1", "T2"])
    evidence_domains: list[str] = Field(default_factory=list)
    context: Optional[str] = None

class GraphRAGQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question")
    domains: list[str] = Field(default_factory=list, description="DK domains to search")
    tier_filter: str = Field(default="T1", description="Minimum tier for evidence")
    max_nodes: int = Field(default=20)

class NodeLookupResponse(BaseModel):
    node_id: str
    label: str
    tier: str
    deployment_status: str
    description: str
    symbolic_name: str
    enterprise_name: str

# ---------------------------------------------------------------------------
# Cypher Template Library
# ---------------------------------------------------------------------------

CYPHER_TEMPLATES = {
    "evidence_by_domain": """
        MATCH (e:Evidence)-[:BELONGS_TO]->(c:EvidenceCluster {domain: $domain})
        WHERE e.tier IN $tiers
        RETURN e.ev_id, e.tier, e.evidence_text, e.content_hash
        ORDER BY e.ev_id
        LIMIT $limit
    """,
    "theory_chain": """
        MATCH path = (e:Evidence)-[:SUPPORTS*1..3]->(t:Theory)
        WHERE e.tier = 'T1'
        RETURN path, length(path) AS depth
        ORDER BY depth DESC
        LIMIT $limit
    """,
    "conversation_to_evidence": """
        MATCH (c:Conversation)-[:REFERENCES_EVIDENCE]->(e:Evidence)
        WHERE c.dk_relevance = 'DK_CORE'
        RETURN c.thread_id, c.title, collect(e.ev_id) AS evidence_refs
        LIMIT $limit
    """,
    "detonator_convergence": """
        MATCH (p:Pillar)<-[:CONFIRMS]-(e:Evidence)
        WHERE e.tier = 'T1'
        WITH p, count(e) AS evidence_count
        WHERE evidence_count >= 1
        MATCH (l:Levee {domain: p.domain})
        RETURN p.pillar_id, p.name, evidence_count, collect(l.levee_id) AS levees
        ORDER BY evidence_count DESC
    """,
    "asc606_calendar_bleed": """
        MATCH (e:Evidence)-[:BELONGS_TO]->(c:EvidenceCluster {domain: 'asc_606_calendar_bleed'})
        OPTIONAL MATCH (e)-[:SUPPORTS]->(t:Theory {name: 'Calendar Bleed'})
        RETURN e.ev_id, e.evidence_text, t.name AS theory
        ORDER BY e.ev_id
    """,
    "michigan_mgcb": """
        MATCH (e:Evidence)-[:BELONGS_TO]->(c:EvidenceCluster {domain: 'michigan_core'})
        WHERE e.tier = 'T1'
        RETURN e.ev_id, e.evidence_text, e.content_hash
        ORDER BY e.ev_id
    """,
    "cftc_railbird": """
        MATCH (e:Evidence)-[:BELONGS_TO]->(c:EvidenceCluster {domain: 'cftc_railbird'})
        RETURN e.ev_id, e.evidence_text
        ORDER BY e.ev_id
    """,
    "mutation_audit": """
        MATCH (m:GraphMutationEvent)
        WHERE m.timestamp >= $since
        RETURN m.event_type, m.node_id, m.actor, m.timestamp
        ORDER BY m.timestamp DESC
        LIMIT $limit
    """,
}

# ---------------------------------------------------------------------------
# NEXUS Node Register (in-memory, loaded from YAML in production)
# ---------------------------------------------------------------------------

NEXUS_NODES = {
    "NEXUS-001": NodeLookupResponse(
        node_id="NEXUS-001", label="MBv54LegalSpine", tier="T1",
        deployment_status="main",
        description="Controlling legal and evidentiary spine. EV-001–EV-291, SB-01–SB-66.",
        symbolic_name="MBv54 Legal Spine",
        enterprise_name="Canonical Evidence Governance Framework v54",
    ),
    "NEXUS-010": NodeLookupResponse(
        node_id="NEXUS-010", label="EvidenceCluster_MichiganCore", tier="T1",
        deployment_status="main",
        description="Michigan MGCB core evidence cluster. EV-001–EV-009.",
        symbolic_name="Michigan Core",
        enterprise_name="Michigan MGCB Litigation Evidence Cluster",
    ),
    "NEXUS-011": NodeLookupResponse(
        node_id="NEXUS-011", label="EvidenceCluster_ASC606", tier="T1",
        deployment_status="main",
        description="ASC 606 calendar bleed cluster. EV-010–EV-019.",
        symbolic_name="Engine 3 / Calendar Bleed",
        enterprise_name="ASC 606 Revenue Recognition Evidence Cluster",
    ),
    "NEXUS-030": NodeLookupResponse(
        node_id="NEXUS-030", label="Agent_MichiganMGCB", tier="T2",
        deployment_status="main",
        description="Michigan MGCB Litigation Agent.",
        symbolic_name="Michigan Agent",
        enterprise_name="Michigan MGCB Litigation Agent",
    ),
    "NEXUS-031": NodeLookupResponse(
        node_id="NEXUS-031", label="Agent_CFTCRailbird", tier="T2",
        deployment_status="main",
        description="CFTC Railbird Agent.",
        symbolic_name="CFTC Agent",
        enterprise_name="CFTC Railbird Agent",
    ),
    "NEXUS-032": NodeLookupResponse(
        node_id="NEXUS-032", label="Agent_ASC606Audit", tier="T2",
        deployment_status="main",
        description="ASC 606 Audit Agent.",
        symbolic_name="ASC 606 Agent",
        enterprise_name="ASC 606 Audit Agent",
    ),
}

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "manus-api",
        "version": "1.0.0",
        "spine": "MasterBrief_v54",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/datasets/register")
def register_dataset(req: DatasetRegistration):
    """Register a new dataset in the DraftKings Data Lake."""
    gate = get_gate()
    node = NexusNode(
        node_id=req.dataset_id,
        label="Dataset",
        tier=Tier(req.tier),
        primary_source_id=req.source_file,
        description=req.description,
        deployment_status=DeploymentStatus(req.deployment_status),
    )
    try:
        gate.run_all_checks(node, actor="manus_api", context="control_logic")
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"NEXUS gate rejected: {e}")

    _datasets[req.dataset_id] = {
        **req.dict(),
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "status": "registered",
        "content_hash": node.content_hash,
    }
    _mutation_log.append({
        "event_type": "REGISTER_DATASET",
        "node_id": req.dataset_id,
        "actor": "manus_api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    return {"registered": True, "dataset_id": req.dataset_id, "content_hash": node.content_hash}


@app.get("/datasets/{dataset_id}")
def get_dataset(dataset_id: str):
    if dataset_id not in _datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _datasets[dataset_id]


@app.get("/datasets")
def list_datasets():
    return {"datasets": list(_datasets.values()), "count": len(_datasets)}


@app.post("/graph/query")
def graph_query(req: GraphQueryRequest):
    """
    Execute a Cypher-backed graph query.
    In production: connects to Neo4j via the neo4j driver.
    """
    # Validate the query is a known template or safe custom query
    template_name = None
    for name, template in CYPHER_TEMPLATES.items():
        if req.cypher.strip() == template.strip():
            template_name = name
            break

    # Simulate query execution (replace with actual Neo4j driver in production)
    result = {
        "query": req.cypher[:200],
        "template": template_name,
        "params": req.params,
        "tier_filter": req.tier_filter,
        "actor": req.actor,
        "status": "simulated",
        "note": "Connect Neo4j via docker/docker-compose.yml to execute live queries.",
        "rows": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return result


@app.get("/graph/node/{node_id}")
def get_node(node_id: str):
    """Get a NEXUS node by ID."""
    if node_id not in NEXUS_NODES:
        raise HTTPException(status_code=404, detail=f"NEXUS node '{node_id}' not found")
    return NEXUS_NODES[node_id]


@app.get("/graph/templates")
def list_cypher_templates():
    """List all available Cypher query templates."""
    return {
        "templates": [
            {"name": k, "preview": v.strip()[:120] + "..."}
            for k, v in CYPHER_TEMPLATES.items()
        ]
    }


@app.post("/agents/orchestrate")
async def orchestrate_agent(req: AgentOrchestrationRequest):
    """
    Orchestrate a Claude/Gemini agent call.
    Routes the goal to the appropriate agent with tier-constrained evidence access.
    """
    if req.agent_id not in NEXUS_NODES:
        raise HTTPException(status_code=404, detail=f"Agent '{req.agent_id}' not registered")

    agent_node = NEXUS_NODES[req.agent_id]
    if agent_node.deployment_status != "main":
        raise HTTPException(
            status_code=403,
            detail=f"Agent '{req.agent_id}' is not in 'main' deployment status"
        )

    # Build the constrained prompt
    tier_constraint = " and ".join(req.allowed_tiers)
    domain_constraint = ", ".join(req.evidence_domains) if req.evidence_domains else "all domains"

    system_prompt = f"""You are the {agent_node.enterprise_name} ({agent_node.symbolic_name}).

NEXUS Governance Rules:
- You may ONLY use evidence from tiers: {tier_constraint}
- You may ONLY reference domains: {domain_constraint}
- All EV-IDs must be from the frozen range EV-001–EV-291
- All SB-IDs must be from the frozen range SB-01–SB-66
- Never invent evidence. Never promote T3 to T2 or T1.
- Anchor all reasoning to MasterBrief_v54 as the controlling spine.

Your goal: {req.goal}
"""

    # In production: call Claude/Gemini API with the constrained prompt
    # For now: return the structured orchestration plan
    result = {
        "agent_id": req.agent_id,
        "agent_name": agent_node.enterprise_name,
        "model": req.model,
        "goal": req.goal,
        "system_prompt_preview": system_prompt[:300] + "...",
        "allowed_tiers": req.allowed_tiers,
        "evidence_domains": req.evidence_domains,
        "status": "orchestration_plan_ready",
        "note": f"Set ANTHROPIC_API_KEY or GEMINI_API_KEY to execute live {req.model} calls.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _mutation_log.append({
        "event_type": "AGENT_ORCHESTRATION",
        "node_id": req.agent_id,
        "actor": "manus_api",
        "goal": req.goal[:100],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    return result


@app.post("/graphrag/query")
async def graphrag_query(req: GraphRAGQueryRequest):
    """
    GraphRAG query: Claude builds Cypher → Neo4j returns subgraph → Gemini synthesizes.
    Manus Directive v1.0, Section 5.3.
    """
    # Step 1: Claude selects the best Cypher template
    domain_to_template = {
        "michigan_core": "michigan_mgcb",
        "asc_606_calendar_bleed": "asc606_calendar_bleed",
        "cftc_railbird": "cftc_railbird",
        "convergence": "detonator_convergence",
    }
    selected_template = None
    for domain in req.domains:
        if domain in domain_to_template:
            selected_template = domain_to_template[domain]
            break
    if not selected_template:
        selected_template = "evidence_by_domain"

    cypher = CYPHER_TEMPLATES[selected_template]

    # Step 2: Execute Cypher (simulated — connect Neo4j for live)
    subgraph_summary = {
        "template_used": selected_template,
        "cypher_preview": cypher.strip()[:200],
        "domains": req.domains,
        "tier_filter": req.tier_filter,
        "max_nodes": req.max_nodes,
        "status": "subgraph_simulated",
    }

    # Step 3: Gemini synthesis prompt (ready for live Gemini call)
    synthesis_prompt = f"""You are the Gemini Data Alchemist for the DraftKings Data Nexus Codex.

Question: {req.question}

Subgraph context (from Neo4j, tier >= {req.tier_filter}):
{json.dumps(subgraph_summary, indent=2)}

Instructions:
- Synthesize a narrative answer grounded STRICTLY in the subgraph data.
- Cite explicit EV-IDs and SB-IDs where available.
- Do NOT introduce information outside the subgraph.
- Label all T2 inferences explicitly.
- If the Detonator Board is relevant, state activation status.
"""

    result = {
        "question": req.question,
        "domains": req.domains,
        "tier_filter": req.tier_filter,
        "cypher_template": selected_template,
        "subgraph": subgraph_summary,
        "synthesis_prompt_preview": synthesis_prompt[:400] + "...",
        "status": "ready_for_gemini_synthesis",
        "note": "Set GEMINI_API_KEY to execute live Gemini synthesis.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return result


@app.get("/detonator/status")
def detonator_status():
    """Get current Detonator Board activation status."""
    detonator = get_detonator()
    return detonator.check_activation()


@app.get("/mutation-log")
def get_mutation_log(limit: int = 50):
    """Get the last N mutation log entries."""
    return {
        "entries": _mutation_log[-limit:],
        "total": len(_mutation_log),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
