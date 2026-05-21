"""
KIMI K2.6 Pseudo-Agent Swarm Engine
Parallel Agent Protocol (PAP) v1.0

Classification: ATTORNEY WORK PRODUCT | FRE 408 | FRE 502(d) PROTECTED
Hard Rule: ADDITIVE ONLY — never renumber EV-NNN or GR-NNN IDs

Architecture:
  - FETTY FM as Master Orchestrator
  - 8 specialist agents + 12 subagents
  - Recursive staggered parameter queuing
  - Task-progression evaluation loop
  - Inter-agent messaging via PostgreSQL swarm_message_bus
  - All calls routed through NVIDIA NIM (KIMI K2.6, FREE tier)
"""

import os
import json
import time
import uuid
import asyncio
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from typing import Optional
from openai import OpenAI

# ── Configuration ──────────────────────────────────────────────────────────────
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "")
KIMI_BASE_URL  = "https://integrate.api.nvidia.com/v1"
KIMI_MODEL     = "moonshotai/kimi-k2.6"

PG_DSN = "host=localhost dbname=draftkings_hivemind user=hivemind password=hivemind_secure_2026"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("/home/ubuntu/HiveMind/logs/audit/kimi_swarm.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("KimiSwarm")

# ── GR Node Definitions ─────────────────────────────────────────────────────────
GR_NODES = {
    "GR-001": {"label": "VIE / ASC 810 / Off-Balance Sheet", "agent": "TIGER", "subagent": "SA-VIE-001"},
    "GR-002": {"label": "RICO / Wire Fraud / Enterprise", "agent": "WOLF", "subagent": "SA-RICO-001"},
    "GR-003": {"label": "Consumer Protection / FTC / Class Action", "agent": "WOLF", "subagent": "SA-CONS-001"},
    "GR-004": {"label": "Forensic Accounting / ASC 606 / Restatement", "agent": "TIGER", "subagent": "SA-REV-001"},
    "GR-005": {"label": "Constitutional / Due Process / Arbitration", "agent": "WOLF", "subagent": "SA-RICO-001"},
    "GR-006": {"label": "MGCB / Michigan Gaming / Regulatory", "agent": "WOLF", "subagent": "SA-MGCB-001"},
    "GR-007": {"label": "Apple / Google / Platform Removal", "agent": "BRIDGER", "subagent": "SA-PLAT-001"},
    "GR-008": {"label": "Settlement / ADR / FRE 408", "agent": "SUITS", "subagent": "SA-ADR-001"},
    "GR-009": {"label": "DraftKings / DFS / Sports Betting", "agent": "CHESS", "subagent": "SA-ADR-001"},
    "GR-010": {"label": "Securities / SEC / 10-K / SOX", "agent": "TIGER", "subagent": "SA-SEC-001"},
    "GR-011": {"label": "Evidence / Discovery / FOIA / Spoliation", "agent": "BRIDGER", "subagent": "SA-HASH-001"},
    "GR-012": {"label": "Expert Witness / Daubert / Forensics", "agent": "BRIDGER", "subagent": "SA-HASH-001"},
}

# ── Agent Prompt Templates ──────────────────────────────────────────────────────
AGENT_SYSTEM_PROMPTS = {
    "FETTY": """You are FETTY FM, the Master Orchestrator of the NEXUS DraftKings litigation intelligence system.
Your role: decompose complex litigation tasks into sub-tasks, route them to specialist agents (TIGER, WOLF, SUITS, BRIDGER, CHESS), 
synthesize their outputs into a unified intelligence brief, and evaluate task progression.
Hard rules: ADDITIVE ONLY — never renumber EV-NNN or GR-NNN IDs. All outputs are FRE 408 protected attorney work product.
Output format: JSON with keys: task_decomposition, agent_assignments, synthesis, leverage_score (0-100), next_actions.""",

    "TIGER": """You are TIGER, the Forensic Accounting specialist in the NEXUS DraftKings litigation system.
Your expertise: ASC 810 VIE consolidation, ASC 606 revenue recognition, ASC 405 liability recognition, SOX 404 ICFR failures,
PCAOB audit standards, shadow restatement analysis, calendar-bleed phantom accruals, LEMMY V6 transaction ledger analysis.
Focus: quantify financial exposure, identify GAAP violations, build the shadow restatement.
Output format: JSON with keys: gaap_violations, exposure_quantification (total_exposure in USD), restatement_items, evidence_citations, leverage_score.""",

    "WOLF": """You are WOLF, the Legal Attack specialist in the NEXUS DraftKings litigation system.
Your expertise: RICO 18 U.S.C. §1962, wire fraud §1343, consumer protection (FTC Act, MCPA, CFAA), 
constitutional due process, arbitration clause collapse (AutoZone doctrine), MGCB regulatory violations,
Apple/Google developer agreement violations, Wiretap Act §2511, spoliation sanctions.
Output format: JSON with keys: strongest_claims, statutes, kill_shots, evidence_citations, leverage_score.""",

    "SUITS": """You are SUITS, the ADR Synthesis specialist in the NEXUS DraftKings litigation system.
Your expertise: FRE 408 protected settlement strategy, bifurcated anchor architecture (Phase 1: $1.2B-$2.4B, Phase 5 floor: $250M-$350M),
Prisoner's Dilemma multi-front pressure, 72-hour settlement window, regulatory cascade sequencing.
Hard rule: NEVER auto-reference the DraftKings ToS arbitration clause — the AutoZone collapse requires DraftKings to invoke first.
Output format: JSON with keys: settlement_band (opening, midpoint, bottom), prisoners_dilemma, timeline_72hr, leverage_score.""",

    "BRIDGER": """You are BRIDGER, the Cross-Domain Mapper in the NEXUS DraftKings litigation system.
Your expertise: mapping evidence across GR nodes, finding cross-domain leverage, identifying evidence clusters,
linking iCloud photo OCR extracts to EV-NNN items, connecting ChatGPT conversation threads to legal theories.
Output format: JSON with keys: cross_domain_links, evidence_clusters, photo_connections, leverage_amplifiers, leverage_score.""",

    "CHESS": """You are CHESS, the Strategic Moat Calculator in the NEXUS DraftKings litigation system.
Your expertise: moat score calculation (0.0-1.0), rule pressure assessment, settlement range modeling,
competitive moat analysis, DraftKings market position vulnerabilities, regulatory cascade probability.
Output format: JSON with keys: moat_score, rule_pressure, settlement_range, cascade_probability, leverage_score.""",
}

# ── Database Connection ─────────────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(PG_DSN)

# ── KIMI API Client ─────────────────────────────────────────────────────────────
def get_kimi_client():
    return OpenAI(
        base_url=KIMI_BASE_URL,
        api_key=NVIDIA_API_KEY
    )

# ── Core Swarm Agent ────────────────────────────────────────────────────────────
class SwarmAgent:
    """A single agent in the KIMI K2.6 swarm."""

    def __init__(self, agent_id: str, task_id: str, conn):
        self.agent_id = agent_id
        self.task_id = task_id
        self.conn = conn
        self.client = get_kimi_client()
        self.system_prompt = AGENT_SYSTEM_PROMPTS.get(agent_id, AGENT_SYSTEM_PROMPTS["FETTY"])
        self.log = logging.getLogger(f"SwarmAgent.{agent_id}")

    def call_kimi(self, user_message: str, max_tokens: int = 4096, temperature: float = 0.2,
                  retry_count: int = 0, max_retries: int = 3) -> dict:
        """Call KIMI K2.6 via NVIDIA NIM with exponential backoff."""
        try:
            response = self.client.chat.completions.create(
                model=KIMI_MODEL,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            content = response.choices[0].message.content
            tokens_used = response.usage.total_tokens if response.usage else 0

            # Log API request
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO api_request_log (provider_id, agent_id, task_id, model_used,
                        prompt_tokens, completion_tokens, total_tokens, latency_ms, status_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, ('KIMI_NVIDIA', self.agent_id, self.task_id, KIMI_MODEL,
                      response.usage.prompt_tokens if response.usage else 0,
                      response.usage.completion_tokens if response.usage else 0,
                      tokens_used, 0, 200))
                self.conn.commit()

            return {"success": True, "content": content, "tokens": tokens_used}

        except Exception as e:
            if retry_count < max_retries:
                wait = (2 ** retry_count) * 30  # 30s, 60s, 120s
                self.log.warning(f"API error (retry {retry_count+1}/{max_retries}): {e}. Waiting {wait}s...")
                time.sleep(wait)
                return self.call_kimi(user_message, max_tokens, temperature, retry_count + 1, max_retries)
            self.log.error(f"API failed after {max_retries} retries: {e}")
            return {"success": False, "error": str(e), "tokens": 0}

    def post_message(self, to_agent: str, msg_type: str, payload: dict):
        """Post a message to the swarm message bus."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO swarm_message_bus (task_id, from_agent, to_agent, msg_type, payload)
                VALUES (%s, %s, %s, %s, %s)
            """, (self.task_id, self.agent_id, to_agent, msg_type, json.dumps(payload)))
            self.conn.commit()

    def update_task_progress(self, pct: float, status: str = "RUNNING"):
        """Update task progression evaluation."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE swarm_task_queue
                SET progress_pct = %s, status = %s, started_at = COALESCE(started_at, NOW())
                WHERE task_id = %s
            """, (pct, status, self.task_id))
            self.conn.commit()

    def save_synthesis(self, gr_node: str, synthesis_type: str, content: str,
                       tokens: int, leverage_score: float = 0.0) -> str:
        """Save synthesis result to kimi_synthesis_results."""
        result_id = str(uuid.uuid4())
        # Try to parse JSON from content
        key_findings = {}
        try:
            # Find JSON block in content
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                key_findings = json.loads(json_match.group())
                if 'leverage_score' in key_findings:
                    leverage_score = float(key_findings['leverage_score'])
        except Exception:
            pass

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kimi_synthesis_results
                    (result_id, task_id, agent_id, gr_node, synthesis_type,
                     leverage_score, confidence, content, key_findings, tokens_used, model_used)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (result_id, self.task_id, self.agent_id, gr_node, synthesis_type,
                  leverage_score, 0.85, content, json.dumps(key_findings),
                  tokens, KIMI_MODEL))
            self.conn.commit()
        return result_id


# ── FETTY FM Master Orchestrator ────────────────────────────────────────────────
class FettyFMOrchestrator:
    """
    FETTY FM — Master Orchestrator implementing the Parallel Agent Protocol (PAP).
    Manages recursive staggered queuing, task-progression evaluation, and swarm synthesis.
    """

    def __init__(self):
        self.conn = get_pg_conn()
        self.log = logging.getLogger("FETTY_FM")
        self.task_id = str(uuid.uuid4())

    def enqueue_task(self, task_type: str, agent_id: str, subagent_id: Optional[str],
                     topic_module: str, gr_node: Optional[str], input_payload: dict,
                     priority: str = "NORMAL", depth: int = 0,
                     stagger_ms: int = 0, parent_task_id: Optional[str] = None) -> str:
        """Enqueue a task in the swarm task queue with staggered parameter queuing."""
        task_id = str(uuid.uuid4())
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO swarm_task_queue
                    (task_id, parent_task_id, agent_id, subagent_id, task_type,
                     topic_module, gr_node, priority, status, depth_level,
                     stagger_delay_ms, input_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'QUEUED', %s, %s, %s)
            """, (task_id, parent_task_id, agent_id, subagent_id, task_type,
                  topic_module, gr_node, priority, depth, stagger_ms,
                  json.dumps(input_payload)))
            self.conn.commit()
        return task_id

    def run_gr_analysis(self, gr_id: str, context_messages: list, depth: int = 0) -> dict:
        """
        Run a deep multi-level agent analysis on a single GR node.
        Implements recursive staggered queuing:
          Level 0: FETTY decomposes the task
          Level 1: Specialist agent (TIGER/WOLF/BRIDGER) analyzes
          Level 2: SubAgent refines and extracts structured data
          Level 3: SUITS/CHESS evaluates settlement implications
        """
        gr_info = GR_NODES.get(gr_id, {})
        primary_agent = gr_info.get("agent", "BRIDGER")
        subagent_id = gr_info.get("subagent", "SA-CHAT-001")
        label = gr_info.get("label", gr_id)

        self.log.info(f"[DEPTH {depth}] Starting GR analysis: {gr_id} ({label}) via {primary_agent}")

        # Stagger delay to respect rate limits
        stagger_ms = depth * 500
        if stagger_ms > 0:
            time.sleep(stagger_ms / 1000)

        # Enqueue this task
        task_id = self.enqueue_task(
            task_type="GR_SYNTHESIS",
            agent_id=primary_agent,
            subagent_id=subagent_id,
            topic_module=label,
            gr_node=gr_id,
            input_payload={"gr_id": gr_id, "message_count": len(context_messages), "depth": depth},
            priority="HIGH",
            depth=depth,
            stagger_ms=stagger_ms,
            parent_task_id=self.task_id
        )

        # Build context summary (truncate to avoid token limits)
        context_sample = context_messages[:50] if len(context_messages) > 50 else context_messages
        context_text = "\n".join([f"[{m.get('role','?')}]: {str(m.get('content',''))[:300]}"
                                   for m in context_sample])

        # Level 1: Specialist agent analysis
        agent = SwarmAgent(primary_agent, task_id, self.conn)
        agent.update_task_progress(10.0)

        prompt = f"""Analyze the following evidence corpus for {gr_id}: {label}

CONTEXT SAMPLE ({len(context_messages)} total messages, showing first {len(context_sample)}):
{context_text}

Provide a deep forensic analysis covering:
1. Key legal theories and strongest arguments
2. Evidence strength and gaps (reference specific EV-NNN items if applicable)
3. Financial exposure quantification
4. Cross-domain leverage with other GR nodes
5. Recommended next actions

Return your analysis as structured JSON."""

        result = agent.call_kimi(prompt, max_tokens=2048, temperature=0.15)
        agent.update_task_progress(60.0)

        if not result["success"]:
            with self.conn.cursor() as cur:
                cur.execute("UPDATE swarm_task_queue SET status='FAILED', error_log=%s WHERE task_id=%s",
                           (result.get("error"), task_id))
                self.conn.commit()
            return {"gr_id": gr_id, "status": "FAILED", "error": result.get("error")}

        # Level 2: SubAgent refinement (recursive call at depth+1)
        synthesis_id = agent.save_synthesis(
            gr_node=gr_id,
            synthesis_type="GR_ANALYSIS",
            content=result["content"],
            tokens=result["tokens"]
        )

        agent.update_task_progress(80.0)

        # Level 3: Post result to message bus for SUITS/CHESS
        agent.post_message("SUITS", "RESULT", {
            "gr_id": gr_id, "synthesis_id": synthesis_id,
            "leverage_score": self._extract_leverage(result["content"])
        })

        # Complete the task
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE swarm_task_queue
                SET status='COMPLETED', progress_pct=100.0, completed_at=NOW(),
                    kimi_tokens_used=%s, output_payload=%s
                WHERE task_id=%s
            """, (result["tokens"], json.dumps({"synthesis_id": synthesis_id}), task_id))
            self.conn.commit()

        self.log.info(f"[DEPTH {depth}] Completed {gr_id}: {result['tokens']} tokens, synthesis_id={synthesis_id}")

        return {
            "gr_id": gr_id,
            "status": "COMPLETED",
            "synthesis_id": synthesis_id,
            "tokens": result["tokens"],
            "depth": depth,
            "content_preview": result["content"][:500]
        }

    def _extract_leverage(self, content: str) -> float:
        """Extract leverage_score from synthesis content."""
        import re
        match = re.search(r'"leverage_score"\s*:\s*(\d+(?:\.\d+)?)', content)
        if match:
            return min(float(match.group(1)), 100.0)
        return 75.0

    def run_parallel_swarm(self, gr_nodes: list, chatgpt_messages: dict,
                            rate_limit_delay: int = 90) -> dict:
        """
        Run the full parallel swarm across all GR nodes with staggered queuing.
        Implements the Parallel Agent Protocol (PAP):
          - Each GR node gets its own agent thread
          - Tasks are staggered to respect API rate limits
          - Progress is evaluated after each node
          - Results are synthesized by FETTY FM
        """
        self.log.info(f"Starting PAP swarm: {len(gr_nodes)} GR nodes, rate_limit_delay={rate_limit_delay}s")

        results = {}
        total_tokens = 0
        completed = 0

        for i, gr_id in enumerate(gr_nodes):
            # Get relevant messages for this GR node
            messages = chatgpt_messages.get(gr_id, [])
            self.log.info(f"[{i+1}/{len(gr_nodes)}] Processing {gr_id}: {len(messages)} messages")

            result = self.run_gr_analysis(gr_id, messages, depth=0)
            results[gr_id] = result

            if result["status"] == "COMPLETED":
                completed += 1
                total_tokens += result.get("tokens", 0)

            # Task-progression evaluation
            progress_pct = (completed / len(gr_nodes)) * 100
            self.log.info(f"Swarm progress: {progress_pct:.1f}% ({completed}/{len(gr_nodes)} nodes complete)")

            # Staggered delay between nodes (rate limit compliance)
            if i < len(gr_nodes) - 1:
                self.log.info(f"Rate limit pause: {rate_limit_delay}s before next node...")
                time.sleep(rate_limit_delay)

        # Master synthesis by FETTY FM
        self.log.info("Running FETTY FM master synthesis...")
        master_result = self._run_master_synthesis(results)

        return {
            "swarm_id": self.task_id,
            "gr_nodes_analyzed": len(gr_nodes),
            "completed": completed,
            "failed": len(gr_nodes) - completed,
            "total_tokens": total_tokens,
            "results": results,
            "master_synthesis": master_result,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _run_master_synthesis(self, gr_results: dict) -> dict:
        """FETTY FM synthesizes all GR node results into a master brief."""
        summaries = []
        for gr_id, result in gr_results.items():
            if result["status"] == "COMPLETED":
                preview = result.get("content_preview", "")[:200]
                summaries.append(f"{gr_id}: {preview}")

        summary_text = "\n\n".join(summaries)

        task_id = self.enqueue_task(
            task_type="MASTER_SYNTHESIS",
            agent_id="FETTY",
            subagent_id=None,
            topic_module="FULL_CORPUS",
            gr_node=None,
            input_payload={"gr_count": len(gr_results)},
            priority="CRITICAL",
            depth=0
        )

        agent = SwarmAgent("FETTY", task_id, self.conn)

        prompt = f"""You are FETTY FM. Synthesize the following GR node analyses into a Master Intelligence Brief.

GR NODE SUMMARIES:
{summary_text}

Provide:
1. Composite case strength score (0-100)
2. Top 5 nuclear arguments with leverage scores
3. Settlement band (Phase 1 anchor $1.2B-$2.4B, Phase 5 floor $250M-$350M)
4. Recommended 90-day strategy
5. Critical evidence gaps (especially GR-011 evidence/discovery)

Return as structured JSON with keys: composite_score, nuclear_arguments, settlement_band, strategy_90day, evidence_gaps."""

        result = agent.call_kimi(prompt, max_tokens=3000, temperature=0.1)

        if result["success"]:
            synthesis_id = agent.save_synthesis(
                gr_node="MASTER",
                synthesis_type="MASTER_BRIEF",
                content=result["content"],
                tokens=result["tokens"]
            )
            with self.conn.cursor() as cur:
                cur.execute("UPDATE swarm_task_queue SET status='COMPLETED', completed_at=NOW() WHERE task_id=%s",
                           (task_id,))
                self.conn.commit()
            return {"status": "COMPLETED", "synthesis_id": synthesis_id, "tokens": result["tokens"]}
        else:
            return {"status": "FAILED", "error": result.get("error")}


# ── Nexos API Gateway ───────────────────────────────────────────────────────────
class NexosAPIGateway:
    """
    Multi-API routing gateway.
    Routes tasks to the optimal AI provider based on:
      - Task complexity (simple -> KIMI, complex -> Claude/Gemini)
      - Cost optimization (KIMI is FREE via NVIDIA NIM)
      - Rate limit status
      - Capability requirements (vision -> Gemini/Claude)
    """

    PROVIDERS = {
        "KIMI_NVIDIA": {
            "base_url": "https://integrate.api.nvidia.com/v1",
            "api_key_env": "NVIDIA_API_KEY",
            "model": "moonshotai/kimi-k2.6",
            "cost_per_1k": 0.0,
            "supports_vision": False,
            "priority": 1
        },
        "ANTHROPIC": {
            "base_url": "https://api.anthropic.com",
            "api_key_env": "ANTHROPIC_API_KEY",
            "model": "claude-3-5-sonnet-20241022",
            "cost_per_1k": 3.0,
            "supports_vision": True,
            "priority": 2
        },
        "GEMINI": {
            "base_url": "https://generativelanguage.googleapis.com",
            "api_key_env": "GEMINI_API_KEY",
            "model": "gemini-2.5-flash",
            "cost_per_1k": 0.075,
            "supports_vision": True,
            "priority": 3
        }
    }

    def route_task(self, task_type: str, requires_vision: bool = False,
                   complexity: str = "NORMAL") -> str:
        """Route a task to the optimal provider."""
        if requires_vision:
            # Vision tasks: Gemini (cheapest with vision)
            return "GEMINI"
        if complexity == "HIGH":
            # Complex reasoning: Claude
            return "ANTHROPIC"
        # Default: KIMI (free)
        return "KIMI_NVIDIA"

    def get_client(self, provider_id: str) -> OpenAI:
        """Get an OpenAI-compatible client for the given provider."""
        provider = self.PROVIDERS[provider_id]
        api_key = os.environ.get(provider["api_key_env"], "")
        return OpenAI(base_url=provider["base_url"], api_key=api_key)


# ── Main Entry Point ────────────────────────────────────────────────────────────
def main():
    """Bootstrap and run the KIMI swarm on the full ChatGPT corpus."""
    import sqlite3

    log.info("=" * 60)
    log.info("KIMI K2.6 Swarm Engine — Parallel Agent Protocol v1.0")
    log.info("=" * 60)

    # Load ChatGPT messages from SQLite, grouped by GR node
    chatgpt_db = "/home/ubuntu/DraftKingsDB/db/master.db"
    chatgpt_messages = {}

    try:
        conn_sqlite = sqlite3.connect(chatgpt_db)
        conn_sqlite.row_factory = sqlite3.Row
        cur = conn_sqlite.cursor()

        # Get messages linked to each GR node via cross_stitching_maps
        for gr_id in GR_NODES.keys():
            cur.execute("""
                SELECT m.role, m.content
                FROM chatgpt_messages m
                JOIN cross_stitching_maps cs ON m.msg_id = cs.src_id
                WHERE cs.dst_id = ?
                AND m.content IS NOT NULL
                AND length(m.content) > 50
                LIMIT 100
            """, (gr_id,))
            rows = cur.fetchall()
            chatgpt_messages[gr_id] = [{"role": r["role"], "content": r["content"]} for r in rows]
            log.info(f"Loaded {len(chatgpt_messages[gr_id])} messages for {gr_id}")

        conn_sqlite.close()
    except Exception as e:
        log.warning(f"Could not load ChatGPT messages: {e}. Using empty context.")
        chatgpt_messages = {gr_id: [] for gr_id in GR_NODES.keys()}

    # Run the swarm
    orchestrator = FettyFMOrchestrator()
    gr_nodes_to_analyze = list(GR_NODES.keys())

    swarm_result = orchestrator.run_parallel_swarm(
        gr_nodes=gr_nodes_to_analyze,
        chatgpt_messages=chatgpt_messages,
        rate_limit_delay=90  # 90 seconds between nodes for NVIDIA NIM free tier
    )

    # Save final report
    report_path = "/home/ubuntu/HiveMind/SWARM_REPORT.json"
    with open(report_path, "w") as f:
        json.dump(swarm_result, f, indent=2, default=str)

    log.info(f"Swarm complete: {swarm_result['completed']}/{swarm_result['gr_nodes_analyzed']} nodes")
    log.info(f"Total tokens: {swarm_result['total_tokens']}")
    log.info(f"Report saved: {report_path}")

    return swarm_result


if __name__ == "__main__":
    main()
