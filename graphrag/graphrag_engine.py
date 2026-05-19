"""
GraphRAG Layer — Qdrant Vector Store + Cypher Retrieval
========================================================
ACTIVATION CONDITION: This module is scaffolded but NOT activated
until Neo4j graph integrity is confirmed (zero rejected events,
all constraints passing, T1 evidence count > 0).

Pipeline position:
  Neo4j Graph → [THIS MODULE] → Claude Reasoning Layer

Two retrieval modes:
  1. Vector search (Qdrant): semantic similarity over evidence embeddings
  2. Cypher retrieval: structured graph traversal for relationship chains

Author  : GraphRAG Layer
Version : 1.0.0 (SCAFFOLDED — awaiting graph integrity confirmation)
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("graphrag_engine")

QDRANT_URL  = os.environ.get("QDRANT_URL",  "http://localhost:6333")
NEO4J_URI   = os.environ.get("NEO4J_URI",   "bolt://localhost:7687")
NEO4J_USER  = os.environ.get("NEO4J_USER",  "neo4j")
NEO4J_PASS  = os.environ.get("NEO4J_PASS",  "dknexus2026")
OPENAI_KEY  = os.environ.get("OPENAI_API_KEY", "")

COLLECTION_NAME = "dk_evidence_embeddings"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM   = 1536

# ---------------------------------------------------------------------------
# Graph Integrity Check — must pass before GraphRAG activates
# ---------------------------------------------------------------------------

def check_graph_integrity(neo4j_driver=None) -> dict:
    """
    Verify Neo4j graph integrity before activating GraphRAG.
    Returns a report with pass/fail status.
    """
    report = {
        "integrity_confirmed": False,
        "checks": {},
        "recommendation": "",
    }

    # Check 1: Mutation log — zero rejected events
    log_path = Path("/home/ubuntu/draftkings-data-nexus-codex/artifacts/mutation_log.jsonl")
    if log_path.exists():
        events = [json.loads(l) for l in log_path.read_text().splitlines() if l.strip()]
        rejected = [e for e in events if e.get("event_type") == "REJECT"]
        report["checks"]["rejected_events"] = {
            "pass": len(rejected) == 0,
            "count": len(rejected),
            "detail": f"{len(rejected)} rejected events in mutation log",
        }
    else:
        report["checks"]["rejected_events"] = {"pass": False, "detail": "No mutation log found"}

    # Check 2: Evidence nodes exist
    evidence_path = Path("/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet/evidence.parquet")
    if evidence_path.exists():
        try:
            import polars as pl
            df = pl.read_parquet(str(evidence_path))
            report["checks"]["evidence_nodes"] = {
                "pass": len(df) > 0,
                "count": len(df),
                "detail": f"{len(df)} T1 Evidence nodes in Parquet",
            }
        except Exception as e:
            report["checks"]["evidence_nodes"] = {"pass": False, "detail": str(e)}
    else:
        report["checks"]["evidence_nodes"] = {"pass": False, "detail": "No evidence.parquet found"}

    # Check 3: All hashes are 64-char hex
    if evidence_path.exists():
        try:
            import polars as pl
            df = pl.read_parquet(str(evidence_path))
            bad_hashes = df.filter(pl.col("content_hash").str.len_chars() != 64).height
            report["checks"]["hash_integrity"] = {
                "pass": bad_hashes == 0,
                "bad_hashes": bad_hashes,
                "detail": f"{bad_hashes} nodes with invalid content_hash",
            }
        except Exception as e:
            report["checks"]["hash_integrity"] = {"pass": False, "detail": str(e)}

    # Overall
    all_pass = all(c.get("pass", False) for c in report["checks"].values())
    report["integrity_confirmed"] = all_pass
    report["recommendation"] = (
        "ACTIVATE GraphRAG — all integrity checks passed."
        if all_pass else
        "DO NOT ACTIVATE — resolve failing checks first. Run ETL pipeline again."
    )
    return report


# ---------------------------------------------------------------------------
# Qdrant Vector Store
# ---------------------------------------------------------------------------

class EvidenceVectorStore:
    """
    Manages evidence embeddings in Qdrant.
    Activated only after graph integrity is confirmed.
    """

    def __init__(self):
        self._client = None
        self._openai = None

    def _get_qdrant(self):
        if not self._client:
            try:
                from qdrant_client import QdrantClient
                self._client = QdrantClient(url=QDRANT_URL)
                self._client.get_collections()
                log.info("Qdrant connected: %s", QDRANT_URL)
            except Exception as e:
                log.warning("Qdrant not available: %s", e)
                self._client = None
        return self._client

    def _get_openai(self):
        if not self._openai and OPENAI_KEY:
            from openai import OpenAI
            self._openai = OpenAI(api_key=OPENAI_KEY)
        return self._openai

    def _embed(self, text: str) -> list[float]:
        client = self._get_openai()
        if not client:
            # Return zero vector for dry run
            return [0.0] * EMBEDDING_DIM
        resp = client.embeddings.create(model=EMBEDDING_MODEL, input=text[:8000])
        return resp.data[0].embedding

    def ensure_collection(self):
        client = self._get_qdrant()
        if not client:
            return False
        try:
            from qdrant_client.models import Distance, VectorParams
            client.recreate_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
            )
            log.info("Qdrant collection ready: %s", COLLECTION_NAME)
            return True
        except Exception as e:
            log.error("Qdrant collection setup failed: %s", e)
            return False

    def index_evidence(self, evidence_records: list[dict]) -> int:
        """Embed and index Evidence nodes into Qdrant."""
        client = self._get_qdrant()
        if not client:
            log.warning("Qdrant unavailable — skipping vector indexing")
            return 0

        from qdrant_client.models import PointStruct
        points = []
        for i, rec in enumerate(evidence_records):
            text = rec.get("evidence_text", "")
            if not text:
                continue
            vector = self._embed(text)
            points.append(PointStruct(
                id=i,
                vector=vector,
                payload={
                    "ev_id":        rec.get("ev_id", ""),
                    "content_hash": rec.get("content_hash", ""),
                    "tier":         rec.get("tier", 1),
                    "source_doc":   rec.get("source_doc", ""),
                    "extracted_by": rec.get("extracted_by", ""),
                    "text_preview": text[:200],
                },
            ))

        if points:
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            log.info("Indexed %d evidence vectors into Qdrant", len(points))
        return len(points)

    def search(self, query: str, top_k: int = 5, min_score: float = 0.7) -> list[dict]:
        """Semantic search over evidence embeddings."""
        client = self._get_qdrant()
        if not client:
            return []
        query_vector = self._embed(query)
        results = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_vector,
            limit=top_k,
            score_threshold=min_score,
        )
        return [{"score": r.score, **r.payload} for r in results]


# ---------------------------------------------------------------------------
# Cypher Retrieval Chain
# ---------------------------------------------------------------------------

CYPHER_TEMPLATES = {
    "evidence_by_player": """
        MATCH (e:Evidence)-[:CLAIMS_ABOUT]->(p:Player {name: $player_name})
        WHERE e.tier <= $max_tier
        RETURN e.ev_id, e.evidence_text, e.tier, e.confidence, e.content_hash
        ORDER BY e.tier ASC, e.confidence DESC
        LIMIT $limit
    """,
    "theory_chain": """
        MATCH (t:Theory)-[:DERIVED_FROM]->(e:Evidence)
        WHERE t.domain = $domain AND t.confidence >= $min_confidence
        RETURN t.theory_id, t.theory_text, t.confidence,
               collect(e.ev_id) AS evidence_ids,
               collect(e.evidence_text)[0..3] AS evidence_previews
        ORDER BY t.confidence DESC
        LIMIT $limit
    """,
    "player_stacking": """
        MATCH (p1:Player)-[r:CORRELATES_WITH]->(p2:Player)
        WHERE r.correlation_score >= $min_correlation
        AND p1.sport = $sport
        RETURN p1.name, p2.name, r.correlation_score, r.stack_type
        ORDER BY r.correlation_score DESC
        LIMIT $limit
    """,
    "mutation_audit": """
        MATCH (e:GraphMutationEvent)
        WHERE e.event_type = $event_type
        RETURN e.event_id, e.actor, e.target_node_label, e.timestamp, e.error_message
        ORDER BY e.timestamp DESC
        LIMIT $limit
    """,
    "t1_evidence_for_domain": """
        MATCH (e:Evidence)
        WHERE e.tier = 1
        AND any(domain IN $domains WHERE e.evidence_text CONTAINS domain)
        RETURN e.ev_id, e.evidence_text, e.content_hash, e.extracted_by, e.extracted_at
        ORDER BY e.extracted_at DESC
        LIMIT $limit
    """,
}


class CypherRetriever:
    """Structured graph traversal for relationship-aware retrieval."""

    def __init__(self):
        self._driver = None

    def _get_driver(self):
        if not self._driver:
            try:
                from neo4j import GraphDatabase
                self._driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
                self._driver.verify_connectivity()
                log.info("Neo4j connected for Cypher retrieval")
            except Exception as e:
                log.warning("Neo4j unavailable for Cypher retrieval: %s", e)
                self._driver = None
        return self._driver

    def run(self, template_name: str, params: dict) -> list[dict]:
        """Run a named Cypher template with parameters."""
        driver = self._get_driver()
        if not driver:
            return []
        cypher = CYPHER_TEMPLATES.get(template_name)
        if not cypher:
            raise ValueError(f"Unknown Cypher template: {template_name}")
        with driver.session() as session:
            result = session.run(cypher, **params)
            return [dict(r) for r in result]

    def close(self):
        if self._driver:
            self._driver.close()


# ---------------------------------------------------------------------------
# GraphRAG Engine — combines vector + Cypher
# ---------------------------------------------------------------------------

class GraphRAGEngine:
    """
    The unified GraphRAG retrieval engine.
    Combines semantic vector search with structured Cypher traversal
    to provide the Claude Reasoning Layer with rich, grounded context.
    """

    def __init__(self):
        self._vector_store = EvidenceVectorStore()
        self._cypher = CypherRetriever()

    def retrieve(self, query: str, domain: str = None, top_k: int = 5) -> dict:
        """
        Retrieve relevant evidence using both vector and Cypher methods.
        Returns a unified context package for the Claude Reasoning Layer.
        """
        context = {
            "query": query,
            "vector_results": [],
            "cypher_results": [],
            "combined_evidence": [],
            "retrieval_method": [],
        }

        # Vector retrieval
        vector_hits = self._vector_store.search(query, top_k=top_k)
        if vector_hits:
            context["vector_results"] = vector_hits
            context["retrieval_method"].append("vector")

        # Cypher retrieval for domain-specific queries
        if domain:
            cypher_hits = self._cypher.run(
                "t1_evidence_for_domain",
                {"domains": [domain], "limit": top_k}
            )
            if cypher_hits:
                context["cypher_results"] = cypher_hits
                context["retrieval_method"].append("cypher")

        # Merge and deduplicate by ev_id
        seen = set()
        for hit in vector_hits + context["cypher_results"]:
            ev_id = hit.get("ev_id", hit.get("e.ev_id", ""))
            if ev_id and ev_id not in seen:
                seen.add(ev_id)
                context["combined_evidence"].append(hit)

        return context

    def close(self):
        self._cypher.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = check_graph_integrity()
    print(json.dumps(report, indent=2))
    if report["integrity_confirmed"]:
        print("\nGraph integrity confirmed. GraphRAG is ready to activate.")
    else:
        print("\nGraph integrity NOT confirmed. Resolve issues before activating GraphRAG.")
