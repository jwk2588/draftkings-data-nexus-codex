"""
ETL Step 4 — Neo4j Ingestor with Tier Enforcement Middleware
=============================================================
Ingests extracted entities and evidence nodes into Neo4j,
enforcing the frozen ontology at every write boundary.

Pipeline position:
  entities.parquet + evidence.parquet → [THIS MODULE] → Neo4j Graph

Implements:
  - validate_tier_permissions()
  - validate_node_schema()
  - validate_evidence_constraints()
  - GraphMutationEvent append-only log
  - SHA-256 duplicate detection

Author  : github-gem-seeker ETL Pipeline
Version : 1.0.0
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import polars as pl

log = logging.getLogger("neo4j_ingestor")

# ---------------------------------------------------------------------------
# Neo4j connection (lazy — only connects when Neo4j is running)
# ---------------------------------------------------------------------------

NEO4J_URI  = os.environ.get("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "dknexus2026")


def _get_driver():
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        return driver
    except Exception as e:
        log.warning("Neo4j not available: %s — running in DRY RUN mode", e)
        return None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Tier Enforcement Middleware
# ---------------------------------------------------------------------------

WRITE_AUTHORITY = {
    "T1": {"gemini_extractor", "chatgpt_etl", "human_curator"},
    "T2": {"claude_reasoning_layer", "gemini_extractor", "algo_code_writer"},
    "T3": None,  # Any agent
}

T1_REQUIRED_FIELDS = {"ev_id", "tier", "source_doc", "evidence_text", "extracted_by", "extracted_at", "content_hash"}
T2_REQUIRED_FIELDS = {"theory_id", "theory_text", "derived_by", "derived_at", "confidence"}

IMMUTABLE_LABELS = {"Evidence", "SourceDocument", "GraphMutationEvent"}


class TierEnforcementError(Exception):
    pass


class SchemaValidationError(Exception):
    pass


class EvidenceConstraintError(Exception):
    pass


def validate_tier_permissions(actor: str, target_tier: int) -> bool:
    """
    Check 1: Does the writing agent have authority for this tier?
    """
    tier_key = f"T{target_tier}"
    allowed = WRITE_AUTHORITY.get(tier_key)
    if allowed is None:
        return True  # T3: any agent allowed
    if actor not in allowed:
        raise TierEnforcementError(
            f"Agent '{actor}' does not have write authority for tier {target_tier}. "
            f"Allowed: {allowed}"
        )
    return True


def validate_node_schema(node_label: str, node_data: dict) -> bool:
    """
    Check 2: Are all required fields present and content_hash valid?
    """
    if node_label == "Evidence":
        missing = T1_REQUIRED_FIELDS - set(node_data.keys())
        if missing:
            raise SchemaValidationError(f"Evidence node missing required fields: {missing}")
        h = node_data.get("content_hash", "")
        if not h or len(h) != 64:
            raise SchemaValidationError(f"Evidence node has invalid content_hash: '{h}'")

    elif node_label == "Theory":
        missing = T2_REQUIRED_FIELDS - set(node_data.keys())
        if missing:
            raise SchemaValidationError(f"Theory node missing required fields: {missing}")
        conf = node_data.get("confidence", -1)
        if not (0.0 <= float(conf) <= 1.0):
            raise SchemaValidationError(f"Theory confidence out of range: {conf}")

    return True


def validate_evidence_constraints(node_label: str, node_data: dict, existing_hashes: set) -> bool:
    """
    Check 3: Duplicate detection and evidence integrity rules.
    """
    content_hash = node_data.get("content_hash", "")
    if content_hash and content_hash in existing_hashes:
        raise EvidenceConstraintError(
            f"Duplicate content_hash detected for {node_label}: {content_hash[:16]}..."
        )
    return True


# ---------------------------------------------------------------------------
# GraphMutationEvent Logger
# ---------------------------------------------------------------------------

class MutationEventLog:
    """
    Append-only log of all graph mutations.
    Writes to both Neo4j (when available) and a local JSONL file.
    """

    def __init__(self, log_path: str = None):
        self._log_path = Path(log_path or "/home/ubuntu/draftkings-data-nexus-codex/artifacts/mutation_log.jsonl")
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._session_id = str(uuid.uuid4())[:8]

    def log(
        self,
        event_type: str,
        target_node_id: str,
        target_node_label: str,
        actor: str,
        actor_tier: str,
        payload: dict,
        previous_hash: str = None,
        reasoning_trace: str = None,
        error_message: str = None,
    ) -> dict:
        event = {
            "event_id":          str(uuid.uuid4()),
            "event_type":        event_type,
            "target_node_id":    target_node_id,
            "target_node_label": target_node_label,
            "actor":             actor,
            "actor_tier":        actor_tier,
            "payload_hash":      _sha256(json.dumps(payload, sort_keys=True)),
            "previous_hash":     previous_hash,
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "session_id":        self._session_id,
            "reasoning_trace":   reasoning_trace,
            "error_message":     error_message,
        }
        with open(self._log_path, "a") as f:
            f.write(json.dumps(event) + "\n")
        return event

    def replay(self) -> list[dict]:
        """Read the full immutable event log."""
        if not self._log_path.exists():
            return []
        events = []
        with open(self._log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
        return events


# ---------------------------------------------------------------------------
# Neo4j Ingestor
# ---------------------------------------------------------------------------

class Neo4jIngestor:
    """
    Ingests entities and evidence into Neo4j with full tier enforcement.
    Falls back to dry-run logging when Neo4j is unavailable.
    """

    def __init__(self):
        self._driver = _get_driver()
        self._event_log = MutationEventLog()
        self._seen_hashes: set[str] = set()
        self._dry_run = self._driver is None
        if self._dry_run:
            log.warning("DRY RUN MODE — Neo4j unavailable. All writes logged to mutation_log.jsonl only.")

    def _write_node(self, session, label: str, data: dict, actor: str, tier: int):
        """Write a single node to Neo4j with full middleware enforcement."""
        # Run all three middleware checks
        try:
            validate_tier_permissions(actor, tier)
            validate_node_schema(label, data)
            validate_evidence_constraints(label, data, self._seen_hashes)
        except (TierEnforcementError, SchemaValidationError, EvidenceConstraintError) as e:
            self._event_log.log(
                event_type="REJECT",
                target_node_id=data.get("ev_id", data.get("player_id", "unknown")),
                target_node_label=label,
                actor=actor,
                actor_tier=f"T{tier}",
                payload=data,
                error_message=str(e),
            )
            log.warning("REJECTED %s node: %s", label, e)
            return False

        # Track hash
        h = data.get("content_hash", "")
        if h:
            self._seen_hashes.add(h)

        node_id = data.get("ev_id", data.get("player_id", data.get("team_id", data.get("market_id", str(uuid.uuid4())))))

        # Log mutation event
        self._event_log.log(
            event_type="CREATE",
            target_node_id=node_id,
            target_node_label=label,
            actor=actor,
            actor_tier=f"T{tier}",
            payload=data,
        )

        if self._dry_run:
            return True

        # Write to Neo4j
        props = {k: v for k, v in data.items() if v is not None}
        cypher = f"MERGE (n:{label} {{content_hash: $content_hash}}) SET n += $props"
        session.run(cypher, content_hash=data.get("content_hash", ""), props=props)
        return True

    def ingest_evidence(self, df: pl.DataFrame) -> dict:
        """Ingest Evidence nodes (T1) from the evidence DataFrame."""
        results = {"written": 0, "rejected": 0}
        if len(df) == 0:
            return results

        if self._dry_run:
            for row in df.iter_rows(named=True):
                ok = self._write_node(None, "Evidence", dict(row), "chatgpt_etl", 1)
                if ok:
                    results["written"] += 1
                else:
                    results["rejected"] += 1
        else:
            with self._driver.session() as session:
                for row in df.iter_rows(named=True):
                    ok = self._write_node(session, "Evidence", dict(row), "chatgpt_etl", 1)
                    if ok:
                        results["written"] += 1
                    else:
                        results["rejected"] += 1

        log.info("Evidence: %d written, %d rejected", results["written"], results["rejected"])
        return results

    def ingest_players(self, df: pl.DataFrame) -> dict:
        """Ingest Player nodes (T3) from the players DataFrame."""
        results = {"written": 0, "rejected": 0}
        if len(df) == 0:
            return results

        if self._dry_run:
            for row in df.iter_rows(named=True):
                ok = self._write_node(None, "Player", dict(row), "chatgpt_etl", 3)
                if ok:
                    results["written"] += 1
                else:
                    results["rejected"] += 1
        else:
            with self._driver.session() as session:
                for row in df.iter_rows(named=True):
                    ok = self._write_node(session, "Player", dict(row), "chatgpt_etl", 3)
                    if ok:
                        results["written"] += 1
                    else:
                        results["rejected"] += 1

        log.info("Players: %d written, %d rejected", results["written"], results["rejected"])
        return results

    def ingest_teams(self, df: pl.DataFrame) -> dict:
        """Ingest Team nodes (T3) from the teams DataFrame."""
        results = {"written": 0, "rejected": 0}
        if len(df) == 0:
            return results

        if self._dry_run:
            for row in df.iter_rows(named=True):
                ok = self._write_node(None, "Team", dict(row), "chatgpt_etl", 3)
                if ok:
                    results["written"] += 1
                else:
                    results["rejected"] += 1
        else:
            with self._driver.session() as session:
                for row in df.iter_rows(named=True):
                    ok = self._write_node(session, "Team", dict(row), "chatgpt_etl", 3)
                    if ok:
                        results["written"] += 1
                    else:
                        results["rejected"] += 1

        log.info("Teams: %d written, %d rejected", results["written"], results["rejected"])
        return results

    def run_full_ingest(self, parquet_dir: str | Path) -> dict:
        """
        Run the complete ingest pipeline from Parquet files.
        """
        parquet_dir = Path(parquet_dir)
        summary = {}

        for label, fname, method in [
            ("Evidence", "evidence.parquet", self.ingest_evidence),
            ("Player",   "players.parquet",  self.ingest_players),
            ("Team",     "teams.parquet",     self.ingest_teams),
        ]:
            path = parquet_dir / fname
            if path.exists():
                df = pl.read_parquet(str(path))
                summary[label] = method(df)
            else:
                log.warning("Parquet not found, skipping: %s", path)
                summary[label] = {"written": 0, "rejected": 0, "skipped": True}

        # Write mutation log summary
        events = self._event_log.replay()
        summary["mutation_events"] = len(events)
        summary["rejected_events"] = sum(1 for e in events if e["event_type"] == "REJECT")
        summary["dry_run"] = self._dry_run

        log.info("Ingest complete: %s", json.dumps(summary))
        return summary

    def close(self):
        if self._driver:
            self._driver.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingestor = Neo4jIngestor()
    summary = ingestor.run_full_ingest("/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet")
    print(json.dumps(summary, indent=2))
    ingestor.close()
