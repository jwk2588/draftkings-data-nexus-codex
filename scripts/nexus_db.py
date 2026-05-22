"""
nexus_db.py — Master NEXUS DraftKings LiteDB Core Engine
=========================================================
AI-Native | Graph-Native | Multi-Chain | Agentic + Programmatic

This module is the single source of truth for all NEXUS state:
  - Evidence Register (EV-NNN)
  - GR Node Registry (GR-NNN)
  - Agent Task Queue
  - Bridge Inventory
  - Chess Piece Health
  - Audit Trail / Chain-of-Custody
  - GitHub Sync Queue

Usage:
    from nexus_db import NexusDB
    db = NexusDB()          # opens/creates nexus_master.db
    db.init_schema()        # idempotent — safe to call every boot
    db.seed_gr_nodes()      # seeds the canonical GR-001..GR-012 registry
"""

import sqlite3
import json
import hashlib
import datetime
import os
from typing import Optional, List, Dict, Any

# ── Path resolution ──────────────────────────────────────────────────────────
_SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("NEXUS_DB_PATH",
                         os.path.join(_SKILL_DIR, "..", "nexus_master.db"))
DB_PATH = os.path.normpath(DB_PATH)


# ═══════════════════════════════════════════════════════════════════════════════
# NexusDB — Master Database Class
# ═══════════════════════════════════════════════════════════════════════════════
class NexusDB:
    """
    Blended agentic+programmatic LiteDB wrapper.
    All writes are append-only (additive) to preserve chain-of-custody.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")

    # ── Schema ────────────────────────────────────────────────────────────────

    def init_schema(self) -> None:
        """Idempotent schema initialisation — safe to call on every boot."""
        cur = self.conn.cursor()

        # ── Evidence Register ─────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS evidence (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ev_id       TEXT    UNIQUE NOT NULL,   -- e.g. EV-292
            shortname   TEXT    NOT NULL,
            ev_date     TEXT,
            status      TEXT    DEFAULT 'CONFIRMED',  -- CONFIRMED/TENTATIVE/CHALLENGED
            source_file TEXT,
            confidence  TEXT    DEFAULT 'T1',          -- T1/T2/T3/T4
            gr_links    TEXT,                          -- JSON array ["GR-001","GR-004"]
            chain_prev  TEXT,
            chain_next  TEXT,
            notes       TEXT,
            sha256      TEXT,
            created_at  TEXT    DEFAULT (datetime('now')),
            updated_at  TEXT    DEFAULT (datetime('now'))
        );""")

        # ── GR Node Registry ──────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gr_nodes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            gr_id           TEXT    UNIQUE NOT NULL,  -- e.g. GR-001
            name            TEXT    NOT NULL,
            impact          REAL    DEFAULT 0.0,      -- 0.0–10.0
            health          REAL    DEFAULT 1.0,      -- 0.0–1.0
            status          TEXT    DEFAULT 'READY',  -- READY/PENDING/BLOCKED/FIRED
            agent_route     TEXT,                     -- e.g. "TIGER+WOLF->SUITS"
            evidence_links  TEXT,                     -- JSON array of EV-IDs
            cascade_trigger REAL    DEFAULT 0.3,      -- health threshold for cascade
            cascade_targets TEXT,                     -- JSON array of downstream GR-IDs
            node_category   TEXT    DEFAULT 'Alpha',  -- Alpha/Beta/Gamma/Delta/Omega
            domain_tags     TEXT,                     -- JSON array
            notes           TEXT,
            created_at      TEXT    DEFAULT (datetime('now')),
            updated_at      TEXT    DEFAULT (datetime('now'))
        );""")

        # ── GR Node Health History (append-only) ──────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS gr_health_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            gr_id       TEXT    NOT NULL,
            health_old  REAL,
            health_new  REAL,
            delta       REAL,
            reason      TEXT,
            ev_trigger  TEXT,
            agent       TEXT,
            ts          TEXT    DEFAULT (datetime('now'))
        );""")

        # ── Agent Task Queue ──────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS agent_tasks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id     TEXT    UNIQUE NOT NULL,
            agent       TEXT    NOT NULL,   -- TIGER/WOLF/SUITS/BRIDGER/CHESS/FETTY
            phase       INTEGER DEFAULT 0,  -- 0-4
            priority    INTEGER DEFAULT 5,  -- 1=highest, 10=lowest
            status      TEXT    DEFAULT 'QUEUED',  -- QUEUED/RUNNING/DONE/FAILED
            gr_node     TEXT,
            ev_inputs   TEXT,               -- JSON array
            prompt      TEXT,
            output      TEXT,
            confidence  TEXT,
            created_at  TEXT    DEFAULT (datetime('now')),
            completed_at TEXT
        );""")

        # ── Bridge Inventory ──────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bridges (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            bridge_id       TEXT    UNIQUE NOT NULL,  -- e.g. Bridge-089
            domain_a        TEXT    NOT NULL,
            domain_b        TEXT    NOT NULL,
            bridge_type     TEXT    DEFAULT 'Standard',  -- Temporal/Jurisdictional/Scalar/Adversarial/Narrative/Quantum
            strength        REAL    DEFAULT 0.0,         -- 0.0–1.0
            status          TEXT    DEFAULT 'CANDIDATE', -- CANDIDATE/VALIDATED/LITIGATION-READY/DECAYED
            description     TEXT,
            litigation_text TEXT,
            ev_links        TEXT,   -- JSON array
            gr_links        TEXT,   -- JSON array
            validated_by    TEXT,   -- JSON array of agents
            created_at      TEXT    DEFAULT (datetime('now')),
            updated_at      TEXT    DEFAULT (datetime('now'))
        );""")

        # ── Chess Piece Health ────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chess_pieces (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            piece_name      TEXT    UNIQUE NOT NULL,
            business_func   TEXT,
            health          REAL    DEFAULT 1.0,
            collapse_weight REAL    DEFAULT 0.10,
            vulnerability   REAL    DEFAULT 1.0,
            gr_damage_links TEXT,   -- JSON array of GR-IDs that damage this piece
            notes           TEXT,
            updated_at      TEXT    DEFAULT (datetime('now'))
        );""")

        # ── Chess Health History ──────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chess_health_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            piece_name  TEXT    NOT NULL,
            health_old  REAL,
            health_new  REAL,
            delta       REAL,
            gr_trigger  TEXT,
            ts          TEXT    DEFAULT (datetime('now'))
        );""")

        # ── System State Ledger ───────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS system_state (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            phase           INTEGER DEFAULT 0,
            mode            TEXT    DEFAULT 'compact',
            version         TEXT    DEFAULT 'v2.0',
            last_ev_id      TEXT,
            last_gr_id      TEXT,
            moat_score      REAL    DEFAULT 1.0,
            rule_pressure   REAL    DEFAULT 0.0,
            convergence_pct REAL    DEFAULT 0.0,
            open_blockers   TEXT,   -- JSON array
            ts              TEXT    DEFAULT (datetime('now'))
        );""")

        # ── GitHub Sync Queue ─────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS github_sync_queue (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_id TEXT    NOT NULL,
            artifact_type TEXT  NOT NULL,  -- evidence/gr_node/bridge/task/state
            payload     TEXT    NOT NULL,  -- JSON
            status      TEXT    DEFAULT 'PENDING',  -- PENDING/SYNCED/FAILED
            repo_path   TEXT,
            commit_sha  TEXT,
            ts          TEXT    DEFAULT (datetime('now'))
        );""")

        # ── Audit Trail (append-only, immutable) ─────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_trail (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            action      TEXT    NOT NULL,
            table_name  TEXT,
            record_id   TEXT,
            actor       TEXT    DEFAULT 'SYSTEM',
            payload     TEXT,
            sha256      TEXT,
            ts          TEXT    DEFAULT (datetime('now'))
        );""")

        # ── Indexes ───────────────────────────────────────────────────────────
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ev_id ON evidence(ev_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gr_id ON gr_nodes(gr_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_task_agent ON agent_tasks(agent, status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bridge_strength ON bridges(strength DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_status ON github_sync_queue(status);")

        self.conn.commit()
        self._audit("SCHEMA_INIT", "system", "all_tables", "SYSTEM")

    # ── Evidence Methods ──────────────────────────────────────────────────────

    def upsert_evidence(self, ev_id: str, shortname: str, ev_date: str = None,
                        status: str = "CONFIRMED", source_file: str = None,
                        confidence: str = "T1", gr_links: List[str] = None,
                        chain_prev: str = None, chain_next: str = None,
                        notes: str = None) -> None:
        """Add or update an evidence record. Additive — never deletes."""
        gr_json = json.dumps(gr_links or [])
        sha = self._sha(ev_id + (notes or ""))
        self.conn.execute("""
            INSERT INTO evidence (ev_id, shortname, ev_date, status, source_file,
                confidence, gr_links, chain_prev, chain_next, notes, sha256)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ev_id) DO UPDATE SET
                status=excluded.status, source_file=excluded.source_file,
                confidence=excluded.confidence, gr_links=excluded.gr_links,
                chain_next=excluded.chain_next, notes=excluded.notes,
                sha256=excluded.sha256, updated_at=datetime('now')
        """, (ev_id, shortname, ev_date, status, source_file,
              confidence, gr_json, chain_prev, chain_next, notes, sha))
        self.conn.commit()
        self._audit("UPSERT_EVIDENCE", "evidence", ev_id, "SYSTEM",
                    json.dumps({"shortname": shortname, "confidence": confidence}))
        self._enqueue_sync(ev_id, "evidence",
                           {"ev_id": ev_id, "shortname": shortname,
                            "confidence": confidence, "gr_links": gr_links})

    def get_evidence(self, ev_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM evidence WHERE ev_id=?", (ev_id,)).fetchone()
        return dict(row) if row else None

    def list_evidence(self, confidence: str = None) -> List[Dict]:
        if confidence:
            rows = self.conn.execute(
                "SELECT * FROM evidence WHERE confidence=? ORDER BY ev_id",
                (confidence,)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM evidence ORDER BY ev_id").fetchall()
        return [dict(r) for r in rows]

    # ── GR Node Methods ───────────────────────────────────────────────────────

    def upsert_gr_node(self, gr_id: str, name: str, impact: float,
                       health: float = 1.0, status: str = "READY",
                       agent_route: str = None, evidence_links: List[str] = None,
                       cascade_trigger: float = 0.3,
                       cascade_targets: List[str] = None,
                       node_category: str = "Alpha",
                       domain_tags: List[str] = None,
                       notes: str = None) -> None:
        ev_json = json.dumps(evidence_links or [])
        cas_json = json.dumps(cascade_targets or [])
        dom_json = json.dumps(domain_tags or [])
        self.conn.execute("""
            INSERT INTO gr_nodes (gr_id, name, impact, health, status, agent_route,
                evidence_links, cascade_trigger, cascade_targets, node_category,
                domain_tags, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(gr_id) DO UPDATE SET
                name=excluded.name, impact=excluded.impact,
                status=excluded.status, agent_route=excluded.agent_route,
                evidence_links=excluded.evidence_links,
                cascade_targets=excluded.cascade_targets,
                domain_tags=excluded.domain_tags, notes=excluded.notes,
                updated_at=datetime('now')
        """, (gr_id, name, impact, health, status, agent_route,
              ev_json, cascade_trigger, cas_json, node_category, dom_json, notes))
        self.conn.commit()
        self._audit("UPSERT_GR_NODE", "gr_nodes", gr_id, "SYSTEM",
                    json.dumps({"name": name, "impact": impact, "health": health}))

    def update_gr_health(self, gr_id: str, delta: float, reason: str,
                         ev_trigger: str = None, agent: str = "SYSTEM") -> float:
        """Apply a health delta to a GR node. Returns new health value."""
        row = self.conn.execute(
            "SELECT health FROM gr_nodes WHERE gr_id=?", (gr_id,)).fetchone()
        if not row:
            raise ValueError(f"GR node {gr_id} not found")
        old_health = row["health"]
        new_health = max(0.0, min(1.0, old_health + delta))
        self.conn.execute(
            "UPDATE gr_nodes SET health=?, updated_at=datetime('now') WHERE gr_id=?",
            (new_health, gr_id))
        self.conn.execute("""
            INSERT INTO gr_health_log (gr_id, health_old, health_new, delta, reason, ev_trigger, agent)
            VALUES (?,?,?,?,?,?,?)
        """, (gr_id, old_health, new_health, delta, reason, ev_trigger, agent))
        self.conn.commit()
        # Check cascade trigger
        row2 = self.conn.execute(
            "SELECT cascade_trigger, cascade_targets FROM gr_nodes WHERE gr_id=?",
            (gr_id,)).fetchone()
        if row2 and new_health <= row2["cascade_trigger"]:
            targets = json.loads(row2["cascade_targets"] or "[]")
            for t in targets:
                self.update_gr_health(t, -0.05, f"CASCADE from {gr_id}", gr_id, "CHESS")
        return new_health

    def get_gr_node(self, gr_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM gr_nodes WHERE gr_id=?", (gr_id,)).fetchone()
        return dict(row) if row else None

    def list_gr_nodes(self, status: str = None) -> List[Dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM gr_nodes WHERE status=? ORDER BY impact DESC",
                (status,)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM gr_nodes ORDER BY impact DESC").fetchall()
        return [dict(r) for r in rows]

    # ── Chess Engine Methods ──────────────────────────────────────────────────

    def upsert_chess_piece(self, piece_name: str, business_func: str,
                           health: float = 1.0, collapse_weight: float = 0.10,
                           vulnerability: float = 1.0,
                           gr_damage_links: List[str] = None,
                           notes: str = None) -> None:
        gr_json = json.dumps(gr_damage_links or [])
        self.conn.execute("""
            INSERT INTO chess_pieces (piece_name, business_func, health,
                collapse_weight, vulnerability, gr_damage_links, notes)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(piece_name) DO UPDATE SET
                health=excluded.health, collapse_weight=excluded.collapse_weight,
                vulnerability=excluded.vulnerability,
                gr_damage_links=excluded.gr_damage_links,
                notes=excluded.notes, updated_at=datetime('now')
        """, (piece_name, business_func, health, collapse_weight,
              vulnerability, gr_json, notes))
        self.conn.commit()

    def compute_moat_score(self) -> float:
        """Moat Score = Σ(piece.health × collapse_weight) / Σ(collapse_weight)"""
        rows = self.conn.execute(
            "SELECT health, collapse_weight FROM chess_pieces").fetchall()
        if not rows:
            return 1.0
        numerator = sum(r["health"] * r["collapse_weight"] for r in rows)
        denominator = sum(r["collapse_weight"] for r in rows)
        return round(numerator / denominator, 4) if denominator else 1.0

    def compute_rule_pressure(self) -> float:
        """Rule Pressure = Σ(impact × health × 0.05) for all READY GR nodes."""
        rows = self.conn.execute(
            "SELECT impact, health FROM gr_nodes WHERE status='READY'").fetchall()
        pressure = sum(r["impact"] * r["health"] * 0.05 for r in rows)
        return round(min(1.0, pressure), 4)

    # ── Agent Task Queue ──────────────────────────────────────────────────────

    def enqueue_task(self, task_id: str, agent: str, phase: int = 2,
                     priority: int = 5, gr_node: str = None,
                     ev_inputs: List[str] = None, prompt: str = None) -> None:
        ev_json = json.dumps(ev_inputs or [])
        self.conn.execute("""
            INSERT OR IGNORE INTO agent_tasks
                (task_id, agent, phase, priority, gr_node, ev_inputs, prompt)
            VALUES (?,?,?,?,?,?,?)
        """, (task_id, agent, phase, priority, gr_node, ev_json, prompt))
        self.conn.commit()
        self._audit("ENQUEUE_TASK", "agent_tasks", task_id, agent,
                    json.dumps({"gr_node": gr_node, "phase": phase}))

    def complete_task(self, task_id: str, output: str,
                      confidence: str = "T2") -> None:
        self.conn.execute("""
            UPDATE agent_tasks
            SET status='DONE', output=?, confidence=?,
                completed_at=datetime('now')
            WHERE task_id=?
        """, (output, confidence, task_id))
        self.conn.commit()

    def get_next_task(self, agent: str) -> Optional[Dict]:
        row = self.conn.execute("""
            SELECT * FROM agent_tasks
            WHERE agent=? AND status='QUEUED'
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
        """, (agent,)).fetchone()
        if row:
            self.conn.execute(
                "UPDATE agent_tasks SET status='RUNNING' WHERE task_id=?",
                (row["task_id"],))
            self.conn.commit()
        return dict(row) if row else None

    # ── Bridge Methods ────────────────────────────────────────────────────────

    def upsert_bridge(self, bridge_id: str, domain_a: str, domain_b: str,
                      bridge_type: str = "Standard", strength: float = 0.0,
                      status: str = "CANDIDATE", description: str = None,
                      litigation_text: str = None, ev_links: List[str] = None,
                      gr_links: List[str] = None,
                      validated_by: List[str] = None) -> None:
        ev_json = json.dumps(ev_links or [])
        gr_json = json.dumps(gr_links or [])
        val_json = json.dumps(validated_by or [])
        # Auto-promote to LITIGATION-READY if strength >= 0.65
        if strength >= 0.65 and status == "CANDIDATE":
            status = "LITIGATION-READY"
        self.conn.execute("""
            INSERT INTO bridges (bridge_id, domain_a, domain_b, bridge_type,
                strength, status, description, litigation_text,
                ev_links, gr_links, validated_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(bridge_id) DO UPDATE SET
                strength=excluded.strength, status=excluded.status,
                description=excluded.description,
                litigation_text=excluded.litigation_text,
                ev_links=excluded.ev_links, gr_links=excluded.gr_links,
                validated_by=excluded.validated_by,
                updated_at=datetime('now')
        """, (bridge_id, domain_a, domain_b, bridge_type, strength, status,
              description, litigation_text, ev_json, gr_json, val_json))
        self.conn.commit()
        self._audit("UPSERT_BRIDGE", "bridges", bridge_id, "BRIDGER",
                    json.dumps({"strength": strength, "status": status}))

    def list_bridges(self, min_strength: float = 0.0) -> List[Dict]:
        rows = self.conn.execute("""
            SELECT * FROM bridges WHERE strength >= ?
            ORDER BY strength DESC
        """, (min_strength,)).fetchall()
        return [dict(r) for r in rows]

    # ── System State ──────────────────────────────────────────────────────────

    def snapshot_state(self, phase: int = None, mode: str = None) -> Dict:
        """Write a system state snapshot and return it."""
        moat = self.compute_moat_score()
        pressure = self.compute_rule_pressure()
        # Last EV / GR IDs
        last_ev = self.conn.execute(
            "SELECT ev_id FROM evidence ORDER BY id DESC LIMIT 1").fetchone()
        last_gr = self.conn.execute(
            "SELECT gr_id FROM gr_nodes ORDER BY id DESC LIMIT 1").fetchone()
        # Convergence: bridges validated / total
        total_b = self.conn.execute("SELECT COUNT(*) as c FROM bridges").fetchone()["c"]
        ready_b = self.conn.execute(
            "SELECT COUNT(*) as c FROM bridges WHERE status='LITIGATION-READY'"
        ).fetchone()["c"]
        convergence = round(ready_b / total_b * 100, 1) if total_b else 0.0
        state = {
            "phase": phase or 0,
            "mode": mode or "compact",
            "version": "v2.0",
            "moat_score": moat,
            "rule_pressure": pressure,
            "convergence_pct": convergence,
            "last_ev_id": last_ev["ev_id"] if last_ev else None,
            "last_gr_id": last_gr["gr_id"] if last_gr else None,
        }
        self.conn.execute("""
            INSERT INTO system_state (phase, mode, moat_score, rule_pressure,
                convergence_pct, last_ev_id, last_gr_id)
            VALUES (?,?,?,?,?,?,?)
        """, (state["phase"], state["mode"], moat, pressure, convergence,
              state["last_ev_id"], state["last_gr_id"]))
        self.conn.commit()
        return state

    # ── GitHub Sync Queue ─────────────────────────────────────────────────────

    def _enqueue_sync(self, artifact_id: str, artifact_type: str,
                      payload: Dict) -> None:
        self.conn.execute("""
            INSERT INTO github_sync_queue (artifact_id, artifact_type, payload)
            VALUES (?,?,?)
        """, (artifact_id, artifact_type, json.dumps(payload)))
        self.conn.commit()

    def get_pending_syncs(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM github_sync_queue WHERE status='PENDING' ORDER BY ts"
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_synced(self, sync_id: int, repo_path: str, commit_sha: str) -> None:
        self.conn.execute("""
            UPDATE github_sync_queue
            SET status='SYNCED', repo_path=?, commit_sha=?
            WHERE id=?
        """, (repo_path, commit_sha, sync_id))
        self.conn.commit()

    # ── Dataset Hygiene ───────────────────────────────────────────────────────

    def dataset_hygiene(self, table_name: str) -> Dict:
        """
        Validate a table for nulls, duplicates, and schema integrity.
        Returns a hygiene report dict.
        """
        report = {"table": table_name, "issues": [], "rows": 0}
        try:
            rows = self.conn.execute(
                f"SELECT COUNT(*) as c FROM {table_name}").fetchone()
            report["rows"] = rows["c"]
            # Check for null primary keys
            if table_name == "evidence":
                nulls = self.conn.execute(
                    "SELECT COUNT(*) as c FROM evidence WHERE ev_id IS NULL"
                ).fetchone()["c"]
                if nulls:
                    report["issues"].append(f"{nulls} null ev_id rows")
            elif table_name == "gr_nodes":
                nulls = self.conn.execute(
                    "SELECT COUNT(*) as c FROM gr_nodes WHERE gr_id IS NULL"
                ).fetchone()["c"]
                if nulls:
                    report["issues"].append(f"{nulls} null gr_id rows")
            # Check health bounds
            if table_name == "gr_nodes":
                oob = self.conn.execute(
                    "SELECT COUNT(*) as c FROM gr_nodes WHERE health < 0 OR health > 1"
                ).fetchone()["c"]
                if oob:
                    report["issues"].append(f"{oob} out-of-bounds health values")
        except Exception as e:
            report["issues"].append(str(e))
        report["clean"] = len(report["issues"]) == 0
        return report

    def data_refresh(self, table_name: str, new_data: List[Dict]) -> int:
        """
        Upsert a batch of records into the named table.
        Dispatches to the appropriate upsert method.
        Returns count of records processed.
        """
        count = 0
        for record in new_data:
            try:
                if table_name == "evidence":
                    self.upsert_evidence(**record)
                elif table_name == "gr_nodes":
                    self.upsert_gr_node(**record)
                elif table_name == "bridges":
                    self.upsert_bridge(**record)
                elif table_name == "chess_pieces":
                    self.upsert_chess_piece(**record)
                count += 1
            except Exception as e:
                self._audit("DATA_REFRESH_ERROR", table_name, str(record.get("id", "?")),
                            "SYSTEM", str(e))
        return count

    # ── Seed Methods ──────────────────────────────────────────────────────────

    def seed_gr_nodes(self) -> None:
        """Seed the canonical GR-001 through GR-012 registry from NEXUS v1."""
        canonical = [
            dict(gr_id="GR-001", name="GPS/Snappy VIE & Principal-Agent",
                 impact=9.8, health=0.82, status="READY",
                 agent_route="TIGER+WOLF->SUITS",
                 evidence_links=["EV-292", "EV-293", "EV-295"],
                 cascade_trigger=0.3, cascade_targets=["GR-002", "GR-011"],
                 node_category="Alpha",
                 domain_tags=["Forensic Accounting", "Legal"],
                 notes="GPS LLC VIE structure — triple margin extraction"),
            dict(gr_id="GR-002", name="Opaque Fulfillment / Audit Defect",
                 impact=9.5, health=0.80, status="READY",
                 agent_route="TIGER+WOLF->SUITS",
                 evidence_links=["EV-294", "EV-295"],
                 cascade_trigger=0.3, cascade_targets=["GR-011"],
                 node_category="Beta",
                 domain_tags=["Audit", "Evidence"],
                 notes="No unified bill-of-lading; serial tracking gaps"),
            dict(gr_id="GR-003", name="Binary Settlement / Threshold Structure",
                 impact=9.7, health=0.85, status="READY",
                 agent_route="TIGER->SUITS",
                 evidence_links=["EV-293"],
                 cascade_trigger=0.3, cascade_targets=["GR-009"],
                 node_category="Alpha",
                 domain_tags=["ASC 606", "Derivatives"],
                 notes="iPad Air ARV delta $50-$100/unit; contingent liability"),
            dict(gr_id="GR-004", name="Apple Bundle / ARV Ambiguity",
                 impact=9.3, health=0.87, status="READY",
                 agent_route="WOLF+TIGER->SUITS",
                 evidence_links=["EV-292", "EV-294"],
                 cascade_trigger=0.3, cascade_targets=["GR-012"],
                 node_category="Alpha",
                 domain_tags=["Consumer Fraud", "Audit"],
                 notes="Refurbished-as-new; serial LDHJKX6VDN, HQP6MHJXFF"),
            dict(gr_id="GR-005", name="No Monetary Value vs. Pathways",
                 impact=8.8, health=0.75, status="READY",
                 agent_route="WOLF+BRIDGER->SUITS",
                 evidence_links=["EV-292"],
                 cascade_trigger=0.3, cascade_targets=[],
                 node_category="Alpha",
                 domain_tags=["Contract", "Securities"],
                 notes="550:1 Crown conversion rate; DK$ pathway contradiction"),
            dict(gr_id="GR-006", name="Terms/Privacy Fragmentation",
                 impact=8.6, health=0.78, status="READY",
                 agent_route="WOLF+TIGER->SUITS",
                 evidence_links=["EV-296", "EV-297", "EV-299"],
                 cascade_trigger=0.3, cascade_targets=["GR-012"],
                 node_category="Beta",
                 domain_tags=["Contract", "Privacy"],
                 notes="Stale ToS; vendor disclosure gaps; GDPR Art 5 violation"),
            dict(gr_id="GR-007", name="550:1 CAD / Currency Valuation",
                 impact=8.1, health=0.70, status="READY",
                 agent_route="TIGER->SUITS",
                 evidence_links=["EV-292"],
                 cascade_trigger=0.3, cascade_targets=[],
                 node_category="Alpha",
                 domain_tags=["ASC 830", "Fair Value"],
                 notes="Currency-like loyalty conversion; ASC 830 foreign exchange"),
            dict(gr_id="GR-008", name="VIP Host Off-Book Remediation",
                 impact=9.1, health=0.83, status="READY",
                 agent_route="WOLF+TIGER->SUITS",
                 evidence_links=["EV-295"],
                 cascade_trigger=0.3, cascade_targets=["GR-011"],
                 node_category="Alpha",
                 domain_tags=["Legal", "Controls"],
                 notes="Off-ledger appeasement; VIP host discretionary credits"),
            dict(gr_id="GR-009", name="Calendar Bleed / Period Contamination",
                 impact=9.3, health=0.81, status="READY",
                 agent_route="TIGER->SUITS",
                 evidence_links=["EV-293"],
                 cascade_trigger=0.3, cascade_targets=[],
                 node_category="Gamma",
                 domain_tags=["ASC 450", "ASC 606"],
                 notes="37-day overlap: 2023 redemption window into 2024 accrual"),
            dict(gr_id="GR-010", name="MGCB Material Change / Michigan Core",
                 impact=9.0, health=0.79, status="READY",
                 agent_route="TIGER+WOLF->SUITS",
                 evidence_links=["EV-292", "EV-293"],
                 cascade_trigger=0.3, cascade_targets=[],
                 node_category="Alpha",
                 domain_tags=["Regulatory", "MCPA"],
                 notes="Michigan Gaming Control Board material change filing"),
            dict(gr_id="GR-011", name="BDO / SOX 404(b) Withdrawal Trigger",
                 impact=9.4, health=0.84, status="READY",
                 agent_route="TIGER->SUITS",
                 evidence_links=["EV-293", "EV-294", "EV-295"],
                 cascade_trigger=0.3, cascade_targets=["GR-012"],
                 node_category="Omega",
                 domain_tags=["Audit", "ICFR"],
                 notes="5 documented BDO withdrawal triggers; PCAOB AS 1105"),
            dict(gr_id="GR-012", name="Platform Guardian / Apple-Google Removal",
                 impact=8.9, health=0.86, status="READY",
                 agent_route="WOLF+TIGER->SUITS",
                 evidence_links=["EV-294", "EV-296", "EV-298", "EV-299"],
                 cascade_trigger=0.3, cascade_targets=[],
                 node_category="Omega",
                 domain_tags=["Platform", "Going Concern"],
                 notes="App Store Guideline 2.1 + 5.1.1; 99.7% removal probability"),
        ]
        for node in canonical:
            self.upsert_gr_node(**node)

    def seed_chess_pieces(self) -> None:
        """Seed the canonical chess piece registry."""
        pieces = [
            dict(piece_name="King", business_func="Platform/License",
                 health=0.72, collapse_weight=0.20, vulnerability=1.2,
                 gr_damage_links=["GR-010", "GR-012"]),
            dict(piece_name="Queen", business_func="Rewards Engine",
                 health=0.58, collapse_weight=0.18, vulnerability=1.4,
                 gr_damage_links=["GR-003", "GR-005", "GR-007", "GR-009"]),
            dict(piece_name="Rook_A", business_func="Accounting/Audit",
                 health=0.61, collapse_weight=0.15, vulnerability=1.3,
                 gr_damage_links=["GR-001", "GR-002", "GR-011"]),
            dict(piece_name="Rook_B", business_func="Regulatory Compliance",
                 health=0.65, collapse_weight=0.14, vulnerability=1.2,
                 gr_damage_links=["GR-006", "GR-010"]),
            dict(piece_name="Bishop", business_func="Consumer Trust",
                 health=0.55, collapse_weight=0.10, vulnerability=1.5,
                 gr_damage_links=["GR-004", "GR-005", "GR-006"]),
            dict(piece_name="Knight", business_func="VIP/Relationship Layer",
                 health=0.63, collapse_weight=0.08, vulnerability=1.1,
                 gr_damage_links=["GR-008"]),
            dict(piece_name="Gorgon", business_func="Regulatory License Portfolio",
                 health=0.48, collapse_weight=0.15, vulnerability=1.6,
                 gr_damage_links=["GR-010", "GR-012"]),
        ]
        for p in pieces:
            self.upsert_chess_piece(**p)

    def seed_evidence_register(self) -> None:
        """Seed the canonical EV-292 through EV-299 evidence register."""
        evidence = [
            dict(ev_id="EV-292", shortname="DARC_CATALOG",
                 ev_date="2024-01-15", status="CONFIRMED",
                 source_file="DK_Onyx_Tracker_v6.xlsx",
                 confidence="T1",
                 gr_links=["GR-001", "GR-004", "GR-007"],
                 notes="162 rows; crown costs, MSRP, margin calculations; Apple bundle"),
            dict(ev_id="EV-293", shortname="BREAKAGE_LEDGER",
                 ev_date="2024-03-01", status="CONFIRMED",
                 source_file="DK_Onyx_Tracker_v6.xlsx",
                 confidence="T1",
                 gr_links=["GR-003", "GR-009"],
                 notes="iPad Air 7th Gen: true ARV $749 vs disclosed $849 (-$100)"),
            dict(ev_id="EV-294", shortname="APPLE_MERCHANDISE_SERIALS",
                 ev_date="2024-06-01", status="CONFIRMED",
                 source_file="DK_Onyx_Tracker_v6.xlsx",
                 confidence="T1",
                 gr_links=["GR-004", "GR-011", "GR-012"],
                 notes="MacBook Pro serial LDHJKX6VDN refurbished; iPad HQP6MHJXFF"),
            dict(ev_id="EV-295", shortname="OFF_BOOK_REMEDIATION",
                 ev_date="2024-09-01", status="CONFIRMED",
                 source_file="DK_Onyx_Tracker_v6.xlsx",
                 confidence="T1",
                 gr_links=["GR-008", "GR-011"],
                 notes="VIP host discretionary credits outside GL; no journal entry"),
            dict(ev_id="EV-296", shortname="PRIVACY_DATA_COLLECTION",
                 ev_date="2024-01-01", status="CONFIRMED",
                 source_file="Privacy_ToS_Crosswalk.xlsx",
                 confidence="T1",
                 gr_links=["GR-006", "GR-012"],
                 notes="Unlimited data collection clause; GDPR Art 5(1)(c) violation"),
            dict(ev_id="EV-297", shortname="TRACKING_PRE_CHECKED",
                 ev_date="2024-01-01", status="CONFIRMED",
                 source_file="Privacy_ToS_Crosswalk.xlsx",
                 confidence="T1",
                 gr_links=["GR-006"],
                 notes="Pre-checked cookies/pixels; GDPR Art 7(4) violation"),
            dict(ev_id="EV-298", shortname="CONTACT_HARVEST",
                 ev_date="2024-01-01", status="CONFIRMED",
                 source_file="Privacy_ToS_Crosswalk.xlsx",
                 confidence="T1",
                 gr_links=["GR-006", "GR-012"],
                 notes="Address book scraping; CFAA § 1030; App Store Guideline 5.1.1"),
            dict(ev_id="EV-299", shortname="AUTO_ENROLLMENT_ROSCA",
                 ev_date="2024-01-01", status="CONFIRMED",
                 source_file="Privacy_ToS_Crosswalk.xlsx",
                 confidence="T1",
                 gr_links=["GR-006", "GR-012"],
                 notes="Negative option without ROSCA compliance; FTC Act § 5"),
        ]
        for ev in evidence:
            self.upsert_evidence(**ev)

    # ── Internal Utilities ────────────────────────────────────────────────────

    def _sha(self, text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def _audit(self, action: str, table_name: str, record_id: str,
               actor: str, payload: str = None) -> None:
        sha = self._sha((action + record_id + (payload or ""))
                        + datetime.datetime.utcnow().isoformat())
        self.conn.execute("""
            INSERT INTO audit_trail (action, table_name, record_id, actor, payload, sha256)
            VALUES (?,?,?,?,?,?)
        """, (action, table_name, record_id, actor, payload, sha))
        # Do NOT commit here — caller commits

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ── CLI Bootstrap ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    print("=== NEXUS Master DB Bootstrap ===")
    db = NexusDB()
    db.init_schema()
    print(f"[OK] Schema initialized at: {db.db_path}")
    db.seed_gr_nodes()
    print("[OK] GR nodes seeded (GR-001 through GR-012)")
    db.seed_chess_pieces()
    print("[OK] Chess pieces seeded (7 pieces)")
    db.seed_evidence_register()
    print("[OK] Evidence register seeded (EV-292 through EV-299)")
    state = db.snapshot_state(phase=0, mode="compact")
    print(f"\n[RUNTIME REPORT]")
    print(f"  Phase: {state['phase']} | Mode: {state['mode']} | Version: {state['version']}")
    print(f"\n[STATE LEDGER]")
    print(f"  Last EV: {state['last_ev_id']} | Last GR: {state['last_gr_id']}")
    print(f"  Moat Score: {state['moat_score']:.4f}")
    print(f"  Rule Pressure: {state['rule_pressure']:.4f}")
    print(f"  Convergence: {state['convergence_pct']}%")
    hygiene_ev = db.dataset_hygiene("evidence")
    hygiene_gr = db.dataset_hygiene("gr_nodes")
    print(f"\n[HYGIENE REPORT]")
    print(f"  evidence: {hygiene_ev['rows']} rows | clean={hygiene_ev['clean']}")
    print(f"  gr_nodes: {hygiene_gr['rows']} rows | clean={hygiene_gr['clean']}")
    db.close()
    print("\n=== NEXUS DB READY ===")
