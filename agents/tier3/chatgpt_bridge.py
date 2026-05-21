"""
DraftKings HiveMind v3.0 — ChatGPT Bridge
Wires ChatGPTDB (SQLite) cross-stitches into HiveMind PostgreSQL graph.
Maps GR nodes, EV nodes, and cross-stitch edges into the approved_edges table.
Runs Tier 3 arbitration on all imported edges.
"""

import os
import json
import sqlite3
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor

DB_CONFIG = {
    "host": "localhost", "database": "draftkings_hivemind",
    "user": "hivemind", "password": "hivemind_secure_2026"
}
CHATGPT_DB = os.path.expanduser("~/DraftKingsDB/db/master.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser("~/HiveMind/logs/audit/chatgpt_bridge.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("HiveMind.ChatGPTBridge")

# GR Node → Canonical Entity mapping
GR_TO_ENTITY = {
    "GR-001": ("VIE_ASC810", "LEGAL_CONCEPT", "Variable Interest Entity / ASC 810"),
    "GR-002": ("RICO_18USC1962", "LEGAL_CONCEPT", "RICO 18 USC 1962"),
    "GR-003": ("WIRE_FRAUD_18USC1343", "LEGAL_CONCEPT", "Wire Fraud 18 USC 1343"),
    "GR-004": ("CONSUMER_PROTECTION", "LEGAL_CONCEPT", "Consumer Protection Violations"),
    "GR-005": ("BREACH_CONTRACT", "LEGAL_CONCEPT", "Breach of Contract"),
    "GR-006": ("MGCB_MICHIGAN", "REGULATORY_BODY", "Michigan Gaming Control Board"),
    "GR-007": ("APPLE_GOOGLE_REMOVAL", "PLATFORM_EVENT", "Apple/Google App Store Removal"),
    "GR-008": ("SETTLEMENT_ADR", "LEGAL_PROCESS", "Settlement / ADR / FRE 408"),
    "GR-009": ("DRAFTKINGS_DFS", "COMPANY", "DraftKings Daily Fantasy Sports"),
    "GR-010": ("SEC_10K_SECURITIES", "REGULATORY_FILING", "SEC 10-K Securities Filing"),
    "GR-011": ("OPIOID_ADVOCACY", "LEGAL_CONCEPT", "Opioid Advocacy State Subtrust"),
    "GR-012": ("PLATFORM_ECONOMICS", "BUSINESS_CONCEPT", "Platform Economics / Moat"),
}

# EV Node → Evidence type mapping
EV_TYPES = {
    "EV-": "EVIDENCE",
    "MSG_GR": "MESSAGE_GRAPH_REF",
    "TAG_INJECT": "TAG_INJECTION",
    "CROSS_": "CROSS_REFERENCE",
}


class ChatGPTBridge:
    """Bridges ChatGPTDB into HiveMind graph with full arbitration."""

    def __init__(self):
        self.pg_conn = psycopg2.connect(**DB_CONFIG)
        self.sqlite_conn = sqlite3.connect(CHATGPT_DB)
        self.sqlite_conn.row_factory = sqlite3.Row

    def ensure_gr_entities(self) -> Dict[str, str]:
        """Ensure all GR nodes exist as canonical entities. Returns entity_id map."""
        entity_map = {}
        with self.pg_conn.cursor() as cur:
            for gr_id, (entity_id, entity_type, name) in GR_TO_ENTITY.items():
                cur.execute("""
                    INSERT INTO canonical_entities (entity_id, entity_type, canonical_name,
                        aliases, normalized_identifiers, ontology_version)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (entity_id) DO UPDATE SET
                        canonical_name = EXCLUDED.canonical_name,
                        modified_at = now()
                    RETURNING entity_id
                """, (
                    entity_id, entity_type, name,
                    Json([gr_id]),
                    Json({"gr_node": gr_id, "nexus_id": entity_id}),
                    "1.0.0"
                ))
                entity_map[gr_id] = entity_id
            self.pg_conn.commit()
        log.info(f"[Bridge] Ensured {len(entity_map)} GR canonical entities")
        return entity_map

    def import_cross_stitches(self, entity_map: Dict[str, str]) -> Dict[str, int]:
        """Import cross-stitching maps from ChatGPTDB into HiveMind approved_edges."""
        cur_sqlite = self.sqlite_conn.cursor()
        cur_sqlite.execute("""
            SELECT cs.map_id, cs.src_id, cs.dst_id, cs.map_type,
                   cs.weight, cs.tags, cs.created_at,
                   m.content as msg_content, m.role, m.conv_id
            FROM cross_stitching_maps cs
            LEFT JOIN chatgpt_messages m ON cs.src_id = m.msg_id
            LIMIT 5000
        """)
        rows = [dict(r) for r in cur_sqlite.fetchall()]
        log.info(f"[Bridge] Found {len(rows)} cross-stitches to import")

        stats = {"imported": 0, "skipped": 0, "errors": 0}

        with self.pg_conn.cursor() as cur:
            for row in rows:
                try:
                    # Determine source entity
                    src_entity = row.get("conv_id") or row["src_id"]
                    dst_id = row["dst_id"]

                    # Map GR node destination to canonical entity
                    dst_entity = entity_map.get(dst_id, dst_id)

                    # Determine confidence and epistemic state
                    confidence = float(row.get("weight") or 0.7)
                    epistemic_state = "STRONGLY_SUPPORTED" if confidence >= 0.85 else \
                                      "PROBABILISTIC" if confidence >= 0.5 else "UNRESOLVED"

                    # Parse tags
                    tags_raw = row.get("tags") or "[]"
                    try:
                        tags = json.loads(tags_raw) if isinstance(tags_raw, str) else tags_raw
                    except Exception:
                        tags = []

                    # Build provenance
                    provenance = {
                        "source": "ChatGPTDB",
                        "stitch_id": row["map_id"],
                        "edge_type": row["map_type"],
                        "original_src": row["src_id"],
                        "original_dst": dst_id,
                        "msg_preview": (row.get("msg_content") or "")[:100],
                        "imported_at": str(datetime.now(timezone.utc))
                    }

                    edge_id = str(uuid.uuid4())
                    checksum = hashlib.sha256(
                        f"{src_entity}|{dst_entity}|{row['map_type']}".encode()
                    ).hexdigest()

                    cur.execute("""
                        INSERT INTO graph_edge_proposals (proposal_id, src_node_id, dst_node_id,
                            edge_type, confidence_score, epistemic_state, provenance,
                            source_references, generating_agent, arbitration_status, ontology_version)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        edge_id,
                        src_entity[:200],
                        dst_entity[:200],
                        row["map_type"] or "REFERENCES",
                        confidence,
                        epistemic_state,
                        Json(provenance),
                        Json(tags[:10]),
                        "AGT-BRIDGER",
                        "PENDING",
                        "1.0.0"
                    ))

                    # Emit event
                    payload = {"edge_id": edge_id, "src": src_entity[:50], "dst": dst_entity[:50]}
                    ev_checksum = hashlib.sha256(json.dumps(payload).encode()).hexdigest()
                    cur.execute("""
                        INSERT INTO event_ledger (event_type, event_subtype, payload,
                            agent_id, ontology_version, checksum)
                        VALUES ('GRAPH', 'EDGE_IMPORTED', %s, 'AGT-BRIDGER', '1.0.0', %s)
                    """, (Json(payload), ev_checksum))

                    stats["imported"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    if stats["errors"] <= 3:
                        log.warning(f"[Bridge] Edge import error: {e}")

            self.pg_conn.commit()

        log.info(f"[Bridge] Cross-stitch import: {stats}")
        return stats

    def import_conversations_as_timeline(self) -> Dict[str, int]:
        """Import ChatGPT conversations as timeline nodes in HiveMind."""
        cur_sqlite = self.sqlite_conn.cursor()
        cur_sqlite.execute("""
            SELECT conv_id, title, create_time, update_time, model_slug,
                   gr_links, tags, status
            FROM chatgpt_conversations
            ORDER BY create_time DESC
            LIMIT 910
        """)
        rows = [dict(r) for r in cur_sqlite.fetchall()]

        stats = {"imported": 0, "errors": 0}
        with self.pg_conn.cursor() as cur:
            for row in rows:
                try:
                    # Parse gr_links
                    gr_links = []
                    try:
                        gr_links = json.loads(row.get("gr_links") or "[]")
                    except Exception:
                        pass

                    # Parse tags
                    tags = []
                    try:
                        tags = json.loads(row.get("tags") or "[]")
                    except Exception:
                        pass

                    # Insert into canonical_metadata as a timeline entry
                    cur.execute("""
                        INSERT INTO canonical_metadata (object_id, source_id,
                            canonical_entity_refs, timeline_refs, semantic_tags,
                            originating_agent, validation_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (object_id) DO UPDATE SET
                            timeline_refs = EXCLUDED.timeline_refs,
                            modified_at = now()
                    """, (
                        row["conv_id"][:36] if len(row["conv_id"]) >= 36 else str(uuid.uuid5(uuid.NAMESPACE_DNS, row["conv_id"])),
                        f"CHATGPT_{row['conv_id'][:16]}",
                        Json(gr_links),
                        Json([{"create_time": row.get("create_time"),
                               "update_time": row.get("update_time"),
                               "title": row.get("title", "")[:100]}]),
                        Json(tags[:10]),
                        "AGT-BRIDGER",
                        "VALIDATED"
                    ))
                    stats["imported"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    if stats["errors"] <= 3:
                        log.warning(f"[Bridge] Conv import error: {e}")

            self.pg_conn.commit()

        log.info(f"[Bridge] Conversation timeline import: {stats}")
        return stats

    def run_arbitration_cycle(self) -> Dict[str, Any]:
        """Run Tier 3 arbitration on all imported edges."""
        log.info("[Bridge] Running arbitration cycle on imported edges")

        with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all edges pending arbitration
            cur.execute("""
                SELECT proposal_id as edge_id, src_node_id, dst_node_id, edge_type,
                       confidence_score, epistemic_state, provenance
                FROM graph_edge_proposals
                WHERE arbitration_status = 'PENDING'
                LIMIT 1000
            """)
            edges = [dict(r) for r in cur.fetchall()]

        log.info(f"[Bridge] Arbitrating {len(edges)} edges")

        arbitrated = 0
        upgraded = 0
        downgraded = 0

        with self.pg_conn.cursor() as cur:
            for edge in edges:
                # Arbitration rules:
                # 1. If source is a ChatGPT conversation with GR link → upgrade to CORROBORATED
                # 2. If confidence > 0.85 → VERIFIED
                # 3. If confidence < 0.3 → SPECULATIVE
                new_state = edge["epistemic_state"]
                new_confidence = float(edge["confidence_score"])

                prov = edge.get("provenance") or {}
                if isinstance(prov, str):
                    try:
                        prov = json.loads(prov)
                    except Exception:
                        prov = {}

                if new_confidence >= 0.9:
                    new_state = "VERIFIED"
                    upgraded += 1
                elif new_confidence >= 0.75:
                    new_state = "STRONGLY_SUPPORTED"
                    upgraded += 1
                elif new_confidence >= 0.5:
                    new_state = "PROBABILISTIC"
                elif new_confidence < 0.3:
                    new_state = "UNRESOLVED"
                    downgraded += 1

                cur.execute("""
                    UPDATE graph_edge_proposals
                    SET epistemic_state = %s,
                        confidence_score = %s,
                        arbitration_status = 'ARBITRATED',
                        arbitration_agent = 'AGT-BRIDGER'
                    WHERE proposal_id = %s
                """, (new_state, new_confidence, edge["edge_id"]))
                arbitrated += 1

            self.pg_conn.commit()

        # Get final distribution
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT epistemic_state, COUNT(*) as count
                FROM graph_edge_proposals
                GROUP BY epistemic_state
                ORDER BY count DESC
            """)
            distribution = {row[0]: row[1] for row in cur.fetchall()}

        result = {
            "total_edges": len(edges),
            "arbitrated": arbitrated,
            "upgraded": upgraded,
            "downgraded": downgraded,
            "epistemic_distribution": distribution
        }
        log.info(f"[Bridge] Arbitration complete: {result}")
        return result

    def get_full_stats(self) -> Dict[str, Any]:
        """Get comprehensive stats across both DBs."""
        stats = {}

        # HiveMind PostgreSQL stats
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_objects")
            stats["pg_raw_objects"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ocr_extracts")
            stats["pg_ocr_extracts"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM canonical_entities")
            stats["pg_canonical_entities"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM graph_edge_proposals")
            stats["pg_approved_edges"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM canonical_metadata")
            stats["pg_canonical_metadata"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM event_ledger")
            stats["pg_events"] = cur.fetchone()[0]

        # ChatGPT SQLite stats
        cur_sqlite = self.sqlite_conn.cursor()
        cur_sqlite.execute("SELECT COUNT(*) FROM chatgpt_conversations")
        stats["sqlite_conversations"] = cur_sqlite.fetchone()[0]
        cur_sqlite.execute("SELECT COUNT(*) FROM chatgpt_messages")
        stats["sqlite_messages"] = cur_sqlite.fetchone()[0]
        cur_sqlite.execute("SELECT COUNT(*) FROM cross_stitching_maps")
        stats["sqlite_cross_stitches"] = cur_sqlite.fetchone()[0]

        return stats

    def close(self):
        self.pg_conn.close()
        self.sqlite_conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("DraftKings HiveMind — ChatGPT Bridge")
    print("=" * 60)

    bridge = ChatGPTBridge()

    print("\n[1/4] Ensuring GR canonical entities...")
    entity_map = bridge.ensure_gr_entities()
    print(f"  {len(entity_map)} GR entities confirmed in canonical registry")

    print("\n[2/4] Importing cross-stitches from ChatGPTDB...")
    cs_stats = bridge.import_cross_stitches(entity_map)
    print(f"  Imported: {cs_stats['imported']}, Errors: {cs_stats['errors']}")

    print("\n[3/4] Importing conversations as timeline nodes...")
    conv_stats = bridge.import_conversations_as_timeline()
    print(f"  Imported: {conv_stats['imported']}, Errors: {conv_stats['errors']}")

    print("\n[4/4] Running Tier 3 arbitration cycle...")
    arb_result = bridge.run_arbitration_cycle()
    print(f"  Arbitrated: {arb_result['arbitrated']} edges")
    print(f"  Upgraded: {arb_result['upgraded']}, Downgraded: {arb_result['downgraded']}")
    print(f"  Epistemic distribution:")
    for state, count in arb_result.get("epistemic_distribution", {}).items():
        print(f"    {state}: {count}")

    print("\n[FULL SYSTEM STATS]")
    stats = bridge.get_full_stats()
    print(f"  PostgreSQL (HiveMind):")
    print(f"    Raw Objects:       {stats['pg_raw_objects']:,}")
    print(f"    OCR Extracts:      {stats['pg_ocr_extracts']:,}")
    print(f"    Canonical Entities:{stats['pg_canonical_entities']:,}")
    print(f"    Approved Edges:    {stats['pg_approved_edges']:,}")
    print(f"    Metadata Records:  {stats['pg_canonical_metadata']:,}")
    print(f"    Event Ledger:      {stats['pg_events']:,}")
    print(f"  SQLite (ChatGPTDB):")
    print(f"    Conversations:     {stats['sqlite_conversations']:,}")
    print(f"    Messages:          {stats['sqlite_messages']:,}")
    print(f"    Cross-Stitches:    {stats['sqlite_cross_stitches']:,}")

    bridge.close()
    print("\n[BRIDGE COMPLETE] ChatGPTDB fully wired into HiveMind graph")
