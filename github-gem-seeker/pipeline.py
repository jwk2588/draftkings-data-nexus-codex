"""
/github-gem-seeker — DraftKings ChatGPT Export ETL Pipeline
=============================================================
Directive: Manus Directive v1.0, Section 3
Spine: MasterBrief_v54 (EV-001–EV-291, SB-01–SB-66)

Pipeline:
  conversations.json
        ↓
  normalize (Polars)
        ↓
  classify (DK_CORE / DK_ADJACENT / NON_DK)
        ↓
  tag (Pillar / Levee / Engine / MBv54 section refs)
        ↓
  image metadata extraction
        ↓
  Parquet output → /data-lake/chatgpt/
        ↓
  Neo4j ingestion (via tier enforcement middleware)
        ↓
  chatgpt_data_dictionary.md

Author  : Manus Directive v1.0
Version : 1.0.0
"""

import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import polars as pl

# Add repo root to path
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from nexus_governance.evidence_gates.evidence_confidence_gate import (
    DeploymentStatus, DKRelevance, NexusNode, Tier, get_gate,
)

log = logging.getLogger("github_gem_seeker")

# ---------------------------------------------------------------------------
# MBv54 Cross-Reference Patterns
# ---------------------------------------------------------------------------

# These patterns detect references to MBv54 structural elements in text.
MBV54_PATTERNS = {
    "ev_id":      re.compile(r"\bEV[-‑](\d{3})\b", re.IGNORECASE),
    "sb_id":      re.compile(r"\bSB[-‑](\d{1,2})\b", re.IGNORECASE),
    "pillar":     re.compile(r"\bPillar[-‑\s]?(\d{1,2})\b", re.IGNORECASE),
    "levee":      re.compile(r"\bLevee[-‑\s]?(\d{1,2})\b", re.IGNORECASE),
    "engine":     re.compile(r"\bEngine[-‑\s]?([1-6])\b", re.IGNORECASE),
    "detonator":  re.compile(r"\bDetonator\s*Board\b", re.IGNORECASE),
    "mb_section": re.compile(r"\bMB[-‑]?v?5[24]\b", re.IGNORECASE),
}

# DK_CORE keywords: directly about DraftKings / Dynasty / MB
DK_CORE_KEYWORDS = [
    "draftkings", "draft kings", "dynasty", "masterbrief", "master brief",
    "mbv54", "mbv52", "mb v54", "mb v52", "ev-", "sb-", "pillar",
    "levee", "detonator board", "six engine", "silver bullet",
    "calendar bleed", "asc 606", "mgcb", "cftc railbird", "autozone lane",
    "483 inflation", "dynasty 483", "dk authority", "dk tos",
]

# DK_ADJACENT keywords: legal/accounting/gaming context
DK_ADJACENT_KEYWORDS = [
    "fantasy sports", "daily fantasy", "dfs", "sports betting",
    "gaming regulation", "gambling", "revenue recognition", "asc 606",
    "deferred revenue", "loyalty program", "terms of service", "tos",
    "privacy policy", "aml", "anti-money laundering", "wire fraud",
    "rico", "racketeering", "class action", "litigation", "settlement",
    "regulatory", "compliance", "michigan", "mgcb", "cftc", "sec",
    "apple app store", "platform fee", "in-app purchase",
]


def _classify_dk_relevance(text: str) -> str:
    """Classify a message as DK_CORE, DK_ADJACENT, or NON_DK."""
    lower = text.lower()
    if any(kw in lower for kw in DK_CORE_KEYWORDS):
        return DKRelevance.DK_CORE.value
    if any(kw in lower for kw in DK_ADJACENT_KEYWORDS):
        return DKRelevance.DK_ADJACENT.value
    return DKRelevance.NON_DK.value


def _extract_mb_refs(text: str) -> dict:
    """Extract all MBv54 structural references from text."""
    refs = {}
    for key, pattern in MBV54_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            refs[key] = list(set(matches))
    return refs


def _extract_image_metadata(message: dict, thread_id: str) -> Optional[dict]:
    """Extract image metadata from a ChatGPT message if present."""
    content = message.get("content", {})
    if isinstance(content, dict):
        parts = content.get("parts", [])
    elif isinstance(content, list):
        parts = content
    else:
        return None

    for part in parts:
        if isinstance(part, dict) and part.get("content_type") == "image_asset_pointer":
            asset_id = part.get("asset_pointer", "")
            return {
                "image_id": hashlib.sha256(asset_id.encode()).hexdigest()[:16],
                "thread_id": thread_id,
                "asset_pointer": asset_id,
                "creation_date": message.get("create_time"),
                "dk_relevance": None,  # Will be set by classifier
                "description": "Image asset from ChatGPT export",
            }
    return None


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Step 1: Load & Normalize
# ---------------------------------------------------------------------------

def load_and_normalize(conversations_path: Path) -> tuple[pl.DataFrame, list[dict]]:
    """
    Parse conversations.json and emit normalized message rows.
    Returns (messages_df, image_records).
    """
    log.info("Loading conversations from: %s", conversations_path)
    with open(conversations_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict):
        # Single conversation export
        conversations = [raw]
    else:
        conversations = raw

    rows = []
    image_records = []

    for conv in conversations:
        thread_id = conv.get("id") or conv.get("conversation_id", "")
        title = conv.get("title", "")
        create_time = conv.get("create_time")
        model = conv.get("default_model_slug", "unknown")

        mapping = conv.get("mapping", {})
        for msg_id, node in mapping.items():
            msg = node.get("message")
            if not msg:
                continue

            role = msg.get("author", {}).get("role", "unknown")
            content_obj = msg.get("content", {})

            # Extract text content
            if isinstance(content_obj, dict):
                parts = content_obj.get("parts", [])
                text = " ".join(str(p) for p in parts if isinstance(p, str))
            elif isinstance(content_obj, str):
                text = content_obj
            else:
                text = ""

            if not text.strip():
                continue

            msg_create_time = msg.get("create_time") or create_time

            # Image metadata
            img = _extract_image_metadata(msg, thread_id)
            if img:
                img["dk_relevance"] = _classify_dk_relevance(text)
                image_records.append(img)

            rows.append({
                "thread_id":        thread_id,
                "message_id":       msg_id,
                "title":            title,
                "timestamp":        msg_create_time,
                "agent":            role,
                "text":             text,
                "model":            model,
                "content_hash":     _content_hash(text),
            })

    df = pl.DataFrame(rows)
    log.info("Normalized %d messages from %d conversations", len(df), len(conversations))
    return df, image_records


# ---------------------------------------------------------------------------
# Step 2: Classify
# ---------------------------------------------------------------------------

def classify(df: pl.DataFrame) -> pl.DataFrame:
    """Add dk_relevance column to each message."""
    log.info("Classifying %d messages for DK relevance...", len(df))
    relevance = [_classify_dk_relevance(t) for t in df["text"].to_list()]
    df = df.with_columns(pl.Series("dk_relevance", relevance))

    counts = df["dk_relevance"].value_counts()
    log.info("Classification results:\n%s", counts)
    return df


# ---------------------------------------------------------------------------
# Step 3: Tag (Pillar / Levee / Engine / MBv54 refs)
# ---------------------------------------------------------------------------

def tag(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add MBv54 structural reference columns:
      pillar_ids, levee_ids, engine_ids, ev_ids, sb_ids, mb_section_ref
    """
    log.info("Tagging %d messages with MBv54 structural references...", len(df))

    pillar_ids   = []
    levee_ids    = []
    engine_ids   = []
    ev_ids       = []
    sb_ids       = []
    mb_section_refs = []
    has_detonator = []

    for text in df["text"].to_list():
        refs = _extract_mb_refs(text)
        pillar_ids.append(json.dumps(refs.get("pillar", [])))
        levee_ids.append(json.dumps(refs.get("levee", [])))
        engine_ids.append(json.dumps(refs.get("engine", [])))
        ev_ids.append(json.dumps(refs.get("ev_id", [])))
        sb_ids.append(json.dumps(refs.get("sb_id", [])))
        mb_section_refs.append(json.dumps(refs.get("mb_section", [])))
        has_detonator.append(bool(refs.get("detonator")))

    df = df.with_columns([
        pl.Series("pillar_ids", pillar_ids),
        pl.Series("levee_ids", levee_ids),
        pl.Series("engine_ids", engine_ids),
        pl.Series("ev_ids", ev_ids),
        pl.Series("sb_ids", sb_ids),
        pl.Series("mb_section_ref", mb_section_refs),
        pl.Series("has_detonator_ref", has_detonator),
    ])

    dk_core_count = (df["dk_relevance"] == "DK_CORE").sum()
    tagged_count  = (df["ev_ids"] != "[]").sum()
    log.info("Tagged: %d DK_CORE messages, %d with EV-ID refs", dk_core_count, tagged_count)
    return df


# ---------------------------------------------------------------------------
# Step 4: Write Parquet outputs
# ---------------------------------------------------------------------------

def write_parquet(df: pl.DataFrame, image_records: list[dict], repo_root: Path):
    """Write normalized, classified, and tagged data to /data-lake/chatgpt/."""
    chatgpt_raw_dir  = repo_root / "data-lake" / "chatgpt" / "raw"
    chatgpt_norm_dir = repo_root / "data-lake" / "chatgpt" / "normalized"
    chatgpt_meta_dir = repo_root / "data-lake" / "chatgpt" / "metadata"

    for d in [chatgpt_raw_dir, chatgpt_norm_dir, chatgpt_meta_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Raw JSONL
    raw_path = chatgpt_raw_dir / "conversations.jsonl"
    with open(raw_path, "w") as f:
        for row in df.to_dicts():
            f.write(json.dumps(row) + "\n")
    log.info("Wrote raw JSONL: %s", raw_path)

    # Full normalized Parquet
    norm_path = chatgpt_norm_dir / "messages.parquet"
    df.write_parquet(norm_path)
    log.info("Wrote normalized Parquet: %s", norm_path)

    # DK_CORE only Parquet
    dk_core_df = df.filter(pl.col("dk_relevance") == "DK_CORE")
    dk_core_path = chatgpt_norm_dir / "messages_dk_core.parquet"
    dk_core_df.write_parquet(dk_core_path)
    log.info("Wrote DK_CORE Parquet (%d rows): %s", len(dk_core_df), dk_core_path)

    # Image metadata JSONL
    if image_records:
        img_path = chatgpt_meta_dir / "image_metadata.jsonl"
        with open(img_path, "w") as f:
            for rec in image_records:
                f.write(json.dumps(rec) + "\n")
        log.info("Wrote image metadata (%d records): %s", len(image_records), img_path)

    return {
        "raw_jsonl": str(raw_path),
        "normalized_parquet": str(norm_path),
        "dk_core_parquet": str(dk_core_path),
        "total_messages": len(df),
        "dk_core_count": len(dk_core_df),
        "dk_adjacent_count": int((df["dk_relevance"] == "DK_ADJACENT").sum()),
        "non_dk_count": int((df["dk_relevance"] == "NON_DK").sum()),
        "image_records": len(image_records),
    }


# ---------------------------------------------------------------------------
# Step 5: Generate Data Dictionary
# ---------------------------------------------------------------------------

def generate_data_dictionary(df: pl.DataFrame, repo_root: Path, stats: dict) -> Path:
    """Generate chatgpt_data_dictionary.md in /data-lake/chatgpt/metadata/."""
    meta_dir = repo_root / "data-lake" / "chatgpt" / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)
    out_path = meta_dir / "chatgpt_data_dictionary.md"

    schema_rows = "\n".join(
        f"| `{col}` | {dtype} | — |"
        for col, dtype in zip(df.columns, [str(t) for t in df.dtypes])
    )

    content = f"""# ChatGPT Export Data Dictionary
*Generated by /github-gem-seeker — Manus Directive v1.0, Section 3.4*
*Date: {datetime.now(timezone.utc).isoformat()}*
*Spine: MasterBrief_v54 (EV-001–EV-291, SB-01–SB-66)*

---

## Overview

This data dictionary describes every field in the ChatGPT normalized tables
produced by the `/github-gem-seeker` ETL pipeline. All records are classified
against the MBv54 legal spine and tagged with Pillar, Levee, Engine, and
EV/SB references where inferable from text.

## Pipeline Statistics

| Metric | Value |
|---|---|
| Total messages | {stats['total_messages']:,} |
| DK_CORE messages | {stats['dk_core_count']:,} |
| DK_ADJACENT messages | {stats['dk_adjacent_count']:,} |
| NON_DK messages | {stats['non_dk_count']:,} |
| Image records | {stats['image_records']:,} |

## DK Relevance Classification

| Label | Meaning |
|---|---|
| `DK_CORE` | Directly about DraftKings / Dynasty / MasterBrief |
| `DK_ADJACENT` | Legal / accounting / gaming context used in MB work |
| `NON_DK` | Not relevant to DraftKings work |

## Schema: messages.parquet

| Field | Type | Description |
|---|---|---|
{schema_rows}
| `dk_relevance` | String | DK_CORE / DK_ADJACENT / NON_DK |
| `pillar_ids` | JSON String | MBv54 Pillar numbers referenced in text |
| `levee_ids` | JSON String | MBv54 Levee numbers referenced in text |
| `engine_ids` | JSON String | Six-Engine IDs referenced in text |
| `ev_ids` | JSON String | EV-IDs (EV-001–EV-291) referenced in text |
| `sb_ids` | JSON String | SB-IDs (SB-01–SB-66) referenced in text |
| `mb_section_ref` | JSON String | MBv54/v52 section references in text |
| `has_detonator_ref` | Boolean | True if text references the Detonator Board |
| `content_hash` | String | SHA-256 hash of message text for dedup/integrity |

## Cross-Sectional Query Examples

The schema supports the following cross-sectional queries:

**"All ChatGPT messages that discuss ASC 606 calendar bleed in the context of DraftKings"**
```python
df.filter(
    (pl.col("dk_relevance") == "DK_CORE") &
    pl.col("text").str.contains("(?i)asc.?606|calendar.?bleed")
)
```

**"All conversation segments that shaped Engines 2, 3, 4, 5, 6"**
```python
df.filter(
    pl.col("engine_ids").str.contains(r'[2-6]')
)
```

**"All messages referencing EV-255 or SB-54"**
```python
df.filter(
    pl.col("ev_ids").str.contains("255") |
    pl.col("sb_ids").str.contains("54")
)
```

**"All Detonator Board references"**
```python
df.filter(pl.col("has_detonator_ref") == True)
```

## MBv54 Relationship Map

| ChatGPT Field | MBv54 Element | Relationship |
|---|---|---|
| `ev_ids` | EV-001–EV-291 | `REFERENCES_EVIDENCE` |
| `sb_ids` | SB-01–SB-66 | `REFERENCES_SILVER_BULLET` |
| `pillar_ids` | 25 Pillars | `REFERENCES_PILLAR` |
| `levee_ids` | 27 Levees | `REFERENCES_LEVEE` |
| `engine_ids` | Six-Engine Stack | `INFLUENCED_ENGINE` |
| `has_detonator_ref` | Detonator Board | `REFERENCES_DETONATOR_BOARD` |

## File Locations

| File | Path | Description |
|---|---|---|
| Raw JSONL | `/data-lake/chatgpt/raw/conversations.jsonl` | All messages, raw |
| Normalized Parquet | `/data-lake/chatgpt/normalized/messages.parquet` | Full normalized dataset |
| DK_CORE Parquet | `/data-lake/chatgpt/normalized/messages_dk_core.parquet` | DK_CORE only |
| Image Metadata | `/data-lake/chatgpt/metadata/image_metadata.jsonl` | Image asset records |
| Data Dictionary | `/data-lake/chatgpt/metadata/chatgpt_data_dictionary.md` | This file |

## Tier Assignment

All ChatGPT-derived nodes are assigned **T2** by default (strong inference,
qualified). They may be promoted to T1 only by a human curator with an
explicit ADR and GraphMutationEvent log entry.

NON_DK records are assigned **T3** (appendix/discovery only).
"""
    out_path.write_text(content)
    log.info("Wrote data dictionary: %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Step 6: Neo4j Ingestion (via NEXUS gate)
# ---------------------------------------------------------------------------

def ingest_to_neo4j(df: pl.DataFrame, gate) -> dict:
    """
    Simulate Neo4j ingestion with tier enforcement.
    In production: replace with actual neo4j.GraphDatabase driver calls.
    """
    log.info("Running NEXUS gate checks before Neo4j ingestion...")
    passed = 0
    rejected = 0
    mutation_events = []

    dk_core_df = df.filter(pl.col("dk_relevance") == "DK_CORE")

    for row in dk_core_df.to_dicts():
        node = NexusNode(
            node_id=f"CHATGPT-{row['message_id'][:8]}",
            label="Conversation",
            tier=Tier.T2,
            primary_source_id="conversations (2).json",
            description=row["text"][:200],
            deployment_status=DeploymentStatus.MAIN,
            dk_relevance=DKRelevance.DK_CORE,
            content_hash=row["content_hash"],
        )
        try:
            gate.run_all_checks(node, actor="chatgpt_etl", context="control_logic")
            passed += 1
            mutation_events.append({
                "event_type": "CREATE_NODE",
                "node_id": node.node_id,
                "label": node.label,
                "tier": node.tier,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": "chatgpt_etl",
                "content_hash": node.content_hash,
            })
        except Exception as e:
            log.warning("Gate REJECTED node %s: %s", node.node_id, e)
            rejected += 1

    log.info("Neo4j ingestion: %d passed, %d rejected", passed, rejected)
    return {
        "passed": passed,
        "rejected": rejected,
        "mutation_events": len(mutation_events),
    }


# ---------------------------------------------------------------------------
# Master Pipeline Runner
# ---------------------------------------------------------------------------

def run_pipeline(conversations_path: Path, repo_root: Path) -> dict:
    """Run the full github-gem-seeker pipeline end-to-end."""
    log.info("=" * 60)
    log.info("github-gem-seeker pipeline starting")
    log.info("Input: %s", conversations_path)
    log.info("=" * 60)

    # Step 1: Load & normalize
    df, image_records = load_and_normalize(conversations_path)

    # Step 2: Classify
    df = classify(df)

    # Step 3: Tag
    df = tag(df)

    # Step 4: Write Parquet
    stats = write_parquet(df, image_records, repo_root)

    # Step 5: Data dictionary
    dict_path = generate_data_dictionary(df, repo_root, stats)
    stats["data_dictionary"] = str(dict_path)

    # Step 6: Neo4j ingestion
    gate = get_gate()
    ingest_stats = ingest_to_neo4j(df, gate)
    stats["neo4j_ingestion"] = ingest_stats

    log.info("Pipeline complete: %s", json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    repo_root = REPO_ROOT
    # Default: look for conversations.json in data-lake/raw or repo root
    candidates = [
        repo_root / "data-lake" / "raw" / "conversations (2).json",
        repo_root / "data-lake" / "raw" / "conversations.json",
        repo_root / "conversations.json",
    ]
    conv_path = next((p for p in candidates if p.exists()), None)
    if not conv_path:
        # Create a synthetic test file for validation
        log.info("No conversations.json found — generating synthetic test data...")
        test_data = [
            {
                "id": "thread-001",
                "title": "DraftKings ASC 606 Calendar Bleed Analysis",
                "create_time": 1700000000,
                "default_model_slug": "gpt-4",
                "mapping": {
                    "msg-001": {
                        "message": {
                            "id": "msg-001",
                            "author": {"role": "user"},
                            "create_time": 1700000001,
                            "content": {
                                "parts": [
                                    "Let's analyze the DraftKings ASC 606 calendar bleed exposure. "
                                    "EV-010 through EV-013 are the core evidence items. "
                                    "Levee 2 is the ASC 606 recognition boundary. Engine 3 handles this."
                                ]
                            },
                        }
                    },
                    "msg-002": {
                        "message": {
                            "id": "msg-002",
                            "author": {"role": "assistant"},
                            "create_time": 1700000002,
                            "content": {
                                "parts": [
                                    "The calendar bleed issue under ASC 606 is critical for DraftKings. "
                                    "SB-20 through SB-22 document the revenue recognition cluster. "
                                    "The Detonator Board would activate if Levee 2 and Levee 3 are both breached."
                                ]
                            },
                        }
                    },
                },
            },
            {
                "id": "thread-002",
                "title": "Michigan MGCB Regulatory Analysis",
                "create_time": 1700001000,
                "default_model_slug": "gpt-4",
                "mapping": {
                    "msg-003": {
                        "message": {
                            "id": "msg-003",
                            "author": {"role": "user"},
                            "create_time": 1700001001,
                            "content": {
                                "parts": [
                                    "What is the Michigan MGCB exposure for DraftKings? "
                                    "EV-001 is the core evidence. Pillar 1 covers this domain. "
                                    "MBv54 section 3.2 is the controlling text."
                                ]
                            },
                        }
                    },
                },
            },
            {
                "id": "thread-003",
                "title": "General Python coding question",
                "create_time": 1700002000,
                "default_model_slug": "gpt-4",
                "mapping": {
                    "msg-004": {
                        "message": {
                            "id": "msg-004",
                            "author": {"role": "user"},
                            "create_time": 1700002001,
                            "content": {
                                "parts": ["How do I sort a list in Python?"]
                            },
                        }
                    },
                },
            },
        ]
        conv_path = repo_root / "data-lake" / "raw" / "conversations_test.json"
        conv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(conv_path, "w") as f:
            json.dump(test_data, f, indent=2)
        log.info("Synthetic test data written to: %s", conv_path)

    stats = run_pipeline(conv_path, repo_root)
    print(json.dumps(stats, indent=2))
