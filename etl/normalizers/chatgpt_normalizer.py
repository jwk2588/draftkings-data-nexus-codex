"""
ETL Step 1 — ChatGPT Export Normalizer
=======================================
Ingests ChatGPT conversations.json export and normalizes it
into a clean Polars DataFrame, then writes to Parquet.

Pipeline position:
  conversations.json → [THIS MODULE] → normalized.parquet

Author  : github-gem-seeker ETL Pipeline
Version : 1.0.0
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import polars as pl

log = logging.getLogger("chatgpt_normalizer")

# ---------------------------------------------------------------------------
# Schema for normalized conversation records
# ---------------------------------------------------------------------------

NORMALIZED_SCHEMA = {
    "record_id":        pl.Utf8,    # UUID derived from content hash
    "conversation_id":  pl.Utf8,
    "conversation_title": pl.Utf8,
    "message_id":       pl.Utf8,
    "role":             pl.Utf8,    # user | assistant | system | tool
    "content":          pl.Utf8,    # Raw message text
    "content_hash":     pl.Utf8,    # SHA-256 of content (64-char hex)
    "created_at":       pl.Utf8,    # ISO-8601
    "model":            pl.Utf8,    # gpt-4|gpt-4o|etc. if available
    "source_system":    pl.Utf8,    # always "chatgpt_export"
    "word_count":       pl.Int32,
    "char_count":       pl.Int32,
}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _extract_text(content) -> str:
    """Extract plain text from ChatGPT content (handles string and parts array)."""
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        parts = content.get("parts", [])
        if parts:
            return " ".join(str(p) for p in parts if p)
        text = content.get("text", "")
        if text:
            return text
    if isinstance(content, list):
        return " ".join(str(p) for p in content if p)
    return str(content) if content else ""


def normalize_conversations(
    input_path: str | Path,
    output_path: str | Path = None,
) -> pl.DataFrame:
    """
    Normalize a ChatGPT conversations.json export into a flat Polars DataFrame.

    Args:
        input_path: Path to conversations.json
        output_path: Optional path to write normalized.parquet

    Returns:
        Polars DataFrame with normalized records
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"conversations.json not found: {input_path}")

    log.info("Loading %s ...", input_path)
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raw = [raw]

    records = []
    for conv in raw:
        conv_id = conv.get("id", "")
        conv_title = conv.get("title", "Untitled")
        mapping = conv.get("mapping", {})

        for node_id, node in mapping.items():
            msg = node.get("message")
            if not msg:
                continue

            role = msg.get("author", {}).get("role", "unknown")
            content_raw = msg.get("content", {})
            content_text = _extract_text(content_raw)

            if not content_text or not content_text.strip():
                continue

            content_text = content_text.strip()
            ts_raw = msg.get("create_time")
            if ts_raw:
                try:
                    ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc).isoformat()
                except Exception:
                    ts = datetime.now(timezone.utc).isoformat()
            else:
                ts = datetime.now(timezone.utc).isoformat()

            # Model metadata
            metadata = msg.get("metadata", {})
            model = metadata.get("model_slug", "unknown")

            # SHA-256 hash — the integrity fingerprint
            content_hash = _sha256(content_text)
            record_id = _sha256(f"{conv_id}:{node_id}:{content_hash}")

            records.append({
                "record_id":           record_id,
                "conversation_id":     conv_id,
                "conversation_title":  conv_title,
                "message_id":          node_id,
                "role":                role,
                "content":             content_text,
                "content_hash":        content_hash,
                "created_at":          ts,
                "model":               model,
                "source_system":       "chatgpt_export",
                "word_count":          len(content_text.split()),
                "char_count":          len(content_text),
            })

    if not records:
        log.warning("No records extracted from %s", input_path)
        return pl.DataFrame(schema=NORMALIZED_SCHEMA)

    df = pl.DataFrame(records, schema=NORMALIZED_SCHEMA)

    # Deduplicate by content_hash
    before = len(df)
    df = df.unique(subset=["content_hash"], keep="first")
    dupes = before - len(df)
    if dupes:
        log.info("Removed %d duplicate records (content_hash collision)", dupes)

    log.info("Normalized %d records from %d conversations", len(df), len(raw))

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(str(output_path))
        log.info("Wrote normalized Parquet → %s", output_path)

    return df


def normalize_sample(output_path: str | Path = None) -> pl.DataFrame:
    """
    Generate a synthetic normalized sample for testing when no
    conversations.json is available.
    """
    sample_conversations = [
        {
            "id": "conv_001",
            "title": "DraftKings NFL Week 14 Strategy",
            "mapping": {
                "msg_001": {"message": {"author": {"role": "user"}, "content": {"parts": ["What are the best value plays for NFL Week 14 DraftKings? I'm looking at Patrick Mahomes at $8200 and Josh Allen at $8500."]}, "create_time": 1733000000, "metadata": {"model_slug": "gpt-4o"}}},
                "msg_002": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["For NFL Week 14 DraftKings, Mahomes at $8200 is excellent value given KC's implied team total of 28.5. Allen is slightly overpriced at $8500 but has the highest ceiling. Consider Lamar Jackson at $7800 as the leverage play — only 18% projected ownership but faces a weak secondary."]}, "create_time": 1733000060, "metadata": {"model_slug": "gpt-4o"}}},
            }
        },
        {
            "id": "conv_002",
            "title": "DraftKings Ownership Modeling GPP",
            "mapping": {
                "msg_003": {"message": {"author": {"role": "user"}, "content": {"parts": ["How do I model ownership for large GPP tournaments on DraftKings? I want to find leverage plays under 10% ownership."]}, "create_time": 1733100000, "metadata": {"model_slug": "gpt-4o"}}},
                "msg_004": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["For GPP ownership modeling: use Vegas implied totals as the primary signal — players on teams with implied totals above 27 get chalk ownership. Target WR2s on high-total teams with under 12% projected ownership. The key metric is points-per-dollar ceiling, not floor. Stack 2-3 players from the same game for correlation."]}, "create_time": 1733100120, "metadata": {"model_slug": "gpt-4o"}}},
            }
        },
        {
            "id": "conv_003",
            "title": "DraftKings Contest Selection Strategy",
            "mapping": {
                "msg_005": {"message": {"author": {"role": "user"}, "content": {"parts": ["What is the optimal contest selection strategy for DraftKings? Should I play more GPPs or cash games?"]}, "create_time": 1733200000, "metadata": {"model_slug": "gpt-4o"}}},
                "msg_006": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["Optimal DraftKings bankroll allocation: 60% cash games (50/50s, double-ups) for consistent ROI, 40% GPPs for upside. Cash games require a floor-based lineup construction — avoid injury risks. GPPs need ceiling-based construction with stacks and leverage plays. Never play more than 5% of bankroll in a single GPP."]}, "create_time": 1733200180, "metadata": {"model_slug": "gpt-4o"}}},
            }
        },
    ]

    sample_path = Path("/tmp/sample_conversations.json")
    sample_path.write_text(json.dumps(sample_conversations))
    return normalize_conversations(sample_path, output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = normalize_sample(output_path="/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet/normalized.parquet")
    print(df)
    print(f"\nSchema: {df.schema}")
    print(f"Records: {len(df)}")
    print(f"\nSample content_hash: {df['content_hash'][0]}")
