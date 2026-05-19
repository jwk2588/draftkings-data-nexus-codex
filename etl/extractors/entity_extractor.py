"""
ETL Step 3 — Entity Extractor
==============================
Extracts DraftKings entities (Players, Teams, Contests, Markets)
from classified records and prepares them for Neo4j ingestion.

Pipeline position:
  classified.parquet → [THIS MODULE] → entities.parquet + evidence.parquet

Uses regex + Claude for high-confidence entity extraction.
All extracted entities get SHA-256 content hashes.

Author  : github-gem-seeker ETL Pipeline
Version : 1.0.0
"""

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

log = logging.getLogger("entity_extractor")

# ---------------------------------------------------------------------------
# Entity extraction patterns
# ---------------------------------------------------------------------------

NFL_POSITIONS = r"\b(QB|RB|WR|TE|FLEX|DST|K)\b"
NFL_TEAMS = r"\b(KC|BUF|SF|PHI|DAL|MIA|BAL|CIN|DET|MIN|GB|SEA|LAR|TB|NYG|NE|PIT|LV|DEN|LAC|ARI|ATL|CAR|CHI|CLE|HOU|IND|JAC|NYJ|NO|TEN|WAS)\b"
SALARY_PATTERN = r"\$(\d{1,2},?\d{3})"
PLAYER_NAME = r"\b([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b"
OWNERSHIP_PATTERN = r"(\d{1,3}(?:\.\d)?)\s*%\s*(?:projected\s+)?ownership"
IMPLIED_TOTAL = r"implied\s+(?:team\s+)?total\s+(?:of\s+)?(\d{1,2}(?:\.\d)?)"
SPREAD_PATTERN = r"(?:spread|line)\s+(?:of\s+)?([+-]?\d{1,2}(?:\.\d)?)"
OVER_UNDER = r"(?:over.?under|total)\s+(?:of\s+)?(\d{2,3}(?:\.\d)?)"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _new_id() -> str:
    return str(uuid.uuid4())


def extract_entities_from_text(
    text: str,
    source_record_id: str,
    conversation_id: str,
    extracted_at: str,
) -> dict:
    """
    Extract all DK entities from a single text record.
    Returns a dict with lists of entity dicts by type.
    """
    entities = {
        "players": [],
        "teams": [],
        "markets": [],
        "evidence_nodes": [],
    }

    # --- Player mentions ---
    names = re.findall(PLAYER_NAME, text)
    positions = re.findall(NFL_POSITIONS, text, re.IGNORECASE)
    salaries = re.findall(SALARY_PATTERN, text)

    for i, name in enumerate(set(names)):
        if len(name.split()) < 2:
            continue
        player_text = f"{name} mentioned in DK context"
        content_hash = _sha256(f"player:{name}:{source_record_id}")
        entities["players"].append({
            "player_id":    content_hash[:16],
            "name":         name,
            "position":     positions[i] if i < len(positions) else "UNKNOWN",
            "salary":       int(salaries[i].replace(",", "")) if i < len(salaries) else 0,
            "sport":        "NFL",
            "content_hash": content_hash,
            "source_record": source_record_id,
            "extracted_at": extracted_at,
        })

    # --- Team mentions ---
    teams_found = list(set(re.findall(NFL_TEAMS, text)))
    for team_abbr in teams_found:
        content_hash = _sha256(f"team:{team_abbr}")
        entities["teams"].append({
            "team_id":      content_hash[:16],
            "abbreviation": team_abbr,
            "full_name":    team_abbr,
            "sport":        "NFL",
            "content_hash": content_hash,
            "source_record": source_record_id,
            "extracted_at": extracted_at,
        })

    # --- Market data ---
    implied_totals = re.findall(IMPLIED_TOTAL, text, re.IGNORECASE)
    spreads = re.findall(SPREAD_PATTERN, text, re.IGNORECASE)
    totals = re.findall(OVER_UNDER, text, re.IGNORECASE)
    ownerships = re.findall(OWNERSHIP_PATTERN, text, re.IGNORECASE)

    for i, total in enumerate(implied_totals):
        content_hash = _sha256(f"market:implied_total:{total}:{source_record_id}:{i}")
        entities["markets"].append({
            "market_id":    content_hash[:16],
            "game":         "UNKNOWN",
            "market_type":  "ImpliedTotal",
            "line":         float(total),
            "source":       "chatgpt_export",
            "content_hash": content_hash,
            "source_record": source_record_id,
            "extracted_at": extracted_at,
        })

    # --- Evidence nodes (the core T1 atoms) ---
    # Each classified record becomes one Evidence node
    evidence_hash = _sha256(text)
    entities["evidence_nodes"].append({
        "ev_id":          _sha256(f"ev:{source_record_id}:{evidence_hash}"),
        "tier":           1,
        "source_doc":     conversation_id,
        "evidence_text":  text[:2000],
        "extracted_by":   "chatgpt_etl",
        "extracted_at":   extracted_at,
        "confidence":     0.85,
        "content_hash":   evidence_hash,
        "player_refs":    json.dumps([p["name"] for p in entities["players"]]),
        "team_refs":      json.dumps([t["abbreviation"] for t in entities["teams"]]),
        "ownership_refs": json.dumps(ownerships),
        "implied_totals": json.dumps(implied_totals),
    })

    return entities


def extract_from_classified(
    input_path: str | Path,
    output_dir: str | Path = None,
    dk_only: bool = True,
) -> dict[str, pl.DataFrame]:
    """
    Run entity extraction over the full classified Parquet file.

    Args:
        input_path: Path to classified.parquet
        output_dir: Directory to write entities.parquet and evidence.parquet
        dk_only: Only process DK-relevant records

    Returns:
        Dict with DataFrames: "evidence", "players", "teams", "markets"
    """
    df = pl.read_parquet(str(input_path))
    if dk_only:
        df = df.filter(pl.col("dk_relevant") == True)
    log.info("Extracting entities from %d DK-relevant records ...", len(df))

    all_evidence = []
    all_players = []
    all_teams = []
    all_markets = []

    for row in df.iter_rows(named=True):
        content = row.get("content", "") or ""
        if not content.strip():
            continue

        extracted_at = datetime.now(timezone.utc).isoformat()
        result = extract_entities_from_text(
            text=content,
            source_record_id=row.get("record_id", ""),
            conversation_id=row.get("conversation_id", ""),
            extracted_at=extracted_at,
        )

        all_evidence.extend(result["evidence_nodes"])
        all_players.extend(result["players"])
        all_teams.extend(result["teams"])
        all_markets.extend(result["markets"])

    # Deduplicate by content_hash
    def dedup(records: list, key: str = "content_hash") -> list:
        seen = set()
        out = []
        for r in records:
            h = r.get(key, "")
            if h not in seen:
                seen.add(h)
                out.append(r)
        return out

    all_evidence = dedup(all_evidence, "ev_id")
    all_players = dedup(all_players, "content_hash")
    all_teams = dedup(all_teams, "content_hash")
    all_markets = dedup(all_markets, "content_hash")

    log.info("Extracted: %d evidence nodes, %d players, %d teams, %d markets",
             len(all_evidence), len(all_players), len(all_teams), len(all_markets))

    dfs = {
        "evidence": pl.DataFrame(all_evidence) if all_evidence else pl.DataFrame(),
        "players":  pl.DataFrame(all_players)  if all_players  else pl.DataFrame(),
        "teams":    pl.DataFrame(all_teams)    if all_teams    else pl.DataFrame(),
        "markets":  pl.DataFrame(all_markets)  if all_markets  else pl.DataFrame(),
    }

    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, frame in dfs.items():
            if len(frame) > 0:
                out_path = output_dir / f"{name}.parquet"
                frame.write_parquet(str(out_path))
                log.info("Wrote %s → %s", name, out_path)

    return dfs


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, "/home/ubuntu/draftkings-data-nexus-codex")

    result = extract_from_classified(
        input_path="/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet/classified.parquet",
        output_dir="/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet",
    )
    for name, df in result.items():
        print(f"\n{name.upper()} ({len(df)} records):")
        if len(df) > 0:
            print(df)
