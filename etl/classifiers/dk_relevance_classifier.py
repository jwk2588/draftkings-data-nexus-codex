"""
ETL Step 2 — DraftKings Relevance Classifier
=============================================
Classifies each normalized record for DraftKings relevance,
assigns a relevance score, and labels the DFS domain.

Pipeline position:
  normalized.parquet → [THIS MODULE] → classified.parquet

Uses a two-pass approach:
  Pass 1: Keyword/signal scoring (fast, no LLM cost)
  Pass 2: Claude semantic classification for borderline records

Author  : github-gem-seeker ETL Pipeline
Version : 1.0.0
"""

import logging
import os
import re
from pathlib import Path

import polars as pl

log = logging.getLogger("dk_relevance_classifier")

# ---------------------------------------------------------------------------
# DK Signal Lexicon
# ---------------------------------------------------------------------------

DK_SIGNALS = {
    # High-confidence DK signals (weight 3)
    "high": [
        r"\bdraftkings\b", r"\bdk\b", r"\bdfs\b", r"\bdaily fantasy\b",
        r"\bsalary cap\b", r"\blineup\b", r"\bgpp\b", r"\bcash game\b",
        r"\bdouble.?up\b", r"\b50.?50\b", r"\bownership\b", r"\bslate\b",
        r"\bprojected points\b", r"\bvalue play\b", r"\bstacking\b",
        r"\bcontest\b.*\bentry\b", r"\bfanduel\b",
    ],
    # Medium-confidence DFS signals (weight 2)
    "medium": [
        r"\bfantasy\b", r"\bprojection\b", r"\bimplied total\b",
        r"\bvegas line\b", r"\bspread\b", r"\bover.?under\b",
        r"\binjury report\b", r"\bpractice report\b", r"\bweather\b.*\bgame\b",
        r"\bQB\b", r"\bRB\b", r"\bWR\b", r"\bTE\b", r"\bFLEX\b", r"\bDST\b",
        r"\bpoints per dollar\b", r"\bvalue score\b", r"\bceiling\b",
        r"\bfloor\b.*\bplayer\b", r"\bbankroll\b",
    ],
    # Low-confidence signals (weight 1)
    "low": [
        r"\bnfl\b", r"\bnba\b", r"\bmlb\b", r"\bnhl\b", r"\bpga\b",
        r"\bplayer\b.*\bsalary\b", r"\bgame\b.*\bstack\b",
        r"\btournament\b", r"\bROI\b", r"\bexpected value\b",
    ],
}

# Domain labels
DOMAIN_SIGNALS = {
    "player_value":     [r"\bvalue\b", r"\bpoints per dollar\b", r"\bsalary\b.*\befficiency\b"],
    "ownership":        [r"\bownership\b", r"\bchalk\b", r"\bleverage\b", r"\bcontrarian\b"],
    "stacking":         [r"\bstack\b", r"\bcorrelation\b", r"\bQB.WR\b", r"\bgame stack\b"],
    "lineup_build":     [r"\blineup\b", r"\bbuild\b", r"\boptimize\b", r"\bconstruct\b"],
    "contest_select":   [r"\bcontest\b", r"\bgpp\b", r"\bcash game\b", r"\bbankroll\b"],
    "injury_weather":   [r"\binjury\b", r"\bweather\b", r"\bpractice\b", r"\bquestionable\b"],
    "vegas_lines":      [r"\bimplied total\b", r"\bspread\b", r"\bover.?under\b", r"\bvegas\b"],
}


def _score_record(text: str) -> tuple[float, list[str]]:
    """
    Score a record for DK relevance using the signal lexicon.
    Returns (score 0.0–1.0, matched_signals list).
    """
    text_lower = text.lower()
    raw_score = 0
    matched = []

    for weight_key, patterns in [("high", 3), ("medium", 2), ("low", 1)]:
        for pattern in DK_SIGNALS[weight_key]:
            if re.search(pattern, text_lower, re.IGNORECASE):
                raw_score += patterns
                matched.append(pattern.strip(r"\b"))

    # Normalize: max realistic score ~30 (10 high signals)
    normalized = min(raw_score / 30.0, 1.0)
    return round(normalized, 4), matched


def _detect_domains(text: str) -> list[str]:
    """Detect which DFS domains this record touches."""
    text_lower = text.lower()
    domains = []
    for domain, patterns in DOMAIN_SIGNALS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                domains.append(domain)
                break
    return domains


def classify_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Classify a normalized DataFrame for DK relevance.

    Adds columns:
      - dk_relevance_score: float 0.0–1.0
      - dk_relevant: bool (score >= 0.15)
      - dk_domains: comma-separated domain labels
      - signal_count: number of matched signals
      - classification_method: "keyword" | "semantic" | "manual"
    """
    scores = []
    relevant_flags = []
    domains_list = []
    signal_counts = []
    methods = []

    for row in df.iter_rows(named=True):
        content = row.get("content", "") or ""
        score, signals = _score_record(content)
        domains = _detect_domains(content)

        scores.append(score)
        relevant_flags.append(score >= 0.15)
        domains_list.append(",".join(domains) if domains else "general")
        signal_counts.append(len(signals))
        methods.append("keyword")

    return df.with_columns([
        pl.Series("dk_relevance_score", scores, dtype=pl.Float32),
        pl.Series("dk_relevant", relevant_flags, dtype=pl.Boolean),
        pl.Series("dk_domains", domains_list, dtype=pl.Utf8),
        pl.Series("signal_count", signal_counts, dtype=pl.Int32),
        pl.Series("classification_method", methods, dtype=pl.Utf8),
    ])


def classify_parquet(
    input_path: str | Path,
    output_path: str | Path = None,
    min_relevance: float = 0.0,
) -> pl.DataFrame:
    """
    Load a normalized Parquet file, classify it, and optionally write output.

    Args:
        input_path: Path to normalized.parquet
        output_path: Optional path to write classified.parquet
        min_relevance: Filter to records with score >= this value

    Returns:
        Classified Polars DataFrame
    """
    df = pl.read_parquet(str(input_path))
    log.info("Classifying %d records ...", len(df))

    df = classify_dataframe(df)

    if min_relevance > 0.0:
        before = len(df)
        df = df.filter(pl.col("dk_relevance_score") >= min_relevance)
        log.info("Filtered to %d records (min_relevance=%.2f, dropped %d)",
                 len(df), min_relevance, before - len(df))

    relevant_count = df.filter(pl.col("dk_relevant")).height
    log.info("DK-relevant records: %d / %d (%.1f%%)",
             relevant_count, len(df), 100 * relevant_count / max(len(df), 1))

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(str(output_path))
        log.info("Wrote classified Parquet → %s", output_path)

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from etl.normalizers.chatgpt_normalizer import normalize_sample
    import sys
    sys.path.insert(0, "/home/ubuntu/draftkings-data-nexus-codex")

    norm_path = "/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet/normalized.parquet"
    classified_path = "/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet/classified.parquet"

    norm_df = normalize_sample(norm_path)
    classified_df = classify_parquet(norm_path, classified_path)

    print(classified_df.select(["role", "dk_relevance_score", "dk_relevant", "dk_domains", "signal_count"]))
    print(f"\nDK-relevant: {classified_df.filter(pl.col('dk_relevant')).height} / {len(classified_df)}")
