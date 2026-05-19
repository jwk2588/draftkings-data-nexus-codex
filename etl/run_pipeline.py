"""
github-gem-seeker ETL Master Pipeline
======================================
Chains all four ETL steps end-to-end:

  Step 1: conversations.json → normalized.parquet   (chatgpt_normalizer)
  Step 2: normalized.parquet → classified.parquet   (dk_relevance_classifier)
  Step 3: classified.parquet → entities.parquet     (entity_extractor)
  Step 4: entities.parquet   → Neo4j graph          (neo4j_ingestor)

Usage:
  python3 etl/run_pipeline.py [--input path/to/conversations.json]

Author  : github-gem-seeker ETL Pipeline
Version : 1.0.0
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ETL] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/home/ubuntu/draftkings-data-nexus-codex/artifacts/pipeline.log"),
    ]
)
log = logging.getLogger("run_pipeline")

ARTIFACTS = Path("/home/ubuntu/draftkings-data-nexus-codex/artifacts/parquet")
ARTIFACTS.mkdir(parents=True, exist_ok=True)


def run(conversations_path: str = None) -> dict:
    report = {"steps": [], "errors": []}

    # ------------------------------------------------------------------
    # Step 1: Normalize
    # ------------------------------------------------------------------
    log.info("=" * 55)
    log.info("STEP 1 — Normalize ChatGPT Export")
    log.info("=" * 55)
    try:
        from etl.normalizers.chatgpt_normalizer import normalize_conversations, normalize_sample
        norm_out = ARTIFACTS / "normalized.parquet"

        if conversations_path and Path(conversations_path).exists():
            df_norm = normalize_conversations(conversations_path, norm_out)
        else:
            log.info("No conversations.json provided — using synthetic sample")
            df_norm = normalize_sample(norm_out)

        report["steps"].append({"step": "normalize", "records": len(df_norm), "status": "ok"})
        log.info("Normalized: %d records → %s", len(df_norm), norm_out)
    except Exception as e:
        log.error("Normalize failed: %s", e)
        report["errors"].append({"step": "normalize", "error": str(e)})
        return report

    # ------------------------------------------------------------------
    # Step 2: Classify
    # ------------------------------------------------------------------
    log.info("=" * 55)
    log.info("STEP 2 — DK Relevance Classification")
    log.info("=" * 55)
    try:
        from etl.classifiers.dk_relevance_classifier import classify_parquet
        classified_out = ARTIFACTS / "classified.parquet"
        df_classified = classify_parquet(ARTIFACTS / "normalized.parquet", classified_out)
        dk_relevant = df_classified.filter(df_classified["dk_relevant"] == True).height
        report["steps"].append({
            "step": "classify",
            "total_records": len(df_classified),
            "dk_relevant": dk_relevant,
            "relevance_rate": f"{100 * dk_relevant / max(len(df_classified), 1):.1f}%",
            "status": "ok",
        })
        log.info("Classified: %d DK-relevant / %d total", dk_relevant, len(df_classified))
    except Exception as e:
        log.error("Classify failed: %s", e)
        report["errors"].append({"step": "classify", "error": str(e)})
        return report

    # ------------------------------------------------------------------
    # Step 3: Extract Entities
    # ------------------------------------------------------------------
    log.info("=" * 55)
    log.info("STEP 3 — Entity Extraction")
    log.info("=" * 55)
    try:
        from etl.extractors.entity_extractor import extract_from_classified
        entity_dfs = extract_from_classified(
            input_path=ARTIFACTS / "classified.parquet",
            output_dir=ARTIFACTS,
            dk_only=True,
        )
        report["steps"].append({
            "step": "extract_entities",
            "evidence_nodes": len(entity_dfs.get("evidence", [])),
            "players": len(entity_dfs.get("players", [])),
            "teams": len(entity_dfs.get("teams", [])),
            "markets": len(entity_dfs.get("markets", [])),
            "status": "ok",
        })
        log.info("Extracted: %d evidence, %d players, %d teams, %d markets",
                 len(entity_dfs.get("evidence", [])), len(entity_dfs.get("players", [])),
                 len(entity_dfs.get("teams", [])), len(entity_dfs.get("markets", [])))
    except Exception as e:
        log.error("Entity extraction failed: %s", e)
        report["errors"].append({"step": "extract_entities", "error": str(e)})
        return report

    # ------------------------------------------------------------------
    # Step 4: Neo4j Ingest
    # ------------------------------------------------------------------
    log.info("=" * 55)
    log.info("STEP 4 — Neo4j Graph Ingest")
    log.info("=" * 55)
    try:
        from etl.neo4j_ingestor import Neo4jIngestor
        ingestor = Neo4jIngestor()
        ingest_summary = ingestor.run_full_ingest(ARTIFACTS)
        ingestor.close()
        report["steps"].append({"step": "neo4j_ingest", **ingest_summary, "status": "ok"})
        log.info("Neo4j ingest: %s", json.dumps(ingest_summary))
    except Exception as e:
        log.error("Neo4j ingest failed: %s", e)
        report["errors"].append({"step": "neo4j_ingest", "error": str(e)})

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    report["success"] = len(report["errors"]) == 0
    report["steps_completed"] = len(report["steps"])

    summary_path = Path("/home/ubuntu/draftkings-data-nexus-codex/artifacts/pipeline_report.json")
    summary_path.write_text(json.dumps(report, indent=2))

    log.info("=" * 55)
    log.info("PIPELINE COMPLETE — %d steps, %d errors", report["steps_completed"], len(report["errors"]))
    log.info("Report: %s", summary_path)
    log.info("=" * 55)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="github-gem-seeker ETL Pipeline")
    parser.add_argument("--input", help="Path to conversations.json", default=None)
    args = parser.parse_args()
    result = run(args.input)
    print(json.dumps(result, indent=2))
