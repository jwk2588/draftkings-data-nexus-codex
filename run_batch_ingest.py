"""
DraftKings HiveMind v3.0 — Batch Ingest Runner
Processes all 11,360 iCloud photos through Tier 1 pipeline.
Runs in background, checkpoints progress, resumes on restart.
"""

import os
import sys
import json
import glob
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/HiveMind"))
from agents.tier1.deterministic_agents import Tier1Pipeline

import psycopg2
from psycopg2.extras import Json

DB_CONFIG = {
    "host": "localhost", "database": "draftkings_hivemind",
    "user": "hivemind", "password": "hivemind_secure_2026"
}

LOG_FILE = os.path.expanduser("~/HiveMind/logs/audit/batch_ingest.log")
CHECKPOINT_FILE = os.path.expanduser("~/HiveMind/logs/audit/batch_checkpoint.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("BatchIngest")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"processed": [], "failed": [], "total_processed": 0, "started_at": str(datetime.now(timezone.utc))}


def save_checkpoint(cp):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f, default=str)


def get_all_photos():
    base = os.path.expanduser("~/icloud_photos")
    photos = []
    for ext in ["*.PNG", "*.JPG", "*.JPEG", "*.HEIC", "*.png", "*.jpg", "*.jpeg", "*.heic"]:
        photos.extend(glob.glob(os.path.join(base, "**", ext), recursive=True))
    return sorted(photos)


def main():
    log.info("=" * 60)
    log.info("DraftKings HiveMind — Batch Ingest Starting")
    log.info("=" * 60)

    all_photos = get_all_photos()
    log.info(f"Total photos found: {len(all_photos)}")

    checkpoint = load_checkpoint()
    processed_set = set(checkpoint.get("processed", []))
    remaining = [p for p in all_photos if p not in processed_set]
    log.info(f"Already processed: {len(processed_set)} | Remaining: {len(remaining)}")

    pipeline = Tier1Pipeline()
    stats = {
        "total": len(all_photos), "processed": len(processed_set),
        "success": 0, "failed": 0, "legal_docs": 0, "dk_screenshots": 0
    }

    try:
        for i, photo_path in enumerate(remaining):
            try:
                result = pipeline.process_photo(photo_path)

                if result.get("status") == "TIER1_COMPLETE":
                    stats["success"] += 1
                    checkpoint["processed"].append(photo_path)

                    # Track interesting content
                    ocr = result.get("ocr", {})
                    if ocr.get("contains_legal"):
                        stats["legal_docs"] += 1
                    if ocr.get("app_ui") == "DraftKings":
                        stats["dk_screenshots"] += 1
                else:
                    stats["failed"] += 1
                    checkpoint["failed"].append(photo_path)

                stats["processed"] += 1
                checkpoint["total_processed"] = stats["processed"]

                # Save checkpoint every 50 files
                if i % 50 == 0:
                    save_checkpoint(checkpoint)
                    log.info(f"Progress: {stats['processed']}/{stats['total']} | "
                             f"Legal: {stats['legal_docs']} | DK: {stats['dk_screenshots']} | "
                             f"Failed: {stats['failed']}")

            except Exception as e:
                log.error(f"Error processing {photo_path}: {e}")
                stats["failed"] += 1
                checkpoint["failed"].append(photo_path)

    finally:
        save_checkpoint(checkpoint)
        pipeline.close()

    # Final report
    log.info("=" * 60)
    log.info("BATCH INGEST COMPLETE")
    log.info(f"Total: {stats['total']} | Success: {stats['success']} | Failed: {stats['failed']}")
    log.info(f"Legal Documents: {stats['legal_docs']} | DraftKings Screenshots: {stats['dk_screenshots']}")
    log.info("=" * 60)

    # Write summary to DB
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO event_ledger (event_type, event_subtype, payload, agent_id, ontology_version, checksum)
            VALUES ('BATCH_INGEST', 'COMPLETED', %s, 'BATCH_RUNNER', '1.0.0', 'batch_complete')
        """, (Json(stats),))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
