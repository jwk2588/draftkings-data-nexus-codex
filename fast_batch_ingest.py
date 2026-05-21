"""
DraftKings HiveMind v3.0 — Fast Parallelized Batch Photo Ingest
Uses multiprocessing with 2 workers (memory-constrained) for OCR + DB insertion.
Processes all 11,360 iCloud photos with checkpoint/resume support.
"""

import os
import sys
import json
import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from multiprocessing import Pool, cpu_count
from functools import partial

import psycopg2
from psycopg2.extras import Json

DB_CONFIG = {
    "host": "localhost", "database": "draftkings_hivemind",
    "user": "hivemind", "password": "hivemind_secure_2026"
}

ICLOUD_DIR = os.path.expanduser("~/icloud_photos")
CHECKPOINT_FILE = os.path.expanduser("~/HiveMind/logs/audit/fast_batch_checkpoint.json")
LOG_FILE = os.path.expanduser("~/HiveMind/logs/audit/fast_batch_ingest.log")
BATCH_SIZE = 50  # Process 50 photos per DB batch commit
NUM_WORKERS = 2  # Limited by 955MB RAM

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("HiveMind.FastBatch")


def compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def extract_exif(path: str) -> Dict[str, Any]:
    """Extract EXIF metadata from image."""
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
        img = Image.open(path)
        exif_data = {}
        raw_exif = img._getexif()
        if raw_exif:
            for tag_id, value in raw_exif.items():
                tag = TAGS.get(tag_id, tag_id)
                if isinstance(value, (str, int, float)):
                    exif_data[str(tag)] = value
        exif_data["width"] = img.width
        exif_data["height"] = img.height
        exif_data["mode"] = img.mode
        img.close()
        return exif_data
    except Exception:
        return {}


def run_ocr(path: str) -> Dict[str, Any]:
    """Run Tesseract OCR on image. Returns text and confidence."""
    try:
        import pytesseract
        from PIL import Image
        img = Image.open(path)
        # Convert to RGB if needed
        if img.mode not in ('RGB', 'L'):
            img = img.convert('RGB')
        # Run OCR with confidence data
        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT, timeout=30)
        words = [w for w, c in zip(data['text'], data['conf']) if w.strip() and int(c) > 0]
        confs = [int(c) for c in data['conf'] if int(c) > 0]
        text = ' '.join(words)
        avg_conf = sum(confs) / len(confs) / 100.0 if confs else 0.0
        img.close()

        # Detect app/scene
        text_lower = text.lower()
        app_detected = None
        if any(kw in text_lower for kw in ['draftkings', 'dkng', 'dk rewards']):
            app_detected = 'DraftKings'
        elif any(kw in text_lower for kw in ['chatgpt', 'openai', 'gpt-4', 'claude']):
            app_detected = 'ChatGPT/AI'
        elif any(kw in text_lower for kw in ['sec.gov', '10-k', 'edgar']):
            app_detected = 'SEC_Filing'

        scene = 'Legal_Document' if any(kw in text_lower for kw in ['exhibit', 'pursuant', 'whereas', 'herein']) else \
                'Financial_Statement' if any(kw in text_lower for kw in ['revenue', 'gaap', 'asc', 'fasb']) else \
                'Screenshot'

        return {
            "text": text,
            "confidence": round(avg_conf, 4),
            "word_count": len(words),
            "app_detected": app_detected,
            "scene": scene,
            "contains_amounts": '$' in text or 'million' in text_lower or 'billion' in text_lower,
            "contains_legal": any(kw in text_lower for kw in ['exhibit', 'asc', 'sec', 'gaap', 'violation']),
            "contains_financial": any(kw in text_lower for kw in ['revenue', 'profit', 'loss', 'earnings'])
        }
    except Exception as e:
        return {"text": "", "confidence": 0.0, "word_count": 0, "error": str(e)[:100]}


def process_single_photo(photo_path: str) -> Optional[Dict[str, Any]]:
    """Process a single photo: hash + EXIF + OCR. Returns result dict."""
    try:
        path = Path(photo_path)
        if not path.exists():
            return None

        file_size = path.stat().st_size
        sha256 = compute_sha256(photo_path)
        exif = extract_exif(photo_path)
        ocr = run_ocr(photo_path)

        # Parse date from directory structure (YYYY/MM/DD)
        parts = path.parts
        capture_date = None
        for i, part in enumerate(parts):
            if part.isdigit() and len(part) == 4 and 2000 <= int(part) <= 2030:
                try:
                    year = int(parts[i])
                    month = int(parts[i+1]) if i+1 < len(parts) else 1
                    day = int(parts[i+2]) if i+2 < len(parts) else 1
                    capture_date = f"{year:04d}-{month:02d}-{day:02d}"
                except (ValueError, IndexError):
                    pass
                break

        return {
            "path": photo_path,
            "filename": path.name,
            "sha256": sha256,
            "file_size": file_size,
            "capture_date": capture_date,
            "exif": exif,
            "ocr_text": ocr.get("text", ""),
            "ocr_confidence": ocr.get("confidence", 0.0),
            "word_count": ocr.get("word_count", 0),
            "app_detected": ocr.get("app_detected"),
            "scene": ocr.get("scene", "Screenshot"),
            "contains_amounts": ocr.get("contains_amounts", False),
            "contains_legal": ocr.get("contains_legal", False),
            "contains_financial": ocr.get("contains_financial", False),
        }
    except Exception as e:
        return {"path": photo_path, "error": str(e)[:200]}


def insert_batch(results: List[Dict]) -> Dict[str, int]:
    """Insert a batch of processed photos into PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    stats = {"inserted": 0, "skipped": 0, "errors": 0}

    try:
        for result in results:
            if not result or "error" in result:
                stats["errors"] += 1
                continue

            try:
                with conn.cursor() as cur:
                    # Check for duplicate
                    cur.execute("SELECT object_id FROM raw_objects WHERE sha256_hash = %s",
                                (result["sha256"],))
                    existing = cur.fetchone()
                    if existing:
                        stats["skipped"] += 1
                        continue

                    object_id = str(uuid.uuid4())
                    provenance_id = f"ICLOUD_{result['sha256'][:16]}"

                    # Insert raw_objects
                    cur.execute("""
                        INSERT INTO raw_objects (object_id, provenance_id, sha256_hash,
                            original_path, original_filename, device_source, mime_type,
                            file_size_bytes, object_type, storage_path, ingestion_agent, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        object_id, provenance_id, result["sha256"],
                        result["path"], result["filename"],
                        "iCloud_iPhone15Pro", "image/png",
                        result["file_size"], "PHOTO",
                        result["path"], "AGT-INGEST-FAST",
                        Json({"capture_date": result.get("capture_date"),
                              "exif_keys": list(result.get("exif", {}).keys())[:10]})
                    ))

                    # Insert canonical_metadata
                    cur.execute("""
                        INSERT INTO canonical_metadata (object_id, source_id,
                            semantic_tags, originating_agent, validation_status)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (object_id) DO NOTHING
                    """, (
                        object_id,
                        f"ICLOUD_{result['sha256'][:16]}",
                        Json([result.get('scene', 'Screenshot'), result.get('app_detected') or 'Unknown']),
                        'AGT-INGEST-FAST',
                        'PENDING'
                    ))

                    # Insert EXIF metadata
                    if result.get("exif"):
                        cur.execute("""
                            INSERT INTO exif_metadata (object_id, raw_exif,
                                device_make, device_model, is_screenshot,
                                image_width, image_height)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            object_id, Json(result.get("exif", {})),
                            result.get("exif", {}).get("Make", "Apple"),
                            result.get("exif", {}).get("Model", "iPhone 15 Pro"),
                            True,  # All iCloud photos are screenshots
                            result.get("exif", {}).get("width"),
                            result.get("exif", {}).get("height")
                        ))

                    # Insert OCR extract if we have text
                    if result.get("ocr_text") and len(result["ocr_text"]) > 10:
                        cur.execute("""
                            INSERT INTO ocr_extracts (object_id, engine_used, raw_text,
                                confidence_score, word_count, contains_amounts,
                                contains_legal_refs, contains_financial,
                                app_ui_detected, scene_classification)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            object_id, "tesseract",
                            result["ocr_text"][:50000],  # Cap at 50KB
                            result["ocr_confidence"],
                            result["word_count"],
                            result["contains_amounts"],
                            result["contains_legal"],
                            result["contains_financial"],
                            result["app_detected"],
                            result["scene"]
                        ))

                    # Emit event
                    payload = {"object_id": object_id, "filename": result["filename"],
                               "ocr_words": result["word_count"]}
                    checksum = hashlib.sha256(json.dumps(payload).encode()).hexdigest()
                    cur.execute("""
                        INSERT INTO event_ledger (event_type, event_subtype, payload,
                            agent_id, ontology_version, checksum)
                        VALUES ('INGEST', 'PHOTO_PROCESSED', %s, 'AGT-INGEST-FAST', '1.0.0', %s)
                    """, (Json(payload), checksum))

                conn.commit()
                stats["inserted"] += 1

            except Exception as e:
                conn.rollback()
                stats["errors"] += 1
                log.warning(f"[FastBatch] Insert error for {result.get('filename', '?')}: {e}")

    finally:
        conn.close()

    return stats


def load_checkpoint() -> set:
    """Load already-processed file paths from checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE) as f:
                data = json.load(f)
                return set(data.get("processed", []))
        except Exception:
            pass
    return set()


def save_checkpoint(processed: set, total_inserted: int, total_errors: int):
    """Save checkpoint to disk."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            "processed_count": len(processed),
            "total_inserted": total_inserted,
            "total_errors": total_errors,
            "last_updated": str(datetime.now(timezone.utc))
        }, f, indent=2)


def run_fast_batch():
    """Main batch runner with parallelized OCR and batched DB inserts."""
    log.info("[FastBatch] Starting parallelized iCloud photo ingest")
    start_time = time.time()

    # Collect all photo paths
    all_photos = []
    for root, dirs, files in os.walk(ICLOUD_DIR):
        for fname in sorted(files):
            if fname.lower().endswith(('.png', '.jpg', '.jpeg', '.heic', '.gif', '.mp4', '.mov')):
                all_photos.append(os.path.join(root, fname))

    log.info(f"[FastBatch] Found {len(all_photos)} photos total")

    # Load checkpoint — skip already processed
    processed = load_checkpoint()
    remaining = [p for p in all_photos if p not in processed]
    log.info(f"[FastBatch] {len(processed)} already processed, {len(remaining)} remaining")

    if not remaining:
        log.info("[FastBatch] All photos already processed!")
        return

    total_inserted = 0
    total_skipped = 0
    total_errors = 0
    batch_num = 0

    # Process in batches using multiprocessing pool
    with Pool(processes=NUM_WORKERS) as pool:
        for i in range(0, len(remaining), BATCH_SIZE):
            batch_paths = remaining[i:i + BATCH_SIZE]
            batch_num += 1

            log.info(f"[FastBatch] Batch {batch_num}: processing {len(batch_paths)} photos "
                     f"({i}/{len(remaining)} total, {total_inserted} inserted so far)")

            # Parallel OCR processing
            results = pool.map(process_single_photo, batch_paths)

            # Batch DB insert
            stats = insert_batch(results)
            total_inserted += stats["inserted"]
            total_skipped += stats["skipped"]
            total_errors += stats["errors"]

            # Update checkpoint
            for path in batch_paths:
                processed.add(path)
            save_checkpoint(processed, total_inserted, total_errors)

            elapsed = time.time() - start_time
            rate = total_inserted / elapsed * 60 if elapsed > 0 else 0
            log.info(f"[FastBatch] Progress: {total_inserted} inserted, {total_skipped} skipped, "
                     f"{total_errors} errors | Rate: {rate:.1f}/min | "
                     f"ETA: {(len(remaining) - i) / max(1, rate) * 60:.0f}s")

    elapsed = time.time() - start_time
    log.info(f"[FastBatch] COMPLETE: {total_inserted} inserted, {total_skipped} skipped, "
             f"{total_errors} errors in {elapsed:.1f}s")
    save_checkpoint(processed, total_inserted, total_errors)


if __name__ == "__main__":
    run_fast_batch()
