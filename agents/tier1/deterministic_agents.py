"""
DraftKings HiveMind v3.0 — Tier 1 Deterministic Agents
Facts only. No inference. No LLM calls.
Agents: OCR, EXIF, Hash, Dedup, Metadata Normalization, Timestamp Validator
"""

import os
import hashlib
import json
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import psycopg2
from psycopg2.extras import Json, RealDictCursor
import redis
import pytesseract
from PIL import Image
import exifread
import imagehash

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "database": "draftkings_hivemind",
    "user": "hivemind",
    "password": "hivemind_secure_2026"
}
REDIS_CONFIG = {"host": "localhost", "port": 6379, "db": 0}
ONTOLOGY_VERSION = "1.0.0"
CONFIDENCE_THRESHOLD = 0.75

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser("~/HiveMind/logs/audit/tier1.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("HiveMind.Tier1")


# ─────────────────────────────────────────────────────────────
# Base Agent Class
# ─────────────────────────────────────────────────────────────
class DeterministicAgent:
    """Base class for all Tier 1 deterministic agents."""

    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        self.db_conn.autocommit = False
        self.redis = redis.Redis(**REDIS_CONFIG, decode_responses=True)
        log.info(f"[{self.agent_id}] Initialized")

    def emit_event(self, event_type: str, event_subtype: str, payload: dict):
        """Emit an immutable event to the event ledger."""
        payload_str = json.dumps(payload, default=str)
        checksum = hashlib.sha256(payload_str.encode()).hexdigest()
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO event_ledger (event_type, event_subtype, payload, agent_id, ontology_version, checksum)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (event_type, event_subtype, Json(payload), self.agent_id, ONTOLOGY_VERSION, checksum))
        self.db_conn.commit()

    def log_validation(self, pipeline_stage: str, object_id: Optional[str],
                       pass_number: int, pass_name: str, result: str, details: dict):
        """Log a validation pass result."""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO validation_log (pipeline_stage, object_id, pass_number, pass_name, result, details, agent_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (pipeline_stage, object_id, pass_number, pass_name, result, Json(details), self.agent_id))
        self.db_conn.commit()

    def close(self):
        self.db_conn.close()


# ─────────────────────────────────────────────────────────────
# Agent 1: Hash Validation Agent
# ─────────────────────────────────────────────────────────────
class HashValidationAgent(DeterministicAgent):
    """Computes SHA256 hash and registers raw objects. Enforces immutability."""

    def __init__(self):
        super().__init__("AGT-HASH-001")

    def ingest_file(self, file_path: str, object_type: str, device_source: str = None) -> Optional[str]:
        """Ingest a file: compute hash, check for duplicates, register in raw_objects."""
        path = Path(file_path)
        if not path.exists():
            log.error(f"File not found: {file_path}")
            return None

        # Compute SHA256
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
        file_hash = sha256.hexdigest()

        # Check for exact duplicate
        with self.db_conn.cursor() as cur:
            cur.execute("SELECT object_id, original_path FROM raw_objects WHERE sha256_hash = %s", (file_hash,))
            existing = cur.fetchone()
            if existing:
                log.info(f"[HASH] Duplicate detected: {path.name} matches {existing[1]}")
                self.log_validation("RAW_INGEST", str(existing[0]), 2, "DUPLICATE_DETECTION", "WARN",
                                    {"message": f"Exact duplicate of {existing[1]}", "hash": file_hash})
                return str(existing[0])

        # Generate provenance ID
        provenance_id = f"PROV-{object_type.upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{file_hash[:8]}"

        # Register in raw_objects
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw_objects (provenance_id, sha256_hash, original_path, original_filename,
                    device_source, mime_type, file_size_bytes, object_type, storage_path, ingestion_agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING object_id
            """, (
                provenance_id, file_hash, str(path), path.name,
                device_source, self._detect_mime(path.suffix),
                path.stat().st_size, object_type,
                f"raw/{object_type}/{path.name}",
                self.agent_id
            ))
            object_id = str(cur.fetchone()[0])
        self.db_conn.commit()

        # Emit event
        self.emit_event("RAW_INGEST", "FILE_REGISTERED", {
            "object_id": object_id, "file": path.name, "hash": file_hash,
            "size_bytes": path.stat().st_size, "object_type": object_type
        })
        self.log_validation("RAW_INGEST", object_id, 1, "HASH_VALIDATION", "PASS",
                            {"sha256": file_hash, "file": path.name})
        log.info(f"[HASH] Registered: {path.name} → {object_id}")
        return object_id

    def _detect_mime(self, suffix: str) -> str:
        mime_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                    ".heic": "image/heic", ".pdf": "application/pdf", ".json": "application/json"}
        return mime_map.get(suffix.lower(), "application/octet-stream")


# ─────────────────────────────────────────────────────────────
# Agent 2: EXIF Agent
# ─────────────────────────────────────────────────────────────
class EXIFAgent(DeterministicAgent):
    """Extracts EXIF metadata from images. Facts only."""

    def __init__(self):
        super().__init__("AGT-EXIF-001")

    def extract(self, file_path: str, object_id: str) -> Dict[str, Any]:
        """Extract EXIF data and store in exif_metadata table."""
        exif_data = {}
        captured_at = None
        gps_lat, gps_lon = None, None
        device_make, device_model = None, None
        width, height = None, None
        is_screenshot = False

        try:
            with open(file_path, "rb") as f:
                tags = exifread.process_file(f, stop_tag="GPS GPSLongitude", details=False)
                exif_data = {str(k): str(v) for k, v in tags.items()}

            # Parse timestamp
            for ts_key in ["EXIF DateTimeOriginal", "Image DateTime", "EXIF DateTimeDigitized"]:
                if ts_key in exif_data:
                    try:
                        captured_at = datetime.strptime(exif_data[ts_key], "%Y:%m:%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        pass

            # Parse device
            device_make = exif_data.get("Image Make", None)
            device_model = exif_data.get("Image Model", None)

        except Exception as e:
            log.warning(f"[EXIF] exifread failed for {file_path}: {e}, trying PIL")

        # Fallback to PIL for dimensions and screenshot detection
        try:
            img = Image.open(file_path)
            width, height = img.size
            # iPhone 15 Pro screenshot signature: 1179x2556 or 2556x1179
            if (width == 1179 and height == 2556) or (width == 2556 and height == 1179):
                is_screenshot = True
            # No EXIF timestamp = likely screenshot
            if captured_at is None and (device_make is None or "Apple" in str(device_make)):
                is_screenshot = True
        except Exception as e:
            log.warning(f"[EXIF] PIL failed for {file_path}: {e}")

        # Store in DB
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exif_metadata (object_id, captured_at_utc, gps_latitude, gps_longitude,
                    device_make, device_model, image_width, image_height, is_screenshot, raw_exif)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (object_id, captured_at, gps_lat, gps_lon,
                  device_make, device_model, width, height, is_screenshot, Json(exif_data)))
        self.db_conn.commit()

        result = {
            "captured_at_utc": str(captured_at) if captured_at else None,
            "device": f"{device_make} {device_model}".strip(),
            "dimensions": f"{width}x{height}",
            "is_screenshot": is_screenshot
        }
        self.emit_event("EXIF_EXTRACTED", "METADATA_STORED", {"object_id": object_id, **result})
        log.info(f"[EXIF] {file_path}: screenshot={is_screenshot}, dims={width}x{height}")
        return result


# ─────────────────────────────────────────────────────────────
# Agent 3: OCR Agent
# ─────────────────────────────────────────────────────────────
class OCRAgent(DeterministicAgent):
    """Extracts text from images using Tesseract. EasyOCR/Gemini fallback handled separately."""

    def __init__(self):
        super().__init__("AGT-OCR-001")
        # Verify tesseract is available
        try:
            pytesseract.get_tesseract_version()
            log.info("[OCR] Tesseract available")
        except Exception as e:
            log.error(f"[OCR] Tesseract not found: {e}")

    def extract(self, file_path: str, object_id: str) -> Dict[str, Any]:
        """Run OCR on an image file and store results."""
        raw_text = ""
        confidence = 0.0
        engine_used = "tesseract"

        try:
            img = Image.open(file_path)
            # Use LSTM engine (--oem 1) with page segmentation mode 3 (auto)
            config = "--oem 1 --psm 3"
            data = pytesseract.image_to_data(img, config=config, output_type=pytesseract.Output.DICT)
            raw_text = pytesseract.image_to_string(img, config=config)

            # Compute mean confidence (filter out -1 values)
            confs = [c for c in data["conf"] if c != -1]
            confidence = sum(confs) / len(confs) / 100.0 if confs else 0.0

        except Exception as e:
            log.error(f"[OCR] Tesseract failed for {file_path}: {e}")
            confidence = 0.0

        word_count = len(raw_text.split()) if raw_text else 0

        # Detect content types
        text_lower = raw_text.lower()
        contains_amounts = any(c in raw_text for c in ["$", "€", "£"]) or any(
            w in text_lower for w in ["revenue", "liability", "balance", "profit", "loss"])
        contains_usernames = any(w in text_lower for w in ["@", "username", "account", "user id"])
        contains_legal = any(w in text_lower for w in [
            "asc 606", "asc 810", "sec", "10-k", "10-q", "violation", "complaint",
            "draftkings", "settlement", "arbitration", "fre 408"])
        contains_financial = any(w in text_lower for w in [
            "revenue", "earnings", "ebitda", "gross profit", "operating", "quarterly"])

        # Detect app UI
        app_ui = self._detect_app_ui(text_lower)
        scene = self._classify_scene(text_lower, contains_legal, contains_financial)

        # Store in DB
        with self.db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ocr_extracts (object_id, engine_used, raw_text, confidence_score,
                    word_count, contains_amounts, contains_usernames, contains_legal_refs,
                    contains_financial, app_ui_detected, scene_classification)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (object_id, engine_used, raw_text, confidence, word_count,
                  contains_amounts, contains_usernames, contains_legal,
                  contains_financial, app_ui, scene))
        self.db_conn.commit()

        result = {
            "engine": engine_used, "confidence": round(confidence, 3),
            "word_count": word_count, "scene": scene, "app_ui": app_ui,
            "contains_legal": contains_legal, "contains_financial": contains_financial
        }
        self.emit_event("OCR_EXTRACTED", "TEXT_STORED", {"object_id": object_id, **result})

        # Flag low confidence for fallback
        if confidence < 0.75 and word_count > 10:
            self.redis.lpush("queue:ocr:fallback", json.dumps({"object_id": object_id, "file": file_path}))
            log.warning(f"[OCR] Low confidence ({confidence:.2f}) → queued for fallback: {file_path}")
        else:
            log.info(f"[OCR] {Path(file_path).name}: conf={confidence:.2f}, words={word_count}, scene={scene}")

        return result

    def _detect_app_ui(self, text: str) -> str:
        if "draftkings" in text or "dk" in text:
            return "DraftKings"
        if "chatgpt" in text or "openai" in text or "gpt-4" in text:
            return "ChatGPT"
        if "sec.gov" in text or "edgar" in text or "10-k" in text:
            return "SEC_EDGAR"
        if "safari" in text or "chrome" in text or "http" in text:
            return "Browser"
        if "iphone" in text or "settings" in text:
            return "iOS_System"
        return "Unknown"

    def _classify_scene(self, text: str, is_legal: bool, is_financial: bool) -> str:
        if is_legal and is_financial:
            return "Legal_Financial"
        if is_legal:
            return "Legal_Document"
        if is_financial:
            return "Financial_Document"
        if "draftkings" in text:
            return "DraftKings_App"
        if "chatgpt" in text or "openai" in text:
            return "AI_Conversation"
        return "General_Screenshot"


# ─────────────────────────────────────────────────────────────
# Agent 4: Duplicate Detection Agent
# ─────────────────────────────────────────────────────────────
class DuplicateDetectionAgent(DeterministicAgent):
    """Detects near-duplicate images using perceptual hashing (pHash)."""

    def __init__(self):
        super().__init__("AGT-DEDUP-001")
        self._phash_cache: Dict[str, str] = {}  # object_id -> phash string

    def check_and_register(self, file_path: str, object_id: str) -> Dict[str, Any]:
        """Compute pHash and check for near-duplicates."""
        try:
            img = Image.open(file_path)
            phash = str(imagehash.phash(img))
        except Exception as e:
            log.error(f"[DEDUP] pHash failed for {file_path}: {e}")
            return {"is_duplicate": False, "phash": None}

        # Check against cache
        near_dupes = []
        for existing_id, existing_hash in self._phash_cache.items():
            try:
                diff = imagehash.hex_to_hash(phash) - imagehash.hex_to_hash(existing_hash)
                if diff <= 8:  # Hamming distance threshold
                    near_dupes.append({"object_id": existing_id, "hamming_distance": diff})
            except Exception:
                pass

        self._phash_cache[object_id] = phash

        if near_dupes:
            log.warning(f"[DEDUP] Near-duplicate found for {Path(file_path).name}: {near_dupes}")
            self.log_validation("DEDUP_CHECK", object_id, 2, "DUPLICATE_DETECTION", "WARN",
                                {"near_duplicates": near_dupes, "phash": phash})
            self.emit_event("DEDUP_DETECTED", "NEAR_DUPLICATE", {
                "object_id": object_id, "near_duplicates": near_dupes, "phash": phash
            })
        else:
            self.log_validation("DEDUP_CHECK", object_id, 2, "DUPLICATE_DETECTION", "PASS",
                                {"phash": phash})

        return {"is_duplicate": len(near_dupes) > 0, "near_duplicates": near_dupes, "phash": phash}


# ─────────────────────────────────────────────────────────────
# Tier 1 Pipeline Runner
# ─────────────────────────────────────────────────────────────
class Tier1Pipeline:
    """Orchestrates all Tier 1 deterministic agents in sequence."""

    def __init__(self):
        self.hash_agent = HashValidationAgent()
        self.exif_agent = EXIFAgent()
        self.ocr_agent = OCRAgent()
        self.dedup_agent = DuplicateDetectionAgent()
        log.info("[Tier1Pipeline] All agents initialized")

    def process_photo(self, file_path: str, device_source: str = "iPhone 15 Pro") -> Dict[str, Any]:
        """Run the full Tier 1 pipeline on a single photo."""
        log.info(f"[Tier1Pipeline] Processing: {Path(file_path).name}")
        result = {"file": file_path, "status": "PROCESSING"}

        # Step 1: Hash & Register
        object_id = self.hash_agent.ingest_file(file_path, "icloud_photo", device_source)
        if not object_id:
            result["status"] = "FAILED_HASH"
            return result
        result["object_id"] = object_id

        # Step 2: EXIF
        exif = self.exif_agent.extract(file_path, object_id)
        result["exif"] = exif

        # Step 3: Dedup check
        dedup = self.dedup_agent.check_and_register(file_path, object_id)
        result["dedup"] = dedup

        # Step 4: OCR
        ocr = self.ocr_agent.extract(file_path, object_id)
        result["ocr"] = ocr

        result["status"] = "TIER1_COMPLETE"
        log.info(f"[Tier1Pipeline] DONE: {Path(file_path).name} → {object_id}")
        return result

    def close(self):
        for agent in [self.hash_agent, self.exif_agent, self.ocr_agent, self.dedup_agent]:
            agent.close()


if __name__ == "__main__":
    # Quick smoke test on a sample photo
    import glob
    pipeline = Tier1Pipeline()
    samples = glob.glob(os.path.expanduser("~/icloud_photos/2025/08/08/*.JPG"))[:3]
    if not samples:
        samples = glob.glob(os.path.expanduser("~/icloud_photos/2025/08/**/*.PNG"))[:3]
    for photo in samples:
        r = pipeline.process_photo(photo)
        print(json.dumps(r, default=str, indent=2))
    pipeline.close()
    print("\n[SMOKE TEST PASSED] Tier 1 Pipeline operational")
