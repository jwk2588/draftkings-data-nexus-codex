"""
DraftKings Dynamic Intelligence Stack — Data Lake Seeder
Phase 3: iCloud Photos Data Dictionary + Metadata Dictionary
         ChatGPT Data Dictionary + Metadata Dictionary
         DuckDB Querying Engine
         Unified Search Index Population

Classification: ATTORNEY WORK PRODUCT | FRE 408 | FRE 502(d) PROTECTED
"""

import os
import json
import sqlite3
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("/home/ubuntu/HiveMind/logs/audit/data_lake_seeder.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("DataLakeSeeder")

PG_DSN = "host=localhost dbname=draftkings_hivemind user=hivemind password=hivemind_secure_2026"

# ── iCloud Photo Data Dictionary ────────────────────────────────────────────────
PHOTO_DATA_DICTIONARY = [
    # Core identifiers
    ("file_path", "File Path", "TEXT", "Absolute path to the photo on the cloud computer filesystem", "photo_metadata", "file_path", "/home/ubuntu/icloud_photos/2025/08/08/Exhibit_F.JPG", False, True, ["GR-011"], "v1.0"),
    ("file_name", "File Name", "TEXT", "Original filename as downloaded from iCloud", "photo_metadata", "file_name", "IMG_1234.PNG", False, False, [], "v1.0"),
    ("sha256_hash", "SHA-256 Hash", "TEXT(64)", "64-character hex SHA-256 hash for evidence integrity (Phase0 §X.A three-location rule)", "photo_metadata", "sha256_hash", "a3f2c1...", False, True, ["GR-011"], "v1.0"),
    ("phash", "Perceptual Hash", "TEXT", "Perceptual hash (pHash) for near-duplicate detection and deduplication", "photo_metadata", "phash", "f8c4a2b1...", False, False, [], "v1.0"),
    ("file_size_bytes", "File Size (Bytes)", "BIGINT", "Raw file size in bytes", "photo_metadata", "file_size_bytes", "2457600", False, False, [], "v1.0"),
    ("file_format", "File Format", "TEXT", "Image format: PNG, JPEG, HEIC, MOV", "photo_metadata", "file_format", "PNG", False, False, [], "v1.0"),
    ("width_px", "Width (px)", "INTEGER", "Image width in pixels. iPhone 15 Pro standard: 1179px", "photo_metadata", "width_px", "1179", False, False, [], "v1.0"),
    ("height_px", "Height (px)", "INTEGER", "Image height in pixels. iPhone 15 Pro standard: 2556px", "photo_metadata", "height_px", "2556", False, False, [], "v1.0"),
    # Temporal metadata
    ("timestamp_utc", "Timestamp (UTC)", "TIMESTAMPTZ", "EXIF DateTimeOriginal converted to UTC — primary temporal anchor for evidence timeline", "photo_metadata", "timestamp_utc", "2025-08-08T14:30:00Z", False, True, ["GR-011", "GR-004"], "v1.0"),
    ("timestamp_local", "Timestamp (Local)", "TEXT", "Original local time string from EXIF before UTC conversion", "photo_metadata", "timestamp_local", "2025:08:08 10:30:00", False, False, [], "v1.0"),
    ("icloud_date_path", "iCloud Date Path", "TEXT", "Directory path segment YYYY/MM/DD from iCloud download structure", "photo_metadata", "icloud_date_path", "2025/08/08", False, False, [], "v1.0"),
    # GPS metadata
    ("gps_latitude", "GPS Latitude", "FLOAT", "GPS latitude decimal degrees. Null if location services disabled", "photo_metadata", "gps_latitude", "42.3314", False, True, [], "v1.0"),
    ("gps_longitude", "GPS Longitude", "FLOAT", "GPS longitude decimal degrees. Null if location services disabled", "photo_metadata", "gps_longitude", "-83.0458", False, True, [], "v1.0"),
    ("gps_altitude_m", "GPS Altitude (m)", "FLOAT", "GPS altitude in meters above sea level", "photo_metadata", "gps_altitude_m", "180.5", False, False, [], "v1.0"),
    # Device metadata
    ("device_make", "Device Make", "TEXT", "Camera manufacturer. Always 'Apple' for iCloud photos", "photo_metadata", "device_make", "Apple", False, False, [], "v1.0"),
    ("device_model", "Device Model", "TEXT", "iPhone model. Corpus is 98.8% iPhone 15 Pro", "photo_metadata", "device_model", "iPhone 15 Pro", False, False, [], "v1.0"),
    ("device_software", "iOS Version", "TEXT", "iOS version at time of capture", "photo_metadata", "device_software", "iOS 18.3.2", False, False, [], "v1.0"),
    # Camera metadata
    ("focal_length_mm", "Focal Length (mm)", "FLOAT", "Lens focal length in millimeters", "photo_metadata", "focal_length_mm", "6.765", False, False, [], "v1.0"),
    ("aperture", "Aperture (f/)", "FLOAT", "Lens aperture f-number", "photo_metadata", "aperture", "1.78", False, False, [], "v1.0"),
    ("iso_speed", "ISO Speed", "INTEGER", "Camera ISO sensitivity setting", "photo_metadata", "iso_speed", "100", False, False, [], "v1.0"),
    ("exposure_time", "Exposure Time", "TEXT", "Shutter speed as fraction string", "photo_metadata", "exposure_time", "1/1000", False, False, [], "v1.0"),
    # OCR fields
    ("ocr_text", "OCR Extracted Text", "TEXT", "Full text extracted from photo via Tesseract/EasyOCR/Gemini Vision", "photo_ocr", "ocr_text", "Dynasty Rewards Tier: Sapphire...", False, True, ["GR-001","GR-004","GR-009"], "v1.0"),
    ("ocr_confidence", "OCR Confidence", "FLOAT", "OCR engine confidence score 0.0-1.0", "photo_ocr", "ocr_confidence", "0.92", False, False, [], "v1.0"),
    ("ocr_engine", "OCR Engine", "TEXT", "Engine used: tesseract, easyocr, gemini_vision", "photo_ocr", "ocr_engine", "tesseract", False, False, [], "v1.0"),
    ("has_legal_content", "Has Legal Content", "BOOLEAN", "True if OCR text contains legal terms, case citations, or exhibit markers", "photo_ocr", "has_legal_content", "true", False, True, ["GR-011"], "v1.0"),
    ("has_financial_data", "Has Financial Data", "BOOLEAN", "True if OCR text contains dollar amounts, percentages, or financial metrics", "photo_ocr", "has_financial_data", "true", False, True, ["GR-004","GR-010"], "v1.0"),
    ("has_dk_content", "Has DraftKings Content", "BOOLEAN", "True if OCR text contains DraftKings branding, Dynasty Rewards, or DK-specific terms", "photo_ocr", "has_dk_content", "true", False, True, ["GR-009"], "v1.0"),
    # Domain tags
    ("gr_node_links", "GR Node Links", "TEXT[]", "Array of GR-NNN node IDs detected in this photo's content", "photo_domain_tags", "gr_node_links", "{GR-001,GR-004}", False, True, [], "v1.0"),
    ("ev_links", "EV Item Links", "TEXT[]", "Array of EV-NNN evidence items this photo corroborates or evidences", "photo_domain_tags", "ev_links", "{EV-001,EV-045}", False, True, ["GR-011"], "v1.0"),
    ("content_type", "Content Type", "TEXT", "Classified content type: LEGAL_EXHIBIT, DK_SCREENSHOT, CHATGPT_SCREENSHOT, FINANCIAL_CHART, OTHER", "photo_domain_tags", "content_type", "LEGAL_EXHIBIT", False, True, [], "v1.0"),
    ("ingest_status", "Ingest Status", "TEXT", "Pipeline status: PENDING, OCR_COMPLETE, TAGGED, INDEXED, COMPLETE", "photo_metadata", "ingest_status", "COMPLETE", False, False, [], "v1.0"),
]

# ── iCloud Photo Metadata Dictionary ───────────────────────────────────────────
PHOTO_METADATA_DICTIONARY = [
    # Temporal metadata
    ("photo_year", "TEMPORAL", "Calendar year extracted from EXIF timestamp. Used for evidence timeline construction.", "EXIF DateTimeOriginal year component", "INTEGER", "2025", ["GR-011","GR-004"], [], "photo_year_anchor", "v1.0"),
    ("photo_month", "TEMPORAL", "Calendar month (1-12) from EXIF timestamp.", "EXIF DateTimeOriginal month component", "INTEGER", "8", ["GR-011"], [], "photo_month_anchor", "v1.0"),
    ("photo_day", "TEMPORAL", "Calendar day from EXIF timestamp.", "EXIF DateTimeOriginal day component", "INTEGER", "8", [], [], "photo_day_anchor", "v1.0"),
    ("photo_hour", "TEMPORAL", "Hour of day (0-23) from EXIF timestamp. Used to establish time-of-capture context.", "EXIF DateTimeOriginal hour component", "INTEGER", "14", [], [], "photo_hour_anchor", "v1.0"),
    ("icloud_batch_period", "TEMPORAL", "Monthly batch period YYYY-MM for bulk analysis grouping.", "Derived from icloud_date_path", "TEXT", "2025-08", ["GR-011"], [], "batch_period_anchor", "v1.0"),
    # Semantic metadata
    ("legal_exhibit_flag", "SEMANTIC", "Binary flag: photo contains legal exhibit markers (Exhibit A-Z, EV-NNN, etc.)", "OCR pattern matching on ocr_text", "BOOLEAN", "true", ["GR-011"], ["EV-001"], "legal_exhibit_anchor", "v1.0"),
    ("dk_rewards_tier", "SEMANTIC", "DraftKings Dynasty Rewards tier detected in screenshot (Bronze/Silver/Gold/Platinum/Sapphire/Diamond/Titanium)", "OCR extraction from DK app screenshots", "TEXT", "Sapphire", ["GR-001","GR-009"], [], "dk_tier_anchor", "v1.0"),
    ("financial_amount_usd", "SEMANTIC", "Dollar amount extracted from financial charts or screenshots", "OCR regex extraction: \\$[\\d,]+", "FLOAT", "1560000000.00", ["GR-004","GR-010"], [], "financial_amount_anchor", "v1.0"),
    ("chatgpt_model_ref", "SEMANTIC", "ChatGPT model name visible in screenshot (gpt-4, gpt-4o, o3-mini-high, etc.)", "OCR pattern matching", "TEXT", "gpt-4o", [], [], "chatgpt_model_anchor", "v1.0"),
    ("sec_filing_ref", "SEMANTIC", "SEC filing reference detected (10-K, 8-K, DEF 14A, etc.)", "OCR pattern matching on sec_filing_patterns", "TEXT", "10-K", ["GR-010"], [], "sec_filing_anchor", "v1.0"),
    # Legal metadata
    ("fre_408_protected", "LEGAL", "Photo was taken in context of settlement discussions — FRE 408 protection applies", "Manual tag or KIMI classification", "BOOLEAN", "false", ["GR-008"], [], "fre408_anchor", "v1.0"),
    ("chain_of_custody_hash", "LEGAL", "SHA-256 hash at time of evidence lockdown per Phase0 §X.A three-location rule", "Computed at ingest, stored immutably", "TEXT(64)", "a3f2c1...", ["GR-011"], [], "coc_hash_anchor", "v1.0"),
    ("exhibit_assignment", "LEGAL", "Formal exhibit code assigned (Exhibit A, B, C, T, U, V, W, Supplemental)", "Manual assignment by counsel", "TEXT", "Exhibit_F", ["GR-011"], [], "exhibit_anchor", "v1.0"),
    # Technical metadata
    ("device_fingerprint", "TECHNICAL", "Composite device fingerprint: make+model+iOS+lens for forensic device attribution", "Concatenation of EXIF device fields", "TEXT", "Apple|iPhone 15 Pro|iOS 18.3.2|6.765mm", [], [], "device_fp_anchor", "v1.0"),
    ("ocr_word_density", "TECHNICAL", "Words per 1000 pixels — high density indicates text-heavy screenshot vs photo", "word_count / (width_px * height_px / 1000)", "FLOAT", "12.4", [], [], "word_density_anchor", "v1.0"),
    ("duplicate_cluster_id", "TECHNICAL", "pHash cluster ID grouping near-duplicate photos (Hamming distance <= 8)", "pHash clustering via imagehash library", "TEXT", "cluster_0042", [], [], "dedup_cluster_anchor", "v1.0"),
    ("ingest_pipeline_version", "TECHNICAL", "Version of the ingest pipeline that processed this photo", "Set at ingest time", "TEXT", "v2.0", [], [], "pipeline_version_anchor", "v1.0"),
]

# ── ChatGPT Data Dictionary ─────────────────────────────────────────────────────
CHATGPT_DATA_DICTIONARY = [
    # Conversations table
    ("conv_id", "Conversation ID", "TEXT", "Unique conversation identifier from ChatGPT export (UUID format)", "chatgpt_conversations", "conv_id", "abc123-def456", False, True, [], [], ["IDENTIFIER"], "v1.0"),
    ("title", "Conversation Title", "TEXT", "User-assigned or auto-generated conversation title", "chatgpt_conversations", "title", "DraftKings VIE Analysis", False, True, ["GR-001","GR-004"], [], ["SEMANTIC_LABEL"], "v1.0"),
    ("create_time", "Create Time", "REAL", "Unix timestamp of conversation creation", "chatgpt_conversations", "create_time", "1722470400.0", False, True, ["GR-011"], [], ["TEMPORAL"], "v1.0"),
    ("update_time", "Update Time", "REAL", "Unix timestamp of last message in conversation", "chatgpt_conversations", "update_time", "1722556800.0", False, False, [], [], ["TEMPORAL"], "v1.0"),
    ("model_slug", "Model Slug", "TEXT", "ChatGPT model used: gpt-4, gpt-4o, o3-mini-high, research, auto", "chatgpt_conversations", "model_slug", "gpt-4o", False, True, [], [], ["TECHNICAL"], "v1.0"),
    ("message_count", "Message Count", "INTEGER", "Total number of messages in the conversation", "chatgpt_conversations", "message_count", "47", False, False, [], [], ["METRIC"], "v1.0"),
    # Messages table
    ("msg_id", "Message ID", "TEXT", "Unique message identifier", "chatgpt_messages", "msg_id", "msg_abc123", False, True, [], [], ["IDENTIFIER"], "v1.0"),
    ("role", "Role", "TEXT", "Message author role: user, assistant, system, tool", "chatgpt_messages", "role", "assistant", False, True, [], [], ["SEMANTIC_LABEL"], "v1.0"),
    ("content", "Content", "TEXT", "Full message text content", "chatgpt_messages", "content", "The VIE consolidation analysis shows...", True, False, ["GR-001","GR-004","GR-010"], [], ["CORPUS_TEXT"], "v1.0"),
    ("author_name", "Author Name", "TEXT", "Author display name (null for user/assistant, tool name for tool calls)", "chatgpt_messages", "author_name", "browser", True, False, [], [], ["IDENTIFIER"], "v1.0"),
    ("create_time_msg", "Message Create Time", "REAL", "Unix timestamp of individual message creation", "chatgpt_messages", "create_time", "1722470400.5", True, False, [], [], ["TEMPORAL"], "v1.0"),
    # Cross-stitching maps
    ("map_id", "Cross-Stitch Map ID", "TEXT", "Unique cross-stitch mapping identifier", "cross_stitching_maps", "map_id", "cs_001", False, True, [], [], ["IDENTIFIER"], "v1.0"),
    ("src_id", "Source ID", "TEXT", "Source entity ID (message, conversation, or photo)", "cross_stitching_maps", "src_id", "msg_abc123", False, True, [], [], ["RELATIONSHIP"], "v1.0"),
    ("dst_id", "Destination ID", "TEXT", "Destination entity ID (GR node, EV item, or other entity)", "cross_stitching_maps", "dst_id", "GR-001", False, True, [], [], ["RELATIONSHIP"], "v1.0"),
    ("map_type", "Map Type", "TEXT", "Type of cross-stitch: MSG_GR, TAG_INJECT, PHOTO_GR, EV_MSG", "cross_stitching_maps", "map_type", "MSG_GR", False, True, [], [], ["CLASSIFICATION"], "v1.0"),
    ("weight", "Weight", "REAL", "Strength of the cross-stitch relationship 0.0-1.0", "cross_stitching_maps", "weight", "0.85", False, False, [], [], ["METRIC"], "v1.0"),
]

# ── ChatGPT Metadata Dictionary ─────────────────────────────────────────────────
CHATGPT_METADATA_DICTIONARY = [
    # Temporal metadata
    ("conv_date_utc", "TEMPORAL", "Conversation creation date in UTC ISO format", "FROM_UNIXTIME(create_time) converted to UTC", "DATE", "2025-08-01", ["GR-011","GR-004"], [], "conv_date_anchor", "v1.0"),
    ("conv_duration_days", "TEMPORAL", "Duration of conversation in days (update_time - create_time)", "Derived: (update_time - create_time) / 86400", "FLOAT", "3.5", [], [], "conv_duration_anchor", "v1.0"),
    ("message_cadence", "TEMPORAL", "Average messages per hour during active conversation periods", "Derived from message timestamps", "FLOAT", "4.2", [], [], "cadence_anchor", "v1.0"),
    ("peak_activity_month", "TEMPORAL", "Month with highest message volume in the corpus", "Aggregation over create_time", "TEXT", "2025-08", ["GR-011"], [], "peak_month_anchor", "v1.0"),
    # Semantic metadata
    ("gr_node_primary", "SEMANTIC", "Primary GR node classification for this conversation (highest cross-stitch weight)", "Derived from cross_stitching_maps max(weight) GROUP BY dst_id", "TEXT", "GR-010", ["GR-010"], [], "gr_primary_anchor", "v1.0"),
    ("gr_node_secondary", "SEMANTIC", "Secondary GR node(s) with significant cross-stitch presence", "Derived from cross_stitching_maps WHERE weight > 0.5", "TEXT[]", "{GR-001,GR-004}", [], [], "gr_secondary_anchor", "v1.0"),
    ("legal_theory_density", "SEMANTIC", "Ratio of messages containing legal theory keywords to total messages", "Keyword matching on content field", "FLOAT", "0.73", ["GR-002","GR-003"], [], "legal_density_anchor", "v1.0"),
    ("financial_figure_count", "SEMANTIC", "Count of distinct dollar amounts mentioned in conversation", "Regex extraction: \\$[\\d,\\.]+[BMK]?", "INTEGER", "12", ["GR-004","GR-010"], [], "financial_count_anchor", "v1.0"),
    ("model_capability_tier", "SEMANTIC", "Model capability tier: REASONING (o3-mini-high), RESEARCH (research), STANDARD (gpt-4o), LEGACY (gpt-4)", "Derived from model_slug", "TEXT", "REASONING", [], [], "model_tier_anchor", "v1.0"),
    # Legal metadata
    ("ev_citation_count", "LEGAL", "Count of EV-NNN citations in conversation messages", "Regex: EV-\\d{3}", "INTEGER", "7", ["GR-011"], [], "ev_citation_anchor", "v1.0"),
    ("fre_408_context", "LEGAL", "True if conversation contains settlement negotiation language triggering FRE 408 protection", "Keyword: settlement, offer, compromise, mediation", "BOOLEAN", "false", ["GR-008"], [], "fre408_chat_anchor", "v1.0"),
    ("attorney_work_product", "LEGAL", "True if conversation contains legal strategy, privileged analysis, or work product", "Keyword + KIMI classification", "BOOLEAN", "true", [], [], "awp_anchor", "v1.0"),
    # Technical metadata
    ("token_estimate", "TECHNICAL", "Estimated token count for the full conversation (content length / 4)", "Derived: SUM(LENGTH(content)) / 4", "INTEGER", "8420", [], [], "token_est_anchor", "v1.0"),
    ("multistring_stage_count", "TECHNICAL", "Number of multistring staging rows generated from this conversation", "COUNT from multistring_staging WHERE conv_id = ?", "INTEGER", "12", [], [], "multistring_anchor", "v1.0"),
    ("cross_stitch_density", "TECHNICAL", "Cross-stitch connections per message (23,856 total / 40,250 messages = 0.59)", "Derived: cross_stitch_count / message_count", "FLOAT", "0.59", [], [], "stitch_density_anchor", "v1.0"),
]


def seed_photo_data_dictionary(conn):
    """Seed the photo_data_dictionary table."""
    log.info("Seeding photo_data_dictionary...")
    with conn.cursor() as cur:
        for row in PHOTO_DATA_DICTIONARY:
            cur.execute("""
                INSERT INTO photo_data_dictionary
                    (field_name, display_name, data_type, description, source_table,
                     source_column, example_value, is_pii, is_legal_sensitive,
                     gr_node_relevance, version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (field_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description
            """, row)
    conn.commit()
    log.info(f"Seeded {len(PHOTO_DATA_DICTIONARY)} photo data dictionary entries")


def seed_photo_metadata_dictionary(conn):
    """Seed photo metadata dictionary as chatgpt_metadata_dictionary entries (photo variant)."""
    log.info("Seeding photo metadata dictionary into chatgpt_metadata_dictionary...")
    with conn.cursor() as cur:
        for row in PHOTO_METADATA_DICTIONARY:
            (meta_key, meta_category, description, extraction_method, data_type,
             example_value, gr_node_links, ev_links, yaml_anchor, version) = row
            # Store photo metadata in chatgpt_metadata_dictionary with photo_ prefix
            cur.execute("""
                INSERT INTO chatgpt_metadata_dictionary
                    (meta_key, meta_category, description, extraction_method,
                     data_type, example_value, gr_node_links, ev_links, yaml_anchor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (meta_key) DO UPDATE SET
                    description = EXCLUDED.description
            """, (f"photo_{meta_key}", meta_category, description, extraction_method,
                  data_type, example_value, gr_node_links, ev_links, yaml_anchor))
    conn.commit()
    log.info(f"Seeded {len(PHOTO_METADATA_DICTIONARY)} photo metadata dictionary entries")


def seed_chatgpt_data_dictionary(conn):
    """Seed the chatgpt_data_dictionary table."""
    log.info("Seeding chatgpt_data_dictionary...")
    with conn.cursor() as cur:
        for row in CHATGPT_DATA_DICTIONARY:
            (field_name, display_name, data_type, description, source_table,
             source_column, example_value, is_nullable, is_indexed,
             gr_node_relevance, ev_relevance, semantic_tags, version) = row
            cur.execute("""
                INSERT INTO chatgpt_data_dictionary
                    (field_name, display_name, data_type, description, source_table,
                     source_column, example_value, is_nullable, is_indexed,
                     gr_node_relevance, ev_relevance, semantic_tags, version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (field_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description
            """, row)
    conn.commit()
    log.info(f"Seeded {len(CHATGPT_DATA_DICTIONARY)} ChatGPT data dictionary entries")


def seed_chatgpt_metadata_dictionary(conn):
    """Seed the chatgpt_metadata_dictionary table."""
    log.info("Seeding chatgpt_metadata_dictionary...")
    with conn.cursor() as cur:
        for row in CHATGPT_METADATA_DICTIONARY:
            (meta_key, meta_category, description, extraction_method, data_type,
             example_value, gr_node_links, ev_links, yaml_anchor, version) = row
            cur.execute("""
                INSERT INTO chatgpt_metadata_dictionary
                    (meta_key, meta_category, description, extraction_method,
                     data_type, example_value, gr_node_links, ev_links, yaml_anchor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (meta_key) DO UPDATE SET
                    description = EXCLUDED.description
            """, (meta_key, meta_category, description, extraction_method,
                  data_type, example_value, gr_node_links, ev_links, yaml_anchor))
    conn.commit()
    log.info(f"Seeded {len(CHATGPT_METADATA_DICTIONARY)} ChatGPT metadata dictionary entries")


def populate_unified_search_index(conn):
    """Populate the unified search index from ChatGPT messages and existing synthesis results."""
    log.info("Populating unified search index from ChatGPT corpus...")
    chatgpt_db = "/home/ubuntu/DraftKingsDB/db/master.db"

    try:
        sqlite_conn = sqlite3.connect(chatgpt_db)
        sqlite_conn.row_factory = sqlite3.Row
        cur_sqlite = sqlite_conn.cursor()

        # Get all messages with GR node links
        cur_sqlite.execute("""
            SELECT m.msg_id, m.content, m.role, m.create_time,
                   GROUP_CONCAT(cs.dst_id) as gr_nodes
            FROM chatgpt_messages m
            LEFT JOIN cross_stitching_maps cs ON m.msg_id = cs.src_id
            WHERE m.content IS NOT NULL AND length(m.content) > 30
            GROUP BY m.msg_id
            LIMIT 5000
        """)
        rows = cur_sqlite.fetchall()
        sqlite_conn.close()

        batch = []
        for row in rows:
            gr_nodes = list(set(row["gr_nodes"].split(",") if row["gr_nodes"] else []))
            ts = None
            if row["create_time"]:
                try:
                    from datetime import datetime
                    ts = datetime.fromtimestamp(float(row["create_time"]), tz=timezone.utc)
                except Exception:
                    pass

            batch.append((
                "CHATGPT_MSG",
                row["msg_id"],
                row["content"][:2000],  # truncate for FTS
                gr_nodes,
                [],  # ev_links populated later
                [],  # predicate_tags
                ts,
                0.7
            ))

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO unified_search_index
                    (source_type, source_id, content_text, gr_node_links,
                     ev_links, predicate_tags, timestamp_utc, confidence)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, batch, template="(%s, %s, %s, %s, %s, %s, %s, %s)")
        conn.commit()
        log.info(f"Indexed {len(batch)} ChatGPT messages in unified search index")

    except Exception as e:
        log.error(f"Failed to populate unified search index: {e}")


def install_duckdb_query_engine():
    """Install DuckDB and create the query engine wrapper."""
    import subprocess
    result = subprocess.run(
        ["pip3", "install", "--break-system-packages", "duckdb"],
        capture_output=True, text=True, timeout=120
    )
    if result.returncode == 0:
        log.info("DuckDB installed successfully")
    else:
        log.warning(f"DuckDB install: {result.stderr[:200]}")


def main():
    log.info("=" * 60)
    log.info("Data Lake Seeder — Phase 3 Deployment")
    log.info("=" * 60)

    conn = psycopg2.connect(PG_DSN)

    # 1. Seed all dictionaries
    seed_photo_data_dictionary(conn)
    seed_photo_metadata_dictionary(conn)
    seed_chatgpt_data_dictionary(conn)
    seed_chatgpt_metadata_dictionary(conn)

    # 2. Populate unified search index
    populate_unified_search_index(conn)

    # 3. Install DuckDB
    install_duckdb_query_engine()

    # 4. Final counts
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM photo_data_dictionary")
        photo_dd = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM chatgpt_data_dictionary")
        chat_dd = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM chatgpt_metadata_dictionary")
        chat_md = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM unified_search_index")
        search_idx = cur.fetchone()[0]

    conn.close()

    log.info("=" * 60)
    log.info(f"Phase 3 Complete:")
    log.info(f"  Photo Data Dictionary:     {photo_dd} fields")
    log.info(f"  ChatGPT Data Dictionary:   {chat_dd} fields")
    log.info(f"  ChatGPT Metadata Dict:     {chat_md} entries")
    log.info(f"  Unified Search Index:      {search_idx} documents")
    log.info("=" * 60)

    return {
        "photo_data_dictionary": photo_dd,
        "chatgpt_data_dictionary": chat_dd,
        "chatgpt_metadata_dictionary": chat_md,
        "unified_search_index": search_idx
    }


if __name__ == "__main__":
    main()
