-- ============================================================
-- DraftKings Dynamic Intelligence Stack — Schema v2.0
-- Grounded in: MasterMetadataDictionaryEVCrosswalk.xlsx (v54)
--              Phase0EvidenceLockdownProtocol.docx
--              MasterMetadataDictionaryEVCrosswalkCompanion.docx
-- Classification: ATTORNEY WORK PRODUCT | FRE 408 | FRE 502(d)
-- Hard Rule: ADDITIVE ONLY — never renumber EV-NNN or GR-NNN IDs
-- ============================================================

-- ============================================================
-- SECTION 1: CONTROLLED VOCABULARY TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS predicate_vocab (
    p_id        TEXT PRIMARY KEY,          -- e.g. P-01, P-02
    label       TEXT NOT NULL,             -- e.g. "Revenue Recognition"
    category    TEXT,                      -- hierarchical parent category
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS domain_vocab (
    d_id        TEXT PRIMARY KEY,          -- e.g. D-01
    label       TEXT NOT NULL,             -- e.g. "Nexus Platform"
    description TEXT,
    is_secondary BOOLEAN DEFAULT TRUE,     -- domain tags are always secondary
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pillar_registry (
    pillar_id   INTEGER PRIMARY KEY,       -- 1-25
    label       TEXT NOT NULL,
    description TEXT,
    version     TEXT DEFAULT 'v44',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS engine_registry (
    engine_id   INTEGER PRIMARY KEY,       -- 1-6
    label       TEXT NOT NULL,
    description TEXT,
    escalation_level INTEGER,             -- 1=technical violations, 6=contract voidance
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS levee_registry (
    levee_id    INTEGER PRIMARY KEY,       -- 1-38
    label       TEXT NOT NULL,
    description TEXT,
    pillar_refs INTEGER[],                 -- cross-ref to pillar_registry
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS exhibit_registry (
    exhibit_code TEXT PRIMARY KEY,         -- A, B, C, D, T, U, V, W, Supplemental
    label        TEXT NOT NULL,
    description  TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 2: EV CROSSWALK — MASTER EVIDENCE REGISTER
-- Ingestion Contract Rules C-01 through C-12 enforced here
-- ============================================================

CREATE TYPE ev_weight_class AS ENUM ('HIGHEST', 'HIGH', 'MEDIUM', 'LOW');
CREATE TYPE ev_confidence AS ENUM ('DOCUMENTED', 'INFERRED', 'ILLUSTRATIVE');
CREATE TYPE ev_bucket AS ENUM ('A', 'B', 'C');
CREATE TYPE ev_cybersec_tag AS ENUM (
    'NONE', 'HERMALYN_DELETION', 'FIREBASE_EXFIL',
    'DISCORD_SURVEILLANCE', 'APPLE_TRACKING', 'PLATFORM_REMOVAL',
    'WIRETAP_ACT', 'CFAA_VIOLATION', 'SPOLIATION'
);

CREATE TABLE IF NOT EXISTS ev_register (
    ev_id               TEXT PRIMARY KEY,   -- EV-NNN or EV-T1-NN etc. NEVER renumber
    description         TEXT NOT NULL,
    weight_class        ev_weight_class,
    exhibit_class       TEXT,               -- Exhibit / Subpoena / Register / Forensic
    predicate_tags      TEXT[],             -- FK refs to predicate_vocab.p_id
    domain_tags         TEXT[],             -- FK refs to domain_vocab.d_id
    pillar_refs         INTEGER[],          -- FK refs to pillar_registry.pillar_id
    engine_refs         INTEGER[],          -- FK refs to engine_registry.engine_id
    levee_refs          INTEGER[],          -- FK refs to levee_registry.levee_id
    exhibit_refs        TEXT[],             -- FK refs to exhibit_registry.exhibit_code
    gr_node_links       TEXT[],             -- GR-NNN cross-references
    sb_links            TEXT[],             -- SB-NN Silver Bullet cross-references
    source_file         TEXT,
    acquisition_date    TIMESTAMPTZ,
    year_primary        INTEGER,            -- V-14: must be <= current year
    sha256_hash         TEXT,               -- 64-char hex; V-10
    sha512_hash         TEXT,               -- 128-char hex; V-11
    sealed              BOOLEAN DEFAULT FALSE,  -- V-12: if TRUE, hashes required
    notarized           BOOLEAN DEFAULT FALSE,  -- V-13: if TRUE, sealed must be TRUE
    bucket              ev_bucket,          -- A=Documented Harm, B=Audit, C=CFAA
    confidence          ev_confidence DEFAULT 'INFERRED',
    cybersec_tag        ev_cybersec_tag DEFAULT 'NONE',
    storage_location_1  TEXT,              -- Three-location rule (Phase0 §X.A)
    storage_location_2  TEXT,
    storage_location_3  TEXT,
    chain_event_log     JSONB DEFAULT '[]', -- C-09: append-only chain event log
    fre_408_marked      BOOLEAN DEFAULT FALSE,
    notes               TEXT,
    version_added       TEXT DEFAULT 'v54',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    -- Ingestion Contract: C-01 READ-ONLY enforcement via trigger
    CONSTRAINT ev_id_pattern CHECK (
        ev_id ~ '^EV-\d{3}$' OR
        ev_id ~ '^EV-(T1|T2|BM|OR|LR)-\d{2}$'
    ),
    CONSTRAINT year_bounds CHECK (year_primary IS NULL OR year_primary <= EXTRACT(YEAR FROM NOW())),
    CONSTRAINT sealed_requires_hash CHECK (
        NOT sealed OR (sha256_hash IS NOT NULL AND length(sha256_hash) = 64)
    ),
    CONSTRAINT notarized_requires_sealed CHECK (NOT notarized OR sealed)
);

-- Immutability trigger: sealed EV items cannot be modified
CREATE OR REPLACE FUNCTION enforce_ev_immutability()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.sealed = TRUE THEN
        RAISE EXCEPTION 'C-01 VIOLATION: Sealed EV item % is read-only. Ingestion contract breach.', OLD.ev_id;
    END IF;
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ev_immutability_trigger ON ev_register;
CREATE TRIGGER ev_immutability_trigger
    BEFORE UPDATE ON ev_register
    FOR EACH ROW EXECUTE FUNCTION enforce_ev_immutability();

-- Chain event log append trigger (C-09)
CREATE OR REPLACE FUNCTION log_ev_chain_event()
RETURNS TRIGGER AS $$
DECLARE
    event_entry JSONB;
BEGIN
    event_entry = jsonb_build_object(
        'timestamp', NOW(),
        'ev_id', NEW.ev_id,
        'action', TG_OP,
        'agent', current_user,
        'sha256_post_read', NEW.sha256_hash
    );
    NEW.chain_event_log = NEW.chain_event_log || event_entry;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ev_chain_event_trigger ON ev_register;
CREATE TRIGGER ev_chain_event_trigger
    BEFORE INSERT OR UPDATE ON ev_register
    FOR EACH ROW EXECUTE FUNCTION log_ev_chain_event();

-- ============================================================
-- SECTION 3: SILVER BULLET CROSSWALK
-- ============================================================

CREATE TABLE IF NOT EXISTS sb_register (
    sb_id           TEXT PRIMARY KEY,   -- SB-01 through SB-66+
    label           TEXT NOT NULL,
    kill_shot       TEXT,               -- the argument this bullet serves
    ev_anchors      TEXT[],             -- EV-NNN items that anchor this bullet
    settlement_tier TEXT,               -- which settlement tier this activates
    bucket          ev_bucket,
    confidence      ev_confidence DEFAULT 'DOCUMENTED',
    notes           TEXT,
    version_added   TEXT DEFAULT 'v54',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 4: AGENT DB — TOPIC-NODE MODULAR ARCHITECTURE
-- ============================================================

CREATE TYPE agent_tier AS ENUM ('TIER1_DETERMINISTIC', 'TIER2_SEMANTIC', 'TIER3_ARBITRATION', 'ORCHESTRATOR');
CREATE TYPE agent_status AS ENUM ('ACTIVE', 'SUSPENDED', 'UPSKILLING', 'QUARANTINED');

CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id            TEXT PRIMARY KEY,   -- e.g. FETTY, TIGER, WOLF, SUITS, BRIDGER, CHESS
    display_name        TEXT NOT NULL,
    tier                agent_tier NOT NULL,
    status              agent_status DEFAULT 'ACTIVE',
    description         TEXT,
    gr_node_scope       TEXT[],             -- which GR nodes this agent covers
    capability_vector   FLOAT[],            -- embedding of agent capabilities
    performance_score   FLOAT DEFAULT 0.5,  -- 0.0-1.0
    upskill_history     JSONB DEFAULT '[]',
    prompt_template_id  TEXT,               -- FK to prompt_registry
    kimi_model          TEXT DEFAULT 'moonshotai/kimi-k2.6', -- NVIDIA NIM model
    api_endpoint        TEXT DEFAULT 'https://integrate.api.nvidia.com/v1',
    max_tokens          INTEGER DEFAULT 4096,
    temperature         FLOAT DEFAULT 0.2,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS subagent_registry (
    subagent_id         TEXT PRIMARY KEY,
    parent_agent_id     TEXT REFERENCES agent_registry(agent_id),
    display_name        TEXT NOT NULL,
    specialization      TEXT,               -- e.g. "OCR_EXTRACTOR", "HASH_DEDUP"
    status              agent_status DEFAULT 'ACTIVE',
    topic_module        TEXT,               -- e.g. "VIE_ASC810", "RICO", "SEC_10K"
    gr_node_primary     TEXT,               -- primary GR node
    gr_node_secondary   TEXT[],             -- secondary GR nodes
    capability_vector   FLOAT[],
    performance_score   FLOAT DEFAULT 0.5,
    task_count          INTEGER DEFAULT 0,
    success_count       INTEGER DEFAULT 0,
    prompt_template_id  TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Cross-stitching: agent-to-agent and agent-to-topic links
CREATE TABLE IF NOT EXISTS agent_cross_stitch (
    stitch_id       SERIAL PRIMARY KEY,
    src_agent_id    TEXT,
    dst_agent_id    TEXT,
    stitch_type     TEXT,   -- e.g. "HANDOFF", "VALIDATE", "ESCALATE", "SYNTHESIZE"
    topic_module    TEXT,
    gr_node         TEXT,
    ev_trigger      TEXT,   -- EV-NNN that triggered this stitch
    confidence      FLOAT DEFAULT 0.8,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 5: KIMI SWARM — PARALLEL AGENT PROTOCOL (PAP)
-- ============================================================

CREATE TYPE task_status AS ENUM ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'STALLED', 'RECURSIVE');
CREATE TYPE task_priority AS ENUM ('CRITICAL', 'HIGH', 'NORMAL', 'LOW', 'BACKGROUND');

CREATE TABLE IF NOT EXISTS swarm_task_queue (
    task_id         UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    parent_task_id  UUID REFERENCES swarm_task_queue(task_id),
    agent_id        TEXT REFERENCES agent_registry(agent_id),
    subagent_id     TEXT REFERENCES subagent_registry(subagent_id),
    task_type       TEXT NOT NULL,          -- e.g. "GR_SYNTHESIS", "EV_ANALYSIS", "PHOTO_OCR"
    topic_module    TEXT,
    gr_node         TEXT,
    ev_scope        TEXT[],                 -- EV-NNN items in scope
    priority        task_priority DEFAULT 'NORMAL',
    status          task_status DEFAULT 'QUEUED',
    depth_level     INTEGER DEFAULT 0,      -- recursion depth
    stagger_delay_ms INTEGER DEFAULT 0,     -- staggered parameter queuing
    input_payload   JSONB,
    output_payload  JSONB,
    progress_pct    FLOAT DEFAULT 0.0,      -- task progression evaluation
    kimi_tokens_used INTEGER DEFAULT 0,
    retry_count     INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 3,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_log       TEXT
);

-- Swarm communication channel (inter-agent messaging)
CREATE TABLE IF NOT EXISTS swarm_message_bus (
    msg_id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    task_id         UUID REFERENCES swarm_task_queue(task_id),
    from_agent      TEXT,
    to_agent        TEXT,
    msg_type        TEXT,   -- "RESULT", "QUERY", "ESCALATE", "VALIDATE", "SYNTHESIZE"
    payload         JSONB,
    processed       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- KIMI synthesis results store
CREATE TABLE IF NOT EXISTS kimi_synthesis_results (
    result_id       UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    task_id         UUID REFERENCES swarm_task_queue(task_id),
    agent_id        TEXT,
    gr_node         TEXT,
    topic_module    TEXT,
    synthesis_type  TEXT,   -- "GR_ANALYSIS", "EV_CLUSTER", "PHOTO_CONTEXT", "MASTER_BRIEF"
    leverage_score  FLOAT,
    confidence      FLOAT,
    content         TEXT,   -- full synthesis text
    key_findings    JSONB,  -- structured extracted findings
    ev_citations    TEXT[], -- EV-NNN items cited
    tokens_used     INTEGER,
    model_used      TEXT DEFAULT 'moonshotai/kimi-k2.6',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 6: ICLOUD PHOTOS DATA LAKE
-- ============================================================

CREATE TABLE IF NOT EXISTS photo_metadata (
    file_path           TEXT PRIMARY KEY,
    file_name           TEXT NOT NULL,
    sha256_hash         TEXT,
    phash               TEXT,               -- perceptual hash for dedup
    file_size_bytes     BIGINT,
    file_format         TEXT,               -- PNG, JPEG, HEIC, MOV
    width_px            INTEGER,
    height_px           INTEGER,
    timestamp_utc       TIMESTAMPTZ,        -- from EXIF DateTimeOriginal
    timestamp_local     TEXT,               -- original local time string
    gps_latitude        FLOAT,
    gps_longitude       FLOAT,
    gps_altitude_m      FLOAT,
    device_make         TEXT DEFAULT 'Apple',
    device_model        TEXT,               -- e.g. iPhone 15 Pro
    device_software     TEXT,               -- iOS version
    camera_lens         TEXT,
    focal_length_mm     FLOAT,
    aperture            FLOAT,
    iso_speed           INTEGER,
    exposure_time       TEXT,
    color_space         TEXT,
    orientation         INTEGER,
    exif_raw            JSONB,              -- full EXIF dump
    icloud_date_path    TEXT,               -- e.g. 2025/08/08
    ingest_status       TEXT DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Photo OCR extracts
CREATE TABLE IF NOT EXISTS photo_ocr (
    ocr_id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    file_path           TEXT REFERENCES photo_metadata(file_path),
    ocr_engine          TEXT,               -- "tesseract", "easyocr", "gemini_vision"
    ocr_text            TEXT,
    ocr_confidence      FLOAT,
    ocr_language        TEXT DEFAULT 'eng',
    word_count          INTEGER,
    has_legal_content   BOOLEAN DEFAULT FALSE,
    has_financial_data  BOOLEAN DEFAULT FALSE,
    has_dk_content      BOOLEAN DEFAULT FALSE,
    processing_time_ms  INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Photo domain tagging (linking photos to GR nodes and EV items)
CREATE TABLE IF NOT EXISTS photo_domain_tags (
    tag_id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    file_path       TEXT REFERENCES photo_metadata(file_path),
    gr_node_links   TEXT[],             -- GR-NNN nodes detected in photo
    ev_links        TEXT[],             -- EV-NNN items this photo evidences
    predicate_tags  TEXT[],             -- from predicate_vocab
    domain_tags     TEXT[],             -- from domain_vocab
    content_type    TEXT,               -- "LEGAL_EXHIBIT", "DK_SCREENSHOT", "CHATGPT_SCREENSHOT", "FINANCIAL_CHART", "OTHER"
    confidence      FLOAT DEFAULT 0.5,
    tagger_agent    TEXT,               -- which agent applied these tags
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Photo data dictionary (YAML-serializable metadata dictionary)
CREATE TABLE IF NOT EXISTS photo_data_dictionary (
    field_name      TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    data_type       TEXT,
    description     TEXT,
    source_table    TEXT,
    source_column   TEXT,
    example_value   TEXT,
    is_pii          BOOLEAN DEFAULT FALSE,
    is_legal_sensitive BOOLEAN DEFAULT FALSE,
    gr_node_relevance TEXT[],
    version         TEXT DEFAULT 'v1.0',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 7: CHATGPT DATA & METADATA DICTIONARIES
-- ============================================================

CREATE TABLE IF NOT EXISTS chatgpt_data_dictionary (
    field_name          TEXT PRIMARY KEY,
    display_name        TEXT NOT NULL,
    data_type           TEXT,
    description         TEXT,
    source_table        TEXT,               -- e.g. chatgpt_conversations, chatgpt_messages
    source_column       TEXT,
    example_value       TEXT,
    is_nullable         BOOLEAN DEFAULT TRUE,
    is_indexed          BOOLEAN DEFAULT FALSE,
    gr_node_relevance   TEXT[],
    ev_relevance        TEXT[],
    semantic_tags       TEXT[],
    version             TEXT DEFAULT 'v1.0',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chatgpt_metadata_dictionary (
    meta_key            TEXT PRIMARY KEY,
    meta_category       TEXT,               -- "TEMPORAL", "SEMANTIC", "LEGAL", "TECHNICAL"
    description         TEXT,
    extraction_method   TEXT,               -- how this metadata is derived
    data_type           TEXT,
    example_value       TEXT,
    gr_node_links       TEXT[],
    ev_links            TEXT[],
    yaml_anchor         TEXT,               -- YAML anchor name for serialization
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 8: NEXOS API GATEWAY — MULTI-API ROUTING
-- ============================================================

CREATE TABLE IF NOT EXISTS api_gateway_config (
    provider_id         TEXT PRIMARY KEY,   -- "KIMI_NVIDIA", "ANTHROPIC", "GEMINI", "OPENAI"
    display_name        TEXT NOT NULL,
    base_url            TEXT NOT NULL,
    api_key_env_var     TEXT,               -- env var name (never store keys in DB)
    model_default       TEXT,
    max_tokens_default  INTEGER DEFAULT 4096,
    temperature_default FLOAT DEFAULT 0.2,
    rate_limit_rpm      INTEGER,            -- requests per minute
    cost_per_1k_tokens  FLOAT,             -- for routing optimization
    supports_streaming  BOOLEAN DEFAULT TRUE,
    supports_vision     BOOLEAN DEFAULT FALSE,
    supports_function_calling BOOLEAN DEFAULT FALSE,
    is_active           BOOLEAN DEFAULT TRUE,
    priority_rank       INTEGER DEFAULT 5,  -- 1=highest priority
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_request_log (
    request_id      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    provider_id     TEXT REFERENCES api_gateway_config(provider_id),
    agent_id        TEXT,
    task_id         UUID,
    model_used      TEXT,
    prompt_tokens   INTEGER,
    completion_tokens INTEGER,
    total_tokens    INTEGER,
    latency_ms      INTEGER,
    status_code     INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SECTION 9: QUERYING ENGINE — UNIFIED SEARCH INDEX
-- ============================================================

CREATE TABLE IF NOT EXISTS unified_search_index (
    doc_id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    source_type     TEXT NOT NULL,  -- "CHATGPT_MSG", "PHOTO_OCR", "EV_ITEM", "SYNTHESIS"
    source_id       TEXT NOT NULL,  -- original ID in source table
    content_text    TEXT,           -- full text for FTS
    gr_node_links   TEXT[],
    ev_links        TEXT[],
    predicate_tags  TEXT[],
    timestamp_utc   TIMESTAMPTZ,
    confidence      FLOAT DEFAULT 0.5,
    search_vector   TSVECTOR,       -- PostgreSQL FTS vector
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- FTS index
CREATE INDEX IF NOT EXISTS unified_search_fts_idx
    ON unified_search_index USING GIN(search_vector);

-- FTS trigger
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector = to_tsvector('english', COALESCE(NEW.content_text, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS search_vector_trigger ON unified_search_index;
CREATE TRIGGER search_vector_trigger
    BEFORE INSERT OR UPDATE ON unified_search_index
    FOR EACH ROW EXECUTE FUNCTION update_search_vector();

-- GR node index for fast cross-domain queries
CREATE INDEX IF NOT EXISTS ev_gr_node_idx ON ev_register USING GIN(gr_node_links);
CREATE INDEX IF NOT EXISTS photo_gr_node_idx ON photo_domain_tags USING GIN(gr_node_links);
CREATE INDEX IF NOT EXISTS photo_ev_idx ON photo_domain_tags USING GIN(ev_links);
CREATE INDEX IF NOT EXISTS task_queue_status_idx ON swarm_task_queue(status, priority, created_at);

-- ============================================================
-- SECTION 10: PARALLEL AGENT PROTOCOL (PAP) — GITHUB REPO SYNC
-- ============================================================

CREATE TABLE IF NOT EXISTS github_sync_log (
    sync_id         UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    repo_name       TEXT NOT NULL,          -- e.g. jwk2588/draftkings-data-nexus-codex
    branch          TEXT DEFAULT 'main',
    commit_sha      TEXT,
    files_synced    TEXT[],
    sync_type       TEXT,                   -- "PUSH", "PULL", "ARTIFACT_EXPORT"
    agent_id        TEXT,
    status          TEXT DEFAULT 'PENDING',
    error_log       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Version tracking for the corpus
CREATE TABLE IF NOT EXISTS corpus_version_log (
    version_id      TEXT PRIMARY KEY,       -- e.g. v54, v55
    description     TEXT,
    ev_count        INTEGER,
    sb_count        INTEGER,
    sha256_workbook TEXT,                   -- hash of the XLSX workbook
    signed_by       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial corpus version
INSERT INTO corpus_version_log (version_id, description, ev_count, sb_count, created_at)
VALUES ('v54', 'Authoritative Master Compiled v54 — EV-001 through EV-291 + provisional tranches', 291, 66, NOW())
ON CONFLICT (version_id) DO NOTHING;

-- ============================================================
-- SEED: API GATEWAY PROVIDERS
-- ============================================================
INSERT INTO api_gateway_config (provider_id, display_name, base_url, api_key_env_var, model_default, max_tokens_default, cost_per_1k_tokens, supports_vision, supports_function_calling, priority_rank)
VALUES
    ('KIMI_NVIDIA', 'KIMI K2.6 via NVIDIA NIM (FREE)', 'https://integrate.api.nvidia.com/v1', 'NVIDIA_API_KEY', 'moonshotai/kimi-k2.6', 8192, 0.0, FALSE, TRUE, 1),
    ('ANTHROPIC', 'Anthropic Claude 3.5 Sonnet', 'https://api.anthropic.com', 'ANTHROPIC_API_KEY', 'claude-3-5-sonnet-20241022', 8192, 3.0, TRUE, TRUE, 2),
    ('GEMINI', 'Google Gemini 2.5 Flash', 'https://generativelanguage.googleapis.com', 'GEMINI_API_KEY', 'gemini-2.5-flash', 8192, 0.075, TRUE, TRUE, 3),
    ('OPENAI', 'OpenAI GPT-4o', 'https://api.openai.com/v1', 'OPENAI_API_KEY', 'gpt-4o', 4096, 5.0, TRUE, TRUE, 4)
ON CONFLICT (provider_id) DO NOTHING;

-- ============================================================
-- SEED: AGENT REGISTRY (8 NEXUS Agents)
-- ============================================================
INSERT INTO agent_registry (agent_id, display_name, tier, gr_node_scope, description)
VALUES
    ('FETTY', 'FETTY FM — Master Orchestrator', 'ORCHESTRATOR', ARRAY['GR-001','GR-002','GR-003','GR-004','GR-005','GR-006','GR-007','GR-008','GR-009','GR-010','GR-011','GR-012'], 'Master orchestrator — routes tasks to specialist agents, manages swarm state'),
    ('TIGER', 'TIGER — Forensic Accounting', 'TIER2_SEMANTIC', ARRAY['GR-001','GR-004','GR-010'], 'ASC 810 VIE analysis, ASC 606 revenue restatement, forensic accounting'),
    ('WOLF', 'WOLF — Legal Attack', 'TIER2_SEMANTIC', ARRAY['GR-002','GR-003','GR-005','GR-006'], 'RICO, consumer protection, constitutional law, regulatory attack'),
    ('SUITS', 'SUITS — ADR Synthesis', 'TIER3_ARBITRATION', ARRAY['GR-008'], 'FRE 408 ADR package synthesis, settlement band calculation, Prisoner''s Dilemma'),
    ('BRIDGER', 'BRIDGER — Cross-Domain Mapper', 'TIER2_SEMANTIC', ARRAY['GR-001','GR-002','GR-003','GR-004','GR-005','GR-006','GR-007','GR-008','GR-009','GR-010','GR-011','GR-012'], 'Maps evidence across GR nodes, finds cross-domain leverage'),
    ('CHESS', 'CHESS — Moat Calculator', 'TIER3_ARBITRATION', ARRAY['GR-009','GR-010'], 'Moat score, rule pressure, settlement range calculation'),
    ('INGEST', 'INGEST — Data Pipeline', 'TIER1_DETERMINISTIC', ARRAY['GR-001','GR-002','GR-003','GR-004','GR-005','GR-006','GR-007','GR-008','GR-009','GR-010','GR-011','GR-012'], 'Deterministic data ingestion: OCR, EXIF, hash, dedup'),
    ('VALIDATOR', 'VALIDATOR — 12-Pass Gate', 'TIER1_DETERMINISTIC', ARRAY['GR-001','GR-002','GR-003','GR-004','GR-005','GR-006','GR-007','GR-008','GR-009','GR-010','GR-011','GR-012'], '12-pass validation framework, trust decay, epistemic state enforcement')
ON CONFLICT (agent_id) DO NOTHING;

-- ============================================================
-- SEED: SUBAGENT REGISTRY (Topic-Node Modules)
-- ============================================================
INSERT INTO subagent_registry (subagent_id, parent_agent_id, display_name, specialization, topic_module, gr_node_primary)
VALUES
    ('SA-VIE-001', 'TIGER', 'VIE Consolidation Analyst', 'ASC810_ANALYSIS', 'VIE_ASC810', 'GR-001'),
    ('SA-REV-001', 'TIGER', 'Revenue Recognition Analyst', 'ASC606_ANALYSIS', 'REVENUE_RECOGNITION', 'GR-004'),
    ('SA-RICO-001', 'WOLF', 'RICO Enterprise Mapper', 'RICO_ANALYSIS', 'RICO_WIRE_FRAUD', 'GR-002'),
    ('SA-CONS-001', 'WOLF', 'Consumer Protection Analyst', 'CONSUMER_PROTECTION', 'CONSUMER_FTC', 'GR-003'),
    ('SA-SEC-001', 'TIGER', 'SEC Filing Analyst', 'SEC_10K_ANALYSIS', 'SEC_SECURITIES', 'GR-010'),
    ('SA-MGCB-001', 'WOLF', 'Michigan Gaming Analyst', 'MGCB_REGULATORY', 'MGCB_GAMING', 'GR-006'),
    ('SA-PLAT-001', 'BRIDGER', 'Platform Removal Analyst', 'PLATFORM_ANALYSIS', 'APPLE_GOOGLE_PLATFORM', 'GR-007'),
    ('SA-ADR-001', 'SUITS', 'ADR Settlement Analyst', 'ADR_SYNTHESIS', 'SETTLEMENT_ADR', 'GR-008'),
    ('SA-OCR-001', 'INGEST', 'Photo OCR Extractor', 'OCR_EXTRACTION', 'PHOTO_PIPELINE', NULL),
    ('SA-EXIF-001', 'INGEST', 'EXIF Metadata Parser', 'EXIF_PARSING', 'PHOTO_PIPELINE', NULL),
    ('SA-HASH-001', 'INGEST', 'Hash & Dedup Engine', 'HASH_DEDUP', 'EVIDENCE_INTEGRITY', NULL),
    ('SA-CHAT-001', 'INGEST', 'ChatGPT Corpus Analyst', 'CHATGPT_ANALYSIS', 'CHATGPT_CORPUS', NULL)
ON CONFLICT (subagent_id) DO NOTHING;

COMMENT ON TABLE ev_register IS 'ATTORNEY WORK PRODUCT — FRE 408 — FRE 502(d) PROTECTED. Ingestion Contract C-01 through C-12 enforced via triggers.';
COMMENT ON TABLE swarm_task_queue IS 'KIMI K2.6 Parallel Agent Protocol (PAP) task queue with recursive staggered queuing.';
COMMENT ON TABLE photo_metadata IS 'iCloud Photos data lake — 11,360 iPhone 15 Pro screenshots with full EXIF and OCR pipeline.';
