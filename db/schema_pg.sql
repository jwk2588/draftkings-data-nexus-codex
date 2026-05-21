-- ============================================================
-- DraftKings HiveMind v3.0 — PostgreSQL Master Schema
-- Incorporates all 8 ChatGPT hardening directives
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ============================================================
-- LAYER 0: EVENT LEDGER (Append-Only, Immutable)
-- Hardening Directive #2: Event-Sourced Architecture
-- ============================================================
CREATE TABLE IF NOT EXISTS event_ledger (
    event_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type      VARCHAR(100) NOT NULL,  -- GRAPH_MUTATION, ONTOLOGY_UPDATE, ARBITRATION_DECISION, etc.
    event_subtype   VARCHAR(100),
    payload         JSONB NOT NULL,
    agent_id        VARCHAR(100),
    session_id      VARCHAR(100),
    ontology_version VARCHAR(20),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum        VARCHAR(64) NOT NULL   -- SHA256 of payload for integrity
);
-- Append-only enforced via trigger
CREATE OR REPLACE FUNCTION prevent_event_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Event ledger is append-only. Updates and deletes are forbidden.';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER event_ledger_immutable
    BEFORE UPDATE OR DELETE ON event_ledger
    FOR EACH ROW EXECUTE FUNCTION prevent_event_modification();

-- ============================================================
-- LAYER 1: RAW OBJECT STORAGE REGISTRY (Immutable Manifest)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_objects (
    object_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provenance_id   VARCHAR(100) UNIQUE NOT NULL,
    sha256_hash     VARCHAR(64) NOT NULL,
    original_path   TEXT NOT NULL,
    original_filename VARCHAR(500) NOT NULL,
    device_source   VARCHAR(200),
    mime_type       VARCHAR(100),
    file_size_bytes BIGINT,
    object_type     VARCHAR(50) NOT NULL,  -- icloud_photo, sec_filing, screenshot, email, ocr_output
    storage_path    TEXT NOT NULL,         -- MinIO/S3 path
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingestion_agent VARCHAR(100),
    is_locked       BOOLEAN NOT NULL DEFAULT TRUE,  -- immutable after ingest
    metadata        JSONB DEFAULT '{}'
);
-- Immutability trigger
CREATE OR REPLACE FUNCTION prevent_raw_modification()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.is_locked THEN
        RAISE EXCEPTION 'Raw object % is locked and immutable.', OLD.object_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER raw_objects_immutable
    BEFORE UPDATE ON raw_objects
    FOR EACH ROW EXECUTE FUNCTION prevent_raw_modification();

-- ============================================================
-- LAYER 2: CANONICAL METADATA ENGINE
-- ============================================================
CREATE TABLE IF NOT EXISTS canonical_metadata (
    object_id           UUID PRIMARY KEY REFERENCES raw_objects(object_id),
    source_id           VARCHAR(100),
    canonical_entity_refs JSONB DEFAULT '[]',   -- array of entity IDs
    timeline_refs       JSONB DEFAULT '[]',
    semantic_tags       JSONB DEFAULT '[]',
    confidence_scores   JSONB DEFAULT '{}',
    embedding_refs      JSONB DEFAULT '[]',
    ocr_refs            JSONB DEFAULT '[]',
    relationship_edges  JSONB DEFAULT '[]',
    case_refs           JSONB DEFAULT '[]',
    originating_agent   VARCHAR(100),
    validation_status   VARCHAR(50) DEFAULT 'PENDING',
    provenance_chain    JSONB DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confidence_history  JSONB DEFAULT '[]',
    ontology_version    VARCHAR(20) DEFAULT '1.0.0'
);

-- ============================================================
-- LAYER 3: CANONICAL ENTITY REGISTRY (Core Spinal System)
-- ============================================================
CREATE TABLE IF NOT EXISTS canonical_entities (
    entity_id           VARCHAR(100) PRIMARY KEY,  -- e.g., ENTITY_DK_0001
    entity_type         VARCHAR(50) NOT NULL,       -- Person, Company, Filing, etc.
    canonical_name      VARCHAR(500) NOT NULL,
    aliases             JSONB DEFAULT '[]',
    semantic_variants   JSONB DEFAULT '[]',
    normalized_identifiers JSONB DEFAULT '{}',
    source_references   JSONB DEFAULT '[]',
    confidence_score    FLOAT DEFAULT 1.0,
    confidence_lineage  JSONB DEFAULT '[]',
    parent_entity_id    VARCHAR(100) REFERENCES canonical_entities(entity_id),
    child_entity_ids    JSONB DEFAULT '[]',
    ontology_version    VARCHAR(20) DEFAULT '1.0.0',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active           BOOLEAN DEFAULT TRUE
);

-- ============================================================
-- EPISTEMIC STATE MODEL (Hardening Directive #6)
-- ============================================================
CREATE TYPE epistemic_state AS ENUM (
    'VERIFIED',
    'STRONGLY_SUPPORTED',
    'PROBABILISTIC',
    'DISPUTED',
    'CONTRADICTED',
    'SUPERSEDED',
    'UNRESOLVED'
);

-- ============================================================
-- GRAPH EDGE STAGING TABLE (Pre-Neo4j arbitration queue)
-- ============================================================
CREATE TABLE IF NOT EXISTS graph_edge_proposals (
    proposal_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    src_node_id         VARCHAR(200) NOT NULL,
    dst_node_id         VARCHAR(200) NOT NULL,
    edge_type           VARCHAR(100) NOT NULL,
    epistemic_state     epistemic_state NOT NULL DEFAULT 'UNRESOLVED',
    confidence_score    FLOAT NOT NULL DEFAULT 0.0,
    provenance          JSONB NOT NULL DEFAULT '{}',
    source_references   JSONB DEFAULT '[]',
    generating_agent    VARCHAR(100) NOT NULL,
    arbitration_status  VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, APPROVED, REJECTED, QUARANTINED
    arbitration_agent   VARCHAR(100),
    arbitration_notes   TEXT,
    trust_decay_rate    FLOAT DEFAULT 0.01,  -- per day for speculative edges
    last_reinforced_at  TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ontology_version    VARCHAR(20) DEFAULT '1.0.0'
);

-- ============================================================
-- ONTOLOGY VERSION REGISTRY (Hardening Directive #3)
-- ============================================================
CREATE TABLE IF NOT EXISTS ontology_versions (
    version_id          VARCHAR(20) PRIMARY KEY,  -- e.g., 1.0.0
    schema_snapshot     JSONB NOT NULL,
    node_types          JSONB NOT NULL,
    edge_types          JSONB NOT NULL,
    deprecated_edges    JSONB DEFAULT '[]',
    migration_notes     TEXT,
    backward_compat_map JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(100),
    is_active           BOOLEAN DEFAULT TRUE
);
-- Seed initial ontology version
INSERT INTO ontology_versions (version_id, schema_snapshot, node_types, edge_types, created_by)
VALUES (
    '1.0.0',
    '{"version": "1.0.0", "description": "Initial HiveMind ontology"}',
    '["Person","Company","Filing","Image","Screenshot","Reward","Transaction","VIPHost","Communication","Device","Session","TimelineEvent","SECDisclosure","LoyaltyProgramChange","LegalTheory","EvidenceObject"]',
    '["APPEARS_IN","SENT","REFERENCED_BY","DISCLOSED_IN","RELATED_TO","REDEEMED","GENERATED","LINKED_TO","PRECEDES","CONTRADICTS","MODIFIED","DERIVED_FROM","EXTRACTED_FROM","EMBEDDED_AS","ASSOCIATED_WITH"]',
    'SYSTEM_INIT'
) ON CONFLICT DO NOTHING;

-- ============================================================
-- PROMPT REGISTRY (Hardening Directive #4)
-- ============================================================
CREATE TABLE IF NOT EXISTS prompt_registry (
    prompt_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    prompt_name         VARCHAR(200) UNIQUE NOT NULL,
    prompt_version      VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    prompt_text         TEXT NOT NULL,
    prompt_type         VARCHAR(50),  -- EXTRACTION, INFERENCE, ARBITRATION, SIMULATION
    agent_target        VARCHAR(100),
    dependencies        JSONB DEFAULT '[]',
    lint_score          FLOAT,
    hallucination_score FLOAT,
    token_efficiency    FLOAT,
    test_results        JSONB DEFAULT '[]',
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshot_at         TIMESTAMPTZ
);

-- ============================================================
-- AGENT FAILURE FORENSICS (Hardening Directive #7)
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_forensics (
    forensic_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_id            VARCHAR(100) NOT NULL,
    failure_type        VARCHAR(100) NOT NULL,  -- HALLUCINATION, CORRUPTION, DRIFT, OVERLOAD, etc.
    origin_chain        JSONB DEFAULT '[]',
    prompt_lineage      JSONB DEFAULT '[]',
    embedding_sources   JSONB DEFAULT '[]',
    recursion_path      JSONB DEFAULT '[]',
    arbitration_context JSONB DEFAULT '{}',
    severity            VARCHAR(20) DEFAULT 'MEDIUM',  -- LOW, MEDIUM, HIGH, CRITICAL
    postmortem_report   TEXT,
    rollback_recommendation TEXT,
    quarantine_protocol TEXT,
    stability_score     FLOAT,
    resolved            BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- OCR EXTRACTS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS ocr_extracts (
    extract_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    object_id           UUID NOT NULL REFERENCES raw_objects(object_id),
    engine_used         VARCHAR(50) NOT NULL,  -- tesseract, easyocr, gemini
    raw_text            TEXT,
    structured_data     JSONB DEFAULT '{}',
    confidence_score    FLOAT,
    word_count          INTEGER,
    contains_amounts    BOOLEAN DEFAULT FALSE,
    contains_usernames  BOOLEAN DEFAULT FALSE,
    contains_legal_refs BOOLEAN DEFAULT FALSE,
    contains_financial  BOOLEAN DEFAULT FALSE,
    app_ui_detected     VARCHAR(100),
    scene_classification VARCHAR(100),
    extracted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ontology_version    VARCHAR(20) DEFAULT '1.0.0'
);

-- ============================================================
-- EXIF METADATA TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS exif_metadata (
    exif_id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    object_id           UUID NOT NULL REFERENCES raw_objects(object_id),
    captured_at_utc     TIMESTAMPTZ,
    gps_latitude        FLOAT,
    gps_longitude       FLOAT,
    device_make         VARCHAR(100),
    device_model        VARCHAR(100),
    image_width         INTEGER,
    image_height        INTEGER,
    is_screenshot       BOOLEAN DEFAULT FALSE,
    raw_exif            JSONB DEFAULT '{}',
    extracted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- VALIDATION PASS LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS validation_log (
    log_id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_stage      VARCHAR(100) NOT NULL,
    object_id           UUID,
    pass_number         INTEGER NOT NULL,  -- 1-12
    pass_name           VARCHAR(100) NOT NULL,
    result              VARCHAR(20) NOT NULL,  -- PASS, FAIL, WARN
    details             JSONB DEFAULT '{}',
    agent_id            VARCHAR(100),
    validated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- SIMULATION ENVIRONMENT RUNS (Hardening Directive #8)
-- ============================================================
CREATE TABLE IF NOT EXISTS simulation_runs (
    run_id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_name       VARCHAR(200) NOT NULL,
    scenario_type       VARCHAR(100),  -- ADVERSARIAL, HALLUCINATION_INJECT, ONTOLOGY_MUTATION, OVERLOAD
    synthetic_dataset   JSONB DEFAULT '{}',
    agents_tested       JSONB DEFAULT '[]',
    results             JSONB DEFAULT '{}',
    passed              BOOLEAN,
    approval_for_production BOOLEAN DEFAULT FALSE,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes               TEXT
);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_event_ledger_type ON event_ledger(event_type);
CREATE INDEX IF NOT EXISTS idx_event_ledger_agent ON event_ledger(agent_id);
CREATE INDEX IF NOT EXISTS idx_event_ledger_created ON event_ledger(created_at);
CREATE INDEX IF NOT EXISTS idx_raw_objects_hash ON raw_objects(sha256_hash);
CREATE INDEX IF NOT EXISTS idx_raw_objects_type ON raw_objects(object_type);
CREATE INDEX IF NOT EXISTS idx_canonical_entities_type ON canonical_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_graph_proposals_status ON graph_edge_proposals(arbitration_status);
CREATE INDEX IF NOT EXISTS idx_graph_proposals_epistemic ON graph_edge_proposals(epistemic_state);
CREATE INDEX IF NOT EXISTS idx_ocr_extracts_object ON ocr_extracts(object_id);
CREATE INDEX IF NOT EXISTS idx_exif_metadata_object ON exif_metadata(object_id);
CREATE INDEX IF NOT EXISTS idx_validation_log_stage ON validation_log(pipeline_stage);
CREATE INDEX IF NOT EXISTS idx_agent_forensics_agent ON agent_forensics(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_forensics_type ON agent_forensics(failure_type);

-- ============================================================
-- SEED: CANONICAL ENTITY REGISTRY — DraftKings Core Entities
-- ============================================================
INSERT INTO canonical_entities (entity_id, entity_type, canonical_name, aliases, source_references) VALUES
('ENTITY_DK_0001', 'Company', 'DraftKings Inc.', '["DraftKings","DK","DKNG"]', '["SEC:DKNG","EDGAR:0001698991"]'),
('ENTITY_EXEC_0001', 'Person', 'Jason Robins', '["Jason K. Robins","J. Robins"]', '["SEC:DKNG:DEF14A"]'),
('ENTITY_REWARD_0001', 'Reward', 'DraftKings Rewards Program', '["DK Rewards","Crown Club"]', '["DK_TOS_2024"]'),
('ENTITY_SEC_10K_2024', 'Filing', 'DraftKings 10-K 2024', '["DKNG 10-K FY2024"]', '["EDGAR:0001698991:10-K:2024"]'),
('ENTITY_LEGAL_ASC606', 'LegalTheory', 'ASC 606 Revenue Recognition Violation', '["ASC606","Revenue Recognition"]', '["GR-002"]'),
('ENTITY_LEGAL_ASC810', 'LegalTheory', 'ASC 810 VIE Off-Balance Sheet', '["ASC810","VIE"]', '["GR-001"]')
ON CONFLICT DO NOTHING;

SELECT 'SCHEMA DEPLOYED SUCCESSFULLY' AS status,
       (SELECT COUNT(*) FROM canonical_entities) AS seed_entities,
       (SELECT COUNT(*) FROM ontology_versions) AS ontology_versions;
