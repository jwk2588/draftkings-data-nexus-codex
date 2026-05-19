// ============================================================
// DraftKings Graph — Canonical Constraints & Indexes
// Run once after Neo4j starts to enforce the frozen ontology.
// ============================================================

// --- Uniqueness Constraints ---

// Evidence: ev_id and content_hash must be globally unique
CREATE CONSTRAINT evidence_ev_id IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.ev_id IS UNIQUE;

CREATE CONSTRAINT evidence_content_hash IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.content_hash IS UNIQUE;

// GraphMutationEvent: event_id must be unique (append-only log)
CREATE CONSTRAINT mutation_event_id IF NOT EXISTS
  FOR (n:GraphMutationEvent) REQUIRE n.event_id IS UNIQUE;

// Player: player_id unique
CREATE CONSTRAINT player_id IF NOT EXISTS
  FOR (n:Player) REQUIRE n.player_id IS UNIQUE;

// Contest: contest_id unique
CREATE CONSTRAINT contest_id IF NOT EXISTS
  FOR (n:Contest) REQUIRE n.contest_id IS UNIQUE;

// Team: team_id unique
CREATE CONSTRAINT team_id IF NOT EXISTS
  FOR (n:Team) REQUIRE n.team_id IS UNIQUE;

// Slate: slate_id unique
CREATE CONSTRAINT slate_id IF NOT EXISTS
  FOR (n:Slate) REQUIRE n.slate_id IS UNIQUE;

// SourceDocument: doc_id unique
CREATE CONSTRAINT source_doc_id IF NOT EXISTS
  FOR (n:SourceDocument) REQUIRE n.doc_id IS UNIQUE;

// Theory: theory_id unique
CREATE CONSTRAINT theory_id IF NOT EXISTS
  FOR (n:Theory) REQUIRE n.theory_id IS UNIQUE;

// --- Property Existence Constraints (T1 Evidence) ---

CREATE CONSTRAINT evidence_tier IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.tier IS NOT NULL;

CREATE CONSTRAINT evidence_source_doc IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.source_doc IS NOT NULL;

CREATE CONSTRAINT evidence_text IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.evidence_text IS NOT NULL;

CREATE CONSTRAINT evidence_extracted_by IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.extracted_by IS NOT NULL;

// --- Indexes for Query Performance ---

CREATE INDEX evidence_tier_idx IF NOT EXISTS
  FOR (n:Evidence) ON (n.tier);

CREATE INDEX evidence_extracted_by_idx IF NOT EXISTS
  FOR (n:Evidence) ON (n.extracted_by);

CREATE INDEX player_position_idx IF NOT EXISTS
  FOR (n:Player) ON (n.position);

CREATE INDEX player_team_idx IF NOT EXISTS
  FOR (n:Player) ON (n.team);

CREATE INDEX player_salary_idx IF NOT EXISTS
  FOR (n:Player) ON (n.salary);

CREATE INDEX contest_sport_idx IF NOT EXISTS
  FOR (n:Contest) ON (n.sport);

CREATE INDEX contest_type_idx IF NOT EXISTS
  FOR (n:Contest) ON (n.contest_type);

CREATE INDEX mutation_event_type_idx IF NOT EXISTS
  FOR (n:GraphMutationEvent) ON (n.event_type);

CREATE INDEX mutation_actor_idx IF NOT EXISTS
  FOR (n:GraphMutationEvent) ON (n.actor);

// ============================================================
// Verify constraints
// ============================================================
SHOW CONSTRAINTS;
