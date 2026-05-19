# ADR-001 — Canonical Evidence Spine

**Status:** Accepted  
**Date:** 2026-05-19  
**Authority:** MBv54 Legal Spine  
**Deciders:** Master Orchestrator, Human Curator

## Context

The platform requires a durable, auditable foundation for all knowledge stored in the graph. Without a canonical evidence layer, LLM outputs will slowly pollute the graph with semantic drift, circular reasoning, and unverifiable claims. This becomes critical when the system is used for litigation support, regulatory analysis, or financial modeling.

## Decision

All knowledge in the graph must be anchored to an **Evidence** node at Tier 1. No Theory, Claim, or analytical output may exist in the graph without at least one traceable path back to a T1 Evidence node. The `MBv54 Legal Spine` governs this requirement as constitutional law for the system.

## Consequences

Every ETL pipeline must produce Evidence nodes as its primary output. The Claude Reasoning Layer may only write T2 nodes that cite T1 evidence. The tier enforcement middleware enforces this at every write boundary.

---

# ADR-002 — Tier Governance

**Status:** Accepted  
**Date:** 2026-05-19

## Context

Different types of knowledge have different reliability levels. Mixing primary source data with LLM inferences in the same tier creates retrieval ambiguity and degrades reasoning quality.

## Decision

Three tiers are defined and frozen:
- **T1 (Primary Evidence):** Immutable, directly sourced facts. Write authority: `gemini_extractor`, `chatgpt_etl`, `human_curator` only.
- **T2 (Derived Evidence):** Analytical conclusions citing T1. Write authority: `claude_reasoning_layer`, `gemini_extractor`, `algo_code_writer`.
- **T3 (Inferred/Speculative):** Low-confidence inferences. Any agent may write. Auto-expires after 72 hours unless promoted.

## Consequences

The Claude Reasoning Layer must tag every output with its tier. The GraphRAG retrieval layer must surface tier metadata in every response. Auditors can filter by tier to see only primary evidence.

---

# ADR-003 — Graph Constraints

**Status:** Accepted  
**Date:** 2026-05-19

## Context

Without enforced schema constraints, the graph will accumulate orphaned nodes, invalid relationships, and duplicate entities over time.

## Decision

All constraints defined in `graph/cypher/01_constraints.cypher` are mandatory and must be applied before any data is written. Uniqueness constraints on `ev_id` and `content_hash` prevent duplicates. Property existence constraints enforce schema completeness. These constraints are applied once at Neo4j startup and are never removed without a new ADR.

## Consequences

Any write that violates a constraint is rejected by Neo4j at the database level, providing a second layer of defense behind the tier enforcement middleware.

---

# ADR-004 — Evidence Hashing

**Status:** Accepted  
**Date:** 2026-05-19

## Context

Duplicate evidence nodes inflate the graph, pollute retrieval, and make provenance analysis unreliable. LLMs can produce semantically identical content with different phrasing, making string comparison insufficient.

## Decision

Every Evidence node must store `content_hash = sha256(evidence_text + source_doc + extracted_by + extracted_at)` as a 64-character hex string. This hash is the primary deduplication key. A secondary semantic similarity check via Qdrant (cosine distance >= 0.92) flags potential near-duplicates for human review. The hash is computed by the ETL pipeline before any write attempt.

## Consequences

The ETL pipeline must compute SHA-256 hashes before Neo4j writes. The tier enforcement middleware checks `content_hash` uniqueness before every CREATE event. This enables chain-of-custody analysis and litigation-grade evidence integrity.

---

# ADR-005 — Immutable Event Log

**Status:** Accepted  
**Date:** 2026-05-19

## Context

Silent mutations to evidence nodes destroy audit trails and make the system untrustworthy for legal or regulatory purposes. There must be a complete, replayable record of every graph mutation.

## Decision

Every graph write — including CREATE, UPDATE, DEPRECATE, PROMOTE, and REJECT events — must first produce a `GraphMutationEvent` node in the append-only log. This log is stored in both Neo4j and a local JSONL file (`artifacts/mutation_log.jsonl`). Evidence nodes are immutable: they may be deprecated but never deleted or overwritten. The `GraphMutationEvent` node itself is immutable and append-only.

## Consequences

The system gains full replayability, rollback capability, and audit trails. This is the foundation for chain-of-custody analysis if the platform evolves into litigation tooling. The mutation log grows indefinitely — periodic archiving to S3/MinIO is recommended after 100k events.
