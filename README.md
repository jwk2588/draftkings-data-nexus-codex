# draftkings-data-nexus-codex

**DraftKings Evidentiary Cognition Engine**  
*Enterprise Name: DraftKings Master Knowledge Repository*  
*Symbolic Name: Data Nexus Codex*

---

## Architecture

```
MBv54 Legal Spine (Canonical Evidence Governance)
         │
         ▼
Canonical Evidence Layer (T1 / T2 / T3 Tier System)
         │
    ┌────┴────┐
    ▼         ▼
Gemini      ChatGPT ETL
Extractor   (github-gem-seeker)
    │         │
    └────┬────┘
         ▼
  Curated DataLake
  (Polars + Parquet)
         │
         ▼
  Neo4j Graph Core
  (DraftKingsDB)
         │
         ▼
  GraphRAG Layer
  (Qdrant + Cypher)
         │
         ▼
Claude Reasoning Layer
(T1/T2 Constrained)
         │
         ▼
API + UI + Agent Shells
```

## Build Order (Strict)

1. **Freeze Ontology** — `schema/` (DONE)
2. **ETL Pipeline** — `etl/` (DONE)
3. **Neo4j + Qdrant** — `docker/` (DONE — requires Docker)
4. **GraphRAG Layer** — `graphrag/` (pending graph integrity)
5. **Claude Reasoning** — `reasoning/` (pending GraphRAG)
6. **API + Shells** — `api/`, `agent_shells/` (last)

## Frozen Schema

All schema files in `schema/` are constitutional law. Changes require an ADR.

| File | Purpose |
|---|---|
| `node_types.yaml` | All allowed graph node types with required fields |
| `relationship_types.yaml` | All allowed edges with from/to constraints |
| `tier_rules.yaml` | T1/T2/T3 governance, promotion rules, pollution prevention |
| `evidence_constraints.yaml` | SHA-256 hashing, GraphMutationEvent spec, tier middleware contract |

## ETL Pipeline

```bash
# Run with your ChatGPT export
python3 etl/run_pipeline.py --input path/to/conversations.json

# Run with synthetic sample
python3 etl/run_pipeline.py
```

Pipeline: `conversations.json → normalized.parquet → classified.parquet → entities.parquet → Neo4j`

## Neo4j Stack

```bash
docker compose -f docker/docker-compose.yml up -d
```

See `docker/README.md` for full setup instructions.

## ADR Index

| ADR | Title |
|---|---|
| ADR-001 | Canonical Evidence Spine |
| ADR-002 | Tier Governance |
| ADR-003 | Graph Constraints |
| ADR-004 | Evidence Hashing |
| ADR-005 | Immutable Event Log |

## Tech Stack

| Function | Technology |
|---|---|
| Graph | Neo4j 5 |
| Vector | Qdrant |
| ETL | Python + Polars |
| Parquet | PyArrow |
| Reasoning | Claude (Anthropic) |
| Extraction | Gemini (Google) |
| Embeddings | OpenAI |
| Orchestration | FastAPI |
| Object Storage | MinIO / S3 |

---
*Governed by MBv54 Legal Spine — Canonical Evidence Governance Framework v54*
