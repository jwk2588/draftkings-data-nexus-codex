# NEXUS DraftKings Codex Repository
## Master GitHub Claude Nexus Codex — AI-Native Litigation Intelligence

> **Classification:** Attorney Work Product | FRE 408 Protected | Internal Use Only
> **Version:** NEXUS v2.0 | Omni-Vault Agile Orchestrator

---

## Architecture Overview

This repository is the **data artifact layer** for the NEXUS multi-agent
DraftKings ADR/litigation intelligence system. It serves as:

1. **Data Artifact Store** — All EV-NNN evidence records, GR-NNN node registry,
   bridge inventory, and system state snapshots are stored as structured JSON.

2. **Hidden DevTools Schema Layer** — AI-written JSON Schema definitions,
   GitHub Copilot prompt templates, graph exports, and raw text extracts
   live in `/devtools/` (not user-facing; consumed by agent pipelines).

3. **Agent Module Repo** — Each NEXUS agent (TIGER, WOLF, SUITS, BRIDGER,
   CHESS, FETTY FM) has its own directory under `/agents/`.

4. **GitHub Actions Automation** — Four workflows automate:
   - Nightly sync of all artifacts
   - Evidence ingestion from raw text
   - ADR package generation on demand
   - CHESS moat monitoring every 6 hours

---

## Repository Structure

```
nexus-dk-codex/
├── data/
│   ├── evidence/        # EV-NNN.json — Evidence register artifacts
│   ├── gr_nodes/        # GR-NNN.json — GR node registry
│   ├── bridges/         # Bridge-NNN.json — Cross-domain bridges
│   ├── chess/           # chess_state.json — CHESS piece health
│   ├── state/           # snapshot_*.json — System state
│   └── adr_packages/    # ADR-*.json — SUITS ADR packages
├── devtools/
│   ├── raw_text/        # Raw text extracts from source documents
│   ├── schema/          # AI-written JSON Schema definitions
│   ├── copilot_prompts/ # GitHub Copilot prompt templates
│   ├── validators/      # Validation and hygiene scripts
│   ├── llm_outputs/     # Cached LLM outputs (TIGER/WOLF/SUITS memos)
│   └── graph/           # GR node graph (adjacency list + Mermaid)
├── agents/
│   ├── tiger/           # Forensic accounting module
│   ├── wolf/            # Legal attack module
│   ├── suits/           # ADR synthesis module
│   ├── bridger/         # Cross-domain mapper
│   ├── chess/           # Moat calculator
│   └── fetty_fm/        # Orchestrator
├── .github/
│   └── workflows/
│       ├── nexus_nightly_sync.yml
│       ├── evidence_ingestion.yml
│       ├── adr_package_generator.yml
│       └── chess_moat_monitor.yml
└── docs/
    └── schemas/
```

---

## Quick Start

### 1. Bootstrap the Database
```bash
python scripts/nexus_db.py
```

### 2. Run a Scenario
```bash
python -c "
from scripts.fetty_fm import FettyFM
fm = FettyFM()
report = fm.run_scenario('PLATFORM_REMOVAL_PINCER')
print(report)
"
```

### 3. Generate ADR Package
```bash
python scripts/agent_pipelines.py
```

### 4. Sync to GitHub
```bash
python scripts/sync_cde.py --repo owner/nexus-dk-codex
```

---

## Agent Roster

| Agent | Layer | Function |
|-------|-------|----------|
| FETTY FM | L0 | Field Marshal Orchestrator |
| NEXUS PRIMARY | L1 | Master Reasoning Engine |
| FLYWHEEL | L2 | Domain Mastery Tracker |
| BRIDGER | L3 | Cross-Domain Bridge Mapper |
| CHESS ENGINE | L4 | Collapse Probability Modeler |
| GHOST RECON | L5 | GR Node Lynchpin Miner |
| TIGER | L6 | Forensic Accounting |
| WOLF | L6 | Legal Attack |
| SUITS | L6 | ADR Synthesis |

---

*NEXUS v2.0 | Attorney Work Product | FRE 408 Protected*
