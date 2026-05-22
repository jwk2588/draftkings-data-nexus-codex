"""
sync_cde.py — GitHub Nexus Codex Repo Sync Engine
===================================================
AI-Native | GitHub-Native | Data Artifact Architecture

This module manages the bidirectional sync between the NEXUS LiteDB
and the GitHub Nexus Codex Repository. It implements:

  1. DATA ARTIFACT LAYER
     - Evidence register → /data/evidence/EV-NNN.json
     - GR node registry  → /data/gr_nodes/GR-NNN.json
     - Bridge inventory  → /data/bridges/Bridge-NNN.json
     - System state      → /data/state/snapshot_YYYYMMDD_HHMMSS.json

  2. HIDDEN DEVTOOLS SCHEMA LAYER
     - Raw text extracts → /devtools/raw_text/
     - AI-written schema → /devtools/schema/
     - Copilot prompts   → /devtools/copilot_prompts/
     - Validation scripts→ /devtools/validators/

  3. SYNC ENGINE
     - Reads pending sync queue from NexusDB
     - Writes JSON artifacts to local repo clone
     - Commits and pushes via GitHub CLI (gh)
     - Updates sync queue with commit SHA

Usage:
    python sync_cde.py --repo <owner/repo> --clone-dir /tmp/nexus_codex
    python sync_cde.py --export-only  # Export to local dir without pushing
"""

import json
import os
import sys
import subprocess
import datetime
import shutil
import argparse
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nexus_db import NexusDB


# ═══════════════════════════════════════════════════════════════════════════════
# Repo Architecture Constants
# ═══════════════════════════════════════════════════════════════════════════════
REPO_STRUCTURE = {
    # ── Data Artifact Layer (public-facing) ───────────────────────────────────
    "data/evidence":          "EV-NNN.json — Evidence register artifacts",
    "data/gr_nodes":          "GR-NNN.json — GR node registry artifacts",
    "data/bridges":           "Bridge-NNN.json — Cross-domain bridge artifacts",
    "data/chess":             "chess_state.json — CHESS piece health snapshot",
    "data/state":             "snapshot_*.json — System state snapshots",
    "data/adr_packages":      "ADR-*.json — SUITS ADR package exports",

    # ── Hidden DevTools Schema Layer ──────────────────────────────────────────
    "devtools/raw_text":      "Raw text extracts from source documents",
    "devtools/schema":        "AI-written schema definitions (JSON Schema / YAML)",
    "devtools/copilot_prompts": "GitHub Copilot prompt templates",
    "devtools/validators":    "Validation and hygiene scripts",
    "devtools/llm_outputs":   "Cached LLM outputs (TIGER/WOLF/SUITS memos)",
    "devtools/graph":         "Graph adjacency lists and NetworkX exports",

    # ── Agent Module Layer ────────────────────────────────────────────────────
    "agents/tiger":           "TIGER forensic accounting module",
    "agents/wolf":            "WOLF legal attack module",
    "agents/suits":           "SUITS ADR synthesis module",
    "agents/bridger":         "BRIDGER cross-domain mapper",
    "agents/chess":           "CHESS moat calculator",
    "agents/fetty_fm":        "FETTY FM orchestrator",

    # ── GitHub Actions Workflows ──────────────────────────────────────────────
    ".github/workflows":      "CI/CD automation workflows",

    # ── Documentation ─────────────────────────────────────────────────────────
    "docs":                   "Architecture documentation",
    "docs/schemas":           "Data dictionary and schema docs",
}


# ═══════════════════════════════════════════════════════════════════════════════
# NexusSyncEngine
# ═══════════════════════════════════════════════════════════════════════════════
class NexusSyncEngine:
    """
    Manages the GitHub Nexus Codex Repo sync.
    Exports NEXUS database artifacts as structured JSON files.
    """

    def __init__(self, db: NexusDB = None,
                 clone_dir: str = "/tmp/nexus_codex",
                 repo: str = None):
        self.db = db or NexusDB()
        self.clone_dir = Path(clone_dir)
        self.repo = repo  # e.g. "username/nexus-dk-codex"

    # ── Repo Initialization ───────────────────────────────────────────────────

    def init_repo_structure(self) -> List[str]:
        """Create the full repo directory structure locally."""
        created = []
        for path, description in REPO_STRUCTURE.items():
            full_path = self.clone_dir / path
            full_path.mkdir(parents=True, exist_ok=True)
            # Write a .gitkeep with description
            gitkeep = full_path / ".gitkeep"
            if not gitkeep.exists():
                gitkeep.write_text(f"# {description}\n")
            created.append(str(full_path))
        return created

    def clone_or_init(self) -> bool:
        """Clone existing repo or initialize a new one."""
        if self.clone_dir.exists() and (self.clone_dir / ".git").exists():
            return True  # Already cloned
        if self.repo:
            result = subprocess.run(
                ["gh", "repo", "clone", self.repo, str(self.clone_dir)],
                capture_output=True, text=True)
            return result.returncode == 0
        else:
            # Init local repo
            self.clone_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "init"], cwd=str(self.clone_dir),
                           capture_output=True)
            return True

    # ── Data Artifact Export ──────────────────────────────────────────────────

    def export_all_artifacts(self) -> Dict:
        """Export all NEXUS database artifacts to the repo directory."""
        self.init_repo_structure()
        report = {
            "evidence": self._export_evidence(),
            "gr_nodes": self._export_gr_nodes(),
            "bridges": self._export_bridges(),
            "chess": self._export_chess(),
            "state": self._export_state_snapshot(),
            "schema": self._export_schema(),
            "copilot_prompts": self._export_copilot_prompts(),
            "graph": self._export_graph(),
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
        return report

    def _export_evidence(self) -> int:
        """Export each evidence record as EV-NNN.json."""
        evidence = self.db.list_evidence()
        path = self.clone_dir / "data" / "evidence"
        for ev in evidence:
            ev_id = ev["ev_id"]
            # Parse gr_links from JSON string
            try:
                ev["gr_links"] = json.loads(ev.get("gr_links") or "[]")
            except Exception:
                pass
            artifact = {
                "_artifact_type": "evidence",
                "_nexus_version": "v2.0",
                "_classification": "Attorney Work Product",
                **ev,
            }
            (path / f"{ev_id}.json").write_text(
                json.dumps(artifact, indent=2, default=str))
        return len(evidence)

    def _export_gr_nodes(self) -> int:
        """Export each GR node as GR-NNN.json."""
        nodes = self.db.list_gr_nodes()
        path = self.clone_dir / "data" / "gr_nodes"
        for node in nodes:
            gr_id = node["gr_id"]
            for field in ["evidence_links", "cascade_targets", "domain_tags"]:
                try:
                    node[field] = json.loads(node.get(field) or "[]")
                except Exception:
                    pass
            artifact = {
                "_artifact_type": "gr_node",
                "_nexus_version": "v2.0",
                "_classification": "Attorney Work Product",
                **node,
            }
            (path / f"{gr_id}.json").write_text(
                json.dumps(artifact, indent=2, default=str))
        return len(nodes)

    def _export_bridges(self) -> int:
        """Export each bridge as Bridge-NNN.json."""
        bridges = self.db.list_bridges(min_strength=0.50)
        path = self.clone_dir / "data" / "bridges"
        for bridge in bridges:
            bid = bridge["bridge_id"].replace("/", "-")
            for field in ["ev_links", "gr_links", "validated_by"]:
                try:
                    bridge[field] = json.loads(bridge.get(field) or "[]")
                except Exception:
                    pass
            artifact = {
                "_artifact_type": "bridge",
                "_nexus_version": "v2.0",
                **bridge,
            }
            (path / f"{bid}.json").write_text(
                json.dumps(artifact, indent=2, default=str))
        return len(bridges)

    def _export_chess(self) -> bool:
        """Export chess piece health state."""
        from fetty_fm import ChessEngine
        chess = ChessEngine(self.db)
        report = chess.full_report()
        path = self.clone_dir / "data" / "chess"
        (path / "chess_state.json").write_text(
            json.dumps({
                "_artifact_type": "chess_state",
                "_nexus_version": "v2.0",
                **report,
            }, indent=2, default=str))
        return True

    def _export_state_snapshot(self) -> bool:
        """Export current system state snapshot."""
        state = self.db.snapshot_state()
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self.clone_dir / "data" / "state"
        (path / f"snapshot_{ts}.json").write_text(
            json.dumps({
                "_artifact_type": "system_state",
                "_nexus_version": "v2.0",
                **state,
            }, indent=2, default=str))
        return True

    # ── Hidden DevTools Schema Layer ──────────────────────────────────────────

    def _export_schema(self) -> bool:
        """Export AI-written JSON Schema definitions."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "NEXUS Evidence Schema",
            "description": "Schema for NEXUS EV-NNN evidence artifacts",
            "type": "object",
            "required": ["ev_id", "shortname", "status", "confidence"],
            "properties": {
                "ev_id": {"type": "string", "pattern": "^EV-\\d{3}$"},
                "shortname": {"type": "string"},
                "ev_date": {"type": ["string", "null"], "format": "date"},
                "status": {"type": "string",
                           "enum": ["CONFIRMED", "TENTATIVE", "CHALLENGED"]},
                "source_file": {"type": ["string", "null"]},
                "confidence": {"type": "string", "enum": ["T1", "T2", "T3", "T4"]},
                "gr_links": {"type": "array",
                             "items": {"type": "string", "pattern": "^GR-\\d{3}$"}},
                "chain_prev": {"type": ["string", "null"]},
                "chain_next": {"type": ["string", "null"]},
                "notes": {"type": ["string", "null"]},
            },
        }
        path = self.clone_dir / "devtools" / "schema"
        (path / "evidence_schema.json").write_text(json.dumps(schema, indent=2))

        gr_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "NEXUS GR Node Schema",
            "type": "object",
            "required": ["gr_id", "name", "impact", "health", "status"],
            "properties": {
                "gr_id": {"type": "string", "pattern": "^GR-\\d{3}$"},
                "name": {"type": "string"},
                "impact": {"type": "number", "minimum": 0, "maximum": 10},
                "health": {"type": "number", "minimum": 0, "maximum": 1},
                "status": {"type": "string",
                           "enum": ["READY", "PENDING", "BLOCKED", "FIRED"]},
                "agent_route": {"type": ["string", "null"]},
                "evidence_links": {"type": "array"},
                "cascade_trigger": {"type": "number"},
                "cascade_targets": {"type": "array"},
                "node_category": {"type": "string",
                                  "enum": ["Alpha", "Beta", "Gamma", "Delta", "Omega"]},
            },
        }
        (path / "gr_node_schema.json").write_text(json.dumps(gr_schema, indent=2))
        return True

    def _export_copilot_prompts(self) -> bool:
        """Export GitHub Copilot prompt templates for each agent."""
        prompts = {
            "tiger_prompt.md": (
                "# TIGER — Forensic Accounting Copilot Prompt\n\n"
                "You are TIGER, a forensic accounting specialist.\n"
                "Apply the following ASC standards to the provided GR node:\n"
                "- ASC 606: Revenue Recognition (5-step model)\n"
                "- ASC 810: VIE Consolidation (3-prong test)\n"
                "- ASC 450: Contingencies (probable + estimable)\n"
                "- ASC 820: Fair Value (Level 1/2/3 hierarchy)\n"
                "- ASC 830: Foreign Currency (remeasurement)\n\n"
                "Output format: Accounting Analysis Memo\n"
                "Quality gate: Litigation-safe phrasing (suggests/raises questions)\n"
                "Route: -> SUITS\n"
            ),
            "wolf_prompt.md": (
                "# WOLF — Legal Attack Copilot Prompt\n\n"
                "You are WOLF, an aggressive legal strategist.\n"
                "Apply the following statutes to the provided GR node:\n"
                "- MCPA § 445.903 (Michigan Consumer Protection)\n"
                "- Lanham Act § 1125(a) (False Advertising)\n"
                "- FTC Act § 5 (Unfair/Deceptive Acts)\n"
                "- RICO 18 U.S.C. § 1962 (Enterprise Theory)\n"
                "- ROSCA (Negative Option)\n\n"
                "Output format: Legal Analysis Memo\n"
                "Quality gate: Arbitration-winning (prove scienter)\n"
                "Route: -> SUITS\n"
            ),
            "suits_prompt.md": (
                "# SUITS — ADR Synthesis Copilot Prompt\n\n"
                "You are SUITS, an ADR synthesis specialist.\n"
                "Aggregate TIGER + WOLF memos and compute:\n"
                "1. Settlement band (Opening/Midpoint/Bottom)\n"
                "2. 8-front Prisoner's Dilemma model\n"
                "3. 72-hour settlement timeline\n"
                "4. FRE 408 demand letter\n\n"
                "Formula:\n"
                "  Opening = Documented × 3 × 1.5\n"
                "  Midpoint = Opening × 0.5\n"
                "  Bottom = Documented × 1.2\n"
            ),
            "bridger_prompt.md": (
                "# BRIDGER — Cross-Domain Bridge Copilot Prompt\n\n"
                "You are BRIDGER, a cross-domain analog mapper.\n"
                "Detect bridges between domains. Threshold ≥0.65 = LITIGATION-READY.\n\n"
                "Bridge types:\n"
                "- Temporal: Events across time showing pattern evolution\n"
                "- Jurisdictional: Same conduct across legal regimes\n"
                "- Scalar: Micro harm to macro class exposure\n"
                "- Adversarial: Plaintiff argument vs. DK's own defense weakness\n"
                "- Narrative: Factual pattern to cultural/political narrative\n\n"
                "Output: Bridge Inventory Update with litigation_text\n"
            ),
        }
        path = self.clone_dir / "devtools" / "copilot_prompts"
        for filename, content in prompts.items():
            (path / filename).write_text(content)
        return True

    def _export_graph(self) -> bool:
        """Export GR node graph as adjacency list JSON."""
        nodes = self.db.list_gr_nodes()
        adjacency = {}
        for node in nodes:
            gr_id = node["gr_id"]
            cascade_targets = json.loads(node.get("cascade_targets") or "[]")
            ev_links = json.loads(node.get("evidence_links") or "[]")
            adjacency[gr_id] = {
                "name": node["name"],
                "impact": node["impact"],
                "health": node["health"],
                "edges_cascade": cascade_targets,
                "edges_evidence": ev_links,
            }
        path = self.clone_dir / "devtools" / "graph"
        (path / "gr_adjacency.json").write_text(
            json.dumps(adjacency, indent=2))

        # Also write Mermaid diagram source
        mermaid_lines = ["graph TD"]
        for gr_id, data in adjacency.items():
            label = data["name"].replace(" ", "_")[:20]
            mermaid_lines.append(f'    {gr_id}["{gr_id}: {label}"]')
            for target in data["edges_cascade"]:
                mermaid_lines.append(f'    {gr_id} -->|cascade| {target}')
        (path / "gr_graph.mmd").write_text("\n".join(mermaid_lines))
        return True

    # ── GitHub Actions Workflows ──────────────────────────────────────────────

    def write_github_workflows(self) -> List[str]:
        """Write GitHub Actions YAML workflows to .github/workflows/."""
        workflows_dir = self.clone_dir / ".github" / "workflows"
        workflows_dir.mkdir(parents=True, exist_ok=True)
        written = []

        # ── Workflow 1: NEXUS Nightly Sync ────────────────────────────────────
        nightly = """name: NEXUS Nightly Sync
on:
  schedule:
    - cron: '0 2 * * *'   # 2am UTC daily
  workflow_dispatch:
    inputs:
      scenario:
        description: 'Scenario to run (PLATFORM_REMOVAL_PINCER, FULL_SPECTRUM, etc.)'
        required: false
        default: 'FULL_SPECTRUM'

jobs:
  nexus-sync:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.NEXUS_GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Bootstrap NEXUS DB
        run: python scripts/nexus_db.py
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Run FETTY FM Scenario
        run: |
          python -c "
          import sys, json
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          from fetty_fm import FettyFM
          from agent_pipelines import BridgerPipeline
          db = NexusDB()
          bridger = BridgerPipeline(db=db)
          bridger.seed_bridges()
          bridger.full_bridge_scan()
          fm = FettyFM(db=db)
          report = fm.run_scenario('${{ github.event.inputs.scenario || \"FULL_SPECTRUM\" }}')
          print(json.dumps(report, indent=2, default=str))
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Export Artifacts
        run: |
          python -c "
          import sys
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          from sync_cde import NexusSyncEngine
          db = NexusDB()
          engine = NexusSyncEngine(db=db, clone_dir='.')
          report = engine.export_all_artifacts()
          import json
          print(json.dumps(report, indent=2, default=str))
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Commit artifacts
        run: |
          git config user.name "NEXUS-Bot"
          git config user.email "nexus-bot@noreply.github.com"
          git add data/ devtools/ docs/
          git diff --staged --quiet || git commit -m "chore: NEXUS nightly sync $(date -u +%Y-%m-%dT%H:%M:%SZ)"
          git push
"""
        (workflows_dir / "nexus_nightly_sync.yml").write_text(nightly)
        written.append("nexus_nightly_sync.yml")

        # ── Workflow 2: Evidence Ingestion ────────────────────────────────────
        ingest = """name: Evidence Ingestion
on:
  push:
    paths:
      - 'devtools/raw_text/**'
  workflow_dispatch:
    inputs:
      ev_id:
        description: 'Evidence ID to ingest (e.g. EV-300)'
        required: true
      source_file:
        description: 'Source filename'
        required: true

jobs:
  ingest-evidence:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.NEXUS_GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Ingest Evidence
        run: |
          python -c "
          import sys, json
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          db = NexusDB()
          db.init_schema()
          db.upsert_evidence(
              ev_id='${{ github.event.inputs.ev_id }}',
              shortname='${{ github.event.inputs.source_file }}',
              source_file='${{ github.event.inputs.source_file }}',
              status='TENTATIVE',
              confidence='T2',
          )
          print(f'Ingested: ${{ github.event.inputs.ev_id }}')
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Export updated evidence
        run: |
          python -c "
          import sys
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          from sync_cde import NexusSyncEngine
          db = NexusDB()
          engine = NexusSyncEngine(db=db, clone_dir='.')
          engine._export_evidence()
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Commit
        run: |
          git config user.name "NEXUS-Bot"
          git config user.email "nexus-bot@noreply.github.com"
          git add data/evidence/
          git diff --staged --quiet || git commit -m "feat: ingest ${{ github.event.inputs.ev_id }}"
          git push
"""
        (workflows_dir / "evidence_ingestion.yml").write_text(ingest)
        written.append("evidence_ingestion.yml")

        # ── Workflow 3: ADR Package Generator ────────────────────────────────
        adr = """name: ADR Package Generator
on:
  workflow_dispatch:
    inputs:
      gr_nodes:
        description: 'Comma-separated GR node IDs (e.g. GR-001,GR-003,GR-011)'
        required: false
        default: 'GR-001,GR-003,GR-004,GR-008,GR-011'

jobs:
  generate-adr:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.NEXUS_GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Generate ADR Package
        run: |
          python -c "
          import sys, json, datetime
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          from agent_pipelines import SuitsPipeline, BridgerPipeline
          db = NexusDB()
          db.init_schema()
          db.seed_gr_nodes()
          db.seed_chess_pieces()
          db.seed_evidence_register()
          bridger = BridgerPipeline(db=db)
          bridger.seed_bridges()
          gr_ids = '${{ github.event.inputs.gr_nodes }}'.split(',')
          suits = SuitsPipeline(db=db)
          pkg = suits.synthesize_adr_package(gr_ids)
          ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
          import pathlib
          pathlib.Path('data/adr_packages').mkdir(parents=True, exist_ok=True)
          with open(f'data/adr_packages/ADR_{ts}.json', 'w') as f:
              json.dump(pkg, f, indent=2, default=str)
          print(json.dumps(pkg['settlement_band'], indent=2))
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db

      - name: Commit ADR package
        run: |
          git config user.name "NEXUS-Bot"
          git config user.email "nexus-bot@noreply.github.com"
          git add data/adr_packages/
          git diff --staged --quiet || git commit -m "feat: ADR package generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
          git push
"""
        (workflows_dir / "adr_package_generator.yml").write_text(adr)
        written.append("adr_package_generator.yml")

        # ── Workflow 4: CHESS Moat Monitor ────────────────────────────────────
        chess_monitor = """name: CHESS Moat Monitor
on:
  schedule:
    - cron: '0 */6 * * *'   # Every 6 hours
  workflow_dispatch:

jobs:
  moat-monitor:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run CHESS Monitor
        run: |
          python -c "
          import sys, json
          sys.path.insert(0, 'scripts')
          from nexus_db import NexusDB
          from fetty_fm import ChessEngine
          db = NexusDB()
          db.init_schema()
          db.seed_gr_nodes()
          db.seed_chess_pieces()
          chess = ChessEngine(db)
          report = chess.full_report()
          print('[CHESS MONITOR]')
          print(f'Moat Score:    {report[\"moat_score\"]:.4f}')
          print(f'Rule Pressure: {report[\"rule_pressure\"]:.4f}')
          print(f'Settlement:    {report[\"settlement_window_open\"]}')
          print(f'Collapse Prob: {report[\"collapse_probability\"]:.1%}')
          if report['settlement_window_open']:
              print('ALERT: Settlement window OPEN')
          db.close()
          "
        env:
          NEXUS_DB_PATH: ${{ github.workspace }}/nexus_master.db
"""
        (workflows_dir / "chess_moat_monitor.yml").write_text(chess_monitor)
        written.append("chess_moat_monitor.yml")

        return written

    # ── README + Requirements ─────────────────────────────────────────────────

    def write_repo_readme(self) -> None:
        """Write the master README for the GitHub Nexus Codex Repo."""
        readme = """# NEXUS DraftKings Codex Repository
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
"""
        (self.clone_dir / "README.md").write_text(readme)

    def write_requirements(self) -> None:
        """Write requirements.txt for the repo."""
        reqs = """# NEXUS DraftKings Codex — Python Requirements
# Core (no external ML deps required for base operation)
# Optional ML deps for BRIDGER semantic scoring:
# sentence-transformers>=2.2.0
# torch>=2.0.0
"""
        (self.clone_dir / "requirements.txt").write_text(reqs)

    # ── Full Export ───────────────────────────────────────────────────────────

    def full_export(self) -> Dict:
        """Run the complete export: structure + artifacts + workflows + docs."""
        self.clone_or_init()
        dirs = self.init_repo_structure()
        artifacts = self.export_all_artifacts()
        workflows = self.write_github_workflows()
        self.write_repo_readme()
        self.write_requirements()
        return {
            "directories_created": len(dirs),
            "artifacts": artifacts,
            "workflows_written": workflows,
            "repo_root": str(self.clone_dir),
        }

    # ── GitHub Push ───────────────────────────────────────────────────────────

    def push_to_github(self, commit_message: str = None) -> bool:
        """Commit and push all changes to GitHub."""
        if not (self.clone_dir / ".git").exists():
            subprocess.run(["git", "init"], cwd=str(self.clone_dir))
        msg = commit_message or (
            f"chore: NEXUS full export "
            f"{datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        subprocess.run(["git", "add", "-A"], cwd=str(self.clone_dir))
        result = subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=str(self.clone_dir), capture_output=True, text=True)
        if "nothing to commit" in result.stdout:
            return True
        if result.returncode != 0:
            return False
        push = subprocess.run(
            ["git", "push"], cwd=str(self.clone_dir),
            capture_output=True, text=True)
        return push.returncode == 0

    def create_github_repo(self, repo_name: str = "nexus-dk-codex",
                           private: bool = True) -> Optional[str]:
        """Create a new private GitHub repo using gh CLI."""
        result = subprocess.run(
            ["gh", "repo", "create", repo_name,
             "--private" if private else "--public",
             "--description", "NEXUS DraftKings Codex — AI-Native Litigation Intelligence",
             "--clone"],
            capture_output=True, text=True)
        if result.returncode == 0:
            return repo_name
        return None


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NEXUS Codex Sync Engine")
    parser.add_argument("--repo", help="GitHub repo (owner/name)", default=None)
    parser.add_argument("--clone-dir", default="/tmp/nexus_codex",
                        help="Local directory for repo clone")
    parser.add_argument("--export-only", action="store_true",
                        help="Export artifacts without pushing to GitHub")
    args = parser.parse_args()

    db = NexusDB()
    db.init_schema()
    db.seed_gr_nodes()
    db.seed_chess_pieces()
    db.seed_evidence_register()

    # Seed bridges
    from agent_pipelines import BridgerPipeline
    bridger = BridgerPipeline(db=db)
    bridger.seed_bridges()

    engine = NexusSyncEngine(db=db, clone_dir=args.clone_dir, repo=args.repo)
    print("=" * 60)
    print("NEXUS CODEX REPO EXPORT")
    print("=" * 60)
    report = engine.full_export()
    print(f"Directories created: {report['directories_created']}")
    print(f"Evidence artifacts:  {report['artifacts']['evidence']}")
    print(f"GR node artifacts:   {report['artifacts']['gr_nodes']}")
    print(f"Bridge artifacts:    {report['artifacts']['bridges']}")
    print(f"Workflows written:   {report['workflows_written']}")
    print(f"Repo root:           {report['repo_root']}")

    if not args.export_only and args.repo:
        pushed = engine.push_to_github()
        print(f"GitHub push:         {'SUCCESS' if pushed else 'FAILED'}")

    db.close()
    print("\n=== NEXUS CODEX REPO READY ===")
