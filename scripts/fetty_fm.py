"""
fetty_fm.py — FETTY FM Orchestrator + CHESS Engine
====================================================
AI-Native Multi-Chain Agentic Module

FETTY FM = Forensic Exploration & Tactical Trigger Yield — Field Marshal
  - Routes analysis through the 7-layer NEXUS execution stack
  - Manages phase transitions, mode switching, and agent dispatch
  - Computes convergence probability via Bayesian updating

CHESS ENGINE
  - Moat Score calculation
  - Rule Pressure quantification
  - Collapse Cascade modeling
  - 72-hour settlement window trigger detection

Usage:
    from fetty_fm import FettyFM
    fm = FettyFM()
    fm.boot()
    report = fm.run_scenario("PLATFORM_REMOVAL_PINCER")
"""

import json
import datetime
import os
import sys

# Allow import from same scripts/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nexus_db import NexusDB


# ═══════════════════════════════════════════════════════════════════════════════
# FETTY FM — Field Marshal Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════
class FettyFM:
    """
    Blended agentic+programmatic orchestrator.
    Manages the 5-phase NEXUS execution pipeline.
    """

    PHASES = {
        0: "Bootstrap — file manifest, capability check, container init",
        1: "Priming — placeholder insertion, dependency mapping",
        2: "Agent Queue — task generation, parallel dispatch",
        3: "Dossier Expansion — section writing, evidence integration",
        4: "Unified Synthesis & Export",
    }

    MODES = {
        "compact":  "Deltas and ledgers only — max speed",
        "standard": "Brief summaries of changes",
        "full":     "Reprint entire active sections",
    }

    # Pre-defined scenario templates
    SCENARIOS = {
        "PLATFORM_REMOVAL_PINCER": {
            "description": "Platform removal + regulatory pincer movement",
            "priority_nodes": ["GR-004", "GR-011", "GR-012", "GR-001", "GR-002"],
            "agents": ["TIGER", "WOLF", "SUITS"],
            "fronts": ["F01_BDO_PCAOB", "F04_APPLE", "F05_GOOGLE", "F03_FTC"],
        },
        "REGULATORY_CASCADE": {
            "description": "PCAOB → MGCB → FTC synchronized enforcement cascade",
            "priority_nodes": ["GR-010", "GR-011", "GR-001", "GR-009"],
            "agents": ["TIGER", "SUITS"],
            "fronts": ["F01_BDO_PCAOB", "F02_MGCB", "F03_FTC"],
        },
        "SETTLEMENT_72HR": {
            "description": "72-hour settlement window — maximum leverage deployment",
            "priority_nodes": ["GR-001", "GR-003", "GR-004", "GR-008", "GR-011"],
            "agents": ["TIGER", "WOLF", "SUITS"],
            "fronts": ["F01_BDO_PCAOB", "F02_MGCB", "F03_FTC",
                       "F04_APPLE", "F07_CLASS_ACTION"],
        },
        "VIE_ACCOUNTING_ATTACK": {
            "description": "ASC 810 VIE consolidation + ASC 606 revenue restatement",
            "priority_nodes": ["GR-001", "GR-003", "GR-007", "GR-009"],
            "agents": ["TIGER", "SUITS"],
            "fronts": ["F01_BDO_PCAOB", "F06_SEC_EDGAR"],
        },
        "FULL_SPECTRUM": {
            "description": "All 8 fronts — Prisoner's Dilemma maximum pressure",
            "priority_nodes": ["GR-001", "GR-002", "GR-003", "GR-004", "GR-005",
                               "GR-006", "GR-007", "GR-008", "GR-009",
                               "GR-010", "GR-011", "GR-012"],
            "agents": ["TIGER", "WOLF", "SUITS", "BRIDGER", "CHESS"],
            "fronts": ["F01_BDO_PCAOB", "F02_MGCB", "F03_FTC", "F04_APPLE",
                       "F05_GOOGLE", "F06_SEC_EDGAR", "F07_CLASS_ACTION", "F08_CFTC"],
        },
    }

    def __init__(self, db: NexusDB = None, mode: str = "compact"):
        self.db = db or NexusDB()
        self.mode = mode
        self.phase = 0
        self.version = "v2.0"
        self.chess = ChessEngine(self.db)

    # ── Phase Control ─────────────────────────────────────────────────────────

    def boot(self) -> dict:
        """Phase 0: Bootstrap — capability check + container init."""
        self.phase = 0
        state = self.db.snapshot_state(phase=0, mode=self.mode)
        report = self._runtime_report(state)
        report["handshake"] = {
            "acknowledgment": "NEXUS/Omni-Vault runtime ACTIVE",
            "memory": "Available (LiteDB)",
            "github_bridge": "Manual Bridge Plan — see sync_cde.py",
            "codex_direct": "Available via GitHub Actions workflow",
        }
        report["start_signal"] = (
            "MASTER RUNTIME STATUS: INITIATED | "
            f"CURRENT PHASE: {self.phase} — CONTAINER CONTROL BOOTSTRAP | "
            f"MODE: {self.mode.upper()}"
        )
        return report

    def advance_phase(self) -> dict:
        """Advance to the next phase and return the runtime report."""
        if self.phase < 4:
            self.phase += 1
        state = self.db.snapshot_state(phase=self.phase, mode=self.mode)
        return self._runtime_report(state)

    def set_mode(self, mode: str) -> None:
        if mode in self.MODES:
            self.mode = mode

    # ── Scenario Runner ───────────────────────────────────────────────────────

    def run_scenario(self, scenario_name: str) -> dict:
        """
        Execute a named scenario:
          1. Identify priority GR nodes
          2. Enqueue agent tasks
          3. Compute CHESS metrics
          4. Return structured runtime report
        """
        if scenario_name not in self.SCENARIOS:
            return {"error": f"Unknown scenario: {scenario_name}",
                    "available": list(self.SCENARIOS.keys())}

        scenario = self.SCENARIOS[scenario_name]
        self.phase = 2  # Agent Queue phase

        # ── Step 1: Activate GR nodes ─────────────────────────────────────────
        activated_nodes = []
        for gr_id in scenario["priority_nodes"]:
            node = self.db.get_gr_node(gr_id)
            if node:
                activated_nodes.append({
                    "gr_id": gr_id,
                    "name": node["name"],
                    "impact": node["impact"],
                    "health": node["health"],
                    "agent_route": node["agent_route"],
                })

        # ── Step 2: Enqueue agent tasks ───────────────────────────────────────
        task_ids = []
        for node_info in activated_nodes:
            gr_id = node_info["gr_id"]
            node = self.db.get_gr_node(gr_id)
            ev_links = json.loads(node.get("evidence_links", "[]"))
            route = node_info["agent_route"]

            # Parse route: "TIGER+WOLF->SUITS" → ["TIGER", "WOLF"]
            agents_in_route = route.split("->")[0].split("+") if route else []
            for agent in agents_in_route:
                task_id = f"{scenario_name}_{gr_id}_{agent}_{self._ts_short()}"
                prompt = self._build_agent_prompt(agent, gr_id, node, ev_links)
                self.db.enqueue_task(
                    task_id=task_id,
                    agent=agent.strip(),
                    phase=self.phase,
                    priority=max(1, int(10 - node_info["impact"])),
                    gr_node=gr_id,
                    ev_inputs=ev_links,
                    prompt=prompt,
                )
                task_ids.append(task_id)

        # ── Step 3: CHESS metrics ─────────────────────────────────────────────
        chess_report = self.chess.full_report()

        # ── Step 4: Build runtime report ──────────────────────────────────────
        state = self.db.snapshot_state(phase=self.phase, mode=self.mode)
        report = self._runtime_report(state)
        report["scenario"] = scenario_name
        report["scenario_description"] = scenario["description"]
        report["activated_nodes"] = activated_nodes
        report["tasks_queued"] = len(task_ids)
        report["task_ids"] = task_ids
        report["chess"] = chess_report
        report["fronts"] = scenario["fronts"]

        # Alert if settlement window open
        if chess_report["moat_score"] < 0.40 or chess_report["rule_pressure"] > 0.60:
            report["ALERT"] = (
                "⚠ PLATFORM REMOVAL WINDOW OPEN — "
                f"Moat={chess_report['moat_score']:.3f} | "
                f"Pressure={chess_report['rule_pressure']:.3f} | "
                "Settlement window: 48–72 hours"
            )

        return report

    # ── Adversarial Mirror Mode ───────────────────────────────────────────────

    def adversarial_mirror(self, gr_id: str) -> dict:
        """
        FM-007: Spin up a shadow NEXUS instance arguing FROM DraftKings' perspective.
        Returns both NEXUS position and Anti-NEXUS (DK defense) position.
        """
        node = self.db.get_gr_node(gr_id)
        if not node:
            return {"error": f"GR node {gr_id} not found"}

        nexus_position = (
            f"[NEXUS] {node['name']}: Impact {node['impact']}/10 — "
            f"Evidence supports enforcement action. "
            f"Route: {node['agent_route']}. "
            f"Status: {node['status']}."
        )
        dk_defense = self._generate_dk_defense(gr_id, node)

        return {
            "gr_id": gr_id,
            "nexus_position": nexus_position,
            "dk_defense": dk_defense,
            "stress_test": self._stress_test_node(gr_id, node),
        }

    def _generate_dk_defense(self, gr_id: str, node: dict) -> str:
        """Generate the anticipated DraftKings defense argument for a GR node."""
        defenses = {
            "GR-001": ("DK Defense: GPS/Snappy operates as independent contractor; "
                       "no VIE relationship exists; DK does not absorb losses. "
                       "Counter: EV-292 margin structure proves DK sets pricing."),
            "GR-003": ("DK Defense: ARV disclosures are estimates; reasonable "
                       "variation is industry-standard. "
                       "Counter: $100/unit systematic delta across all iPad Air variants "
                       "exceeds any reasonable estimate threshold."),
            "GR-004": ("DK Defense: 'Refurbished' products were disclosed in fine print; "
                       "customers had opportunity to review. "
                       "Counter: EV-294 serial numbers prove delivery as refurbished "
                       "despite catalog representation as 'new.'"),
            "GR-011": ("DK Defense: BDO has full access to all records; "
                       "off-book channel is immaterial. "
                       "Counter: Dual-track fulfillment creates completeness assertion "
                       "failure under PCAOB AS 1105 regardless of materiality."),
            "GR-012": ("DK Defense: Privacy practices comply with App Store guidelines; "
                       "removal is speculative. "
                       "Counter: EV-298 contact harvesting without consent chain "
                       "is per-se Guideline 5.1.1 violation; 99.7% removal probability."),
        }
        return defenses.get(gr_id,
                            f"DK Defense: [Standard denial — insufficient evidence "
                            f"to establish {node['name']}]")

    def _stress_test_node(self, gr_id: str, node: dict) -> dict:
        """Run a red-team stress test on a GR node."""
        health = node["health"]
        impact = node["impact"]
        # Stress score: how much health loss can the node absorb before falling below cascade?
        cascade_trigger = node.get("cascade_trigger", 0.3)
        buffer = health - cascade_trigger
        return {
            "current_health": health,
            "cascade_trigger": cascade_trigger,
            "buffer": round(buffer, 3),
            "resilience": "HIGH" if buffer > 0.4 else "MEDIUM" if buffer > 0.2 else "LOW",
            "estimated_attacks_to_cascade": max(1, int(buffer / 0.05)),
        }

    # ── Convergence Probability (Bayesian) ───────────────────────────────────

    def compute_convergence(self) -> dict:
        """
        FM-008: Bayesian convergence probability.
        Prior: 0.50 (neutral)
        Updates: +0.05 per LITIGATION-READY bridge, +0.03 per READY GR node
        """
        prior = 0.50
        bridges = self.db.list_bridges(min_strength=0.65)
        gr_nodes = self.db.list_gr_nodes(status="READY")

        bridge_update = len(bridges) * 0.05
        node_update = len(gr_nodes) * 0.03
        # Bayesian update (simplified log-odds)
        import math
        log_odds = math.log(prior / (1 - prior))
        log_odds += bridge_update + node_update
        posterior = 1 / (1 + math.exp(-log_odds))
        posterior = min(0.99, max(0.01, posterior))

        return {
            "prior": prior,
            "bridge_updates": len(bridges),
            "node_updates": len(gr_nodes),
            "posterior_convergence_pct": round(posterior * 100, 1),
            "credibility_interval": (
                round(max(0, posterior - 0.08) * 100, 1),
                round(min(1, posterior + 0.08) * 100, 1),
            ),
            "interpretation": (
                "CONVERGENCE PROBABLE" if posterior > 0.75
                else "CONVERGENCE BUILDING" if posterior > 0.50
                else "CONVERGENCE EARLY STAGE"
            ),
        }

    # ── Internal Helpers ──────────────────────────────────────────────────────

    def _runtime_report(self, state: dict) -> dict:
        return {
            "RUNTIME_REPORT": {
                "phase": state["phase"],
                "mode": self.mode,
                "version": self.version,
                "next_auto_step": self.PHASES.get(state["phase"] + 1, "COMPLETE"),
            },
            "STATE_LEDGER": {
                "last_ev_id": state["last_ev_id"],
                "last_gr_id": state["last_gr_id"],
                "moat_score": state["moat_score"],
                "rule_pressure": state["rule_pressure"],
                "convergence_pct": state["convergence_pct"],
                "open_blockers": json.loads(state.get("open_blockers") or "[]"),
            },
        }

    def _build_agent_prompt(self, agent: str, gr_id: str,
                             node: dict, ev_links: list) -> str:
        ev_str = ", ".join(ev_links)
        prompts = {
            "TIGER": (
                f"/sim TIGER — Forensic Accounting Analysis\n"
                f"GR Node: {gr_id} — {node['name']}\n"
                f"Evidence: {ev_str}\n"
                f"Task: Apply ASC 606/810/450/820 analysis. "
                f"Identify standard violated, quantify exposure, "
                f"assess PCAOB enforcement likelihood. "
                f"Output: Accounting Analysis Memo (litigation-safe phrasing).\n"
                f"Route: -> SUITS"
            ),
            "WOLF": (
                f"/sim WOLF — Legal Attack Analysis\n"
                f"GR Node: {gr_id} — {node['name']}\n"
                f"Evidence: {ev_str}\n"
                f"Task: Apply MCPA/Lanham Act/UDAP/RICO analysis. "
                f"Map conduct to statutory elements, identify strongest case law, "
                f"anticipate defense arguments, draft arbitration-winning language. "
                f"Output: Legal Analysis Memo.\n"
                f"Route: -> SUITS"
            ),
            "BRIDGER": (
                f"/sim BRIDGER — Cross-Domain Bridge Detection\n"
                f"GR Node: {gr_id} — {node['name']}\n"
                f"Evidence: {ev_str}\n"
                f"Task: Detect cross-domain bridges (threshold ≥0.65). "
                f"Score bridge strength, generate litigation language. "
                f"Output: Bridge Inventory Update.\n"
                f"Route: -> NEXUS PRIMARY"
            ),
        }
        return prompts.get(agent, f"/sim {agent} — Analyze {gr_id} using {ev_str}")

    def _ts_short(self) -> str:
        return datetime.datetime.utcnow().strftime("%H%M%S")


# ═══════════════════════════════════════════════════════════════════════════════
# CHESS ENGINE — Collapse Probability Modeler
# ═══════════════════════════════════════════════════════════════════════════════
class ChessEngine:
    """
    Models DraftKings as a chess position.
    Tracks piece health, computes Moat Score and Rule Pressure,
    models collapse cascade timelines.
    """

    COLLAPSE_TIMELINE = [
        (1,  "Apple & Google remove app from stores"),
        (2,  "DK stock drops 18–23% (market cap -$3B)"),
        (3,  "Institutional investors file derivative suits"),
        (5,  "BDO withdraws audit opinion (GR-011 cascade)"),
        (7,  "PCAOB enforcement likely"),
        (14, "DK credit rating downgrade"),
        (21, "Settlement demand: $180–$400M"),
        (72, "~95%+ settlement probability"),
    ]

    def __init__(self, db: NexusDB):
        self.db = db

    def full_report(self) -> dict:
        """Compute full CHESS report: moat, pressure, cascade, settlement window."""
        moat = self.db.compute_moat_score()
        pressure = self.db.compute_rule_pressure()
        pieces = self.db.conn.execute(
            "SELECT piece_name, business_func, health, collapse_weight "
            "FROM chess_pieces ORDER BY collapse_weight DESC"
        ).fetchall()

        piece_status = []
        for p in pieces:
            piece_status.append({
                "piece": p["piece_name"],
                "function": p["business_func"],
                "health": round(p["health"], 3),
                "weight": p["collapse_weight"],
                "contribution": round(p["health"] * p["collapse_weight"], 4),
                "status": (
                    "CRITICAL" if p["health"] < 0.30
                    else "DEGRADED" if p["health"] < 0.60
                    else "STABLE"
                ),
            })

        # Settlement window detection
        settlement_open = moat < 0.40 or pressure > 0.60
        settlement_range = self._compute_settlement_range(moat, pressure)

        return {
            "moat_score": moat,
            "rule_pressure": pressure,
            "settlement_window_open": settlement_open,
            "settlement_range": settlement_range,
            "piece_status": piece_status,
            "collapse_timeline": (
                self.COLLAPSE_TIMELINE if settlement_open else []
            ),
            "collapse_probability": self._collapse_probability(moat, pressure),
        }

    def apply_gr_damage(self, gr_id: str) -> dict:
        """
        Apply damage from a triggered GR node to linked chess pieces.
        Returns dict of pieces affected and new health values.
        """
        node = self.db.get_gr_node(gr_id)
        if not node:
            return {}
        # Find pieces linked to this GR node
        pieces = self.db.conn.execute(
            "SELECT piece_name, health, vulnerability, gr_damage_links "
            "FROM chess_pieces"
        ).fetchall()
        affected = {}
        for piece in pieces:
            links = json.loads(piece["gr_damage_links"] or "[]")
            if gr_id in links:
                damage = node["impact"] * 0.01 * piece["vulnerability"]
                new_health = max(0.0, piece["health"] - damage)
                self.db.conn.execute(
                    "UPDATE chess_pieces SET health=?, updated_at=datetime('now') "
                    "WHERE piece_name=?",
                    (new_health, piece["piece_name"]))
                self.db.conn.execute("""
                    INSERT INTO chess_health_log
                        (piece_name, health_old, health_new, delta, gr_trigger)
                    VALUES (?,?,?,?,?)
                """, (piece["piece_name"], piece["health"],
                      new_health, -damage, gr_id))
                affected[piece["piece_name"]] = {
                    "old": round(piece["health"], 3),
                    "new": round(new_health, 3),
                    "damage": round(damage, 4),
                }
        self.db.conn.commit()
        return affected

    def _collapse_probability(self, moat: float, pressure: float) -> float:
        """Heuristic collapse probability from moat + pressure."""
        # Inverse moat × pressure amplifier
        raw = (1 - moat) * 0.6 + pressure * 0.4
        return round(min(0.99, max(0.01, raw)), 3)

    def _compute_settlement_range(self, moat: float, pressure: float) -> dict:
        """
        Settlement range based on moat score and rule pressure.
        Scenario A (platform removal): $180M–$250M
        Scenario B (no platform removal): $75M–$125M
        Scenario C (litigation escalation): $250M–$400M
        """
        if pressure > 0.70:
            return {"scenario": "A", "low": 180_000_000, "high": 250_000_000,
                    "label": "Platform removal triggers — $180M–$250M"}
        elif pressure > 0.40:
            return {"scenario": "B", "low": 75_000_000, "high": 125_000_000,
                    "label": "No platform removal — $75M–$125M"}
        else:
            return {"scenario": "C", "low": 250_000_000, "high": 400_000_000,
                    "label": "Litigation escalation — $250M–$400M"}


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    db = NexusDB()
    db.init_schema()
    db.seed_gr_nodes()
    db.seed_chess_pieces()
    db.seed_evidence_register()

    fm = FettyFM(db=db, mode="compact")
    print("=" * 60)
    print("FETTY FM — NEXUS ORCHESTRATOR BOOT")
    print("=" * 60)

    boot = fm.boot()
    print(json.dumps(boot, indent=2))

    print("\n" + "=" * 60)
    print("RUNNING SCENARIO: PLATFORM_REMOVAL_PINCER")
    print("=" * 60)
    report = fm.run_scenario("PLATFORM_REMOVAL_PINCER")
    # Print key sections
    print(json.dumps(report["RUNTIME_REPORT"], indent=2))
    print(json.dumps(report["STATE_LEDGER"], indent=2))
    print(f"\nScenario: {report['scenario_description']}")
    print(f"Tasks Queued: {report['tasks_queued']}")
    chess = report["chess"]
    print(f"\nMoat Score:      {chess['moat_score']:.4f}")
    print(f"Rule Pressure:   {chess['rule_pressure']:.4f}")
    print(f"Settlement Open: {chess['settlement_window_open']}")
    print(f"Settlement:      {chess['settlement_range']['label']}")
    print(f"Collapse Prob:   {chess['collapse_probability']:.1%}")
    if "ALERT" in report:
        print(f"\n{report['ALERT']}")

    print("\n" + "=" * 60)
    print("CONVERGENCE PROBABILITY (BAYESIAN)")
    print("=" * 60)
    conv = fm.compute_convergence()
    print(json.dumps(conv, indent=2))

    print("\n" + "=" * 60)
    print("ADVERSARIAL MIRROR — GR-004")
    print("=" * 60)
    mirror = fm.adversarial_mirror("GR-004")
    print(f"NEXUS:  {mirror['nexus_position']}")
    print(f"DK Def: {mirror['dk_defense']}")
    print(f"Stress: {mirror['stress_test']}")

    db.close()
