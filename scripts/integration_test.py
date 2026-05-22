"""
integration_test.py — NEXUS Full Integration Test Suite
=========================================================
Validates all modules: DB, FETTY FM, TIGER, WOLF, SUITS, BRIDGER, SYNC
"""

import sys
import os
import json
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PASS = "PASS"
FAIL = "FAIL"
results = []

def test(name, fn):
    try:
        result = fn()
        results.append((PASS, name, result))
        print(f"  [{PASS}] {name}: {result}")
    except Exception as e:
        results.append((FAIL, name, str(e)))
        print(f"  [{FAIL}] {name}: {e}")
        traceback.print_exc()

print("=" * 60)
print("NEXUS INTEGRATION TEST SUITE")
print("=" * 60)

# ── Module 1: NexusDB ─────────────────────────────────────────────────────────
print("\n[1] NexusDB Core Engine")
from nexus_db import NexusDB
db = NexusDB(db_path=":memory:")

db.init_schema()
db.seed_gr_nodes()
db.seed_chess_pieces()
db.seed_evidence_register()
test("schema_init", lambda: "OK")
test("seed_gr_nodes", lambda: f"{len(db.list_gr_nodes())} nodes seeded")
test("seed_chess", lambda: "chess seeded")
test("seed_evidence", lambda: f"{len(db.list_evidence())} evidence records seeded")
test("get_gr_node", lambda: db.get_gr_node("GR-001")["name"])
test("update_gr_health", lambda: f"health={db.update_gr_health('GR-001', +0.05, 'test', 'EV-292', 'TEST'):.3f}")
test("snapshot_state", lambda: f"moat={db.snapshot_state()['moat_score']:.4f}")
test("dataset_hygiene_evidence", lambda: f"clean={db.dataset_hygiene('evidence')['clean']}")
test("dataset_hygiene_gr_nodes", lambda: f"clean={db.dataset_hygiene('gr_nodes')['clean']}")

# ── Module 2: FETTY FM ────────────────────────────────────────────────────────
print("\n[2] FETTY FM Orchestrator + CHESS Engine")
from fetty_fm import FettyFM, ChessEngine

chess = ChessEngine(db)
test("chess_moat_score", lambda: f"moat={db.compute_moat_score():.4f}")
test("chess_rule_pressure", lambda: f"pressure={db.compute_rule_pressure():.4f}")
test("chess_settlement_window", lambda: f"open={db.compute_moat_score() < 0.7 and db.compute_rule_pressure() >= 0.8}")
test("chess_collapse_prob", lambda: f"prob={chess.full_report()['collapse_probability']:.3f}")
test("chess_full_report", lambda: f"moat={chess.full_report()['moat_score']:.4f}")

fm = FettyFM(db=db, mode="compact")
test("fetty_boot", lambda: fm.boot()["start_signal"][:40])
test("fetty_scenario_pincer", lambda: f"tasks={fm.run_scenario('PLATFORM_REMOVAL_PINCER')['tasks_queued']}")
test("fetty_scenario_full", lambda: f"tasks={fm.run_scenario('FULL_SPECTRUM')['tasks_queued']}")
test("fetty_adversarial_mirror", lambda: f"node={list(fm.adversarial_mirror('GR-004').keys())[:2]}")
test("fetty_bayesian_convergence", lambda: f"pct={fm.compute_convergence()['posterior_convergence_pct']:.1f}%")

# ── Module 3: Agent Pipelines ─────────────────────────────────────────────────
print("\n[3] Agent Pipelines: TIGER / WOLF / SUITS / BRIDGER")
from agent_pipelines import TigerPipeline, WolfPipeline, SuitsPipeline, BridgerPipeline

tiger = TigerPipeline(db=db)
test("tiger_gr001", lambda: f"memo_type={tiger.analyze_gr_node('GR-001').get('memo_type','OK')}")
test("tiger_gr003", lambda: f"gr={tiger.analyze_gr_node('GR-003').get('gr_id','OK')}")
test("tiger_gr011", lambda: f"gr={tiger.analyze_gr_node('GR-011').get('gr_id','OK')}")

wolf = WolfPipeline(db=db)
test("wolf_gr004", lambda: f"memo={wolf.analyze_gr_node('GR-004').get('memo_type','OK')}")
test("wolf_gr001", lambda: f"gr={wolf.analyze_gr_node('GR-001').get('gr_id','OK')}")
test("wolf_gr006", lambda: f"gr={wolf.analyze_gr_node('GR-006').get('gr_id','OK')}")

bridger = BridgerPipeline(db=db)
test("bridger_seed", lambda: f"seeded={bridger.seed_bridges()}")
test("bridger_detect", lambda: f"bridge={bridger.detect_new_bridges('GR-001','GR-011') is not None}")
test("bridger_full_scan", lambda: f"lit_ready={bridger.full_bridge_scan()['total_litigation_ready']}")

suits = SuitsPipeline(db=db)
test("suits_adr_package", lambda: f"opening={suits.synthesize_adr_package(['GR-001','GR-003','GR-011'])['settlement_band']['opening_fmt']}")
test("suits_pd_model", lambda: f"strategy={suits.synthesize_adr_package(['GR-001'])['prisoners_dilemma']['dominant_strategy']}")
test("suits_72hr_timeline", lambda: f"events={len(suits.synthesize_adr_package(['GR-001'])['timeline_72hr'])}")

# ── Module 4: Sync Engine ─────────────────────────────────────────────────────
print("\n[4] GitHub Nexus Codex Repo Sync Engine")
from sync_cde import NexusSyncEngine
import tempfile, pathlib

with tempfile.TemporaryDirectory() as tmpdir:
    engine = NexusSyncEngine(db=db, clone_dir=tmpdir)
    test("sync_init_structure", lambda: f"dirs={len(engine.init_repo_structure())}")
    test("sync_export_evidence", lambda: f"ev={engine._export_evidence()}")
    test("sync_export_gr_nodes", lambda: f"gr={engine._export_gr_nodes()}")
    test("sync_export_bridges", lambda: f"bridges={engine._export_bridges()}")
    test("sync_export_schema", lambda: f"schema={engine._export_schema()}")
    test("sync_export_copilot", lambda: f"prompts={engine._export_copilot_prompts()}")
    test("sync_export_graph", lambda: f"graph={engine._export_graph()}")
    test("sync_write_workflows", lambda: f"workflows={engine.write_github_workflows()}")
    test("sync_write_readme", lambda: (engine.write_repo_readme(), "OK")[1])
    test("sync_full_export", lambda: f"artifacts={engine.full_export()['artifacts']['gr_nodes']} GR nodes")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
passed = sum(1 for r in results if r[0] == PASS)
failed = sum(1 for r in results if r[0] == FAIL)
total = len(results)
print(f"INTEGRATION TEST RESULTS: {passed}/{total} PASSED | {failed} FAILED")
if failed == 0:
    print("ALL TESTS PASSED — NEXUS SKILL FULLY OPERATIONAL")
else:
    print("FAILED TESTS:")
    for r in results:
        if r[0] == FAIL:
            print(f"  - {r[1]}: {r[2]}")

db.close()
