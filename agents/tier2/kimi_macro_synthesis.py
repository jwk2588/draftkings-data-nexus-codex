#!/usr/bin/env python3
"""
KIMI K2.6 Macro-Synthesis Engine — HiveMind DraftKings Intelligence System
Uses NVIDIA NIM free API (moonshotai/kimi-k2.6) to perform deep forensic
synthesis across the full ChatGPT corpus (40,250 messages) and iCloud OCR data.

Architecture:
- Phase 1: Corpus chunking (by GR node / topic cluster)
- Phase 2: Per-cluster synthesis (Kimi K2.6 with reasoning enabled)
- Phase 3: Cross-cluster arbitration synthesis
- Phase 4: Master intelligence brief generation
- Phase 5: Results wired back to PostgreSQL HiveMind graph
"""

import os
import sys
import json
import time
import sqlite3
import hashlib
import psycopg2
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# ─── Config ───────────────────────────────────────────────────────────────────
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "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")
NVIDIA_API_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
KIMI_MODEL    = "moonshotai/kimi-k2.6"

CHATGPT_DB    = os.path.expanduser("~/DraftKingsDB/db/master.db")
PG_DSN        = "host=localhost dbname=draftkings_hivemind user=hivemind password=hivemind_secure_2026"
LOG_FILE      = os.path.expanduser("~/HiveMind/logs/audit/kimi_synthesis.log")
RESULTS_FILE  = os.path.expanduser("~/HiveMind/kimi_synthesis_results.json")

# GR node definitions (from NEXUS ontology)
GR_NODES = {
    "GR-001": "VIE / ASC 810 / Off-Balance Sheet Entities / Consolidation",
    "GR-002": "RICO / Racketeering / Wire Fraud / Pattern of Conduct",
    "GR-003": "Consumer Protection / FTC / State AG / Class Action",
    "GR-004": "Forensic Accounting / Revenue Recognition / ASC 606",
    "GR-005": "Constitutional / Due Process / Equal Protection / Preemption",
    "GR-006": "MGCB / Michigan Gaming Control Board / State Regulatory",
    "GR-007": "Apple / Google Platform Removal / App Store / Antitrust",
    "GR-008": "Settlement / ADR / FRE 408 / Mediation / Arbitration",
    "GR-009": "DraftKings / DFS / Sports Betting / iGaming Operations",
    "GR-010": "Securities / SEC / 10-K / 10-Q / Material Misstatement",
    "GR-011": "Evidence / Discovery / FOIA / Subpoena / Chain of Custody",
    "GR-012": "Expert Witness / Daubert / Technical Standards / Methodology",
}

# ─── Logging ──────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log(msg: str):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ─── KIMI API Call ────────────────────────────────────────────────────────────
def kimi_call(messages: List[Dict], max_tokens: int = 4096, thinking: bool = True,
              retries: int = 3) -> Optional[str]:
    """Call KIMI K2.6 via NVIDIA NIM with retry logic."""
    headers = {
        "Authorization": f"Bearer {NVIDIA_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "model": KIMI_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
        "top_p": 0.9,
        "stream": False,
        "chat_template_kwargs": {"thinking": thinking},
    }
    for attempt in range(retries):
        try:
            resp = requests.post(NVIDIA_API_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            elif resp.status_code == 429:
                wait = 2 ** attempt * 5
                log(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                log(f"  API error {resp.status_code}: {resp.text[:200]}")
                time.sleep(2)
        except Exception as e:
            log(f"  Request exception (attempt {attempt+1}): {e}")
            time.sleep(3)
    return None

# ─── Data Loading ─────────────────────────────────────────────────────────────
def load_chatgpt_corpus() -> Dict[str, List[Dict]]:
    """Load ChatGPT messages grouped by GR node from cross-stitching maps."""
    conn = sqlite3.connect(CHATGPT_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all conversations with their titles
    cur.execute("SELECT conv_id, title, model_slug, create_time FROM chatgpt_conversations ORDER BY create_time")
    convs = {r["conv_id"]: dict(r) for r in cur.fetchall()}

    # Get cross-stitch mappings to GR nodes
    cur.execute("""
        SELECT cs.src_id, cs.dst_id, cs.map_type, cs.weight,
               m.role, m.content, m.create_time, m.conv_id
        FROM cross_stitching_maps cs
        JOIN chatgpt_messages m ON cs.src_id = m.msg_id
        WHERE cs.dst_id LIKE 'GR-%'
        ORDER BY cs.dst_id, m.create_time
    """)
    rows = cur.fetchall()
    conn.close()

    # Group by GR node
    by_gr: Dict[str, List[Dict]] = {gr: [] for gr in GR_NODES}
    for row in rows:
        gr = row["dst_id"]
        if gr in by_gr:
            conv_info = convs.get(row["conv_id"], {})
            conv_title = conv_info.get("title") or "Unknown"
            by_gr[gr].append({
                "msg_id": row["src_id"],
                "conv_id": row["conv_id"],
                "conv_title": conv_title,
                "role": row["role"],
                "content": row["content"][:800] if row["content"] else "",  # cap per message
                "create_time": row["create_time"],
                "weight": row["weight"],
            })

    return by_gr, convs

def load_ocr_extracts() -> List[Dict]:
    """Load OCR extracts from PostgreSQL for DraftKings-relevant photos."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        cur.execute("""
            SELECT o.object_id, o.file_name, o.capture_date,
                   e.ocr_text, e.ocr_confidence, e.ocr_engine
            FROM raw_objects o
            JOIN ocr_extracts e ON o.object_id = e.object_id
            WHERE e.ocr_text IS NOT NULL
              AND length(e.ocr_text) > 50
              AND e.ocr_confidence > 0.5
            ORDER BY o.capture_date DESC
            LIMIT 500
        """)
        rows = cur.fetchall()
        conn.close()
        return [{"object_id": str(r[0]), "file_name": r[1], "capture_date": str(r[2]),
                 "ocr_text": r[3][:600], "confidence": float(r[4]), "engine": r[5]} for r in rows]
    except Exception as e:
        log(f"  OCR load error: {e}")
        return []

# ─── Synthesis Functions ──────────────────────────────────────────────────────
def synthesize_gr_cluster(gr_id: str, gr_desc: str, messages: List[Dict]) -> Dict:
    """Run Kimi K2.6 synthesis on a single GR node cluster."""
    if not messages:
        return {"gr_id": gr_id, "status": "no_data", "synthesis": None}

    # Build context — top messages by weight, capped at ~12K tokens
    sorted_msgs = sorted(messages, key=lambda x: x.get("weight", 0), reverse=True)[:60]

    # Format as conversation excerpts
    excerpts = []
    for m in sorted_msgs[:40]:
        excerpts.append(f"[Conv: {m['conv_title'][:50]} | Role: {m['role']}]\n{m['content'][:500]}")

    context_text = "\n\n---\n\n".join(excerpts)

    system_prompt = f"""You are CHESS — the Strategic Intelligence Synthesizer in the NEXUS DraftKings litigation intelligence system.

Your task: Perform deep forensic synthesis of the following ChatGPT conversation excerpts, all related to the legal/financial domain: {gr_desc}

SYNTHESIS REQUIREMENTS:
1. Extract the strongest legal/financial arguments and evidence patterns
2. Identify key entities (companies, individuals, regulators, statutes, case citations)
3. Map causal chains: conduct → harm → legal theory → remedy
4. Flag any admissions, contradictions, or high-value evidentiary statements
5. Calculate a Settlement Leverage Score (0-100) based on evidence strength
6. Identify the top 3 "moonshot" arguments with highest potential impact
7. Note any gaps in evidence that need to be filled

Output as structured JSON with keys: entities, arguments, causal_chains, key_evidence, settlement_leverage_score, moonshot_arguments, evidence_gaps, synthesis_summary"""

    user_prompt = f"""NEXUS GR Node: {gr_id} — {gr_desc}
Total messages in cluster: {len(messages)}
Showing top {len(sorted_msgs)} by relevance weight:

{context_text}

Provide your deep forensic synthesis as JSON."""

    log(f"  Synthesizing {gr_id} ({len(messages)} msgs, {len(sorted_msgs)} shown)...")
    result = kimi_call([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ], max_tokens=3000, thinking=True)

    if result:
        # Try to parse JSON from result
        try:
            # Extract JSON if wrapped in markdown
            if "```json" in result:
                result_json = result.split("```json")[1].split("```")[0].strip()
            elif "```" in result:
                result_json = result.split("```")[1].split("```")[0].strip()
            else:
                result_json = result.strip()
            synthesis_data = json.loads(result_json)
        except:
            synthesis_data = {"raw_synthesis": result}

        return {
            "gr_id": gr_id,
            "gr_desc": gr_desc,
            "message_count": len(messages),
            "status": "synthesized",
            "synthesis": synthesis_data,
            "synthesized_at": datetime.now(timezone.utc).isoformat()
        }
    else:
        return {"gr_id": gr_id, "status": "api_error", "synthesis": None}

def synthesize_master_brief(gr_results: List[Dict], ocr_summary: str) -> str:
    """Generate the master intelligence brief from all GR syntheses."""
    # Build summary of all GR results
    gr_summaries = []
    for r in gr_results:
        if r.get("status") == "synthesized" and r.get("synthesis"):
            s = r["synthesis"]
            score = s.get("settlement_leverage_score", "N/A")
            summary = s.get("synthesis_summary", str(s)[:300])
            moonshots = s.get("moonshot_arguments", [])
            gr_summaries.append(
                f"**{r['gr_id']} — {r.get('gr_desc','')[:50]}** (msgs: {r.get('message_count',0)}, leverage: {score}/100)\n"
                f"Summary: {summary[:400]}\n"
                f"Moonshots: {json.dumps(moonshots[:2])[:200]}"
            )

    combined = "\n\n".join(gr_summaries)

    system_prompt = """You are the NEXUS Master Intelligence Synthesizer. You have just completed deep forensic analysis across 12 legal/financial domain clusters derived from thousands of ChatGPT research conversations about DraftKings litigation.

Generate the MASTER INTELLIGENCE BRIEF — a comprehensive strategic document that:
1. Identifies the highest-leverage legal theories across all domains
2. Maps the strongest cross-domain evidence chains
3. Calculates the overall case strength and settlement band
4. Recommends the optimal litigation/ADR strategy
5. Identifies the top 5 "nuclear arguments" that could be case-decisive
6. Provides a 90-day action roadmap

This brief will be used by litigation counsel and forensic accountants."""

    user_prompt = f"""NEXUS MASTER SYNTHESIS — All GR Node Results:

{combined}

ICLOUD PHOTO OCR INTELLIGENCE:
{ocr_summary[:2000]}

Generate the MASTER INTELLIGENCE BRIEF as a comprehensive strategic document."""

    log("  Generating Master Intelligence Brief...")
    result = kimi_call([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ], max_tokens=4096, thinking=True)

    return result or "Master brief generation failed — API error"

# ─── PostgreSQL Wiring ────────────────────────────────────────────────────────
def wire_synthesis_to_graph(gr_results: List[Dict]) -> int:
    """Write synthesis results back to HiveMind PostgreSQL graph."""
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        wired = 0

        for r in gr_results:
            if r.get("status") != "synthesized":
                continue

            synthesis_json = json.dumps(r.get("synthesis", {}))
            score = r.get("synthesis", {}).get("settlement_leverage_score", 0)
            if isinstance(score, str):
                try:
                    score = float(score)
                except:
                    score = 0.0

            # Upsert synthesis result as a canonical entity annotation
            cur.execute("""
                INSERT INTO canonical_metadata (object_id, metadata_key, metadata_value, source, confidence, created_at)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """, (
                f"KIMI_SYNTHESIS_{r['gr_id']}",
                synthesis_json[:8000],
                "kimi-k2.6-nvidia-nim",
                min(float(score) / 100.0, 1.0) if score else 0.5
            ))

            # Log to audit ledger
            cur.execute("""
                INSERT INTO audit_ledger (event_id, event_type, actor, object_id, payload, created_at)
                VALUES (gen_random_uuid(), 'KIMI_SYNTHESIS_COMPLETE', 'KIMI_K2_6', %s, %s, NOW())
            """, (r["gr_id"], json.dumps({"message_count": r.get("message_count", 0), "status": r["status"]})))

            wired += 1

        conn.commit()
        conn.close()
        return wired
    except Exception as e:
        log(f"  Graph wiring error: {e}")
        return 0

# ─── Main Orchestration ───────────────────────────────────────────────────────
def main():
    log("=" * 70)
    log("KIMI K2.6 MACRO-SYNTHESIS ENGINE — STARTING")
    log(f"Model: {KIMI_MODEL} via NVIDIA NIM (FREE)")
    log("=" * 70)

    # Phase 1: Load corpus
    log("\n[PHASE 1] Loading ChatGPT corpus from DraftKingsDB...")
    by_gr, convs = load_chatgpt_corpus()
    total_msgs = sum(len(v) for v in by_gr.values())
    log(f"  Loaded {total_msgs} cross-stitched messages across {len(by_gr)} GR nodes")
    for gr_id, msgs in by_gr.items():
        log(f"  {gr_id}: {len(msgs)} messages")

    # Phase 2: Load OCR extracts
    log("\n[PHASE 2] Loading iCloud OCR extracts...")
    ocr_extracts = load_ocr_extracts()
    log(f"  Loaded {len(ocr_extracts)} OCR extracts from iCloud photos")
    ocr_summary = "\n".join([
        f"[{e['file_name']} | {e['capture_date']} | conf:{e['confidence']:.2f}]\n{e['ocr_text'][:300]}"
        for e in ocr_extracts[:30]
    ])

    # Phase 3: Per-GR synthesis
    log("\n[PHASE 3] Running per-GR-node Kimi K2.6 synthesis...")
    gr_results = []
    for gr_id, gr_desc in GR_NODES.items():
        messages = by_gr.get(gr_id, [])
        if len(messages) < 5:
            log(f"  Skipping {gr_id} — only {len(messages)} messages (threshold: 5)")
            gr_results.append({"gr_id": gr_id, "gr_desc": gr_desc,
                                "message_count": len(messages), "status": "skipped"})
            continue

        result = synthesize_gr_cluster(gr_id, gr_desc, messages)
        gr_results.append(result)

        score = result.get("synthesis", {}).get("settlement_leverage_score", "N/A") if result.get("synthesis") else "N/A"
        log(f"  {gr_id}: {result['status']} | leverage score: {score}")
        time.sleep(1)  # Rate limit courtesy

    # Phase 4: Master brief
    log("\n[PHASE 4] Generating Master Intelligence Brief...")
    master_brief = synthesize_master_brief(gr_results, ocr_summary)
    log(f"  Master brief generated: {len(master_brief)} chars")

    # Phase 5: Wire to graph
    log("\n[PHASE 5] Wiring synthesis results to HiveMind PostgreSQL graph...")
    wired = wire_synthesis_to_graph(gr_results)
    log(f"  Wired {wired} synthesis results to graph")

    # Save full results
    final_results = {
        "synthesis_run_id": hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()[:12],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model": KIMI_MODEL,
        "provider": "NVIDIA NIM (FREE)",
        "total_messages_analyzed": total_msgs,
        "total_conversations": len(convs),
        "ocr_extracts_analyzed": len(ocr_extracts),
        "gr_syntheses": gr_results,
        "master_intelligence_brief": master_brief,
        "stats": {
            "synthesized": sum(1 for r in gr_results if r["status"] == "synthesized"),
            "skipped": sum(1 for r in gr_results if r["status"] == "skipped"),
            "errors": sum(1 for r in gr_results if r["status"] == "api_error"),
            "graph_nodes_wired": wired
        }
    }

    with open(RESULTS_FILE, "w") as f:
        json.dump(final_results, f, indent=2, default=str)

    # Save master brief separately
    brief_file = os.path.expanduser("~/HiveMind/MASTER_INTELLIGENCE_BRIEF.md")
    with open(brief_file, "w") as f:
        f.write(f"# NEXUS MASTER INTELLIGENCE BRIEF\n")
        f.write(f"**Generated:** {final_results['timestamp']}\n")
        f.write(f"**Model:** {KIMI_MODEL} via NVIDIA NIM\n")
        f.write(f"**Corpus:** {total_msgs:,} messages | {len(convs):,} conversations | {len(ocr_extracts):,} OCR extracts\n\n")
        f.write("---\n\n")
        f.write(master_brief)

    log("\n" + "=" * 70)
    log("SYNTHESIS COMPLETE")
    log(f"  GR nodes synthesized: {final_results['stats']['synthesized']}/{len(GR_NODES)}")
    log(f"  Graph nodes wired: {wired}")
    log(f"  Results: {RESULTS_FILE}")
    log(f"  Master Brief: {brief_file}")
    log("=" * 70)

    return final_results

if __name__ == "__main__":
    results = main()
    print(f"\nFINAL STATS: {json.dumps(results['stats'], indent=2)}")
