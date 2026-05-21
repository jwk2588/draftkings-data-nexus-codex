#!/usr/bin/env python3
"""
KIMI K2.6 Paced Synthesis — Rate-limit-aware version
Processes one GR node every 90 seconds, saves results incrementally.
Supports resume from checkpoint.
"""

import os, sys, json, time, sqlite3, hashlib, requests
from datetime import datetime, timezone

NVIDIA_API_KEY = "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"
NVIDIA_API_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
KIMI_MODEL     = "moonshotai/kimi-k2.6"
CHATGPT_DB     = os.path.expanduser("~/DraftKingsDB/db/master.db")
RESULTS_FILE   = os.path.expanduser("~/HiveMind/kimi_synthesis_results.json")
CHECKPOINT     = os.path.expanduser("~/HiveMind/kimi_checkpoint.json")
LOG_FILE       = os.path.expanduser("~/HiveMind/logs/audit/kimi_paced.log")
BRIEF_FILE     = os.path.expanduser("~/HiveMind/MASTER_INTELLIGENCE_BRIEF.md")

# Rate limit: 1 request per 90 seconds on free tier
RATE_LIMIT_DELAY = 90

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

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log(msg):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def kimi_call(messages, max_tokens=3000):
    """Single KIMI call with full retry logic."""
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
    }
    for attempt in range(5):
        try:
            resp = requests.post(NVIDIA_API_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            elif resp.status_code == 429:
                wait = 90 + (attempt * 30)
                log(f"  Rate limited (429), waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
            elif resp.status_code == 503:
                wait = 30 + (attempt * 15)
                log(f"  Service unavailable (503), waiting {wait}s...")
                time.sleep(wait)
            else:
                log(f"  API error {resp.status_code}: {resp.text[:300]}")
                time.sleep(15)
        except Exception as e:
            log(f"  Request exception (attempt {attempt+1}): {e}")
            time.sleep(10)
    return None

def load_corpus():
    conn = sqlite3.connect(CHATGPT_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT conv_id, title, model_slug, create_time FROM chatgpt_conversations")
    convs = {r["conv_id"]: dict(r) for r in cur.fetchall()}
    cur.execute("""
        SELECT cs.src_id, cs.dst_id, cs.weight,
               m.role, m.content, m.create_time, m.conv_id
        FROM cross_stitching_maps cs
        JOIN chatgpt_messages m ON cs.src_id = m.msg_id
        WHERE cs.dst_id LIKE 'GR-%'
        ORDER BY cs.dst_id, cs.weight DESC
    """)
    rows = cur.fetchall()
    conn.close()
    by_gr = {gr: [] for gr in GR_NODES}
    for row in rows:
        gr = row["dst_id"]
        if gr in by_gr:
            conv_title = (convs.get(row["conv_id"]) or {}).get("title") or "Unknown"
            by_gr[gr].append({
                "conv_title": conv_title[:50],
                "role": row["role"],
                "content": (row["content"] or "")[:600],
                "weight": row["weight"],
            })
    return by_gr

def synthesize_gr(gr_id, gr_desc, messages):
    if len(messages) < 5:
        return {"gr_id": gr_id, "status": "skipped", "message_count": len(messages)}

    top = sorted(messages, key=lambda x: x.get("weight", 0), reverse=True)[:40]
    excerpts = "\n\n---\n\n".join([
        f"[Conv: {m['conv_title']} | {m['role']}]\n{m['content'][:500]}"
        for m in top[:30]
    ])

    system = f"""You are CHESS — NEXUS Strategic Intelligence Synthesizer for DraftKings litigation.
Perform deep forensic synthesis of ChatGPT research excerpts about: {gr_desc}

Return a JSON object with these exact keys:
- entities: list of key entities (companies, people, statutes, cases)
- strongest_arguments: list of top 3 legal/financial arguments
- key_evidence: list of most valuable evidentiary items found
- causal_chain: string describing conduct → harm → legal theory → remedy
- settlement_leverage_score: integer 0-100 based on evidence strength
- moonshot_arguments: list of top 2 highest-impact arguments
- evidence_gaps: list of missing evidence that would strengthen the case
- synthesis_summary: 2-3 sentence strategic summary"""

    user = f"""GR Node: {gr_id} — {gr_desc}
Messages in cluster: {len(messages)} (showing top {len(top)} by relevance)

{excerpts}

Return JSON synthesis only."""

    result = kimi_call([
        {"role": "system", "content": system},
        {"role": "user", "content": user}
    ], max_tokens=2500)

    if not result:
        return {"gr_id": gr_id, "status": "api_error", "message_count": len(messages)}

    # Parse JSON
    try:
        if "```json" in result:
            result_json = result.split("```json")[1].split("```")[0].strip()
        elif "```" in result:
            result_json = result.split("```")[1].split("```")[0].strip()
        else:
            result_json = result.strip()
        synthesis = json.loads(result_json)
    except:
        synthesis = {"raw_synthesis": result[:3000]}

    return {
        "gr_id": gr_id,
        "gr_desc": gr_desc,
        "message_count": len(messages),
        "status": "synthesized",
        "synthesis": synthesis,
        "synthesized_at": datetime.now(timezone.utc).isoformat()
    }

def generate_master_brief(gr_results):
    """Generate master brief from all completed syntheses."""
    summaries = []
    for r in gr_results:
        if r.get("status") == "synthesized" and r.get("synthesis"):
            s = r["synthesis"]
            score = s.get("settlement_leverage_score", "?")
            summary = s.get("synthesis_summary", "")
            moonshots = s.get("moonshot_arguments", [])
            summaries.append(
                f"**{r['gr_id']} — {r.get('gr_desc','')[:50]}** "
                f"(msgs: {r.get('message_count',0)}, leverage: {score}/100)\n"
                f"Summary: {summary[:400]}\n"
                f"Moonshots: {'; '.join(str(m) for m in moonshots[:2])[:200]}"
            )

    if not summaries:
        return "No GR nodes successfully synthesized."

    combined = "\n\n".join(summaries)
    system = """You are the NEXUS Master Intelligence Synthesizer for DraftKings litigation.
Generate a MASTER INTELLIGENCE BRIEF covering:
1. Top 5 highest-leverage legal theories across all domains
2. Strongest cross-domain evidence chains
3. Overall case strength assessment (score 0-100) and settlement band estimate
4. Optimal litigation/ADR strategy recommendation
5. Top 5 "nuclear arguments" that could be case-decisive
6. 90-day action roadmap with specific next steps

Be specific, forensic, and strategic. This is for litigation counsel."""

    user = f"""NEXUS Multi-Domain Synthesis Results:

{combined}

Generate the MASTER INTELLIGENCE BRIEF."""

    log("  Generating Master Intelligence Brief (final Kimi call)...")
    return kimi_call([
        {"role": "system", "content": system},
        {"role": "user", "content": user}
    ], max_tokens=4000) or "Master brief generation failed."

def main():
    log("=" * 60)
    log("KIMI K2.6 PACED SYNTHESIS — STARTING")
    log(f"Rate limit: {RATE_LIMIT_DELAY}s between calls")
    log("=" * 60)

    # Load checkpoint
    checkpoint = {}
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            checkpoint = json.load(f)
        log(f"Resuming from checkpoint: {list(checkpoint.keys())}")

    # Load corpus
    log("\n[PHASE 1] Loading corpus...")
    by_gr = load_corpus()
    total = sum(len(v) for v in by_gr.values())
    log(f"  {total:,} cross-stitched messages across {len(by_gr)} GR nodes")

    # Process each GR node
    log("\n[PHASE 2] Per-GR synthesis (paced)...")
    gr_results = []
    for i, (gr_id, gr_desc) in enumerate(GR_NODES.items()):
        # Check checkpoint
        if gr_id in checkpoint:
            log(f"  {gr_id}: CACHED from checkpoint")
            gr_results.append(checkpoint[gr_id])
            continue

        msgs = by_gr.get(gr_id, [])
        log(f"  [{i+1}/{len(GR_NODES)}] {gr_id}: {len(msgs)} messages")

        result = synthesize_gr(gr_id, gr_desc, msgs)
        gr_results.append(result)

        score = result.get("synthesis", {}).get("settlement_leverage_score", "N/A") if result.get("synthesis") else "N/A"
        log(f"  {gr_id}: {result['status']} | leverage: {score}")

        # Save checkpoint
        checkpoint[gr_id] = result
        with open(CHECKPOINT, "w") as f:
            json.dump(checkpoint, f, indent=2, default=str)

        # Rate limit delay (skip after last node)
        if i < len(GR_NODES) - 1:
            log(f"  Waiting {RATE_LIMIT_DELAY}s (rate limit)...")
            time.sleep(RATE_LIMIT_DELAY)

    # Master brief
    log("\n[PHASE 3] Generating Master Intelligence Brief...")
    time.sleep(RATE_LIMIT_DELAY)  # Wait before final call
    master_brief = generate_master_brief(gr_results)

    # Save results
    final = {
        "synthesis_run_id": hashlib.sha256(datetime.now().isoformat().encode()).hexdigest()[:12],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model": KIMI_MODEL,
        "provider": "NVIDIA NIM (FREE)",
        "total_messages_analyzed": total,
        "gr_syntheses": gr_results,
        "master_intelligence_brief": master_brief,
        "stats": {
            "synthesized": sum(1 for r in gr_results if r["status"] == "synthesized"),
            "skipped": sum(1 for r in gr_results if r["status"] == "skipped"),
            "errors": sum(1 for r in gr_results if r["status"] == "api_error"),
        }
    }

    with open(RESULTS_FILE, "w") as f:
        json.dump(final, f, indent=2, default=str)

    # Save master brief as markdown
    with open(BRIEF_FILE, "w") as f:
        f.write("# NEXUS MASTER INTELLIGENCE BRIEF\n\n")
        f.write(f"**Generated:** {final['timestamp']}\n")
        f.write(f"**Model:** {KIMI_MODEL} via NVIDIA NIM (FREE)\n")
        f.write(f"**Corpus:** {total:,} messages analyzed\n\n---\n\n")
        f.write(master_brief)

    log("\n" + "=" * 60)
    log("SYNTHESIS COMPLETE")
    log(f"  Synthesized: {final['stats']['synthesized']}/{len(GR_NODES)}")
    log(f"  Results: {RESULTS_FILE}")
    log(f"  Brief: {BRIEF_FILE}")
    log("=" * 60)
    print(json.dumps(final["stats"], indent=2))

if __name__ == "__main__":
    main()
