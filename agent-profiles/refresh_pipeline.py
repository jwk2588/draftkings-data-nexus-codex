"""
Agent Refresh Pipeline
========================
Manus Directive v1.0, Section 7.2

Rebuilds each agent's prompt + retrieval template when:
  - New MB versions are added
  - New github-gem-seeker DK_CORE batches are ingested
  - New Authority packs are added

Ensures all EV/SB/NODE/Tier metadata is preserved at refresh time.
Dual-Nexus agent graphs (live learning + archival corpus) stay synchronized.

Author  : Manus Directive v1.0
Version : 1.0.0
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

log = logging.getLogger("agent_refresh_pipeline")

AGENT_PROFILES = [
    REPO_ROOT / "agent-profiles" / "michigan_mgcb_agent.yaml",
    REPO_ROOT / "agent-profiles" / "cftc_railbird_agent.yaml",
    REPO_ROOT / "agent-profiles" / "asc606_audit_agent.yaml",
]

REFRESH_LOG_PATH = REPO_ROOT / "artifacts" / "agent_refresh_log.jsonl"


def load_agent_profile(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_refresh_context(agent: dict, repo_root: Path) -> dict:
    """
    Build the refresh context for an agent:
    - Load latest DK_CORE Parquet stats
    - Check for new EV/SB IDs in the agent's allowed ranges
    - Build updated system prompt with current corpus stats
    """
    context = {
        "agent_id": agent["agent_id"],
        "agent_name": agent["enterprise_name"],
        "refresh_timestamp": datetime.now(timezone.utc).isoformat(),
        "corpus_stats": {},
        "ev_id_ranges": agent.get("allowed_ev_id_ranges", []),
        "sb_id_ranges": agent.get("allowed_sb_id_ranges", []),
        "allowed_domains": agent.get("allowed_ev_domains", []),
        "allowed_tiers": agent.get("allowed_tiers", []),
    }

    # Load DK_CORE Parquet stats if available
    dk_core_path = repo_root / "data-lake" / "chatgpt" / "normalized" / "messages_dk_core.parquet"
    if dk_core_path.exists():
        try:
            import polars as pl
            df = pl.read_parquet(dk_core_path)
            context["corpus_stats"]["dk_core_messages"] = len(df)
            context["corpus_stats"]["threads"] = df["thread_id"].n_unique()

            # Count messages with EV-ID refs in this agent's domain
            ev_refs = df.filter(pl.col("ev_ids") != "[]")
            context["corpus_stats"]["messages_with_ev_refs"] = len(ev_refs)
        except Exception as e:
            log.warning("Could not load DK_CORE Parquet: %s", e)

    # Load NEXUS node register stats
    nexus_path = repo_root / "nexus-governance" / "node-register" / "nexus_node_register.yaml"
    if nexus_path.exists():
        with open(nexus_path) as f:
            nexus = yaml.safe_load(f)
        context["corpus_stats"]["nexus_nodes"] = len(nexus.get("nodes", []))

    return context


def build_refreshed_system_prompt(agent: dict, context: dict) -> str:
    """Build the refreshed system prompt with current corpus stats."""
    base_prompt = agent.get("system_prompt_template", "")
    corpus_stats = context.get("corpus_stats", {})

    refresh_header = f"""
--- REFRESH CONTEXT (auto-generated {context['refresh_timestamp']}) ---
Corpus stats:
  DK_CORE messages: {corpus_stats.get('dk_core_messages', 'N/A')}
  Unique threads: {corpus_stats.get('threads', 'N/A')}
  Messages with EV-ID refs: {corpus_stats.get('messages_with_ev_refs', 'N/A')}
  NEXUS nodes registered: {corpus_stats.get('nexus_nodes', 'N/A')}

Allowed EV ranges: {', '.join(context['ev_id_ranges'])}
Allowed SB ranges: {', '.join(context.get('sb_id_ranges', []))}
Allowed domains: {', '.join(context['allowed_domains'])}
Allowed tiers: {', '.join(context['allowed_tiers'])}
--- END REFRESH CONTEXT ---

"""
    return refresh_header + base_prompt


def log_refresh_event(agent_id: str, context: dict, prompt_hash: str):
    """Append a refresh event to the immutable refresh log."""
    REFRESH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "event_type": "AGENT_REFRESH",
        "agent_id": agent_id,
        "timestamp": context["refresh_timestamp"],
        "corpus_stats": context["corpus_stats"],
        "prompt_hash": prompt_hash,
        "actor": "refresh_pipeline",
    }
    with open(REFRESH_LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def run_refresh_pipeline(repo_root: Path = REPO_ROOT) -> dict:
    """Run the full agent refresh pipeline."""
    import hashlib
    log.info("=" * 60)
    log.info("Agent Refresh Pipeline starting")
    log.info("=" * 60)

    results = []
    for profile_path in AGENT_PROFILES:
        if not profile_path.exists():
            log.warning("Profile not found: %s", profile_path)
            continue

        agent = load_agent_profile(profile_path)
        log.info("Refreshing agent: %s", agent["enterprise_name"])

        context = build_refresh_context(agent, repo_root)
        refreshed_prompt = build_refreshed_system_prompt(agent, context)
        prompt_hash = hashlib.sha256(refreshed_prompt.encode()).hexdigest()

        log_refresh_event(agent["agent_id"], context, prompt_hash)

        # Write refreshed prompt to artifacts
        prompt_out = repo_root / "artifacts" / "agent_prompts" / f"{agent['agent_id']}_prompt.txt"
        prompt_out.parent.mkdir(parents=True, exist_ok=True)
        prompt_out.write_text(refreshed_prompt)

        results.append({
            "agent_id": agent["agent_id"],
            "agent_name": agent["enterprise_name"],
            "corpus_stats": context["corpus_stats"],
            "prompt_hash": prompt_hash,
            "prompt_path": str(prompt_out),
            "status": "refreshed",
        })
        log.info("Refreshed: %s (prompt_hash: %s...)", agent["agent_id"], prompt_hash[:12])

    log.info("Refresh complete: %d agents refreshed", len(results))
    return {"agents_refreshed": len(results), "results": results}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    result = run_refresh_pipeline()
    print(json.dumps(result, indent=2))
