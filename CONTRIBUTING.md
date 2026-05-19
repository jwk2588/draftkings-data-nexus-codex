# Contributing to draftkings-data-nexus-codex

## Governing Authority

This repository is governed by **Manus Directive v1.0** and the **MBv54 Legal Spine**.  
All contributions must comply with the NEXUS/NODE governance framework.

---

## Branch Strategy

| Branch | Purpose |
|---|---|
| `main` | Stable, ADR-ready code and schemas only. PR required. |
| `develop` | Active Manus / Copilot development. Default working branch. |
| `data-lab` | Experimental ETL, parsers, and graph transformations. |

Never commit directly to `main`. All merges to `main` require a passing CI run and manual review.

---

## Schema Change Rules (MANDATORY)

**All schema changes must be reviewed by Claude before commit.**

This means any modification to:
- `schema/*.yaml`
- `draftkings-graph/schema/*`
- `nexus-governance/*`
- Any file containing `node_label`, `relationship_type`, `tier`, `ev_id`, or `sb_id` fields.

The review process:
1. Open a PR from `develop` or `data-lab` to `main`.
2. Tag the PR with `schema-change`.
3. Include a Claude-generated schema review in the PR description.
4. Await manual approval before merging.

---

## GitHub Copilot Guardrails

Copilot is an **advisory tool only**. It accelerates boilerplate and repetitive patterns.

**Copilot MAY be used for:**
- Boilerplate FastAPI endpoints.
- Repetitive Cypher/Python/TypeScript patterns once the schema is fixed.
- Test scaffolding and docstrings.
- Type hints and minor glue code.

**Copilot MUST NEVER:**
- Invent new schema elements without Claude's schema review.
- Modify EV-ID or SB-ID spaces (EV-001 through EV-291, SB-01 through SB-66).
- Modify NEXUS node IDs or Tier labels.
- Touch `legal-spine/`, `nexus-governance/`, or `schema/` without explicit human approval.
- Be accepted as-is without Manus + Claude review.

> **Rule:** No Copilot-suggested change may touch the EV/SB ID spaces or NEXUS node maps without explicit manual review.

---

## EV/SB ID Preservation

The following ID spaces are **frozen and immutable**:
- `EV-001` through `EV-291` — Evidence items from MBv54.
- `SB-01` through `SB-66` — Silver Bullets from MBv54.
- 25 Pillar IDs, 27 Levee IDs, Six-Engine IDs.

These IDs must never be renumbered, reassigned, or deleted. New evidence items must use IDs above EV-291 and SB-66, pending a formal ADR.

---

## Tier Labels

| Tier | Meaning | Usage |
|---|---|---|
| T1 | Native-doc, high confidence | Safe in main prose/code |
| T2 | Strong inference | Main prose/code with explicit qualification |
| T3 | Discovery/roadmap only | Appendix only, never control logic |

Never promote a T3 node to T2 or T1 without a GraphMutationEvent log entry and human approval.

---

## Commit Message Format

```
<type>(<scope>): <short description>

[optional body]
[optional ADR reference: ADR-XXX]
```

Types: `feat`, `fix`, `schema`, `etl`, `graph`, `agent`, `docs`, `ci`, `refactor`

Example:
```
schema(nexus-governance): add Levee-14 node to NEXUS register

Adds Levee-14 (Cross-Platform Liability Boundary) to the NEXUS node register.
Reviewed by Claude. ADR-003 compliant.
```
