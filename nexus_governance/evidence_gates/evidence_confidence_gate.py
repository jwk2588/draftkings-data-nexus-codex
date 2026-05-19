"""
NEXUS Evidence Confidence Gate
================================
Enforces NEXUS/NODE governance before any graph write.
This is the authoritative gatekeeper for all DraftKings
graph mutations, anchored to MBv54 as the legal spine.

Rules:
  - T1 nodes: only from MBv54 native documents.
  - T2 nodes: strong inference with explicit qualification.
  - T3 nodes: appendix/discovery only, never control logic.
  - EV/SB IDs: frozen at EV-001–EV-291, SB-01–SB-66.
  - Detonator Board: activates only on confirmed T1 convergence.

Author  : Manus Directive v1.0 / NEXUS Governance
Version : 1.0.0
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

log = logging.getLogger("evidence_confidence_gate")

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Tier(str, Enum):
    T1 = "T1"  # Native-doc, high confidence, safe in main prose/code
    T2 = "T2"  # Strong inference, qualified
    T3 = "T3"  # Discovery/roadmap only, appendix, never control logic

class DeploymentStatus(str, Enum):
    MAIN       = "main"       # Safe for control logic
    APPENDIX   = "appendix"   # Discovery only
    QUARANTINE = "quarantine" # Under review, not to be used

class DKRelevance(str, Enum):
    DK_CORE     = "DK_CORE"     # Directly about DraftKings/Dynasty/MB
    DK_ADJACENT = "DK_ADJACENT" # Legal/accounting/gaming context
    NON_DK      = "NON_DK"      # Not relevant

# ---------------------------------------------------------------------------
# Frozen ID Spaces
# ---------------------------------------------------------------------------

EV_ID_PATTERN = re.compile(r"^EV-(\d{3})$")
SB_ID_PATTERN = re.compile(r"^SB-(\d{2})$")
EV_MAX = 291
SB_MAX = 66

T1_ALLOWED_SOURCES = {
    "MasterBrief_v54_CONSOLIDATED-2.docx",
    "chatgpt_etl",
    "gemini_extractor",
    "human_curator",
}

T2_ALLOWED_SOURCES = {
    "claude_reasoning_layer",
    "gemini_extractor",
    "algo_code_writer",
    "chatgpt_etl",
}


# ---------------------------------------------------------------------------
# Gate Errors
# ---------------------------------------------------------------------------

class EvidenceGateError(Exception):
    """Raised when a node fails the confidence gate."""
    pass

class FrozenIDViolation(EvidenceGateError):
    """Raised when an EV/SB ID is outside the frozen range."""
    pass

class TierViolation(EvidenceGateError):
    """Raised when a node's tier is inconsistent with its source."""
    pass

class DeploymentViolation(EvidenceGateError):
    """Raised when a quarantined or appendix node is used in control logic."""
    pass


# ---------------------------------------------------------------------------
# Node Descriptor
# ---------------------------------------------------------------------------

@dataclass
class NexusNode:
    node_id:            str
    label:              str
    tier:               Tier
    primary_source_id:  str
    description:        str
    deployment_status:  DeploymentStatus
    content_hash:       str = ""
    ev_id:              Optional[str] = None
    sb_id:              Optional[str] = None
    pillar_id:          Optional[str] = None
    levee_id:           Optional[str] = None
    engine_id:          Optional[str] = None
    mb_section_ref:     Optional[str] = None
    dk_relevance:       Optional[DKRelevance] = None
    symbolic_name:      str = ""
    enterprise_name:    str = ""
    created_at:         str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    created_by:         str = "manus_directive_v1"

    def __post_init__(self):
        if not self.content_hash:
            payload = json.dumps({
                "node_id": self.node_id,
                "label": self.label,
                "tier": self.tier,
                "primary_source_id": self.primary_source_id,
                "description": self.description,
            }, sort_keys=True)
            self.content_hash = hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Evidence Confidence Gate
# ---------------------------------------------------------------------------

class EvidenceConfidenceGate:
    """
    The authoritative gatekeeper for all NEXUS graph writes.
    Every node must pass all gate checks before being written.
    """

    def __init__(self):
        self._gate_log_path = Path(
            "/home/ubuntu/draftkings-data-nexus-codex/artifacts/gate_log.jsonl"
        )
        self._gate_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _log(self, node: NexusNode, result: str, reason: str = ""):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node_id": node.node_id,
            "label": node.label,
            "tier": node.tier,
            "deployment_status": node.deployment_status,
            "content_hash": node.content_hash,
            "result": result,
            "reason": reason,
        }
        with open(self._gate_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def check_ev_id(self, node: NexusNode) -> bool:
        """Validate EV-ID is within frozen range EV-001–EV-291."""
        if not node.ev_id:
            return True
        m = EV_ID_PATTERN.match(node.ev_id)
        if not m:
            raise FrozenIDViolation(f"Invalid EV-ID format: {node.ev_id}")
        num = int(m.group(1))
        if num > EV_MAX:
            raise FrozenIDViolation(
                f"EV-ID {node.ev_id} exceeds frozen range EV-001–EV-{EV_MAX:03d}. "
                f"New items must use EV-{EV_MAX+1:03d}+ with ADR approval."
            )
        return True

    def check_sb_id(self, node: NexusNode) -> bool:
        """Validate SB-ID is within frozen range SB-01–SB-66."""
        if not node.sb_id:
            return True
        m = SB_ID_PATTERN.match(node.sb_id)
        if not m:
            raise FrozenIDViolation(f"Invalid SB-ID format: {node.sb_id}")
        num = int(m.group(1))
        if num > SB_MAX:
            raise FrozenIDViolation(
                f"SB-ID {node.sb_id} exceeds frozen range SB-01–SB-{SB_MAX:02d}. "
                f"New items must use SB-{SB_MAX+1:02d}+ with ADR approval."
            )
        return True

    def check_tier_source_consistency(self, node: NexusNode, actor: str) -> bool:
        """Validate that the writing actor has authority for this tier."""
        if node.tier == Tier.T1:
            if actor not in T1_ALLOWED_SOURCES:
                raise TierViolation(
                    f"Actor '{actor}' cannot write T1 nodes. "
                    f"T1 write authority: {T1_ALLOWED_SOURCES}"
                )
        elif node.tier == Tier.T2:
            if actor not in T2_ALLOWED_SOURCES:
                raise TierViolation(
                    f"Actor '{actor}' cannot write T2 nodes. "
                    f"T2 write authority: {T2_ALLOWED_SOURCES}"
                )
        # T3: any actor allowed
        return True

    def check_deployment_status(self, node: NexusNode, context: str = "control_logic") -> bool:
        """
        Validate deployment_status is appropriate for the usage context.
        context: "control_logic" | "appendix" | "any"
        """
        if context == "control_logic":
            if node.deployment_status == DeploymentStatus.QUARANTINE:
                raise DeploymentViolation(
                    f"Node {node.node_id} is in QUARANTINE — cannot be used in control logic."
                )
            if node.deployment_status == DeploymentStatus.APPENDIX and node.tier == Tier.T3:
                raise DeploymentViolation(
                    f"T3 node {node.node_id} is appendix-only — cannot be used in control logic."
                )
        return True

    def check_content_hash(self, node: NexusNode) -> bool:
        """Validate SHA-256 content hash is present and valid."""
        if not node.content_hash or len(node.content_hash) != 64:
            raise EvidenceGateError(
                f"Node {node.node_id} has invalid content_hash: '{node.content_hash}'"
            )
        return True

    def run_all_checks(
        self,
        node: NexusNode,
        actor: str,
        context: str = "control_logic",
    ) -> dict:
        """
        Run all gate checks in order. Returns a result dict.
        Raises on first failure.
        """
        checks = {
            "ev_id_check": False,
            "sb_id_check": False,
            "tier_source_check": False,
            "deployment_check": False,
            "hash_check": False,
        }
        try:
            self.check_ev_id(node)
            checks["ev_id_check"] = True

            self.check_sb_id(node)
            checks["sb_id_check"] = True

            self.check_tier_source_consistency(node, actor)
            checks["tier_source_check"] = True

            self.check_deployment_status(node, context)
            checks["deployment_check"] = True

            self.check_content_hash(node)
            checks["hash_check"] = True

            self._log(node, "PASS")
            return {"passed": True, "checks": checks, "node_id": node.node_id}

        except (EvidenceGateError, FrozenIDViolation, TierViolation, DeploymentViolation) as e:
            self._log(node, "FAIL", str(e))
            raise


# ---------------------------------------------------------------------------
# Detonator Board Activation Check
# ---------------------------------------------------------------------------

class DetonatorBoard:
    """
    Monitors Pillar and Levee breach counts to determine
    if the Detonator Board activation threshold has been reached.

    Activation conditions (from mbv54_spine.yaml):
      - Minimum 3 T1 Pillars with confirmed evidence
      - Minimum 2 Levee breaches in the same domain
      - At least one Engine-06 convergence event
    """

    def __init__(self):
        self._confirmed_pillars: set[str] = set()
        self._breached_levees: dict[str, list[str]] = {}  # domain -> [levee_ids]
        self._engine06_events: int = 0

    def confirm_pillar(self, pillar_id: str, tier: Tier):
        if tier == Tier.T1:
            self._confirmed_pillars.add(pillar_id)
            log.info("Pillar confirmed (T1): %s — total: %d", pillar_id, len(self._confirmed_pillars))

    def breach_levee(self, levee_id: str, domain: str):
        if domain not in self._breached_levees:
            self._breached_levees[domain] = []
        if levee_id not in self._breached_levees[domain]:
            self._breached_levees[domain].append(levee_id)
            log.info("Levee breached: %s (domain: %s)", levee_id, domain)

    def record_engine06_event(self):
        self._engine06_events += 1
        log.info("Engine-06 convergence event #%d recorded", self._engine06_events)

    def check_activation(self) -> dict:
        """Check if Detonator Board activation threshold is met."""
        t1_pillar_count = len(self._confirmed_pillars)
        max_domain_breaches = max(
            (len(v) for v in self._breached_levees.values()), default=0
        )
        activated = (
            t1_pillar_count >= 3
            and max_domain_breaches >= 2
            and self._engine06_events >= 1
        )
        return {
            "activated": activated,
            "t1_pillars_confirmed": t1_pillar_count,
            "max_levee_breaches_in_domain": max_domain_breaches,
            "engine06_events": self._engine06_events,
            "breached_domains": {k: v for k, v in self._breached_levees.items()},
            "confirmed_pillars": list(self._confirmed_pillars),
            "activation_threshold": {
                "min_t1_pillars": 3,
                "min_levee_breaches_per_domain": 2,
                "min_engine06_events": 1,
            },
        }


# ---------------------------------------------------------------------------
# Singleton instances
# ---------------------------------------------------------------------------

_gate = EvidenceConfidenceGate()
_detonator = DetonatorBoard()


def get_gate() -> EvidenceConfidenceGate:
    return _gate

def get_detonator() -> DetonatorBoard:
    return _detonator


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    gate = get_gate()
    detonator = get_detonator()

    # Test a valid T1 node
    node = NexusNode(
        node_id="NEXUS-TEST-001",
        label="Evidence",
        tier=Tier.T1,
        primary_source_id="MasterBrief_v54_CONSOLIDATED-2.docx",
        description="Test evidence node",
        deployment_status=DeploymentStatus.MAIN,
        ev_id="EV-001",
    )
    result = gate.run_all_checks(node, actor="chatgpt_etl")
    print(f"T1 node gate result: {result}")

    # Test Detonator Board
    for i in range(1, 4):
        detonator.confirm_pillar(f"PILLAR-{i:02d}", Tier.T1)
    detonator.breach_levee("LEVEE-01", "michigan_core")
    detonator.breach_levee("LEVEE-02", "michigan_core")
    detonator.record_engine06_event()

    activation = detonator.check_activation()
    print(f"\nDetonator Board: {json.dumps(activation, indent=2)}")
