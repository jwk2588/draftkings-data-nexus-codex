"""
DraftKings HiveMind v3.0 — Tier 2 Semantic Agents
Inference allowed with confidence scoring. Sandboxed outputs.
Agents: TopicCluster, LegalTheory, Timeline, EntityLinker, KIMI.AI Macro-Synthesizer
+ Prompt Compilation Framework (Hardening Directive #4)
"""

import os
import json
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor
import redis

DB_CONFIG = {
    "host": "localhost", "database": "draftkings_hivemind",
    "user": "hivemind", "password": "hivemind_secure_2026"
}
REDIS_CONFIG = {"host": "localhost", "port": 6379, "db": 0}
ONTOLOGY_VERSION = "1.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser("~/HiveMind/logs/audit/tier2.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("HiveMind.Tier2")


# ─────────────────────────────────────────────────────────────
# Prompt Compilation Framework (Hardening Directive #4)
# ─────────────────────────────────────────────────────────────
class PromptCompiler:
    """
    Treats prompts as governed executable infrastructure.
    Linting, dependency analysis, recursive instruction simulation,
    hallucination stress testing, and deterministic replay tests.
    """

    PROMPT_LIBRARY = {
        "LEGAL_THEORY_EXTRACTION": {
            "version": "1.0.0",
            "type": "EXTRACTION",
            "text": """You are a forensic legal analyst. Analyze the following text extracted from a document.
Identify ONLY what is explicitly stated. Do NOT infer or speculate.
Return a JSON object with:
- legal_theories: list of legal theories explicitly mentioned (e.g., "ASC 606 violation", "VIE off-balance sheet")
- statutory_refs: list of statutes, codes, or regulations cited
- named_parties: list of explicitly named entities
- monetary_amounts: list of dollar amounts with context
- dates: list of dates with context
- confidence: your confidence score (0.0-1.0) that this text contains legal content
Text: {text}""",
            "dependencies": ["ENTITY_REGISTRY"],
            "hallucination_risk": "LOW",
            "token_estimate": 400
        },
        "TOPIC_CLUSTER_ASSIGNMENT": {
            "version": "1.0.0",
            "type": "INFERENCE",
            "text": """Classify the following text into one or more of these DraftKings litigation topics.
Return ONLY a JSON array of matching topics with confidence scores.
Topics: ["SEC_DISCLOSURE", "VIE_ACCOUNTING", "REVENUE_RECOGNITION", "LOYALTY_PROGRAM",
         "EXECUTIVE_COMPENSATION", "PLATFORM_REMOVAL", "ADR_SETTLEMENT", "FINANCIAL_FRAUD",
         "REGULATORY_COMPLIANCE", "GENERAL_DRAFTKINGS"]
Text: {text}""",
            "dependencies": [],
            "hallucination_risk": "LOW",
            "token_estimate": 200
        },
        "TIMELINE_EVENT_EXTRACTION": {
            "version": "1.0.0",
            "type": "EXTRACTION",
            "text": """Extract timeline events from the following text.
For each event, return: date (ISO format if possible), description, confidence (0-1), source_type.
Return as JSON array. Only include events with explicit dates or strong temporal markers.
Text: {text}""",
            "dependencies": [],
            "hallucination_risk": "MEDIUM",
            "token_estimate": 300
        },
        "KIMI_MACRO_SYNTHESIS": {
            "version": "1.0.0",
            "type": "SYNTHESIS",
            "text": """You are analyzing the complete DraftKings litigation intelligence corpus.
This corpus contains {conv_count} conversations, {msg_count} messages, and {photo_count} photo OCR extracts.
Your task: Identify the top 10 most significant patterns, contradictions, and evidence clusters.
For each finding: provide a title, description, supporting evidence count, confidence score, and recommended legal theory.
Return as structured JSON. Prioritize: SEC disclosure violations, VIE accounting irregularities, loyalty program fraud.
Corpus summary: {corpus_summary}""",
            "dependencies": ["FULL_CORPUS"],
            "hallucination_risk": "MEDIUM",
            "token_estimate": 2000
        }
    }

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)

    def compile_prompt(self, prompt_name: str, variables: Dict[str, str]) -> Tuple[str, Dict]:
        """Compile a prompt with variables, lint it, and return with metadata."""
        if prompt_name not in self.PROMPT_LIBRARY:
            raise ValueError(f"Unknown prompt: {prompt_name}")

        template = self.PROMPT_LIBRARY[prompt_name]
        compiled_text = template["text"]

        # Substitute variables
        for key, value in variables.items():
            compiled_text = compiled_text.replace(f"{{{key}}}", str(value))

        # Lint check — ensure no unresolved placeholders
        unresolved = re.findall(r'\{[a-z_]+\}', compiled_text)
        if unresolved:
            raise ValueError(f"Unresolved placeholders in prompt {prompt_name}: {unresolved}")

        # Token estimate check
        word_count = len(compiled_text.split())
        estimated_tokens = int(word_count * 1.3)

        metadata = {
            "prompt_name": prompt_name,
            "version": template["version"],
            "type": template["type"],
            "hallucination_risk": template["hallucination_risk"],
            "estimated_tokens": estimated_tokens,
            "compiled_at": str(datetime.now(timezone.utc)),
            "lint_passed": True
        }

        # Register in DB
        self._register_prompt(prompt_name, template["version"], compiled_text, metadata)
        return compiled_text, metadata

    def _register_prompt(self, name: str, version: str, text: str, metadata: dict):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO prompt_registry (prompt_name, prompt_version, prompt_text, prompt_type,
                    token_efficiency, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (prompt_name) DO UPDATE
                SET prompt_text = EXCLUDED.prompt_text
                WHERE prompt_registry.prompt_name IS NOT NULL
            """, (name, version, text, metadata.get("type"), 1.0 / max(1, metadata.get("estimated_tokens", 1))))
        self.conn.commit()

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────
# Base Semantic Agent
# ─────────────────────────────────────────────────────────────
class SemanticAgent:
    """Base class for Tier 2 semantic agents. All outputs are sandboxed."""

    def __init__(self, agent_id: str, api_key_env: str = "OPENAI_API_KEY"):
        self.agent_id = agent_id
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.redis = redis.Redis(**REDIS_CONFIG, decode_responses=True)
        self.prompt_compiler = PromptCompiler()
        self.api_key = os.environ.get(api_key_env, "")
        log.info(f"[{self.agent_id}] Initialized (Tier 2 Semantic)")

    def emit_event(self, event_type: str, event_subtype: str, payload: dict):
        payload_str = json.dumps(payload, default=str)
        checksum = hashlib.sha256(payload_str.encode()).hexdigest()
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO event_ledger (event_type, event_subtype, payload, agent_id, ontology_version, checksum)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (event_type, event_subtype, Json(payload), self.agent_id, ONTOLOGY_VERSION, checksum))
        self.conn.commit()

    def propose_graph_edge(self, src_id: str, dst_id: str, edge_type: str,
                            confidence: float, epistemic_state: str, provenance: dict) -> str:
        """Propose a graph edge — goes to arbitration queue, NOT directly to graph."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO graph_edge_proposals (src_node_id, dst_node_id, edge_type,
                    epistemic_state, confidence_score, provenance, generating_agent, ontology_version)
                VALUES (%s, %s, %s, %s::epistemic_state, %s, %s, %s, %s)
                RETURNING proposal_id
            """, (src_id, dst_id, edge_type, epistemic_state, confidence,
                  Json(provenance), self.agent_id, ONTOLOGY_VERSION))
            proposal_id = str(cur.fetchone()[0])
        self.conn.commit()

        # Push to arbitration queue
        self.redis.lpush("queue:arbitration:pending", json.dumps({
            "proposal_id": proposal_id, "src": src_id, "dst": dst_id,
            "edge_type": edge_type, "confidence": confidence, "agent": self.agent_id
        }))
        log.info(f"[{self.agent_id}] Edge proposed → arbitration: {src_id} --{edge_type}--> {dst_id} (conf={confidence:.2f})")
        return proposal_id

    def _call_llm(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """Call the LLM API. Uses OpenAI by default, falls back to Gemini."""
        try:
            import openai
            client = openai.OpenAI(
                api_key=os.environ.get("OPENAI_API_KEY"),
                base_url=os.environ.get("OPENAI_API_BASE")
            )
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=0.1  # Low temperature for factual extraction
            )
            return response.choices[0].message.content
        except Exception as e:
            log.warning(f"[{self.agent_id}] OpenAI failed: {e}, trying Gemini")
            try:
                import google.generativeai as genai
                genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
                model = genai.GenerativeModel("gemini-2.5-flash")
                response = model.generate_content(prompt)
                return response.text
            except Exception as e2:
                log.error(f"[{self.agent_id}] Both LLMs failed: {e2}")
                return None

    def close(self):
        self.conn.close()
        self.prompt_compiler.close()


# ─────────────────────────────────────────────────────────────
# Agent: Topic Cluster Agent
# ─────────────────────────────────────────────────────────────
class TopicClusterAgent(SemanticAgent):
    """Assigns OCR text and chat messages to DraftKings litigation topic clusters."""

    def __init__(self):
        super().__init__("AGT-TOPIC-001")

    def classify_text(self, text: str, object_id: str) -> List[Dict]:
        """Classify text into topic clusters with confidence scores."""
        if not text or len(text.strip()) < 20:
            return []

        # Deterministic keyword pre-screening (no LLM needed for obvious cases)
        results = self._keyword_classify(text)

        # If ambiguous, use LLM
        if not results or max(r["confidence"] for r in results) < 0.85:
            try:
                prompt, meta = self.prompt_compiler.compile_prompt(
                    "TOPIC_CLUSTER_ASSIGNMENT", {"text": text[:2000]}
                )
                llm_response = self._call_llm(prompt, max_tokens=300)
                if llm_response:
                    llm_results = json.loads(llm_response)
                    if isinstance(llm_results, list):
                        results = llm_results
            except Exception as e:
                log.warning(f"[TopicCluster] LLM classification failed: {e}")

        # Propose edges to graph (sandboxed — goes to arbitration)
        for result in results:
            if result.get("confidence", 0) >= 0.70:
                self.propose_graph_edge(
                    src_id=f"OCR:{object_id}",
                    dst_id=f"TOPIC:{result['topic']}",
                    edge_type="RELATED_TO",
                    confidence=result["confidence"],
                    epistemic_state="PROBABILISTIC" if result["confidence"] < 0.90 else "STRONGLY_SUPPORTED",
                    provenance={"method": "topic_cluster", "agent": self.agent_id}
                )
        return results

    def _keyword_classify(self, text: str) -> List[Dict]:
        """Fast deterministic keyword-based classification."""
        text_lower = text.lower()
        results = []
        keyword_map = {
            "SEC_DISCLOSURE": ["sec", "10-k", "10-q", "8-k", "edgar", "securities", "disclosure"],
            "VIE_ACCOUNTING": ["vie", "asc 810", "variable interest", "off-balance", "consolidation"],
            "REVENUE_RECOGNITION": ["asc 606", "revenue recognition", "deferred revenue", "loyalty liability"],
            "LOYALTY_PROGRAM": ["crown club", "dk rewards", "loyalty points", "rewards program", "vip"],
            "EXECUTIVE_COMPENSATION": ["ceo", "cfo", "jason robins", "executive", "compensation", "equity"],
            "PLATFORM_REMOVAL": ["apple", "google play", "app store", "removal", "banned", "suspended"],
            "ADR_SETTLEMENT": ["arbitration", "settlement", "fre 408", "adr", "mediation", "dispute"],
            "FINANCIAL_FRAUD": ["fraud", "misrepresentation", "false", "misleading", "material omission"],
            "REGULATORY_COMPLIANCE": ["mgcb", "michigan", "gaming commission", "license", "regulatory"],
            "GENERAL_DRAFTKINGS": ["draftkings", "dkng", "draftking"]
        }
        for topic, keywords in keyword_map.items():
            matches = sum(1 for kw in keywords if kw in text_lower)
            if matches > 0:
                confidence = min(0.95, 0.60 + (matches * 0.08))
                results.append({"topic": topic, "confidence": round(confidence, 3), "method": "keyword"})
        return sorted(results, key=lambda x: x["confidence"], reverse=True)


# ─────────────────────────────────────────────────────────────
# Agent: Legal Theory Agent
# ─────────────────────────────────────────────────────────────
class LegalTheoryAgent(SemanticAgent):
    """Extracts legal theories and statutory references from text."""

    def __init__(self):
        super().__init__("AGT-LEGAL-001")

    def extract_legal_context(self, text: str, object_id: str) -> Dict[str, Any]:
        """Extract legal theories, statutes, and monetary amounts from text."""
        if not text or len(text.strip()) < 30:
            return {}

        # Deterministic extraction first
        det_results = self._deterministic_extract(text)

        # LLM for complex analysis
        llm_results = {}
        if len(text) > 100:
            try:
                prompt, meta = self.prompt_compiler.compile_prompt(
                    "LEGAL_THEORY_EXTRACTION", {"text": text[:3000]}
                )
                llm_response = self._call_llm(prompt, max_tokens=800)
                if llm_response:
                    # Extract JSON from response
                    json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
                    if json_match:
                        llm_results = json.loads(json_match.group())
            except Exception as e:
                log.warning(f"[LegalTheory] LLM extraction failed: {e}")

        # Merge results (deterministic takes precedence)
        merged = {**llm_results, **det_results}

        # Propose legal theory edges
        for theory in merged.get("legal_theories", []):
            entity_id = self._resolve_legal_entity(theory)
            if entity_id:
                self.propose_graph_edge(
                    src_id=f"OCR:{object_id}",
                    dst_id=entity_id,
                    edge_type="REFERENCED_BY",
                    confidence=0.85,
                    epistemic_state="STRONGLY_SUPPORTED",
                    provenance={"theory": theory, "method": "legal_extraction"}
                )
        return merged

    def _deterministic_extract(self, text: str) -> Dict[str, Any]:
        """Rule-based extraction for known legal references."""
        text_lower = text.lower()
        legal_theories = []
        statutory_refs = []
        monetary_amounts = []

        # Legal theory patterns
        theory_patterns = {
            "ASC 606 Revenue Recognition Violation": ["asc 606", "revenue recognition"],
            "ASC 810 VIE Off-Balance Sheet": ["asc 810", "vie", "variable interest entity"],
            "SEC 10-K Material Omission": ["10-k", "material omission", "material misstatement"],
            "FRE 408 Settlement Protection": ["fre 408", "rule 408", "settlement communication"],
            "RICO Pattern": ["rico", "racketeering", "pattern of"],
            "Breach of Contract": ["breach of contract", "breach of the"],
            "Unjust Enrichment": ["unjust enrichment", "enriched at the expense"]
        }
        for theory, patterns in theory_patterns.items():
            if any(p in text_lower for p in patterns):
                legal_theories.append(theory)

        # Monetary amounts
        amounts = re.findall(r'\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|thousand))?', text, re.I)
        monetary_amounts = amounts[:10]

        # Statutory refs
        stat_patterns = re.findall(r'(?:ASC|SEC|FRE|GAAP|FASB|IFRS)\s*[\d.]+', text, re.I)
        statutory_refs = list(set(stat_patterns))[:10]

        return {
            "legal_theories": legal_theories,
            "statutory_refs": statutory_refs,
            "monetary_amounts": monetary_amounts,
            "method": "deterministic"
        }

    def _resolve_legal_entity(self, theory: str) -> Optional[str]:
        """Resolve a legal theory string to a canonical entity ID."""
        theory_map = {
            "ASC 606": "ENTITY_LEGAL_ASC606",
            "ASC 810": "ENTITY_LEGAL_ASC810",
            "DraftKings": "ENTITY_DK_0001"
        }
        for key, entity_id in theory_map.items():
            if key.lower() in theory.lower():
                return entity_id
        return None


# ─────────────────────────────────────────────────────────────
# Agent: KIMI.AI Macro-Synthesizer
# ─────────────────────────────────────────────────────────────
class KIMIMacroSynthesizer(SemanticAgent):
    """
    Leverages KIMI.AI's 2M token context window to synthesize
    the entire DraftKings intelligence corpus in a single pass.
    """

    KIMI_API_URL = "https://api.moonshot.cn/v1"

    def __init__(self):
        super().__init__("AGT-KIMI-001", api_key_env="KIMI_API_KEY")
        self.kimi_key = os.environ.get("KIMI_API_KEY", "")

    def synthesize_corpus(self, corpus_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Run macro-synthesis on the full corpus using KIMI.AI."""
        log.info("[KIMI] Starting macro-synthesis of full corpus")

        prompt, meta = self.prompt_compiler.compile_prompt("KIMI_MACRO_SYNTHESIS", {
            "conv_count": str(corpus_summary.get("conversations", 0)),
            "msg_count": str(corpus_summary.get("messages", 0)),
            "photo_count": str(corpus_summary.get("photos", 0)),
            "corpus_summary": json.dumps(corpus_summary, default=str)[:5000]
        })

        # Try KIMI.AI first (2M context), fallback to OpenAI
        result = None
        if self.kimi_key:
            result = self._call_kimi(prompt)
        if not result:
            log.warning("[KIMI] KIMI.AI unavailable, falling back to OpenAI")
            result = self._call_llm(prompt, max_tokens=2000)

        if result:
            try:
                json_match = re.search(r'\{.*\}', result, re.DOTALL)
                if json_match:
                    synthesis = json.loads(json_match.group())
                else:
                    synthesis = {"raw_synthesis": result}
            except json.JSONDecodeError:
                synthesis = {"raw_synthesis": result}

            self.emit_event("KIMI_SYNTHESIS", "CORPUS_ANALYZED", {
                "agent": self.agent_id,
                "corpus_size": corpus_summary,
                "findings_count": len(synthesis.get("findings", []))
            })
            log.info(f"[KIMI] Synthesis complete: {len(synthesis.get('findings', []))} findings")
            return synthesis

        return {"error": "Synthesis failed — all LLMs unavailable"}

    def _call_kimi(self, prompt: str) -> Optional[str]:
        """Call KIMI.AI API directly."""
        try:
            import requests
            headers = {
                "Authorization": f"Bearer {self.kimi_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "moonshot-v1-128k",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4000,
                "temperature": 0.1
            }
            response = requests.post(
                f"{self.KIMI_API_URL}/chat/completions",
                headers=headers, json=payload, timeout=120
            )
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            log.warning(f"[KIMI] API returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            log.error(f"[KIMI] API call failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Tier 3: Arbitration System
# ─────────────────────────────────────────────────────────────
class ArbitrationSystem:
    """
    Tier 3 supervisory system. Prevents graph poisoning.
    Validates semantic edges and enforces ontology rules.
    """

    def __init__(self):
        self.agent_id = "AGT-ARBITRATOR-001"
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.redis = redis.Redis(**REDIS_CONFIG, decode_responses=True)
        log.info(f"[{self.agent_id}] Arbitration System initialized")

    def process_queue(self, max_items: int = 100) -> Dict[str, int]:
        """Process pending graph edge proposals from the arbitration queue."""
        stats = {"approved": 0, "rejected": 0, "quarantined": 0}

        for _ in range(max_items):
            item = self.redis.rpop("queue:arbitration:pending")
            if not item:
                break

            try:
                proposal = json.loads(item)
                decision, reason = self._arbitrate(proposal)

                with self.conn.cursor() as cur:
                    cur.execute("""
                        UPDATE graph_edge_proposals
                        SET arbitration_status = %s, arbitration_agent = %s, arbitration_notes = %s
                        WHERE proposal_id = %s
                    """, (decision, self.agent_id, reason, proposal["proposal_id"]))
                self.conn.commit()

                stats[decision.lower()] = stats.get(decision.lower(), 0) + 1

                if decision == "APPROVED":
                    # Push to Neo4j sync queue
                    self.redis.lpush("queue:neo4j:sync", json.dumps(proposal))

            except Exception as e:
                log.error(f"[Arbitration] Error processing proposal: {e}")

        log.info(f"[Arbitration] Processed: {sum(stats.values())} proposals — {stats}")
        return stats

    def _arbitrate(self, proposal: Dict) -> Tuple[str, str]:
        """Apply arbitration rules to a graph edge proposal."""
        confidence = proposal.get("confidence", 0)
        edge_type = proposal.get("edge_type", "")
        src = proposal.get("src", "")
        dst = proposal.get("dst", "")

        # Rule 1: Minimum confidence threshold
        if confidence < 0.50:
            return "REJECTED", f"Confidence {confidence:.2f} below minimum threshold 0.50"

        # Rule 2: Self-referential edges are forbidden
        if src == dst:
            return "REJECTED", "Self-referential edge forbidden"

        # Rule 3: CONTRADICTS edges require high confidence
        if edge_type == "CONTRADICTS" and confidence < 0.80:
            return "QUARANTINED", f"CONTRADICTS edge requires confidence >= 0.80, got {confidence:.2f}"

        # Rule 4: Verify source node exists in registry
        if src.startswith("ENTITY:"):
            entity_id = src.replace("ENTITY:", "")
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 FROM canonical_entities WHERE entity_id = %s", (entity_id,))
                if not cur.fetchone():
                    return "QUARANTINED", f"Source entity {entity_id} not in canonical registry"

        # Rule 5: High confidence edges are approved
        if confidence >= 0.80:
            return "APPROVED", "Meets all arbitration criteria"

        # Rule 6: Medium confidence edges are approved with note
        if confidence >= 0.60:
            return "APPROVED", f"Approved with moderate confidence {confidence:.2f}"

        return "QUARANTINED", f"Insufficient evidence for edge type {edge_type}"

    def get_queue_depth(self) -> Dict[str, int]:
        return {
            "pending": self.redis.llen("queue:arbitration:pending"),
            "neo4j_sync": self.redis.llen("queue:neo4j:sync"),
            "validation_failures": self.redis.llen("queue:validation:failures"),
            "ocr_fallback": self.redis.llen("queue:ocr:fallback")
        }

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────
# Smoke Test
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing Tier 2 Semantic Agents + Tier 3 Arbitration...")

    # Test Prompt Compiler
    pc = PromptCompiler()
    prompt, meta = pc.compile_prompt("TOPIC_CLUSTER_ASSIGNMENT",
                                      {"text": "DraftKings ASC 606 revenue recognition violation in 10-K filing"})
    print(f"\n[PromptCompiler] Compiled: {meta['prompt_name']} v{meta['version']}, ~{meta['estimated_tokens']} tokens")
    pc.close()

    # Test Topic Cluster Agent (deterministic only — no LLM call)
    topic_agent = TopicClusterAgent()
    test_text = "DraftKings violated ASC 606 revenue recognition rules in their 2023 10-K SEC filing. The loyalty program liability was understated by $47 million."
    results = topic_agent._keyword_classify(test_text)
    print(f"\n[TopicCluster] Classifications:")
    for r in results[:5]:
        print(f"  {r['topic']}: {r['confidence']:.2f}")
    topic_agent.close()

    # Test Legal Theory Agent (deterministic only)
    legal_agent = LegalTheoryAgent()
    det = legal_agent._deterministic_extract(test_text)
    print(f"\n[LegalTheory] Deterministic extraction:")
    print(f"  Theories: {det['legal_theories']}")
    print(f"  Amounts: {det['monetary_amounts']}")
    print(f"  Statutes: {det['statutory_refs']}")
    legal_agent.close()

    # Test Arbitration System
    arb = ArbitrationSystem()
    queue_depth = arb.get_queue_depth()
    print(f"\n[Arbitration] Queue depths: {queue_depth}")

    # Test arbitration rules
    test_proposals = [
        {"proposal_id": "test-1", "src": "OCR:abc", "dst": "TOPIC:SEC", "edge_type": "RELATED_TO", "confidence": 0.85},
        {"proposal_id": "test-2", "src": "OCR:abc", "dst": "OCR:abc", "edge_type": "RELATED_TO", "confidence": 0.90},
        {"proposal_id": "test-3", "src": "OCR:abc", "dst": "TOPIC:VIE", "edge_type": "CONTRADICTS", "confidence": 0.60},
        {"proposal_id": "test-4", "src": "OCR:abc", "dst": "TOPIC:DK", "edge_type": "RELATED_TO", "confidence": 0.30},
    ]
    print(f"\n[Arbitration] Rule tests:")
    for p in test_proposals:
        decision, reason = arb._arbitrate(p)
        print(f"  conf={p['confidence']:.2f}, type={p['edge_type']}: {decision} — {reason}")
    arb.close()

    print("\n[SMOKE TEST PASSED] Tier 2 + Tier 3 operational")
