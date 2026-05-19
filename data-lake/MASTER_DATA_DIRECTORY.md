# Master DraftKings Data Directory
*Manus Directive v1.0, Section 4.3 — Auto-generated index of all source artifacts*
*Spine: MasterBrief_v54 (EV-001–EV-291, SB-01–SB-66)*

---

## Source Artifacts

| Artifact | DK Domain | Tier | Status | Raw Path | Curated Path | Graph Nodes |
|---|---|---|---|---|---|---|
| MasterBrief_v54_CONSOLIDATED-2.docx | All Domains | T1 | CONTROLLING | `/data-lake/raw/` | `/data-lake/curated/mbv54/` | NEXUS-001 |
| MBv52_Reformatted | All Domains | T2 | RETAINED DEPTH | `/data-lake/raw/` | `/data-lake/curated/mbv52/` | NEXUS-002 |
| DK-Master-Conversation-Summary | All Domains | T2 | RETAINED DEPTH | `/data-lake/raw/` | `/data-lake/curated/conv-summary/` | NEXUS-003 |
| conversations (2).json | ChatGPT Export | T2 | ACTIVE ETL | `/data-lake/raw/` | `/data-lake/chatgpt/normalized/` | NEXUS-020–023 |
| chat (1).html | ChatGPT Export | T2 | OPTIONAL | `/data-lake/raw/` | — | — |
| DK Authority 2026 MasterBrief | Platform / ToS | T1 | PENDING UPLOAD | `/data-lake/raw/` | `/data-lake/curated/authority-2026/` | — |
| DK ToS Change-Log Analyses | ToS Drift | T1 | PENDING UPLOAD | `/data-lake/raw/` | `/data-lake/curated/tos/` | — |
| Levee Inversion PDF | Levee Analysis | T1 | PENDING UPLOAD | `/data-lake/raw/` | `/data-lake/curated/levees/` | — |
| Crosswalks | Regulatory | T1 | PENDING UPLOAD | `/data-lake/raw/` | `/data-lake/curated/crosswalks/` | — |

---

## DK Domain Map

| Domain | Symbolic Name | Enterprise Name | Controlling EV Range | Pillars | Levees |
|---|---|---|---|---|---|
| Michigan Core | Michigan Core | Michigan MGCB Litigation | EV-001–EV-009 | PILLAR-01 | LEVEE-01 |
| ASC 606 / Calendar Bleed | Engine 3 | Revenue Recognition Cluster | EV-010–EV-019 | PILLAR-02, PILLAR-13 | LEVEE-02, LEVEE-15 |
| CFTC Railbird | CFTC Railbird | CFTC Jurisdictional Exposure | EV-020–EV-029 | PILLAR-03 | LEVEE-03 |
| Apple Platform | Apple Lane | Apple Platform Economics | EV-030–EV-039 | PILLAR-04 | LEVEE-04 |
| Privacy / AML | Privacy/AML | Privacy & AML Compliance | EV-040–EV-049 | PILLAR-05 | LEVEE-05, LEVEE-06 |
| RICO | RICO | RICO Pattern Evidence | EV-050–EV-059 | PILLAR-06 | LEVEE-08 |
| AutoZone Lane | AutoZone Lane | AutoZone Competitive Lane | EV-060–EV-069 | PILLAR-07 | LEVEE-09 |
| Dynasty 483 | Dynasty 483 | Dynasty 483 Inflation | EV-070–EV-079 | PILLAR-08 | LEVEE-10 |
| Convergence | Detonator Board | High-Impact Decision Trigger Matrix | All | PILLAR-25 | LEVEE-27 |

---

## Data Lake Layer Map

| Layer | Path | Contents | Tier |
|---|---|---|---|
| raw | `/data-lake/raw/` | Original files, unmodified | T1/T2 as sourced |
| staged | `/data-lake/staged/` | Extracted text + basic metadata | T2 |
| curated | `/data-lake/curated/` | Normalized, deduplicated, DK-focused | T1/T2 |
| chatgpt/raw | `/data-lake/chatgpt/raw/` | Raw ChatGPT JSONL | T2 |
| chatgpt/normalized | `/data-lake/chatgpt/normalized/` | Classified + tagged Parquet | T2 |
| chatgpt/metadata | `/data-lake/chatgpt/metadata/` | Data dictionary + image metadata | T2 |
| triples | `/data-lake/curated/triples/` | Gemini-extracted ER triples | T2 |

---

## Graph Node Index

All graph nodes are registered in `/nexus-governance/node-register/nexus_node_register.yaml`.

| NEXUS ID | Label | Tier | Status |
|---|---|---|---|
| NEXUS-001 | MBv54LegalSpine | T1 | main |
| NEXUS-010 | EvidenceCluster_MichiganCore | T1 | main |
| NEXUS-011 | EvidenceCluster_ASC606 | T1 | main |
| NEXUS-012 | EvidenceCluster_CFTC | T1 | main |
| NEXUS-013 | EvidenceCluster_Apple | T1 | main |
| NEXUS-014 | EvidenceCluster_PrivacyAML | T1 | main |
| NEXUS-015 | EvidenceCluster_RICO | T1 | main |
| NEXUS-016 | EvidenceCluster_AutoZone | T1 | main |
| NEXUS-017 | EvidenceCluster_Dynasty483 | T1 | main |
| NEXUS-020 | ChatGPTExport_Raw | T2 | quarantine |
| NEXUS-021 | ChatGPTExport_DK_CORE | T2 | main |
| NEXUS-022 | ChatGPTExport_DK_ADJACENT | T2 | appendix |
| NEXUS-023 | ChatGPTExport_NON_DK | T3 | quarantine |
| NEXUS-030 | Agent_MichiganMGCB | T2 | main |
| NEXUS-031 | Agent_CFTCRailbird | T2 | main |
| NEXUS-032 | Agent_ASC606Audit | T2 | main |
| NEXUS-040 | GraphMutationEventLog | T1 | main |
| NEXUS-041 | DetectorBoard | T1 | main |
