# Principal Data Architect Study Guide — Round 6
## Data Quality, Governance & Lineage (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Data Quality Architecture

### A.1 Why a Reusable Framework, Not Per-Pipeline Rules

The weak pattern: every team writes their own ad-hoc validation logic inside their own transformation jobs — inconsistent rule definitions, no central visibility, no way to answer "what's our overall data quality posture" without asking every team individually. The correct pattern for an enterprise banking platform: a **centrally defined, centrally executed, but domain-configurable** DQ framework, where rules are declared as metadata (not buried in code), executed consistently at defined pipeline gates, and surfaced through a single operational view.

**Design principle:** data quality rules are configuration, not code. A risk analyst or data steward should be able to define "this column must not be null" or "this value must exist in the reference table" without needing an engineer to write and deploy new pipeline code every time a rule changes.

---

### A.2 The Six Dimensions of Data Quality — Applied Concretely

| Dimension | Definition | Concrete Banking Example |
|---|---|---|
| **Completeness** | Required data is present, not missing | `settlement_date` must not be null on any settled transaction |
| **Accuracy** | Data correctly reflects reality | Transaction amount matches the amount confirmed by the counterparty system |
| **Validity** | Data conforms to defined format/domain rules | `currency` must be a valid ISO 4217 code; `account_id` must match expected format |
| **Consistency** | Data agrees across systems/tables | The same `customer_id` has the same `risk_rating` whether queried from the lending domain or the payments domain |
| **Uniqueness** | No unintended duplicates | `transaction_id` + `source_system` combination must be unique in the fact table |
| **Timeliness** | Data arrives and is available within expected SLA | EOD batch data available in curated by 06:00 UTC; late arrival beyond that triggers an alert, not silent absence |

**Referential integrity and reconciliation are added as banking-specific extensions on top of these six:**

- **Referential integrity**: every `counterparty_id` in `fact_transactions` must exist in `dim_counterparty` — enforced as a DQ check, not just an aspirational foreign key (BigQuery doesn't enforce FK constraints natively, so this must be an active validation step, not an assumption).
- **Reconciliation**: aggregate totals (record count, sum of transaction amounts) in the curated layer must match aggregate totals from the source system extraction — this is the direct implementation of the "prove nothing was lost" principle from Round 2's batch ingestion design.

---

### A.3 Where Rules Are Configured

**A central DQ rules repository** — practically implemented as a BigQuery control table (or a lightweight config-as-code approach in the CI/CD repository, versioned like any other artifact):

```
control.dq_rules
├── rule_id
├── dataset_name
├── table_name
├── column_name (nullable, for table-level rules)
├── rule_type          (completeness | validity | uniqueness | referential | reconciliation)
├── rule_definition     (e.g., SQL expression or reference-table lookup)
├── severity            (BLOCKING | WARNING)
├── owner               (data steward responsible)
├── is_active
└── effective_from
```

**Why severity matters as a first-class field, not an afterthought:** not every failed rule should stop a pipeline. A `BLOCKING` rule (e.g., duplicate `transaction_id`) halts promotion to curated and routes to quarantine. A `WARNING` rule (e.g., a slightly unusual but not impossible value) logs and alerts but allows the pipeline to continue — conflating these into a single "fail/pass" model either creates constant false-alarm pipeline stoppages or, worse, trains teams to ignore failures altogether because too many are non-critical.

---

### A.4 Where Rules Execute

**Rules execute at defined pipeline gates, not scattered arbitrarily through transformation logic:**

```
Raw → [DQ Gate 1: Structural/Completeness] → Validated
Validated → [DQ Gate 2: Business Rules/Referential/Uniqueness] → Curated
Curated → [DQ Gate 3: Reconciliation against source] → Published (flag=GREEN)
```

This directly extends the flag-table gating pattern already established earlier in this preparation series — DQ Gate 3's reconciliation check is precisely what sets the flag table to GREEN before consumers are permitted to query.

**Implementation choice:** DQ Gate 1 and 2 checks run as part of the Dataflow/Dataproc/Dataform transformation job itself (checking data as it's processed, cheapest point to catch and route failures). DQ Gate 3 (reconciliation) runs as a **separate, dedicated Dataform/BigQuery SQL job** after curated load completes, comparing aggregate figures — this separation matters because reconciliation inherently requires comparing *before* and *after* states across the full pipeline, which doesn't fit naturally inside a single transformation stage.

---

### A.5 How Failures Are Stored

Every DQ failure — whether `BLOCKING` or `WARNING` — writes a structured record to a central failures table, **not** just a log line buried in job execution logs:

```
control.dq_failures
├── failure_id
├── rule_id
├── dataset_name / table_name / column_name
├── record_identifier      (e.g., transaction_id, for traceability back to the specific record)
├── failure_value
├── severity
├── batch_run_id            (links back to Round 2's batch_run_audit table)
├── detected_at
├── status                  (OPEN | INVESTIGATING | RESOLVED | ACCEPTED_RISK)
└── resolution_notes
```

**Why this table matters beyond operations:** this is directly what you show a regulator or internal audit team when asked "how do you know your data is trustworthy" — a queryable, historical, auditable record of every quality issue detected and how it was resolved, not a claim of "we have good processes" without evidence.

---

### A.6 How Alerts Are Generated

`BLOCKING` failures trigger immediate Cloud Monitoring alerts (routed to the on-call data engineering team) — a blocked pipeline is an active incident, not a background metric. `WARNING` failures aggregate into a daily digest reviewed by the relevant data steward, unless volume spikes unexpectedly (a sudden increase in warnings for a previously-stable rule is itself alert-worthy, since it often signals an upstream schema or business process change before anyone officially reports one).

---

### A.7 How Consumers See Data Quality Status

A **data quality dashboard** (built on top of `control.dq_failures` and the flag table), surfaced to consumers alongside the data itself — ideally integrated into Data Catalog's table description/tags, so an analyst opening a table in BigQuery sees its current DQ status and recent history directly, not as a separate system they have to remember to check. For regulatory-critical tables specifically, the flag table's GREEN/AMBER/RED status (established earlier in this series) is the authoritative, binary "is this safe to use" signal — the dashboard provides the supporting detail behind that status.

---

### A.8 How Failed Data Is Quarantined

Records failing a `BLOCKING` rule are written to the `quarantine` GCS zone (Round 3) with the full failure context attached (which rule, what value, when detected) — never silently dropped, never allowed to proceed into curated. The quarantine zone is the operational queue data stewards work from; once a root cause is identified and fixed (a source system correction, a reference data update), the specific quarantined records are reprocessed through the pipeline as a **targeted replay** — the same replay mechanism established in Round 2, now applied specifically to DQ-driven reprocessing rather than only late-arriving-data reprocessing.

---

## Part B: Metadata, Governance and Lineage

### B.1 Business Metadata vs. Technical Metadata

- **Technical metadata**: schema definitions, column types, partition/cluster keys, table size, last-modified timestamp, physical storage location. Largely auto-captured by Dataplex/Data Catalog scanning GCS and BigQuery directly — minimal manual effort required.
- **Business metadata**: what a table/column actually *means* in business terms — "this is the customer's risk rating as assessed by the credit risk model," a table's business owner, its regulatory classification, its approved use cases. This requires deliberate human input (data stewards, domain owners) and is where governance efforts most commonly fail — technical metadata capture is largely automatic; business metadata requires organisational discipline to keep current.

**Design decision:** technical metadata capture is fully automated via Dataplex discovery jobs. Business metadata is entered by designated data stewards per domain, but the platform enforces a **minimum required field set** (business owner, classification, description) before a table can be promoted from validated to curated — making business metadata a gate, not an optional nice-to-have that quietly never gets filled in.

---

### B.2 Data Ownership and Data Product Ownership

Every curated dataset has a named **business owner** (accountable for the data's correctness and business definition) and a named **technical owner** (accountable for the pipeline producing it) — recorded as required metadata fields, not implicit or assumed. This directly supports the separation-of-duties and accountability requirements from Round 1, and becomes essential when a DQ failure needs a fast, unambiguous escalation path — "whose data is this" should never be a question that takes time to answer during an incident.

**Data product ownership** extends this concept — increasingly, mature data platforms treat curated domains (payments, lending, customer) as **data products** with defined SLAs, documented schemas/contracts, and accountable owners, rather than as anonymous pipeline output. This connects to the data mesh thinking discussed earlier in this preparation series — decentralised ownership, centrally governed standards.

---

### B.3 Data Classification and Sensitive Data Discovery

Every column in curated datasets is classified along a defined taxonomy:

```
PUBLIC        → no restriction
INTERNAL      → bank employees only, no external sharing
CONFIDENTIAL  → restricted to specific roles, masking required for others
PII           → subject to GDPR right-to-erasure, strict masking, audit-logged access
REGULATORY    → subject to specific regulatory handling (retention, access, audit) rules
```

**Dataplex's sensitive data discovery** (built on DLP - Data Loss Prevention API integration) scans curated data to automatically flag likely PII (names, account numbers, national insurance numbers) that may not have been explicitly classified by a human — a genuine safety net, since manual classification alone risks missing fields, especially as new columns get added over time. Auto-discovered PII is flagged for steward review, not automatically enforced blind — false positives (a column named `customer_reference` that's actually an internal code, not personal data) need human judgement, but the discovery process itself should never rely solely on manual classification being remembered for every new field.

---

### B.4 PII, Data Retention, and Data Access Policies

**Column-level policy tags** (Dataplex, propagating to BigQuery column-level security, as established in Round 1) enforce that a column classified `PII` is automatically masked for any role not explicitly granted the "view unmasked PII" permission — this is enforced centrally at the classification level, meaning a new report or dashboard built against that table inherits the masking automatically, rather than requiring every report builder to remember to apply masking themselves.

**Data retention** ties directly to classification — `REGULATORY` data follows the 7-10 year retention/archival lifecycle from Round 3; genuinely non-regulatory `PII` data is subject to **right-to-erasure** workflows under GDPR, which creates a specific architectural tension worth naming directly in an interview:

**The right-to-erasure tension in an immutable raw layer:** Round 2 and 3 established raw data as immutable — the permanent regulatory system of record. But GDPR's right to erasure requires the ability to delete a specific individual's data on request. These two requirements are in direct tension, and a Principal Architect must resolve it explicitly, not gloss over it.

**Resolution approach:** distinguish between data with an active regulatory retention justification (transaction records required for BCBS 239/PRA reconstruction — retention *overrides* erasure requests under GDPR's own legal-obligation exemption) versus data with no such override (marketing preference data, non-regulatory customer profile attributes). For the latter, implement **crypto-shredding** — personal data fields are encrypted with per-customer keys; "erasure" is achieved by destroying that customer's specific encryption key, rendering the data permanently unreadable without physically rewriting the immutable raw files. This satisfies both the immutability requirement (files are never edited) and the erasure requirement (data becomes cryptographically inaccessible).

**Interview-ready framing:** *"Immutable raw storage and GDPR erasure aren't actually incompatible if you architect for it upfront — the key insight is that most banking data has a legitimate legal-obligation basis for retention that overrides erasure requests, and for the genuinely erasable subset, crypto-shredding lets me satisfy 'delete this person's data' without violating the immutability that regulatory reconstruction depends on. I wouldn't want to discover this tension after the raw layer is already built."*

---

### B.5 Lineage

**Automated lineage** (Dataplex/Data Catalog, integrated with Dataflow, Dataproc, and Dataform job metadata) traces every curated table's data back through every transformation stage to its raw source — this is the direct replacement for the "data modelling sheet" manual-documentation anti-pattern discussed earlier in this preparation series' Databricks conversation. Automated lineage means:

- A schema or logic change in an upstream transformation is immediately visible in terms of every downstream table it affects — critical for impact analysis before making a change.
- A data quality investigation ("why is this number wrong") can trace backward through actual executed transformations, not rely on documentation that may be stale or was never updated after a code change.
- Regulatory lineage requests (BCBS 239's aggregation traceability requirement) are answered by querying the lineage graph directly, not by reconstructing history manually from memory or scattered documentation.

**Design principle stated plainly for the interview:** *"Lineage has to be a byproduct of how the pipeline actually executes, captured automatically by the platform, not a separately-maintained document that drifts out of sync with reality the first time someone changes code without updating the doc. That drift is exactly the failure mode I've seen cause real incidents in past platforms."*

---

### B.6 How Dataplex Fits the Whole Architecture — Synthesis

Dataplex isn't a single-purpose tool bolted on for compliance — across this round and Round 1, it's doing four distinct jobs simultaneously:

1. **Zone-based governance** on GCS — landing/raw/curated as policy-enforced zones (Round 1/3).
2. **Automated technical metadata discovery** — schema, structure, freshness, feeding Data Catalog (B.1 above).
3. **Sensitive data discovery** — DLP-integrated PII flagging feeding the classification taxonomy (B.3 above).
4. **Lineage capture** — tracing transformations across Dataflow/Dataproc/Dataform automatically (B.5 above).

**Interview-ready synthesis:** *"I think of Dataplex as the governance backbone that makes the rest of the architecture's compliance claims actually provable — without it, 'we have good data governance' is a statement of intent; with it, classification, lineage, and access control become structurally enforced and auditable, which is the difference that actually matters when a regulator asks for evidence rather than a description."*

---

## Part C: Design Decisions Summary Table

| Decision | Choice | Why |
|---|---|---|
| DQ rule definition | Config-as-metadata in central control table | Rules changeable by data stewards without engineering redeploys |
| DQ severity model | BLOCKING vs. WARNING, not binary pass/fail | Prevents both false-alarm pipeline halts and alert fatigue |
| DQ execution point | Embedded in transformation (Gates 1-2) + dedicated reconciliation job (Gate 3) | Reconciliation inherently needs before/after comparison, doesn't fit a single stage |
| Failure storage | Structured, queryable `dq_failures` table, not log lines | Auditable evidence for regulatory/internal audit review |
| Business metadata | Mandatory gated fields before curated promotion | Prevents business metadata from being an ignored nice-to-have |
| PII discovery | Automated (DLP via Dataplex) + human steward review | Manual classification alone misses fields; pure automation risks false positives |
| Right-to-erasure vs. immutable raw | Legal-obligation override for regulatory data; crypto-shredding for genuinely erasable data | Resolves the GDPR/immutability tension explicitly rather than ignoring it |
| Lineage capture | Fully automated via Dataplex/Data Catalog | Manual documentation drifts and becomes unreliable; automation is a byproduct of execution, not a separate maintained artifact |

---

## Interview Talking Points — Quick Reference

1. **DQ rules are configuration, not code** — a data steward can change a rule without an engineering deployment. Name this explicitly; it signals platform maturity.
2. **Severity (BLOCKING vs. WARNING) is a deliberate design choice**, not a missing detail — explain why a single pass/fail model fails in practice (alert fatigue or false-alarm halts).
3. **The GDPR right-to-erasure vs. immutable-raw-layer tension is a genuinely sharp interview question** — having crypto-shredding and the legal-obligation-override distinction ready as a resolved answer (not a hand-wave) is a strong differentiator.
4. **Lineage must be automated, not documented** — directly and explicitly contrast this against the "data modelling spreadsheet" anti-pattern; it's a concrete, memorable point.
5. **Dataplex does four distinct jobs, not one** — governance zones, metadata discovery, sensitive data discovery, lineage. Naming all four shows depth beyond "Dataplex is the governance tool."
6. **Business metadata requires a gate, technical metadata is automatic** — this distinction, and the fact that most governance failures happen on the business-metadata side, is a mature, experience-signalling point.

---

## Next Rounds (Planned)

- **Round 7**: Security Architecture Deep Dive
- **Round 8**: Orchestration, Error Handling & Resilience
- **Round 9**: Observability & Cost Optimisation
- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 6 of ongoing Lloyds Data Architect interview preparation series.*
