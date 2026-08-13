# Principal Data Architect Study Guide — Round 1
## End-to-End Architecture & Service Selection (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Business & Non-Functional Requirements

Before touching architecture, a Principal Architect anchors every decision to explicit requirements. Vague requirements produce vague (and usually over-engineered or under-engineered) architectures.

### Functional Requirements (What the platform must do)

- Ingest data from relational databases (core banking, loan systems), mainframe feeds, REST APIs (third-party/fintech), files (CSV/JSON/XML/Parquet from partners), and real-time streaming events (payments, transactions)
- Support both batch (nightly/hourly) and streaming (sub-minute) processing
- Serve regulatory reporting, finance reporting, operational dashboards, and API-based consumption
- Support machine learning feature pipelines (fraud detection, credit risk)
- Provide historical, point-in-time queryable data for audit and regulatory reconstruction

### Non-Functional Requirements (How well the platform must do it)

| Category | Realistic Assumption |
|---|---|
| **Data Volume** | 300GB/day today, growing to 5-8TB/day within 3 years |
| **Velocity** | Mix — 70% batch (EOD feeds), 30% streaming (payments, card transactions) |
| **Latency (streaming)** | Fraud detection: < 2 seconds; operational dashboards: < 5 minutes |
| **Latency (batch)** | EOD batch complete within 4-hour overnight window |
| **Availability** | 99.9% for serving layer; 99.99% for streaming ingestion (payments cannot be dropped) |
| **RPO** | 0 for transactional/payment data; 15 minutes for analytical layers |
| **RTO** | 1 hour for critical regulatory pipelines; 4 hours for analytical/reporting |
| **Retention** | 7 years raw data (regulatory minimum), 10 years for certain risk data |
| **Regulatory** | UK GDPR, PRA/FCA reporting (Basel III/IV, IFRS 9), PSD2, BCBS 239 (data aggregation risk) |
| **Data Privacy** | PII masking/tokenisation mandatory for non-privileged consumers; right-to-erasure workflows |
| **Query Concurrency** | 200+ concurrent analysts/BI users during business hours; burst to 500 during month-end close |
| **Reporting** | Scheduled regulatory submissions (daily/monthly/quarterly) + ad-hoc analyst queries |
| **API Consumption** | Sub-second lookups for customer-facing and internal API applications |
| **Growth** | 10-15x volume growth over 3 years; concurrency growth 3-4x |

### Why This Matters Before Architecture

BCBS 239 (risk data aggregation) is a real, specific regulatory driver in banking that most generic GCP tutorials never mention — it directly demands lineage, auditability, and the ability to reconstruct any report at any historical point. This single requirement eliminates architectures that can't prove data provenance.

**Interview tip:** Naming BCBS 239 or PRA/FCA signals real domain fluency, not generic cloud knowledge. Generic "GDPR compliance" answers sound junior; naming BCBS 239's data aggregation/reconstruction requirement sounds like you've actually worked in banking.

---

## Part B: End-to-End Architecture

### Text-Based Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                                │
│  Core Banking (Oracle/DB2) │ Mainframe (COBOL/VSAM) │ REST APIs        │
│  Partner Files (CSV/XML)   │ Kafka/Streaming Events  │ Batch Feeds     │
└───────────┬─────────────────────────┬──────────────────┬──────────────┘
            │                         │                  │
     ┌──────▼──────┐          ┌───────▼───────┐   ┌──────▼──────┐
     │  Datastream │          │    Pub/Sub     │   │  Transfer   │
     │    (CDC)    │          │  (Streaming)   │   │  Service /  │
     │             │          │                │   │  SFTP→GCS   │
     └──────┬──────┘          └───────┬───────┘   └──────┬──────┘
            │                         │                  │
            │                  ┌──────▼──────┐            │
            │                  │  Dataflow   │            │
            │                  │ (Streaming) │            │
            │                  └──────┬──────┘            │
            │                         │                  │
     ┌──────▼─────────────────────────▼──────────────────▼──────┐
     │              CLOUD STORAGE — DATA LAKE                     │
     │   Landing → Raw → Quarantine → Validated → Curated        │
     └──────┬───────────────────────────────────────────────────┘
            │
     ┌──────▼──────┐
     │  Dataflow / │
     │  Dataproc / │
     │  Dataform   │  ← Validation, DQ, Transformation, Enrichment
     └──────┬──────┘
            │
     ┌──────▼──────────────────────────────────────────────────┐
     │                      BIGQUERY                             │
     │   Curated Datasets → Modelled (Star/Vault) → Marts        │
     └──────┬─────────────────┬──────────────────┬──────────────┘
            │                 │                  │
     ┌──────▼──────┐   ┌──────▼──────┐    ┌──────▼──────┐
     │  BI / Looker│   │  Reg. Report│    │  APIs (Cloud│
     │  Analysts   │   │  Marts      │    │  Run/Fns)   │
     └─────────────┘   └─────────────┘    └─────────────┘
                                                  │
                                           ┌──────▼──────┐
                                           │  Vertex AI  │
                                           │  (ML/Fraud) │
                                           └─────────────┘

  CROSS-CUTTING LAYERS (apply to every stage above):
  ─────────────────────────────────────────────────
  Governance/Lineage : Dataplex + Data Catalog
  Security            : IAM, KMS, VPC-SC, Secret Manager
  Orchestration       : Cloud Composer
  Observability       : Cloud Logging, Cloud Monitoring
  CI/CD               : Terraform, Cloud Build, Artifact Registry
```

### Data Flow Narrative

**Batch path:** Core banking and mainframe data lands via Datastream (CDC where the source supports log-based capture) or scheduled extracts pushed via SFTP into a GCS landing zone. From landing, a validation stage checks schema and structural integrity before promoting files to raw. Raw is immutable — the permanent system of record. Dataflow or Dataproc batch jobs then apply deduplication, standardisation, and business transformations, writing curated output either back to GCS (for large-scale Parquet-based marts) or directly into BigQuery native tables.

**Streaming path:** Payment and transaction events publish to Pub/Sub. Dataflow streaming pipelines consume, apply windowing and enrichment, and write simultaneously to BigQuery (for near-real-time analytics) and GCS (for the permanent raw record — streaming data is not exempt from the "GCS is source of truth" principle).

**Serving path:** BigQuery is the single analytical serving engine — curated datasets feed dimensional marts for reporting, isolated regulatory marts for PRA/FCA submissions, and low-latency API lookups via Cloud Run querying BigQuery or a synced operational store (Bigtable/Cloud SQL) where sub-100ms response is required and BigQuery's query latency profile doesn't fit.

**Governance runs through everything, not beside it:** Dataplex enforces zone-based governance directly on the GCS lake (landing/raw/curated as governed zones, not just folder conventions), while Data Catalog captures technical and business metadata, and column-level security/data masking policies propagate automatically to BigQuery consumers.

---

## Part C: GCP Service Selection — With Rationale

For each service: why selected, problem solved, alternatives considered, why alternatives lose (or win in specific cases).

### 1. Ingestion — Batch: Datastream

**Why selected:** Log-based CDC from Oracle/MySQL/PostgreSQL core banking systems without hand-rolled extraction jobs; writes directly into GCS or BigQuery with schema drift handling built in.

**Alternatives considered:**
- *Custom JDBC extraction via Dataflow* — viable, but rebuilds CDC logic, checkpointing, and schema drift handling Datastream already solves. Justified only if the source isn't Datastream-supported.
- *Debezium on GKE* — more flexible, supports more source types (including some legacy DB2 variants), but adds operational burden of running/patching a Kafka Connect cluster. Justified only when Datastream's source support genuinely doesn't cover the estate.

**Operational implication:** Datastream has finite source-system support. Genuine mainframe (COBOL/VSAM) CDC isn't natively handled — you'll likely still need a mainframe-side extract process (IBM replication tools or batch unload) landing files into GCS.

**Interview honesty point:** Don't claim Datastream solves mainframe CDC seamlessly — naming the gap and proposing a realistic extract pattern is a stronger answer than pretending it's seamless.

**Cost implication:** Priced per GB processed — cheap at low volume, scales linearly, worth monitoring as CDC volume grows with transaction volume.

---

### 2. Ingestion — Streaming: Pub/Sub

**Why selected:** Fully managed, no cluster to run, native Dataflow integration, at-least-once delivery with exactly-once processing achievable in combination with Dataflow.

**Alternatives considered:**
- *Confluent Kafka on GCP* — better fit with existing enterprise Kafka standards, fine-grained partition control, or cross-cloud portability needs. Legitimate choice if the bank already has Kafka expertise elsewhere (very plausible during an on-prem migration).
- *Cloud Tasks* — wrong tool; designed for task queuing/work distribution, not high-throughput event streaming.

**When Kafka beats Pub/Sub:** If the bank already operates Kafka on-premise (highly likely) and is doing a lift-and-shift rather than green-field build, Kafka preserves existing operational knowledge, existing MirrorMaker-based DR patterns, and avoids re-architecting consumer applications. Pub/Sub wins when building green-field, wanting zero operational overhead, and not needing Kafka-specific features (exact partition-key control, long retention replay beyond Pub/Sub's 7-day max, mature multi-region active-active patterns).

**Operational implication:** Pub/Sub retention caps at 7 days — genuine long-term replay requires landing to GCS immediately, reinforcing GCS as the real system of record, not Pub/Sub itself.

---

### 3. Streaming Processing: Dataflow

**Why selected:** Apache Beam-based, unified batch and streaming programming model, autoscaling, native windowing/watermark support essential for late-arriving payment events.

**Alternatives considered:**
- *Dataproc (Spark Structured Streaming)* — legitimate if the engineering team's skill set is Spark-heavy (common migrating from Hadoop/Spark/Databricks) and a consistent Spark codebase between batch and streaming is wanted. Dataflow's advantage: serverless autoscaling, no cluster sizing decisions. Dataproc's advantage: portability, team familiarity.
- *Cloud Functions/Cloud Run* — wrong tool beyond simple, stateless per-message transformation. No native windowing, exactly-once semantics, or backpressure handling.

**Performance implication:** Dataflow's autoscaling reacts to Pub/Sub backlog automatically — directly relevant to a fraud-detection 2-second SLA, since a manually-sized Dataproc cluster requires pre-provisioning for peak load or risking latency spikes during transaction bursts.

---

### 4. Data Lake Storage: Cloud Storage

**Why selected:** Durable, cheap, immutable object storage as the permanent system of record — non-negotiable for regulatory reconstruction requirements (BCBS 239, PRA data lineage expectations).

No real alternative for this role — this is the correct default. Detailed zone structure, file formats, and partitioning strategy covered in Part D and in Round 2/3.

---

### 5. Batch Transformation: Dataflow, Dataproc, and Dataform — Three Tools, Three Distinct Jobs

Don't default to one tool for everything.

- **Dataflow**: Transformations that must run identically whether data arrives via batch or streaming (genuine unified-pipeline advantage); complex record-level transformations (parsing nested XML/JSON, custom deduplication logic).
- **Dataproc**: When transformation logic is inherited from an existing Spark codebase (likely during EMR/Cloudera/Databricks migration) and rewriting in Beam isn't justified by timeline/budget. Stronger for large-scale joins/shuffles needing direct Spark tuning control.
- **Dataform**: Transformations that are fundamentally SQL — curated marts, dimensional models, business logic living inside BigQuery. This is the dbt-equivalent: version-controlled, tested, documented SQL. Most Silver→Gold logic (fact/dimension construction, regulatory mart calculations) belongs here.

**Decision rule for interview:** *"Push transformation as close to SQL as the complexity allows, because SQL is auditable by non-engineers — which matters enormously in a regulated bank where finance and risk teams need to verify transformation logic themselves. Reserve Dataflow/Dataproc for transformations SQL genuinely cannot express — complex XML/mainframe copybook parsing, custom deduplication algorithms, ML feature engineering."*

---

### 6. Analytical Serving: BigQuery

**Why selected:** Serverless, separates storage/compute, scales to concurrency and volume requirements without cluster management, native partitioning/clustering/materialized views (detailed in Round 2/3 performance sections).

**Alternatives considered:**
- *Bigtable* — wrong layer for analytical/SQL reporting; it's for low-latency operational/NoSQL lookups (e.g., real-time fraud scoring). Has a role in this architecture, just not as the warehouse.
- *Cloud SQL / Spanner* — Spanner relevant only if the platform also owns a transactional system of record requiring globally-consistent, horizontally-scalable OLTP storage. Over-engineered for an analytical/reporting-focused use case unless a genuine multi-region strong-consistency transactional requirement exists.

---

### 7. Low-Latency Operational Store: Bigtable (used selectively)

**Why included:** The API consumption NFR requires sub-second latency. BigQuery, even with BI Engine acceleration, isn't designed for single-row point lookups at that latency profile reliably under concurrent load. Bigtable serves that specific need (e.g., "give me this customer's current risk score"), fed by a sync pipeline from BigQuery or directly from the streaming Dataflow job.

**Deliberate inclusion, not sprawl:** Added only because a specific NFR (sub-second API latency) demands it — not "more services = better architecture."

---

### 8. Orchestration: Cloud Composer

**Why selected:** Managed Airflow — necessary because the pipeline has genuine cross-system dependencies (ingestion → validation → DQ → transformation → curated load → snapshot → consumer notification) needing DAG-based dependency management, retry logic, and SLA monitoring.

**Alternatives considered:**
- *Workflows* — lighter-weight, good for simple service-orchestration chains. Wrong tool for a multi-stage data pipeline with backfill/replay requirements.
- *Dataform's built-in scheduling* — fine for orchestrating purely SQL transformation dependencies within Dataform, but doesn't extend to ingestion or cross-tool orchestration. Composer sits above Dataform and calls it as one stage among many.

---

### 9. Governance: Dataplex + Data Catalog

**Why selected:** Dataplex applies zone-based governance (landing/raw/curated as logical, policy-enforced zones) directly on GCS without physically restructuring buckets, and auto-discovers metadata into Data Catalog — including automated PII/sensitive data discovery.

**Why this matters more than a generic answer:** In a bank, "who can see this column" is the difference between compliant and non-compliant. Dataplex's policy tags propagate directly into BigQuery column-level security — a single classification decision (tagging a column as PII) enforces access control everywhere that column is queried, without per-report manual masking logic.

---

### 10. Security: IAM, Cloud KMS, VPC Service Controls, Secret Manager

- **IAM**: Least-privilege service accounts per pipeline stage — ingestion service account cannot read curated regulatory marts, and vice versa. Separation of duties by design.
- **Cloud KMS (CMEK)**: Mandatory for a bank — default Google-managed encryption is often insufficient for regulatory/audit expectations around key ownership and rotation control.
- **VPC Service Controls**: Creates a security perimeter around BigQuery/GCS so data cannot be exfiltrated outside the approved network boundary, even with valid IAM credentials — addresses "legitimate access used to accidentally or maliciously copy data out."
- **Secret Manager**: Credentials for source system connections — never in code, never in Composer DAG files directly.

---

## Part D: Data Lake Zone Structure (Preview)

```
gs://bank-data-lake/
├── landing/       ← raw drop zone, source format, short retention
├── raw/           ← immutable, permanent, source format preserved
├── quarantine/    ← failed validation/DQ, held for investigation
├── validated/     ← passed schema/structural checks
├── curated/       ← business-ready, Parquet, partitioned
└── archive/       ← lifecycle-managed cold storage, 7yr+ retention
```

Full detail on file formats, partitioning strategy, small-file problems, and compaction to be covered in a later round.

---

## Interview Talking Points — Quick Reference

1. **Name the regulatory driver, not just "compliance"** — BCBS 239, PRA/FCA, PSD2. Specificity signals real banking experience.
2. **Be honest about Datastream's mainframe gap** — don't claim CDC tools solve everything.
3. **Have a real answer for "why not Kafka"** — most banks have existing Kafka estates; acknowledge migration cost rather than assuming Pub/Sub is obviously superior.
4. **Justify every service by NFR, not by default** — e.g., Bigtable exists because of a specific sub-second latency requirement, not because "more services = better architecture."
5. **SQL-first transformation philosophy** — push logic into Dataform/BigQuery SQL where possible, because it's auditable by non-engineers (finance/risk teams) — a real differentiator in regulated environments.

---

## Next Rounds (Planned)

- **Round 2**: Batch + Streaming Ingestion Design (CDC, idempotency, late-arriving files, exactly-once, dead letter queues, replay/backfill)
- **Round 3**: Data Lake + BigQuery Architecture (file formats, partitioning, clustering, materialized views, large-table strategies)
- **Round 4**: Data Modelling (star schema vs. data vault vs. wide tables for regulatory/finance/risk reporting)
- **Round 5**: Performance Optimisation (Dataflow/Beam tuning, Spark on Dataproc, BigQuery query troubleshooting)
- **Round 6**: Data Quality, Governance & Lineage Deep Dive
- **Round 7**: Security Architecture Deep Dive
- **Round 8**: Orchestration, Error Handling & Resilience
- **Round 9**: Observability & Cost Optimisation
- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 1 of ongoing Lloyds Data Architect interview preparation series.*
