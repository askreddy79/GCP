# Principal Data Architect Study Guide — Round 2
## Batch & Streaming Ingestion Design (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Batch Ingestion Design

### A.1 The Core Design Problem

Batch ingestion in banking isn't "copy files from A to B on a schedule." It's about guaranteeing that every file, every row, and every change is accounted for — provably — because regulators can ask you to reconstruct any historical state. Every design decision below exists to answer one underlying question: **"Can you prove nothing was lost, duplicated, or silently corrupted?"**

---

### A.2 Database Extraction

**Pattern: Datastream CDC where supported, watermark-based extraction where not.**

For Oracle/PostgreSQL/MySQL core banking systems, Datastream reads the transaction log directly (log-based CDC) — this avoids querying the source database repeatedly (which would add load to a production banking system you don't want to touch) and captures inserts, updates, and deletes as discrete change events rather than full-table snapshots.

**Where Datastream doesn't apply (legacy DB2, certain mainframe-adjacent stores):** Fall back to watermark-based extraction — a scheduled Dataflow or Composer-orchestrated job that queries `WHERE last_modified_ts > :last_watermark` and advances the watermark only after a successful, verified load. The watermark itself must be stored durably (a small BigQuery control table or Firestore), never in pipeline memory — if the job crashes mid-run, restart must resume from the last *committed* watermark, not the last *attempted* one.

**Why this distinction matters in an interview:** A common mistake is treating "we use CDC" as a blanket answer. A Principal Architect names exactly which sources get log-based CDC and which get watermark extraction, and explains why — that specificity is what separates a real design from a slide.

---

### A.3 File Ingestion

**Pattern: SFTP/Transfer Service → GCS landing zone → validation-gated promotion to raw.**

Partner and third-party files (CSV/XML) typically arrive via SFTP or a push mechanism outside your control. The landing zone is deliberately separate from raw:

- **Landing**: files arrive in their original, uncontrolled form. Short retention (a few days). Nothing downstream reads directly from landing.
- **Raw**: only files that pass structural validation (file is complete, checksum matches manifest if provided, schema matches expected contract) are promoted here. Raw is immutable and permanent.

This separation exists because a partially-transferred or corrupted file must never contaminate raw — raw's entire value as a regulatory system of record depends on every file in it being verified-complete.

---

### A.4 Incremental Loads & Watermarking

**Design principle: the watermark is the single source of truth for "what have we already processed," and it must survive pipeline restarts, worker failures, and reprocessing.**

Store watermarks in a dedicated BigQuery control table:

```
control.ingestion_watermarks
├── source_system_id
├── table_name
├── last_successful_watermark_ts
├── last_run_id
├── last_run_status
└── updated_at
```

The watermark advances **only** after the full load-and-validate cycle succeeds — not optimistically at job start. This is the single most common bug in batch ingestion design: advancing the watermark before confirming the data actually landed correctly, which silently skips records on the next run if something failed partway through.

---

### A.5 Idempotency

**Every batch load must be safely re-runnable without creating duplicates.**

Two complementary techniques:

1. **Natural-key based MERGE, not INSERT.** Load into a staging table, then `MERGE` into the target using the source's natural key (e.g., transaction_id + source_system_id). Re-running the same batch twice produces the same end state, not duplicated rows.
2. **Batch-level idempotency tokens.** Each batch run gets a unique `batch_id`. Before processing, check whether that `batch_id` has already been marked complete in the control table — if so, skip (or force-reprocess only on explicit operator override).

**Why both, not just one:** The MERGE handles row-level duplication; the batch token handles the case where an entire batch is accidentally triggered twice (e.g., an orchestration bug re-fires the same DAG run).

---

### A.6 Late-Arriving Files

**Design principle: late arrival must not silently corrupt already-published curated data.**

If a partner file for `2026-08-10` arrives on `2026-08-13` (three days late), the ingestion pipeline must:

- Detect the file belongs to a historical partition (based on filename convention or file content date, not arrival date)
- Load it into raw as normal (raw accepts data regardless of arrival timing — that's what makes it a reliable record)
- Trigger a **targeted reprocessing** of only the affected downstream partition (`2026-08-10`'s curated tables and any snapshot/regulatory marts derived from it) — not a full historical rebuild
- Flag the affected downstream consumers (regulatory reporting teams especially) that a historical partition was updated, since a report already submitted for that date may now be based on incomplete data

This last point is a genuine banking-specific concern: if a regulatory report for a prior date was already submitted and late data arrives, that's not just a technical reprocessing question — it's a business process question about whether a restated report is required. The architecture's job is to make that discoverable and auditable, not to make the decision itself.

---

### A.7 Duplicate Files

**Detection at landing, before any processing cost is incurred:**

- Compute a checksum (MD5/SHA-256) of each incoming file at landing
- Compare against a control table of previously-processed file checksums for that source
- Exact duplicate → quarantine with a `DUPLICATE_FILE` reason code, alert but don't fail the pipeline
- Same filename but different checksum (a corrected resend) → this is legitimate and common in banking file exchanges; route to a defined "corrected file" handling path rather than treating it as an error

---

### A.8 Schema Validation & Bad Record Handling

**Two-tier validation: structural (file level) and record level.**

**File-level (landing → raw gate):**
- Column count matches contract
- Required columns present
- File encoding valid
- Row count within expected tolerance of historical average (a file with 5% of expected rows likely indicates a truncated transfer)

**Record-level (raw → validated gate):**
- Data type conformance (a string in a decimal field)
- Mandatory field null checks
- Referential integrity against reference data (a currency code that doesn't exist in the reference table)
- Business rule validation (a settlement date before a trade date)

**Bad record handling:** Records failing record-level validation are written to a `quarantine` zone with the original record, the specific rule(s) it failed, and a timestamp — never silently dropped. This quarantine table becomes the operational queue that data quality analysts work from, and it's directly auditable if a regulator asks "how do you handle bad data."

---

### A.9 Replay & Backfill

**Because raw is immutable and complete, replay and backfill are the same operation run against a different time range — this is precisely why the medallion/zone separation exists.**

- **Replay** (reprocess a period due to a transformation bug fix): re-run the validated→curated transformation logic against the existing raw data for the affected date range. Raw never needs to be re-fetched from source.
- **Backfill** (load historical data not yet in the platform): a one-time, larger-scale version of the same batch ingestion pattern, typically run with higher parallelism and against a dedicated backfill GCS prefix to avoid interfering with live daily ingestion, then merged in after validation.

**Design implication:** every transformation job must be pure/deterministic — given the same raw input, it produces the same curated output every time. Any transformation with hidden external state (e.g., "current date" used implicitly rather than passed as a parameter) breaks replay-ability. This is a specific, testable interview point: *"How do you guarantee your transformation jobs are replay-safe?"* — the answer is deterministic, parameterised jobs with no implicit runtime state.

---

### A.10 Auditability

Every batch run must be reconstructable after the fact:

```
control.batch_run_audit
├── run_id
├── source_system_id
├── file_names_processed[]
├── file_checksums[]
├── record_count_landed
├── record_count_validated
├── record_count_quarantined
├── record_count_curated
├── watermark_before
├── watermark_after
├── started_at / completed_at
└── status
```

This table itself becomes a queryable, permanent record — when a regulator asks "prove this report reflects exactly the source data available as of date X," this audit trail plus immutable raw data is the proof.

---

### A.11 Decision Framework: Datastream vs. Dataflow vs. Dataproc vs. Custom Ingestion

| Scenario | Recommended Tool |
|---|---|
| Source supports log-based CDC (Oracle, PostgreSQL, MySQL) | **Datastream** |
| Source is legacy/mainframe without CDC support | **Custom extract (mainframe-side) → SFTP → GCS**, then Dataflow for landing→raw promotion |
| Complex file parsing (nested XML, copybook-derived formats) | **Dataflow** — Beam's per-record processing model handles complex parsing well |
| Transformation logic inherited from existing Spark codebase | **Dataproc** — avoid a costly full rewrite if timeline doesn't justify it |
| High-volume watermark-based JDBC extraction, no CDC available | **Dataflow** with JDBC connector, custom watermark logic |
| Simple, well-defined batch triggers with no complex transformation | **Composer + Dataform** (SQL-only path, no need for Beam/Spark at all) |

---

## Part B: Streaming Architecture

### B.1 Reference Design

```
Source Event
    │
    ▼
Pub/Sub Topic (partitioned by ordering key = account_id or transaction_id)
    │
    ▼
Dataflow Streaming Pipeline
    │  - Windowing (event-time based)
    │  - Watermark tracking
    │  - Deduplication (based on message ID + ordering key)
    │  - Enrichment (reference data lookup)
    │
    ├──► BigQuery (streaming insert — near-real-time analytics)
    ├──► GCS (permanent raw record — same principle as batch)
    └──► Dead Letter Topic (Pub/Sub) — records that fail processing
```

---

### B.2 Partitioning & Ordering

**Pub/Sub does not guarantee global ordering by default — ordering keys are required for any sequence-dependent data (e.g., a transaction and its reversal).**

Use `orderingKey = account_id` (or `transaction_id` for related event chains) — this guarantees messages with the same key are delivered in publish order to a single subscriber, at the cost of some parallelism (messages with the same key can't be processed concurrently across workers). For payment/transaction data where order genuinely matters (a debit must be processed before its corresponding reversal), this trade-off is mandatory, not optional.

**Design implication for Dataflow:** the pipeline must be written to respect ordering keys through the transformation — a naive implementation that reshuffles data (e.g., a `GroupByKey` on a different key) can silently break ordering guarantees even though Pub/Sub delivered messages in order. This is a genuine, common bug — worth naming explicitly in an interview.

---

### B.3 Exactly-Once Considerations

Pub/Sub guarantees **at-least-once** delivery. Exactly-once *processing* is achieved through the combination of Pub/Sub + Dataflow, which deduplicates based on message ID within a configurable time window (default 10 minutes, extendable).

**Where this breaks down and what to do about it:** if a duplicate arrives *outside* Dataflow's dedup window (a redelivery after a long consumer outage, for example), Dataflow-level dedup won't catch it. The safety net is the same MERGE-based idempotency pattern used in batch — the final BigQuery write uses a natural key MERGE, not a blind append, so even a duplicate that slips past Dataflow's window doesn't create a duplicate row in the served table.

**Interview-ready answer:** *"Exactly-once is achieved in layers — Pub/Sub plus Dataflow's built-in dedup handles the common case, and a natural-key MERGE at the BigQuery write layer is the backstop for the edge case where a duplicate arrives outside the dedup window."*

---

### B.4 Event Time vs. Processing Time

- **Event time**: when the transaction actually occurred (as recorded by the source system)
- **Processing time**: when the pipeline actually handles the record

Banking analytics — especially regulatory reporting — must use **event time** for windowing and aggregation, because a transaction that occurred at 23:58 but arrives at the pipeline at 00:02 the next day still belongs to the prior business day for reporting purposes. Using processing time here would produce genuinely incorrect regulatory numbers.

---

### B.5 Watermarks & Late Events

Dataflow's watermark estimates "how far behind real event-time has the pipeline caught up" and determines when a windowed aggregation is considered complete enough to emit.

**Design for late events, don't just tolerate them:**
- Set an **allowed lateness** window (e.g., 24 hours) during which late-arriving events still update an already-emitted window's aggregate, via Beam's triggering/accumulation mode
- Events arriving after the allowed lateness window are routed to a **late-data side output** — logged, alerted, and handled via a separate reconciliation path rather than silently dropped

This directly matters for banking: a transaction that arrives 25 hours late (past a 24-hour lateness allowance) must still be accounted for — it cannot simply vanish because it missed a window. The late-data side output feeds back into the same reconciliation process described in batch's late-arriving-file handling.

---

### B.6 Dead Letter Queues

Any record that fails schema validation, enrichment lookup (e.g., reference data missing), or transformation logic is routed to a **dead letter Pub/Sub topic**, not dropped and not allowed to fail the entire pipeline (a single bad record must never block the stream).

The dead letter topic feeds:
- An alerting pipeline (Cloud Monitoring alert if DLQ volume exceeds a threshold — a sudden spike often indicates an upstream schema change, not isolated bad records)
- A separate reprocessing pipeline once the root cause is fixed, replaying DLQ messages back through the main pipeline

---

### B.7 Replay Strategy (Streaming)

Because Pub/Sub retention is capped (7 days max), long-term replay for streaming data relies on the **GCS raw layer**, exactly as in batch — this is the specific reason streaming pipelines write to GCS in parallel with BigQuery, even though it adds a write path. Without this, replaying more than 7 days of streaming history would be impossible.

**Replay execution:** a Dataflow **batch** pipeline (not streaming) reads the GCS raw records for the affected time range and reprocesses them through the same transformation logic used by the streaming path — this is why keeping transformation logic in shared, reusable code (not duplicated between batch and streaming pipeline definitions) matters architecturally, not just for code cleanliness.

---

### B.8 Back Pressure

Dataflow's autoscaling responds to Pub/Sub subscription backlog — if downstream (e.g., a BigQuery streaming insert) slows down, Dataflow scales workers up to compensate, and Pub/Sub itself buffers messages up to its retention window rather than dropping them.

**Where back pressure becomes a real design concern:** if a downstream dependency (reference data lookup service, enrichment API) becomes the bottleneck rather than raw throughput, autoscaling Dataflow workers doesn't help — you're just creating more concurrent callers to an already-overloaded dependency. The fix is architectural: cache reference data locally in the Dataflow workers (side input pattern) rather than calling out per-record, which removes the external dependency from the hot path entirely.

---

### B.9 Schema Evolution

Streaming schemas will change — a source system adds a new field, changes a type, or deprecates a column.

**Design approach:**
- Use a **schema registry pattern** (even a simple BigQuery control table tracking known schema versions per source works, though Pub/Sub schema support can be used natively)
- New optional fields: forward-compatible by design — the pipeline should not fail on unrecognized fields, simply pass them through and pick them up on the next code deployment
- Breaking changes (type changes, field removal): trigger a DLQ routing for affected messages and an alert, rather than a silent failure or a pipeline crash — a schema-breaking change should surface as a visible, actionable event, not an outage

---

### B.10 Failure Recovery

Dataflow streaming pipelines checkpoint state automatically — on worker failure, processing resumes from the last checkpoint without data loss, provided the pipeline is written with idempotent side effects (the MERGE pattern again, not blind appends).

**Pipeline-level failure (not just worker failure):** if the entire streaming job needs to be redeployed (a bug fix, a scaling change), Dataflow supports **update-in-place** for compatible pipeline graph changes, preserving in-flight state. For incompatible changes, a **drain** (process all in-flight data, then stop cleanly) followed by a fresh pipeline start from the last committed Pub/Sub subscription position is the safe pattern — never a hard `cancel`, which can lose in-flight, unacknowledged messages.

---

### B.11 When Pub/Sub + Dataflow Beats a Kafka-Based Approach — And When It Doesn't

**Pub/Sub + Dataflow wins when:**
- Building green-field with no existing Kafka investment
- Minimizing operational overhead is a priority (no cluster/broker management)
- Autoscaling based on backlog is valuable (variable, bursty transaction volumes — e.g., payment spikes)
- Tight native integration with BigQuery/GCS is the primary consumption pattern

**Kafka wins when:**
- The bank already operates Kafka on-premise — migration cost of re-architecting every producer/consumer application is a real, often underestimated cost
- Retention beyond 7 days for raw replay from the broker itself is required (though the GCS raw-layer pattern above mitigates this for GCP)
- Fine-grained partition control or exotic consumer group patterns are needed
- Existing DR tooling (MirrorMaker-based multi-region active-active) is a proven, trusted pattern the bank doesn't want to abandon

**Interview-ready answer:** *"I wouldn't default to Pub/Sub just because it's GCP-native. If Lloyds has an existing Kafka estate — which is likely — the real question is whether this is a green-field build or a migration. For green-field, Pub/Sub plus Dataflow reduces operational burden significantly. For a migration of an existing Kafka-dependent estate, Confluent Kafka on GCP may be the lower-risk choice, preserving existing consumer applications and DR patterns, with a longer-term evaluation of migrating to Pub/Sub once the estate stabilizes on GCP."*

---

## Part C: Batch vs. Streaming Decision Framework

| Question | Batch | Streaming |
|---|---|---|
| Does the consumer need sub-minute freshness? | No | Yes |
| Is the source system capable of CDC/event emission? | Either | Requires event-capable source |
| Is the data genuinely continuous (payments) or naturally batched (EOD file drops)? | Naturally batched | Continuous |
| Does the use case tolerate reprocessing delay for corrections? | Yes — reprocess the batch | Requires DLQ + targeted replay |
| Regulatory requirement for real-time monitoring (e.g., fraud, AML)? | No | Yes |
| Cost sensitivity (streaming infra runs continuously, batch runs on schedule) | Lower cost | Higher baseline cost |

**Principal Architect framing for the interview:** *"I don't choose streaming because it's more sophisticated — I choose it when the business genuinely needs sub-minute freshness, like fraud detection or real-time balance updates. For EOD reconciliation feeds or partner file drops, batch is not just adequate, it's the more cost-effective and operationally simpler correct answer. The mistake I see architects make is defaulting to streaming everywhere because it's the more impressive-sounding architecture."*

---

## Interview Talking Points — Quick Reference

1. **Name specific sources for CDC vs. watermark extraction** — don't say "we use CDC" as a blanket statement.
2. **Watermark commits only after verified success** — the single most common batch ingestion bug is advancing the watermark optimistically.
3. **Idempotency is layered** — MERGE-based natural keys at the row level, batch tokens at the run level, and a MERGE backstop even in streaming for duplicates outside the dedup window.
4. **Late data triggers targeted reprocessing, not full rebuilds** — and surfaces a business question (restated regulatory report?) that the architecture makes discoverable, not decides.
5. **Event time vs. processing time is a regulatory correctness issue in banking**, not just a technical nuance.
6. **Ordering keys have a real parallelism cost** — name the trade-off, don't pretend it's free.
7. **Reference data should be a side input/cache, not a per-record external call** — this is the real fix for streaming back pressure caused by dependency bottlenecks, not just "add more workers."
8. **Pub/Sub vs. Kafka is a migration-cost question, not a technical superiority question** — acknowledge existing Kafka estates honestly.

---

## Next Rounds (Planned)

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

*Document 2 of ongoing Lloyds Data Architect interview preparation series.*
