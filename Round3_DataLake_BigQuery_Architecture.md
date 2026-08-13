# Principal Data Architect Study Guide — Round 3
## Data Lake & BigQuery Architecture (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Cloud Storage Data Lake Architecture

### A.1 Zone Structure — Full Detail

```
gs://bank-data-lake/
├── landing/
│   └── {source_system}/{YYYY-MM-DD}/{filename}
├── raw/
│   └── {source_system}/{table_or_feed}/{YYYY}/{MM}/{DD}/{file}
├── quarantine/
│   └── {source_system}/{table_or_feed}/{YYYY}/{MM}/{DD}/{file}
├── validated/
│   └── {source_system}/{table_or_feed}/{YYYY}/{MM}/{DD}/{file}
├── curated/
│   └── {domain}/{table_name}/{partition_column}={value}/{file}
└── archive/
    └── {source_system}/{table_or_feed}/{YYYY}/{file}
```

**Why each zone exists — not just what it's called:**

- **Landing**: uncontrolled, source-format, short retention (days). Nothing downstream ever reads from here directly. Exists purely to decouple "a file arrived" from "a file is trustworthy."
- **Raw**: immutable, permanent, source-format preserved. This is your regulatory system of record — the thing you point to when a regulator asks "show me exactly what you received." Never transformed, never deleted within retention period, never overwritten.
- **Quarantine**: records or files that failed validation. Not an error state to be ignored — an operational queue that data quality analysts actively work from.
- **Validated**: passed structural/schema checks, still in source-aligned structure, not yet business-transformed.
- **Curated**: business-ready, transformed, standardised, partitioned for query performance — this is what Dataform/Dataflow write to and what BigQuery external tables or native loads consume.
- **Archive**: lifecycle-managed cold storage for data past active query need but still within regulatory retention (7-10 years). Cheaper storage class, rarely accessed, but must remain retrievable within a defined SLA if a historical audit demands it.

**Interview point:** A common weak answer is "we have raw and curated." A Principal Architect explains *why five distinct zones exist* — each one exists to answer a specific failure mode (corrupted transfer, bad data, need for full historical replay, cost control) rather than being folder-naming convention for its own sake.

---

### A.2 File Formats — Parquet vs. Avro vs. JSON

| Format | Best For | Why |
|---|---|---|
| **Parquet** | Curated layer, analytical queries | Columnar — BigQuery and Dataflow read only needed columns, dramatically reducing bytes scanned (and cost, since BigQuery pricing is bytes-scanned-based). Best compression ratio for repetitive banking data (many repeated currency codes, product types). |
| **Avro** | Raw/streaming layer, schema evolution-heavy sources | Row-based — better for write-heavy, full-record access patterns (exactly how streaming ingestion writes). Embeds schema with the data, making schema evolution (adding fields) safer and self-describing — valuable when raw must preserve exactly what was received, including schema version. |
| **JSON** | Landing zone only (source format), API payloads | Human-readable, universally compatible, but expensive to query directly at scale (no columnar pruning, larger file size). Should be converted to Parquet/Avro as soon as it moves past landing — never used as a long-term curated format. |

**Concrete decision for this architecture:** Landing keeps source format (often JSON/CSV/XML — whatever the source sends). Raw stores as **Avro** for streaming-originated data (schema evolution safety) and preserves original format for file-based sources (since raw's job is fidelity to source, not query optimisation). Curated is **always Parquet** — this is the layer BigQuery, Dataflow, and any analytical consumer actually queries at volume, so query performance and cost dominate the format decision.

---

### A.3 Compression

**Snappy** is the default choice for Parquet in this architecture — it prioritises fast decompression (important for query-heavy BigQuery external table access) over maximum compression ratio. **GZIP** offers better compression ratios but slower decompression — appropriate for the archive zone, where data is rarely read and storage cost matters more than read speed, but wrong for curated, where read performance during business-hours querying matters most.

---

### A.4 Partitioning Strategy (GCS Level)

Partition curated data by a column that matches the dominant query filter pattern — for banking, this is almost always **transaction date** or **business date**, not ingestion date. This distinction matters: a transaction that occurred on `2026-08-10` but was ingested late on `2026-08-13` must live in the `2026-08-10` partition for regulatory and reporting correctness, even though it physically landed three days later. Getting this wrong is a common, costly mistake — partitioning by ingestion date instead of business date breaks the entire late-arriving-data reprocessing model discussed in Round 2.

```
curated/payments/transaction_date=2026-08-10/part-0000.parquet
curated/payments/transaction_date=2026-08-10/part-0001.parquet
curated/payments/transaction_date=2026-08-11/part-0000.parquet
```

**Secondary partitioning consideration:** for very high-volume domains (payments at TB/day scale), a second-level partition by `source_system` or `region` can further reduce bytes scanned for queries that filter on both dimensions — but over-partitioning creates the small-file problem discussed next, so this is a genuine trade-off, not a free win.

---

### A.5 File Sizing & the Small File Problem

**The problem:** streaming pipelines and frequent micro-batches naturally produce many small files (a Dataflow streaming write every few minutes creates a new small Parquet file each time). Small files are expensive to query — BigQuery and Dataflow both pay per-file overhead (metadata reads, file-open costs) that dominates when files are, say, 5MB instead of an efficient 128-512MB.

**Design targets:** aim for Parquet files in the **128MB-512MB range** for curated data. Below ~64MB, per-file overhead starts to meaningfully hurt query performance at scale; above ~1GB, you lose parallelism benefits during query execution (fewer, larger files means fewer parallel read units).

**Mitigation — compaction:**
A scheduled compaction job (Dataflow or Dataproc, run daily or hourly depending on volume) reads many small files from a partition and rewrites them as fewer, appropriately-sized files. This runs as a distinct pipeline stage, separate from the ingestion pipeline itself — ingestion writes small files as a natural consequence of frequent micro-batches; compaction is a deliberate, scheduled cleanup pass.

**Interview-ready articulation:** *"Streaming and frequent batch writes will always produce small files as a natural side effect — trying to prevent this at write time by buffering longer just trades the small-file problem for a latency problem. The correct pattern is to accept small files at write time and run a scheduled compaction job that rewrites them into query-optimised sizes, decoupling ingestion latency from query performance."*

---

### A.6 Lifecycle Management

GCS Lifecycle Policies automate the retention and storage-class transitions without manual intervention:

```
Landing:    Delete after 7 days
Raw:        Standard storage → Nearline after 90 days → Coldline after 1 year
            → Archive storage class after 3 years → Delete after 7-10 years
            (per regulatory retention requirement)
Curated:    Standard storage (actively queried) → Nearline after 1 year
            for older partitions rarely queried
Archive:    Archive storage class from day one
```

**Why this matters for cost, not just compliance:** Standard storage is the most expensive tier; Archive storage class can be 80%+ cheaper. For a platform accumulating 5-8TB/day, letting old raw data silently sit in Standard storage for years is a significant, avoidable cost — lifecycle policies enforce the transition automatically rather than relying on someone remembering to do it manually.

---

### A.7 Encryption & Access Controls (GCS-Specific)

- **CMEK (Customer-Managed Encryption Keys)** applied at the bucket level via Cloud KMS — every zone, including landing, encrypted with bank-controlled keys, not Google-default keys, satisfying the audit/key-ownership expectation from Round 1.
- **IAM at the bucket and, where needed, object-prefix level** — the ingestion service account can write to landing/raw but not read curated regulatory-mart-bound data; the transformation service account can read raw/validated but not write directly to raw (raw is written only by ingestion, preserving its integrity as an immutable record).
- **Dataplex zone-level policies** (introduced in Round 1) apply governance rules — like "curated is the only zone external consumers can query via BigLake" — as policy rather than tribal knowledge.

---

## Part B: BigQuery Architecture

### B.1 Dataset & Table Organisation

```
Project: bank-analytics-prod
├── dataset: raw_replica          ← BigQuery-native copies of raw where needed for query
├── dataset: curated_payments     ← domain-aligned curated datasets
├── dataset: curated_lending
├── dataset: curated_customer
├── dataset: marts_regulatory     ← isolated, restricted access, PRA/FCA submission-ready
├── dataset: marts_finance        ← finance reporting marts
├── dataset: marts_risk           ← risk analytics marts
└── dataset: shared_reference     ← currency codes, product hierarchies, calendars
```

**Design principle: dataset boundaries are also access-control boundaries.** Regulatory marts live in a physically separate dataset with its own IAM bindings — not because the data is technically different, but because the *access policy* is different (regulatory reporting teams need isolation from general analyst access, per the separation-of-duties requirement from Round 1). This is a deliberate governance decision expressed through structure, not an afterthought.

---

### B.2 Partitioning

**Every large fact table is partitioned — almost always by ingestion or business date, using BigQuery's native partitioned tables (not just a column, but genuine partition pruning).**

```sql
CREATE TABLE curated_payments.fact_transactions (
  transaction_id STRING,
  transaction_date DATE,
  account_id STRING,
  amount NUMERIC,
  currency STRING,
  ...
)
PARTITION BY transaction_date
CLUSTER BY account_id, currency;
```

**Why partitioning matters beyond "best practice":** BigQuery pricing is bytes-scanned based (for on-demand pricing). A query filtering `WHERE transaction_date = '2026-08-10'` on a partitioned table scans only that day's data — potentially megabytes instead of terabytes. Un-partitioned tables force full-table scans for every query, which is both slow and directly, measurably expensive. For a table accumulating years of daily transaction data, this is the single highest-leverage performance and cost decision in the entire warehouse design.

**Partition expiration** can also be set per table where regulatory retention allows automatic partition deletion after a defined period — though for genuinely regulated data (7-10 year retention), this is usually disabled in favour of the GCS lifecycle-managed archive pattern instead, since BigQuery storage cost for old partitions is higher than GCS Archive storage class.

---

### B.3 Clustering

**Clustering sorts data within each partition by the specified columns, enabling BigQuery to skip blocks of data that don't match a query's filter — without the cardinality constraints of a second partition column.**

`CLUSTER BY account_id, currency` above means a query filtering on `account_id` (a very high-cardinality column, unsuitable for partitioning itself) still benefits from block pruning within each date partition.

**Design rule: partition by the coarse, low-cardinality time/date dimension; cluster by the higher-cardinality dimensions actually used in WHERE/JOIN clauses.** Getting this backwards — trying to partition by a high-cardinality column like `account_id` — creates an excessive number of tiny partitions and actively hurts performance rather than helping it.

---

### B.4 Materialized Views vs. Standard Views

- **Standard views**: a saved query, re-executed in full every time it's queried. Zero storage cost, but zero performance benefit — appropriate for lightweight, infrequently-run logic, or as a governed access layer (a view can restrict/mask columns without duplicating data).
- **Materialized views**: BigQuery pre-computes and incrementally maintains the result, automatically refreshing as underlying data changes, and automatically routes matching queries to the materialized result even when the query wasn't written against the view directly (a genuinely underappreciated feature).

**Where materialized views earn their cost in this architecture:** a regulatory mart aggregation (e.g., daily total exposure by counterparty) that's queried repeatedly throughout the day by multiple reporting consumers is a strong materialized view candidate — the aggregation cost is paid once during refresh, not on every consumer query. A one-off ad-hoc analyst query is not — the materialized view's storage and maintenance cost isn't justified for a query pattern that won't repeat.

---

### B.5 External Tables & BigLake

**External tables** let BigQuery query data sitting in GCS (Parquet/Avro) without loading it into BigQuery-managed storage — directly relevant to the curated GCS zone, which can be queried via BigQuery without a separate load/copy step.

**BigLake** extends this with fine-grained security (row-level and column-level access control) applied consistently whether the query comes through BigQuery, Dataproc/Spark, or another BigLake-compatible engine — solving a real problem where external tables historically had weaker security controls than native BigQuery tables.

**Design decision for this architecture:** curated data that's queried frequently and needs the full performance/concurrency profile of BigQuery (regulatory marts, high-concurrency BI dashboards) gets **loaded into native BigQuery storage**. Curated data that's queried less frequently, or that needs to remain queryable by both BigQuery and Dataproc/Spark jobs without duplication, stays in GCS and is accessed via **BigLake external tables** — avoiding the storage duplication and sync complexity of maintaining both a GCS copy and a BigQuery-native copy of the same data.

---

### B.6 Snapshots & Incremental Processing

**Table snapshots** (a BigQuery-native feature, not to be confused with the "daily snapshot table" pattern used in fact-table design) provide a low-cost, point-in-time copy of a table without duplicating storage for unchanged data — genuinely useful for satisfying "reconstruct the report exactly as it was on date X" regulatory requests without maintaining full historical copies manually.

**Incremental processing pattern:** rather than reprocessing entire curated tables daily, Dataform/Dataflow jobs process only the current day's partition (or the specific late-arriving partitions flagged from Round 2's late-data handling) and MERGE the result into the target table — this is both a performance necessity at scale and a direct continuation of the idempotent, replay-safe design principle established in ingestion.

---

### B.7 Query Concurrency & Workload Management

**The problem this section solves:** 200+ concurrent analysts during business hours, bursting to 500 during month-end close, alongside scheduled regulatory report generation that cannot be starved of resources by ad-hoc analyst queries.

**Reservations and slot management** address this directly — rather than relying on BigQuery's shared on-demand slot pool (which is fine at low concurrency but becomes unpredictable under genuine contention), dedicated **reservations** are assigned per workload type:

```
Reservation: regulatory-reporting-reservation
  - Guaranteed slots for scheduled regulatory queries
  - Highest priority, protected from analyst query contention

Reservation: bi-analyst-reservation
  - Shared pool for ad-hoc analyst/BI tool queries
  - Autoscaling enabled to absorb month-end burst

Reservation: etl-transformation-reservation
  - Dataform/scheduled transformation jobs
  - Isolated so a runaway analyst query never delays the nightly batch pipeline
```

**Why this matters as a design decision, not an operational afterthought:** without workload isolation, a single poorly-written analyst query (an accidental full-table scan, a Cartesian join) can consume enough on-demand slots to delay a regulatory submission — which is a genuine business/compliance risk, not just a performance inconvenience. Reservations turn "don't let analysts starve regulatory reporting" from a hope into an enforced guarantee.

---

### B.8 Data Sharing

**BigQuery's Analytics Hub** (or simpler authorized views/datasets) allows controlled sharing of curated datasets across teams or business units without physically copying data — relevant for a large bank where risk, finance, and operational teams may all need access to overlapping but governance-distinct views of the same underlying curated data.

**Design principle:** share via **authorized views** with column-level masking applied, not via granting direct dataset access — this keeps the single source of truth intact (one curated table) while giving each consuming team a governed, appropriately-restricted lens onto it, consistent with the column-level security model established via Dataplex policy tags in Round 1.

---

### B.9 How the Architecture Changes for Very Large Tables

At genuine scale (a fact table accumulating years of daily transaction data at TB/day ingest rates, reaching hundreds of TB to PB), several design choices shift:

- **Partition pruning becomes non-negotiable, not just best practice** — an un-partitioned or poorly-partitioned query against a table this size isn't just slow, it can be prohibitively expensive under on-demand pricing, making reservations with flat-rate/edition pricing more cost-predictable than on-demand at this scale.
- **Clustering becomes essential, not optional** — the cardinality and query-pattern reasoning from B.3 has real financial consequences at this size; a poorly-clustered table means every query scans far more data blocks than necessary.
- **Materialized views shift from "nice performance win" to "operational necessity"** for any aggregation queried repeatedly — recomputing an aggregate over hundreds of TB on every query is not viable at business-hour concurrency.
- **Table partitioning strategy may need a second dimension** — a single date-partitioned table growing indefinitely benefits from periodic archival of old partitions to a separate, cheaper-tier table or GCS/BigLake external table, keeping the "hot" native BigQuery table bounded to the actively-queried recent window (e.g., rolling 2 years native, older data via BigLake external table against GCS archive).
- **Streaming insert costs and quotas become a genuine capacity planning input**, not an afterthought — at TB/day streaming volume, BigQuery streaming insert quotas and costs need explicit modelling, and batching micro-inserts (rather than per-record streaming inserts) becomes a meaningful cost lever.

**Interview-ready framing:** *"The architecture doesn't fundamentally change shape at large scale — partitioning, clustering, and materialized views are the same tools — but decisions that were 'best practice' at moderate scale become 'load-bearing and non-negotiable' at large scale, and cost-model assumptions that held at GB/day volumes need to be explicitly re-validated at TB/day volumes, particularly around on-demand vs. flat-rate pricing and streaming insert costs."*

---

## Part C: Design Decisions Summary Table

| Decision | Choice | Primary Driver |
|---|---|---|
| Zone count in GCS lake | 5 zones (landing/raw/quarantine/validated/curated) + archive | Each zone answers a distinct failure mode, not naming convention |
| Curated file format | Parquet | Columnar pruning directly reduces BigQuery bytes-scanned cost |
| Raw file format (streaming-origin) | Avro | Schema evolution safety, row-based fits write pattern |
| Partition key (GCS + BigQuery) | Business/transaction date, not ingestion date | Regulatory correctness for late-arriving data |
| Small file handling | Accept at write, compact on schedule | Decouples ingestion latency from query performance |
| Regulatory marts | Separate BigQuery dataset | Access-control boundary matches governance requirement, not just data domain |
| Frequently-queried aggregates | Materialized views | Aggregation cost paid once on refresh, not per consumer query |
| Curated data serving pattern | Native BigQuery for high-concurrency; BigLake external for cross-engine/lower-frequency | Avoids storage duplication while meeting performance needs where it matters |
| Concurrency management | Dedicated reservations per workload type | Protects regulatory SLA from analyst query contention |
| Large table historical data | Rolling native window + BigLake external archive | Keeps hot table bounded; controls storage cost at scale |

---

## Interview Talking Points — Quick Reference

1. **Partition by business date, not ingestion date** — this single decision determines whether late-arriving data handling actually works correctly.
2. **Small files are an accepted consequence of streaming, not a bug to prevent at write time** — solve with scheduled compaction, not by adding write-time latency.
3. **Reservations exist to protect regulatory SLAs from analyst query contention** — frame this as a compliance/business-risk control, not just a performance feature.
4. **BigLake vs. native BigQuery storage is a deliberate trade-off** — query frequency/concurrency needs vs. storage duplication cost, not a default choice.
5. **At large scale, "best practice becomes load-bearing"** — partitioning and clustering aren't optional refinements once you're past GB/day into TB/day.
6. **Dataset boundaries double as access-control boundaries** — regulatory marts are physically separated because the governance requirement, not the data itself, demands isolation.

---

## Next Rounds (Planned)

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

*Document 3 of ongoing Lloyds Data Architect interview preparation series.*
