# Principal Data Architect Study Guide — Round 5
## Performance Optimisation (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: The Diagnostic Mindset Before Any Tuning

A weak answer jumps straight to "increase workers" or "add clustering." A Principal Architect diagnoses first, tunes second — because the wrong fix applied to the wrong bottleneck wastes time and often makes cost worse without improving performance.

**The universal diagnostic sequence, applicable to Dataflow, Dataproc, and BigQuery alike:**

1. **Where is time actually being spent?** (which stage, which query, which stage of query execution)
2. **Is the bottleneck compute, I/O, network, or skew?** (these require different fixes)
3. **Is this a one-off anomaly or a systemic pattern?** (a single slow run vs. a query that's been degrading over weeks)
4. **What changed?** (data volume growth, a schema change, a new consumer, a code change) — most performance regressions have a specific triggering change, not a random appearance

---

## Part B: Dataflow / Apache Beam Pipeline Optimisation

### B.1 Diagnosing a Slow Dataflow Pipeline — Step by Step

**Step 1: Open the Dataflow job graph and identify the slowest stage.**
The Dataflow UI shows per-stage wall-clock time and throughput. A pipeline isn't "slow" as a whole — one specific stage almost always dominates. Don't tune the pipeline; tune the stage.

**Step 2: Check for stragglers (skewed processing time across workers).**
If most workers finish a stage quickly but a handful take dramatically longer, this is **data skew** — a small number of keys (e.g., a single high-volume account_id or a default/null key catching disproportionate records) are overloading specific workers while others sit idle. This shows up in the UI as a long tail on an otherwise fast stage.

**Step 3: Check the autoscaling graph against the Pub/Sub backlog (streaming) or input size (batch).**
If workers are consistently maxed at CPU/memory while backlog grows, you're compute-bound — scale up worker machine type or increase max workers. If workers are scaling but throughput isn't improving proportionally, you likely have a bottleneck *outside* Dataflow's control — a downstream sink (BigQuery streaming insert quota, an external API call for enrichment) that autoscaling more Dataflow workers won't fix, and may make worse (more concurrent callers hitting an already-saturated downstream dependency).

**Step 4: Check for excessive shuffling.**
Any `GroupByKey`, `Combine`, or windowing operation triggers a shuffle — data movement across workers. Beam's shuffle service (or Dataflow Shuffle for batch, Streaming Engine for streaming) handles this, but poorly-designed pipelines with unnecessary re-groupings multiply shuffle cost. Look for redundant `GroupByKey` operations that could be combined into a single aggregation step.

**Step 5: Check serialization cost.**
Custom coders or overly complex Java/Python objects passed between pipeline stages add serialization/deserialization overhead at every shuffle boundary. Using Beam's built-in, efficient coders (Avro-based schemas where possible) instead of generic/custom object serialization is a real, measurable win at scale.

---

### B.2 Concrete Example — Poor Design vs. Improved Design

**Poor design:** A streaming pipeline enriches every transaction record with reference data (currency conversion rates, counterparty details) via a **per-record synchronous API call** to an external reference data service.

```
Problem: Each record blocks on a network call. Throughput is capped by
the reference service's response time × available concurrent connections,
not by Dataflow's compute capacity. Autoscaling adds more workers, which
just creates more concurrent callers hammering the same overloaded service.
```

**Improved design:** Reference data (currency rates, counterparty details) is loaded as a **side input** — a bounded, periodically-refreshed dataset cached in each worker's memory, joined against the main stream in-process with no network call per record.

```
Result: Enrichment becomes a local, in-memory lookup. Throughput now
scales with Dataflow's own compute capacity, not an external dependency.
Side input refresh (e.g., every 15 minutes from a BigQuery table) is a
separate, controlled, low-frequency operation.
```

**Why this example matters in an interview:** this is the exact same architectural fix described in Round 2's back-pressure discussion, now applied specifically as a performance tuning example — showing the same principle recurring across different framings (resilience, performance, cost) is what signals genuine understanding rather than memorised advice.

---

### B.3 Worker Sizing, Parallelism, and Autoscaling

- **Worker machine type**: default `n1-standard` machines are a reasonable starting point, but memory-intensive transformations (large side inputs, complex windowed aggregations holding significant state) benefit from `highmem` variants — undersized worker memory causes spilling to disk, which silently degrades throughput without an obvious error.
- **Max workers**: setting this too low caps throughput artificially during genuine load spikes (month-end processing, payment volume bursts); setting it unnecessarily high risks cost surprises and can overwhelm downstream dependencies (the reference-service problem above, or BigQuery streaming insert quotas) even if Dataflow itself could scale further.
- **Number of shards / key distribution**: for batch pipelines reading from GCS, ensure the input is split into enough files/shards to actually use available parallelism — a single enormous file (or, at the other extreme, thousands of tiny files, tying back to Round 3's small-file problem) both limit effective parallelism, just in opposite ways.

---

### B.4 Memory and CPU Utilisation

Persistent high memory utilisation with degrading throughput over the life of a streaming job usually indicates **unbounded state growth** — a windowing or aggregation pattern that never triggers/cleans up old state (e.g., a global window with no trigger, accumulating forever). The fix is architectural: use appropriately-bounded windows (fixed, sliding, or session windows with explicit triggers) rather than relying on a global window and hoping memory holds out.

---

## Part C: Spark on Dataproc — Performance Tuning

### C.1 Reading the Spark UI to Diagnose Performance Problems

**This is a specific, testable interview skill — be ready to walk through it concretely, not just name the UI exists.**

**Step 1: Open the Stages tab, sort by duration.**
Identify the longest-running stage. As with Dataflow, don't tune the whole job — tune the specific stage consuming the most time.

**Step 2: Within that stage, check task duration distribution.**
The Spark UI shows min/median/max task duration for a stage. A large gap between median and max task duration is the direct signature of **data skew** — most partitions process quickly, a few partitions (holding disproportionately many records for a given key) take far longer, and the stage's total wall-clock time is dictated by the slowest task, not the average.

**Step 3: Check the Shuffle Read/Write columns.**
Large shuffle volumes relative to input size indicate an expensive `groupBy`, `join`, or `repartition` operation. If shuffle write for a stage is dramatically larger than the stage's input size, look for an avoidable wide transformation — often a join that could be a broadcast join instead (see C.3).

**Step 4: Check the Executors tab for spill (memory) and GC time.**
High **shuffle spill** (spill to disk shown per executor) means executor memory is insufficient for the shuffle partition size being processed — data that should stay in memory is being written to disk, which is dramatically slower. High **garbage collection time** relative to task time indicates memory pressure, often from too few, too-large partitions being processed per executor, or from an inefficient serialization format increasing object overhead.

**Step 5: Check the SQL tab (for DataFrame/SQL jobs) for the physical query plan.**
Confirms whether predicate pushdown and partition pruning actually occurred as expected — a join or filter that *should* have pruned partitions but didn't (visible as a full scan in the plan) points to a partitioning mismatch between the query filter and the actual table partition scheme (directly connects to Round 3's partitioning design).

---

### C.2 Partition Counts and Shuffle Partitions

**The most common, highest-leverage Spark tuning mistake:** leaving `spark.sql.shuffle.partitions` at its default (200) regardless of actual data volume. For a 1.5TB+ daily banking dataset, 200 shuffle partitions often produces partitions far larger than optimal (target roughly 100-200MB per partition post-shuffle), causing spill and slow shuffle stages. For a much smaller job, 200 partitions can be excessive overhead-wise (task scheduling cost dominating actual work per tiny partition).

**Design rule:** shuffle partition count should be set based on **actual data volume ÷ target partition size**, not left at the default — and re-evaluated as data volume grows over the platform's life, not set once and forgotten. This is a specific, concrete point that signals real hands-on Spark experience rather than textbook knowledge.

---

### C.3 Broadcast Joins

When joining a large fact table (transactions) against a small dimension table (currency codes, product reference data — thousands of rows, not millions), a standard shuffle join unnecessarily shuffles the *large* table across the cluster to co-locate matching keys. A **broadcast join** instead sends the small table to every executor's memory, avoiding the shuffle entirely for the large side.

Spark's Catalyst optimizer applies broadcast joins automatically below a size threshold (`spark.sql.autoBroadcastJoinThreshold`, default 10MB) — but banking reference tables can legitimately exceed this default threshold while still being genuinely small relative to the fact table. **Explicitly hinting** the broadcast join (or raising the threshold) for known-small dimension tables is a concrete, high-value tuning action, not a theoretical one.

---

### C.4 Adaptive Query Execution (AQE)

AQE (enabled by default in modern Spark/Dataproc images) dynamically re-optimises the query plan at runtime based on actual data statistics observed during execution — specifically relevant to two problems already discussed:

- **Dynamically coalescing shuffle partitions** post-shuffle, addressing the "default 200 partitions is wrong for this data volume" problem automatically in many cases.
- **Dynamically switching join strategies** and **splitting skewed partitions** (`spark.sql.adaptive.skewJoin.enabled`) — directly mitigating the data skew problem identified in C.1, by detecting a disproportionately large partition at runtime and splitting it into smaller sub-tasks rather than letting one task become the bottleneck for the entire stage.

**Interview-ready point:** *"AQE handles a meaningful portion of skew and partition-sizing problems automatically now, but I still design partitioning and joins deliberately rather than relying on AQE alone — it's a safety net for cases I didn't anticipate, not a substitute for getting the join strategy and partition key right in the first place."*

---

### C.5 Caching

`.cache()` / `.persist()` is valuable when a DataFrame is reused across multiple subsequent actions (e.g., a cleaned/deduplicated dataset used to build both a fact table and a data quality summary in the same job) — recomputing the same upstream transformation chain twice is wasted work. It is actively harmful when applied to a DataFrame used only once, or when cached data doesn't fit in available executor memory (causing eviction and effectively wasted caching overhead with no benefit). Cache deliberately, based on actual reuse in the DAG, not by default.

---

### C.6 Small Files (Spark-Specific Angle)

Beyond the GCS/query-performance angle covered in Round 3, small files specifically hurt Spark by creating far more partitions/tasks than necessary — task scheduling overhead dominates actual processing time when each task handles a trivially small amount of data. Reading many small files also multiplies GCS API call overhead. The same compaction pattern from Round 3 applies; from the Spark side, `coalesce()` or `repartition()` before writing output is the direct in-job mitigation, complementing the scheduled compaction job.

---

## Part D: BigQuery Query Performance

### D.1 Diagnosing a Query That Used to Take 30 Seconds, Now Takes 10 Minutes

**This is a named, specific interview scenario from the study template — walk through it methodically, out loud, in an interview:**

**Step 1: Check the execution plan (Query execution details in the BigQuery UI / `EXPLAIN`).**
Compare stages and bytes processed against what you'd expect. The first question: **has bytes scanned increased dramatically?** If yes, this is very likely a **data volume growth** issue, not a query logic issue — the query itself didn't change, but the table it queries has grown substantially (directly relevant to a platform projected to grow 10-15x over 3 years, per Round 1's NFRs).

**Step 2: Check whether partition pruning is still occurring.**
If the query filters on the partition column but bytes scanned equals (or approaches) the full table size, partition pruning has broken — common causes: a filter using a function on the partition column (`WHERE DATE(transaction_timestamp) = ...` instead of filtering the partition column directly, which can defeat pruning depending on how the predicate is expressed), or a join that implicitly removes the partition filter's effectiveness.

**Step 3: Check for a schema or data distribution change.**
Has clustering effectiveness degraded? Clustering benefit decreases over time as new data is appended without periodic re-clustering — for very actively-written tables, this can silently erode the clustering benefit that made the query fast six months ago. BigQuery auto-reclusters in the background, but under continuous heavy write load, clustering quality can still lag.

**Step 4: Check for slot contention.**
If using flat-rate/reservation pricing, check whether this query is now sharing its reservation with significantly more concurrent workload than before — a query that used to get generous slot allocation might now be queuing/sharing slots with a newly onboarded consumer team, degrading wall-clock time without the query itself doing more logical work. This is a workload management problem (Round 3, B.7), not a query-tuning problem — the fix is reservation sizing/isolation, not query rewriting.

**Step 5: Check if the query joined against a table that changed shape.**
A join that used to hit a small, well-clustered dimension table might now be joining against a dimension table that's grown substantially, or lost its clustering — the *query* is identical, but one of its inputs changed character.

**Step 6: Check for a broadcast/shuffle join issue at the BigQuery engine level.**
Large join reordering or a change in join strategy chosen by BigQuery's optimizer (influenced by updated table statistics) can shift a previously efficient join into a more expensive one as underlying table sizes and cardinalities change over time.

**Interview-ready synthesis:** *"My first move is always bytes-scanned comparison — that single number tells me whether I'm looking at a data-growth problem (expected, needs partitioning/clustering revisited) or a genuine regression (something broke — pruning, clustering, or contention). I don't start rewriting the query until I know which of those it is, because the fix is completely different depending on the answer."*

---

### D.2 Common BigQuery Anti-Patterns and Fixes

| Anti-Pattern | Why It's Costly | Fix |
|---|---|---|
| `SELECT *` | Scans every column regardless of need — defeats columnar storage's core advantage | Select only required columns explicitly |
| Repeated CTE re-computation | A CTE referenced multiple times in a query may be recomputed each time rather than materialized once, depending on query complexity | Materialize into a temp table for genuinely expensive, multiply-referenced CTEs |
| Filtering after joining large tables | Scans and joins full volume before reducing it | Filter as early as possible, ideally on partition/cluster columns, before joining |
| Cross join / accidental Cartesian product | Explodes row count catastrophically | Explicit join keys always; validate join cardinality before deploying to production |
| Exact aggregation on massive distinct counts | `COUNT(DISTINCT ...)` on a huge cardinality column is expensive | Use `APPROX_COUNT_DISTINCT()` where exact precision isn't operationally required (rarely acceptable for regulatory figures, often fine for operational dashboards) |
| No partition filter on a partitioned table | Forces full table scan despite partitioning being available | Enforce partition filter requirement at the table level (`require_partition_filter = true`) to prevent this by policy, not just convention |

---

### D.3 Materialized Views and Denormalisation as Performance Levers

Already covered structurally in Round 3 — the performance angle specifically: a materialized view converts a repeated, expensive aggregation query into a cheap read against a pre-computed, incrementally-maintained result. For the "query took 30 seconds, now takes 10 minutes" scenario, if root cause analysis reveals the query is a frequently-repeated aggregation over a growing table, converting it to a materialized view is often the correct long-term fix — not just tuning the existing query pattern.

---

## Part E: Cross-Cutting Performance Principles

1. **Diagnose before tuning** — identify the specific bottleneck (compute, I/O, skew, contention, external dependency) before applying a fix; the wrong fix wastes effort and can worsen cost.
2. **Skew is the most common root cause across all three engines** (Dataflow stragglers, Spark task duration spread, BigQuery join reordering issues) — always check for it specifically, don't assume uniform data distribution.
3. **External dependencies (APIs, reference lookups) don't scale by adding more pipeline workers** — this is an architectural fix (side inputs / caching), not a sizing fix, and applies identically whether framed as a performance, resilience, or cost problem.
4. **Defaults are starting points, not final answers** — shuffle partition counts, broadcast join thresholds, autoscaling limits all have defaults tuned for generic workloads, not for a 1.5TB+/day banking platform specifically.
5. **Performance regressions almost always have a specific trigger** — data volume growth, a schema change, a new concurrent consumer, degraded clustering over time. "It just got slower" is never the real answer; find the trigger.

---

## Interview Talking Points — Quick Reference

1. **Always name the specific diagnostic step, not just the tool** — "I'd check the Spark UI" is weak; "I'd check task duration spread in the Stages tab to identify skew" is strong.
2. **Bytes-scanned comparison is the first move for any BigQuery slowdown** — it immediately separates "data grew" from "something broke."
3. **Side inputs / caching is the recurring fix for external-dependency bottlenecks** — reuse this example across performance, resilience, and cost discussions to show it's a genuinely internalised principle, not a memorised fact.
4. **AQE is a safety net, not a substitute for deliberate design** — don't over-credit automatic optimisation features as a replacement for getting partitioning/joins right upfront.
5. **`require_partition_filter = true` is a concrete, policy-level fix** worth naming — it prevents an entire class of expensive-query incidents by design rather than relying on every analyst remembering to filter correctly.

---

## Next Rounds (Planned)

- **Round 6**: Data Quality, Governance & Lineage Deep Dive
- **Round 7**: Security Architecture Deep Dive
- **Round 8**: Orchestration, Error Handling & Resilience
- **Round 9**: Observability & Cost Optimisation
- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 5 of ongoing Lloyds Data Architect interview preparation series.*
