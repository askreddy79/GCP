# Principal Data Architect Study Guide — Round 9
## Observability & Cost Optimisation (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Observability Design

### A.1 Why Technical Metrics Alone Are Insufficient

The common weak pattern: a monitoring setup that tracks CPU, memory, job success/failure — purely infrastructure health. This tells you the pipeline *ran*, not that the *business outcome* it exists to produce is trustworthy. A Principal Architect designs observability around **two distinct but connected layers** — technical health (is the infrastructure working) and business health (is the data correct, complete, and on time) — because a pipeline can be technically green (every job succeeded) while being business-red (data is late, incomplete, or has failed reconciliation), and vice versa in rarer cases. Both layers need independent visibility, not one inferred from the other.

---

### A.2 Technical Metrics

| Metric | What It Answers | GCP Source |
|---|---|---|
| Job success/failure rate | Are pipeline tasks completing? | Cloud Composer task state, Cloud Logging |
| Dataflow throughput / backlog | Is streaming keeping pace with input? | Dataflow job metrics (Cloud Monitoring) |
| Worker CPU/memory utilisation | Is compute sized correctly? | Cloud Monitoring (Dataflow, Dataproc) |
| BigQuery slot utilisation | Is reserved capacity being used efficiently, or contended? | BigQuery `INFORMATION_SCHEMA.JOBS`, Cloud Monitoring |
| Query latency (p50/p95/p99) | Are consumers experiencing acceptable performance? | BigQuery job statistics |
| Error rates by pipeline stage | Where are failures concentrated? | Cloud Logging, structured DQ failure table (Round 6) |
| Storage growth rate | Is the platform tracking toward projected volume growth (Round 1's 10-15x)? | GCS/BigQuery storage metrics |

---

### A.3 Business Metrics

| Metric | What It Answers | Why This Matters More Than It Sounds |
|---|---|---|
| **Data freshness** | Is curated data available by the 06:00 UTC SLA (Round 1)? | This is the metric regulatory reporting teams actually care about — not "did the job run," but "is my data here on time" |
| **Row count vs. expected baseline** | Did this batch load a plausible volume, or a suspicious anomaly? | A batch loading 3% of the expected row count is technically a "success" (no error thrown) but a business-critical anomaly — this is precisely the gap technical-only monitoring misses |
| **DQ pass/fail rate by rule** | Is data quality stable or degrading over time? | Trend analysis surfaces slow degradation (e.g., an upstream source gradually sending more nulls) that a single day's snapshot wouldn't show |
| **Reconciliation variance** | Does curated match source, within tolerance? | Directly answers the "did we lose or duplicate anything" question from Round 2/6 as a continuously monitored metric, not just a one-off gate |
| **Flag table status history** | How often, and for how long, has consumer access been blocked (RED)? | A rising trend of RED periods is an early warning of systemic pipeline health decline, even if each individual incident was resolved |

**The row-count anomaly point deserves emphasis in an interview:** a pipeline job "succeeding" and a pipeline job "producing correct output" are different claims, and a monitoring design that only tracks the former has a real, exploitable blind spot — a silently truncated source file, a broken join that drops 90% of rows, or a filter condition bug would all show as a clean, green, successful job run with no technical error, while being a genuine business incident. Row-count-against-baseline monitoring is what catches this class of failure that technical monitoring structurally cannot.

---

### A.4 Pipeline SLA Monitoring

Extending Round 8's SLA discussion: SLA tracking isn't binary (met/missed) — it's tracked as a **time-series of "time remaining until SLA breach"** for in-flight pipeline runs, so that an on-call engineer sees "curated load is running 40 minutes behind schedule, SLA breach in 90 minutes" as an actionable early warning, not just a post-breach alert after the fact. This distinction — leading indicator vs. lagging indicator — is a genuinely important observability design principle worth naming explicitly.

---

### A.5 Example Operational Dashboard

A single-pane dashboard (built on Cloud Monitoring + a BigQuery-backed custom dashboard, since some business metrics like reconciliation variance don't map to native Cloud Monitoring metrics directly) structured in three tiers:

```
┌───────────────────────────────────────────────────────────────┐
│  TIER 1 — EXECUTIVE / AT-A-GLANCE                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐│
│  │ Flag Status  │ │ SLA Status  │ │ DQ Pass Rate│ │ Cost Today││
│  │   GREEN      │ │ On Track    │ │   99.94%    │ │  £X,XXX   ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘│
├───────────────────────────────────────────────────────────────┤
│  TIER 2 — PIPELINE HEALTH                                       │
│  Batch Ingestion:  ████████████████░░  87% complete, on pace   │
│  Streaming Lag:    12s (target < 2s SLA — WARNING)              │
│  DQ Gate 1/2/3:    PASS / PASS / IN PROGRESS                    │
│  Reconciliation:   Row variance 0.002% (within tolerance)       │
├───────────────────────────────────────────────────────────────┤
│  TIER 3 — DETAILED / DRILL-DOWN (per source system, per table)  │
│  Source System │ Rows Loaded │ Baseline │ Variance │ DQ Failures│
│  FX_TRADING     │  1,204,382  │ 1.2M avg │  +0.3%   │     3      │
│  CORE_BANKING   │  8,991,201  │ 9.0M avg │  -0.1%   │     0      │
│  MAINFRAME_LEDGER│   402,119  │ 410K avg │  -1.9%   │    12 ⚠   │
└───────────────────────────────────────────────────────────────┘
```

**Why the three-tier structure matters, not just the metrics themselves:** an executive/incident-commander glancing at Tier 1 needs a 5-second answer to "is everything OK." An on-call engineer investigating a specific WARNING needs Tier 2's pipeline-stage granularity. A data steward chasing a specific quarantined record needs Tier 3's per-source detail. Designing one dashboard that tries to serve all three audiences equally usually serves none of them well — tiered structure with drill-down is a deliberate UX decision for observability, not an afterthought.

---

### A.6 Alerting Philosophy — Signal vs. Noise

**A specific, concrete design principle worth stating in an interview:** every alert must be actionable by whoever receives it — an alert that fires but requires no action, or that the on-call engineer can't actually do anything about at 2am, trains people to ignore alerts generally, which is more dangerous than having no alert at all (alert fatigue silently degrading response to genuinely critical alerts). This is why Round 6's BLOCKING/WARNING DQ severity split and Round 8's three-tier escalation severity model both exist — they're not bureaucratic categorisation, they're the mechanism that keeps alerts meaningful.

---

## Part B: Cost Optimisation

### B.1 The Core Principle: Architecture Decisions Drive Cost, Not Just Usage Discipline

A weak cost-optimisation answer talks about turning off unused resources. A Principal Architect recognises that **most meaningful cost outcomes are determined by architecture decisions made much earlier** — partitioning strategy, file format, storage tier, reservation vs. on-demand — not by after-the-fact cost policing. This section connects directly back to decisions already made in Rounds 3 and 5, now viewed explicitly through the cost lens.

---

### B.2 BigQuery Cost Control

- **Partitioning and clustering (Round 3/5)** are the single highest-leverage cost control in the entire platform for on-demand pricing — bytes-scanned reduction from correct partition pruning directly reduces query cost, often by orders of magnitude for a well-filtered query against a poorly-partitioned table.
- **`require_partition_filter = true`** (Round 5) prevents an entire class of accidental full-table-scan cost incidents by policy, not by hoping every analyst remembers to filter.
- **Reservations vs. on-demand pricing**: for a platform with predictable, high-volume, continuous query load (regulatory reporting, scheduled transformation jobs), **flat-rate/edition-based reservations** provide cost predictability that on-demand per-byte pricing doesn't — on-demand is appropriate for genuinely variable, ad-hoc analyst workloads where a reservation would be idle capacity much of the time. **Mixing both within the same platform**, matched to each workload's actual usage pattern (Round 3's per-workload reservation design), is the correct answer — not choosing one pricing model platform-wide.
- **Materialized views** (Round 3/5) trade a small, predictable maintenance cost for eliminating repeated full-cost recomputation of the same aggregation — a direct cost lever, not just a performance one.

---

### B.3 Dataflow Cost Control

- **Autoscaling bounds (max workers)** must be set deliberately, not left uncapped — an uncapped streaming pipeline experiencing an unexpected input spike can scale far beyond what's actually cost-justified, especially if the real bottleneck is a downstream dependency (Round 5's reference-service example) where more Dataflow workers wouldn't even help throughput, just cost.
- **Dataflow Prime / right-sized worker machine types**: selecting worker machine types matched to actual workload characteristics (compute-bound vs. memory-bound, per Round 5) avoids paying for oversized machine types that sit under-utilised.
- **Batch vs. streaming cost trade-off, revisited from a cost lens:** a streaming pipeline runs continuously, incurring cost even during low-volume overnight periods; a batch pipeline only incurs cost while actively running. Round 2 established this decision should be driven by genuine freshness requirements — the cost angle reinforces why defaulting to streaming "because it's more sophisticated" (Round 2's explicit warning) is a real, quantifiable cost mistake when a business need doesn't actually require sub-minute freshness.

---

### B.4 Cloud Storage Cost Control

- **Lifecycle management (Round 3)** — automated transition from Standard to Nearline/Coldline/Archive storage classes as data ages out of active query use — is the primary GCS cost lever at the multi-year retention scale this platform requires. Manually managed storage-tier transitions (relying on someone remembering to move old data) reliably fail to happen consistently at scale; automated lifecycle policies are a cost control, not just a convenience.
- **Small file compaction (Round 3/5)** has a cost dimension beyond query performance: many small files increase the number of storage operations (Class A/B operations are billed separately from storage volume in GCS), so uncompacted small files are both a performance problem and a direct, measurable cost line item.

---

### B.5 Pub/Sub and Network Transfer Cost Control

- **Pub/Sub pricing** is throughput-based (data volume published/consumed) — for high-volume payment event streams, message payload size matters directly; sending a full enriched record through Pub/Sub when only a lean event notification is needed (with enrichment happening downstream via side input, per Round 5) is both an architecture-quality decision and a cost decision simultaneously.
- **Network egress**, specifically cross-region data movement (e.g., the Production/DR MSK-equivalent pattern from the ingestion design, or BigQuery datasets queried cross-region), incurs real, sometimes underestimated cost — a Principal Architect explicitly considers **regional co-location** of compute and storage wherever DR requirements don't specifically demand cross-region movement, rather than defaulting to convenient-but-costly cross-region access patterns.

---

### B.6 Where a Technically Correct Architecture Becomes Unnecessarily Expensive — Concrete Examples

This is a specifically named section in the study template and a strong interview topic — have 2-3 of these ready, articulated precisely.

**Example 1: Streaming everything by default.**
A technically correct streaming pipeline (Dataflow, properly designed, exactly-once, well-monitored) processing a source that only genuinely needs hourly freshness runs 24/7, incurring continuous compute cost, when a batch job running for 20 minutes once an hour would meet the actual business requirement at a fraction of the cost. Nothing about the streaming design is *wrong* — it's simply solving a freshness requirement that doesn't exist, and doing so expensively. This is precisely the "default to streaming because it sounds more sophisticated" trap named explicitly in Round 2, now quantified in cost terms.

**Example 2: Uniform Standard storage class regardless of access pattern.**
A technically correct, well-partitioned, well-compacted GCS data lake that never transitions old partitions out of Standard storage class is architecturally sound in every other respect but silently accumulates significant, avoidable cost as historical data (rarely queried, per Round 3's retention discussion) sits in the most expensive storage tier for years. The fix isn't a redesign — it's a lifecycle policy that should have been present from day one.

**Example 3: Reservation sized for peak, used at average.**
A BigQuery reservation sized generously enough to handle month-end close's 500-concurrent-user burst (Round 1's NFR), but provisioned as a **flat, always-on reservation** rather than an **autoscaling reservation** that scales down during the other 25 days of the month when concurrency is much lower — technically guarantees the SLA is always met, but pays for peak capacity 100% of the time when it's only needed roughly 15-20% of the time. The architecturally correct fix is an autoscaling reservation baseline with burst capacity, not a fixed peak-sized allocation.

**Example 4: Over-normalised broadcast-join-eligible reference data left un-hinted.**
A technically correct Spark/Dataproc join against a legitimately small reference table, but without an explicit broadcast hint (Round 5) and above Spark's default auto-broadcast threshold, executes as a full shuffle join — technically correct results, but consumes far more compute (and therefore cost, on billed-by-usage Dataproc clusters) than necessary for what should have been a cheap broadcast join.

**Interview-ready synthesis:** *"None of these examples involve a bug or an incorrect result — that's exactly what makes them dangerous. A technically correct architecture that's quietly expensive doesn't throw an error or fail a test; it just costs more than it needs to, indefinitely, until someone specifically goes looking for it. This is why I treat cost review as a deliberate, recurring architecture activity — not just an incident response to a surprising bill."*

---

## Part C: Design Decisions Summary Table

| Decision | Choice | Why |
|---|---|---|
| Metrics layers | Technical + business metrics, tracked independently | A pipeline can be technically green while business-red (silent data loss, lateness) |
| Row-count baseline monitoring | Explicit anomaly detection vs. historical average | Catches silent failures (dropped rows, broken filters) that produce no technical error |
| SLA tracking | Leading indicator ("time to breach") not just lagging (breach/no-breach) | Enables intervention before a regulatory deadline is actually missed |
| Dashboard structure | Three-tier (executive / pipeline health / drill-down) | Different audiences need different granularity; one flat dashboard serves none well |
| Alert design | Every alert must be actionable | Non-actionable alerts cause fatigue, which degrades response to genuinely critical alerts |
| BigQuery pricing model | Mixed reservation + on-demand, matched per workload | Reservation for predictable load, on-demand for genuinely variable ad-hoc queries |
| Storage tiering | Automated lifecycle policy, not manual | Manual tier management reliably fails to happen consistently at scale |
| Streaming vs. batch (cost lens) | Batch unless genuine sub-minute freshness need exists | Continuous compute cost for a freshness requirement that doesn't exist is pure waste |
| Reservation sizing | Autoscaling with burst headroom, not flat peak-provisioned | Paying for month-end peak capacity 100% of the time is a quantifiable, avoidable cost |

---

## Interview Talking Points — Quick Reference

1. **Technical success and business correctness are two different claims** — lead with the "job succeeded but produced wrong output" framing; it's the single strongest point in this round.
2. **Row-count-against-baseline is a specific, concrete monitoring technique** worth naming by name — it's the direct answer to "how would you catch a silent data problem that doesn't throw an error."
3. **Cost-expensive-but-technically-correct is a distinct failure category** from bugs — name this explicitly; it reframes cost optimisation as an ongoing architecture discipline, not incident response.
4. **The streaming-by-default cost trap connects directly back to Round 2's technical warning** — reusing this thread across rounds (technical correctness in Round 2, cost consequence in Round 9) shows the same principle held consistently, not two disconnected observations.
5. **Reservation sizing should match actual usage curve, not worst-case peak held permanently** — autoscaling reservations with burst headroom is a specific, concrete, correct answer.
6. **Three-tier dashboard design is a deliberate UX decision** — naming the distinct audience for each tier (executive, on-call engineer, data steward) shows real operational thinking, not just "we have a dashboard."

---

## Next Rounds (Planned)

- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 9 of ongoing Lloyds Data Architect interview preparation series.*
