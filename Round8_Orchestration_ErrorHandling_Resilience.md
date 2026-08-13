# Principal Data Architect Study Guide — Round 8
## Orchestration, Error Handling & Resilience (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: Orchestration Design

### A.1 Comparing the Orchestration Options

| Tool | Best Fit | Why |
|---|---|---|
| **Cloud Composer** | End-to-end cross-system pipeline orchestration | DAG-based dependency management, retry policies, SLA monitoring, backfill support — the only option built for genuinely complex multi-stage dependencies |
| **Workflows** | Simple service-to-service chains | Lightweight, good for "call service A, then B, then C" — wrong tool for a pipeline with conditional branching, backfill, and reprocessing needs |
| **Dataform orchestration** | SQL transformation dependencies within Dataform itself | Handles dependency ordering *inside* the SQL transformation layer well, but doesn't extend to ingestion, DQ gating, or cross-tool coordination |
| **Dataflow scheduling** | Triggering individual pipeline runs | Not an orchestrator — a single Dataflow job doesn't know about upstream ingestion status or downstream consumer notification |
| **Cloud Scheduler** | Simple time-based triggers | Fires a job on a schedule; has no concept of dependency, retry-with-context, or DAG state — appropriate only as the entry-point trigger for Composer itself |

### A.2 Why Composer Controls the End-to-End Workflow

**The core reasoning:** this platform's pipeline has genuine cross-system, conditional dependencies — ingestion must complete and be validated before transformation starts; transformation must pass DQ gates before curated load; curated load must reconcile successfully before the flag table goes GREEN; regulatory mart refresh depends on curated being GREEN, not just complete. This is precisely the dependency-graph-with-conditional-logic problem Airflow/Composer is built for. Workflows and Cloud Scheduler lack the retry-with-full-context, backfill, and DAG-visibility capabilities this complexity demands.

**Composer sits above Dataform, Dataflow, and Dataproc — calling each as a task within the broader DAG**, not replacing their internal logic. Dataform still owns its own SQL dependency graph internally; Composer triggers Dataform runs as one node in the larger cross-system DAG and reacts to Dataform's success/failure state.

---

### A.3 Example Pipeline Dependency DAG

```
                    ┌─────────────────┐
                    │  Ingestion       │
                    │  (Datastream /   │
                    │   Dataflow /     │
                    │   DataSync)      │
                    └────────┬─────────┘
                             │ success + record count check
                             ▼
                    ┌─────────────────┐
                    │  DQ Gate 1       │
                    │  (Structural /   │
                    │   Completeness)  │
                    └────────┬─────────┘
                    ┌────────┴────────┐
              pass  │                 │  fail
                    ▼                 ▼
          ┌─────────────────┐   ┌──────────────┐
          │  Transformation  │   │  Quarantine  │
          │  (Dataflow/      │   │  + Alert     │
          │   Dataproc/      │   └──────────────┘
          │   Dataform)      │
          └────────┬─────────┘
                    │ success
                    ▼
          ┌─────────────────┐
          │  DQ Gate 2       │
          │  (Business Rules/│
          │   Referential/   │
          │   Uniqueness)    │
          └────────┬─────────┘
                    │ pass
                    ▼
          ┌─────────────────┐
          │  Curated Load    │
          │  (MERGE into     │
          │   BigQuery)      │
          └────────┬─────────┘
                    │ success
                    ▼
          ┌─────────────────┐
          │  DQ Gate 3       │
          │  (Reconciliation │
          │   vs. source)    │
          └────────┬─────────┘
              ┌─────┴─────┐
        pass  │           │  fail
              ▼           ▼
    ┌──────────────┐  ┌──────────────────┐
    │  Snapshot     │  │  Flag = RED       │
    │  Generation   │  │  + Block consumer │
    └──────┬────────┘  │    access + Alert │
           │            └──────────────────┘
           ▼
    ┌──────────────┐
    │  Flag Table   │
    │  = GREEN      │
    └──────┬────────┘
           ▼
    ┌──────────────────┐
    │  Regulatory Mart  │
    │  Refresh          │
    └──────┬────────────┘
           ▼
    ┌──────────────────┐
    │  Consumer          │
    │  Notification      │
    │  (Pub/Sub event →  │
    │   downstream teams)│
    └────────────────────┘
```

**Why the flag table sits precisely where it does in this DAG:** it's the explicit gate between "data has been technically loaded" and "data is certified safe for consumption" — every downstream task (regulatory mart refresh, consumer notification) depends on the flag being GREEN, not merely on the curated load task having completed. This is a deliberate DAG design choice, not a coincidence — a task succeeding technically and data being trustworthy are treated as two distinct conditions in the dependency graph, exactly because Round 6 established that reconciliation, not just successful execution, is what determines trustworthiness.

---

### A.4 SLA Monitoring Within Composer

Each DAG task has a defined SLA (e.g., ingestion must complete within 2 hours of the scheduled trigger; the full pipeline must reach GREEN by 06:00 UTC per the batch latency NFR from Round 1). Composer's SLA miss alerting fires distinctly from task-failure alerting — a task that's still running but exceeding its expected duration is a different, earlier warning signal than a task that has outright failed, and both need distinct alert handling: an SLA-miss alert gives the on-call team a chance to intervene *before* a downstream regulatory deadline is actually breached, not just notification after the fact.

---

## Part B: Error Handling and Resilience — Per Layer

### B.1 Retries

**Not all failures should be retried the same way.** A transient failure (a momentary network blip calling an external API, a temporary BigQuery quota throttle) genuinely benefits from automatic retry with exponential backoff. A deterministic failure (a schema mismatch, a malformed record) will fail identically on every retry — retrying it just delays the inevitable failure notification and wastes compute.

**Design rule:** Composer/Dataflow retry policies are configured per-task-type, not globally uniform:

```
Transient-failure-prone tasks (API calls, external service dependency):
    retry_count = 3, exponential backoff, alert only after exhaustion

Deterministic transformation logic (schema validation, business rules):
    retry_count = 0 or 1, immediate alert on failure —
    retrying a deterministic bug just delays detection
```

**Interview-ready point:** *"I don't set one blanket retry policy across the whole pipeline. Retrying a genuinely transient failure is good resilience; retrying a deterministic code or data bug three times before alerting just adds 15 minutes of delay before someone finds out about a problem that was never going to resolve itself on retry."*

---

### B.2 Dead Letter Queues — Recap and Extension

Established in Round 2 for streaming; the batch equivalent is the **quarantine zone** (Round 3/6). Both serve the identical architectural purpose: isolate the specific failing unit of work (a message, a record, a file) without blocking the healthy majority of the pipeline. The unifying principle worth stating explicitly in an interview: **failure isolation at the smallest reasonable granularity** — failing an entire batch of 10 million records because 200 of them are malformed is a design failure; routing those 200 to quarantine and processing the remaining 9,999,800 correctly is the correct default behaviour everywhere in this platform.

---

### B.3 Checkpointing

**Streaming (Dataflow):** automatic, built into the Streaming Engine — on worker failure, processing resumes from the last checkpointed offset without data loss, provided downstream writes are idempotent (the MERGE pattern, established repeatedly through this series, is precisely what makes checkpointed-and-resumed processing safe rather than risking duplicate writes on resume).

**Batch (Composer DAG state):** Composer persists task state — a DAG run that fails at the "Curated Load" task doesn't need to re-run ingestion or transformation; it resumes from the failed task once the underlying issue is fixed, using **task-level idempotency** (the same MERGE pattern) to make a re-run of just that task safe.

**Why idempotency is really the checkpointing enabler, not a separate topic:** checkpointing only provides real resilience if resuming from a checkpoint (or re-running a failed task) is guaranteed not to create duplicates or corrupt state — this is why idempotent design (Round 2) is a prerequisite for meaningful checkpointing, not an independent nice-to-have. A pipeline that checkpoints but isn't idempotent just resumes into a duplicate-data bug.

---

### B.4 Restartability and Reprocessing

Every DAG task in this pipeline is designed to be **safely restartable from the beginning of that specific task**, not requiring a full pipeline restart from ingestion. This is only possible because of the zone-based architecture from Round 3 — since raw is immutable and every intermediate zone (validated, curated) is a MERGE-based, idempotent write target, restarting the "Transformation" task doesn't require re-fetching from source; it simply re-reads the already-landed raw/validated data and re-runs the deterministic transformation logic.

**This directly connects restartability, replay, and backfill as the same underlying mechanism** (a point worth making explicitly, since interviewers often probe whether a candidate sees these as genuinely related or as three separate features to bolt on independently): all three are "re-run deterministic, idempotent logic against a defined historical range of already-durable data," differing only in *why* you're doing it (recovering from a failure, fixing a bug, loading data not yet in the platform) and *what range* you're targeting.

---

### B.5 Partial Failures

**The scenario:** a batch run processes data from five source systems; four succeed, one fails. The weak design fails the entire batch run, delaying all five source systems' data equally due to one problem. The correct design treats each source system's processing as an **independently restartable unit within the same DAG**, using Composer's dynamic task mapping or per-source sub-DAGs — four source systems' data reaches curated on schedule; the fifth is isolated, alerted, and retried/fixed independently without holding the other four hostage.

**Why this matters concretely for this platform:** regulatory reporting deadlines don't care that one of five source feeds had a problem — a design that blocks all curated data because of an isolated single-source issue creates unnecessary regulatory risk across four unrelated, healthy data feeds. Partial-failure isolation is a genuine business-risk reduction, not just an engineering nicety.

---

### B.6 Reprocessing, Replay, Backfill — Operational Triggers

| Trigger | Mechanism | Scope |
|---|---|---|
| **DQ failure fixed** (root cause resolved) | Targeted replay of quarantined records | Specific records/batch_run_id |
| **Late-arriving file** | Targeted reprocessing of affected historical partition | Specific transaction_date partition |
| **Transformation bug fixed** | Replay | Affected date range where bug was active |
| **Historical data not yet loaded** | Backfill | New, previously-unloaded date range |
| **Source system outage recovery** | Resume from last committed watermark | From watermark forward — no manual date range needed |

Each of these is operationally triggered differently (a DQ analyst resolving a quarantine record vs. an engineer deploying a bug fix and specifying a date range) but executes through the **same underlying idempotent, replay-safe pipeline logic** — this reuse is the payoff of the architectural discipline established across Rounds 2, 3, and 6, not a coincidence.

---

### B.7 Source Unavailable Scenarios

**On-premise source system down / Datastream CDC connection lost:**
- Composer's ingestion task fails per its retry policy; MSK/Pub/Sub (streaming) or the watermark-based extraction (batch) simply has nothing new to consume — this is not data loss, since nothing was missed, only delayed.
- On source recovery, streaming resumes from the last committed offset (Round 2's checkpointing); batch resumes extraction from the last committed watermark.
- **The distinction worth naming:** a source outage causes a *delay*, not *data loss*, specifically because watermarks/offsets are durable and committed only after verified success (Round 2's core design principle) — this is precisely why that design choice matters operationally, not just architecturally.
- If the outage exceeds the regulatory SLA window (e.g., threatens the 06:00 UTC curated availability target), this becomes an **SLA-miss alert** escalation, not a silent retry-forever situation — a human decision point (do we need to notify regulatory reporting teams of a delay) is deliberately triggered rather than the pipeline quietly retrying indefinitely.

---

### B.8 Target Unavailable Scenarios

**BigQuery streaming insert quota exceeded, or a transient BigQuery service issue:**
- Dataflow's built-in retry/backoff handles transient BigQuery unavailability automatically for streaming inserts; Pub/Sub's own retention (up to 7 days) provides a natural buffer, meaning a BigQuery outage of hours, not just seconds, doesn't lose data — it just delays visibility.
- For batch (Dataform/Dataflow writing to curated), a target-write failure fails that specific task, which per B.4's restartability design, is safely retried without needing to re-run upstream stages.
- **GCS unavailable** (rare, given GCS's own durability, but worth naming for completeness): since GCS is the ultimate replay source (Round 2/3), any pipeline stage genuinely depends on GCS's own extremely high durability guarantee — this is a deliberate reliance, not an overlooked single point of failure, because GCS's durability SLA is stronger than almost anything the platform could build to replace it.

---

### B.9 Schema Incompatibility

**A breaking schema change from a source system** (a field type change, a required field removed) is detected at DQ Gate 1 (structural/completeness) and routes affected records to quarantine with a specific `SCHEMA_INCOMPATIBLE` failure reason — this is treated as a distinct, higher-severity alert category from a routine DQ rule failure, because a schema break typically affects *all* subsequent records from that source, not an isolated few, and needs faster human attention (likely a source-system-side fix or an urgent pipeline code update) rather than routine quarantine review.

**Design decision:** schema validation happens **before** any business-rule DQ checks, not after — a batch with a broken schema shouldn't proceed through expensive business-rule validation logic that will fail meaninglessly on malformed structure; fail fast, fail cheap, at the earliest possible gate.

---

### B.10 Poison Messages

**A specific streaming message that causes the processing logic itself to crash repeatedly** (not just fail validation, but cause an unhandled exception in the transformation code) is a distinct, more dangerous failure mode than a normal DQ failure — left unhandled, it can block an entire Dataflow pipeline's progress if the same message is redelivered and re-crashes the worker indefinitely.

**Mitigation:** a **maximum redelivery/retry count** at the Pub/Sub subscription level, after which the message is automatically routed to the dead letter topic regardless of processing outcome — this is a hard circuit-breaker, distinct from application-level retry logic, specifically to prevent a single malformed message from stalling the entire streaming pipeline indefinitely. This is a concrete, specific point worth naming directly, since it's a genuinely common, real production incident pattern in streaming systems.

---

### B.11 Operational Escalation

```
Severity 1 (Regulatory SLA at risk):
    Immediate page to on-call data engineering + notify
    regulatory reporting team lead. Flag table stays RED.

Severity 2 (Pipeline failure, no immediate regulatory SLA risk):
    On-call alert, standard incident process, next-business-day
    escalation if unresolved.

Severity 3 (DQ WARNING volume anomaly, non-blocking):
    Daily digest to data steward, no immediate page.
```

**Why severity-tiered escalation matters as an architectural decision, not just an ops process detail:** it's the direct consequence of the BLOCKING/WARNING DQ severity model (Round 6) and the flag-table gating pattern extended into an operational response model — the architecture and the incident response process are designed together, consistently, rather than the technical design being built first and an escalation process bolted on afterward.

---

## Part C: Design Decisions Summary Table

| Decision | Choice | Why |
|---|---|---|
| Orchestration tool | Cloud Composer for end-to-end DAG | Only tool handling genuine cross-system, conditional dependency + backfill needs |
| Retry policy | Per-task-type, not global | Transient vs. deterministic failures need fundamentally different handling |
| Checkpointing prerequisite | Idempotent writes (MERGE) everywhere | Checkpointing without idempotency just resumes into duplicate-data bugs |
| Restartability granularity | Per-task, not full-pipeline | Avoids re-running expensive, already-successful upstream stages |
| Partial failure handling | Per-source independent sub-DAGs | One failing source shouldn't delay four healthy ones |
| Poison message handling | Hard max-redelivery circuit breaker at Pub/Sub subscription | Prevents a single malformed message stalling the entire streaming pipeline |
| Schema validation ordering | Before business-rule DQ checks | Fail fast/cheap; a broken schema invalidates business-rule checks anyway |
| Escalation model | Severity-tiered, tied to flag-table/DQ-severity model | Architecture and incident response designed together, not bolted on separately |

---

## Interview Talking Points — Quick Reference

1. **Replay, backfill, and restart are the same mechanism** — "re-run deterministic idempotent logic against a defined historical range" — differing only in trigger and scope. Say this explicitly; it signals systems thinking, not feature-listing.
2. **Idempotency is the prerequisite for checkpointing to actually provide resilience**, not a separate topic — checkpointing without idempotent writes just resumes into duplication bugs.
3. **Retry policy must differ by failure type** — retrying a deterministic bug three times before alerting is a design mistake, not conservative resilience.
4. **Poison message handling needs a hard circuit-breaker**, distinct from normal DQ failure routing — name the specific mechanism (max redelivery count at the subscription level).
5. **A source outage causes delay, not data loss** — and this is only true because of the durable, commit-after-verification watermark/offset design from Round 2. Connect the dots explicitly.
6. **Partial failure isolation (per-source sub-DAGs) is a business-risk reduction**, not just an engineering nicety — frame it in terms of not blocking four healthy regulatory feeds because of one unrelated failure.

---

## Next Rounds (Planned)

- **Round 9**: Observability & Cost Optimisation
- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 8 of ongoing Lloyds Data Architect interview preparation series.*
