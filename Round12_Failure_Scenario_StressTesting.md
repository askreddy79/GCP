# Principal Data Architect Study Guide — Round 12
## Failure Scenario Stress-Testing (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Why This Round Matters Most in an Interview

Everything in Rounds 1-11 describes the architecture working as designed. A Principal Architect interview will almost always pivot to "now break it" — because designing for the happy path is table stakes; designing (and reasoning live, under questioning) for failure is what actually distinguishes seniority. Each scenario below follows the same discipline: **what breaks first, what contains it, what doesn't get affected, and what a human still has to decide.** That last part matters — a mature answer never claims the architecture makes every decision automatically; some things are deliberately left as human judgement calls, and naming which ones is itself a sign of maturity.

---

## Scenario 1: Data Volume Increases Ten Times Overnight

**What actually breaks first, in order:**

1. **Dataflow autoscaling hits its configured max-worker ceiling** (Round 5/9) — if `maxNumWorkers` was sized for the current volume, not 10x headroom, throughput plateaus even though more compute could theoretically help. This is the first, fastest failure point — not a crash, but a growing backlog.
2. **Pub/Sub backlog grows** as Dataflow can't keep pace — Pub/Sub itself doesn't fail (it buffers up to retention, 7 days max), but downstream freshness SLAs (Round 1's 2-second fraud detection target) start breaching well before Pub/Sub itself is at risk.
3. **BigQuery reservation slot contention** — a reservation sized for the previous volume's query/load patterns now has 10x the data to process within the same batch window; regulatory reporting queries queue behind a backlog of curated-load MERGE operations competing for the same slots (Round 3 B.7).
4. **GCS small-file problem compounds** (Round 3/5) — if compaction jobs were scheduled at a cadence sized for the old volume, 10x more raw micro-batches accumulate faster than compaction can clear them, degrading downstream query performance further, compounding problem #3.

**What contains it, by design:**

- Autoscaling bounds are a **known, single Terraform-managed configuration value** (Round 10) — raising `maxNumWorkers` and reservation slot allocation is a fast, reviewed, redeployable change, not an architectural rebuild.
- The **flag-table gate** (Round 6/8) means degraded processing shows as a *late* GREEN, or a RED with clear reconciliation variance, rather than consumers silently seeing incomplete data — the failure is visible and honest, not silent.
- Per-source **partial-failure isolation** (Round 8, B.5) means if the 10x spike is concentrated in one source system, other sources aren't held hostage by the one struggling feed.

**What a human still has to decide:** whether the 10x is a genuine, sustained new baseline (justifying a permanent reservation/autoscaling reconfiguration and cost increase) or a temporary anomaly (justifying a manual temporary capacity bump rather than a permanent architecture change) — the monitoring (Round 9) surfaces the anomaly, but the capacity-planning decision itself is deliberately not automated, because over-provisioning permanently for a one-off spike is its own cost mistake (Round 9's "technically correct but expensive" category).

---

## Scenario 2: A Source Suddenly Sends Duplicate Events

**What actually breaks first:**

Nothing breaks catastrophically — this is precisely the scenario the idempotency design (Round 2, B.5; Round 8, B.3) exists for. But it's worth being specific about *where* duplicates get caught, because a shallow answer just says "we handle duplicates" without naming the layers.

**Layer-by-layer containment:**

1. **Streaming path:** Dataflow's built-in dedup catches duplicates within its window (default ~10 minutes) via message ID.
2. **Beyond the dedup window:** the natural-key MERGE at the BigQuery write layer (Round 2, B.3) is the backstop — even a duplicate arriving hours later doesn't create a duplicate row, because the write is a MERGE on `transaction_id + source_system`, not a blind append.
3. **Batch path:** the same MERGE pattern, plus batch-level idempotency tokens (Round 2, A.5) catching the case where an entire batch was accidentally re-triggered, not just individual duplicate records within a batch.
4. **Reconciliation (DQ Gate 3, Round 6)** would surface an *unexpected row-count anomaly* if duplicates were somehow still slipping through — the row-count-against-baseline monitoring (Round 9, A.3) is specifically designed to catch exactly this class of "technically successful, quietly wrong" failure.

**What's actually worth flagging as the deeper question here, and a strong thing to say unprompted:** *"My first question wouldn't be 'how do we handle the duplicates' — the architecture already handles that safely by design. My first question would be 'why is the source suddenly sending duplicates' — because a sudden behavioural change in a source system, even one my pipeline tolerates gracefully, usually means something changed upstream (a retry misconfiguration, a source-side bug, a failover event) that the source-system owning team needs to know about. I'd treat the DQ/reconciliation alert as a trigger for a conversation with the source team, not just confirmation that my dedup logic worked."* This shows you think beyond your own pipeline's boundary — a genuinely senior instinct.

---

## Scenario 3: The Schema Changes Unexpectedly

**What actually breaks first:**

DQ Gate 1 (structural/schema validation, Round 6/8) is specifically positioned *before* business-rule checks precisely for this scenario — a breaking schema change (field removed, type changed) fails fast at the earliest, cheapest gate, routing the affected batch/messages to quarantine with a `SCHEMA_INCOMPATIBLE` reason code (Round 8, B.9), rather than propagating malformed data deeper into the pipeline where it's more expensive to detect and unwind.

**What contains it:**

- **Streaming:** a schema-breaking message hits the dead letter topic; Cloud Monitoring alerts on DLQ volume spike specifically (Round 2, B.6) — a sudden spike is the signal that distinguishes "isolated bad message" from "the source changed its schema," since a genuine schema change produces a *sustained* spike, not a one-off.
- **Batch:** the file-level structural check (Round 2, A.8) catches column-count/mandatory-field mismatches before the file is even promoted to raw-validated processing.
- **Forward-compatible changes** (a new optional field added) are explicitly designed to *not* trigger this failure path (Round 2, B.9) — the pipeline passes unrecognized new fields through without failing, only breaking changes trigger the DLQ/quarantine path. This distinction is worth stating explicitly, since conflating "any schema change" with "breaking schema change" is a common, weaker answer.

**What a human still has to decide:** whether the schema change is a legitimate, planned source-system update (requiring a coordinated pipeline code deployment to handle the new schema) or an unplanned/erroneous change (requiring escalation back to the source system owner to fix) — the DQ alert surfaces the *fact* of the change; distinguishing "expected evolution" from "upstream bug" is a judgement call requiring context the pipeline itself doesn't have.

---

## Scenario 4: A Dataflow Pipeline Crashes

**What actually breaks first:**

Depends on *why* it crashed — this distinction matters and is worth drawing out explicitly rather than giving one generic answer:

- **Worker-level failure** (a single worker VM issue): Dataflow's automatic checkpointing (Round 8, B.3) resumes processing from the last committed checkpoint on a replacement worker — largely self-healing, provided the pipeline's writes are idempotent (which they are, by design, per ADR-007-adjacent reasoning throughout this series).
- **Pipeline-level crash** (a bug in the transformation code causing repeated failure, not just one bad worker): this is where the **poison-message circuit breaker** (Round 8, B.10) matters — without a max-redelivery cap at the Pub/Sub subscription level, a single malformed message repeatedly crashing the processing logic could stall the entire pipeline indefinitely, since Dataflow would keep redelivering and re-crashing. With the circuit breaker, the offending message routes to DLQ after the redelivery threshold, and the pipeline continues processing the healthy remainder.

**What contains it:**

- The **GCS raw layer's replay capability** (ADR-003) means even a pipeline crash severe enough to require redeployment doesn't lose data — a batch Dataflow job can reprocess the affected time range from raw once the bug is fixed (Round 8, B.6's replay mechanism).
- **Composer's task-level restartability** (Round 8, B.4) means the crash is contained to the specific failed task/stage, not requiring a full pipeline restart from ingestion.

**What a human still has to decide:** whether to redeploy a fix and let checkpointed resume handle recovery automatically, or whether the crash indicates data already processed before the crash needs to be specifically re-validated (e.g., if the bug might have silently corrupted output before crashing, not just failed cleanly) — this is a judgement call about the *nature* of the bug, not something the resilience architecture itself can determine.

---

## Scenario 5: BigQuery Queries Become Slow

**What actually breaks first:**

This is the exact diagnostic scenario walked through in Round 5, D.1 — worth restating the sequence concisely here in stress-test framing: bytes-scanned comparison first (data growth vs. genuine regression), then partition pruning check, then clustering degradation check, then reservation/slot contention check.

**What contains it:**

- **Reservation isolation** (Round 3, B.7; Round 7, G.1 Layer 5) means a slowdown caused by contention from one workload (e.g., an analyst running an expensive ad-hoc query) doesn't degrade regulatory reporting query performance — they're on separate reservations by design, so this specific failure mode is architecturally prevented, not just monitored.
- **`require_partition_filter = true`** (Round 5, D.2) prevents the single most common cause of sudden query slowdown — an accidental full-table scan — from being possible in the first place, by policy.

**What a human still has to decide:** if the root cause is genuine data volume growth outpacing the current partitioning/clustering strategy (e.g., a table's natural query patterns have shifted such that the original clustering columns are no longer the right choice), redesigning the clustering strategy is a deliberate, reviewed schema change — not something to do reactively under incident pressure without validating it against actual current query patterns first.

---

## Scenario 6: One Region Becomes Unavailable

**Directly covered in Round 10, B.1-B.5 — the stress-test framing worth adding here:** the response differs by data domain, and stating this domain-differentiated response *without being asked* is a strong signal, since a weaker answer treats "region down" as one uniform event requiring one uniform response.

- **Payment/regulatory data** (active-active dual-region, RPO=0): failover is a traffic redirect to the already-current DR region — the interesting operational question here isn't data recovery, it's **how fast can query/serving traffic actually be redirected**, and whether that redirect itself is tested/automated or a manual runbook step (Round 10, B.7) — this is worth naming as the actual bottleneck in an RPO=0 posture, since the data itself was never at risk.
- **Lighter-DR-posture analytical marts**: genuine recovery time is bounded by Terraform redeployment speed (ADR-007-adjacent reasoning) plus the most recent cross-region replication lag — worth being able to state a concrete number here (e.g., "if replication runs every 15 minutes and Terraform redeploy takes 20 minutes, worst-case data loss is 15 minutes and worst-case unavailability is roughly 20-30 minutes") rather than a vague "we'd recover reasonably quickly."

**What a human still has to decide:** whether to actually declare a DR event and fail over, versus waiting for the primary region to recover (if the outage appears short-lived) — failover itself has a cost/risk (the redirect, the potential for a brief dual-write or split-brain window if not carefully sequenced), so this is explicitly named in the runbook as a decision requiring defined authority (Round 10, B.7), not an automatic trigger.

---

## Scenario 7: A Source Sends Corrupted Data

**What actually breaks first:**

Distinguish two sub-cases, since a shallow answer treats "corrupted" as one thing:

- **Structurally corrupted** (a truncated file, invalid encoding, unparseable format): caught at the file-level validation gate before promotion to raw (Round 2, A.3/A.8) — the file itself is quarantined, never contaminating raw.
- **Structurally valid but semantically wrong** (a file that parses fine but contains nonsensical values — e.g., a transaction amount of -999999999, or a settlement date decades in the future): this is where record-level DQ rules (Round 6, A.2's validity/accuracy dimensions) and reconciliation (Round 6, A.2) catch it — a row-count-against-baseline anomaly (Round 9, A.3) is often the first real signal here too, since genuinely corrupted semantic data often correlates with unusual volume patterns.

**What contains it:** the same quarantine-and-alert pattern throughout this series — corrupted records/files never reach curated, and the flag table (Round 6/8) prevents consumer access to a batch with unresolved corruption until it's fixed or explicitly, deliberately worked around.

**What a human still has to decide:** whether corrupted data indicates a source-system bug requiring escalation, or a genuine, legitimate edge case the DQ rules were simply too strict about (a false positive) — this requires a data steward's domain judgement (Round 6, quarantine as an "operational queue... actively worked from," not an automatic reject).

---

## Scenario 8: A Reporting Team Launches Hundreds of Concurrent Queries

**What actually breaks first:**

Without workload isolation, this is precisely Round 3 B.7's named risk — a burst of analyst queries consuming enough shared on-demand slots to delay a regulatory submission.

**What contains it — this should be stated as already-designed-for, not a new problem:**

- **Dedicated reservations per workload type** (Round 3, B.7): the BI/analyst reservation absorbs the burst (with autoscaling headroom, Round 9's B.6 Example 3 reasoning) while the regulatory-reporting reservation remains isolated and unaffected.
- If the burst genuinely exceeds even the BI reservation's autoscaled capacity, queries queue *within that reservation* — degraded analyst experience, but a contained, expected degradation that doesn't cascade into regulatory SLA risk.

**What a human still has to decide:** whether the burst pattern (e.g., month-end close, per Round 1's stated NFR) is a known, expected, recurring pattern that should be permanently provisioned for, versus a genuinely unexpected surge that warrants investigating *why* — the monitoring dashboard (Round 9, A.5) surfaces the burst; understanding its cause and whether it recurs is an ongoing capacity-planning conversation, not a one-time fix.

---

## Scenario 9: A Regulatory Report Must Be Recreated for a Date Six Months Ago

**This is the scenario that most directly tests whether the whole architecture actually delivers on its foundational promise (ADR-003) — walk through it as a coherent chain, since this is likely to be asked as a direct, standalone question.**

**Step-by-step reconstruction:**

1. **Identify the exact raw data available as of that historical date** — GCS raw is immutable and timestamped (Round 2, B.10's envelope/audit metadata), so the precise set of source records that existed at that point is unambiguous, not reconstructed from memory or approximate logs.
2. **Identify which pipeline code version processed that date's data originally** — this requires the CI/CD deployment history (Round 10, A.2) to know which version of the transformation logic was live on that date, since a bug fix deployed since then means simply re-running *current* code against historical raw data would NOT reproduce the original report — a genuinely important, easy-to-miss subtlety worth naming explicitly.
3. **Reprocess using that specific historical code version** against the immutable raw data for that date range — this is the replay mechanism (Round 2, A.9; Round 8, B.4) applied to a genuinely historical reconstruction, not a recent-failure recovery.
4. **Apply the SCD Type 2 dimension state as it existed at that time** (Round 4, B.2) — critically, dimension joins must use the dimension's historical state (a customer's risk rating *as of six months ago*, not today's current rating), which is exactly why SCD Type 2 discipline on regulatory-relevant dimensions was established as non-negotiable in Round 4 — this scenario is the concrete payoff of that earlier design decision.
5. **Validate via reconciliation** (Round 6) that the reconstructed output matches whatever audit trail exists from the original submission (the `batch_run_audit` table, Round 2, B.10) — proving the reconstruction is faithful, not just plausible.

**Interview-ready synthesis, worth stating explicitly as the closing point:** *"This scenario is really the test of everything else in the architecture working together — immutable raw data, versioned pipeline code, SCD Type 2 dimensional history, and audit trail metadata all have to be correct simultaneously for this to actually work. If any one of those four is missing — say, if we'd used SCD Type 1 on risk rating, or hadn't versioned our transformation code — the reconstruction would silently produce a plausible-looking but wrong report. That's exactly why BCBS 239 reconstruction isn't a single feature I bolt on, it's a property that has to be designed into raw storage, modelling, and CI/CD simultaneously from day one."*

---

## Part B: Cross-Scenario Pattern — What to Notice

Across all nine scenarios, the same handful of architectural properties keep doing the actual work, which is worth naming as a closing, unifying point if an interviewer asks "is there a common thread":

1. **Immutable raw + idempotent processing** (ADR-003, Round 2) — the foundation nearly every recovery/replay/reconstruction scenario depends on.
2. **Fail-fast, isolate-narrowly** (quarantine, DLQ, per-source sub-DAGs) — failures are contained to the smallest reasonable unit, never allowed to cascade or force an all-or-nothing outcome.
3. **The flag-table gate** — consumers are protected from seeing untrustworthy data by an explicit, binary signal, not by hoping every failure mode is caught before it matters.
4. **Workload/domain differentiation, not uniform treatment** — reservations, DR posture, and RPO/RTO are all deliberately non-uniform, matched to actual consequence.
5. **A human decision point deliberately preserved at the judgement layer** — the architecture handles mechanical recovery automatically, but escalation, root-cause interpretation, and "is this the new normal" capacity decisions are consistently left to a human, by design, not automated away.

---

## Interview Talking Points — Quick Reference

1. **For every failure scenario, structure your answer as: what breaks first → what contains it → what a human still decides.** This three-part structure signals systematic thinking under a "break this" question, rather than an improvised, scattered answer.
2. **The duplicate-events scenario is a chance to show instinct beyond your own pipeline** — "why is the source suddenly duplicating" is a stronger answer than just describing your dedup mechanism.
3. **The six-months-ago regulatory reconstruction scenario is the single best synthesis question in this entire series** — have the four-part chain (immutable raw + versioned code + SCD Type 2 + audit trail) ready to state fluently; it's very likely to come up close to verbatim.
4. **Always distinguish sub-cases within a scenario** ("corrupted" isn't one thing; "schema change" isn't one thing) — this granularity is what separates a genuinely experienced answer from a memorised one.
5. **State the cross-scenario pattern unprompted if you get the chance** — recognising that immutability, isolation, and domain-differentiation are recurring principles (not five separate unrelated designs) is the clearest signal of Principal-level systems thinking available in this whole preparation series.

---

## Next Round (Planned)

- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 12 of ongoing Lloyds Data Architect interview preparation series.*
