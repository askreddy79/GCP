# Principal Data Architect Study Guide — Round 4
## Data Modelling (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: The Core Question Before Choosing a Modelling Approach

A weak answer picks one modelling pattern and applies it everywhere. A Principal Architect recognises that **different consumers have different modelling requirements**, and a large bank almost always runs multiple modelling patterns simultaneously, deliberately, at different layers of the same platform — not as inconsistency, but as fit-for-purpose design.

The question to ask for every dataset: **who consumes this, how do they query it, and what does "correct" mean to them?** A regulator reconstructing a report needs full historical traceability. A finance analyst needs fast, well-understood aggregates. An API needs a single-row lookup in milliseconds. These are not the same modelling problem, even if the underlying data is related.

---

## Part B: The Modelling Patterns, Evaluated Honestly

### B.1 Third Normal Form (3NF)

**What it is:** Fully normalised relational modelling — every non-key attribute depends on the whole key and nothing but the key. Minimises redundancy.

**Where it genuinely fits in this architecture:** 3NF is rarely the *serving* model in a modern analytical warehouse, but it's often the natural shape of **source system data** (core banking systems are typically 3NF-normalised OLTP schemas) and can be a reasonable intermediate modelling choice in the **Silver/validated layer**, where the goal is fidelity to source structure and minimal redundancy before business-level remodelling happens downstream.

**Why it's the wrong choice for serving/reporting:** highly normalised schemas require many joins to answer a single business question — exactly the query pattern that hurts performance and cost in BigQuery (more joins, more bytes scanned across more tables, harder for BI tools to reason about). Finance and risk analysts generally cannot write, and shouldn't need to write, 12-table joins to get "total exposure by counterparty."

**Honest interview framing:** *"3NF isn't a serving-layer choice for me — it's often what I find already in the source data, and I might preserve that shape briefly in an intermediate layer for traceability, but I actively move away from it before anything reaches a business consumer."*

---

### B.2 Star Schema (Dimensional Modelling — Kimball)

**What it is:** A central fact table (transactions, trades, payments) surrounded by denormalised dimension tables (customer, product, counterparty, calendar), optimised for query simplicity and aggregation performance.

**Where it genuinely fits:** This is the **default choice for finance reporting, operational reporting, and most BI/analyst consumption** in this architecture. It maps naturally to how business users think ("show me total transaction volume by product, by month, by region") and performs well in BigQuery specifically because of how partitioning/clustering interact with a fact table's structure (Round 3).

**Concrete example for this platform:**

```
dim_customer ──┐
dim_product ───┼──► fact_transactions ◄─── dim_calendar
dim_branch ────┘         │
                  dim_counterparty
```

**Slowly Changing Dimensions (SCD) — this is where interview depth actually gets tested:**

- **SCD Type 1** (overwrite): used when historical accuracy of the *dimension attribute itself* doesn't matter — e.g., correcting a typo in a customer's name. No history preserved, simplest to implement.
- **SCD Type 2** (new row per change, with `effective_from`/`effective_to`/`is_current` flags): the default for anything with regulatory or audit relevance — e.g., a customer's risk rating, a counterparty's credit classification, an account's product tier. If a customer's risk rating changes from "medium" to "high" on a given date, a transaction from before that date must still join to the risk rating **as it was at the time of the transaction**, not the current rating. This is not an implementation detail — it's a regulatory correctness requirement (directly connects to BCBS 239's reconstruction demands from Round 1).
- **SCD Type 3** (add a new column for the previous value): rarely used in this architecture — only appropriate when you need exactly one prior value, not full history, which is uncommon in banking's audit requirements.

**Design rule for this platform:** any dimension attribute that could plausibly affect how a historical transaction is interpreted or reported gets **SCD Type 2**, full stop. This includes risk ratings, product classifications, counterparty status, and customer segment. Getting this wrong — using Type 1 on a dimension that regulators care about historically — is a genuine, serious design failure, not a minor modelling preference.

---

### B.3 Data Vault

**What it is:** A modelling pattern built from **Hubs** (business keys), **Links** (relationships between business keys), and **Satellites** (descriptive attributes with full history) — designed explicitly for auditability, traceability, and resilience to source system change.

**Where it genuinely fits:** Data Vault earns its complexity specifically for **regulatory reporting and any layer where "prove exactly what you knew and when" is the primary requirement** — not because it's more sophisticated, but because its structure makes full historical traceability and source-system change resilience a *structural property* rather than something bolted on via SCD Type 2 discipline alone.

**Concrete example — trade/transaction hub:**

```
HUB: hub_transaction
  - transaction_id (business key)
  - load_date
  - record_source

LINK: link_transaction_counterparty
  - transaction_id (FK to hub)
  - counterparty_id (FK to hub_counterparty)
  - load_date

SATELLITE: sat_transaction_details
  - transaction_id (FK to hub)
  - load_date
  - amount, currency, status, ...
  - hash_diff (for change detection)
```

**Why this matters specifically for a bank, not modelling theory:** Data Vault's insert-only, full-history-by-design structure means you never lose the ability to answer "what did we know about this transaction as of any point in time" — every Satellite record is a timestamped, immutable fact. This maps directly onto BCBS 239's reconstruction requirement in a way that's structurally enforced, not dependent on every engineer correctly applying SCD Type 2 discipline everywhere.

**The honest trade-off — don't oversell this:** Data Vault is significantly more complex to build, query, and explain to business consumers than a star schema. Querying a Data Vault model directly (many Hub-Link-Satellite joins) is not something you'd expose to a finance analyst — Data Vault is typically a **modelling layer that sits between raw/validated and the star-schema marts**, not the final serving layer itself. You build Data Vault for traceability, then build star schema marts *from* the Data Vault for actual consumption.

**Interview-ready framing:** *"I wouldn't put Data Vault in front of business users — it's the wrong query experience for them. Where I'd use it is as an intermediate modelling layer specifically for regulatory-sensitive domains, where full historical traceability needs to be a structural guarantee rather than a modelling discipline I hope every engineer follows consistently. Star schema marts get built on top of it for actual consumption."*

---

### B.4 Wide Tables / One Big Table (OBT)

**What it is:** Denormalising dimensions directly into the fact table itself — no joins required at query time, everything a consumer needs is in a single wide table.

**Where it genuinely fits:** **API consumption and low-latency operational lookups**, where join cost at query time is unacceptable and the consumer needs a single-row, self-contained answer. Also increasingly relevant for **BI tool performance** where a wide, pre-joined table avoids repeated join costs for dashboards hit by hundreds of concurrent users.

**Concrete example — customer risk API lookup table:**

```
wide_customer_risk_snapshot
├── customer_id
├── customer_name
├── risk_rating_current
├── product_tier
├── branch_name
├── branch_region
├── last_transaction_date
├── total_exposure_amount
└── ... (everything a single API call needs, pre-joined)
```

**The honest trade-off:** wide tables trade storage and update complexity for query simplicity — every dimension change requires updating the wide table (not just the dimension), and storage redundancy is real (the same branch name repeated across millions of rows). This is a deliberate trade for the specific consumers (API, high-concurrency BI) where query-time join cost genuinely matters more than storage cost or update complexity.

**Where this connects back to Round 3:** this is precisely the modelling pattern behind the Bigtable-backed low-latency API store — the wide table concept, physically implemented in a store designed for fast single-row reads.

---

## Part C: Recommendation by Consumer Type

This is the section most likely to be directly probed in an interview — be ready to justify each choice, not just state it.

### Regulatory Reporting (PRA/FCA, BCBS 239, MiFID/EMIR-equivalent)

**Recommendation: Data Vault as the intermediate traceability layer, feeding purpose-built regulatory star-schema marts for actual report generation.**

**Why:** Regulatory reporting has two competing needs simultaneously — the ability to *prove* full historical accuracy and traceability (Data Vault's structural strength), and the ability to *efficiently generate* a specific, well-defined report on a schedule (star schema's query performance strength). Neither pattern alone satisfies both needs well. The two-layer approach — Vault for traceability, star schema marts for generation — resolves this without compromise.

**Concrete regulatory mart example:** a "PRA110 liquidity reporting" mart is a purpose-built star schema, refreshed from the underlying Data Vault, isolated in its own BigQuery dataset (Round 3's access-control-boundary design), with full SCD Type 2 on every dimension that affects historical interpretation.

---

### Finance Reporting

**Recommendation: Star schema, standard Kimball dimensional modelling.**

**Why:** Finance reporting is fundamentally about well-defined, repeatable aggregations (P&L, balance sheet views, cost centre reporting) that map naturally to fact/dimension structures. Finance teams are typically SQL-literate but not engineers — a star schema is queryable and explainable to them directly, which matters for their own internal audit and review processes. Data Vault's complexity isn't justified here unless a specific finance domain also carries heavy regulatory reconstruction requirements (in which case, treat it like regulatory reporting above).

---

### Operational Reporting

**Recommendation: Star schema, often with materialized views (Round 3) for the most frequently-run operational dashboards.**

**Why:** Operational reporting (transaction volumes, branch performance, daily processing status) is dashboard-heavy, high-concurrency, and benefits from the same query-simplicity argument as finance reporting — plus materialized views specifically address the "same aggregation queried repeatedly throughout the day" pattern operational dashboards create.

---

### API Applications

**Recommendation: Wide/OBT pattern, physically served from Bigtable or a heavily denormalised BigQuery table depending on latency requirement.**

**Why:** APIs need single-row, low-latency lookups — join cost at query time is the enemy here, not storage redundancy. This connects directly to the Bigtable design decision from Round 1/3: the wide table pattern is the *logical* model; Bigtable (or a denormalised BigQuery table for less latency-sensitive APIs) is the *physical* implementation.

---

### Risk Analytics

**Recommendation: Hybrid — Data Vault-sourced history for model training/backtesting (where full historical accuracy of every input matters), star schema marts for risk reporting dashboards, and feature-store-style wide tables for real-time risk scoring (feeding Vertex AI).**

**Why:** Risk analytics genuinely spans all three needs simultaneously — a credit risk model being trained needs full historical accuracy of every input as it was known at the time (Data Vault's strength, avoiding a subtle but serious bug class called "look-ahead bias," where a model accidentally trains on information that wasn't actually available at prediction time), while a risk dashboard needs star-schema query performance, and a real-time fraud/risk score needs OBT-style low-latency lookup. This is the strongest example in the entire platform of why "pick one modelling pattern" is the wrong framing — risk analytics uses all three, deliberately, for three different sub-problems.

---

## Part D: Facts, Dimensions, and Historical Tracking — Deeper Detail

### D.1 Fact Table Types

- **Transaction fact tables**: one row per business event (a payment, a trade). This is the dominant pattern for this platform — `fact_transactions`, `fact_trades`.
- **Periodic snapshot fact tables**: one row per entity per time period (e.g., daily account balance snapshot), used when the *state* at a point in time matters more than individual events — directly relevant to the "daily snapshot table" performance pattern that solves the query-scanning problem discussed in earlier rounds.
- **Accumulating snapshot fact tables**: one row per process instance, updated as the process moves through stages (e.g., a loan application from submission through approval through disbursement) — useful for operational reporting on process pipelines, less common in core transaction reporting.

**Design decision for this platform:** `fact_transactions` (transaction fact) is the immutable event-level truth. A separate `fact_account_balance_daily_snapshot` (periodic snapshot) is derived from it specifically to avoid the performance problem of aggregating millions of transaction rows every time someone needs "current balance" — this is the same architectural principle as the OTC trade snapshot pattern discussed earlier in this preparation series, now formalised as a named fact table type.

### D.2 Grain — The Most Important, Most Overlooked Modelling Decision

**Grain** is the precise definition of what one row in a fact table represents. Get this wrong and every downstream aggregation is subtly incorrect.

For `fact_transactions`: is the grain "one row per transaction" or "one row per transaction leg" (a transfer has a debit leg and a credit leg — are these one row or two)? This is not a pedantic detail — a finance team summing `amount` across the table will get double the correct value if the grain is actually two rows per transaction and nobody documented it clearly.

**Interview-ready point:** *"Before I build any fact table, I write down the grain explicitly — one sentence, unambiguous — and that sentence becomes part of the table's documentation in Data Catalog. 'One row per transaction leg, where a transfer produces two rows' is a very different modelling contract than 'one row per transaction,' and getting this ambiguous is one of the most common, costly mistakes I've seen in dimensional modelling."*

### D.3 Conformed Dimensions

`dim_customer`, `dim_counterparty`, and `dim_calendar` are **conformed dimensions** — built once, with consistent keys and definitions, and reused across every fact table and every mart (regulatory, finance, risk) that needs them. This is what makes cross-domain analysis possible (e.g., joining risk exposure data with finance P&L data using the same `customer_id` and getting a coherent answer) and is a direct extension of the "single source of truth" principle running through this entire architecture — conformed dimensions are how that principle is enforced at the modelling layer specifically.

---

## Part E: Design Decisions Summary Table

| Consumer | Primary Pattern | Why |
|---|---|---|
| Regulatory Reporting | Data Vault (traceability) → Star schema marts (generation) | Needs structural historical proof + efficient report generation |
| Finance Reporting | Star schema | SQL-literate consumers, well-defined repeatable aggregations |
| Operational Reporting | Star schema + Materialized views | High-concurrency dashboards, repeated aggregation patterns |
| API Applications | Wide/OBT (physically: Bigtable or denormalised BQ) | Query-time join cost unacceptable at required latency |
| Risk Analytics | Hybrid: Vault (training/backtesting) + Star (dashboards) + OBT (real-time scoring) | Spans historical accuracy, reporting, and real-time needs simultaneously |
| Silver/intermediate layer | 3NF-adjacent, source-aligned | Fidelity to source before business remodelling, not a serving pattern |

---

## Interview Talking Points — Quick Reference

1. **Never present one modelling pattern as "the" answer** — the strongest answer is naming which pattern fits which consumer, and why, in the same breath.
2. **SCD Type 2 on any regulatory-relevant dimension is non-negotiable** — connect this explicitly to BCBS 239's reconstruction requirement, not just "it's best practice."
3. **Data Vault is a traceability layer, not a serving layer** — never claim you'd expose Data Vault directly to business users; star schema marts sit on top of it.
4. **Grain must be explicitly documented, not assumed** — this is a specific, concrete example of modelling discipline that signals real experience.
5. **Risk analytics is the strongest example of "use multiple patterns deliberately"** — Vault for training data integrity (avoiding look-ahead bias), star schema for dashboards, OBT for real-time scoring.
6. **Conformed dimensions are what make cross-domain analysis (and audit) actually possible** — built once, reused everywhere, consistent keys.

---

## Next Rounds (Planned)

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

*Document 4 of ongoing Lloyds Data Architect interview preparation series.*
