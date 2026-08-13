# Principal Data Architect Study Guide — Round 11
## Architecture Decision Records (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Why ADRs Matter as an Interview Artifact

A weak candidate describes an architecture as a set of choices that were obviously correct. A Principal Architect documents decisions as genuine trade-offs — naming what was given up, not just what was gained. Interviewers probing architecture depth often ask "why not X instead" specifically to see whether a candidate has actually weighed the alternative or is just defending a default. Each ADR below is written so you can defend the *trade-off*, not just recite the *choice*.

**Standard ADR format used throughout:** Context → Decision → Alternatives Considered → Trade-offs → Consequences.

---

## ADR-001: BigQuery as the Primary Analytical Warehouse

**Context:** The platform needs a serving layer supporting regulatory reporting, finance/operational BI, and analyst ad-hoc queries at 200+ concurrent users bursting to 500, over a dataset growing from ~300GB/day to 5-8TB/day.

**Decision:** BigQuery is the primary analytical serving engine.

**Alternatives Considered:**
- **Cloud SQL / Spanner** — wrong tool class; built for OLTP, not analytical aggregation at this scale and concurrency.
- **Self-managed Spark/Presto on Dataproc as the serving layer** — technically possible, but reintroduces cluster management overhead BigQuery's serverless model eliminates, and lacks BigQuery's native partitioning/clustering/reservation cost-management tooling.

**Trade-offs:** BigQuery's on-demand pricing model can become expensive/unpredictable without disciplined partitioning and query governance (Round 3/9) — this is a real, accepted cost we mitigate architecturally (partition filters required, reservations for predictable workloads) rather than a cost-free choice.

**Consequences:** Every downstream design decision (partitioning strategy, clustering, reservation sizing, materialized views) exists specifically to manage BigQuery's cost/performance profile at this scale — choosing BigQuery is not a one-time decision but an ongoing architectural discipline.

---

## ADR-002: Datastream for CDC, With an Explicit Gap for Mainframe Sources

**Context:** Core banking data must move from Oracle/PostgreSQL/MySQL and legacy mainframe/DB2 systems into the platform without adding load to production OLTP systems.

**Decision:** Datastream provides log-based CDC for Datastream-supported relational sources. Legacy mainframe/DB2 sources use a separate, mainframe-side extract process landing files into GCS.

**Alternatives Considered:**
- **Debezium on GKE for all sources** — broader source support (including some DB2 variants) but adds a self-managed Kafka Connect operational burden across the entire estate, not just the gap Datastream leaves.
- **Custom JDBC extraction via Dataflow for all sources** — avoids the mainframe gap being a special case, but rebuilds CDC/watermarking logic Datastream provides natively for the majority of sources that don't need it.

**Trade-offs:** Using two different ingestion mechanisms (Datastream + mainframe-specific extract) is architecturally less uniform than a single tool for everything, but avoids taking on Debezium's operational cost platform-wide to solve a problem that only affects a minority of sources.

**Consequences:** The mainframe extract path needs its own monitoring, watermarking, and DQ gate design, run in parallel with the Datastream path — this is explicitly acknowledged complexity, not glossed over as if Datastream handled everything uniformly.

---

## ADR-003: Cloud Storage as the Immutable System of Record, Not Pub/Sub or BigQuery

**Context:** Regulatory reconstruction requirements (BCBS 239) demand the ability to prove exactly what data was received, unaltered, indefinitely.

**Decision:** GCS raw zone is the permanent, immutable system of record. Pub/Sub (7-day max retention) and BigQuery (mutable, queryable) are not treated as the source of truth for raw data, regardless of how convenient direct BigQuery streaming inserts might seem.

**Alternatives Considered:**
- **Treat BigQuery raw tables as the system of record** — rejected because BigQuery tables, even with time travel, are designed for query convenience and mutability, not immutable long-term archival; time travel's default 7-day window is far short of the 7-10 year regulatory retention requirement.
- **Rely on Pub/Sub retention for replay** — rejected outright; 7-day cap makes this structurally impossible for genuine long-term replay.

**Trade-offs:** Writing to GCS in parallel with BigQuery (in the streaming path specifically) adds a second write path and associated cost/complexity versus a single-write design, but this is the direct, necessary cost of having a genuine immutable record independent of the serving layer.

**Consequences:** Every replay, backfill, and DR recovery mechanism across the entire platform (Rounds 2, 8, 10) depends on GCS raw being complete and immutable — this single decision is foundational to nearly everything else in the architecture, which is worth stating explicitly in an interview as evidence of how one early decision cascades.

---

## ADR-004: Dataform for SQL Transformations, Reserving Dataflow/Dataproc for Complex Logic

**Context:** Transformation logic ranges from straightforward SQL aggregation (building regulatory marts) to complex nested-XML/FpML-style parsing and custom deduplication algorithms.

**Decision:** Push transformation logic into Dataform (SQL) wherever the complexity allows; reserve Dataflow/Dataproc for transformations SQL genuinely cannot express.

**Alternatives Considered:**
- **Do all transformation in Dataflow/Dataproc uniformly** — technically capable of everything SQL can do, but produces Beam/Spark code that finance and risk analysts cannot read or independently verify — a real governance cost in a regulated environment where transformation logic sometimes needs non-engineer review.
- **Do all transformation in Dataform/SQL, including complex parsing** — SQL genuinely struggles with deeply nested XML parsing and complex custom algorithms; forcing this would produce unreadable, unmaintainable SQL as a worse trade than using the right tool.

**Trade-offs:** Maintaining two transformation paradigms (SQL-based Dataform and code-based Dataflow/Dataproc) is more operationally complex than a single paradigm, but the auditability gain — finance/risk teams can read and verify SQL logic directly — is judged worth that complexity for a regulated bank specifically.

**Consequences:** New transformation requirements must be evaluated against the decision rule explicitly ("could this be SQL?") rather than defaulting to whichever tool the engineer building it happens to prefer — this needs to be an enforced team convention, not just a stated preference.

---

## ADR-005: Data Vault as an Intermediate Traceability Layer, Not the Serving Model

**Context:** Regulatory reporting requires structural, provable historical traceability; business consumers need query-simple, performant serving.

**Decision:** Data Vault (Hubs/Links/Satellites) is used as an intermediate modelling layer specifically for regulatory-sensitive domains, with star-schema marts built on top of it for actual consumption. Data Vault is never exposed directly to business users.

**Alternatives Considered:**
- **Star schema with disciplined SCD Type 2 everywhere, no Data Vault** — simpler, single-pattern architecture, but relies on every engineer correctly applying SCD Type 2 discipline consistently rather than having traceability as a structural property of the model itself — a real risk for a requirement (BCBS 239 reconstruction) with serious compliance consequences if discipline lapses even once.
- **Data Vault as the direct serving layer** — rejected; Hub-Link-Satellite query patterns are too complex for business/BI consumption, would require every analyst to understand Data Vault modelling to write a query.

**Trade-offs:** Building and maintaining two modelling layers (Vault + star marts) for regulatory domains specifically is more work than a single-layer approach, accepted specifically because the compliance consequence of a traceability gap is judged to outweigh the added modelling effort — this is a domain-specific decision, not applied platform-wide (finance/operational reporting uses star schema alone, per Round 4).

**Consequences:** Only regulatory-sensitive domains carry this dual-layer complexity — this decision must be scoped deliberately per domain (Round 4), not applied as a blanket platform standard, or the added complexity becomes unjustified overhead everywhere.

---

## ADR-006: Pub/Sub + Dataflow for Green-Field Streaming, With an Explicit Kafka Exception

**Context:** The bank may have an existing on-premise Kafka estate; new streaming requirements need a GCP-native ingestion approach.

**Decision:** Pub/Sub + Dataflow is the default for new, green-field streaming pipelines. Where an existing Kafka-dependent estate is being migrated rather than newly built, Confluent Kafka on GCP is evaluated as the lower-migration-risk alternative.

**Alternatives Considered:**
- **Kafka everywhere, including new green-field pipelines** — preserves consistency with any existing estate, but takes on Kafka's operational overhead (cluster management, MirrorMaker-based DR) for pipelines that have no existing dependency forcing that choice.
- **Pub/Sub everywhere, including migrating existing Kafka-dependent consumers** — underestimates the real cost of re-architecting every existing Kafka producer/consumer application, a cost easy to dismiss on a slide but expensive in practice.

**Trade-offs:** Running two different streaming technologies (Pub/Sub for green-field, Kafka for migrated estates) during a transition period is less architecturally uniform than picking one platform-wide, but avoids both extremes — forcing unnecessary Kafka operational overhead onto new pipelines, or forcing an expensive, risky re-architecture onto existing ones.

**Consequences:** This is explicitly a transitional decision, not a permanent dual-stack — the platform should have a defined evaluation point (stated in the ingestion-design rounds) for whether to eventually consolidate onto one streaming technology once the estate stabilises on GCP.

---

## ADR-007: Separate Service Accounts and IAM Boundaries Per Pipeline Stage

**Context:** A single compromised or misconfigured credential should never be able to both corrupt the immutable raw record and exfiltrate sensitive regulatory data.

**Decision:** Every pipeline stage (ingestion, DQ validation, transformation, curated load, regulatory mart load, BI query) uses a distinct, narrowly-scoped service account, with no single account holding both broad read access to sensitive marts and write access to raw.

**Alternatives Considered:**
- **A single broad pipeline service account** — operationally simpler to set up and manage, but creates an unacceptable blast radius if ever compromised — rejected specifically because the consequence (both data corruption and exfiltration capability in one credential) is judged too severe for a regulated banking platform.

**Trade-offs:** Managing many narrowly-scoped service accounts is more operational overhead (more IAM bindings to maintain, more Terraform resources) than a single shared account, accepted as a direct, necessary cost of genuine blast-radius containment.

**Consequences:** Terraform-as-code (Round 10) is what makes this operational overhead manageable at scale — without infrastructure as code, this many narrowly-scoped service accounts would be genuinely difficult to maintain consistently, which is worth naming as a connection between two separate architectural decisions reinforcing each other.

---

## ADR-008: VPC Service Controls Around All Data Services

**Context:** IAM alone prevents unauthorized access but does not prevent a legitimately authorized user from exfiltrating data outside the approved boundary.

**Decision:** VPC Service Controls establishes a perimeter around BigQuery, GCS, and related data services, blocking data movement outside the perimeter regardless of the acting identity's IAM permissions.

**Alternatives Considered:**
- **Rely on IAM and audit logging alone** — audit logging tells you *after the fact* that exfiltration happened; it doesn't *prevent* it. Rejected as insufficient for a threat model that specifically includes legitimate-access misuse, not just unauthorized access.

**Trade-offs:** VPC-SC adds genuine operational complexity — perimeter configuration must be carefully managed to avoid inadvertently blocking legitimate cross-service data flows the pipeline itself depends on (a real, non-trivial configuration risk) — accepted because the alternative (no structural prevention of authorized-access exfiltration) is judged a more serious gap for a banking platform.

**Consequences:** Perimeter changes must go through the same Terraform-reviewed change process as other infrastructure (Round 10), since a misconfigured perimeter can either fail open (security gap) or fail closed (breaking legitimate pipeline data flows) — this makes VPC-SC configuration a genuinely high-stakes category of infrastructure change requiring particular care in review.

---

## ADR-009: Crypto-Shredding to Resolve the GDPR Erasure vs. Immutable Raw Tension

**Context:** Raw data is architecturally immutable (ADR-003) for regulatory reconstruction, but GDPR grants individuals a right to erasure — two requirements in direct, real tension.

**Decision:** Data with a legitimate regulatory retention basis (transaction records supporting BCBS 239/PRA reconstruction) relies on GDPR's legal-obligation exemption to override erasure requests. For genuinely erasable personal data without that override, per-customer encryption keys enable crypto-shredding — destroying the key renders the data permanently unreadable without rewriting immutable files.

**Alternatives Considered:**
- **Physically delete/rewrite raw files on erasure request** — directly violates the immutability property the entire regulatory reconstruction architecture depends on (ADR-003); rejected outright as incompatible with the platform's foundational design.
- **Ignore the tension, treat all data as retention-exempt** — legally indefensible; GDPR's legal-obligation exemption applies only to data genuinely covered by a specific retention obligation, not blanket immunity for all banking data.

**Trade-offs:** Crypto-shredding adds real complexity — per-customer key management, and the operational discipline of ensuring genuinely erasable data fields are actually encrypted with erasable keys from the point of ingestion, not retrofitted later. This complexity is accepted because the alternative (an unresolved legal tension discovered only when an actual erasure request or regulatory audit forces the question) is a far worse position to be in.

**Consequences:** This decision must be made explicit and designed in from the point of raw data ingestion — classifying which fields get per-customer erasable keys versus which fall under retention override is a data-classification decision (Round 6) that has to happen upfront, not be retrofitted after the raw layer already exists in production.

---

## ADR-010: Domain-Differentiated Disaster Recovery Posture, Not Uniform RPO=0

**Context:** RPO=0, dual-region active-active DR provides the strongest protection but at real, ongoing infrastructure cost; not every data domain's business/regulatory consequence of loss justifies that cost uniformly.

**Decision:** Payment/transactional data and regulatory marts use active-active dual-region DR (RPO=0). Other analytical/curated domains use a lighter posture (RPO ~15 minutes) via cross-region replication and Terraform-driven redeployment rather than continuous dual-active infrastructure.

**Alternatives Considered:**
- **Uniform RPO=0 for the entire platform** — technically the strongest protection, but a genuinely unjustified, ongoing cost for domains where a 15-minute RPO carries no meaningful business or regulatory consequence — rejected as over-engineering driven by "more protection is always better" rather than by actual requirement.
- **Uniform lighter DR posture for the entire platform, including regulatory data** — rejected specifically because regulatory data's loss has direct compliance consequence regardless of its position downstream in the pipeline (established in Round 10) — a cost-cutting choice that would create real regulatory risk.

**Trade-offs:** A domain-differentiated DR posture requires more nuanced architecture and operational runbooks (different recovery procedures per domain) than a single uniform posture, accepted because it correctly matches cost to actual business/regulatory consequence rather than either over- or under-protecting uniformly.

**Consequences:** Every new data domain onboarded to the platform requires an explicit RPO/RTO classification decision at design time (tied to its business/regulatory consequence of loss, not just "is it derived or source data") — this must be a standing step in the onboarding process, not assumed by default.

---

## ADR-011: Dataplex-Enforced Column Classification Over Manual Per-Report Masking

**Context:** PII and confidential data must be consistently masked across every report, dashboard, and query touching it, without relying on each report builder remembering to apply masking individually.

**Decision:** Column-level classification (Dataplex policy tags) is applied once at the column level and automatically enforced by BigQuery for any consuming query or report — not implemented as per-report masking logic maintained independently by each downstream consumer.

**Alternatives Considered:**
- **Per-report/per-dashboard masking logic** — gives each report builder direct control, but creates a real, likely inconsistency risk — a new report someone forgets to apply masking to becomes an unintended PII exposure, discovered only after the fact.

**Trade-offs:** Centralised policy-tag enforcement requires upfront classification discipline (every new PII column must be tagged before it reaches curated) — a real process dependency — but this is judged far more reliable than trusting every future report builder, indefinitely, to remember manual masking correctly.

**Consequences:** Column classification becomes a required gate before promotion to curated (Round 6), meaning new pipeline development cannot skip this step under delivery pressure without deliberately bypassing a policy control — this needs to be enforced as a genuine gate, not a guideline easily skipped when a deadline is tight.

---

## ADR-012: Batch as the Default, Streaming Reserved for Genuine Sub-Minute Freshness Needs

**Context:** Streaming architectures are available and technically capable for every data source, but carry continuous compute cost (Round 9) and additional architectural complexity (ordering, watermarks, exactly-once handling) that isn't free.

**Decision:** Batch processing is the default choice for any source without a genuine sub-minute freshness business requirement. Streaming is reserved specifically for use cases like fraud detection and real-time balance updates, where the freshness requirement is a genuine, named business need, not an assumed default.

**Alternatives Considered:**
- **Streaming by default for all sources, on the basis that it's more capable and future-proof** — rejected explicitly (named directly in Round 2 and revisited with cost quantification in Round 9) as a common architectural over-engineering trap — streaming's continuous cost and added complexity are only justified when the freshness requirement genuinely demands it.

**Trade-offs:** Batch introduces inherent latency (data available only after the next scheduled run, not continuously) that would be unacceptable for genuinely time-sensitive use cases — this is why the decision is explicitly conditional (batch *unless* a genuine need exists), not a blanket anti-streaming stance.

**Consequences:** Every new ingestion requirement must be evaluated against an explicit freshness-requirement question at design time, using the decision framework established in Round 2 — defaulting to whichever pattern is more familiar or impressive-sounding to the engineer proposing it is explicitly the failure mode this ADR exists to prevent.

---

## Interview Talking Points — Quick Reference

1. **Every ADR should be defensible as a trade-off, not a default** — practice stating the alternative you rejected and *why*, out loud, not just the choice you made.
2. **ADR-003 (GCS as system of record) is foundational** — nearly every other decision in this series traces back to it; use this to show you understand how architecture decisions cascade, not just that you can list them.
3. **ADR-009 (crypto-shredding) is a genuinely sharp, memorable answer** — few candidates have a resolved position on the GDPR-vs-immutability tension; having this ready is a real differentiator.
4. **ADR-010 and ADR-012 both make the same underlying point from different angles** — match protection/complexity level to actual business consequence, not to "more is always better." Naming this as a recurring principle across two ADRs shows genuine, internalised architectural judgement.
5. **Be ready for "why not just do X everywhere" follow-ups** — nearly every ADR here explicitly rejects a "uniform, one-size-fits-all" alternative in favour of a differentiated approach; this pattern itself is worth naming as a general design philosophy if asked directly.

---

## Next Rounds (Planned)

- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 11 of ongoing Lloyds Data Architect interview preparation series.*
