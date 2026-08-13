# Principal Data Architect Study Guide — Round 7
## Security Architecture (Google Cloud Platform)
### Enterprise Banking Data Platform — Lloyds Interview Prep

---

## Part A: The Security Design Principle Before Any Control

A weak answer lists controls (IAM, encryption, VPC-SC) as a checklist. A Principal Architect frames security as answering one question repeatedly, at every layer: **"If this specific control failed or was bypassed, what's the blast radius, and what's the next layer that still contains it?"** This is defence-in-depth as a design discipline, not a slogan — every control below exists because it assumes the control before it might fail.

---

## Part B: Identity and Access Management

### B.1 Service Account Design — Per-Stage, Not Per-Project

The common anti-pattern: one broad service account with wide permissions used across an entire pipeline, because it's operationally simpler to set up. The correct pattern for this platform: **a distinct service account per pipeline stage**, each scoped to only what that stage needs.

```
sa-ingestion-landing        → Write: landing bucket only. No read access elsewhere.
sa-ingestion-raw-promoter   → Read: landing. Write: raw. Cannot write to curated.
sa-dq-validator             → Read: raw/validated. Write: quarantine, dq_failures table.
sa-transform-curated        → Read: validated. Write: curated. Cannot write to raw.
sa-regulatory-mart-loader   → Read: curated (specific datasets only). Write: marts_regulatory.
sa-bi-query-runner          → Read: curated, marts_finance, marts_risk. No write access anywhere.
```

**Why this matters beyond "least privilege" as a slogan:** if `sa-transform-curated` is ever compromised (a leaked key, a misconfigured Dataflow job with an injected dependency), the blast radius is bounded — it cannot write to raw (protecting the immutable regulatory record established in Round 2/3) and cannot read regulatory marts it has no business touching. Each service account boundary is a genuine containment boundary, not a bureaucratic label.

**Concrete rule for the interview:** *"No service account used by an ingestion or transformation job ever has both read access to a sensitive downstream mart and write access to raw. That combination would let a single compromised credential both exfiltrate regulatory data and corrupt the system of record — I explicitly design service account boundaries so no single credential can do both."*

---

### B.2 IAM at the Resource Level, Not Just Project Level

Project-level IAM roles (e.g., `BigQuery Data Viewer` at the project) are too coarse for this platform — they'd grant visibility across regulatory, finance, and general analytical datasets uniformly. Instead, IAM bindings are applied at the **dataset level** (and, via authorized views/BigLake row-column security, at finer granularity still):

- `marts_regulatory` dataset: IAM restricted to a named regulatory reporting group + the specific service accounts that load it. General analysts have zero access, not "read but masked" — some regulatory data shouldn't be visible to general analysts at all, not just partially visible.
- `curated_payments`, `curated_lending` etc.: broader analyst group access, but still gated behind column-level policy tags (Round 6) for PII/confidential columns within those datasets.

**Design principle:** dataset boundaries encode *who*, column-level policy tags encode *what within that access, is still restricted*. Both layers are necessary — dataset-level IAM alone can't express "this analyst can see the transactions table but not the customer name column within it."

---

### B.3 Least Privilege and Just-in-Time Access

Standing broad access (a permanent "senior analyst" role with wide read access, held indefinitely) is a real audit and breach-blast-radius risk even when technically "least privilege" was followed at grant time — permissions accumulate and are rarely revoked as people change roles. For genuinely sensitive access (regulatory mart access, unmasked PII viewing), this platform uses **time-bound, approval-gated access** (via IAM Conditions with time-based expiry, or a broader access-request workflow tool) rather than permanent grants — access is requested for a specific task, expires automatically, and every grant is itself an auditable event.

---

### B.4 Separation of Duties

Directly connecting to Round 1's requirement: the people/service accounts that **build and deploy pipeline code** are not the same identities that **have query access to production regulatory data** — a data engineer with deploy permissions to the transformation pipeline should not, by default, also have standing query access to `marts_regulatory`. This prevents a single compromised or malicious identity from both altering how data is processed *and* directly viewing its most sensitive output — a real separation-of-duties control, not just a policy document.

---

## Part C: Encryption

### C.1 Encryption at Rest — Customer-Managed Encryption Keys (CMEK)

Every storage layer — GCS buckets (all zones), BigQuery datasets, Bigtable — uses **CMEK via Cloud KMS**, not Google-default encryption. This matters for two concrete reasons beyond "encryption is good practice":

- **Key ownership and control**: the bank controls key rotation policy, can revoke a key entirely (rendering all data encrypted under it permanently unreadable — a genuine incident-response capability, e.g., if a specific dataset needs to be immediately and irreversibly rendered inaccessible), and can produce key-management audit evidence independently of Google's own controls.
- **Separate keys per sensitivity tier**: `raw` (regulatory system of record), `marts_regulatory`, and general `curated` datasets each use **distinct KMS keys**, not one key for the whole platform. This means a key compromise or required key rotation in one tier doesn't require touching, re-encrypting, or risk-assessing every other tier simultaneously — genuine blast-radius containment applied to encryption itself, not just access control.

### C.2 Encryption in Transit

All data movement — Direct Connect/interconnect from on-premise sources, Dataflow-to-BigQuery writes, any API traffic — uses TLS by default across GCP's managed services. The specific design decision worth naming: **Private Google Access / Private Service Connect** ensures traffic between GCP services (Dataflow to BigQuery, Dataflow to GCS) never traverses the public internet even though it's encrypted — encryption in transit and network-path privacy are two distinct protections, and this platform applies both, not treating TLS alone as sufficient.

### C.3 Tokenisation

For specific highly sensitive fields (full account numbers, national insurance numbers) used across multiple downstream systems, **tokenisation** — replacing the real value with a non-reversible or securely-reversible token, with the mapping held in a separate, tightly-controlled vault (Cloud KMS-backed) — is used in preference to simple masking for fields that need to remain **joinable across tables** without exposing the real value. Masking (showing `****1234`) is sufficient for display-only consumption; tokenisation is necessary when a downstream process needs to consistently match the same customer's records across datasets without ever seeing their real account number — a distinction worth making explicitly, since conflating masking and tokenisation is a common, superficial answer.

---

## Part D: Network Security

### D.1 VPC Service Controls (VPC-SC)

Established in Round 1, worth deepening here: VPC-SC creates a **service perimeter** around BigQuery, GCS, and other data services such that even a correctly-authenticated, correctly-authorized identity **cannot move data outside the perimeter** — e.g., cannot export a BigQuery table to a GCS bucket in an unapproved project, cannot copy data to a personal Google account, cannot exfiltrate via `bq extract` to an external destination.

**Why this is the control that matters most for insider risk specifically:** every other control in this document (IAM, encryption, tokenisation) assumes the person accessing data is doing so legitimately within their granted permissions. VPC-SC is the control that specifically addresses **legitimate access misused** — an analyst with genuine, correctly-scoped query access who then tries to copy that data somewhere it shouldn't go. This is a distinct threat model from "unauthorized access," and a Principal Architect should name it as such explicitly in an interview — conflating "prevent unauthorized access" (IAM's job) with "prevent authorized-access data exfiltration" (VPC-SC's job) is a common gap in weaker answers.

### D.2 Private Google Access and Private Connectivity

On-premise-to-GCP traffic (Direct Connect, established in the ingestion rounds) combined with Private Google Access ensures pipeline traffic never has a public-internet-routable path, even transiently. Internal GCP-to-GCP service communication similarly stays on Google's private network backbone rather than public endpoints — relevant specifically because a misconfigured public endpoint is a common, avoidable source of real-world data exposure incidents industry-wide, not a theoretical risk.

---

## Part E: Row-Level and Column-Level Security

### E.1 Column-Level Security — Recap and Extension

Established via Dataplex policy tags (Round 1/6): a column tagged `PII` or `CONFIDENTIAL` is automatically masked for any querying identity without the specific "view unmasked" permission — enforced centrally, inherited by every report/dashboard/query touching that column, regardless of who builds the downstream artifact.

### E.2 Row-Level Security

Distinct from column-level security: **row-level security** restricts *which rows* a given identity can see, not which columns. Concrete banking example: a regional operations analyst should see transaction rows for their region only, not the full national dataset, even though they have legitimate access to the `fact_transactions` table's columns generally.

Implemented via BigQuery **row-level access policies** (`CREATE ROW ACCESS POLICY`), defined declaratively against the querying identity's group membership:

```sql
CREATE ROW ACCESS POLICY region_filter
ON curated_payments.fact_transactions
GRANT TO ('group:analysts-region-north@bank.com')
FILTER USING (region = 'NORTH');
```

**Why this matters as a design decision, not just a feature to mention:** row-level security means the *same physical table* serves multiple audiences with different visibility, without maintaining separate physical copies per region/audience — directly consistent with the "single source of truth, governed views onto it" principle running through this entire architecture (Round 3's authorized views, Round 6's masking).

---

## Part F: Audit Logging

### F.1 What Gets Logged

**Cloud Audit Logs**, specifically **Data Access logs** (which must be explicitly enabled for BigQuery/GCS — they're not on by default due to volume, a genuinely important operational detail worth naming), capture every query, every read, every export — who, what, when, from where. For a bank, this log itself becomes a regulated artifact: retained for the same 7-10 year window as the data it describes, stored immutably, and itself subject to access controls (a person shouldn't be able to view or tamper with the audit log of their own data access).

### F.2 Why Audit Logs Matter Beyond "We Log Things"

The concrete regulatory scenario this serves: a regulator or internal audit asks "did anyone access this specific customer's data in the last 12 months, and if so, who and why." Without Data Access logs specifically enabled and retained, this question is unanswerable after the fact — a genuine gap, not a hypothetical one, since Data Access logs are opt-in in BigQuery/GCS specifically because of their volume and cost, meaning a platform that didn't deliberately enable them has no way to reconstruct this history.

**Interview-ready point:** *"Data Access audit logs aren't on by default in GCP because of the volume and cost involved — for a regulated banking platform, I'd explicitly enable them for every dataset containing PII or regulatory data from day one, because the alternative is discovering during an actual audit or incident investigation that you can't answer 'who accessed this' for anything that happened before you turned logging on."*

---

## Part G: Isolating Regulatory Reporting Data from General Analytical Users

This is very likely to be asked directly and specifically — bring together every control above into one coherent, layered answer.

### G.1 The Layered Isolation Design

```
Layer 1 — Dataset Boundary:
  marts_regulatory lives in a physically separate BigQuery dataset,
  not a filtered view within a shared dataset.

Layer 2 — IAM:
  Only sa-regulatory-mart-loader (write) and a named
  regulatory-reporting-team group (read) have any access.
  General analyst groups have zero grants on this dataset —
  not "read but restricted," genuinely no access.

Layer 3 — Network:
  VPC-SC perimeter includes marts_regulatory, preventing even an
  authorized regulatory-team member from exporting data outside
  the approved boundary.

Layer 4 — Encryption:
  Separate CMEK key for marts_regulatory, independent rotation
  and revocation from general curated data.

Layer 5 — Compute Isolation:
  Dedicated BigQuery reservation (Round 3, B.7) for regulatory
  workloads — general analyst query load cannot contend for the
  same slots, addressing both a performance SLA concern and a
  subtle security concern (workload isolation reduces the surface
  for a noisy or malicious analyst workload to interfere with
  regulatory processing).

Layer 6 — Audit:
  Data Access logs specifically enabled and separately monitored
  for this dataset, with alerting on any access pattern anomaly
  (e.g., a bulk export, an access outside business hours).

Layer 7 — Separation of Duties:
  The team that builds/deploys the regulatory mart pipeline is
  distinct from the team with standing query access to its output,
  per Section B.4.
```

**Interview-ready synthesis:** *"Isolating regulatory data isn't one control — it's the same defence-in-depth principle applied consistently: a separate dataset, separate IAM grants, inside the VPC-SC perimeter, encrypted with a separate key, on isolated compute, with its own audit trail, built by people who don't also have standing query access to it. Any single one of those layers being misconfigured still leaves six others standing between a general analyst and regulatory data they shouldn't see."*

---

## Part H: Design Decisions Summary Table

| Decision | Choice | Why |
|---|---|---|
| Service accounts | Per pipeline stage, narrowly scoped | Bounds blast radius of any single compromised credential |
| IAM granularity | Dataset-level + column policy tags, not project-level | Project-level is too coarse to express "this table yes, this column no" |
| Sensitive access model | Time-bound, approval-gated (not standing) | Standing broad access is a real audit/breach risk even if "least privilege" at grant time |
| Encryption keys | CMEK, separate key per sensitivity tier | Blast-radius containment applied to encryption, not just access |
| Masking vs. tokenisation | Tokenisation for joinable-but-sensitive fields; masking for display-only | Conflating the two is a superficial answer; they solve different problems |
| VPC-SC | Enabled around all data services | Specifically addresses authorized-access exfiltration, distinct from unauthorized access |
| Row-level security | BigQuery native row access policies | Single physical table serves multiple audiences without duplicated copies |
| Audit logging | Data Access logs explicitly enabled, retained long-term | Off by default in GCP; must be a deliberate day-one decision, not assumed |
| Regulatory data isolation | Seven-layer defence-in-depth (dataset, IAM, network, encryption, compute, audit, duties) | No single control is treated as sufficient on its own |

---

## Interview Talking Points — Quick Reference

1. **Frame every control by its blast-radius/containment logic**, not just "we have X control" — this is the single biggest differentiator between a checklist answer and a Principal-level answer.
2. **VPC-SC vs. IAM is a distinct threat model** — unauthorized access (IAM's job) vs. authorized-access misuse/exfiltration (VPC-SC's job). Naming this distinction explicitly signals real depth.
3. **Masking vs. tokenisation is a real, nameable distinction** — display-only restriction vs. joinable-but-protected data. Don't conflate them.
4. **Data Access logs are opt-in in GCP** — this is a specific, concrete, slightly surprising fact worth stating plainly; it shows hands-on GCP knowledge, not generic security theory.
5. **The regulatory isolation question deserves the full seven-layer answer**, not just "separate dataset with IAM" — this is very likely to come up directly, so have the layered structure memorised well enough to state fluently.
6. **Separation of duties applies to pipeline builders vs. data consumers**, not just to data access itself — a subtler point that most candidates miss.

---

## Next Rounds (Planned)

- **Round 8**: Orchestration, Error Handling & Resilience
- **Round 9**: Observability & Cost Optimisation
- **Round 10**: CI/CD & Disaster Recovery
- **Round 11**: Architecture Decision Records (10+ ADRs)
- **Round 12**: Failure Scenario Stress-Testing
- **Round 13**: Mock Interview — 25+ Principal Data Architect Questions with Live Critique

---

*Document 7 of ongoing Lloyds Data Architect interview preparation series.*
