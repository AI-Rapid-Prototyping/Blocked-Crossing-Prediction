# Phase 1 Plan: Source Audit and Incident Deduplication

## Status

Initial implementation reviewed; remediation required. The active plan is
[Phase 1 Remediation: Auditable Incident Deduplication](01a-incident-deduplication-remediation.md).
This original document remains the Phase 1 requirements baseline and does not
authorize later modeling phases or changes to raw data.

Parent roadmap: [Blocked-Crossing Modeling Roadmap](../modeling-roadmap.md)

## Objective

Transform the authoritative 2020–2025 report workbook into an auditable table of
candidate distinct reported incidents while preserving the relationship to every
source row.

This phase consolidates duplicate reports. It does not verify actual blockages,
infer unreported events, label unreported intervals as unblocked, select hotspot
crossings, or train predictive models.

## Inputs

### Authoritative source

- `data/blocked_crossings_2020through2025.xlsx`

This workbook is the only report source used to construct Phase 1 incidents. It
must be read without modification.

### Reconciliation source

- `data/blocked_crossings_2025.xlsx`

The 2025 workbook is used only to determine whether it is a subset, alternate
export, or conflicting representation of 2025 rows in the authoritative source.
It must not be appended to the authoritative workbook during this phase.

### Out-of-scope legacy sources

- `archive/houston-reported-blockage-duration-prototype/reports.xlsx`
- `archive/houston-reported-blockage-duration-prototype/cleaned_houston_data.csv`
- Scripts and generated outputs preserved with that archived prototype

## Deliverables

The implementation phase should produce reproducible, ignored artifacts under a
dedicated output directory such as `analysis_outputs/deduplication/`:

1. `source_reports_with_ids` — normalized source rows with stable source-row IDs.
2. `reported_incidents` — one row per deduplicated candidate incident.
3. `report_incident_crosswalk` — one row per source report mapping it to an
   incident ID or documented exception.
4. `duplicate_groups_for_review` — unresolved or lower-confidence groups requiring
   review.
5. `deduplication_summary` — counts and rates before and after each rule.
6. A machine-readable run manifest containing input fingerprints, configuration,
   execution time, and row counts.

Exact file formats will be selected during implementation. Parquet is preferred
for full row-level tables; CSV or JSON is appropriate for compact summaries and
review queues.

No generated row-level data should be committed without a separate decision about
size, provenance, and data governance.

## Work Breakdown

### 1. Inventory and profile the source

- Record workbook name, sheet names, file fingerprint, row count, and column
  names.
- Establish a stable `source_row_id` using the source file identity, sheet, and
  original row number or another demonstrated stable key.
- Profile missingness, invalid timestamps, malformed crossing IDs, duration
  values, reasons, states, and other fields used for deduplication.
- Quantify exact duplicate rows before normalization and after non-destructive
  normalization.
- Preserve original values alongside normalized comparison fields where values
  change.

### 2. Reconcile the 2025 workbook

- Compare file metadata, schemas, row counts, and stable row signatures.
- Report rows present only in the authoritative source, only in the 2025
  workbook, and in both.
- Investigate timestamp, text-normalization, and export-format differences before
  declaring mismatched rows unique.
- Document whether the 2025 workbook is a complete subset, partial subset, or
  conflicting export.
- Do not merge reconciliation-only rows into the authoritative dataset during
  this phase.

### 3. Define non-destructive normalization

Candidate comparison fields may include:

- Trimmed and consistently formatted crossing IDs.
- Parsed timestamps with the original timestamp retained.
- Normalized whitespace and case for selected categorical or text fields.
- Standardized missing-value representations.
- Parsed duration categories without converting them into unsupported exact
  durations.

Normalization rules must not silently correct uncertain crossing IDs, timestamps,
or report content.

### 4. Establish duplicate-rule tiers

Rules should be conservative, deterministic, ordered, and separately reported.
Initial tiers to evaluate are:

1. **Exact duplicate:** All material source fields match.
2. **Normalized exact duplicate:** Material fields match after documented
   whitespace, case, ID, or timestamp normalization.
3. **Probable duplicate:** The same crossing has reports close in time with
   compatible duration, reason, and contextual fields.
4. **Unresolved candidate:** Some evidence suggests duplication, but automatic
   consolidation would risk merging distinct incidents.
5. **Distinct report:** No rule links the report to another report.

The implementation plan must define the exact material fields, time-window
logic, missing-value behavior, and rule precedence from profiling evidence. No
probable-duplicate time threshold should be chosen before timestamp and duration
distributions are inspected.

Reports must never be grouped across different normalized crossing IDs without a
separately documented crossing-ID correction rule and supporting evidence.

### 5. Construct stable incident groups

- Apply rules in documented precedence order.
- Use a deterministic incident ID that remains stable when the same inputs and
  configuration are rerun.
- Retain group size, rule tier, confidence or review status, earliest and latest
  report timestamps, and all contributing source-row IDs.
- Define the representative incident timestamp without discarding the source
  timestamp range.
- Preserve conflicting attributes or summarize them explicitly rather than
  selecting an arbitrary value.
- Treat invalid or insufficiently identified rows as documented exceptions, not
  silent deletions.

### 6. Validate and review

- Verify that every authoritative source row appears exactly once in the
  crosswalk or exception table.
- Confirm that incident groups do not cross normalized crossing IDs.
- Review all rule tiers using targeted samples, with larger or complete review
  for probable and unresolved groups where feasible.
- Inspect high-volume crossings separately because dense event histories create
  the greatest false-merge risk.
- Compare incident counts and duplicate rates by year, state, crossing, reason,
  and rule tier.
- Run the process twice and confirm identical IDs, group assignments, and
  summaries.

### 7. Document findings and the Phase 2 gate

The completion report must state:

- Source and reconciled workbook coverage.
- Counts of source reports, incidents, exceptions, and duplicate groups by tier.
- Timestamp semantics supported by the available documentation and data.
- Duration limitations relevant to interval construction.
- Distinct-incident distributions by crossing and time period.
- Remaining uncertainty and any manual-review backlog.
- Whether the evidence supports designing hourly reported-event intervals in
  Phase 2.

## Required Tests

Automated tests should cover at least:

- Exact duplicate reports.
- Normalized exact duplicates involving whitespace, case, and crossing-ID
  formatting.
- Multiple reports at one crossing within a probable-duplicate window.
- Nearby reports that must remain distinct.
- Reports at different crossings with otherwise identical values.
- Missing or invalid crossing IDs.
- Missing, invalid, and boundary timestamps.
- Conflicting reasons or durations within a candidate group.
- Stable incident IDs across repeated runs.
- One-to-one source-row coverage in the crosswalk.
- Reconciliation rows appearing in both workbooks or only one workbook.

Tests should use small synthetic fixtures. They must not require modifying or
committing derived copies of the source workbooks.

## Acceptance Criteria

Phase 1 is complete only when:

- Raw workbooks are unchanged.
- Input fingerprints and source-row identities are recorded.
- Duplicate rules and their precedence are explicit and versioned.
- Every authoritative source row maps to exactly one incident or documented
  exception.
- Exact and normalized-exact duplicates are handled deterministically.
- Probable and unresolved groups are separately visible and reviewable.
- No incident group silently combines different crossing IDs.
- Rerunning the process with identical inputs produces identical assignments and
  summaries.
- Before-and-after diagnostics are available by year, state, crossing, and rule
  tier.
- Required automated tests pass.
- Timestamp and duration findings are sufficient to make the Phase 2 resolution
  decision, or the blocker is explicitly documented.
- No hotspot selection, negative-hour generation, or predictive modeling has been
  added.

## Risks and Controls

| Risk | Control |
|---|---|
| Multiple public reports describe one blockage | Preserve all reports in the crosswalk and consolidate only under explicit rules |
| Two distinct events occur close together at a habitual crossing | Use conservative matching, contextual fields, and focused review of dense crossings |
| Timestamp represents reporting rather than event start | Preserve raw timestamps and defer interval assumptions to Phase 2 |
| Duration is categorical or uncertain | Retain the original category and avoid unsupported exact interval endpoints |
| The 2025 workbook overlaps the authoritative workbook | Reconcile but do not append it |
| Normalization changes source meaning | Preserve original fields and expose every normalization rule |
| Rule changes destabilize incident IDs | Version configuration and record it in the run manifest |

## Out of Scope

This phase does not include:

- Confirming reported incidents against physical sensors or operational records.
- Estimating unreported incidents.
- Treating missing reports as verified negative observations.
- Selecting hotspot thresholds or monitored crossings.
- Constructing hourly, daily, or other exposure grids.
- Negative sampling.
- Feature engineering for prediction.
- Model training, tuning, evaluation, dashboards, or deployment.

## Phase 2 Handoff Questions

Phase 2 planning should not begin until Phase 1 can answer:

1. What does `Date/Time` represent, and how precise is it?
2. Can duration fields support an interval-overlap label, and with what
   uncertainty?
3. How many distinct incidents exist per crossing and evaluation period?
4. Which candidate groups remain unresolved?
5. Is hourly resolution supportable, or should coarser intervals be evaluated
   first?
6. Which intervals can be called `no_report_observed`, and which should remain
   unknown because of data coverage or timing ambiguity?
