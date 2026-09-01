# Phase 2 Preliminary Plan: Reported-Event Interval, Exposure, and Geography Construction

## Status

**Draft for team discussion — blocked by Phase 1 remediation.**

This is the canonical preliminary Phase 2 plan, but it is not active and does
not authorize implementation. The active implementation plan remains
[Phase 1 Remediation: Auditable Incident Deduplication](01a-incident-deduplication-remediation.md).

Phase 2 may become active only after Phase 1 remediation passes, every blocking
placeholder in this document is resolved, the team reviews the completed plan,
and the [modeling roadmap](../modeling-roadmap.md) is updated to make Phase 2
active.

## Plain-Language Briefing

Phase 2 is the bridge between a trustworthy incident ledger and later predictive
modeling. It will turn the remediated Phase 1 reported incidents into explicit
time, label, exposure, and geographic data contracts.

For a requested region and a crossing cohort defined from prior history, Phase 2
will determine which time intervals are eligible observations, whether each
interval contains an observed report, and which intervals remain unknown because
of source-coverage or timing uncertainty. It will also establish reproducible
crossing-to-region membership and determine whether one-, two-, or four-hour
prediction units are supportable.

Phase 2 will not select the production hotspot cohort or fit a predictive model.
Phase 3 will select and evaluate the regional monitoring cohort. Phase 4 will fit
the pooled timing and geographic baselines.

## Objective

Produce reproducible, leakage-safe data contracts and generated tables that:

1. Preserve the Phase 1 incident and source-report lineage.
2. Represent observed reports, no observed reports, and uncertainty without
   claiming verified physical blockage status.
3. Support one-, two-, and four-hour candidate prediction units.
4. Assign crossings to versioned regions through a reusable many-to-many
   crosswalk.
5. Generate detailed exposure only after requested-region and past-only cohort
   filtering.
6. Provide the evidence required to resolve every Phase 2 decision gate in the
   docs/modeling-roadmap.md in this repository.

## Claim Boundary

The source data observe reports, not every physical blockage. Phase 2 must use
only these primary claim-safe labels:

- `report_observed`: at least one canonical reported incident is assigned to the
  interval under the primary point-report rule.
- `no_report_observed`: the interval is within a demonstrated source-coverage
  period and no canonical reported incident is assigned to it.
- `unknown`: source coverage or timestamp semantics do not support either of the
  preceding claims.

`no_report_observed` must never be described as `unblocked`, a verified negative,
or proof that no physical blockage occurred. Model probabilities created in later
phases will refer to future observed reports under the available reporting
process, not calibrated probabilities of actual blockage.

Duration-overlap results are sensitivity outputs, not additional independent
reported incidents. Temporal duplicate candidates and documented exceptions must
remain visible in uncertainty diagnostics.

## Activation Prerequisites

Before this plan can be finalized or activated:

1. Every acceptance criterion in the Phase 1 remediation plan must pass.
2. The deterministic Phase 1 review sample must be labeled and summarized.
3. `phase_1_gate_report.json` must contain the evidence required for the
   interval-resolution and uncertainty analyses.
4. The accepted Phase 1 ruleset, run manifest, artifact fingerprints, and
   remaining candidate or exception counts must be recorded in this plan.
5. Every blocking placeholder below must be resolved without guessing.

### Required Phase 1 inputs

The accepted Phase 1 run under `analysis_outputs/deduplication/v2/` must provide:

1. `source_reports_with_ids.parquet`
2. `reported_incidents.parquet`
3. `report_incident_crosswalk.parquet`
4. `documented_exceptions.parquet`
5. `duplicate_candidates.parquet`
6. `candidate_review_sample.csv`
7. `inventory_profile.json`
8. `deduplication_summary.json`
9. `diagnostics_by_year.csv`
10. `diagnostics_by_crossing.csv`
11. `timestamp_granularity_by_year.csv`
12. `phase_1_gate_report.json`
13. `run_manifest.json`

Phase 2 must validate the manifest, artifact fingerprints, ruleset version, and
Phase 1 completion status before constructing any interval or exposure output. A
missing, mismatched, or non-passing dependency must stop the run with a clear
message.

The current Form 71 candidate input is
`data/Crossing_Inventory_Source_Data_(Form_71)_-_Current_20260707.csv`. Its
fingerprint, source version, revision-field treatment, and role in geographic
assignment must be recorded before use. A later reviewed inventory version may
replace it through configuration; it must not be silently substituted.

Note: once the MPO dataset is known, name it here as a required Phase 2 geographic input with description of owner, path/URL or retrieval process.

## Blocking Placeholders

These entries record Phase 2 decisions (per docs/modeling-roadmap.md "## Phase Decision Gates" section) that require Phase 1 results or additional source
research. They are deliberately unresolved in this preliminary plan.

| ID | Blocking decision | Required evidence | Current status |
|---|---|---|---|
| BP-01 | Accepted Phase 1 run and ruleset | Passing gate report, manifest, artifact fingerprints, and labeled review summary | **Unresolved — Phase 1 remediation pending** |
| BP-02 | Demonstrated source-coverage periods | Source-system evidence and Phase 1 date diagnostics sufficient to distinguish covered from unknown periods | **Unresolved — do not generate `no_report_observed` labels** |
| BP-03 | Timestamp semantics and local-time presentation policy | Phase 1 timestamp profile and source documentation; any required geographic time-zone mapping and daylight-saving-time presentation policy | **Partially resolved — source timestamps are UTC, based on an electronic communication from the FRA data owner dated September 1, 2026; do not assume exact event start** |
| BP-04 | Selected prediction interval | One-, two-, and four-hour comparison using accepted incidents, coverage, uncertainty, and table-size diagnostics | **Unresolved — hourly is not pre-approved** |
| BP-05 | Adjacent-interval uncertainty treatment | Point-label, duration-overlap, timestamp-boundary, candidate-group, and exception sensitivity results | **Unresolved — preserve separate outputs** |
| BP-06 | Full exposure or weighted training sample | Filtered exposure size, sparsity, inclusion probabilities, computational profile, and fidelity checks | **Unresolved — future evaluation remains unsampled** |
| BP-07 | Remaining duplicate-candidate and exception effect | Accepted candidate-review results and comparison of canonical versus uncertainty-sensitive counts | **Unresolved — do not silently merge or discard** |
| BP-08 | H-GAC boundary or membership source | Authoritative provider, exact version or effective date, usage terms, assignment method, and coverage QA | **Unresolved — H-GAC is a candidate only** |

## Proposed Implementation Structure

Once this plan is activated, use the existing repository pattern of an importable
module, versioned configuration, thin analyst-facing notebook, synthetic unit
tests, and ignored reproducible outputs:

- `analysis/reported_event_exposure.py` — deterministic processing functions and
  command-line entry point.
- `analysis/reported_event_exposure_config.json` — source, time, coverage,
  geography, candidate-unit, and output configuration.
- `analysis/phase_2_analysis.ipynb` — thin execution and diagnostic-reporting
  interface that imports the module rather than duplicating its logic.
- `unit_tests/test_reported_event_exposure.py` — synthetic tests for time,
  geography, labels, filtering, sampling, and reproducibility.

The proposed command-line shape is:

```powershell
python analysis/reported_event_exposure.py `
  --phase-1-dir analysis_outputs/deduplication/v2 `
  --config analysis/reported_event_exposure_config.json `
  --output-dir analysis_outputs/reported_event_exposure/v1
```

The module and command line must use the same code path. The notebook must derive
all counts, tables, conclusions, and gate statements from the returned results or
generated artifacts rather than hard-coded Markdown.

## Data Contracts

All identifiers, enumeration values, interval conventions, and field types must
be recorded in the versioned configuration or output schema. Raw inputs remain
immutable.

### Source-coverage periods

The source-coverage table must contain, at minimum:

- `coverage_id`
- `source_name`
- `source_version`
- `scope_type`
- `scope_id`
- `coverage_start`
- `coverage_end`
- `time_zone`
- `coverage_status`
- `evidence_reference`

Intervals use half-open boundaries, `[interval_start, interval_end)`, and all
canonical timestamps and interval boundaries are UTC. Coverage start and end
values must use the same documented convention. Coverage may vary by time or
geography and must not be inferred solely from the earliest and latest report in
the workbook. An interval outside demonstrated coverage is `unknown`, not
`no_report_observed`.

The UTC interpretation is based on an electronic communication from the FRA data
owner dated September 1, 2026. BP-03 remains open only for timestamp semantics
and any local-time or daylight-saving-time presentation policy. If the activated
plan converts UTC timestamps for presentation or local operational use, it must
define a deterministic, documented conversion policy and handle nonexistent or
ambiguous local times explicitly.

### Region definitions

Each versioned region definition must contain, at minimum:

- `region_id`
- `region_name`
- `region_type`
- `membership_source_name`
- `membership_source_version`
- `effective_date`
- `assignment_method`

Supported region types may include state, county, city, MPO, and custom partner
regions. State, county, and city membership may use normalized Form 71 fields
when their semantics and source version are adequate. MPO and custom membership
require an authoritative boundary or membership source and, when applicable, a
documented spatial join to Form 71 coordinates.

H-GAC is the leading demonstration-region candidate, not a committed selection.
No H-GAC-specific behavior may be hard-coded into the shared pipeline.

### Crossing-to-region membership

The reusable many-to-many membership crosswalk must contain, at minimum:

- `crossing_id`
- `region_id`
- `region_type`
- `membership_source_version`
- `assignment_method`
- `assignment_status`
- `assignment_notes`

A crossing may belong to multiple regions simultaneously. Assigned, unmatched,
and ambiguous crossings must be counted separately. Boundary joins must record
the coordinate reference system and boundary predicate. Ambiguous or invalid
coordinates must enter diagnostics rather than being silently dropped or forced
into a region.

### Historical cohort input

Detailed exposure generation must accept an explicit crossing cohort and an
`eligibility_as_of` time. Every eligibility field must be derived using only
information available on or before that time.

Phase 2 will prove this interface with reviewed candidate cohorts or synthetic
fixtures. Phase 3 will define and freeze the production hotspot eligibility rule.
Phase 2 must not use future incident counts to create or revise a cohort.

### Crossing-interval exposure

The filtered exposure table must contain, at minimum:

- `interval_id`
- `crossing_id`
- `interval_start`
- `interval_end`
- `interval_unit_hours`
- `time_zone`
- `coverage_id`
- `eligibility_as_of`
- `point_label`
- `canonical_incident_count`
- `has_duration_overlap_sensitivity`
- `has_temporal_candidate_uncertainty`
- `has_exception_uncertainty`

`point_label` must contain only `report_observed`, `no_report_observed`, or
`unknown`. Canonical incident identifiers assigned to intervals must be preserved
in a separate interval-to-incident crosswalk so multiple incidents in one
interval do not destroy lineage or get miscounted as one incident.

If weighted sampling is selected, the training-sample artifact must also contain
the known inclusion probability, sampling stratum, and inverse-probability sample
weight for each retained `no_report_observed` interval. The canonical exposure
table and future evaluation population remain unsampled.

### Duration-overlap sensitivity

The primary label assigns a canonical incident to the interval containing its
accepted point timestamp. Duration categories must not be treated as verified
incident endpoints.

A separate sensitivity artifact may record intervals that could overlap an
incident under documented interpretations of its duration category. It must link
back to the canonical incident, state the interpretation used, and never count an
overlap interval as a separate incident. The final adjacent-interval policy is
blocked by BP-05.

### Configuration and provenance

The run configuration must identify:

- The accepted Phase 1 manifest and ruleset.
- Form 71 and geographic-source fingerprints and versions.
- The region definition and crossing-membership version.
- The source-coverage definition version.
- Requested region IDs.
- Cohort input and `eligibility_as_of` time.
- Candidate interval units.
- Time-zone and interval-boundary conventions.
- Sampling mode and seed, when applicable.
- Output schema version and output directory.

The run manifest must record input and configuration fingerprints, Git commit and
dirty-worktree status, software versions, execution metadata, output row counts
and fingerprints, validation results, and the status of every Phase 2 gate.
Execution timestamps and durations must not affect deterministic identifiers.

## Intended Workflow

### 1. Validate the Phase 1 handoff

Load the Phase 1 manifest and gate report before data tables. Verify the expected
ruleset, completion status, file fingerprints, schemas, row counts, and one-to-one
source-row disposition. Reject an incomplete, mismatched, or stale handoff.

### 2. Establish coverage and time semantics

Profile accepted incident timestamps by year and geography and reconcile those
results with source documentation. Define coverage periods independently of
report occurrence. Resolve the timestamp meaning, time-zone mapping, daylight-
saving-time behavior, and half-open interval boundaries before producing labels.

### 3. Construct canonical crossing and regional tables

Build one reproducible national crossing table from the approved Form 71 version
without changing the Phase 1 incident records. Preserve source revisions and
report duplicate, malformed, unmatched, or ambiguous crossing IDs.

Build versioned region definitions and the many-to-many membership crosswalk from
normalized inventory fields or authoritative boundary or membership data. Do not
create region-specific pipeline forks.

### 4. Apply region and historical-cohort filtering

Select requested regions through the versioned crosswalk, then apply the explicit
past-only cohort and coverage window. Record counts after each filter and reasons
for exclusions.

The exposure generator must accept only the resulting crossing set and applicable
coverage periods. It must not first materialize all national crossing-hours.

### 5. Evaluate candidate interval units

Construct comparable one-, two-, and four-hour candidate observations for the
same requested scope. For each unit, report:

- Total covered, unknown, and report-observed intervals.
- Natural report prevalence.
- Distinct incidents and intervals containing multiple incidents.
- Incidents near interval boundaries.
- Point-label and duration-overlap differences.
- Temporal-candidate and exception sensitivity.
- Counts by year, geography, crossing, and crossing-volume tier.
- Estimated full-exposure rows, storage, memory, and generation time.

The gate report must select one production interval unit or document a blocker.
Hourly resolution is acceptable only if timestamp semantics, incident density,
uncertainty, and operational usefulness support it.

### 6. Construct primary and sensitivity labels

Create primary point-report labels from canonical incidents and coverage-aware
`no_report_observed` or `unknown` labels. Preserve multiple-incident counts and
lineage.

Create duration-overlap, temporal-candidate, and exception sensitivity outputs
separately. Compare them with the primary labels and document which adjacent
intervals remain uncertain. Do not silently overwrite primary labels.

### 7. Compare exposure strategies

Measure the filtered full exposure before choosing a training strategy. Compare:

1. The full filtered exposure table.
2. A reproducible sample that retains every `report_observed` interval and samples
   `no_report_observed` intervals with recorded inclusion probabilities and
   inverse-probability weights.

The comparison must confirm that weighted summaries reproduce the full filtered
population within predeclared tolerances across time, geography, and crossing-
volume strata. Model evaluation in later phases must always use the unsampled
future population.

### 8. Produce diagnostics and the gate report

Generate human-readable notebook summaries and machine-readable diagnostics. The
Phase 2 gate report must record the decision, evidence, status, and unresolved
risks for every item under `After Phase 2` in the roadmap.

## Generated Artifacts

Write new results under `analysis_outputs/reported_event_exposure/v1/`. Do not
delete or overwrite Phase 1 artifacts or commit generated data as new source
data.

Expected outputs are:

1. `canonical_crossings.parquet`
2. `source_coverage_periods.parquet`
3. `region_definitions.json`
4. `crossing_region_membership.parquet`
5. `region_assignment_diagnostics.csv`
6. `interval_exposure.parquet`
7. `interval_incident_crosswalk.parquet`
8. `duration_overlap_sensitivity.parquet`
9. `uncertainty_diagnostics.csv`
10. `interval_unit_comparison.csv`
11. `exposure_strategy_comparison.json`
12. `phase_2_gate_report.json`
13. `run_manifest.json`

Only the selected requested-region and historical-cohort exposure may be written
as the primary `interval_exposure.parquet`. Candidate-unit comparisons should use
on-demand generation or scoped temporary data rather than save national crossing-
hour tables.

## Automated Tests

Synthetic tests must cover at least:

- Phase 1 gate, manifest, fingerprint, schema, and row-count failures.
- One-, two-, and four-hour half-open interval boundaries.
- Reports exactly on, immediately before, and immediately after a boundary.
- Multiple canonical incidents at one crossing in one interval.
- Covered intervals with no report receiving `no_report_observed`.
- Uncovered or ambiguous intervals receiving `unknown`.
- Point-report labels remaining separate from duration-overlap sensitivity.
- Temporal candidates and documented exceptions appearing in uncertainty
  diagnostics without changing canonical assignments.
- Missing, invalid, and ambiguous crossing IDs and coordinates.
- Many-to-many region membership and crossings on region boundaries.
- Unmatched crossings remaining visible in diagnostics.
- Region filtering occurring before interval materialization.
- Cohort eligibility excluding information after `eligibility_as_of`.
- Deterministic sampling, inclusion probabilities, and sample weights.
- Weighted sample summaries reproducing full-population fixture summaries within
  declared tolerances.
- Stable interval, region-membership, and manifest identifiers across repeated
  runs and reordered in-memory input.

Real-data validation, once authorized, must include:

1. Two identical runs with matching deterministic output fingerprints.
2. Reconciliation of Phase 2 incident totals to the accepted Phase 1 incident
   and exception artifacts.
3. Coverage, label, uncertainty, and geography summaries by year and region.
4. Confirmation that raw inputs and Phase 1 outputs were not modified.
5. A clean notebook run from a fresh kernel with no stale counts or conclusions.

## Phase 2 Decision Gates

Phase 2 is complete only when the evidence supports and the gate report records:

1. The selected hourly or coarser prediction interval.
2. The treatment of uncertain adjacent intervals.
3. Full filtered exposure or weighted `no_report_observed` sampling for training.
4. The accepted region-configuration and geographic-crosswalk contracts.
5. Authoritative, versioned sources for each initial demonstration region.
6. Proof that requested-region and past-only cohort filtering occurs before
   detailed exposure generation.

If any gate cannot be resolved, Phase 2 must report the blocker and must not be
marked complete.

## Acceptance Criteria

Phase 2 implementation may be marked complete only when:

- Phase 1 prerequisites and every blocking placeholder have been resolved.
- All automated and authorized real-data validation checks pass.
- Every reported incident remains traceable to Phase 1 and every assigned
  interval remains traceable to its incidents and coverage definition.
- `report_observed`, `no_report_observed`, and `unknown` are applied consistently
  without claims of confirmed physical blockage status.
- Point labels and duration-overlap sensitivity remain separate.
- Requested regions can be reproduced from versioned definitions and membership
  sources, with unmatched and ambiguous crossings reported.
- One-, two-, and four-hour units have been compared and the selected unit is
  justified by current evidence.
- Training-exposure sampling, if selected, has known probabilities and weights;
  future evaluation remains unsampled.
- Exposure is generated after requested-region and past-only cohort filtering,
  not across all national crossing-hours.
- Two runs produce identical deterministic artifacts and manifests except for
  permitted execution metadata.
- Raw source files, Phase 1 artifacts, and archived legacy artifacts remain
  unchanged.
- The notebook contains no hard-coded results or stale claims.
- The roadmap is updated with the Phase 2 decisions before a Phase 3 plan is
  finalized.

## Out of Scope

- Implementing or revising Phase 1 deduplication.
- Treating the reconciliation workbook as an independent report source.
- Inferring unreported physical blockage events or verified unblocked intervals.
- Selecting or freezing the production hotspot cohort.
- Feature engineering for the predictive timing model.
- Model fitting, tuning, calibration, threshold selection, or deployment.
- Creating region-specific forks of the shared pipeline.
- Treating the archived Houston duration-severity prototype as a current model or
  Phase 2 input.
