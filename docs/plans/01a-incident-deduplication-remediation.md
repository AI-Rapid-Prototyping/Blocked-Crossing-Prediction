# Phase 1 Remediation Plan: Auditable Incident Deduplication

## Status

Active. This plan remediates the initial Phase 1 notebook implementation before
Phase 2 is designed or implemented.

Parent roadmap: [Blocked-Crossing Modeling Roadmap](../modeling-roadmap.md)

Original plan: [Phase 1: Source Audit and Incident Deduplication](01-incident-deduplication.md)

## Objective

Produce a reproducible and tested Phase 1 data foundation that conservatively
consolidates only demonstrated duplicates, preserves every source report, and
supports an evidence-based Phase 2 interval-resolution decision.

The current notebook remains useful as the analyst-facing execution and reporting
interface. Deterministic data processing will move into an importable Python
module so the same logic can be called from the notebook, tested with synthetic
fixtures, and run from the command line.

This phase still produces **candidate reported incidents**, not confirmed physical
blockage incidents. It does not infer unreported events or treat intervals without
a report as confirmed unblocked periods.

## Why Remediation Is Required

A review of `analysis/phase_1_analysis.ipynb` and its generated artifacts found
the following blocking issues. Counts below document the reviewed run and must be
recalculated after remediation.

1. **Duration normalization changes deduplication incorrectly.** The workbook
   contains 39,733 rows labeled `31-60 minutes`, while the code expects
   `31 to 60 minutes`. The fallback maps every unmapped value to 15 minutes.
2. **The exact-duplicate rule is not a full-row comparison.** It uses only
   crossing ID, timestamp, and duration. Of 4,821 reports labeled exact
   duplicates, 1,940 differ in at least one other original field. City and State columns
   should also be used in addition to the three currently used.
   Other columns can be ignored for the exact-duplicate rule.
3. **Probable and unresolved candidates are automatically merged.** The existing
   duration-window rule, its 15-minute buffer, and chained window extensions were
   not validated before use. Tier 4 candidates are merged even though their label
   says they require review.
4. **Timestamp conclusions are overstated.** FRA
   documentation supports describing the field as a user-entered reported date
   and time, not necessarily submission time or the physical start time.
5. **Required validation is incomplete.** The notebook checks three invariants
   but has no synthetic automated tests, real-data repeated-run comparison, or
   complete diagnostics by the dimensions required in the original plan.
6. **Saved evidence is inconsistent.** The notebook retains an earlier Parquet
   error and narrative counts that disagree with the most recent artifacts.
7. **Boundary and malformed values need explicit handling.** The source contains
   four malformed crossing IDs, two malformed duration strings, and 27 records
   dated January 1, 2026 despite the workbook name ending in 2025.

## Implementation Structure

### Module, configuration, and notebook

Add the following tracked files:

- `analysis/incident_deduplication.py` — importable processing functions and CLI.
- `analysis/incident_deduplication_config.json` — versioned schema, normalization,
  candidate-generation, and expected-input configuration.
- `unit_tests/test_incident_deduplication.py` — synthetic `unittest` coverage.

Refactor `analysis/phase_1_analysis.ipynb` into a thin notebook that:

1. Resolves the repository root without a computer-specific path.
2. Imports and calls the module directly from ordinary Python code cells.
3. Displays current summaries, review samples, tables, and plots.
4. Derives all narrative counts and gate conclusions from the returned results or
   generated artifacts rather than hard-coding them in Markdown.

The notebook is expected to remain the primary working interface.
Direct function imports are preferred over `%run`, shell `!python` commands, or
duplicating module code in notebook cells because imports keep notebook, tests,
and CLI behavior on the same code path.

Expose this primary Python interface:

```python
run_phase_1(
    authoritative_path: Path,
    reconciliation_path: Path,
    config_path: Path,
    output_dir: Path,
) -> Phase1Result
```

`Phase1Result` should provide generated artifact paths and compact summary and
validation dictionaries. The notebook may load full Parquet artifacts only when
it needs row-level displays.

Expose equivalent CLI arguments:

```powershell
uv run python analysis/incident_deduplication.py `
  --authoritative data/blocked_crossings_2020through2025.xlsx `
  --reconciliation data/blocked_crossings_2025.xlsx `
  --config analysis/incident_deduplication_config.json `
  --output-dir analysis_outputs/deduplication/v2
```

The module must not execute the pipeline during import.

## Configuration Contract

The checked-in JSON configuration must include:

- `ruleset_version`, initially `phase1-v2`.
- The authoritative sheet name.
- The 11 material columns:
  - `Crossing ID`
  - `City`
  - `State`
  - `Street`
  - `County`
  - `Railroad`
  - `Date/Time`
  - `Duration`
  - `Reason`
  - `Immediate Impacts`
  - `Additional Comments`
- Crossing-ID pattern `^\d{6}[A-Z]$`.
- `auto_merge_tiers` limited to `exact` and `normalized_exact`.
- Review-only proximity bands of 15, 30, 60, and 120 minutes.
- The duration contract below.

| Canonical duration | Lower minutes | Upper minutes |
|---|---:|---:|
| `0-15 minutes` | 0 | 15 |
| `16-30 minutes` | 16 | 30 |
| `31-60 minutes` | 31 | 60 |
| `1-2 hours` | 60 | 120 |
| `2-6 hours` | 120 | 360 |
| `6-12 hours` | 360 | 720 |
| `12-24 hours` | 720 | 1440 |
| `More than one day` | 1440 | null |

Map the two observed values `2-6 hours'` and `2-6 hours"` to `2-6 hours` only
through explicit aliases. Preserve their original values and mark their
normalization status as `known_alias`. An unrecognized value must retain null
bounds and status `unmapped`; it must never receive a default duration.

## Source Inventory and Normalization

1. Parse the timestamp into a parallel normalized column without overwriting the
   original value. Interpret `Date/Time` as UTC, based on an electronic
   communication from the FRA data owner dated September 1, 2026, and store the
   normalized value as a timezone-aware UTC timestamp.
2. Normalize crossing IDs by trimming and uppercasing, then validate the result.
   Invalid IDs remain in the source table but map to documented exceptions rather
   than canonical incidents.
3. Normalize comparison text by standardizing nulls, trimming leading/trailing
   whitespace, collapsing repeated whitespace, and case-folding. Preserve raw
   text in every case.
8. Retain all 27 January 1, 2026 records and flag them as
   `outside_named_source_period`; do not silently exclude or relabel them.

## Conservative Deduplication Rules

Apply the following precedence to rows with valid crossing IDs and timestamps:

1. **Exact:** all original material values match using null-aware equality, based on these
five fields: crossing ID, timestamp, duration, City, and State.
2. **Normalized exact:** all 5 normalized comparison values match.
3. **Distinct candidate incident:** every other row remains a separate candidate
   reported incident.

Do not auto-merge reports based on temporal proximity, categorical duration,
reason compatibility, or a chained active window.

Create each `canonical_incident_id` as `INC-` plus a SHA-256 prefix of the sorted
contributing source-row IDs. Record the exact ruleset version, consolidation tier,
report count, earliest and latest reported timestamps, primary source-row ID, and
all source lineage through the crosswalk. Select the primary source row
deterministically by the lowest original Excel row number.

Rows with invalid crossing IDs or timestamps receive unique documented exception
records. They count toward source coverage but not toward canonical incident
counts.

## Review-Only Temporal Candidates

After exact consolidation, compare distinct candidate incidents only when they
have the same valid normalized crossing ID.

- Use an efficient sliding time window rather than a full all-pairs join.
- Assign each pair to its smallest qualifying separation band: 15, 30, 60, or
  120 minutes.
- Retain both incident IDs, time difference, timestamps, raw and normalized
  durations, reasons, contextual fields, year, and crossing report-volume tier.
- Create deterministic connected-component candidate group IDs for navigation,
  but do not interpret membership as proof of one incident and do not change the
  canonical incident table.

Use these crossing-volume tiers, calculated from the pre-review Phase 1 history:

- `low`: 1-3 candidate incidents.
- `medium`: 4-19 candidate incidents.
- `high`: 20 or more candidate incidents.

Create a deterministic review sample of up to 300 pair rows. Allocate up to 25
rows to each combination of four proximity bands and three volume tiers. When a
stratum contains more than 25 rows, select the 25 lowest stable content hashes;
when it contains fewer, include all. Include blank `review_label` and
`review_notes` fields. Permitted labels are `same_incident`, `distinct`, and
`uncertain`.

This sample is diagnostic. Review labels must not be converted into a new
automatic merge rule during this remediation without a separate reviewed change
to the ruleset.

## 2025 Workbook Reconciliation

1. Delete the reconciliation of `blocked_crossings_2025.xlsx` with `blocked_crossings_2020through2025.xlsx` 
and use only `blocked_crossings_2020through2025.xlsx` as the authoritative source.

## Generated Artifacts

Write new results under `analysis_outputs/deduplication/v2/`. Do not delete or
overwrite the existing ignored artifacts.

Required outputs are:

1. `source_reports_with_ids.parquet`
2. `reported_incidents.parquet`
3. `report_incident_crosswalk.parquet`
4. `documented_exceptions.parquet`
5. `duplicate_candidates.parquet`
6. `candidate_review_sample.csv`
7. `inventory_profile.json`
8. `deduplication_summary.json`
9. `diagnostics_by_year.csv`
10. `diagnostics_by_state.csv`
11. `diagnostics_by_crossing.csv`
12. `diagnostics_by_reason.csv`
13. `timestamp_granularity_by_year.csv`
14. `phase_1_gate_report.json`
15. `run_manifest.json`

Every authoritative source row must appear exactly once in the crosswalk, either
with one canonical incident ID or one exception ID.

The run manifest must contain:

- Raw input paths, fingerprints, sheet names, and row counts.
- Configuration path, fingerprint, and ruleset version.
- Git commit and dirty-worktree status.
- Python and relevant package versions.
- Execution timestamp and duration.
- Output paths and row counts.
- Timestamp interpretation and its evidence reference.
- Validation results.

Execution timestamps and durations are allowed to differ between runs. They must
not participate in row IDs or incident IDs.

## Diagnostics and Gate Report

Report source, incident, duplicate, exception, and candidate counts by:

- Year.
- State.
- Crossing.
- Consolidation tier.
- Duration normalization status.
- Crossing-volume tier.

The timestamp profile must report by year:

- Total rows.
- Missing or invalid values.
- Counts and percentages with nonzero seconds.
- Counts and percentages on five-, fifteen-, thirty-, and sixty-minute marks.
- Minimum and maximum timestamps.

The gate report must use this claim boundary:

- `Date/Time` is a user-entered reported incident date and time.
- The reported timestamp is in UTC, based on an electronic communication from
  the FRA data owner dated September 1, 2026.
- Available evidence does not prove it is submission time, exact physical start
  time, or second-accurate observation time.
- Duration is a user-selected category, not a verified incident endpoint.
- An interval without a report can eventually be labeled
  `no_report_observed`, not `unblocked`.

The report must not pre-approve hourly resolution. It must provide the corrected
incident density, timestamp granularity, review-candidate distribution, and
duration limitations needed for Phase 2 to evaluate one-, two-, and four-hour
units.

## Automated Tests

Use the repository's existing standard-library `unittest` convention. Tests must
use synthetic DataFrames and temporary directories; they must not modify the raw
workbooks or tracked output files.

Cover at least:

- Every configured duration category.
- Both known duration aliases and an unmapped value.
- Full-row exact duplicates.
- Same crossing, timestamp, and duration with conflicting reason, impacts, or
  comments remaining distinct.
- Normalized-exact whitespace and case differences.
- Close reports remaining distinct while entering the candidate queue.
- Otherwise identical reports at different crossings.
- Missing, malformed, and boundary crossing IDs and timestamps.
- The December 31, 2025 to January 1, 2026 boundary.
- Unique source-row coverage in the crosswalk.
- Stable source, incident, exception, and candidate IDs across repeated runs and
  reordered in-memory input.
- Deterministic review-sample selection.
- Importing the module without executing the pipeline.

Run all tests with:

```powershell
uv run python -m unittest discover -s unit_tests -p "test_*.py"
```

## Real-Data Validation

1. Run the CLI twice into separate ignored temporary output directories.
2. Compare source IDs, incident IDs, crosswalk assignments, exceptions,
   candidates, review sample, summaries, and diagnostics.
3. Require exact equality except for explicitly nondeterministic execution
   metadata.
4. Confirm both raw input fingerprints are unchanged before and after each run.
5. Restart the notebook kernel, run every cell in order, and save the notebook.
6. Confirm the saved notebook contains no error outputs and every narrative count
   matches the `v2` artifacts.
7. Run `git diff --check` and verify that no derived row-level output has been
   added to Git.

## Acceptance Criteria

Phase 1 remediation is complete only when:

- All automated tests pass.
- Two real-data runs produce identical deterministic results.
- Raw workbooks remain unchanged.
- Every source row maps exactly once to an incident or exception.
- Unsupported durations are never silently coerced.
- Only full-field exact and normalized-exact rows are automatically consolidated.
- Temporal candidates remain review-only and do not affect canonical assignments.
- The deterministic review sample has been labeled and summarized.
- Reconciliation uses normalized full rows with multiplicity.
- Required diagnostics and the machine-readable gate report are complete.
- The notebook runs cleanly from a fresh kernel and contains no stale claims.
- The completion documentation consistently says candidate **reported** incidents,
  not verified physical blockage incidents.
- The roadmap is updated with the conservative Phase 1 definition and remaining
  temporal-candidate uncertainty.

If the review sample has not yet been labeled, implementation may be reported as
`awaiting_review`, but Phase 1 must not be marked complete.
