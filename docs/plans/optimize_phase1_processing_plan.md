# Simplify and Accelerate Phase 1 Validation

## Summary

Speed up incident consolidation, save a reusable checkpoint after step 5, and make the second real-data run optional and disabled by default.

Do not add cache keys, implementation fingerprints, dependency hashes, automatic invalidation, or automatic cache reuse.

## Implementation Changes

### Optimize consolidation

In `analysis/incident_deduplication.py`:

- Replace the per-group DataFrame loop in `consolidate_reports()` with one grouped aggregation and merge.
- Preserve existing canonical IDs, consolidation tiers, row ordering, columns, null handling, and dtypes.
- Continue sorting source-row IDs before generating each incident ID.
- Print separate timings for local-time enrichment and consolidation.

### Add a simple step-5 checkpoint

After successful enrichment and consolidation, always write:

```text
<output_dir>/step_5_checkpoint/
├── source.parquet
├── reconciliation.parquet
├── crossing_timezones.parquet
├── incidents.parquet
├── crosswalk.parquet
├── exceptions.parquet
└── checkpoint.json
```

`checkpoint.json` contains only:

- Checkpoint format version
- Creation timestamp
- Ruleset version
- Source filenames
- Row counts
- Columns for each saved frame

Before rewriting a checkpoint, remove its old `checkpoint.json`. Write the new metadata file last so an interrupted write cannot appear complete.

Extend `run_phase_1()` with:

```python
reuse_step_5_checkpoint: bool = False
```

When false:

- Run steps 1–5 normally.
- Write the checkpoint.
- Continue through steps 6–9.

When true:

- Require all checkpoint files.
- Verify the format version, ruleset version, filenames, row counts, and columns.
- Fail with a clear message if validation fails; do not silently fall back.
- Print the checkpoint timestamp and warn that current input contents are not automatically compared with the checkpoint.
- Load the checkpoint and continue at step 6.

Document that users must rebuild after changing inputs, configuration, or pipeline code.

Add CLI flag `--reuse-step-5-checkpoint`. CLI behavior remains fresh by default.

## Notebook Workflow

In `analysis/phase_1_analysis.ipynb`, add:

```python
REUSE_STEP_5_CHECKPOINT = False
RUN_REPEATABILITY_CHECK = False
```

Behavior:

- The normal workflow performs one primary run.
- Setting `REUSE_STEP_5_CHECKPOINT = True` resumes that run from its saved step-5 checkpoint.
- Setting `RUN_REPEATABILITY_CHECK = True` performs two fresh independent runs and compares their outputs.
- If both switches are true, repeatability mode takes precedence and checkpoint reuse is disabled for both runs.
- Keep the existing before/after SHA-256 comparison solely to prove the notebook did not modify its raw inputs.
- Do not use hashes to validate checkpoints.
- Record repeatability evidence as `not_run` when the optional second run is disabled; do not report it as passed.
- Include `two_real_data_runs_match` in acceptance checks only when the optional repeatability run was requested.
- Preserve all manual-review labels.

Update `docs/plans/01a-incident-deduplication-remediation.md` so two full real-data runs are an optional diagnostic for major pipeline changes rather than a normal acceptance requirement.

## Tests and Acceptance Criteria

In `unit_tests/test_incident_deduplication.py`:

- Compare the optimized consolidation output against a small test-only reference implementation.
- Cover singleton, exact, normalized-exact, invalid-ID, invalid-timestamp, and shuffled-input cases.
- Assert exact columns, row order, values, IDs, and dtypes.
- Test checkpoint creation and successful loading.
- Test missing files, incompatible ruleset versions, row-count mismatches, and schema mismatches.
- Prove checkpoint reuse skips reading inputs, timezone resolution, enrichment, and consolidation.
- Verify the two notebook switches and their precedence.
- Retain the existing small-fixture repeatability test.

Verification:

1. Record the current 1,000-group consolidation benchmark before editing.
2. Require at least a 10× improvement on the same fixture.
3. Run the complete unit suite.
4. Run one fresh real-data execution and confirm all validations pass.
5. Rerun with checkpoint reuse and confirm execution begins at step 6.
6. Enable optional repeatability mode once and confirm two fresh outputs compare equal.

## Boundaries

- Existing outputs without `step_5_checkpoint/checkpoint.json` cannot be resumed.
- Checkpoint freshness is deliberately controlled by the analyst.
- Do not add dependencies, change Phase 2, or optimize step 6 without separate timing evidence.
- Keep checkpoint and output files under the already ignored `analysis_outputs/`.
- Recheck the worktree before implementation and preserve unrelated changes.