# Archived Houston Reported-Blockage Duration Prototype

## Status

This directory preserves a legacy exploratory prototype for provenance and
possible reuse. It is **not part of the active blocked-crossing modeling
pipeline**, and its figures and performance results should not be presented as
evidence for the project's current modeling objective.

The active direction is documented in the
[Blocked-Crossing Modeling Roadmap](../../docs/modeling-roadmap.md).

## What This Prototype Modeled

The prototype uses Houston blockage reports to model blockage duration after a
report already exists. Its scripts address two related conditional-severity
tasks:

- Multiclass prediction of the reported duration category.
- Binary prediction of a severe reported blockage, defined as at least 31
  minutes, versus a shorter reported blockage.

It does not model whether a crossing will have a report during an interval, and
it cannot distinguish an actual unblocked interval from an unreported blockage.
The archived `confusion_matrices_comparison.png` therefore compares short and
severe reported blockages rather than blocked and unblocked observations.

## Archived Files

| File | Role |
|---|---|
| `reports.xlsx` | Source Houston blockage reports used by the prototype |
| `cleaned_houston_data.csv` | Derived event-level modeling table |
| `data_prep.py` | Timestamp, duration-target, and frequency-feature preparation |
| `train_models.py` | Multiclass logistic, random-forest, and neural-network comparison |
| `tune_random_forest.py` | Multiclass random-forest tuning and evaluation |
| `evaluate_matrix.py` | Binary severe-versus-short comparison and plots |
| `confusion_matrices_comparison.png` | Binary conditional-severity confusion matrices |
| `threshold_tradeoff.png` | Severe-duration precision-recall threshold plot |

## Why It Was Archived

The prototype does not align with the current regional-screening and pooled
hotspot-timing roadmap. Keeping the files in the repository root made them appear
to be active or authoritative even though they answer a different question.

Known limitations that must be addressed before reusing its modeling results
include:

- Every modeled row is already a reported blockage; there are no verified
  blocked-versus-unblocked examples.
- The severe-duration target is highly imbalanced, so raw accuracy is close to an
  always-severe baseline.
- Random event-row splitting can place the same crossing and duplicate or related
  reports in both training and evaluation data.
- Crossing and railroad frequencies are calculated from the complete dataset
  before splitting.
- `Reason` may be unavailable at the time of a prospective prediction.
- The multiclass scripts and binary evaluation script do not implement one
  consistent prediction contract.
- The active project dependency manifest does not reproduce all packages imported
  by these scripts.

## Potential Reuse

The following ideas may be reconsidered within a future, leakage-safe plan:

- Timestamp-derived candidate features.
- Scikit-learn preprocessing and model-comparison structure.
- A secondary model of duration severity conditional on an observed report.
- Plotting patterns for threshold and confusion-matrix diagnostics.

Reuse should copy or redesign only the needed concepts in the active pipeline.
The archived files should not become implicit dependencies of new work.

## Reproducibility Notes

The scripts use working-directory-relative paths. To inspect the historical
workflow, run them from this directory in a separately prepared environment.
They are retained as-is and are not supported by the active project environment.
Running them may overwrite the archived derived CSV or figures.

## Provenance

The prototype files were introduced together in commit
`3005fd422d7a19176fa158690700f45d1543f3d6` (`Add model evaluation matrix`).
Commit `d92c2631d94a8f02cf1c9aed74de8644b7579fc0` later added comments to
`evaluate_matrix.py`. Other changes in that later commit affected national
analysis files and remain in their active locations.
