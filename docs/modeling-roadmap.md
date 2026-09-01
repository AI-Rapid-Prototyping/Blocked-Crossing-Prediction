# Blocked-Crossing Modeling Roadmap

## Purpose

This roadmap defines the long-term modeling direction for the repository and the
sequence of evidence needed before more complex models are implemented. Detailed
implementation decisions belong in phase-specific plans and should be added only
when their prerequisites are complete.

The active implementation plan is
[Phase 1 Remediation: Auditable Incident Deduplication](plans/01a-incident-deduplication-remediation.md).

## Objective

Develop two related capabilities from deduplicated historical blockage reports:

1. A regional screening layer that identifies crossings with enough historical
   reported activity to warrant closer monitoring.
2. A pooled hotspot timing model that estimates when a reported blockage is most
   likely at crossings selected by the screening layer.

Both capabilities will use a canonical national data foundation and one
configuration-driven pipeline that can run nationally or for a defined state,
county set, city, metropolitan planning organization (MPO), or custom partner
region. National source coverage does not require every model or exposure table
to include every national crossing.

The observable outcome is a **reported blockage**, not a confirmed actual
blockage. The repository currently has no independent sensors, operational feeds,
or other ground truth with which to identify unreported blockages or verify that a
crossing was unblocked when no report was received.

## Observation and Claim Boundary

The available data observe the reporting process:

```text
Actual blockage
|-- reported     -> observed positive report
`-- not reported -> not observable in the current data

No actual blockage
`-- no report    -> observationally identical to an unreported blockage
```

Until independent validation data become available:

- Reported events are treated as observed reports after data-quality review and
  deduplication.
- An interval without a report means `no_report_observed`, not `unblocked`.
- Model probabilities refer to future observed reports under the current
  reporting process. They are not calibrated probabilities of actual blockage.
- Apparent geographic or temporal patterns may reflect reporting behavior as well
  as blockage behavior.
- Deduplication can consolidate reports; it cannot identify missed events or
  validate the underlying incident.

These limitations must appear in model documentation, metrics, plots, and
practitioner-facing outputs.

## Authoritative Data Scope

For the new modeling workflow:

- `data/blocked_crossings_2020through2025.xlsx` is the authoritative Phase 1
  report source.
- `data/blocked_crossings_2025.xlsx` is a reconciliation subset. It must not be
  appended to the authoritative workbook or treated as a second independent
  source without evidence that its rows are distinct.
- `archive/houston-reported-blockage-duration-prototype/` preserves the prior
  Houston duration-severity data, scripts, and figures. Those artifacts are
  outside the new pipeline and must not be treated as current model results.
- Form 71 inventory data may support regional context and crossing-level
  features. Its state, county, city, latitude, and longitude fields can support
  geographic assignment. The current inventory does not supply an MPO identifier,
  so MPO and other custom jurisdictions require a separate, versioned geographic
  crosswalk.

Raw source files remain immutable. Generated datasets must be reproducible from
code and written to ignored output locations rather than committed as new source
data.

## Proposed Modeling Architecture

### National foundation and configurable geographic scope

Source auditing, deduplication, and canonical incident and crossing tables will
be national. Regional filtering must occur through shared configuration and
geographic membership data rather than region-specific copies of the pipeline.

| Component | Default scope |
|---|---|
| Source audit and incident deduplication | National |
| Canonical incident and crossing tables | National |
| Crossing-to-region membership | All configured regions |
| Exposure generation | Requested region and historically eligible cohort only |
| Baseline modeling | National pooled model |
| Partner analysis | Regional calibration or regional model when supported |
| Evaluation | National summary plus region-specific performance |

A modeling run must accept a versioned region definition. Simple regions may use
normalized state, county, or city identifiers. MPOs and custom jurisdictions
should use an authoritative boundary or membership source and, where appropriate,
a spatial join to Form 71 coordinates.

Crossing-to-region membership should be represented as a reusable many-to-many
crosswalk with, at minimum, a crossing ID, region ID, region type, boundary or
membership-source version, and assignment method. A crossing may belong to a
city, county, MPO, state, and custom partner region simultaneously.

The Houston-Galveston Area Council may be used as a demonstration region after an
authoritative jurisdiction definition is obtained, but no H-GAC-specific logic
should be hard-coded into the shared pipeline.

### Regional screening layer

The screening layer will operate over a defined regional crossing population and
use only information available before its screening date. Its purpose is to rank
or classify crossings for monitoring, not to assert that crossings without
reports were confirmed unblocked.

The exact unit and horizon remain a decision gate. Candidate units include a
crossing-month, crossing-quarter, or crossing-year. A coarser unit avoids creating
a national crossing-hour table solely for screening.

### Pooled hotspot timing layer

The timing layer will estimate the probability of an observed report in a future
time interval for crossings selected from historical information. One pooled
model is preferred over a separate model for every crossing so that low-volume
crossings can share information while retaining crossing-specific effects.

The production prediction interval may be hourly, but the appropriate interval
must be supported by timestamp semantics and distinct-incident density. Two-hour,
four-hour, or daily intervals remain valid alternatives if hourly labels are too
uncertain or sparse.

### Geographic model strategy

The pipeline must support an evidence-based progression rather than assume one
geographic model scope will always be best:

1. A national pooled model using crossing and geographic effects.
2. A national model with region-specific calibration or alert thresholds.
3. A partially pooled model that permits regional differences while sharing
   information across regions.
4. A fully regional model when training history and future validation contain
   enough distinct incidents to support it.

The choice among these approaches must use training-period evidence and
chronological validation. A region must not qualify for a local model because of
event counts observed in the future evaluation period.

## Modeling and Validation Principles

1. Consolidate reports into auditable incident records before calculating event
   rates or selecting hotspots.
2. Preserve a crosswalk from every source report to its incident record or
   documented exception.
3. Define cohorts, features, and preprocessing constants using training history
   only.
4. Use chronological backtesting rather than random row splitting.
5. Define an explicit prediction time, lead time, and outcome window before
   constructing features.
6. Never use report attributes that become available only during or after the
   predicted interval as prospective features.
7. Avoid materializing the full national crossing-hour population when negative
   sampling, regional filtering, coarser screening units, or on-demand generation
   will suffice.
8. Evaluate on an unsampled future population even if negative intervals are
   sampled for training.
9. Compare every model with a transparent historical-rate baseline.
10. Prefer the simplest model that meets operational performance and calibration
    requirements.
11. Use one shared pipeline for all geographic scopes; represent regional
    differences through versioned data and configuration rather than code forks.
12. Select regional cohorts and modeling strategies using training history only.
13. Report geographic coverage and region-specific performance so national
    averages do not conceal weak local results.

## Phased Delivery

| Phase | Outcome | Primary decision gate |
|---|---|---|
| 1. Source audit and incident deduplication | Deterministic incident table, source crosswalk, and deduplication diagnostics | Are timestamps and report density adequate for interval construction? |
| 2. Reported-event interval, exposure, and geography construction | Canonical national tables, versioned region membership, candidate prediction units, and leakage-safe labels | Is hourly resolution supportable, and can requested regions be assigned reproducibly? |
| 3. Configurable hotspot cohort and regional screening | Past-only monitoring cohorts and regional baselines produced by the shared pipeline | Which screening horizon and eligibility rule provide useful regional coverage? |
| 4. Pooled timing and geographic baselines | Leakage-safe pooled model, historical-rate comparator, and geographic model comparisons | Does pooling, regional calibration, or regional fitting perform best on future data? |
| 5. Backtesting, calibration, transfer, and threshold selection | Rolling and geographic validation, calibrated probabilities, and operational thresholds | Is performance stable within and across regions? |
| 6. Regional outputs and operational documentation | Reproducible risk tables, comparisons, and limitations | Are outputs interpretable and appropriately scoped? |

Only one phase-specific implementation plan should be active at a time. Completion
of a phase updates this roadmap before the next plan is finalized.

## Phase Decision Gates

### After Phase 1

- Confirm the meaning and precision of `Date/Time`.
- Quantify distinct incidents per crossing and per evaluation period.
- Determine whether duration fields can support interval-overlap labels.
- Decide whether probable duplicate groups require manual adjudication.

### After Phase 2

- Select hourly or coarser prediction intervals.
- Define how uncertain adjacent intervals are represented.
- Determine whether training will use the full exposure table or weighted
  negative sampling.
- Establish the region-configuration contract and required geographic crosswalk
  fields.
- Identify authoritative, versioned boundary or membership sources for initial
  demonstration regions.
- Confirm that exposure generation can occur after regional and historical cohort
  filtering rather than across all national crossing-hours.

### After Phase 3

- Freeze a hotspot eligibility rule based only on prior history.
- Quantify regional coverage excluded by the hotspot cohort.
- Confirm that the cohort contains enough future events for meaningful
  validation without using those future events to select the cohort.
- Determine from training history which regions support only national pooling,
  regional calibration, partial pooling, or a fully regional baseline.

### After Phases 4 and 5

- Compare national pooled, regionally calibrated, partially pooled, fully
  regional, per-crossing, and historical-rate baselines where sample size
  permits.
- Decide whether added complexity produces material and stable improvement.
- Select alert thresholds using practitioner costs rather than accuracy alone.
- Evaluate transfer to regions not used for model fitting when the available
  geographic coverage supports that test.

## Evaluation Framework

Primary evaluation should include:

- Precision-recall area under the curve on the natural future prevalence.
- Calibration of predicted report probabilities.
- Recall of distinct reported incidents.
- Precision and false alerts per crossing-day at candidate thresholds.
- Lift over an hour-of-week or equivalent historical-rate baseline.
- Performance by crossing, geography, season, and reporting-volume tier.
- Regional calibration and differences in operational thresholds.
- Geographic coverage and sample sufficiency for every reported regional result.
- Transfer performance for held-out regions when feasible.

All metrics remain relative to observed reports unless independent ground truth is
introduced. Standard accuracy is not a primary metric for sparse interval data.

## External Validation Research Track

External data are not a prerequisite for the initial reported-event models, but
they are required before interpreting predictions as actual-blockage risk. Future
research may investigate railroad operational records, gate-status feeds, traffic
cameras, dispatch or emergency-response records, or other independently collected
sources.

Any external source must be evaluated for geographic coverage, timestamp quality,
access restrictions, independence from the existing reporting system, and whether
it supplies verified negative intervals. Until then, the project must preserve the
reported-event claim boundary.

## Current Status

| Phase | Status | Plan |
|---|---|---|
| 1. Source audit and incident deduplication | Remediation planned | [Phase 1 remediation plan](plans/01a-incident-deduplication-remediation.md) |
| 2. Reported-event interval, exposure, and geography construction | Not started — draft blocked by Phase 1 remediation | [Preliminary Phase 2 plan](plans/02-reported-event-interval-exposure-geography.md) |
| 3. Configurable hotspot cohort and regional screening | Not started | To be drafted after Phase 2 |
| 4. Pooled timing and geographic baselines | Not started | To be drafted after Phase 3 |
| 5. Backtesting, calibration, transfer, and threshold selection | Not started | To be drafted after Phase 4 |
| 6. Regional outputs and operational documentation | Not started | To be drafted after Phase 5 |

## Change Control

Changes to the objective, claim boundary, authoritative source, prediction unit,
geographic-scope contract, boundary source, or validation design must be recorded
in this roadmap. Implementation details and temporary experiments belong in the
active phase plan rather than expanding this file into a task log.
