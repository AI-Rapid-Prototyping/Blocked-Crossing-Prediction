# Phase 1 Amendment: Deterministic Two-Outcome Pair Decisions

## Status

Active. This amendment supersedes the review-only temporal-candidate and
manual-label requirements in
[01a-incident-deduplication-remediation.md](01a-incident-deduplication-remediation.md).
The original Phase 1 plan and remediation plan remain as an audit trail.

Parent roadmap: [Blocked-Crossing Modeling Roadmap](../modeling-roadmap.md)

## Objective and Claim Boundary

Assign every configured same-crossing comparison pair exactly one deterministic
decision:

- `auto_merge`
- `keep_distinct`

Uncertainty describes the strength and basis of a completed decision. It is not
an unresolved workflow state. The ruleset must not emit `needs_review`, leave a
decision blank, or require a manual label before a pair is decided.

These decisions organize observed reports. They do not establish that a physical
blockage occurred or that two reports describe the same physical event. Human
assessments, if collected for audit or future ruleset development, are reviewer
opinions rather than truth labels and must not change production assignments.

## Decision Universe

Begin with the exact and normalized-exact report groups created under the Phase 1
normalization contract. Compare distinct groups only when they share a valid
normalized crossing ID and their timestamp separation is within the configured
15-, 30-, 60-, or 120-minute decision bands. Every generated pair receives one
decision. Reports outside that explicit comparison universe remain distinct by
construction.

The earlier endpoint is determined by UTC timestamp, then stable report-group ID
for ties. UTC remains authoritative; local time is display and diagnostic
evidence only.

## Duration Proxy Contract

Duration is a user-selected proxy interval, not a measured event endpoint.
Preserve the raw value, normalized category, lower bound, and upper bound.

For each pair:

1. If the earlier report has a finite maximum proxy duration shorter than
   `separation_minutes`, assign `keep_distinct` with
   `decision_basis=duration_incompatible`.
2. If the finite maximum is equal to or greater than the separation, record
   `possible_temporal_overlap`. This is necessary but insufficient evidence for
   `auto_merge`.
3. `More than one day` has no finite maximum. It therefore cannot authorize a
   temporal auto-merge. Assign `keep_distinct` with
   `decision_basis=open_ended_duration_proxy` and retain
   `possible_temporal_overlap` uncertainty.
4. An unmapped or otherwise unavailable duration proxy cannot authorize a
   temporal auto-merge. Assign `keep_distinct` with
   `decision_basis=duration_proxy_unavailable`.

Exact and normalized-exact full-row matches remain eligible for their existing
stronger auto-merge rules before temporal pairs are evaluated.

## Non-Temporal Compatibility Contract

A finite possible overlap may become `auto_merge` only when all of these fields
match after the existing documented, meaning-preserving normalization:

- `City`
- `State`
- `Street`
- `County`
- `Railroad`
- `Reason`
- `Immediate Impacts`
- `Additional Comments`

The normalized crossing ID must already match. `Date/Time` and `Duration` are
excluded from this non-temporal set because they supply the separation and proxy
overlap evidence. Null-to-null is compatible; null-to-value is incompatible. No
fuzzy, semantic, or state-inferred matching is permitted.

## Decision Matrix

| Evidence | `pair_decision` | `decision_basis` | Uncertainty |
|---|---|---|---|
| Exact full-row match | `auto_merge` | `exact_match` | none |
| Normalized full-row match | `auto_merge` | `normalized_exact_match` | none |
| Finite maximum shorter than separation | `keep_distinct` | `duration_incompatible` | none |
| Finite possible overlap and every non-temporal field matches | `auto_merge` | `overlap_proxy_and_non_temporal_match` | `possible_temporal_overlap` |
| Finite possible overlap and any non-temporal field differs | `keep_distinct` | `overlap_proxy_but_non_temporal_mismatch` | `possible_temporal_overlap` |
| Open-ended proxy | `keep_distinct` | `open_ended_duration_proxy` | `possible_temporal_overlap` |
| Missing or unmapped proxy | `keep_distinct` | `duration_proxy_unavailable` | `duration_proxy_unavailable` |
| Otherwise eligible edge rejected to preserve complete-link grouping | `keep_distinct` | `complete_link_conflict` | `possible_temporal_overlap` |

The pair artifact must contain `pair_decision`, `decision_basis`,
`uncertainty_flag`, and `uncertainty_basis`, together with the duration and field
compatibility evidence used to reach the decision.

## Safe Incident Grouping

Temporal overlap is not transitive. If A-B and B-C are `auto_merge` but A-C is
`keep_distinct`, connected-component grouping would create a false merge.

Apply temporal auto-merges using deterministic complete-link grouping. A report
group may join an incident group only when every cross-group pair initially meets
the auto-merge evidence rule. An ungenerated pair is not an auto-merge edge. If
an otherwise eligible edge cannot be applied without placing a `keep_distinct`
pair in one incident, its final decision becomes `keep_distinct` with
`decision_basis=complete_link_conflict`. Thus every final `auto_merge` pair is
coalesced and no final incident contains a materialized `keep_distinct` pair.

Final incident IDs remain deterministic hashes of all contributing source-row
IDs. The crosswalk must continue to map every authoritative source row exactly
once to a final incident or documented exception.

## Outputs and Diagnostics

Ruleset `phase1-v3` writes:

- `pair_decisions.parquet`
- `pair_decision_audit_sample.csv`
- `pair_decision_summary.json`

The audit sample is deterministic and contains the ruleset decision and evidence.
It has no blank label field and requires no human completion. Diagnostics and the
gate report must provide counts by decision, decision basis, uncertainty basis,
duration status, year, crossing, and crossing-volume tier.

The gate status is `complete` only when all automated checks pass; otherwise it
is `incomplete`. `awaiting_review` is not a terminal status.

## Required Tests

Add focused regression coverage for:

- finite maximum shorter than separation;
- equality at the duration boundary;
- possible overlap plus complete non-temporal compatibility;
- possible overlap plus a mismatch in each configured non-temporal field;
- missing, unmapped, and open-ended duration proxies;
- different crossing IDs;
- deterministic orientation, IDs, and results after input reordering;
- exactly one allowed decision on every generated pair;
- complete-link prevention of chained false merges;
- no `keep_distinct` pair inside a final incident;
- source-row coverage, repeatability, manifest schemas, and clean notebook use.

## Acceptance Criteria

This amendment is implemented only when:

- every generated pair has exactly one allowed decision;
- duration is described and used only as proxy evidence;
- temporal auto-merge requires every configured non-temporal field;
- open-ended and unavailable proxies do not authorize temporal auto-merge;
- uncertainty is recorded separately from decision;
- no manual assessment changes a production decision or is described as truth;
- complete-link grouping prevents transitive false merges;
- all source reports retain deterministic lineage;
- automated tests and repeatability checks pass; and
- the saved notebook and downstream documentation contain no stale review-only,
  manual-truth, or `awaiting_review` claims.
