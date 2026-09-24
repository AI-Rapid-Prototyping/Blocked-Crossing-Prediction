"""Phase 2 reported-event exposure and geography construction.

The production pipeline is deliberately fail-closed. It validates the accepted
Phase 1 v3 handoff first and does not construct exposure rows until coverage,
geography, region, and past-only cohort inputs are explicitly configured.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd


PHASE1_REQUIRED_FILES = {
    "source": "source_reports_with_ids.parquet",
    "incidents": "reported_incidents.parquet",
    "crosswalk": "report_incident_crosswalk.parquet",
    "exceptions": "documented_exceptions.parquet",
    "pair_decisions": "pair_decisions.parquet",
    "pair_summary": "pair_decision_summary.json",
    "gate": "phase_1_gate_report.json",
    "manifest": "run_manifest.json",
}

PHASE1_REQUIRED_COLUMNS = {
    "source": {
        "source_row_id", "norm_crossing_id", "reported_at_utc", "norm_duration",
        "duration_lower_minutes", "duration_upper_minutes", "duration_normalization_status",
        "iana_time_zone",
    },
    "incidents": {
        "canonical_incident_id", "norm_crossing_id", "earliest_reported_at_utc",
        "primary_source_row_id", "ruleset_version",
    },
    "crosswalk": {"source_row_id", "canonical_incident_id", "exception_id"},
    "exceptions": {"exception_id", "source_row_id", "norm_crossing_id", "reported_at_utc"},
    "pair_decisions": {
        "pair_decision_id", "pair_decision", "decision_basis", "uncertainty_flag",
        "uncertainty_basis", "left_final_incident_id", "right_final_incident_id",
    },
}

COVERAGE_COLUMNS = {
    "coverage_id", "source_name", "source_version", "scope_type", "scope_id",
    "coverage_start", "coverage_end", "time_zone", "coverage_status",
    "evidence_reference",
}
COHORT_COLUMNS = {"norm_crossing_id", "eligibility_as_of"}


@dataclass(frozen=True)
class Phase2Result:
    artifact_paths: dict[str, Path]
    summary: dict[str, Any]
    validations: dict[str, bool]
    gate: dict[str, Any]


def stable_hash(*parts: object, length: int = 20) -> str:
    payload = "\x1f".join("<NULL>" if value is None else str(value) for value in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def _volume_tier(count: int) -> str:
    return "low" if count <= 3 else "medium" if count <= 19 else "high"


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.casefold() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.casefold() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported table format: {path}")


def _resolve_path(repo_root: Path, configured: str | None) -> Path | None:
    if not configured:
        return None
    path = Path(configured)
    return path if path.is_absolute() else repo_root / path


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def validate_phase1_handoff(
    phase1_dir: Path, expected_ruleset: str
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Validate Phase 1 status, schemas, counts, and source disposition."""

    phase1_dir = Path(phase1_dir)
    paths = {name: phase1_dir / filename for name, filename in PHASE1_REQUIRED_FILES.items()}
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Phase 1 handoff is incomplete; missing: {missing}")

    gate = json.loads(paths["gate"].read_text(encoding="utf-8"))
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    if gate.get("status") != "complete":
        raise ValueError(f"Phase 1 gate is not complete: {gate.get('status')!r}")
    if not gate.get("validation") or not all(gate["validation"].values()):
        raise ValueError("Phase 1 gate does not contain a complete passing validation set.")
    if manifest.get("ruleset_version") != expected_ruleset:
        raise ValueError(
            f"Phase 1 ruleset mismatch: expected {expected_ruleset!r}, "
            f"found {manifest.get('ruleset_version')!r}."
        )

    frames = {
        name: pd.read_parquet(paths[name])
        for name in ("source", "incidents", "crosswalk", "exceptions", "pair_decisions")
    }
    for name, required in PHASE1_REQUIRED_COLUMNS.items():
        _require_columns(frames[name], required, f"Phase 1 {name}")

    manifest_outputs = manifest.get("outputs", {})
    for name in ("source", "incidents", "crosswalk", "exceptions", "pair_decisions"):
        manifest_name = PHASE1_REQUIRED_FILES[name]
        details = next(
            (
                value
                for value in manifest_outputs.values()
                if isinstance(value, dict) and value.get("filename") == manifest_name
            ),
            None,
        )
        if details is None or details.get("row_count") != len(frames[name]):
            raise ValueError(f"Phase 1 manifest row count is missing or stale for {manifest_name}.")
        if details.get("schema") != list(frames[name].columns):
            raise ValueError(f"Phase 1 manifest schema is missing or stale for {manifest_name}.")

    source_ids = set(frames["source"]["source_row_id"].astype(str))
    if not frames["crosswalk"]["source_row_id"].is_unique:
        raise ValueError("Phase 1 crosswalk contains duplicate source_row_id values.")
    if set(frames["crosswalk"]["source_row_id"].astype(str)) != source_ids:
        raise ValueError("Phase 1 crosswalk does not dispose every source row exactly once.")
    assigned = frames["crosswalk"]["canonical_incident_id"].notna()
    excepted = frames["crosswalk"]["exception_id"].notna()
    if not assigned.ne(excepted).all():
        raise ValueError("Each Phase 1 source row must map to exactly one incident or exception.")
    incident_ids = set(frames["incidents"]["canonical_incident_id"].astype(str))
    if not frames["incidents"]["ruleset_version"].eq(expected_ruleset).all():
        raise ValueError("Phase 1 incident rows do not all use the accepted ruleset.")
    if not set(frames["crosswalk"].loc[assigned, "canonical_incident_id"].astype(str)).issubset(
        incident_ids
    ):
        raise ValueError("Phase 1 crosswalk references unknown incident IDs.")
    if not frames["pair_decisions"]["pair_decision"].isin({"auto_merge", "keep_distinct"}).all():
        raise ValueError("Phase 1 pair decisions contain an unsupported or missing outcome.")
    pair_endpoints = set(frames["pair_decisions"]["left_final_incident_id"].dropna().astype(str)) | set(
        frames["pair_decisions"]["right_final_incident_id"].dropna().astype(str)
    )
    if not pair_endpoints.issubset(incident_ids):
        raise ValueError("Phase 1 pair decisions reference unknown final incident IDs.")
    pair_summary = json.loads(paths["pair_summary"].read_text(encoding="utf-8"))
    if pair_summary.get("pair_count") != len(frames["pair_decisions"]):
        raise ValueError("Phase 1 pair-decision summary row count is stale.")

    evidence = {
        "ruleset_version": expected_ruleset,
        "gate_status": gate["status"],
        "source_rows": len(frames["source"]),
        "reported_incidents": len(frames["incidents"]),
        "documented_exceptions": len(frames["exceptions"]),
        "pair_decisions": len(frames["pair_decisions"]),
    }
    return frames, evidence


def phase2_preflight_blockers(config: dict[str, Any], repo_root: Path) -> list[str]:
    blockers: list[str] = []
    inventory_path = _resolve_path(repo_root, config.get("form71_inventory_path"))
    coverage_path = _resolve_path(repo_root, config.get("source_coverage_path"))
    cohort_path = _resolve_path(repo_root, config.get("cohort_path"))
    region = config.get("region_source", {})
    region_path = _resolve_path(repo_root, region.get("path"))

    if inventory_path is None or not inventory_path.is_file():
        blockers.append("configured Form 71 inventory file is missing")
    if coverage_path is None or not coverage_path.is_file():
        blockers.append("BP-02: approved source coverage table is not configured")
    if cohort_path is None or not cohort_path.is_file():
        blockers.append("historical cohort input is not configured")
    if not config.get("requested_region_ids"):
        blockers.append("requested_region_ids is empty")
    if not config.get("eligibility_as_of"):
        blockers.append("eligibility_as_of is not configured")
    required_region_metadata = ("source_name", "source_version", "effective_date", "license")
    if region.get("approval_status") != "approved" or any(
        not region.get(field) for field in required_region_metadata
    ):
        blockers.append("BP-08: geographic source provenance is not approved and complete")
    if region_path is None or not region_path.is_file():
        blockers.append("configured geographic source file is missing")
    return blockers


def build_canonical_crossings(
    inventory_path: Path,
    phase1_incidents: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Select one deterministic current inventory record per valid crossing."""

    columns = config["inventory_columns"]
    raw = pd.read_csv(inventory_path, encoding="utf-8-sig", low_memory=False)
    _require_columns(raw, set(columns.values()), "Form 71 inventory")
    work = raw[list(columns.values())].copy().reset_index(names="inventory_source_row_number")
    work["inventory_source_row_number"] += 2
    work["norm_crossing_id"] = (
        work[columns["crossing_id"]].astype("string").str.strip().str.upper()
    )
    work["id_status"] = np.where(
        work["norm_crossing_id"].str.match(config["crossing_id_pattern"], na=False),
        "valid",
        "invalid",
    )
    work["inventory_revision_date"] = pd.to_datetime(
        work[columns["revision_date"]], errors="coerce", utc=True
    )
    work["latitude"] = pd.to_numeric(work[columns["latitude"]], errors="coerce")
    work["longitude"] = pd.to_numeric(work[columns["longitude"]], errors="coerce")
    work["coordinate_status"] = np.where(
        work["latitude"].between(-90, 90) & work["longitude"].between(-180, 180),
        "valid",
        "invalid_or_missing",
    )
    work["is_closed_current"] = (
        work[columns["closed"]].astype("string").str.strip().str.casefold().isin({"1", "true", "yes", "y"})
    )

    rename = {
        source_column: output_column
        for output_column, source_column in columns.items()
        if output_column not in {"crossing_id", "revision_date", "latitude", "longitude", "closed"}
    }
    work = work.rename(columns=rename)
    state_numbers = pd.to_numeric(work.get("state_code"), errors="coerce").astype("Int64")
    county_numbers = pd.to_numeric(work.get("county_code"), errors="coerce").astype("Int64")
    work["state_fips"] = state_numbers.map(
        lambda value: pd.NA if pd.isna(value) else f"{int(value):02d}"
    ).astype("string")

    def county_fips(state_fips: object, county_number: object) -> object:
        if pd.isna(state_fips) or pd.isna(county_number):
            return pd.NA
        digits = str(int(county_number))
        return digits.zfill(5) if len(digits) > 3 else f"{state_fips}{digits.zfill(3)}"

    work["county_fips"] = [
        county_fips(state, county)
        for state, county in zip(work["state_fips"], county_numbers)
    ]
    valid = work.loc[work["id_status"].eq("valid")].sort_values(
        ["norm_crossing_id", "inventory_revision_date", "inventory_source_row_number"],
        kind="stable",
        na_position="first",
    )
    duplicate_counts = valid.groupby("norm_crossing_id").size()
    canonical = valid.drop_duplicates("norm_crossing_id", keep="last").copy()
    canonical["inventory_record_count"] = canonical["norm_crossing_id"].map(duplicate_counts).astype(int)
    phase1_crossings = set(phase1_incidents["norm_crossing_id"].dropna().astype(str))
    canonical["has_phase1_report_history"] = canonical["norm_crossing_id"].isin(phase1_crossings)
    canonical["phase1_match_status"] = np.where(
        canonical["has_phase1_report_history"], "matched", "no_phase1_report_history"
    )

    keep = [
        "norm_crossing_id", "inventory_source_row_number", "inventory_revision_date",
        "inventory_record_count", "id_status", "coordinate_status", "latitude", "longitude",
        "is_closed_current", "has_phase1_report_history", "phase1_match_status",
        "state_fips", "county_fips",
        *rename.values(),
    ]
    canonical = canonical[list(dict.fromkeys(keep))].sort_values("norm_crossing_id").reset_index(drop=True)
    diagnostics = pd.DataFrame(
        [
            {"metric": "inventory_rows", "count": len(work)},
            {"metric": "canonical_crossings", "count": len(canonical)},
            {"metric": "invalid_crossing_ids", "count": int(work["id_status"].ne("valid").sum())},
            {"metric": "duplicate_inventory_rows", "count": int((duplicate_counts - 1).clip(lower=0).sum())},
            {"metric": "invalid_or_missing_coordinates", "count": int(canonical["coordinate_status"].ne("valid").sum())},
            {"metric": "phase1_crossings_missing_from_inventory", "count": len(phase1_crossings - set(canonical["norm_crossing_id"]))},
        ]
    )
    return canonical, diagnostics


def build_region_membership(
    crossings: pd.DataFrame,
    region_source: dict[str, Any],
    repo_root: Path,
) -> tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame]:
    """Build versioned region definitions and a many-to-many membership table."""

    path = _resolve_path(repo_root, region_source["path"])
    assert path is not None
    regions = gpd.read_file(path)
    if regions.crs is None:
        raise ValueError("Geographic source has no coordinate reference system.")
    regions = regions.to_crs("EPSG:4326")
    id_field, name_field = region_source["id_field"], region_source["name_field"]
    _require_columns(regions, {id_field, name_field, "geometry"}, "region source")
    region_type = region_source["region_type"]
    regions = regions.copy()
    regions["region_id"] = region_type + ":" + regions[id_field].astype("string").str.strip()
    regions["region_name"] = regions[name_field].astype("string").str.strip()
    definitions = (
        regions[["region_id", "region_name"]]
        .drop_duplicates()
        .sort_values("region_id")
        .assign(
            region_type=region_type,
            membership_source_name=region_source["source_name"],
            membership_source_version=region_source["source_version"],
            effective_date=region_source["effective_date"],
            assignment_method="point_in_polygon_intersects",
        )
        .to_dict("records")
    )

    valid_points = crossings.loc[crossings["coordinate_status"].eq("valid")].copy()
    points = gpd.GeoDataFrame(
        valid_points[["norm_crossing_id", "latitude", "longitude"]],
        geometry=gpd.points_from_xy(valid_points["longitude"], valid_points["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(
        points,
        regions[["region_id", "region_name", "geometry"]],
        how="left",
        predicate="intersects",
    )
    assigned = joined.loc[joined["region_id"].notna(), ["norm_crossing_id", "region_id"]].drop_duplicates()
    match_counts = assigned.groupby("norm_crossing_id").size()
    assigned["assignment_status"] = assigned["norm_crossing_id"].map(
        lambda value: "ambiguous" if match_counts[value] > 1 else "assigned"
    )
    assigned["assignment_notes"] = ""

    unmatched_ids = sorted(set(crossings["norm_crossing_id"]) - set(assigned["norm_crossing_id"]))
    unmatched = pd.DataFrame(
        {
            "norm_crossing_id": unmatched_ids,
            "region_id": pd.NA,
            "assignment_status": "unmatched",
            "assignment_notes": "No intersecting configured region or invalid coordinates",
        }
    )
    membership = pd.concat([assigned, unmatched], ignore_index=True)
    membership["region_type"] = region_type
    membership["membership_source_version"] = region_source["source_version"]
    membership["assignment_method"] = "point_in_polygon_intersects"
    membership = membership[
        [
            "norm_crossing_id", "region_id", "region_type", "membership_source_version",
            "assignment_method", "assignment_status", "assignment_notes",
        ]
    ].sort_values(["norm_crossing_id", "region_id"], na_position="last").reset_index(drop=True)
    diagnostics = (
        membership.groupby("assignment_status", dropna=False)
        .agg(membership_rows=("norm_crossing_id", "size"), crossings=("norm_crossing_id", "nunique"))
        .reset_index()
    )
    return definitions, membership, diagnostics


def load_coverage(path: Path) -> pd.DataFrame:
    coverage = _read_table(path)
    _require_columns(coverage, COVERAGE_COLUMNS, "source coverage")
    coverage = coverage.copy()
    coverage["coverage_start"] = pd.to_datetime(coverage["coverage_start"], errors="raise", utc=True)
    coverage["coverage_end"] = pd.to_datetime(coverage["coverage_end"], errors="raise", utc=True)
    if coverage["coverage_id"].duplicated().any():
        raise ValueError("source coverage contains duplicate coverage_id values.")
    if not coverage["time_zone"].eq("UTC").all():
        raise ValueError("All source coverage periods must use UTC.")
    if not coverage["coverage_status"].isin({"demonstrated", "unknown"}).all():
        raise ValueError("coverage_status must be demonstrated or unknown.")
    if not coverage["coverage_start"].lt(coverage["coverage_end"]).all():
        raise ValueError("Every source coverage period must have start before end.")
    if not coverage["scope_type"].isin({"global", "region", "crossing"}).all():
        raise ValueError("scope_type must be global, region, or crossing.")
    return coverage.sort_values("coverage_id", kind="stable").reset_index(drop=True)


def load_cohort(path: Path) -> pd.DataFrame:
    cohort = _read_table(path)
    _require_columns(cohort, COHORT_COLUMNS, "historical cohort")
    cohort = cohort.copy()
    cohort["norm_crossing_id"] = cohort["norm_crossing_id"].astype("string").str.strip().str.upper()
    cohort["eligibility_as_of"] = pd.to_datetime(cohort["eligibility_as_of"], errors="raise", utc=True)
    if cohort["norm_crossing_id"].duplicated().any():
        raise ValueError("Historical cohort contains duplicate norm_crossing_id values.")
    return cohort.sort_values("norm_crossing_id").reset_index(drop=True)


def _coverage_for_interval(
    crossing_id: str,
    region_ids: set[str],
    interval_start: pd.Timestamp,
    interval_end: pd.Timestamp,
    coverage: pd.DataFrame,
) -> str | None:
    applicable = coverage.loc[
        coverage["coverage_status"].eq("demonstrated")
        & coverage["coverage_start"].le(interval_start)
        & coverage["coverage_end"].ge(interval_end)
        & (
            coverage["scope_type"].eq("global")
            | (coverage["scope_type"].eq("crossing") & coverage["scope_id"].eq(crossing_id))
            | (coverage["scope_type"].eq("region") & coverage["scope_id"].isin(region_ids))
        )
    ].copy()
    if applicable.empty:
        return None
    rank = {"crossing": 0, "region": 1, "global": 2}
    applicable["_scope_rank"] = applicable["scope_type"].map(rank)
    return str(applicable.sort_values(["_scope_rank", "coverage_id"]).iloc[0]["coverage_id"])


def build_interval_tables(
    unit_hours: int,
    analysis_start: pd.Timestamp,
    analysis_end: pd.Timestamp,
    cohort: pd.DataFrame,
    membership: pd.DataFrame,
    coverage: pd.DataFrame,
    incidents: pd.DataFrame,
    source: pd.DataFrame,
    pair_decisions: pd.DataFrame,
    exceptions: pd.DataFrame,
    max_rows: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build one scoped candidate-unit exposure using half-open UTC intervals."""

    delta = pd.Timedelta(hours=unit_hours)
    starts = pd.date_range(analysis_start, analysis_end, freq=delta, inclusive="left")
    starts = starts[starts + delta <= analysis_end]
    estimated_rows = sum(int((starts >= row.eligibility_as_of).sum()) for row in cohort.itertuples())
    if estimated_rows > max_rows:
        raise ValueError(
            f"Filtered {unit_hours}h exposure would contain {estimated_rows:,} rows, "
            f"above max_materialized_rows={max_rows:,}. Narrow region, cohort, or dates."
        )

    regions_by_crossing = (
        membership.loc[membership["region_id"].notna()]
        .groupby("norm_crossing_id")["region_id"]
        .agg(lambda values: set(values.astype(str)))
        .to_dict()
    )
    rows: list[dict[str, Any]] = []
    for cohort_row in cohort.itertuples(index=False):
        crossing_id = str(cohort_row.norm_crossing_id)
        for interval_start in starts[starts >= cohort_row.eligibility_as_of]:
            interval_end = interval_start + delta
            coverage_id = _coverage_for_interval(
                crossing_id,
                regions_by_crossing.get(crossing_id, set()),
                interval_start,
                interval_end,
                coverage,
            )
            rows.append(
                {
                    "interval_id": f"INT-{stable_hash(crossing_id, interval_start.isoformat(), unit_hours)}",
                    "crossing_id": crossing_id,
                    "interval_start": interval_start,
                    "interval_end": interval_end,
                    "interval_unit_hours": unit_hours,
                    "time_zone": "UTC",
                    "coverage_id": coverage_id,
                    "eligibility_as_of": cohort_row.eligibility_as_of,
                    "crossing_volume_tier_as_of": getattr(
                        cohort_row, "crossing_volume_tier_as_of", "unknown"
                    ),
                }
            )
    exposure = pd.DataFrame(rows)
    if exposure.empty:
        raise ValueError("The requested region/cohort/window produced no exposure intervals.")

    eligible_incidents = incidents.merge(
        cohort[["norm_crossing_id", "eligibility_as_of"]], on="norm_crossing_id", how="inner"
    )
    eligible_incidents = eligible_incidents.loc[
        eligible_incidents["earliest_reported_at_utc"].ge(eligible_incidents["eligibility_as_of"])
        & eligible_incidents["earliest_reported_at_utc"].ge(analysis_start)
        & eligible_incidents["earliest_reported_at_utc"].lt(analysis_end)
    ].copy()
    offset = (
        (eligible_incidents["earliest_reported_at_utc"] - analysis_start) // delta
    ).astype("int64")
    eligible_incidents["interval_start"] = analysis_start + offset * delta
    incident_crosswalk = eligible_incidents.merge(
        exposure[["interval_id", "crossing_id", "interval_start"]],
        left_on=["norm_crossing_id", "interval_start"],
        right_on=["crossing_id", "interval_start"],
        how="inner",
        validate="many_to_one",
    )[["interval_id", "canonical_incident_id"]].sort_values(
        ["interval_id", "canonical_incident_id"]
    ).reset_index(drop=True)
    incident_positions = eligible_incidents["earliest_reported_at_utc"] - eligible_incidents[
        "interval_start"
    ]
    boundary_distance = np.minimum(
        incident_positions.dt.total_seconds(),
        delta.total_seconds() - incident_positions.dt.total_seconds(),
    )
    incident_counts = incident_crosswalk.groupby("interval_id").size()
    exposure["canonical_incident_count"] = exposure["interval_id"].map(incident_counts).fillna(0).astype(int)
    exposure["point_label"] = np.where(
        exposure["canonical_incident_count"].gt(0),
        "report_observed",
        np.where(exposure["coverage_id"].notna(), "no_report_observed", "unknown"),
    )

    primary = eligible_incidents.merge(
        source[
            [
                "source_row_id", "duration_upper_minutes", "duration_normalization_status",
                "norm_duration",
            ]
        ],
        left_on="primary_source_row_id",
        right_on="source_row_id",
        how="left",
        validate="many_to_one",
    )
    sensitivity_rows: list[dict[str, Any]] = []
    exposure_by_crossing = {key: value for key, value in exposure.groupby("crossing_id", sort=False)}
    for incident in primary.itertuples(index=False):
        if pd.isna(incident.duration_upper_minutes):
            continue
        proxy_end = incident.earliest_reported_at_utc + pd.Timedelta(
            minutes=int(incident.duration_upper_minutes)
        )
        candidate_intervals = exposure_by_crossing.get(str(incident.norm_crossing_id))
        if candidate_intervals is None:
            continue
        overlaps = candidate_intervals.loc[
            candidate_intervals["interval_start"].lt(proxy_end)
            & candidate_intervals["interval_end"].gt(incident.earliest_reported_at_utc)
        ]
        for interval in overlaps.itertuples(index=False):
            sensitivity_rows.append(
                {
                    "interval_id": interval.interval_id,
                    "canonical_incident_id": incident.canonical_incident_id,
                    "proxy_start": incident.earliest_reported_at_utc,
                    "proxy_end": proxy_end,
                    "duration_category": incident.norm_duration,
                    "interpretation": "maximum_duration_proxy",
                }
            )
    duration_sensitivity = pd.DataFrame(
        sensitivity_rows,
        columns=[
            "interval_id", "canonical_incident_id", "proxy_start", "proxy_end",
            "duration_category", "interpretation",
        ],
    )
    duration_ids = set(duration_sensitivity["interval_id"])
    uncertain_incident_ids = set(
        pair_decisions.loc[pair_decisions["uncertainty_flag"], "left_final_incident_id"].dropna().astype(str)
    ) | set(
        pair_decisions.loc[pair_decisions["uncertainty_flag"], "right_final_incident_id"].dropna().astype(str)
    )
    uncertain_interval_ids = set(
        incident_crosswalk.loc[
            incident_crosswalk["canonical_incident_id"].isin(uncertain_incident_ids), "interval_id"
        ]
    )
    exception_interval_ids: set[str] = set()
    assignable_exceptions = exceptions.dropna(subset=["norm_crossing_id", "reported_at_utc"])
    if not assignable_exceptions.empty:
        for exception in assignable_exceptions.itertuples(index=False):
            candidate = exposure_by_crossing.get(str(exception.norm_crossing_id))
            if candidate is not None:
                matched = candidate.loc[
                    candidate["interval_start"].le(exception.reported_at_utc)
                    & candidate["interval_end"].gt(exception.reported_at_utc)
                ]
                exception_interval_ids.update(matched["interval_id"])
    exposure["has_duration_overlap_sensitivity"] = exposure["interval_id"].isin(duration_ids)
    exposure["has_pair_decision_uncertainty"] = exposure["interval_id"].isin(uncertain_interval_ids)
    exposure["has_exception_uncertainty"] = exposure["interval_id"].isin(exception_interval_ids)

    metrics = {
        "interval_unit_hours": unit_hours,
        "total_intervals": len(exposure),
        "crossings": int(exposure["crossing_id"].nunique()),
        "report_observed": int(exposure["point_label"].eq("report_observed").sum()),
        "no_report_observed": int(exposure["point_label"].eq("no_report_observed").sum()),
        "unknown": int(exposure["point_label"].eq("unknown").sum()),
        "multiple_incident_intervals": int(exposure["canonical_incident_count"].gt(1).sum()),
        "distinct_incidents": int(incident_crosswalk["canonical_incident_id"].nunique()),
        "incidents_within_five_minutes_of_boundary": int(boundary_distance.le(300).sum()),
        "duration_sensitivity_intervals": int(exposure["has_duration_overlap_sensitivity"].sum()),
        "duration_only_sensitivity_intervals": int(
            (
                exposure["has_duration_overlap_sensitivity"]
                & exposure["point_label"].ne("report_observed")
            ).sum()
        ),
        "pair_uncertainty_intervals": int(exposure["has_pair_decision_uncertainty"].sum()),
        "exception_uncertainty_intervals": int(exposure["has_exception_uncertainty"].sum()),
        "natural_report_prevalence": float(
            exposure["point_label"].eq("report_observed").sum()
            / max(exposure["point_label"].ne("unknown").sum(), 1)
        ),
        "estimated_memory_bytes": int(exposure.memory_usage(index=True, deep=True).sum()),
    }
    return exposure, incident_crosswalk, duration_sensitivity, metrics


def build_uncertainty_diagnostics(
    exposure: pd.DataFrame,
    membership: pd.DataFrame,
    exceptions: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize labels and uncertainty by required analysis dimensions."""

    work = exposure.copy()
    work["year"] = work["interval_start"].dt.year.astype(str)
    rows: list[dict[str, Any]] = []

    def append_groups(frame: pd.DataFrame, dimension: str, column: str) -> None:
        for value, group in frame.groupby(column, dropna=False, sort=True):
            rows.append(
                {
                    "dimension": dimension,
                    "dimension_value": str(value),
                    "total_intervals": len(group),
                    "report_observed": int(group["point_label"].eq("report_observed").sum()),
                    "no_report_observed": int(group["point_label"].eq("no_report_observed").sum()),
                    "unknown": int(group["point_label"].eq("unknown").sum()),
                    "duration_overlap": int(group["has_duration_overlap_sensitivity"].sum()),
                    "pair_decision_uncertainty": int(group["has_pair_decision_uncertainty"].sum()),
                    "exception_uncertainty": int(group["has_exception_uncertainty"].sum()),
                    "source_exception_records": 0,
                }
            )

    overall = work.assign(_overall="all")
    append_groups(overall, "overall", "_overall")
    append_groups(work, "year", "year")
    append_groups(work, "crossing", "crossing_id")
    append_groups(work, "crossing_volume_tier", "crossing_volume_tier_as_of")
    region_rows = work.merge(
        membership.loc[membership["region_id"].notna(), ["norm_crossing_id", "region_id"]],
        left_on="crossing_id",
        right_on="norm_crossing_id",
        how="inner",
    )
    if not region_rows.empty:
        append_groups(region_rows, "region", "region_id")
    rows.append(
        {
            "dimension": "source_exception",
            "dimension_value": "unassignable",
            "total_intervals": 0,
            "report_observed": 0,
            "no_report_observed": 0,
            "unknown": 0,
            "duration_overlap": 0,
            "pair_decision_uncertainty": 0,
            "exception_uncertainty": 0,
            "source_exception_records": len(
                exceptions.loc[
                    exceptions["reported_at_utc"].isna() | exceptions["norm_crossing_id"].isna()
                ]
            ),
        }
    )
    return pd.DataFrame(rows)


def deterministic_training_sample(
    exposure: pd.DataFrame, negative_fraction: float, seed: int
) -> pd.DataFrame:
    """Keep positives and deterministically sample covered no-report intervals."""

    if not 0 < negative_fraction <= 1:
        raise ValueError("negative_fraction must be in (0, 1].")
    _require_columns(
        exposure, {"interval_id", "crossing_id", "interval_start", "point_label"}, "exposure"
    )
    eligible = exposure.loc[exposure["point_label"].ne("unknown")].copy()
    eligible["sampling_stratum"] = (
        eligible["crossing_id"].astype(str)
        + "|"
        + eligible["interval_start"].dt.year.astype(str)
    )
    positives = eligible.loc[eligible["point_label"].eq("report_observed")].copy()
    negatives = eligible.loc[eligible["point_label"].eq("no_report_observed")].copy()
    threshold = int(negative_fraction * (2**64 - 1))
    hashes = negatives["interval_id"].map(
        lambda value: int(stable_hash(seed, value, length=16), 16)
    )
    sampled_negatives = negatives.loc[hashes.le(threshold)].copy()
    positives["inclusion_probability"] = 1.0
    positives["sample_weight"] = 1.0
    sampled_negatives["inclusion_probability"] = negative_fraction
    sampled_negatives["sample_weight"] = 1.0 / negative_fraction
    return pd.concat([positives, sampled_negatives], ignore_index=True).sort_values(
        "interval_id"
    ).reset_index(drop=True)


def _git_metadata(repo_root: Path) -> dict[str, str | None]:
    def run(*args: str) -> str | None:
        try:
            return subprocess.check_output(
                args, cwd=repo_root, text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "commit": run("git", "rev-parse", "HEAD"),
        "dirty_worktree": run("git", "status", "--porcelain"),
    }


def compare_phase2_outputs(first_dir: Path, second_dir: Path) -> dict[str, Any]:
    """Compare deterministic Phase 2 artifacts while ignoring execution metadata."""

    first_dir, second_dir = Path(first_dir), Path(second_dir)
    first_files = {path.name for path in first_dir.iterdir() if path.is_file()}
    second_files = {path.name for path in second_dir.iterdir() if path.is_file()}
    mismatches: list[str] = []
    if first_files != second_files:
        mismatches.append(
            f"artifact sets differ: first_only={sorted(first_files-second_files)}, "
            f"second_only={sorted(second_files-first_files)}"
        )
    for name in sorted(first_files & second_files):
        left, right = first_dir / name, second_dir / name
        try:
            if name == "run_manifest.json":
                left_value = json.loads(left.read_text(encoding="utf-8"))
                right_value = json.loads(right.read_text(encoding="utf-8"))
                left_value.pop("execution", None)
                right_value.pop("execution", None)
                for value in (left_value, right_value):
                    value.pop("git", None)
                    value["configuration"].pop("path", None)
                    for details in value.get("inputs", {}).values():
                        if isinstance(details, dict):
                            details.pop("path", None)
                if left_value != right_value:
                    raise AssertionError("normalized manifests differ")
            elif left.suffix == ".json":
                if json.loads(left.read_text(encoding="utf-8")) != json.loads(right.read_text(encoding="utf-8")):
                    raise AssertionError("JSON values differ")
            elif left.suffix == ".parquet":
                pd.testing.assert_frame_equal(pd.read_parquet(left), pd.read_parquet(right))
            elif left.suffix == ".csv":
                pd.testing.assert_frame_equal(pd.read_csv(left), pd.read_csv(right))
        except Exception as exc:
            mismatches.append(f"{name}: {exc}")
    return {"passed": not mismatches, "mismatches": mismatches}


def _artifact_metadata(artifact_paths: dict[str, Path]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for name, path in artifact_paths.items():
        if path.suffix == ".parquet" and path.exists():
            frame = pd.read_parquet(path)
            metadata[name] = {
                "filename": path.name,
                "row_count": len(frame),
                "schema": list(frame.columns),
            }
        elif path.suffix == ".csv" and path.exists():
            frame = pd.read_csv(path)
            metadata[name] = {
                "filename": path.name,
                "row_count": len(frame),
                "schema": list(frame.columns),
            }
        else:
            metadata[name] = {"filename": path.name, "row_count": None, "schema": None}
    return metadata


def run_phase_2(phase_1_dir: Path, config_path: Path, output_dir: Path) -> Phase2Result:
    """Run Phase 2 or emit an incomplete gate when required policy inputs are absent."""

    started = time.monotonic()
    phase_1_dir, config_path, output_dir = map(Path, (phase_1_dir, config_path, output_dir))
    output_dir.mkdir(parents=True, exist_ok=True)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    repo_root = config_path.resolve().parent.parent
    frames, phase1_evidence = validate_phase1_handoff(
        phase_1_dir, config["expected_phase1_ruleset"]
    )
    blockers = phase2_preflight_blockers(config, repo_root)
    gate_path = output_dir / "phase_2_gate_report.json"
    manifest_path = output_dir / "run_manifest.json"
    artifact_paths = {"phase_2_gate_report": gate_path, "run_manifest": manifest_path}
    if blockers:
        gate = {
            "status": "incomplete",
            "phase1_handoff": phase1_evidence,
            "blockers": blockers,
            "decisions": {},
        }
        manifest = {
            "pipeline_version": config["pipeline_version"],
            "phase1_handoff": phase1_evidence,
            "inputs": {
                "phase1_dir": str(phase_1_dir),
                "inventory": config.get("form71_inventory_path"),
                "coverage": config.get("source_coverage_path"),
                "cohort": config.get("cohort_path"),
                "region_source": config.get("region_source", {}).get("path"),
            },
            "configuration": {"path": str(config_path), "sha256": file_sha256(config_path)},
            "environment": {
                "python": sys.version,
                "platform": platform.platform(),
                "pandas": pd.__version__,
                "geopandas": gpd.__version__,
            },
            "git": _git_metadata(repo_root),
            "outputs": {
                "phase_2_gate_report": {"filename": gate_path.name, "row_count": None, "schema": None},
                "run_manifest": {"filename": manifest_path.name, "row_count": None, "schema": None},
            },
            "validation": {"phase1_handoff_valid": True, "preflight_complete": False},
            "execution": {
                "timestamp_utc": pd.Timestamp.now(tz=timezone.utc).isoformat(),
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        }
        _write_json(gate_path, gate)
        _write_json(manifest_path, manifest)
        return Phase2Result(
            artifact_paths=artifact_paths,
            summary={"status": "incomplete", "blockers": blockers},
            validations=manifest["validation"],
            gate=gate,
        )

    inventory_path = _resolve_path(repo_root, config["form71_inventory_path"])
    coverage_path = _resolve_path(repo_root, config["source_coverage_path"])
    cohort_path = _resolve_path(repo_root, config["cohort_path"])
    assert inventory_path is not None and coverage_path is not None and cohort_path is not None
    crossings, inventory_diagnostics = build_canonical_crossings(
        inventory_path, frames["incidents"], config
    )
    region_definitions, membership, region_diagnostics = build_region_membership(
        crossings, config["region_source"], repo_root
    )
    coverage = load_coverage(coverage_path)
    cohort = load_cohort(cohort_path)
    requested = set(config["requested_region_ids"])
    region_crossings = set(
        membership.loc[membership["region_id"].isin(requested), "norm_crossing_id"].astype(str)
    )
    cohort = cohort.loc[cohort["norm_crossing_id"].isin(region_crossings)].copy()
    configured_as_of = pd.Timestamp(config["eligibility_as_of"])
    configured_as_of = configured_as_of.tz_localize("UTC") if configured_as_of.tzinfo is None else configured_as_of.tz_convert("UTC")
    if cohort.empty:
        raise ValueError("Requested regions and historical cohort have no crossings in common.")
    if not cohort["eligibility_as_of"].eq(configured_as_of).all():
        raise ValueError("Every cohort eligibility_as_of must equal the configured value.")
    historical_incidents = frames["incidents"].merge(
        cohort[["norm_crossing_id", "eligibility_as_of"]],
        on="norm_crossing_id",
        how="inner",
    )
    historical_counts = (
        historical_incidents.loc[
            historical_incidents["earliest_reported_at_utc"].lt(
                historical_incidents["eligibility_as_of"]
            )
        ]
        .groupby("norm_crossing_id")
        .size()
    )
    cohort["historical_report_count_as_of"] = (
        cohort["norm_crossing_id"].map(historical_counts).fillna(0).astype(int)
    )
    cohort["crossing_volume_tier_as_of"] = cohort["historical_report_count_as_of"].map(
        _volume_tier
    )

    analysis_start = pd.Timestamp(config["analysis_window"]["start"])
    analysis_end = pd.Timestamp(config["analysis_window"]["end"])
    analysis_start = analysis_start.tz_localize("UTC") if analysis_start.tzinfo is None else analysis_start.tz_convert("UTC")
    analysis_end = analysis_end.tz_localize("UTC") if analysis_end.tzinfo is None else analysis_end.tz_convert("UTC")
    if analysis_start >= analysis_end:
        raise ValueError("analysis_window start must precede end.")

    comparisons: list[dict[str, Any]] = []
    selected_outputs: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None = None
    selected_value = config.get("selected_interval_unit_hours")
    selected_unit = int(selected_value) if selected_value is not None else None
    if selected_unit is not None and selected_unit not in config["candidate_interval_units_hours"]:
        raise ValueError("selected_interval_unit_hours is not one of the candidate units.")
    for unit in config["candidate_interval_units_hours"]:
        outputs = build_interval_tables(
            int(unit), analysis_start, analysis_end, cohort, membership, coverage,
            frames["incidents"], frames["source"], frames["pair_decisions"],
            frames["exceptions"], int(config["max_materialized_rows"]),
        )
        comparisons.append(outputs[3])
        if int(unit) == selected_unit:
            selected_outputs = outputs[:3]

    artifact_paths.update(
        {
            "canonical_crossings": output_dir / "canonical_crossings.parquet",
            "source_coverage_periods": output_dir / "source_coverage_periods.parquet",
            "region_definitions": output_dir / "region_definitions.json",
            "crossing_region_membership": output_dir / "crossing_region_membership.parquet",
            "region_assignment_diagnostics": output_dir / "region_assignment_diagnostics.csv",
            "interval_unit_comparison": output_dir / "interval_unit_comparison.csv",
        }
    )
    crossings.to_parquet(artifact_paths["canonical_crossings"], index=False)
    coverage.to_parquet(artifact_paths["source_coverage_periods"], index=False)
    _write_json(artifact_paths["region_definitions"], {"regions": region_definitions})
    membership.to_parquet(artifact_paths["crossing_region_membership"], index=False)
    pd.concat(
        [
            inventory_diagnostics.assign(diagnostic_group="inventory"),
            region_diagnostics.rename(
                columns={"assignment_status": "metric", "membership_rows": "count"}
            ).assign(diagnostic_group="region"),
        ],
        ignore_index=True,
        sort=False,
    ).to_csv(artifact_paths["region_assignment_diagnostics"], index=False)
    pd.DataFrame(comparisons).to_csv(artifact_paths["interval_unit_comparison"], index=False)

    if selected_outputs is None:
        blockers = ["BP-04: selected interval unit is pending comparison review"]
        validations = {
            "phase1_handoff_valid": True,
            "preflight_complete": True,
            "candidate_units_compared": len(comparisons)
            == len(config["candidate_interval_units_hours"]),
            "selected_interval_unit_recorded": False,
        }
        gate = {
            "status": "incomplete",
            "phase1_handoff": phase1_evidence,
            "blockers": blockers,
            "decisions": {
                "selected_interval_unit_hours": None,
                "adjacent_interval_treatment": "primary_point_label_plus_separate_duration_sensitivity",
            },
            "validation": validations,
        }
        _write_json(gate_path, gate)
        manifest = {
            "pipeline_version": config["pipeline_version"],
            "phase1_handoff": phase1_evidence,
            "configuration": {"path": str(config_path), "sha256": file_sha256(config_path)},
            "outputs": _artifact_metadata(artifact_paths),
            "validation": validations,
            "execution": {
                "timestamp_utc": pd.Timestamp.now(tz=timezone.utc).isoformat(),
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        }
        _write_json(manifest_path, manifest)
        return Phase2Result(
            artifact_paths=artifact_paths,
            summary={"status": "incomplete", "blockers": blockers, "candidate_unit_comparisons": comparisons},
            validations=validations,
            gate=gate,
        )

    exposure, interval_crosswalk, duration_sensitivity = selected_outputs

    sampling = config["sampling"]
    selected_strategy = sampling.get("selected_strategy")
    if selected_strategy not in {None, "full_filtered_exposure", "weighted_sample"}:
        raise ValueError("selected_strategy must be full_filtered_exposure, weighted_sample, or null.")
    sample = deterministic_training_sample(
        exposure, float(sampling["negative_fraction"]), int(sampling["seed"])
    )
    no_report_count = int(exposure["point_label"].eq("no_report_observed").sum())
    weighted_no_report = float(
        sample.loc[sample["point_label"].eq("no_report_observed"), "sample_weight"].sum()
    )
    exposure_strategy = {
        "selected_strategy": selected_strategy,
        "full_filtered_rows": len(exposure),
        "sample_rows": len(sample),
        "negative_fraction": sampling["negative_fraction"],
        "weighted_no_report_estimate": weighted_no_report,
        "actual_no_report_count": no_report_count,
        "absolute_error": abs(weighted_no_report - no_report_count),
        "by_sampling_stratum": [],
    }
    full_strata = (
        exposure.loc[exposure["point_label"].eq("no_report_observed")]
        .assign(
            sampling_stratum=lambda frame: frame["crossing_id"].astype(str)
            + "|"
            + frame["interval_start"].dt.year.astype(str)
        )
        .groupby("sampling_stratum")
        .size()
        .rename("actual_no_report_count")
    )
    sampled_strata = (
        sample.loc[sample["point_label"].eq("no_report_observed")]
        .groupby("sampling_stratum")["sample_weight"]
        .sum()
        .rename("weighted_no_report_estimate")
    )
    stratum_comparison = pd.concat([full_strata, sampled_strata], axis=1).fillna(0).reset_index()
    stratum_comparison["absolute_error"] = (
        stratum_comparison["weighted_no_report_estimate"]
        - stratum_comparison["actual_no_report_count"]
    ).abs()
    exposure_strategy["by_sampling_stratum"] = stratum_comparison.to_dict("records")
    uncertainty = build_uncertainty_diagnostics(
        exposure, membership, frames["exceptions"]
    )
    duration_source_diagnostics = pd.DataFrame(
        [
            {
                "dimension": "source_duration",
                "dimension_value": "open_ended_proxy",
                "source_records": int(
                    frames["source"]["norm_duration"].eq("More than one day").sum()
                ),
            },
            {
                "dimension": "source_duration",
                "dimension_value": "unmapped_proxy",
                "source_records": int(
                    frames["source"]["duration_normalization_status"].eq("unmapped").sum()
                ),
            },
        ]
    )
    uncertainty = pd.concat([uncertainty, duration_source_diagnostics], ignore_index=True)

    artifact_paths.update(
        {
            "interval_exposure": output_dir / "interval_exposure.parquet",
            "interval_incident_crosswalk": output_dir / "interval_incident_crosswalk.parquet",
            "duration_overlap_sensitivity": output_dir / "duration_overlap_sensitivity.parquet",
            "uncertainty_diagnostics": output_dir / "uncertainty_diagnostics.csv",
            "exposure_strategy_comparison": output_dir / "exposure_strategy_comparison.json",
        }
    )
    if selected_strategy == "weighted_sample":
        artifact_paths["training_exposure_sample"] = output_dir / "training_exposure_sample.parquet"

    exposure.to_parquet(artifact_paths["interval_exposure"], index=False)
    interval_crosswalk.to_parquet(artifact_paths["interval_incident_crosswalk"], index=False)
    duration_sensitivity.to_parquet(artifact_paths["duration_overlap_sensitivity"], index=False)
    uncertainty.to_csv(artifact_paths["uncertainty_diagnostics"], index=False)
    _write_json(artifact_paths["exposure_strategy_comparison"], exposure_strategy)
    if "training_exposure_sample" in artifact_paths:
        sample.to_parquet(artifact_paths["training_exposure_sample"], index=False)

    validations = {
        "phase1_handoff_valid": True,
        "preflight_complete": True,
        "labels_use_allowed_vocabulary": exposure["point_label"].isin(
            {"report_observed", "no_report_observed", "unknown"}
        ).all(),
        "no_report_requires_coverage": exposure.loc[
            exposure["point_label"].eq("no_report_observed"), "coverage_id"
        ].notna().all(),
        "interval_incident_lineage_valid": interval_crosswalk["canonical_incident_id"].isin(
            frames["incidents"]["canonical_incident_id"]
        ).all(),
        "requested_region_filter_applied": set(cohort["norm_crossing_id"]).issubset(region_crossings),
        "eligibility_as_of_applied": exposure["interval_start"].ge(exposure["eligibility_as_of"]).all(),
        "exposure_strategy_selected": selected_strategy is not None,
        "weighted_summary_within_tolerance": (
            True
            if selected_strategy != "weighted_sample"
            else (
                exposure_strategy["absolute_error"]
                <= float(sampling["weighted_summary_absolute_tolerance"])
                and stratum_comparison["absolute_error"]
                .le(float(sampling["weighted_summary_absolute_tolerance"]))
                .all()
            )
        ),
    }
    required_validations = {
        key: value for key, value in validations.items() if key != "exposure_strategy_selected"
    }
    if not all(required_validations.values()):
        raise AssertionError(f"Phase 2 validation failed: {validations}")

    completion_blockers = [] if selected_strategy is not None else [
        "BP-06: exposure strategy is pending comparison review"
    ]
    gate = {
        "status": "complete" if not completion_blockers else "incomplete",
        "phase1_handoff": phase1_evidence,
        "blockers": completion_blockers,
        "decisions": {
            "selected_interval_unit_hours": selected_unit,
            "adjacent_interval_treatment": "primary_point_label_plus_separate_duration_sensitivity",
            "exposure_strategy": selected_strategy,
            "region_source_version": config["region_source"]["source_version"],
        },
        "validation": validations,
    }
    _write_json(gate_path, gate)

    output_metadata = _artifact_metadata(artifact_paths)
    manifest = {
        "pipeline_version": config["pipeline_version"],
        "phase1_handoff": phase1_evidence,
        "inputs": {
            "phase1_dir": str(phase_1_dir),
            "inventory": {"path": str(inventory_path), "sha256": file_sha256(inventory_path)},
            "coverage": {"path": str(coverage_path), "sha256": file_sha256(coverage_path)},
            "cohort": {"path": str(cohort_path), "sha256": file_sha256(cohort_path)},
            "region_source": {"path": str(_resolve_path(repo_root, config["region_source"]["path"])), "sha256": file_sha256(_resolve_path(repo_root, config["region_source"]["path"]))},
        },
        "configuration": {"path": str(config_path), "sha256": file_sha256(config_path)},
        "environment": {"python": sys.version, "platform": platform.platform(), "pandas": pd.__version__, "geopandas": gpd.__version__},
        "git": _git_metadata(repo_root),
        "execution": {"timestamp_utc": pd.Timestamp.now(tz=timezone.utc).isoformat(), "duration_seconds": round(time.monotonic() - started, 3)},
        "outputs": output_metadata,
        "validation": validations,
    }
    _write_json(manifest_path, manifest)
    summary = {
        "status": gate["status"],
        "selected_interval_unit_hours": selected_unit,
        "selected_exposure_rows": len(exposure),
        "point_label_counts": exposure["point_label"].value_counts().to_dict(),
        "candidate_unit_comparisons": comparisons,
    }
    return Phase2Result(artifact_paths=artifact_paths, summary=summary, validations=validations, gate=gate)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 2 reported-event exposure construction.")
    parser.add_argument("--phase-1-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = run_phase_2(args.phase_1_dir, args.config, args.output_dir)
    print(json.dumps(result.summary, indent=2, default=str))


if __name__ == "__main__":
    main()
