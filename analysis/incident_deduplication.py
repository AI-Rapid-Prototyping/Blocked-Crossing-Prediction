"""Auditable Phase 1 incident consolidation and crossing-local time enrichment.

The pipeline deliberately consolidates only full-row exact and normalized-exact
reports.  UTC timestamps remain the authoritative comparison timestamp; local
time is a derived, coordinate-backed reporting field.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np


INVENTORY_REQUIRED_COLUMNS = ("Crossing ID", "Latitude", "Longitude", "Revision Date")
STEP_5_CHECKPOINT_FORMAT_VERSION = 1
STEP_5_CHECKPOINT_FILES = {
    "source": "source.parquet",
    "reconciliation": "reconciliation.parquet",
    "crossing_timezones": "crossing_timezones.parquet",
    "incidents": "incidents.parquet",
    "crosswalk": "crosswalk.parquet",
    "exceptions": "exceptions.parquet",
}


@dataclass(frozen=True)
class Phase1Result:
    """Compact result returned by :func:`run_phase_1`."""

    artifact_paths: dict[str, Path]
    summary: dict[str, Any]
    validations: dict[str, Any]


def stable_hash(*parts: object, length: int = 20) -> str:
    payload = "\x1f".join("<NULL>" if value is None else str(value) for value in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return a content hash used to prove an input was not changed by a run."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_input_files(input_paths: dict[str, Path]) -> dict[str, str]:
    """Hash named inputs without treating the hashes as a permanent allowlist."""

    require_input_files(input_paths)
    return {name: file_sha256(Path(path)) for name, path in sorted(input_paths.items())}


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open(encoding="utf-8") as handle:
        return json.load(handle)


def require_input_files(input_paths: dict[str, Path]) -> None:
    """Raise one actionable error containing every missing input path."""

    missing = [
        (label, Path(path))
        for label, path in input_paths.items()
        if not Path(path).is_file()
    ]
    if missing:
        details = "\n".join(f"- {label}: {path}" for label, path in missing)
        raise FileNotFoundError(f"Required Phase 1 input files are missing:\n{details}")


def _null_aware_raw_value(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def raw_signature(row: pd.Series, columns: list[str]) -> str:
    values = [_null_aware_raw_value(row[column]) for column in columns]
    return stable_hash(json.dumps(values, ensure_ascii=False, default=str), length=64)


def normalize_comparison_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.casefold()
    )


def _require_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    duplicated = frame.columns[frame.columns.duplicated()].tolist()
    missing = [column for column in columns if column not in frame.columns]
    if duplicated or missing:
        detail = []
        if missing:
            detail.append(f"missing columns: {missing}")
        if duplicated:
            detail.append(f"duplicate columns: {duplicated}")
        raise ValueError(f"{label} schema is invalid ({'; '.join(detail)}).")


def normalize_source_dataframe(
    frame: pd.DataFrame,
    config: dict[str, Any],
    source_sheet: str,
    source_name: str,
) -> pd.DataFrame:
    """Preserve raw values while adding deterministic comparison fields."""

    material = config["material_columns"]
    _require_columns(frame, material, source_name)
    result = frame.copy()
    result.insert(0, "source_name", source_name)
    result.insert(1, "source_sheet", source_sheet)
    result.insert(2, "source_excel_row_number", range(2, len(result) + 2))
    result["raw_full_row_signature"] = result.apply(raw_signature, axis=1, columns=material)
    result.insert(
        3,
        "source_row_id",
        [
            f"SRC-{stable_hash(source_name, source_sheet, row_number, row_signature, length=20)}"
            for row_number, row_signature in zip(
                result["source_excel_row_number"], result["raw_full_row_signature"]
            )
        ],
    )

    raw_crossing = result["Crossing ID"].astype("string")
    result["norm_crossing_id"] = raw_crossing.str.strip().str.upper()
    missing_crossing = raw_crossing.isna() | result["norm_crossing_id"].eq("")
    result["crossing_id_status"] = "valid"
    result.loc[missing_crossing, "crossing_id_status"] = "missing"
    result.loc[
        ~missing_crossing
        & ~result["norm_crossing_id"].str.match(config["crossing_id_pattern"], na=False),
        "crossing_id_status",
    ] = "invalid_format"

    # The source contract supplied for this remediation declares these values UTC.
    result["reported_at_utc"] = pd.to_datetime(result["Date/Time"], errors="coerce", utc=True)
    result["timestamp_status"] = "valid"
    result.loc[result["reported_at_utc"].isna(), "timestamp_status"] = "invalid_or_missing"
    result["norm_datetime_utc"] = result["reported_at_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    duration_raw = result["Duration"].astype("string")
    canonical_duration = duration_raw.map(config["duration_aliases"]).fillna(duration_raw)
    known_duration = canonical_duration.isin(config["duration_categories"])
    result["norm_duration"] = canonical_duration.where(known_duration)
    result["duration_normalization_status"] = "unmapped"
    result.loc[known_duration, "duration_normalization_status"] = "canonical"
    result.loc[duration_raw.isin(config["duration_aliases"]), "duration_normalization_status"] = "known_alias"
    result["duration_lower_minutes"] = result["norm_duration"].map(
        {key: value[0] for key, value in config["duration_categories"].items()}
    ).astype("Int64")
    result["duration_upper_minutes"] = result["norm_duration"].map(
        {key: value[1] for key, value in config["duration_categories"].items()}
    ).astype("Int64")

    comparison_columns: list[str] = []
    for column in material:
        comparison_column = f"comparison__{column}"
        comparison_columns.append(comparison_column)
        if column == "Date/Time":
            result[comparison_column] = result["norm_datetime_utc"].fillna("<INVALID_TIMESTAMP>")
        elif column == "Duration":
            # Compare known aliases by their canonical category while retaining
            # normalized raw values for unmapped durations.
            result[comparison_column] = normalize_comparison_text(canonical_duration)
        else:
            result[comparison_column] = normalize_comparison_text(result[column])
    result["normalized_full_row_signature"] = result[comparison_columns].agg("\x1f".join, axis=1).map(
        lambda value: stable_hash(value, length=64)
    )
    result["outside_named_source_period"] = result["reported_at_utc"].dt.date.eq(pd.Timestamp("2026-01-01").date())
    return result


def _load_timezone_finder() -> Any:
    try:
        from timezonefinder import TimezoneFinder
    except ImportError as exc:  # pragma: no cover - exercised by a clear runtime message
        raise RuntimeError(
            "timezonefinder is required for crossing-local time conversion. "
            "Install the locked project dependencies before running Phase 1."
        ) from exc
    return TimezoneFinder(in_memory=True)


def build_crossing_timezones(
    inventory: pd.DataFrame,
    config: dict[str, Any],
    timezone_resolver: Callable[[float, float], str | None] | None = None,
) -> pd.DataFrame:
    """Make a deterministic current-inventory crossing-to-IANA-timezone lookup."""

    _require_columns(inventory, list(INVENTORY_REQUIRED_COLUMNS), "Form 71 inventory")
    cols = config["inventory_columns"]
    lookup = inventory.copy().reset_index(names="inventory_source_row_number")
    lookup["norm_crossing_id"] = lookup[cols["crossing_id"]].astype("string").str.strip().str.upper()
    lookup["inventory_revision_date"] = pd.to_datetime(lookup[cols["revision_date"]], errors="coerce")
    lookup["latitude"] = pd.to_numeric(lookup[cols["latitude"]], errors="coerce")
    lookup["longitude"] = pd.to_numeric(lookup[cols["longitude"]], errors="coerce")
    valid_coordinates = lookup["latitude"].between(-90, 90) & lookup["longitude"].between(-180, 180)
    lookup["inventory_coordinate_status"] = "invalid_inventory_coordinates"
    lookup.loc[valid_coordinates, "inventory_coordinate_status"] = "valid_coordinates"
    lookup = lookup.sort_values(
        ["norm_crossing_id", "inventory_revision_date", "inventory_source_row_number"],
        kind="stable",
    )
    # A current valid coordinate is preferred. If none exists, retain the latest
    # invalid record so affected reports are distinguished from missing inventory.
    latest_valid = lookup[valid_coordinates].drop_duplicates("norm_crossing_id", keep="last")
    latest_any = lookup.drop_duplicates("norm_crossing_id", keep="last")
    lookup = pd.concat(
        [latest_valid, latest_any[~latest_any["norm_crossing_id"].isin(latest_valid["norm_crossing_id"])]],
        ignore_index=True,
    )

    if timezone_resolver is None:
        finder = _load_timezone_finder()
        timezone_resolver = lambda latitude, longitude: finder.timezone_at(lat=latitude, lng=longitude)
        
    lookup["iana_time_zone"] = pd.NA
    valid_rows = lookup["inventory_coordinate_status"].eq("valid_coordinates")

    if valid_rows.any():
        # Deduplicate coordinates for fast timezone lookups
        valid_coords = lookup.loc[valid_rows, ["latitude", "longitude"]].drop_duplicates()
        valid_coords["tz_resolved"] = [
            timezone_resolver(float(lat), float(lng))
            for lat, lng in zip(valid_coords["latitude"], valid_coords["longitude"])
        ]
        
        # Map resolved timezones back using a series mapping to avoid overwriting lookup
        coord_map = valid_coords.set_index(["latitude", "longitude"])["tz_resolved"].to_dict()
        lookup.loc[valid_rows, "iana_time_zone"] = [
            coord_map.get((lat, lng)) 
            for lat, lng in zip(lookup.loc[valid_rows, "latitude"], lookup.loc[valid_rows, "longitude"])
        ]

    lookup["timezone_assignment_status"] = "invalid_inventory_coordinates"
    lookup.loc[valid_rows, "timezone_assignment_status"] = "assigned"
    lookup.loc[valid_rows & lookup["iana_time_zone"].isna(), "timezone_assignment_status"] = "timezone_not_found"
    
    return lookup[
        [
            "norm_crossing_id",
            "latitude",
            "longitude",
            "inventory_revision_date",
            "inventory_source_row_number",
            "inventory_coordinate_status",
            "iana_time_zone",
            "timezone_assignment_status",
        ]
    ].sort_values("norm_crossing_id", kind="stable").reset_index(drop=True)


def enrich_with_local_time(source: pd.DataFrame, crossing_timezones: pd.DataFrame) -> pd.DataFrame:
    """Attach local civil timestamps without changing the UTC comparison timestamp."""

    timezone_columns = [
        column
        for column in crossing_timezones.columns
        if column not in {"norm_crossing_id", "timezone_assignment_status"}
    ]
    result = source.merge(
        crossing_timezones[["norm_crossing_id", "timezone_assignment_status", *timezone_columns]],
        on="norm_crossing_id",
        how="left",
    )
    result["timezone_assignment_status"] = result["timezone_assignment_status"].fillna("no_inventory_match")
    result.loc[result["crossing_id_status"] != "valid", "timezone_assignment_status"] = "invalid_crossing_id"
    result.loc[result["timestamp_status"] != "valid", "timezone_assignment_status"] = "invalid_timestamp"
    result["reported_at_local"] = pd.Series(pd.NA, index=result.index, dtype="string")
    result["utc_offset_minutes"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    result["reported_local_date"] = pd.Series(pd.NA, index=result.index, dtype="string")
    result["reported_local_hour"] = pd.Series(pd.NA, index=result.index, dtype="Int64")

    for zone in sorted(result.loc[result["timezone_assignment_status"].eq("assigned"), "iana_time_zone"].dropna().unique()):
        mask = result["timezone_assignment_status"].eq("assigned") & result["iana_time_zone"].eq(zone)
        local = result.loc[mask, "reported_at_utc"].dt.tz_convert(ZoneInfo(zone))
        result.loc[mask, "reported_at_local"] = local.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        result.loc[mask, "utc_offset_minutes"] = local.map(
            lambda timestamp: timestamp.utcoffset().total_seconds() // 60
        ).astype("Int64")

        result.loc[mask, "reported_local_date"] = local.dt.strftime("%Y-%m-%d")
        result.loc[mask, "reported_local_hour"] = local.dt.hour.astype("Int64")
    return result

def consolidate_reports(source: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Consolidate only full-row exact and normalized-exact reports."""

    valid = source["crossing_id_status"].eq("valid") & source["timestamp_status"].eq("valid")
    invalid = source.loc[~valid].sort_values("source_excel_row_number", kind="stable")
    if invalid.empty:
        exceptions = pd.DataFrame()
        exception_crosswalk = pd.DataFrame()
    else:
        exception_reasons = pd.Series(
            np.where(
                invalid["crossing_id_status"].ne("valid"),
                "invalid_crossing_id",
                "invalid_timestamp",
            ),
            index=invalid.index,
        )
        exception_ids = [
            f"EXC-{stable_hash(source_row_id, reason, length=20)}"
            for source_row_id, reason in zip(invalid["source_row_id"], exception_reasons)
        ]
        exceptions = pd.DataFrame(
            {
                "exception_id": exception_ids,
                "source_row_id": invalid["source_row_id"].to_numpy(),
                "exception_reason": exception_reasons.to_numpy(),
                "norm_crossing_id": invalid["norm_crossing_id"].to_numpy(),
                "reported_at_utc": invalid["reported_at_utc"].array,
            }
        )
        exception_crosswalk = pd.DataFrame(
            {
                "source_row_id": invalid["source_row_id"].to_numpy(),
                "canonical_incident_id": pd.NA,
                "exception_id": exception_ids,
                "consolidation_tier": "exception",
                "is_primary_report": False,
            }
        )

    candidates = source.loc[valid].sort_values("source_excel_row_number", kind="stable")
    if candidates.empty:
        incidents = pd.DataFrame()
        valid_crosswalk = pd.DataFrame()
    else:
        signature_column = "normalized_full_row_signature"
        grouped = candidates.groupby(signature_column, sort=True, dropna=False)
        consolidated = grouped.agg(
            raw_group_count=("raw_full_row_signature", "nunique"),
            report_count=("source_row_id", "size"),
            norm_crossing_id=("norm_crossing_id", "first"),
            earliest_reported_at_utc=("reported_at_utc", "min"),
            latest_reported_at_utc=("reported_at_utc", "max"),
            primary_source_row_id=("source_row_id", "first"),
            sorted_source_row_ids=("source_row_id", lambda values: sorted(values.tolist())),
        ).reset_index()
        consolidated["consolidation_tier"] = np.select(
            [
                consolidated["raw_group_count"].gt(1),
                consolidated["report_count"].gt(1),
            ],
            ["normalized_exact", "exact"],
            default="distinct_candidate",
        )
        consolidated["canonical_incident_id"] = consolidated["sorted_source_row_ids"].map(
            lambda source_ids: f"INC-{stable_hash(*source_ids, length=20)}"
        )
        consolidated["ruleset_version"] = config["ruleset_version"]
        incidents = consolidated[
            [
                "canonical_incident_id",
                "ruleset_version",
                "consolidation_tier",
                "report_count",
                "norm_crossing_id",
                "earliest_reported_at_utc",
                "latest_reported_at_utc",
                "primary_source_row_id",
            ]
        ].copy()
        incidents = pd.DataFrame(
            {column: incidents[column].tolist() for column in incidents.columns}
        )

        assignments = candidates[
            ["source_row_id", "source_excel_row_number", signature_column]
        ].merge(
            consolidated[
                [
                    signature_column,
                    "canonical_incident_id",
                    "consolidation_tier",
                    "primary_source_row_id",
                ]
            ],
            on=signature_column,
            how="left",
            validate="many_to_one",
        )
        assignments = assignments.sort_values(
            [signature_column, "source_excel_row_number"], kind="stable"
        )
        valid_crosswalk = assignments.assign(
            exception_id=pd.NA,
            is_primary_report=lambda frame: frame["source_row_id"].eq(
                frame["primary_source_row_id"]
            ),
        )[
            [
                "source_row_id",
                "canonical_incident_id",
                "exception_id",
                "consolidation_tier",
                "is_primary_report",
            ]
        ]

    crosswalk_parts = [frame for frame in (exception_crosswalk, valid_crosswalk) if not frame.empty]
    if crosswalk_parts:
        combined_crosswalk = pd.concat(crosswalk_parts, ignore_index=True)
        crosswalk = pd.DataFrame(
            {
                column: combined_crosswalk[column].tolist()
                for column in combined_crosswalk.columns
            }
        )
    else:
        crosswalk = pd.DataFrame()
    return incidents, crosswalk, exceptions


def _volume_tier(count: int) -> str:
    return "low" if count <= 3 else "medium" if count <= 19 else "high"


def generate_duplicate_candidates(incidents: pd.DataFrame, source: pd.DataFrame, bands: list[int]) -> pd.DataFrame:
    """Generate proximity evidence without altering canonical assignments."""

    if incidents.empty:
        return pd.DataFrame()
        
    source = source.copy()
    for column in ("reported_at_local", "iana_time_zone", "utc_offset_minutes"):
        if column not in source.columns:
            source[column] = pd.NA
            
    primary_details = source.set_index("source_row_id").loc[
        incidents["primary_source_row_id"],
        ["Duration", "norm_duration", "Reason", "State", "City", "reported_at_local", "iana_time_zone", "utc_offset_minutes"],
    ].reset_index().rename(columns={"source_row_id": "primary_source_row_id"})
    
    work = incidents.merge(primary_details, on="primary_source_row_id", how="left")
    volume = work.groupby("norm_crossing_id")["canonical_incident_id"].transform("size").astype(int)
    work["crossing_volume_tier"] = volume.map(_volume_tier)
    
    pairs: list[dict[str, Any]] = []
    max_band = max(bands)
    
    # Sort once upfront instead of per-group
    work_sorted = work.sort_values(["norm_crossing_id", "earliest_reported_at_utc"], kind="stable")
    
    for crossing_id, group in work_sorted.groupby("norm_crossing_id", sort=False):
        n = len(group)
        if n < 2:
            continue
            
        # Extract column vectors directly to avoid expensive `to_dict('records')` overhead
        timestamps = group["earliest_reported_at_utc"].values
        ids = group["canonical_incident_id"].values
        reported_local = group["reported_at_local"].values
        iana_tz = group["iana_time_zone"].values
        utc_offset = group["utc_offset_minutes"].values
        dur_raw = group["Duration"].values
        dur_norm = group["norm_duration"].values
        reasons = group["Reason"].values
        states = group["State"].values
        cities = group["City"].values
        vol_tiers = group["crossing_volume_tier"].values
        
        for left_idx in range(n):
            t_left = timestamps[left_idx]
            id_left = ids[left_idx]
            
            for right_idx in range(left_idx + 1, n):
                separation = (timestamps[right_idx] - t_left) / np.timedelta64(1, 'm')
                
                if separation > max_band:
                    break
                    
                band = next(limit for limit in bands if separation <= limit)
                id_right = ids[right_idx]
                pair_id = f"PAIR-{stable_hash(*sorted([id_left, id_right]), length=20)}"
                
                pairs.append(
                    {
                        "candidate_pair_id": pair_id,
                        "left_incident_id": id_left,
                        "right_incident_id": id_right,
                        "norm_crossing_id": crossing_id,
                        "separation_minutes": separation,
                        "proximity_band_minutes": band,
                        "crossing_volume_tier": vol_tiers[left_idx],
                        "left_reported_at_utc": t_left,
                        "right_reported_at_utc": timestamps[right_idx],
                        "left_reported_at_local": reported_local[left_idx],
                        "right_reported_at_local": reported_local[right_idx],
                        "left_iana_time_zone": iana_tz[left_idx],
                        "right_iana_time_zone": iana_tz[right_idx],
                        "left_utc_offset_minutes": utc_offset[left_idx],
                        "right_utc_offset_minutes": utc_offset[right_idx],
                        "left_duration_raw": dur_raw[left_idx],
                        "right_duration_raw": dur_raw[right_idx],
                        "left_duration_normalized": dur_norm[left_idx],
                        "right_duration_normalized": dur_norm[right_idx],
                        "left_reason": reasons[left_idx],
                        "right_reason": reasons[right_idx],
                        "state": states[left_idx],
                        "city": cities[left_idx],
                    }
                )
                
    candidates = pd.DataFrame(pairs)
    if candidates.empty:
        return candidates

    # Components are navigation aids only: they never alter canonical assignments.
    parent: dict[str, str] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        if parent[value] != value:
            parent[value] = find(parent[value])
        return parent[value]

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for pair in candidates.itertuples(index=False):
        union(pair.left_incident_id, pair.right_incident_id)
    members: dict[str, list[str]] = defaultdict(list)
    for incident_id in parent:
        members[find(incident_id)].append(incident_id)
    group_ids = {
        incident_id: f"CGRP-{stable_hash(*sorted(component), length=20)}"
        for component in members.values()
        for incident_id in component
    }
    candidates["candidate_group_id"] = candidates["left_incident_id"].map(group_ids)
    return candidates.sort_values(["norm_crossing_id", "candidate_pair_id"], kind="stable").reset_index(drop=True)


def deterministic_review_sample(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates.assign(review_label=pd.Series(dtype="string"), review_notes=pd.Series(dtype="string"))
    sample_parts = []
    for _, group in candidates.groupby(["proximity_band_minutes", "crossing_volume_tier"], sort=True):
        ranked = group.assign(
            _content_hash=group.apply(lambda row: stable_hash(*row.astype(str).tolist(), length=64), axis=1)
        ).sort_values("_content_hash", kind="stable")
        sample_parts.append(ranked.head(25))
    return (
        pd.concat(sample_parts, ignore_index=True)
        .drop(columns="_content_hash")
        .assign(review_label="", review_notes="")
    )


REVIEW_LABELS = {"same_incident", "distinct", "uncertain"}


def review_label_template(sample: pd.DataFrame) -> pd.DataFrame:
    """Create the stable, separately maintained manual-review input."""

    if "candidate_pair_id" not in sample.columns:
        if sample.empty:
            return pd.DataFrame(columns=["candidate_pair_id", "review_label", "review_notes"])
        raise ValueError("Review sample is missing candidate_pair_id.")
    return (
        sample[["candidate_pair_id"]]
        .drop_duplicates()
        .sort_values("candidate_pair_id", kind="stable")
        .assign(review_label="", review_notes="")
        .reset_index(drop=True)
    )


def validate_review_labels(
    sample: pd.DataFrame, labels: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate manual labels and merge them onto a deterministic review sample."""

    required = ["candidate_pair_id", "review_label", "review_notes"]
    _require_columns(labels, required, "candidate review labels")
    if "candidate_pair_id" not in sample.columns:
        if sample.empty:
            sample = pd.DataFrame(columns=["candidate_pair_id", "review_label", "review_notes"])
        else:
            raise ValueError("Candidate review sample is missing candidate_pair_id.")
    if sample["candidate_pair_id"].duplicated().any():
        raise ValueError("Candidate review sample contains duplicate candidate_pair_id values.")
    if labels["candidate_pair_id"].duplicated().any():
        raise ValueError("Candidate review labels contain duplicate candidate_pair_id values.")

    normalized = labels[required].copy()
    normalized["candidate_pair_id"] = normalized["candidate_pair_id"].astype("string").str.strip()
    normalized["review_label"] = normalized["review_label"].astype("string").fillna("").str.strip().str.lower()
    normalized["review_notes"] = normalized["review_notes"].astype("string").fillna("")
    sample_ids = set(sample["candidate_pair_id"].astype(str))
    unknown_ids = sorted(set(normalized["candidate_pair_id"].astype(str)) - sample_ids)
    if unknown_ids:
        raise ValueError(f"Review labels contain unknown candidate_pair_id values: {unknown_ids[:5]}")
    invalid_labels = sorted(set(normalized.loc[~normalized["review_label"].isin(REVIEW_LABELS | {""}), "review_label"]))
    if invalid_labels:
        raise ValueError(f"Review labels contain unsupported values: {invalid_labels}")

    merged = sample.drop(columns=["review_label", "review_notes"], errors="ignore").merge(
        normalized, on="candidate_pair_id", how="left", validate="one_to_one"
    )
    merged["review_label"] = merged["review_label"].fillna("")
    merged["review_notes"] = merged["review_notes"].fillna("")
    reviewed = merged["review_label"].isin(REVIEW_LABELS)
    label_counts = merged.loc[reviewed, "review_label"].value_counts().sort_index()
    by_stratum = (
        merged.loc[reviewed]
        .groupby(["proximity_band_minutes", "crossing_volume_tier", "review_label"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
        .to_dict("records")
        if reviewed.any()
        else []
    )
    summary = {
        "sample_rows": int(len(merged)),
        "reviewed_rows": int(reviewed.sum()),
        "unreviewed_rows": int((~reviewed).sum()),
        "complete": bool(reviewed.all()),
        "label_counts": {str(key): int(value) for key, value in label_counts.items()},
        "by_stratum": by_stratum,
    }
    return merged, summary


def reconcile_2025_workbooks(authoritative: pd.DataFrame, reconciliation: pd.DataFrame, config: dict[str, Any]) -> tuple[dict[str, Any], pd.DataFrame]:
    auth_2025 = authoritative.loc[authoritative["reported_at_utc"].dt.year.eq(2025)].copy()
    signature_column = "normalized_full_row_signature"
    auth_counts = Counter(auth_2025[signature_column])
    recon_counts = Counter(reconciliation[signature_column])
    only_auth = auth_counts - recon_counts
    only_recon = recon_counts - auth_counts
    all_signatures = set(auth_counts) | set(recon_counts)
    multiplicity_only = sum(
        1
        for signature in all_signatures
        if signature in auth_counts and signature in recon_counts and auth_counts[signature] != recon_counts[signature]
    )
    def select_surplus(frame: pd.DataFrame, surplus: Counter[str], side: str) -> pd.DataFrame:
        parts = []
        for signature, count in surplus.items():
            selected = frame[frame[signature_column].eq(signature)].sort_values(
                "source_excel_row_number", kind="stable"
            ).head(count).copy()
            selected["reconciliation_side"] = side
            parts.append(selected)
        return pd.concat(parts, ignore_index=True) if parts else frame.iloc[0:0].assign(reconciliation_side=pd.Series(dtype="string"))

    discrepancies = pd.concat(
        [
            select_surplus(auth_2025, only_auth, "authoritative_2025"),
            select_surplus(reconciliation, only_recon, "reconciliation"),
        ],
        ignore_index=True,
    )
    summary = {
        "comparison": "normalized full-row multiset over all configured material columns",
        "authoritative_2025_rows": len(auth_2025),
        "authoritative_2025_unique_signatures": len(auth_counts),
        "reconciliation_rows": len(reconciliation),
        "reconciliation_unique_signatures": len(recon_counts),
        "rows_present_only_in_authoritative": sum(only_auth.values()),
        "rows_present_only_in_reconciliation": sum(only_recon.values()),
        "unique_signatures_with_multiplicity_difference": multiplicity_only,
    }
    return summary, discrepancies


def timestamp_granularity(source: pd.DataFrame) -> pd.DataFrame:
    valid = source.loc[source["reported_at_utc"].notna()].copy()
    valid["year"] = valid["reported_at_utc"].dt.year
    rows = []
    for year, group in valid.groupby("year", sort=True):
        seconds = group["reported_at_utc"].dt.second
        minutes = group["reported_at_utc"].dt.minute
        total = len(group)
        rows.append(
            {
                "year": int(year),
                "total_rows": total,
                "missing_or_invalid_values": int(source["reported_at_utc"].isna().sum()) if year is None else 0,
                "nonzero_seconds_count": int(seconds.ne(0).sum()),
                "nonzero_seconds_percentage": round(float(seconds.ne(0).mean() * 100), 4),
                "five_minute_mark_count": int(minutes.mod(5).eq(0).sum()),
                "five_minute_mark_percentage": round(float(minutes.mod(5).eq(0).mean() * 100), 4),
                "fifteen_minute_mark_count": int(minutes.mod(15).eq(0).sum()),
                "fifteen_minute_mark_percentage": round(float(minutes.mod(15).eq(0).mean() * 100), 4),
                "thirty_minute_mark_count": int(minutes.mod(30).eq(0).sum()),
                "thirty_minute_mark_percentage": round(float(minutes.mod(30).eq(0).mean() * 100), 4),
                "sixty_minute_mark_count": int(minutes.eq(0).sum()),
                "sixty_minute_mark_percentage": round(float(minutes.eq(0).mean() * 100), 4),
                "minimum_timestamp_utc": group["reported_at_utc"].min(),
                "maximum_timestamp_utc": group["reported_at_utc"].max(),
            }
        )
    invalid_count = int(source["reported_at_utc"].isna().sum())
    if invalid_count:
        rows.append(
            {
                "year": "unknown",
                "total_rows": invalid_count,
                "missing_or_invalid_values": invalid_count,
                "nonzero_seconds_count": 0,
                "nonzero_seconds_percentage": 0.0,
                "five_minute_mark_count": 0,
                "five_minute_mark_percentage": 0.0,
                "fifteen_minute_mark_count": 0,
                "fifteen_minute_mark_percentage": 0.0,
                "thirty_minute_mark_count": 0,
                "thirty_minute_mark_percentage": 0.0,
                "sixty_minute_mark_count": 0,
                "sixty_minute_mark_percentage": 0.0,
                "minimum_timestamp_utc": pd.NaT,
                "maximum_timestamp_utc": pd.NaT,
            }
        )
    return pd.DataFrame(rows)


def _diagnostics(
    source: pd.DataFrame,
    incidents: pd.DataFrame,
    crosswalk: pd.DataFrame,
    candidates: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build auditable counts without treating candidate pairs as merged incidents."""

    source_with_crosswalk = source.merge(crosswalk, on="source_row_id", how="left")
    source_with_crosswalk["year"] = source_with_crosswalk["reported_at_utc"].dt.year.astype("Int64")
    source_with_crosswalk["collapsed_duplicate_report"] = (
        source_with_crosswalk["canonical_incident_id"].notna()
        & ~source_with_crosswalk["is_primary_report"].fillna(False)
    )

    incident_volume = incidents.groupby("norm_crossing_id")["canonical_incident_id"].transform("size")
    incident_dimensions = incidents[["canonical_incident_id", "primary_source_row_id", "consolidation_tier"]].copy()
    incident_dimensions["crossing_volume_tier"] = incident_volume.map(_volume_tier)
    primary_columns = [
        "source_row_id", "reported_at_utc", "State", "norm_crossing_id", "Reason",
        "duration_normalization_status",
    ]
    incident_dimensions = incident_dimensions.merge(
        source[primary_columns],
        left_on="primary_source_row_id",
        right_on="source_row_id",
        how="left",
        validate="one_to_one",
    )
    incident_dimensions["year"] = incident_dimensions["reported_at_utc"].dt.year.astype("Int64")
    volume_by_crossing = incident_dimensions.drop_duplicates("norm_crossing_id").set_index("norm_crossing_id")["crossing_volume_tier"]
    source_with_crosswalk["crossing_volume_tier"] = source_with_crosswalk["norm_crossing_id"].map(volume_by_crossing)

    candidate_ids: set[str] = set()
    endpoint_dimensions = pd.DataFrame()
    if not candidates.empty:
        candidate_ids = set(candidates["left_incident_id"]) | set(candidates["right_incident_id"])
        endpoints = pd.concat(
            [
                candidates[["candidate_pair_id", "left_incident_id"]].rename(columns={"left_incident_id": "canonical_incident_id"}),
                candidates[["candidate_pair_id", "right_incident_id"]].rename(columns={"right_incident_id": "canonical_incident_id"}),
            ],
            ignore_index=True,
        ).drop_duplicates()
        endpoint_dimensions = endpoints.merge(
            incident_dimensions.drop(columns=["primary_source_row_id", "source_row_id", "reported_at_utc"]),
            on="canonical_incident_id",
            how="left",
            validate="many_to_one",
        )
    source_with_crosswalk["temporal_candidate_incident"] = source_with_crosswalk["canonical_incident_id"].isin(candidate_ids)

    dimensions = {
        "year": "year",
        "state": "State",
        "crossing": "norm_crossing_id",
        "reason": "Reason",
        "consolidation_tier": "consolidation_tier",
        "duration_status": "duration_normalization_status",
        "crossing_volume_tier": "crossing_volume_tier",
    }
    result: dict[str, pd.DataFrame] = {}
    for name, column in dimensions.items():
        grouped = source_with_crosswalk.groupby(column, dropna=False)
        frame = grouped.agg(
            source_report_count=("source_row_id", "size"),
            candidate_reported_incident_count=("canonical_incident_id", "nunique"),
            collapsed_duplicate_report_count=("collapsed_duplicate_report", "sum"),
            exception_count=("exception_id", "nunique"),
        )
        temporal_incidents = (
            source_with_crosswalk[source_with_crosswalk["temporal_candidate_incident"]]
            .groupby(column, dropna=False)["canonical_incident_id"]
            .nunique()
            .rename("temporal_candidate_incident_count")
        )
        frame = frame.join(temporal_incidents, how="left")
        if not endpoint_dimensions.empty and column in endpoint_dimensions.columns:
            pair_counts = (
                endpoint_dimensions.groupby(column, dropna=False)["candidate_pair_id"]
                .nunique()
                .rename("temporal_candidate_pair_count")
            )
            frame = frame.join(pair_counts, how="left")
        else:
            frame["temporal_candidate_pair_count"] = 0
        count_columns = [column_name for column_name in frame.columns if column_name.endswith("_count")]
        frame[count_columns] = frame[count_columns].fillna(0).astype(int)
        result[f"diagnostics_by_{name}"] = frame.reset_index()
    return result


def _write_json(path: Path, value: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, default=str)


def _write_step_5_checkpoint(
    output_dir: Path,
    frames: dict[str, pd.DataFrame],
    config: dict[str, Any],
    source_metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Write a complete step-5 checkpoint, publishing its metadata last."""

    checkpoint_dir = Path(output_dir) / "step_5_checkpoint"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = checkpoint_dir / "checkpoint.json"
    metadata_path.unlink(missing_ok=True)

    frame_metadata: dict[str, dict[str, Any]] = {}
    for name, filename in STEP_5_CHECKPOINT_FILES.items():
        frame = frames[name]
        frame.to_parquet(checkpoint_dir / filename, index=False)
        frame_metadata[name] = {
            "filename": filename,
            "row_count": len(frame),
            "columns": list(frame.columns),
        }

    metadata = {
        "checkpoint_format_version": STEP_5_CHECKPOINT_FORMAT_VERSION,
        "created_at_utc": pd.Timestamp.now(tz=timezone.utc).isoformat(),
        "ruleset_version": config["ruleset_version"],
        "source_files": source_metadata,
        "frames": frame_metadata,
    }
    _write_json(metadata_path, metadata)
    return metadata


def _load_step_5_checkpoint(
    output_dir: Path,
    config: dict[str, Any],
    source_paths: dict[str, Path],
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, Any]], dict[str, Any]]:
    """Load and validate a complete analyst-selected step-5 checkpoint."""

    checkpoint_dir = Path(output_dir) / "step_5_checkpoint"
    required_paths = [checkpoint_dir / "checkpoint.json"] + [
        checkpoint_dir / filename for filename in STEP_5_CHECKPOINT_FILES.values()
    ]
    missing = [path for path in required_paths if not path.is_file()]
    if missing:
        details = "\n".join(f"- {path}" for path in missing)
        raise FileNotFoundError(f"Required step-5 checkpoint files are missing:\n{details}")

    metadata_path = checkpoint_dir / "checkpoint.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Step-5 checkpoint metadata is unreadable: {metadata_path}") from exc

    if metadata.get("checkpoint_format_version") != STEP_5_CHECKPOINT_FORMAT_VERSION:
        raise ValueError(
            "Step-5 checkpoint format version mismatch: "
            f"expected {STEP_5_CHECKPOINT_FORMAT_VERSION!r}, "
            f"found {metadata.get('checkpoint_format_version')!r}."
        )
    if metadata.get("ruleset_version") != config["ruleset_version"]:
        raise ValueError(
            "Step-5 checkpoint ruleset version mismatch: "
            f"expected {config['ruleset_version']!r}, "
            f"found {metadata.get('ruleset_version')!r}."
        )

    source_metadata = metadata.get("source_files")
    if not isinstance(source_metadata, dict):
        raise ValueError("Step-5 checkpoint metadata is missing source_files.")
    expected_source_names = {name: Path(path).name for name, path in source_paths.items()}
    checkpoint_source_names = {
        name: details.get("filename") if isinstance(details, dict) else None
        for name, details in source_metadata.items()
    }
    if checkpoint_source_names != expected_source_names:
        raise ValueError(
            "Step-5 checkpoint source filename mismatch: "
            f"expected {expected_source_names!r}, found {checkpoint_source_names!r}."
        )

    frame_metadata = metadata.get("frames")
    if not isinstance(frame_metadata, dict) or set(frame_metadata) != set(STEP_5_CHECKPOINT_FILES):
        raise ValueError("Step-5 checkpoint frame inventory is incomplete or incompatible.")

    frames: dict[str, pd.DataFrame] = {}
    for name, filename in STEP_5_CHECKPOINT_FILES.items():
        details = frame_metadata[name]
        if not isinstance(details, dict) or details.get("filename") != filename:
            raise ValueError(f"Step-5 checkpoint filename mismatch for frame {name!r}.")
        frame = pd.read_parquet(checkpoint_dir / filename)
        expected_rows = details.get("row_count")
        if len(frame) != expected_rows:
            raise ValueError(
                f"Step-5 checkpoint row-count mismatch for {name!r}: "
                f"expected {expected_rows!r}, found {len(frame)!r}."
            )
        expected_columns = details.get("columns")
        if list(frame.columns) != expected_columns:
            raise ValueError(
                f"Step-5 checkpoint column mismatch for {name!r}: "
                f"expected {expected_columns!r}, found {list(frame.columns)!r}."
            )
        frames[name] = frame

    return frames, source_metadata, metadata


_ACCEPTANCE_MUTABLE_ARTIFACTS = {
    "candidate_review_sample_labeled.csv",
    "candidate_review_summary.json",
    "phase_1_acceptance_report.json",
}


def _normalized_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.pop("execution", None)
    for output in manifest.get("outputs", {}).values():
        output.pop("path", None)
    return manifest


def compare_phase_1_outputs(primary_dir: Path, repeat_dir: Path) -> dict[str, Any]:
    """Compare deterministic Phase 1 outputs by logical content, not file bytes."""

    primary_dir, repeat_dir = Path(primary_dir), Path(repeat_dir)
    primary_files = {
        path.name for path in primary_dir.iterdir()
        if path.is_file() and path.name not in _ACCEPTANCE_MUTABLE_ARTIFACTS
    }
    repeat_files = {
        path.name for path in repeat_dir.iterdir()
        if path.is_file() and path.name not in _ACCEPTANCE_MUTABLE_ARTIFACTS
    }
    mismatches: list[str] = []
    if primary_files != repeat_files:
        missing_primary = sorted(repeat_files - primary_files)
        missing_repeat = sorted(primary_files - repeat_files)
        if missing_primary:
            mismatches.append(f"Missing from primary run: {missing_primary}")
        if missing_repeat:
            mismatches.append(f"Missing from repeat run: {missing_repeat}")

    compared: list[str] = []
    for name in sorted(primary_files & repeat_files):
        left, right = primary_dir / name, repeat_dir / name
        try:
            if name == "run_manifest.json":
                if _normalized_manifest(left) != _normalized_manifest(right):
                    raise AssertionError("normalized manifests differ")
            elif left.suffix == ".json":
                if json.loads(left.read_text(encoding="utf-8")) != json.loads(right.read_text(encoding="utf-8")):
                    raise AssertionError("JSON values differ")
            elif left.suffix == ".parquet":
                pd.testing.assert_frame_equal(pd.read_parquet(left), pd.read_parquet(right))
            elif left.suffix == ".csv":
                pd.testing.assert_frame_equal(pd.read_csv(left), pd.read_csv(right))
            else:
                if file_sha256(left) != file_sha256(right):
                    raise AssertionError("file contents differ")
            compared.append(name)
        except Exception as exc:  # comparison report must retain every mismatch
            mismatches.append(f"{name}: {exc}")
    return {
        "passed": not mismatches,
        "compared_artifacts": compared,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "excluded_nondeterministic_manifest_fields": [
            "execution.timestamp_utc",
            "execution.duration_seconds",
            "execution.reused_step_5_checkpoint",
            "execution.step_5_checkpoint_created_at_utc",
            "outputs.*.path",
        ],
    }


def build_acceptance_report(
    checks: dict[str, bool | None], evidence: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Create the machine-readable Phase 1 acceptance decision."""

    failed = sorted(name for name, value in checks.items() if value is False)
    pending = sorted(name for name, value in checks.items() if value is None)
    passed = sorted(name for name, value in checks.items() if value is True)
    return {
        "status": "complete" if not failed and not pending else "awaiting_review",
        "checks": checks,
        "passed_checks": passed,
        "failed_checks": failed,
        "pending_checks": pending,
        "evidence": evidence or {},
    }


def write_acceptance_report(
    output_dir: Path,
    checks: dict[str, bool | None],
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write the acceptance report and synchronize the Phase 1 gate status."""

    output_dir = Path(output_dir)
    report = build_acceptance_report(checks, evidence)
    _write_json(output_dir / "phase_1_acceptance_report.json", report)
    gate_path = output_dir / "phase_1_gate_report.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.exists() else {}
    gate["status"] = report["status"]
    gate["acceptance"] = report
    _write_json(gate_path, gate)
    return report


def _git_metadata(repo_root: Path) -> dict[str, str | None]:
    def run(*args: str) -> str | None:
        try:
            return subprocess.check_output(args, cwd=repo_root, text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return None
    return {"commit": run("git", "rev-parse", "HEAD"), "dirty_worktree": run("git", "status", "--porcelain")}


def run_phase_1(
    authoritative_path: Path,
    reconciliation_path: Path,
    inventory_path: Path,
    config_path: Path,
    output_dir: Path,
    reuse_step_5_checkpoint: bool = False,
) -> Phase1Result:
    """Run deterministic Phase 1 v2 and write its required artifacts."""

    started = time.monotonic()
    
    print("[1/9] Initializing output directory and checking input files...")
    authoritative_path = Path(authoritative_path)
    reconciliation_path = Path(reconciliation_path)
    inventory_path = Path(inventory_path)
    config_path = Path(config_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_paths = {
        "authoritative": authoritative_path,
        "reconciliation": reconciliation_path,
        "form_71_inventory": inventory_path,
    }
    require_input_files({"configuration": config_path})
    config = load_config(config_path)

    if reuse_step_5_checkpoint:
        print("[2-5/9] Loading and validating the step-5 checkpoint...")
        frames, source_metadata, checkpoint_metadata = _load_step_5_checkpoint(
            output_dir, config, source_paths
        )
        source = frames["source"]
        reconciliation = frames["reconciliation"]
        crossing_timezones = frames["crossing_timezones"]
        incidents = frames["incidents"]
        crosswalk = frames["crosswalk"]
        exceptions = frames["exceptions"]
        local_time_columns = [
            column
            for column in crossing_timezones.columns
            if column not in {"norm_crossing_id"}
        ] + [
            "reported_at_local",
            "utc_offset_minutes",
            "reported_local_date",
            "reported_local_hour",
        ]
        authoritative = source.drop(columns=local_time_columns)
        print(f"      -> Loaded checkpoint created at {checkpoint_metadata['created_at_utc']}")
        print(
            "      -> Warning: current input contents are not automatically compared "
            "with this checkpoint. Rebuild it after inputs, configuration, or pipeline code change."
        )
    else:
        require_input_files(
            {
                "authoritative workbook": authoritative_path,
                "reconciliation workbook": reconciliation_path,
                "Form 71 inventory": inventory_path,
            }
        )
        print("[2/9] Reading raw source data files (Excel & CSV)...")
        t0 = time.monotonic()
        authoritative_raw = pd.read_excel(
            authoritative_path, sheet_name=config["authoritative_sheet"]
        )
        reconciliation_raw = pd.read_excel(reconciliation_path)
        inventory_raw = pd.read_csv(
            inventory_path,
            encoding="utf-8-sig",
            low_memory=False,
            usecols=list(INVENTORY_REQUIRED_COLUMNS),
        )
        print(f"      -> Loaded raw datasets in {time.monotonic() - t0:.1f}s")
        source_metadata = {
            "authoritative": {
                "filename": authoritative_path.name,
                "row_count": len(authoritative_raw),
                "columns": list(authoritative_raw.columns),
            },
            "reconciliation": {
                "filename": reconciliation_path.name,
                "row_count": len(reconciliation_raw),
                "columns": list(reconciliation_raw.columns),
            },
            "form_71_inventory": {
                "filename": inventory_path.name,
                "row_count": len(inventory_raw),
                "columns": list(inventory_raw.columns),
            },
        }

        print("[3/9] Normalizing source data structures...")
        authoritative = normalize_source_dataframe(
            authoritative_raw, config, config["authoritative_sheet"], "authoritative"
        )
        reconciliation = normalize_source_dataframe(
            reconciliation_raw, config, "Sheet1", "reconciliation"
        )

        print("[4/9] Resolving crossing time zones (timezonefinder)...")
        t0 = time.monotonic()
        crossing_timezones = build_crossing_timezones(inventory_raw, config)
        print(f"      -> Resolved time zones in {time.monotonic() - t0:.1f}s")

        print("[5/9] Enriching local time and consolidating incident reports...")
        t0 = time.monotonic()
        source = enrich_with_local_time(authoritative, crossing_timezones)
        print(f"      -> Enriched local time in {time.monotonic() - t0:.1f}s")
        t0 = time.monotonic()
        incidents, crosswalk, exceptions = consolidate_reports(source, config)
        print(
            f"      -> Consolidated {len(source):,} source reports into "
            f"{len(incidents):,} incidents in {time.monotonic() - t0:.1f}s"
        )
        t0 = time.monotonic()
        checkpoint_metadata = _write_step_5_checkpoint(
            output_dir,
            {
                "source": source,
                "reconciliation": reconciliation,
                "crossing_timezones": crossing_timezones,
                "incidents": incidents,
                "crosswalk": crosswalk,
                "exceptions": exceptions,
            },
            config,
            source_metadata,
        )
        print(f"      -> Wrote step-5 checkpoint in {time.monotonic() - t0:.1f}s")

    print("[6/9] Generating duplicate candidates...")
    t0 = time.monotonic()
    candidates = generate_duplicate_candidates(incidents, source, config["proximity_bands_minutes"])
    print(f"      -> Generated {len(candidates):,} candidate pairs in {time.monotonic() - t0:.1f}s")

    print("[7/9] Sampling review cases & running 2025 reconciliation...")
    review_sample = deterministic_review_sample(candidates)
    reconciliation_summary, reconciliation_discrepancies = reconcile_2025_workbooks(authoritative, reconciliation, config)
    diagnostics = _diagnostics(source, incidents, crosswalk, candidates)
    granularity = timestamp_granularity(source)

    print("[8/9] Validating dataset integrity checks...")
    validations = {
        "source_rows_map_once": len(crosswalk) == len(source) and crosswalk["source_row_id"].is_unique,
        "only_configured_auto_merge_tiers": set(incidents["consolidation_tier"].unique()).issubset(
            {"exact", "normalized_exact", "distinct_candidate"}
        ),
        "unsupported_duration_bounds_are_null": bool(
            source.loc[source["duration_normalization_status"].eq("unmapped"), ["duration_lower_minutes", "duration_upper_minutes"]]
            .isna()
            .all()
            .all()
        ),
        "all_incident_groups_share_normalized_11_field_signature": bool(
            source.merge(crosswalk[["source_row_id", "canonical_incident_id"]], on="source_row_id")
            .dropna(subset=["canonical_incident_id"])
            .groupby("canonical_incident_id")["normalized_full_row_signature"]
            .nunique()
            .le(1)
            .all()
        ),
        "candidate_pairs_reference_canonical_incidents": bool(
            candidates.empty
            or (
                candidates["left_incident_id"].isin(incidents["canonical_incident_id"]).all()
                and candidates["right_incident_id"].isin(incidents["canonical_incident_id"]).all()
            )
        ),
        "diagnostics_have_required_counts": all(
            {
                "source_report_count",
                "candidate_reported_incident_count",
                "collapsed_duplicate_report_count",
                "exception_count",
                "temporal_candidate_incident_count",
                "temporal_candidate_pair_count",
            }.issubset(frame.columns)
            for frame in diagnostics.values()
        ),
    }
    if not validations["source_rows_map_once"]:
        raise AssertionError(f"Phase 1 validation failed: {validations}")

    timezone_summary = source["timezone_assignment_status"].value_counts(dropna=False).rename_axis("timezone_assignment_status").reset_index(name="source_report_count")
    local_time_diagnostics = (
        source[source["timezone_assignment_status"].eq("assigned")]
        .groupby(["iana_time_zone", "reported_local_hour"], dropna=False)
        .size()
        .rename("source_report_count")
        .reset_index()
    )
    summary = {
        "ruleset_version": config["ruleset_version"],
        "source_reports": len(source),
        "candidate_reported_incidents": len(incidents),
        "exceptions": len(exceptions),
        "auto_merged_reports": int(len(source) - len(incidents) - len(exceptions)),
        "consolidation_tiers": {key: int(value) for key, value in incidents["consolidation_tier"].value_counts().items()},
        "temporal_candidate_pairs": len(candidates),
        "review_sample_rows": len(review_sample),
        "timezone_assignment": {str(row.timezone_assignment_status): int(row.source_report_count) for row in timezone_summary.itertuples(index=False)},
    }
    gate = {
        "status": "awaiting_review",
        "claim_boundary": [
            "Date/Time is a user-entered reported incident date and time supplied as UTC for this pipeline.",
            "Crossing-local time is derived from current Form 71 coordinates and historical IANA offsets.",
            "Available evidence does not prove submission time, exact physical start time, or second-accurate observation time.",
            "Duration is a user-selected category, not a verified incident endpoint.",
            "An interval without a report can be labeled no_report_observed, not unblocked.",
        ],
        "summary": summary,
        "validation": validations,
        "timestamp_granularity_by_year": granularity.to_dict("records"),
        "review_candidate_distribution": (
            candidates.groupby(["proximity_band_minutes", "crossing_volume_tier"], dropna=False)
            .size()
            .rename("candidate_pair_count")
            .reset_index()
            .to_dict("records")
            if not candidates.empty
            else []
        ),
        "duration_normalization": {
            str(key): int(value)
            for key, value in source["duration_normalization_status"].value_counts(dropna=False).items()
        },
        "interval_resolution_decision": {
            "status": "deferred_to_phase_2",
            "candidate_units_hours": [1, 2, 4],
        },
        "timezone_localization": {
            "inventory_path": str(inventory_path),
            "inventory_filename": inventory_path.name,
            "inventory_rows": source_metadata["form_71_inventory"]["row_count"],
            "no_state_fallback": True,
            "unresolved_rows_remain_utc": True,
        },
    }

    print("[9/9] Writing output artifacts, gate report, and run manifest...")
    t0 = time.monotonic()
    artifact_paths = {
        "source_reports_with_ids": output_dir / "source_reports_with_ids.parquet",
        "reported_incidents": output_dir / "reported_incidents.parquet",
        "report_incident_crosswalk": output_dir / "report_incident_crosswalk.parquet",
        "documented_exceptions": output_dir / "documented_exceptions.parquet",
        "duplicate_candidates": output_dir / "duplicate_candidates.parquet",
        "candidate_review_sample": output_dir / "candidate_review_sample.csv",
        "candidate_review_summary": output_dir / "candidate_review_summary.json",
        "crossing_timezones": output_dir / "crossing_timezones.parquet",
        "inventory_profile": output_dir / "inventory_profile.json",
        "reconciliation_summary": output_dir / "reconciliation_summary.json",
        "reconciliation_discrepancies": output_dir / "reconciliation_discrepancies.parquet",
        "deduplication_summary": output_dir / "deduplication_summary.json",
        "timestamp_granularity_by_year": output_dir / "timestamp_granularity_by_year.csv",
        "timezone_assignment_diagnostics": output_dir / "timezone_assignment_diagnostics.csv",
        "local_time_diagnostics": output_dir / "local_time_diagnostics.csv",
        "phase_1_gate_report": output_dir / "phase_1_gate_report.json",
        "run_manifest": output_dir / "run_manifest.json",
    }
    source.to_parquet(artifact_paths["source_reports_with_ids"], index=False)
    incidents.to_parquet(artifact_paths["reported_incidents"], index=False)
    crosswalk.to_parquet(artifact_paths["report_incident_crosswalk"], index=False)
    exceptions.to_parquet(artifact_paths["documented_exceptions"], index=False)
    candidates.to_parquet(artifact_paths["duplicate_candidates"], index=False)
    review_sample.to_csv(artifact_paths["candidate_review_sample"], index=False)
    _, initial_review_summary = validate_review_labels(
        review_sample, review_label_template(review_sample).iloc[0:0]
    )
    _write_json(artifact_paths["candidate_review_summary"], initial_review_summary)
    crossing_timezones.to_parquet(artifact_paths["crossing_timezones"], index=False)
    timezone_summary.to_csv(artifact_paths["timezone_assignment_diagnostics"], index=False)
    local_time_diagnostics.to_csv(artifact_paths["local_time_diagnostics"], index=False)
    granularity.to_csv(artifact_paths["timestamp_granularity_by_year"], index=False)
    for name, frame in diagnostics.items():
        path = output_dir / f"{name}.csv"
        frame.to_csv(path, index=False)
        artifact_paths[name] = path
    _write_json(
        artifact_paths["inventory_profile"],
        {
            "authoritative_rows": len(source),
            "columns": source_metadata["authoritative"]["columns"],
            "duration_normalization": source["duration_normalization_status"].value_counts(dropna=False).to_dict(),
            "crossing_id_status": source["crossing_id_status"].value_counts(dropna=False).to_dict(),
            "outside_named_source_period_count": int(source["outside_named_source_period"].sum()),
        },
    )
    _write_json(artifact_paths["reconciliation_summary"], reconciliation_summary)
    reconciliation_discrepancies.to_parquet(artifact_paths["reconciliation_discrepancies"], index=False)
    _write_json(artifact_paths["deduplication_summary"], summary)
    _write_json(artifact_paths["phase_1_gate_report"], gate)

    try:
        import timezonefinder
        timezonefinder_version = getattr(timezonefinder, "__version__", "unknown")
    except ImportError:  # pragma: no cover - dependency is required above
        timezonefinder_version = None
    output_row_counts: dict[str, int | None] = {
        "source_reports_with_ids": len(source),
        "reported_incidents": len(incidents),
        "report_incident_crosswalk": len(crosswalk),
        "documented_exceptions": len(exceptions),
        "duplicate_candidates": len(candidates),
        "candidate_review_sample": len(review_sample),
        "crossing_timezones": len(crossing_timezones),
        "reconciliation_discrepancies": len(reconciliation_discrepancies),
        "timestamp_granularity_by_year": len(granularity),
        "timezone_assignment_diagnostics": len(timezone_summary),
        "local_time_diagnostics": len(local_time_diagnostics),
    }
    output_row_counts.update({name: len(frame) for name, frame in diagnostics.items()})
    output_schemas = {
        "source_reports_with_ids": list(source.columns),
        "reported_incidents": list(incidents.columns),
        "report_incident_crosswalk": list(crosswalk.columns),
        "documented_exceptions": list(exceptions.columns),
        "duplicate_candidates": list(candidates.columns),
        "candidate_review_sample": list(review_sample.columns),
        "crossing_timezones": list(crossing_timezones.columns),
        "reconciliation_discrepancies": list(reconciliation_discrepancies.columns),
        "timestamp_granularity_by_year": list(granularity.columns),
        "timezone_assignment_diagnostics": list(timezone_summary.columns),
        "local_time_diagnostics": list(local_time_diagnostics.columns),
        **{name: list(frame.columns) for name, frame in diagnostics.items()},
    }
    manifest_outputs = {
        name: {
            "path": str(path),
            "filename": path.name,
            "row_count": output_row_counts.get(name),
            "schema": output_schemas.get(name),
        }
        for name, path in artifact_paths.items()
    }
    manifest = {
        "ruleset_version": config["ruleset_version"],
        "inputs": {
            "authoritative": {
                "path": str(authoritative_path),
                "filename": authoritative_path.name,
                "sheet": config["authoritative_sheet"],
                "rows": source_metadata["authoritative"]["row_count"],
                "schema": source_metadata["authoritative"]["columns"],
            },
            "reconciliation": {
                "path": str(reconciliation_path),
                "filename": reconciliation_path.name,
                "sheet": "Sheet1",
                "rows": source_metadata["reconciliation"]["row_count"],
                "schema": source_metadata["reconciliation"]["columns"],
            },
            "form_71_inventory": {
                "path": str(inventory_path),
                "filename": inventory_path.name,
                "rows": source_metadata["form_71_inventory"]["row_count"],
                "schema": source_metadata["form_71_inventory"]["columns"],
            },
            "configuration": {"path": str(config_path), "filename": config_path.name},
        },
        "environment": {"python": sys.version, "platform": platform.platform(), "timezonefinder": timezonefinder_version},
        "git": _git_metadata(config_path.parent.parent),
        "execution": {
            "timestamp_utc": pd.Timestamp.now(tz=timezone.utc).isoformat(),
            "duration_seconds": round(time.monotonic() - started, 3),
            "reused_step_5_checkpoint": reuse_step_5_checkpoint,
            "step_5_checkpoint_created_at_utc": checkpoint_metadata["created_at_utc"],
        },
        "outputs": manifest_outputs,
        "validation": validations,
    }
    _write_json(artifact_paths["run_manifest"], manifest)
    print(f"      -> Artifacts & manifest written in {time.monotonic() - t0:.1f}s")
    print(f"=== Phase 1 finished successfully in {time.monotonic() - started:.1f}s ===")

    return Phase1Result(artifact_paths=artifact_paths, summary=summary, validations=validations)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 1 v2 incident deduplication.")
    parser.add_argument("--authoritative", type=Path, required=True)
    parser.add_argument("--reconciliation", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--reuse-step-5-checkpoint",
        action="store_true",
        help="Resume from the validated checkpoint under OUTPUT_DIR instead of rebuilding steps 1-5.",
    )
    args = parser.parse_args()
    result = run_phase_1(
        args.authoritative,
        args.reconciliation,
        args.inventory,
        args.config,
        args.output_dir,
        reuse_step_5_checkpoint=args.reuse_step_5_checkpoint,
    )
    print(json.dumps(result.summary, indent=2))


if __name__ == "__main__":
    main()
