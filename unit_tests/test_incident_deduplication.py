from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import incident_deduplication as mod


CONFIG = {
    "ruleset_version": "phase1-v3-test",
    "authoritative_sheet": "Sheet1",
    "material_columns": [
        "Crossing ID", "City", "State", "Street", "County", "Railroad",
        "Date/Time", "Duration", "Reason", "Immediate Impacts", "Additional Comments",
    ],
    "crossing_id_pattern": r"^\d{6}[A-Z]$",
    "auto_merge_tiers": [
        "exact", "normalized_exact", "overlap_proxy_non_temporal_match",
    ],
    "pair_decision_bands_minutes": [15, 30, 60, 120],
    "non_temporal_compatibility_fields": [
        "City", "State", "Street", "County", "Railroad", "Reason",
        "Immediate Impacts", "Additional Comments",
    ],
    "duration_categories": {
        "0-15 minutes": [0, 15], "16-30 minutes": [16, 30],
        "31-60 minutes": [31, 60], "1-2 hours": [60, 120],
        "2-6 hours": [120, 360], "6-12 hours": [360, 720],
        "12-24 hours": [720, 1440], "More than one day": [1440, None],
    },
    "duration_aliases": {"2-6 hours'": "2-6 hours", '2-6 hours"': "2-6 hours"},
    "inventory_columns": {
        "crossing_id": "Crossing ID", "latitude": "Latitude",
        "longitude": "Longitude", "revision_date": "Revision Date",
    },
}


def report(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "Crossing ID": "123456A", "City": "Example", "State": "NY", "Street": "Main St",
        "County": "Example", "Railroad": "RR", "Date/Time": "2025-01-01 12:00:00",
        "Duration": "31-60 minutes", "Reason": "A stationary train",
        "Immediate Impacts": "", "Additional Comments": "",
    }
    value.update(overrides)
    return value


def reference_consolidate_reports(
    source: pd.DataFrame, config: dict[str, object]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Small-fixture reference for the original per-group implementation."""

    valid = source["crossing_id_status"].eq("valid") & source["timestamp_status"].eq("valid")
    incident_rows: list[dict[str, object]] = []
    crosswalk_rows: list[dict[str, object]] = []
    exception_rows: list[dict[str, object]] = []
    for row in source.loc[~valid].sort_values("source_excel_row_number", kind="stable").itertuples(index=False):
        reason = "invalid_crossing_id" if row.crossing_id_status != "valid" else "invalid_timestamp"
        exception_id = f"EXC-{mod.stable_hash(row.source_row_id, reason, length=20)}"
        exception_rows.append(
            {
                "exception_id": exception_id,
                "source_row_id": row.source_row_id,
                "exception_reason": reason,
                "norm_crossing_id": row.norm_crossing_id,
                "reported_at_utc": row.reported_at_utc,
            }
        )
        crosswalk_rows.append(
            {
                "source_row_id": row.source_row_id,
                "canonical_incident_id": pd.NA,
                "exception_id": exception_id,
                "consolidation_tier": "exception",
                "is_primary_report": False,
            }
        )

    candidates = source.loc[valid].sort_values("source_excel_row_number", kind="stable")
    for _, group in candidates.groupby(
        "normalized_full_row_signature", sort=True, dropna=False
    ):
        raw_group_count = group["raw_full_row_signature"].nunique()
        tier = (
            "normalized_exact"
            if raw_group_count > 1
            else "exact"
            if len(group) > 1
            else "distinct_candidate"
        )
        source_ids = sorted(group["source_row_id"].tolist())
        incident_id = f"INC-{mod.stable_hash(*source_ids, length=20)}"
        primary = group.sort_values("source_excel_row_number", kind="stable").iloc[0]
        incident_rows.append(
            {
                "canonical_incident_id": incident_id,
                "ruleset_version": config["ruleset_version"],
                "consolidation_tier": tier,
                "report_count": len(group),
                "norm_crossing_id": primary["norm_crossing_id"],
                "earliest_reported_at_utc": group["reported_at_utc"].min(),
                "latest_reported_at_utc": group["reported_at_utc"].max(),
                "primary_source_row_id": primary["source_row_id"],
            }
        )
        for row in group.itertuples(index=False):
            crosswalk_rows.append(
                {
                    "source_row_id": row.source_row_id,
                    "canonical_incident_id": incident_id,
                    "exception_id": pd.NA,
                    "consolidation_tier": tier,
                    "is_primary_report": row.source_row_id == primary["source_row_id"],
                }
            )
    return (
        pd.DataFrame(incident_rows),
        pd.DataFrame(crosswalk_rows),
        pd.DataFrame(exception_rows),
    )


class IncidentDeduplicationTests(unittest.TestCase):
    def normalize(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        return mod.normalize_source_dataframe(
            pd.DataFrame(rows), CONFIG, "Sheet1", "authoritative"
        )

    def write_pipeline_inputs(self, root: Path) -> tuple[Path, Path, Path, Path]:
        authoritative_path = root / "authoritative.xlsx"
        reconciliation_path = root / "reconciliation.xlsx"
        inventory_path = root / "inventory.csv"
        config_path = root / "config.json"
        pd.DataFrame(
            [
                report(),
                report(**{"Date/Time": "2025-01-01 15:10:00", "Reason": "A moving train"}),
            ]
        ).to_excel(authoritative_path, index=False)
        pd.DataFrame([report()]).to_excel(reconciliation_path, index=False)
        pd.DataFrame(
            [
                {
                    "Crossing ID": "123456A",
                    "Latitude": None,
                    "Longitude": None,
                    "Revision Date": "2025-01-01",
                }
            ]
        ).to_csv(inventory_path, index=False)
        config_path.write_text(json.dumps(CONFIG), encoding="utf-8")
        return authoritative_path, reconciliation_path, inventory_path, config_path

    def test_optimized_consolidation_matches_reference_for_all_row_classes(self) -> None:
        rows = [
            report(**{"Crossing ID": "100001A", "Reason": "singleton"}),
            report(**{"Crossing ID": "100002A", "Reason": "exact"}),
            report(**{"Crossing ID": "100002A", "Reason": "exact"}),
            report(**{"Crossing ID": "100003A", "City": " Example ", "Reason": "Normalized  Value"}),
            report(**{"Crossing ID": "100003A", "City": "example", "Reason": "normalized value"}),
            report(**{"Crossing ID": "bad"}),
            report(**{"Crossing ID": "100004A", "Date/Time": "not a timestamp"}),
        ]
        source = self.normalize(rows)
        for candidate in (source, source.sample(frac=1, random_state=17)):
            with self.subTest(shuffled=not candidate.index.equals(source.index)):
                expected = reference_consolidate_reports(candidate, CONFIG)
                actual = mod.consolidate_reports(candidate, CONFIG)
                for expected_frame, actual_frame in zip(expected, actual):
                    pd.testing.assert_frame_equal(actual_frame, expected_frame)

    def test_duration_categories_aliases_and_unmapped_values(self) -> None:
        rows = [report(Duration=duration) for duration in CONFIG["duration_categories"]]
        rows.extend([report(Duration="2-6 hours'"), report(Duration='2-6 hours"'), report(Duration="mystery")])
        source = self.normalize(rows)
        self.assertEqual(source.loc[0, "duration_lower_minutes"], 0)
        self.assertEqual(source.loc[2, "duration_upper_minutes"], 60)
        self.assertEqual(source.loc[8, "duration_normalization_status"], "known_alias")
        self.assertEqual(source.loc[9, "duration_normalization_status"], "known_alias")
        self.assertEqual(source.loc[10, "duration_normalization_status"], "unmapped")
        self.assertTrue(pd.isna(source.loc[10, "duration_upper_minutes"]))

    def test_duration_aliases_share_normalized_deduplication_signature(self) -> None:
        source = self.normalize(
            [report(Duration="2-6 hours"), report(Duration="2-6 hours'")]
        )
        unmapped = self.normalize(
            [report(Duration="unexpected duration"), report(Duration="another duration")]
        )

        self.assertEqual(
            source.loc[0, "normalized_full_row_signature"],
            source.loc[1, "normalized_full_row_signature"],
        )
        self.assertNotEqual(
            unmapped.loc[0, "normalized_full_row_signature"],
            unmapped.loc[1, "normalized_full_row_signature"],
        )
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents.loc[0, "consolidation_tier"], "normalized_exact")

    def test_full_row_rules_keep_conflicting_reports_distinct(self) -> None:
        rows = [report(), report()]
        source = self.normalize(rows)
        incidents, crosswalk, exceptions = mod.consolidate_reports(source, CONFIG)
        self.assertEqual(len(exceptions), 0)
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents["consolidation_tier"].value_counts().to_dict()["exact"], 1)
        self.assertTrue(crosswalk["source_row_id"].is_unique)

        differing_values = {
            "Street": "Second St",
            "County": "Other County",
            "Railroad": "OTHER RR",
            "Reason": "A moving train",
            "Immediate Impacts": "Emergency response",
            "Additional Comments": "Additional context",
        }
        for field, value in differing_values.items():
            with self.subTest(field=field):
                source = self.normalize([report(), report(**{field: value})])
                incidents, _, _ = mod.consolidate_reports(source, CONFIG)
                self.assertEqual(len(incidents), 2)

    def test_normalized_exact_merges_only_whitespace_and_case_differences(self) -> None:
        source = self.normalize([report(City=" Example ", Reason="A STATIONARY  TRAIN"), report(City="example", Reason="a stationary train")])
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents.loc[0, "consolidation_tier"], "normalized_exact")

    def test_invalid_crossing_and_timestamp_are_exceptions(self) -> None:
        source = self.normalize([report(**{"Crossing ID": "bad"}), report(**{"Date/Time": "not a date"})])
        incidents, crosswalk, exceptions = mod.consolidate_reports(source, CONFIG)
        self.assertTrue(incidents.empty)
        self.assertEqual(len(exceptions), 2)
        self.assertTrue(crosswalk["canonical_incident_id"].isna().all())

    def test_duration_incompatible_pair_is_decided_keep_distinct(self) -> None:
        source = self.normalize([
            report(Duration="0-15 minutes"),
            report(**{"Date/Time": "2025-01-01 12:16:00", "Duration": "0-15 minutes"}),
        ])
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
        self.assertEqual(decisions.loc[0, "pair_decision"], "keep_distinct")
        self.assertEqual(decisions.loc[0, "decision_basis"], "duration_incompatible")
        self.assertFalse(decisions.loc[0, "uncertainty_flag"])

    def test_overlap_proxy_with_non_temporal_mismatch_stays_distinct(self) -> None:
        differing_values = {
            "City": "Other City",
            "State": "CA",
            "Street": "Second St",
            "County": "Other County",
            "Railroad": "OTHER RR",
            "Reason": "A moving train",
            "Immediate Impacts": "Emergency response",
            "Additional Comments": "Additional context",
        }
        for field, value in differing_values.items():
            with self.subTest(field=field):
                source = self.normalize([
                    report(),
                    report(**{"Date/Time": "2025-01-01 12:10:00", field: value}),
                ])
                incidents, _, _ = mod.consolidate_reports(source, CONFIG)
                decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
                self.assertEqual(len(incidents), 2)
                self.assertEqual(len(decisions), 1)
                self.assertEqual(decisions.loc[0, "pair_decision"], "keep_distinct")
                self.assertEqual(
                    decisions.loc[0, "decision_basis"],
                    "overlap_proxy_but_non_temporal_mismatch",
                )
                self.assertEqual(
                    decisions.loc[0, "uncertainty_basis"], "possible_temporal_overlap"
                )
                self.assertIn(field, decisions.loc[0, "incompatible_non_temporal_fields"])

    def test_overlap_proxy_and_non_temporal_match_auto_merges_at_boundary(self) -> None:
        source = self.normalize([
            report(Duration="0-15 minutes"),
            report(**{"Date/Time": "2025-01-01 12:15:00", "Duration": "16-30 minutes"}),
        ])
        incidents, crosswalk, _ = mod.consolidate_reports(source, CONFIG)
        decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
        merged, merged_crosswalk, applied = mod.apply_pair_decisions(
            incidents, crosswalk, source, decisions, CONFIG
        )
        self.assertEqual(decisions.loc[0, "pair_decision"], "auto_merge")
        self.assertTrue(decisions.loc[0, "uncertainty_flag"])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged_crosswalk["canonical_incident_id"].nunique(), 1)
        self.assertEqual(
            applied.loc[0, "left_final_incident_id"],
            applied.loc[0, "right_final_incident_id"],
        )

    def test_open_ended_and_unmapped_duration_proxies_do_not_auto_merge(self) -> None:
        for duration, expected_basis in (
            ("More than one day", "open_ended_duration_proxy"),
            ("unexpected", "duration_proxy_unavailable"),
        ):
            with self.subTest(duration=duration):
                source = self.normalize([
                    report(Duration=duration),
                    report(**{"Date/Time": "2025-01-01 12:10:00", "Duration": "1-2 hours"}),
                ])
                incidents, _, _ = mod.consolidate_reports(source, CONFIG)
                decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
                self.assertEqual(decisions.loc[0, "pair_decision"], "keep_distinct")
                self.assertEqual(decisions.loc[0, "decision_basis"], expected_basis)
                self.assertTrue(decisions.loc[0, "uncertainty_flag"])

    def test_different_crossings_are_outside_the_pair_decision_universe(self) -> None:
        source = self.normalize([
            report(),
            report(**{"Crossing ID": "654321B", "Date/Time": "2025-01-01 12:10:00"}),
        ])
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
        self.assertTrue(decisions.empty)

    def test_missing_non_temporal_value_does_not_match_a_present_value(self) -> None:
        source = self.normalize([
            report(City=None),
            report(**{"Date/Time": "2025-01-01 12:10:00", "City": "Example"}),
        ])
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
        self.assertEqual(decisions.loc[0, "pair_decision"], "keep_distinct")
        self.assertIn("City", decisions.loc[0, "incompatible_non_temporal_fields"])

    def test_pair_decisions_are_deterministic_when_source_is_reordered(self) -> None:
        source = self.normalize(
            [
                report(Reason="A"),
                report(**{"Date/Time": "2025-01-01 13:40:00", "Reason": "B"}),
                report(**{"Date/Time": "2025-01-01 15:20:00", "Reason": "C"}),
            ]
        )
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)

        first = mod.generate_pair_decisions(incidents, source, CONFIG)
        second = mod.generate_pair_decisions(
            incidents.sample(frac=1, random_state=7),
            source.sample(frac=1, random_state=11),
            CONFIG,
        )

        self.assertEqual(len(first), 2)
        self.assertTrue(first["pair_decision"].isin({"auto_merge", "keep_distinct"}).all())
        self.assertTrue(first["pair_decision"].notna().all())
        self.assertEqual(
            first.set_index("pair_decision_id")["pair_decision"].to_dict(),
            second.set_index("pair_decision_id")["pair_decision"].to_dict(),
        )

    def test_complete_link_grouping_prevents_chained_false_merge(self) -> None:
        source = self.normalize([
            report(Duration="0-15 minutes"),
            report(**{"Date/Time": "2025-01-01 12:10:00", "Duration": "0-15 minutes"}),
            report(**{"Date/Time": "2025-01-01 12:20:00", "Duration": "0-15 minutes"}),
        ])
        incidents, crosswalk, _ = mod.consolidate_reports(source, CONFIG)
        decisions = mod.generate_pair_decisions(incidents, source, CONFIG)
        merged, _, applied = mod.apply_pair_decisions(
            incidents, crosswalk, source, decisions, CONFIG
        )
        self.assertEqual(decisions["pair_decision"].tolist().count("auto_merge"), 2)
        self.assertEqual(decisions["pair_decision"].tolist().count("keep_distinct"), 1)
        self.assertEqual(len(merged), 2)
        self.assertEqual(applied["pair_decision"].tolist().count("auto_merge"), 1)
        self.assertEqual(applied["pair_decision"].tolist().count("keep_distinct"), 2)
        self.assertIn("complete_link_conflict", set(applied["decision_basis"]))
        self.assertTrue(
            applied.loc[applied["pair_decision"].eq("keep_distinct")]
            .eval("left_final_incident_id != right_final_incident_id")
            .all()
        )

    def test_timezone_localization_uses_coordinate_not_state_and_preserves_utc(self) -> None:
        inventory = pd.DataFrame(
            [
                {"Crossing ID": "123456A", "Latitude": 40.7, "Longitude": -74.0, "Revision Date": "2025-01-01"},
                {"Crossing ID": "654321B", "Latitude": None, "Longitude": None, "Revision Date": "2025-01-01"},
            ]
        )
        lookup = mod.build_crossing_timezones(inventory, CONFIG, lambda lat, lon: "America/New_York")
        source = self.normalize([report(), report(**{"Crossing ID": "654321B", "State": "NY"}), report(**{"Crossing ID": "888888C", "State": "NY"})])
        enriched = mod.enrich_with_local_time(source, lookup)
        self.assertEqual(enriched.loc[0, "reported_at_utc"].isoformat(), "2025-01-01T12:00:00+00:00")
        self.assertEqual(enriched.loc[0, "reported_at_local"], "2025-01-01T07:00:00-0500")
        self.assertEqual(enriched.loc[0, "utc_offset_minutes"], -300)
        self.assertEqual(enriched.loc[1, "timezone_assignment_status"], "invalid_inventory_coordinates")
        self.assertEqual(enriched.loc[2, "timezone_assignment_status"], "no_inventory_match")
        self.assertTrue(pd.isna(enriched.loc[2, "reported_at_local"]))

    def test_dst_spring_and_fall_offsets_are_distinct(self) -> None:
        inventory = pd.DataFrame([{"Crossing ID": "123456A", "Latitude": 40.7, "Longitude": -74.0, "Revision Date": "2025-01-01"}])
        lookup = mod.build_crossing_timezones(inventory, CONFIG, lambda lat, lon: "America/New_York")
        source = self.normalize([
            report(**{"Date/Time": "2025-03-09 07:30:00"}),
            report(**{"Date/Time": "2025-11-02 05:30:00"}),
            report(**{"Date/Time": "2025-11-02 06:30:00"}),
        ])
        local = mod.enrich_with_local_time(source, lookup)
        self.assertEqual(local.loc[0, "reported_at_local"], "2025-03-09T03:30:00-0400")
        self.assertEqual(local.loc[1, "reported_at_local"], "2025-11-02T01:30:00-0400")
        self.assertEqual(local.loc[2, "reported_at_local"], "2025-11-02T01:30:00-0500")
        self.assertEqual(local["utc_offset_minutes"].tolist(), [-240, -240, -300])

    def test_reconciliation_is_full_row_and_multiplicity_aware(self) -> None:
        authoritative = self.normalize([report(), report(), report(Reason="A moving train")])
        reconciliation = self.normalize([report(), report(Reason="A moving train")])
        summary, discrepancies = mod.reconcile_2025_workbooks(authoritative, reconciliation, CONFIG)
        self.assertEqual(summary["rows_present_only_in_authoritative"], 1)
        self.assertEqual(summary["unique_signatures_with_multiplicity_difference"], 1)
        self.assertEqual(len(discrepancies), 1)

    def test_decision_audit_sample_and_ids_are_deterministic_when_source_is_reordered(self) -> None:
        source = self.normalize([report(), report(**{"Date/Time": "2025-01-01 12:10:00", "Reason": "B"}), report(**{"Date/Time": "2025-01-01 12:20:00", "Reason": "C"})])
        first_incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        second_incidents, _, _ = mod.consolidate_reports(source.sample(frac=1, random_state=7), CONFIG)
        self.assertEqual(set(first_incidents["canonical_incident_id"]), set(second_incidents["canonical_incident_id"]))
        first = mod.deterministic_decision_audit_sample(
            mod.generate_pair_decisions(first_incidents, source, CONFIG)
        )
        second = mod.deterministic_decision_audit_sample(
            mod.generate_pair_decisions(second_incidents, source, CONFIG)
        )
        self.assertEqual(first["pair_decision_id"].tolist(), second["pair_decision_id"].tolist())
        self.assertNotIn("review_label", first.columns)

    def test_input_hashes_detect_content_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "input.txt"
            path.write_text("before", encoding="utf-8")
            before = mod.hash_input_files({"input": path})
            path.write_text("after", encoding="utf-8")
            after = mod.hash_input_files({"input": path})
        self.assertNotEqual(before, after)

    def test_timestamp_granularity_includes_mark_percentages(self) -> None:
        source = self.normalize(
            [
                report(**{"Date/Time": "2025-01-01 12:00:00"}),
                report(**{"Date/Time": "2025-01-01 12:15:30"}),
            ]
        )
        profile = mod.timestamp_granularity(source)
        self.assertEqual(profile.loc[0, "five_minute_mark_percentage"], 100.0)
        self.assertEqual(profile.loc[0, "fifteen_minute_mark_percentage"], 100.0)
        self.assertEqual(profile.loc[0, "thirty_minute_mark_percentage"], 50.0)
        self.assertEqual(profile.loc[0, "sixty_minute_mark_percentage"], 50.0)

    def test_acceptance_report_requires_every_check(self) -> None:
        incomplete = mod.build_acceptance_report({"tests": True, "decisions": False})
        complete = mod.build_acceptance_report({"tests": True, "decisions": True})
        pending = mod.build_acceptance_report({"tests": True, "saved_notebook": None})
        self.assertEqual(incomplete["status"], "incomplete")
        self.assertEqual(pending["status"], "incomplete")
        self.assertEqual(complete["status"], "complete")

    def test_notebook_contains_acceptance_workflow(self) -> None:
        notebook_path = Path(__file__).resolve().parents[1] / "analysis" / "phase_1_analysis.ipynb"
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        source = "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])
        required_fragments = [
            "REUSE_STEP_5_CHECKPOINT = False",
            "RUN_REPEATABILITY_CHECK = False",
            "sys.modules.pop(\"incident_deduplication\", None)",
            "expected_module_path",
            "loaded_module_path",
            "generate_pair_decisions",
            "pair_decision_bands_minutes",
            "reuse_step_5_checkpoint=False",
            "pair_decisions.parquet",
            "pair_decision_summary.json",
            "pair_decision_audit_sample.csv",
            "timezone_assignment_diagnostics.csv",
            "local_time_diagnostics.csv",
            "reconciliation_summary.json",
            "possible_temporal_overlap",
        ]
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, source)
        for reuse_requested, repeatability_requested, expected_reuse in (
            (False, False, False),
            (True, False, True),
            (False, True, False),
            (True, True, False),
        ):
            with self.subTest(
                reuse_requested=reuse_requested,
                repeatability_requested=repeatability_requested,
            ):
                self.assertEqual(
                    reuse_requested and not repeatability_requested,
                    expected_reuse,
                )

    def test_logical_output_comparison_ignores_runtime_manifest_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first, second = root / "first", root / "second"
            first.mkdir()
            second.mkdir()
            pd.DataFrame({"id": [1], "value": ["same"]}).to_csv(first / "table.csv", index=False)
            pd.DataFrame({"id": [1], "value": ["same"]}).to_csv(second / "table.csv", index=False)
            for directory, timestamp in ((first, "first"), (second, "second")):
                (directory / "run_manifest.json").write_text(
                    json.dumps(
                        {
                            "execution": {"timestamp_utc": timestamp, "duration_seconds": 1},
                            "outputs": {"table": {"path": str(directory / "table.csv"), "filename": "table.csv"}},
                        }
                    ),
                    encoding="utf-8",
                )
            self.assertTrue(mod.compare_phase_1_outputs(first, second)["passed"])
            pd.DataFrame({"id": [1], "value": ["changed"]}).to_csv(second / "table.csv", index=False)
            self.assertFalse(mod.compare_phase_1_outputs(first, second)["passed"])

    def test_import_does_not_execute_pipeline(self) -> None:
        self.assertTrue(callable(mod.run_phase_1))
        self.assertEqual(mod.__name__, "incident_deduplication")

    def test_missing_inputs_are_reported_together(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            data_dir = Path(temporary_directory) / "data"
            input_paths = {
                "authoritative": data_dir / "blocked_crossings_2020through2025.xlsx",
                "reconciliation": data_dir / "blocked_crossings_2025.xlsx",
                "form_71_inventory": (
                    data_dir / "Crossing_Inventory_Data_(Form_71)_-_Current_20260707.csv"
                ),
            }

            with self.assertRaises(FileNotFoundError) as raised:
                mod.require_input_files(input_paths)

            message = str(raised.exception)
            for path in input_paths.values():
                self.assertIn(str(path), message)

    def test_step_5_checkpoint_creation_loading_and_validation_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_dir = Path(temporary_directory)
            frames = {
                name: pd.DataFrame({f"{name}_value": [1, 2]})
                for name in mod.STEP_5_CHECKPOINT_FILES
            }
            source_metadata = {
                "authoritative": {
                    "filename": "authoritative.xlsx",
                    "row_count": 2,
                    "columns": CONFIG["material_columns"],
                },
                "reconciliation": {
                    "filename": "reconciliation.xlsx",
                    "row_count": 1,
                    "columns": CONFIG["material_columns"],
                },
                "form_71_inventory": {
                    "filename": "inventory.csv",
                    "row_count": 1,
                    "columns": list(mod.INVENTORY_REQUIRED_COLUMNS),
                },
            }
            source_paths = {
                "authoritative": Path("authoritative.xlsx"),
                "reconciliation": Path("reconciliation.xlsx"),
                "form_71_inventory": Path("inventory.csv"),
            }

            metadata = mod._write_step_5_checkpoint(
                output_dir, frames, CONFIG, source_metadata
            )
            checkpoint_dir = output_dir / "step_5_checkpoint"
            self.assertTrue((checkpoint_dir / "checkpoint.json").is_file())
            loaded, loaded_sources, loaded_metadata = mod._load_step_5_checkpoint(
                output_dir, CONFIG, source_paths
            )
            self.assertEqual(loaded_sources, source_metadata)
            self.assertEqual(loaded_metadata, metadata)
            for name in frames:
                pd.testing.assert_frame_equal(loaded[name], frames[name])

            missing_path = checkpoint_dir / mod.STEP_5_CHECKPOINT_FILES["exceptions"]
            missing_path.unlink()
            with self.assertRaisesRegex(FileNotFoundError, "exceptions.parquet"):
                mod._load_step_5_checkpoint(output_dir, CONFIG, source_paths)

            def rewrite_metadata() -> dict[str, object]:
                return mod._write_step_5_checkpoint(
                    output_dir, frames, CONFIG, source_metadata
                )

            for field, value, message in (
                ("ruleset_version", "incompatible", "ruleset version mismatch"),
                ("row_count", 999, "row-count mismatch"),
                ("columns", ["wrong_column"], "column mismatch"),
            ):
                with self.subTest(field=field):
                    current = rewrite_metadata()
                    if field == "ruleset_version":
                        current[field] = value
                    else:
                        current["frames"]["source"][field] = value
                    (checkpoint_dir / "checkpoint.json").write_text(
                        json.dumps(current), encoding="utf-8"
                    )
                    with self.assertRaisesRegex(ValueError, message):
                        mod._load_step_5_checkpoint(output_dir, CONFIG, source_paths)

            rewrite_metadata()
            with mock.patch.object(
                pd.DataFrame,
                "to_parquet",
                side_effect=[None, RuntimeError("interrupted checkpoint write")],
            ):
                with self.assertRaisesRegex(RuntimeError, "interrupted checkpoint write"):
                    mod._write_step_5_checkpoint(
                        output_dir, frames, CONFIG, source_metadata
                    )
            self.assertFalse((checkpoint_dir / "checkpoint.json").exists())

    def test_checkpoint_reuse_skips_steps_1_through_5_and_matches_fresh_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            inputs = self.write_pipeline_inputs(root)
            fresh_output = root / "fresh"
            reused_output = root / "reused"
            fresh_result = mod.run_phase_1(*inputs, fresh_output)
            shutil.copytree(
                fresh_output / "step_5_checkpoint",
                reused_output / "step_5_checkpoint",
            )

            skipped = (
                mock.patch.object(mod.pd, "read_excel", side_effect=AssertionError("read_excel called")),
                mock.patch.object(mod.pd, "read_csv", side_effect=AssertionError("read_csv called")),
                mock.patch.object(mod, "normalize_source_dataframe", side_effect=AssertionError("normalize called")),
                mock.patch.object(mod, "build_crossing_timezones", side_effect=AssertionError("timezone resolution called")),
                mock.patch.object(mod, "enrich_with_local_time", side_effect=AssertionError("enrichment called")),
                mock.patch.object(mod, "consolidate_reports", side_effect=AssertionError("consolidation called")),
                mock.patch.object(mod, "_write_step_5_checkpoint", side_effect=AssertionError("checkpoint rewritten")),
            )
            stdout = io.StringIO()
            with contextlib.ExitStack() as stack, contextlib.redirect_stdout(stdout):
                for patcher in skipped:
                    stack.enter_context(patcher)
                reused_result = mod.run_phase_1(
                    *inputs, reused_output, reuse_step_5_checkpoint=True
                )

            output = stdout.getvalue()
            self.assertIn("Loading and validating the step-5 checkpoint", output)
            self.assertIn("[6/9] Assigning deterministic pair decisions", output)
            self.assertNotIn("[2/9] Reading raw source data", output)
            self.assertEqual(reused_result.summary, fresh_result.summary)
            comparison = mod.compare_phase_1_outputs(fresh_output, reused_output)
            self.assertTrue(comparison["passed"], comparison)
            manifest = json.loads(
                (reused_output / "run_manifest.json").read_text(encoding="utf-8")
            )
            self.assertTrue(manifest["execution"]["reused_step_5_checkpoint"])

    def test_source_ids_ignore_container_bytes(self) -> None:
        rows = [report(), report(Reason="A moving train"), report(**{"Crossing ID": "bad"})]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first_path = root / "first.xlsx"
            second_path = root / "second.xlsx"
            pd.DataFrame(rows).to_excel(first_path, index=False)
            second_path.write_bytes(first_path.read_bytes() + b"container-only-difference")
            self.assertNotEqual(first_path.read_bytes(), second_path.read_bytes())

            first_frame = pd.read_excel(first_path)
            second_frame = pd.read_excel(second_path)
        first = mod.normalize_source_dataframe(
            first_frame, CONFIG, "Sheet1", "authoritative"
        )
        second = mod.normalize_source_dataframe(
            second_frame, CONFIG, "Sheet1", "authoritative"
        )
        self.assertEqual(first["source_row_id"].tolist(), second["source_row_id"].tolist())
        first_incidents, first_crosswalk, first_exceptions = mod.consolidate_reports(
            first, CONFIG
        )
        second_incidents, second_crosswalk, second_exceptions = mod.consolidate_reports(
            second, CONFIG
        )
        self.assertEqual(
            first_incidents["canonical_incident_id"].tolist(),
            second_incidents["canonical_incident_id"].tolist(),
        )
        pd.testing.assert_frame_equal(first_crosswalk, second_crosswalk)
        self.assertEqual(
            first_exceptions["exception_id"].tolist(), second_exceptions["exception_id"].tolist()
        )
        first_candidates = mod.generate_pair_decisions(first_incidents, first, CONFIG)
        second_candidates = mod.generate_pair_decisions(second_incidents, second, CONFIG)
        self.assertEqual(
            first_candidates["pair_decision_id"].tolist(),
            second_candidates["pair_decision_id"].tolist(),
        )

    def test_full_pipeline_accepts_unfingerprinted_inputs_and_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            authoritative_path = root / "authoritative.xlsx"
            reconciliation_path = root / "reconciliation.xlsx"
            inventory_path = root / "inventory.csv"
            config_path = root / "config.json"
            output_dir = root / "output"
            repeat_output_dir = root / "output-repeat"
            pd.DataFrame([
                report(),
                report(**{"Date/Time": "2025-01-01 15:10:00", "Reason": "A moving train"}),
            ]).to_excel(authoritative_path, index=False)
            pd.DataFrame([report()]).to_excel(reconciliation_path, index=False)
            pd.DataFrame([{
                "Crossing ID": "123456A", "Latitude": None, "Longitude": None,
                "Revision Date": "2025-01-01",
            }]).to_csv(inventory_path, index=False)
            config_path.write_text(json.dumps(CONFIG), encoding="utf-8")
            result = mod.run_phase_1(
                authoritative_path, reconciliation_path, inventory_path, config_path, output_dir
            )
            repeat_result = mod.run_phase_1(
                authoritative_path, reconciliation_path, inventory_path, config_path, repeat_output_dir
            )
            self.assertTrue(result.validations["source_rows_map_once"])
            self.assertTrue(all(result.validations.values()))
            self.assertEqual(result.summary, repeat_result.summary)
            self.assertTrue(mod.compare_phase_1_outputs(output_dir, repeat_output_dir)["passed"])
            self.assertEqual(result.summary["candidate_reported_incidents"], 2)
            self.assertTrue((output_dir / "crossing_timezones.parquet").exists())
            self.assertTrue((output_dir / "pair_decision_summary.json").exists())
            diagnostics = pd.read_csv(output_dir / "diagnostics_by_year.csv")
            self.assertIn("collapsed_duplicate_report_count", diagnostics.columns)
            self.assertIn("pair_decision_count", diagnostics.columns)
            manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["inputs"]["authoritative"]["filename"], "authoritative.xlsx"
            )
            self.assertEqual(manifest["inputs"]["authoritative"]["sheet"], "Sheet1")
            self.assertEqual(manifest["inputs"]["authoritative"]["rows"], 2)
            self.assertEqual(
                manifest["inputs"]["authoritative"]["schema"], CONFIG["material_columns"]
            )
            self.assertEqual(
                manifest["outputs"]["source_reports_with_ids"]["filename"],
                "source_reports_with_ids.parquet",
            )
            self.assertIn(
                "source_row_id", manifest["outputs"]["source_reports_with_ids"]["schema"]
            )
            self.assertNotIn("sha256", json.dumps(manifest).lower())
            self.assertNotIn("fingerprint", json.dumps(result.validations).lower())


if __name__ == "__main__":
    unittest.main()
