from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ANALYSIS_DIR = Path(__file__).resolve().parents[1] / "analysis"
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_DIR))

import reported_event_exposure as mod


class ReportedEventExposureTests(unittest.TestCase):
    def write_phase1(self, root: Path, *, ruleset: str = "phase1-v3") -> Path:
        phase1 = root / "phase1"
        phase1.mkdir()
        source = pd.DataFrame(
            [
                {
                    "source_row_id": "SRC-1",
                    "norm_crossing_id": "123456A",
                    "reported_at_utc": pd.Timestamp("2025-01-01T00:30:00Z"),
                    "norm_duration": "1-2 hours",
                    "duration_lower_minutes": 60,
                    "duration_upper_minutes": 120,
                    "duration_normalization_status": "canonical",
                    "iana_time_zone": "America/Chicago",
                },
                {
                    "source_row_id": "SRC-2",
                    "norm_crossing_id": "123456A",
                    "reported_at_utc": pd.Timestamp("2025-01-01T00:45:00Z"),
                    "norm_duration": "0-15 minutes",
                    "duration_lower_minutes": 0,
                    "duration_upper_minutes": 15,
                    "duration_normalization_status": "canonical",
                    "iana_time_zone": "America/Chicago",
                },
            ]
        )
        incidents = pd.DataFrame(
            [
                {
                    "canonical_incident_id": "INC-1",
                    "norm_crossing_id": "123456A",
                    "earliest_reported_at_utc": pd.Timestamp("2025-01-01T00:30:00Z"),
                    "primary_source_row_id": "SRC-1",
                    "ruleset_version": ruleset,
                },
                {
                    "canonical_incident_id": "INC-2",
                    "norm_crossing_id": "123456A",
                    "earliest_reported_at_utc": pd.Timestamp("2025-01-01T00:45:00Z"),
                    "primary_source_row_id": "SRC-2",
                    "ruleset_version": ruleset,
                },
            ]
        )
        crosswalk = pd.DataFrame(
            [
                {"source_row_id": "SRC-1", "canonical_incident_id": "INC-1", "exception_id": pd.NA},
                {"source_row_id": "SRC-2", "canonical_incident_id": "INC-2", "exception_id": pd.NA},
            ]
        )
        exceptions = pd.DataFrame(
            columns=["exception_id", "source_row_id", "norm_crossing_id", "reported_at_utc"]
        )
        pair_decisions = pd.DataFrame(
            [
                {
                    "pair_decision_id": "PAIR-1",
                    "pair_decision": "keep_distinct",
                    "decision_basis": "overlap_proxy_but_non_temporal_mismatch",
                    "uncertainty_flag": True,
                    "uncertainty_basis": "possible_temporal_overlap",
                    "left_final_incident_id": "INC-1",
                    "right_final_incident_id": "INC-2",
                }
            ]
        )
        frames = {
            "source_reports_with_ids.parquet": source,
            "reported_incidents.parquet": incidents,
            "report_incident_crosswalk.parquet": crosswalk,
            "documented_exceptions.parquet": exceptions,
            "pair_decisions.parquet": pair_decisions,
        }
        for filename, frame in frames.items():
            frame.to_parquet(phase1 / filename, index=False)
        (phase1 / "pair_decision_summary.json").write_text(
            json.dumps({"pair_count": 1}), encoding="utf-8"
        )
        (phase1 / "phase_1_gate_report.json").write_text(
            json.dumps({"status": "complete", "validation": {"fixture": True}}),
            encoding="utf-8",
        )
        (phase1 / "run_manifest.json").write_text(
            json.dumps(
                {
                    "ruleset_version": ruleset,
                    "outputs": {
                        filename.removesuffix(".parquet"): {
                            "filename": filename,
                            "row_count": len(frame),
                            "schema": list(frame.columns),
                        }
                        for filename, frame in frames.items()
                    },
                }
            ),
            encoding="utf-8",
        )
        return phase1

    def write_inputs(self, root: Path) -> tuple[Path, Path, Path]:
        inventory = root / "inventory.csv"
        pd.DataFrame(
            [
                {
                    "Crossing ID": "123456A", "Crossing Closed": "No",
                    "Revision Date": "2024-01-01", "Latitude": 29.0, "Longitude": -95.0,
                    "State Code": "48", "State Name": "Texas", "County Code": "201",
                    "County Name": "Harris", "Railroad Code": "RR", "Railroad Name": "Railroad",
                },
                {
                    "Crossing ID": "123456A", "Crossing Closed": "No",
                    "Revision Date": "2025-01-01", "Latitude": 29.1, "Longitude": -95.1,
                    "State Code": "48", "State Name": "Texas", "County Code": "201",
                    "County Name": "Harris", "Railroad Code": "RR", "Railroad Name": "Railroad",
                },
                {
                    "Crossing ID": "654321B", "Crossing Closed": "No",
                    "Revision Date": "2025-01-01", "Latitude": None, "Longitude": None,
                    "State Code": "48", "State Name": "Texas", "County Code": "201",
                    "County Name": "Harris", "Railroad Code": "RR", "Railroad Name": "Railroad",
                },
                {
                    "Crossing ID": "999999C", "Crossing Closed": "No",
                    "Revision Date": "2025-01-01", "Latitude": 40.0, "Longitude": -110.0,
                    "State Code": "49", "State Name": "Utah", "County Code": "035",
                    "County Name": "Salt Lake", "Railroad Code": "RR", "Railroad Name": "Railroad",
                },
            ]
        ).to_csv(inventory, index=False)
        coverage = root / "coverage.csv"
        pd.DataFrame(
            [
                {
                    "coverage_id": "COV-1", "source_name": "fixture",
                    "source_version": "1", "scope_type": "global", "scope_id": "*",
                    "coverage_start": "2025-01-01T00:00:00Z",
                    "coverage_end": "2025-01-01T02:00:00Z", "time_zone": "UTC",
                    "coverage_status": "demonstrated", "evidence_reference": "fixture",
                }
            ]
        ).to_csv(coverage, index=False)
        cohort = root / "cohort.csv"
        pd.DataFrame(
            [{"norm_crossing_id": "123456A", "eligibility_as_of": "2025-01-01T00:00:00Z"}]
        ).to_csv(cohort, index=False)
        return inventory, coverage, cohort

    def write_regions(self, root: Path, *, overlapping: bool = False) -> Path:
        features = [
            {
                "type": "Feature",
                "properties": {"MPO_ID": "001", "MPO_NAME": "Test MPO"},
                "geometry": {"type": "Polygon", "coordinates": [[[-96, 28], [-94, 28], [-94, 30], [-96, 30], [-96, 28]]]},
            }
        ]
        if overlapping:
            features.append(
                {
                    "type": "Feature",
                    "properties": {"MPO_ID": "002", "MPO_NAME": "Overlap MPO"},
                    "geometry": {"type": "Polygon", "coordinates": [[[-96, 28], [-94, 28], [-94, 30], [-96, 30], [-96, 28]]]},
                }
            )
        path = root / "regions.geojson"
        path.write_text(
            json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8"
        )
        return path

    def config(self, inventory: Path, coverage: Path, cohort: Path, regions: Path) -> dict:
        return {
            "pipeline_version": "phase2-v1-test",
            "expected_phase1_ruleset": "phase1-v3",
            "form71_inventory_path": str(inventory),
            "source_coverage_path": str(coverage),
            "cohort_path": str(cohort),
            "requested_region_ids": ["mpo:001"],
            "eligibility_as_of": "2025-01-01T00:00:00Z",
            "analysis_window": {"start": "2025-01-01T00:00:00Z", "end": "2025-01-01T04:00:00Z"},
            "candidate_interval_units_hours": [1, 2, 4],
            "selected_interval_unit_hours": 1,
            "max_materialized_rows": 20,
            "crossing_id_pattern": r"^\d{6}[A-Z]$",
            "inventory_columns": {
                "crossing_id": "Crossing ID", "closed": "Crossing Closed",
                "revision_date": "Revision Date", "latitude": "Latitude", "longitude": "Longitude",
                "state_code": "State Code", "state_name": "State Name",
                "county_code": "County Code", "county_name": "County Name",
                "railroad_code": "Railroad Code", "railroad_name": "Railroad Name",
            },
            "region_source": {
                "region_type": "mpo", "path": str(regions), "id_field": "MPO_ID",
                "name_field": "MPO_NAME", "source_name": "fixture", "source_version": "1",
                "effective_date": "2025-01-01", "license": "test", "approval_status": "approved",
            },
            "sampling": {
                "selected_strategy": "full_filtered_exposure", "negative_fraction": 0.5,
                "seed": 7, "weighted_summary_absolute_tolerance": 2.0,
            },
        }

    def test_rejects_old_phase1_ruleset_and_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root, ruleset="phase1-v2")
            with self.assertRaisesRegex(ValueError, "ruleset mismatch"):
                mod.validate_phase1_handoff(phase1, "phase1-v3")

            manifest = json.loads((phase1 / "run_manifest.json").read_text())
            manifest["ruleset_version"] = "phase1-v3"
            (phase1 / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            incidents = pd.read_parquet(phase1 / "reported_incidents.parquet").drop(
                columns="norm_crossing_id"
            )
            incidents.to_parquet(phase1 / "reported_incidents.parquet", index=False)
            with self.assertRaisesRegex(ValueError, "missing required columns"):
                mod.validate_phase1_handoff(phase1, "phase1-v3")

            (phase1 / "phase_1_gate_report.json").write_text(
                json.dumps({"status": "incomplete"}), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "gate is not complete"):
                mod.validate_phase1_handoff(phase1, "phase1-v3")

    def test_preflight_fails_closed_without_policy_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            inventory, _, _ = self.write_inputs(root)
            regions = self.write_regions(root)
            config = self.config(inventory, root / "missing.csv", root / "missing-cohort.csv", regions)
            config["requested_region_ids"] = []
            config["eligibility_as_of"] = None
            config["selected_interval_unit_hours"] = None
            config["region_source"]["approval_status"] = "provisional"
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            result = mod.run_phase_2(phase1, config_path, root / "output")
            self.assertEqual(result.gate["status"], "incomplete")
            self.assertFalse((root / "output" / "interval_exposure.parquet").exists())
            self.assertTrue((root / "output" / "phase_2_gate_report.json").exists())

    def test_inventory_preserves_nulls_and_selects_latest_revision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            frames, _ = mod.validate_phase1_handoff(phase1, "phase1-v3")
            inventory, coverage, cohort = self.write_inputs(root)
            regions = self.write_regions(root)
            config = self.config(inventory, coverage, cohort, regions)
            crossings, diagnostics = mod.build_canonical_crossings(inventory, frames["incidents"], config)
            selected = crossings.set_index("norm_crossing_id")
            self.assertEqual(selected.loc["123456A", "latitude"], 29.1)
            self.assertTrue(pd.isna(selected.loc["654321B", "latitude"]))
            self.assertEqual(selected.loc["654321B", "coordinate_status"], "invalid_or_missing")
            self.assertEqual(
                int(diagnostics.set_index("metric").loc["duplicate_inventory_rows", "count"]), 1
            )

    def test_many_to_many_region_membership_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            frames, _ = mod.validate_phase1_handoff(phase1, "phase1-v3")
            inventory, coverage, cohort = self.write_inputs(root)
            regions = self.write_regions(root, overlapping=True)
            config = self.config(inventory, coverage, cohort, regions)
            crossings, _ = mod.build_canonical_crossings(inventory, frames["incidents"], config)
            _, membership, _ = mod.build_region_membership(crossings, config["region_source"], root)
            matches = membership.loc[membership["norm_crossing_id"].eq("123456A")]
            self.assertEqual(len(matches), 2)
            self.assertTrue(matches["assignment_status"].eq("ambiguous").all())

    def test_half_open_labels_coverage_uncertainty_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            frames, _ = mod.validate_phase1_handoff(phase1, "phase1-v3")
            inventory, coverage_path, cohort_path = self.write_inputs(root)
            cohort = mod.load_cohort(cohort_path)
            coverage = mod.load_coverage(coverage_path)
            membership = pd.DataFrame(
                [{"norm_crossing_id": "123456A", "region_id": "mpo:001"}]
            )
            exceptions = pd.DataFrame(
                [
                    {
                        "exception_id": "EXC-1", "source_row_id": "SRC-X",
                        "norm_crossing_id": "123456A",
                        "reported_at_utc": pd.Timestamp("2025-01-01T01:30:00Z"),
                    }
                ]
            )
            exposure, crosswalk, sensitivity, metrics = mod.build_interval_tables(
                1, pd.Timestamp("2025-01-01T00:00:00Z"), pd.Timestamp("2025-01-01T04:00:00Z"),
                cohort, membership, coverage, frames["incidents"], frames["source"],
                frames["pair_decisions"], exceptions, 20,
            )
            self.assertEqual(exposure["point_label"].tolist(), [
                "report_observed", "no_report_observed", "unknown", "unknown"
            ])
            self.assertEqual(len(crosswalk), 2)
            self.assertEqual(crosswalk["interval_id"].nunique(), 1)
            self.assertGreater(len(sensitivity), 1)
            self.assertTrue(exposure.loc[0, "has_pair_decision_uncertainty"])
            self.assertTrue(exposure.loc[1, "has_exception_uncertainty"])
            self.assertTrue(exposure.loc[2, "has_duration_overlap_sensitivity"])
            self.assertEqual(exposure.loc[2, "point_label"], "unknown")
            self.assertEqual(metrics["multiple_incident_intervals"], 1)

    def test_incident_on_boundary_is_assigned_to_following_interval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            frames, _ = mod.validate_phase1_handoff(phase1, "phase1-v3")
            _, coverage_path, cohort_path = self.write_inputs(root)
            frames["incidents"].loc[1, "earliest_reported_at_utc"] = pd.Timestamp(
                "2025-01-01T01:00:00Z"
            )
            exposure, crosswalk, _, _ = mod.build_interval_tables(
                1, pd.Timestamp("2025-01-01T00:00:00Z"), pd.Timestamp("2025-01-01T04:00:00Z"),
                mod.load_cohort(cohort_path),
                pd.DataFrame([{"norm_crossing_id": "123456A", "region_id": "mpo:001"}]),
                mod.load_coverage(coverage_path), frames["incidents"], frames["source"],
                frames["pair_decisions"], frames["exceptions"], 20,
            )
            assigned = crosswalk.set_index("canonical_incident_id")["interval_id"]
            interval_starts = exposure.set_index("interval_id")["interval_start"]
            self.assertEqual(interval_starts[assigned["INC-2"]], pd.Timestamp("2025-01-01T01:00:00Z"))

    def test_eligibility_as_of_excludes_prior_intervals_and_incidents(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            frames, _ = mod.validate_phase1_handoff(phase1, "phase1-v3")
            _, coverage_path, cohort_path = self.write_inputs(root)
            cohort = pd.read_csv(cohort_path)
            cohort["eligibility_as_of"] = "2025-01-01T01:00:00Z"
            cohort.to_csv(cohort_path, index=False)
            exposure, crosswalk, _, _ = mod.build_interval_tables(
                1, pd.Timestamp("2025-01-01T00:00:00Z"), pd.Timestamp("2025-01-01T04:00:00Z"),
                mod.load_cohort(cohort_path),
                pd.DataFrame([{"norm_crossing_id": "123456A", "region_id": "mpo:001"}]),
                mod.load_coverage(coverage_path), frames["incidents"], frames["source"],
                frames["pair_decisions"], frames["exceptions"], 20,
            )
            self.assertEqual(exposure["interval_start"].min(), pd.Timestamp("2025-01-01T01:00:00Z"))
            self.assertTrue(crosswalk.empty)

    def test_sampling_is_deterministic_and_weighted(self) -> None:
        exposure = pd.DataFrame(
            {
                "interval_id": [f"INT-{index}" for index in range(20)],
                "crossing_id": ["123456A"] * 20,
                "interval_start": pd.date_range("2025-01-01", periods=20, freq="h", tz="UTC"),
                "point_label": ["report_observed"] + ["no_report_observed"] * 18 + ["unknown"],
            }
        )
        first = mod.deterministic_training_sample(exposure, 0.5, 7)
        second = mod.deterministic_training_sample(exposure.sample(frac=1), 0.5, 7)
        pd.testing.assert_frame_equal(first, second)
        self.assertEqual(first.loc[first["point_label"].eq("report_observed"), "sample_weight"].iloc[0], 1.0)
        self.assertTrue(first.loc[first["point_label"].eq("no_report_observed"), "sample_weight"].eq(2.0).all())
        self.assertTrue(first["sampling_stratum"].eq("123456A|2025").all())
        self.assertNotIn("unknown", set(first["point_label"]))

    def test_interval_comparison_runs_before_interval_selection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            inventory, coverage, cohort = self.write_inputs(root)
            regions = self.write_regions(root)
            config = self.config(inventory, coverage, cohort, regions)
            config["selected_interval_unit_hours"] = None
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            result = mod.run_phase_2(phase1, config_path, root / "output")
            self.assertEqual(result.gate["status"], "incomplete")
            self.assertTrue((root / "output" / "interval_unit_comparison.csv").exists())
            self.assertFalse((root / "output" / "interval_exposure.parquet").exists())
            self.assertEqual(len(result.summary["candidate_unit_comparisons"]), 3)

    def test_region_and_cohort_filtering_precede_interval_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            inventory, coverage, cohort_path = self.write_inputs(root)
            pd.DataFrame(
                [
                    {"norm_crossing_id": "123456A", "eligibility_as_of": "2025-01-01T00:00:00Z"},
                    {"norm_crossing_id": "999999C", "eligibility_as_of": "2025-01-01T00:00:00Z"},
                ]
            ).to_csv(cohort_path, index=False)
            regions = self.write_regions(root)
            config = self.config(inventory, coverage, cohort_path, regions)
            config["max_materialized_rows"] = 4
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            result = mod.run_phase_2(phase1, config_path, root / "output")
            exposure = pd.read_parquet(result.artifact_paths["interval_exposure"])
            self.assertEqual(set(exposure["crossing_id"]), {"123456A"})
            self.assertEqual(len(exposure), 4)

    def test_complete_pipeline_writes_contract_artifacts_and_repeats(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            phase1 = self.write_phase1(root)
            inventory, coverage, cohort = self.write_inputs(root)
            regions = self.write_regions(root)
            config = self.config(inventory, coverage, cohort, regions)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            first_dir, second_dir = root / "first", root / "second"
            first = mod.run_phase_2(phase1, config_path, first_dir)
            second = mod.run_phase_2(phase1, config_path, second_dir)
            self.assertEqual(first.gate["status"], "complete")
            self.assertTrue(all(first.validations.values()))
            self.assertEqual(first.summary, second.summary)
            self.assertTrue(mod.compare_phase2_outputs(first_dir, second_dir)["passed"])
            expected = {
                "canonical_crossings.parquet", "source_coverage_periods.parquet",
                "region_definitions.json", "crossing_region_membership.parquet",
                "region_assignment_diagnostics.csv", "interval_exposure.parquet",
                "interval_incident_crosswalk.parquet", "duration_overlap_sensitivity.parquet",
                "uncertainty_diagnostics.csv", "interval_unit_comparison.csv",
                "exposure_strategy_comparison.json", "phase_2_gate_report.json", "run_manifest.json",
            }
            self.assertEqual(expected, {path.name for path in first_dir.iterdir()})
            diagnostics = pd.read_csv(first_dir / "uncertainty_diagnostics.csv")
            self.assertTrue(
                {"overall", "year", "crossing", "region", "crossing_volume_tier"}.issubset(
                    set(diagnostics["dimension"])
                )
            )
            comparison = pd.read_csv(first_dir / "interval_unit_comparison.csv")
            self.assertEqual(set(comparison["interval_unit_hours"]), {1, 2, 4})
            self.assertIn("natural_report_prevalence", comparison.columns)

    def test_notebook_is_thin_and_has_no_legacy_claims(self) -> None:
        notebook = json.loads(
            (Path(__file__).resolve().parents[1] / "analysis" / "phase_2_analysis.ipynb").read_text(
                encoding="utf-8"
            )
        )
        source = "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])
        self.assertIn("run_phase_2", source)
        self.assertEqual(source.count("run_phase_2("), 1)
        self.assertNotIn("\ndef ", source)
        self.assertIn("analysis_outputs' / 'deduplication' / 'v3", source)
        self.assertIn("analysis_outputs' / 'reported_event_exposure' / 'v1", source)
        for forbidden in (
            "def generate_phase2_hourly_grid_batched", "target_y", "Tier 4",
            "first_report_time", "est_end_time", "hotspot_report_threshold",
            "997,395,072", "PASSED",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
