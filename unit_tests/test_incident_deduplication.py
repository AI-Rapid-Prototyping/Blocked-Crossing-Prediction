from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import incident_deduplication as mod


CONFIG = {
    "ruleset_version": "phase1-v2-test",
    "authoritative_sheet": "Sheet1",
    "material_columns": [
        "Crossing ID", "City", "State", "Street", "County", "Railroad",
        "Date/Time", "Duration", "Reason", "Immediate Impacts", "Additional Comments",
    ],
    "crossing_id_pattern": r"^\d{6}[A-Z]$",
    "auto_merge_tiers": ["exact", "normalized_exact"],
    "proximity_bands_minutes": [15, 30, 60, 120],
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


class IncidentDeduplicationTests(unittest.TestCase):
    def normalize(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        return mod.normalize_source_dataframe(
            pd.DataFrame(rows), CONFIG, "f" * 64, "Sheet1", "authoritative"
        )

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

    def test_full_row_rules_keep_conflicting_reports_distinct(self) -> None:
        rows = [report(), report(), report(Reason="A moving train"), report(**{"Immediate Impacts": "Emergency response"})]
        source = self.normalize(rows)
        incidents, crosswalk, exceptions = mod.consolidate_reports(source, CONFIG)
        self.assertEqual(len(exceptions), 0)
        self.assertEqual(len(incidents), 3)
        self.assertEqual(incidents["consolidation_tier"].value_counts().to_dict()["exact"], 1)
        self.assertTrue(crosswalk["source_row_id"].is_unique)

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

    def test_close_reports_are_review_only_candidates(self) -> None:
        source = self.normalize([report(), report(**{"Date/Time": "2025-01-01 12:10:00", "Reason": "A moving train"})])
        incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        candidates = mod.generate_duplicate_candidates(incidents, source, CONFIG["proximity_bands_minutes"])
        self.assertEqual(len(incidents), 2)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates.loc[0, "proximity_band_minutes"], 15)

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

    def test_reconciliation_is_full_row_and_multiplicity_aware(self) -> None:
        authoritative = self.normalize([report(), report(), report(Reason="A moving train")])
        reconciliation = self.normalize([report(), report(Reason="A moving train")])
        summary, discrepancies = mod.reconcile_2025_workbooks(authoritative, reconciliation, CONFIG)
        self.assertEqual(summary["rows_present_only_in_authoritative"], 1)
        self.assertEqual(summary["unique_signatures_with_multiplicity_difference"], 1)
        self.assertEqual(len(discrepancies), 1)

    def test_review_sample_and_ids_are_deterministic_when_source_is_reordered(self) -> None:
        source = self.normalize([report(), report(**{"Date/Time": "2025-01-01 12:10:00", "Reason": "B"}), report(**{"Date/Time": "2025-01-01 12:20:00", "Reason": "C"})])
        first_incidents, _, _ = mod.consolidate_reports(source, CONFIG)
        second_incidents, _, _ = mod.consolidate_reports(source.sample(frac=1, random_state=7), CONFIG)
        self.assertEqual(set(first_incidents["canonical_incident_id"]), set(second_incidents["canonical_incident_id"]))
        first = mod.deterministic_review_sample(mod.generate_duplicate_candidates(first_incidents, source, CONFIG["proximity_bands_minutes"]))
        second = mod.deterministic_review_sample(mod.generate_duplicate_candidates(second_incidents, source, CONFIG["proximity_bands_minutes"]))
        self.assertEqual(first["candidate_pair_id"].tolist(), second["candidate_pair_id"].tolist())

    def test_import_does_not_execute_pipeline(self) -> None:
        self.assertTrue(callable(mod.run_phase_1))
        self.assertEqual(mod.__name__, "incident_deduplication")

    def test_full_pipeline_writes_fingerprinted_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            authoritative_path = root / "authoritative.xlsx"
            reconciliation_path = root / "reconciliation.xlsx"
            inventory_path = root / "inventory.csv"
            config_path = root / "config.json"
            output_dir = root / "output"
            pd.DataFrame([
                report(),
                report(**{"Date/Time": "2025-01-01 12:10:00", "Reason": "A moving train"}),
            ]).to_excel(authoritative_path, index=False)
            pd.DataFrame([report()]).to_excel(reconciliation_path, index=False)
            pd.DataFrame([{
                "Crossing ID": "123456A", "Latitude": 40.7, "Longitude": -74.0,
                "Revision Date": "2025-01-01",
            }]).to_csv(inventory_path, index=False)
            config = dict(CONFIG)
            config["expected_inputs"] = {
                "authoritative_sha256": mod.compute_file_sha256(authoritative_path),
                "reconciliation_sha256": mod.compute_file_sha256(reconciliation_path),
                "inventory_sha256": mod.compute_file_sha256(inventory_path),
            }
            config_path.write_text(json.dumps(config), encoding="utf-8")
            result = mod.run_phase_1(
                authoritative_path, reconciliation_path, inventory_path, config_path, output_dir
            )
            self.assertTrue(result.validations["source_rows_map_once"])
            self.assertEqual(result.summary["candidate_reported_incidents"], 2)
            self.assertTrue((output_dir / "crossing_timezones.parquet").exists())
            manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertIsNotNone(manifest["outputs"]["source_reports_with_ids"]["sha256"])


if __name__ == "__main__":
    unittest.main()
