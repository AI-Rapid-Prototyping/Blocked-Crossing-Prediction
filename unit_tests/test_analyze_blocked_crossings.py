"""
FILE: test_analyze_blocked_crossings.py
PURPOSE: Tests our main analysis script using fake sample data so we don't 
         have to mess with real files. Makes sure dates and text clean up properly.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import analyze_blocked_crossings as mod


class PreprocessBlockedCrossingsTests(unittest.TestCase):
    def test_preprocess_blocked_crossings_reads_xlsx_and_preserves_datetime_dtype(self) -> None:
        # Build a minimal blocked-crossings sample that matches the current preprocess contract.
        sample = pd.DataFrame(
            {
                "Crossing ID": [" 1001 ", "1002"],
                "Date/Time": pd.to_datetime(["2025-01-01 12:34:56", "2025-01-02 00:00:00"]),
                "Reason": ["A", "B"],
                "Duration": ["10", "20"],
                "State": ["TX", "CA"],
            }
        )

        xlsx_path = Path("blocked_crossings.xlsx")
        csv_path = Path("blocked_crossings.csv")

        with patch.object(mod.pd, "read_excel", return_value=sample.copy()), patch.object(
            mod.pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None
        ):
            result = mod.preprocess_blocked_crossings(xlsx_path, csv_path)

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["Date/Time"]))


class PreprocessGxapsTests(unittest.TestCase):
    def test_merge_gxaps_headers_combines_major_and_subheaders(self) -> None:
        # Build the exact major and subheader rows used by the GXAPS annual report.
        major_headers = pd.Series(
            ["", "", "", "", "", "", "", "", "Yearly Accident Count", None, None, None, None, None, None, None, None, None, None, None, None, None]
        )
        sub_headers = pd.Series(
            [
                "Predicted Accident Rank",
                "Annual Average Predicted Accidents",
                "Crossing ID",
                "RR Code",
                "State",
                "County",
                "City",
                "Street",
                25,
                24,
                23,
                22,
                21,
                "Date Chg",
                "Warning Device",
                "Total Trns",
                "Total Trks",
                "Timetable Speed",
                "Hwy Paved",
                "Hwy Lanes",
                "AADT",
                None,
            ]
        )

        columns, keep_mask = mod.merge_gxaps_headers(major_headers, sub_headers)

        # Verify the normalized output names and the dropped blank trailing column.
        self.assertEqual(
            columns,
            [
                "Predicted Accident Rank",
                "Annual Average Predicted Accidents",
                "Crossing ID",
                "RR Code",
                "State",
                "County",
                "City",
                "Street",
                "Yearly Accident Count 25",
                "Yearly Accident Count 24",
                "Yearly Accident Count 23",
                "Yearly Accident Count 22",
                "Yearly Accident Count 21",
                "Date Chg",
                "Warning Device",
                "Total Trns",
                "Total Trks",
                "Timetable Speed",
                "Hwy Paved",
                "Hwy Lanes",
                "AADT",
            ],
        )
        self.assertEqual(
            keep_mask,
            [
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                False,
            ],
        )

    def test_preprocess_gxaps_combines_workbooks_into_aps(self) -> None:
        # Create fake GXAPS workbook names so the loader can discover them with glob().
        gxaps_dir = Path("ignored")
        first_path = Path("GXAPS_20240101_alpha.xlsx")
        second_path = Path("GXAPS_20240102_beta.xlsx")

        # Return workbook-specific frames so we can verify row stacking and key cleanup.
        workbook_frames = {
            first_path.name: pd.DataFrame(
                {
                    "Crossing ID": [" 1001 ", "1002"],
                    "State": ["TX", "CA"],
                }
            ),
            second_path.name: pd.DataFrame(
                {
                    "Crossing ID": ["1003"],
                    "State": ["WA"],
                }
            ),
        }

        def fake_read_excel(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
            return workbook_frames[Path(path).name].copy()

        with patch.object(mod.Path, "glob", return_value=[first_path, second_path]), patch.object(
            mod.pd, "read_excel", side_effect=fake_read_excel
        ), patch.object(mod.Path, "mkdir", return_value=None), patch.object(
            mod.pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None
        ):
            aps = mod.preprocess_gxaps(gxaps_dir)

        self.assertEqual(list(aps["Crossing ID"]), ["1001", "1002", "1003"])
        self.assertEqual(list(aps["State"]), ["TX", "CA", "WA"])

    def test_preprocess_gxaps_drops_total_rows_with_blank_crossing_ids(self) -> None:
        # Include a TTL summary row to confirm it is removed before key validation.
        gxaps_dir = Path("ignored")
        workbook_path = Path("GXAPS_20240101_alpha.xlsx")

        workbook_frame = pd.DataFrame(
            {
                "Crossing ID": ["1001", "", "1002"],
                "State": ["TX", "", "CA"],
                "Predicted Accident Rank": [1, "TTL:", 2],
            }
        )

        def fake_read_excel(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
            return workbook_frame.copy()

        with patch.object(mod.Path, "glob", return_value=[workbook_path]), patch.object(
            mod.pd, "read_excel", side_effect=fake_read_excel
        ), patch.object(mod.Path, "mkdir", return_value=None), patch.object(
            mod.pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None
        ):
            aps = mod.preprocess_gxaps(gxaps_dir)

        self.assertEqual(list(aps["Crossing ID"]), ["1001", "1002"])
        self.assertNotIn("TTL:", aps["Predicted Accident Rank"].astype("string").tolist())

    def test_preprocess_gxaps_exports_and_discards_duplicate_crossing_ids(self) -> None:
        # Reuse a single crossing ID across two workbooks to simulate a primary-key collision.
        gxaps_dir = Path("ignored")
        first_path = Path("GXAPS_20240101_alpha.xlsx")
        second_path = Path("GXAPS_20240102_beta.xlsx")

        workbook_frames = {
            first_path.name: pd.DataFrame({"Crossing ID": ["1001"], "State": ["TX"]}),
            second_path.name: pd.DataFrame({"Crossing ID": [" 1001 "], "State": ["CA"]}),
        }
        written_frames: list[pd.DataFrame] = []
        written_paths: list[Path] = []

        def fake_read_excel(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
            return workbook_frames[Path(path).name].copy()

        def capture_to_csv(self: pd.DataFrame, path: Path, *args: object, **kwargs: object) -> None:
            written_frames.append(self.copy())
            written_paths.append(Path(path))

        with patch.object(mod.Path, "glob", return_value=[first_path, second_path]), patch.object(
            mod.pd, "read_excel", side_effect=fake_read_excel
        ), patch.object(mod.Path, "mkdir", return_value=None), patch.object(
            mod.pd.DataFrame, "to_csv", capture_to_csv
        ):
            aps = mod.preprocess_gxaps(gxaps_dir)

        self.assertEqual(list(aps["Crossing ID"]), [])
        self.assertEqual(len(written_frames), 1)
        self.assertEqual(written_paths[0], mod.DEFAULT_OUTPUT / "discarded_gxaps_records.csv")
        self.assertEqual(list(written_frames[0]["Crossing ID"]), ["1001", "1001"])
        self.assertEqual(list(written_frames[0]["State"]), ["TX", "CA"])

    def test_preprocess_gxaps_requires_crossing_id_column(self) -> None:
        # Simulate a workbook that loads successfully but does not expose the required key column.
        gxaps_dir = Path("ignored")
        gxaps_path = Path("GXAPS_20240101_alpha.xlsx")

        def fake_read_excel(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
            return pd.DataFrame({"State": ["TX"]})

        with patch.object(mod.Path, "glob", return_value=[gxaps_path]), patch.object(
            mod.pd, "read_excel", side_effect=fake_read_excel
        ), patch.object(mod.Path, "mkdir", return_value=None), patch.object(
            mod.pd.DataFrame, "to_csv", lambda self, *args, **kwargs: None
        ):
            with self.assertRaises(KeyError):
                mod.preprocess_gxaps(gxaps_dir)


if __name__ == "__main__":
    unittest.main()
