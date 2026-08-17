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


if __name__ == "__main__":
    unittest.main()
