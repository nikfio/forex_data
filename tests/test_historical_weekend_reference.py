# -*- coding: utf-8 -*-
"""Unit tests for NY market-close weekend reference handling in HistoricalManagerDB."""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from forex_data.data_management.historicaldata import (
    HistoricalManagerDB,
    _get_market_reference_utc,
)


class TestHistoricalWeekendReference(unittest.TestCase):
    """Tests for NY 17:00 weekend clamping in HistoricalManagerDB freshness checks."""

    temp_dir: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def test_market_reference_utc_est_and_edt(self) -> None:
        # Winter EST (UTC-5): Friday close is 22:00 UTC
        self.assertEqual(
            _get_market_reference_utc(datetime(2026, 11, 13, 21, 59, 0)),
            datetime(2026, 11, 13, 21, 59, 0),
        )
        expected_nov_close = datetime(2026, 11, 13, 22, 0, 0)
        for dt in (
            datetime(2026, 11, 13, 22, 30, 0),
            datetime(2026, 11, 14, 0, 30, 0),
            datetime(2026, 11, 14, 14, 0, 0),
            datetime(2026, 11, 15, 0, 30, 0),
            datetime(2026, 11, 15, 20, 57, 0),
            datetime(2026, 11, 15, 22, 27, 0),
        ):
            self.assertEqual(_get_market_reference_utc(dt), expected_nov_close)

        # Summer EDT (UTC-4): Friday close is 21:00 UTC
        self.assertEqual(
            _get_market_reference_utc(datetime(2026, 7, 10, 20, 59, 0)),
            datetime(2026, 7, 10, 20, 59, 0),
        )
        expected_jul_close = datetime(2026, 7, 10, 21, 0, 0)
        for dt in (
            datetime(2026, 7, 10, 21, 30, 0),
            datetime(2026, 7, 11, 0, 30, 0),
            datetime(2026, 7, 12, 21, 30, 0),
        ):
            self.assertEqual(_get_market_reference_utc(dt), expected_jul_close)

        # Monday 00:30 UTC is open and never clamped back to Friday
        self.assertEqual(
            _get_market_reference_utc(datetime(2026, 11, 16, 0, 30, 0)),
            datetime(2026, 11, 16, 0, 30, 0),
        )

    def test_is_current_year_update_required_catches_friday_evening_gap(self) -> None:
        with patch.object(
            HistoricalManagerDB,
            "__attrs_post_init__",
            lambda self, **kw: None,
        ):
            manager = HistoricalManagerDB()
        self.assertEqual(manager.max_discrepancy_with_now, "2h")
        manager.data_path = Path(self.temp_dir.name)
        manager.offline = False

        fri_incomplete = datetime(2026, 11, 13, 15, 59, 59, 542000)
        fri_complete = datetime(2026, 11, 13, 21, 59, 59, 0)

        for now_ts in (
            datetime(2026, 11, 14, 1, 29, 0),
            datetime(2026, 11, 15, 20, 57, 0),
            datetime(2026, 11, 15, 22, 27, 0),
        ):
            self.assertTrue(
                manager._is_current_year_update_required(
                    ticker_available_last_timestamp=fri_incomplete,
                    end=now_ts,
                    now_utc=now_ts,
                )
            )
            self.assertFalse(
                manager._is_current_year_update_required(
                    ticker_available_last_timestamp=fri_complete,
                    end=now_ts,
                    now_utc=now_ts,
                )
            )


if __name__ == "__main__":
    unittest.main()
