# -*- coding: utf-8 -*-
"""Tests for incremental database recognition and overlap margin."""

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from forex_data import (
    COLUMN_NAME,
    HistoricalManagerDB,
    POLARS_DTYPE_DICT,
)
from forex_data.data_management import (
    DukascopyConnector,
    LocalDBYearConnector,
)


class TestIncrementalDownload(unittest.TestCase):
    """Verify incremental write deduplication and download boundary logic."""

    temp_dir: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.root_path = Path(cls.temp_dir.name)
        cls.sample_tick_df = pl.DataFrame({
            COLUMN_NAME.TIMESTAMP: [
                datetime(2026, 10, 19, 20, 0, 0),
                datetime(2026, 10, 19, 21, 0, 0),
                datetime(2026, 10, 19, 21, 59, 35),
            ],
            COLUMN_NAME.ASK: [1.0850, 1.0852, 1.0855],
            COLUMN_NAME.BID: [1.0848, 1.0850, 1.0853],
            COLUMN_NAME.ASK_VOLUME: [100.0, 120.0, 150.0],
            COLUMN_NAME.BID_VOLUME: [100.0, 120.0, 150.0],
            COLUMN_NAME.VWMP: [1.0849, 1.0851, 1.0854],
        }).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

        cls.overlapping_tick_df = pl.DataFrame({
            COLUMN_NAME.TIMESTAMP: [
                datetime(2026, 10, 19, 21, 0, 0),
                datetime(2026, 10, 19, 21, 59, 35),
                datetime(2026, 10, 20, 10, 0, 0),
                datetime(2026, 10, 22, 21, 0, 0),
            ],
            COLUMN_NAME.ASK: [1.0852, 1.0855, 1.0860, 1.0870],
            COLUMN_NAME.BID: [1.0850, 1.0853, 1.0858, 1.0868],
            COLUMN_NAME.ASK_VOLUME: [120.0, 150.0, 200.0, 250.0],
            COLUMN_NAME.BID_VOLUME: [120.0, 150.0, 200.0, 250.0],
            COLUMN_NAME.VWMP: [1.0851, 1.0854, 1.0859, 1.0869],
        }).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def test_read_last_timestamp_case_insensitive_and_year(self) -> None:
        """Test read_last_timestamp with year parameter and case-insensitivity."""
        db_dir = self.root_path / "case_insensitive_db"
        connector = LocalDBYearConnector(
            data_path=str(db_dir), data_type="parquet"
        )

        self.assertIsNone(
            connector.read_last_timestamp("forex", "EURUSD", "tick")
        )
        self.assertIsNone(
            connector.read_last_timestamp("forex", "eurusd", "tick", year=2026)
        )

        key = connector._db_key("forex", "eurusd", "tick", 2026)
        connector.write_data(key, self.sample_tick_df)

        expected = datetime(2026, 10, 19, 21, 59, 35)
        self.assertEqual(
            connector.read_last_timestamp("forex", "eurusd", "tick"), expected
        )
        self.assertEqual(
            connector.read_last_timestamp("forex", "EURUSD", "tick"), expected
        )
        self.assertEqual(
            connector.read_last_timestamp("forex", "EURUSD", "tick", year=2026),
            expected,
        )
        self.assertIsNone(
            connector.read_last_timestamp("forex", "EURUSD", "tick", year=2025)
        )

    def test_incremental_write_deduplication(self) -> None:
        """Test writing overlapping data merges and deduplicates timestamps."""
        db_dir = self.root_path / "dedup_db"
        connector = LocalDBYearConnector(
            data_path=str(db_dir), data_type="parquet"
        )
        key = connector._db_key("forex", "eurusd", "tick", 2026)

        connector.write_data(key, self.sample_tick_df)
        first_read = connector.read_data(
            "forex",
            "eurusd",
            "tick",
            datetime(2026, 10, 1),
            datetime(2026, 10, 31),
        ).collect()
        self.assertEqual(len(first_read), 3)

        connector.write_data(key, self.overlapping_tick_df)
        second_read = connector.read_data(
            "forex",
            "eurusd",
            "tick",
            datetime(2026, 10, 1),
            datetime(2026, 10, 31),
        ).collect()
        self.assertEqual(len(second_read), 5)
        self.assertTrue(second_read[COLUMN_NAME.TIMESTAMP].is_sorted())

        connector._last_timestamp_cache = {}
        updated_ts = connector.read_last_timestamp(
            "forex", "EURUSD", "tick", year=2026
        )
        self.assertEqual(updated_ts, datetime(2026, 10, 22, 21, 0, 0))

    def test_dukascopy_download_month_raw_empty_on_invalid_bounds(self) -> None:
        """Test download_month_raw returns empty LazyFrame when start >= end."""
        db_dir = self.root_path / "invalid_bounds_duka"
        connector = DukascopyConnector(data_path=str(db_dir))
        try:
            with patch.object(
                DukascopyConnector,
                "get_available_tickers",
                return_value=["EURUSD"],
            ):
                result = connector.download_month_raw(
                    ticker="EURUSD",
                    year=2026,
                    month_num=10,
                    start_date=datetime(2026, 10, 20, 12, 0, 0),
                    end_date=datetime(2026, 10, 20, 10, 0, 0),
                    engine="polars_lazy",
                )
            self.assertIsInstance(result, pl.LazyFrame)
            collected = result.collect()
            self.assertEqual(collected.height, 0)
        finally:
            connector.clear_temporary_folder()

    def test_download_year_overlap_boundary_calculation(self) -> None:
        """Test _download_year calculates start from last_db_ts minus overlap."""
        db_dir = self.root_path / "overlap_margin_db"
        mock_last_ts = datetime(2026, 10, 19, 21, 59, 35)
        captured = {}

        def mock_download_month(
            t, y, m, engine="polars_lazy", start_date=None, end_date=None, **kwargs
        ):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return pl.DataFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE).lazy()

        with patch.object(
            DukascopyConnector,
            "get_available_tickers",
            return_value=["EURUSD"],
        ):
            manager = HistoricalManagerDB(
                data_path=str(db_dir),
                download_overlap_margin="2h",
                volume_data=True,
            )
        try:
            with patch.object(
                LocalDBYearConnector,
                "read_last_timestamp",
                return_value=mock_last_ts,
            ):
                with patch.object(
                    DukascopyConnector,
                    "download_month_raw",
                    side_effect=mock_download_month,
                ):
                    with patch.object(
                        DukascopyConnector,
                        "get_available_tickers",
                        return_value=["EURUSD"],
                    ):
                        manager._download_year(
                            "EURUSD", 2026, start_month=10, end_month=10
                        )

            self.assertIn("start_date", captured)
            self.assertEqual(
                captured["start_date"], datetime(2026, 10, 19, 19, 0, 0)
            )
            self.assertIsNotNone(captured["end_date"])
            self.assertLessEqual(
                captured["end_date"], datetime.now() + timedelta(minutes=1)
            )
        finally:
            manager.close()
