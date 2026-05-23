# -*- coding: utf-8 -*-
"""
Created on Mon May 18 21:40:00 2026

@author: Antigravity
"""

import sys
import unittest
from pathlib import Path
from datetime import timedelta

import polars as pl
from loguru import logger

from forex_data import (
    RealTimeDBConnectorTwelveData,
    POLARS_DTYPE_DICT
)

_base_path = Path.home() / ".test_database"
_data_path = _base_path
_counter = 1
while _data_path.exists():
    _data_path = Path.home() / f".test_database_{_counter}"
    _counter += 1

_data_path = Path.home() / ".test_database" / "Realtime"


class TestRealTimeDBConnectorTwelveData(unittest.TestCase):
    """
    Direct live tests for RealTimeDBConnectorTwelveData.
    Verifies that the main interface methods (get_realtime_price, get_data,
    get_recent_data) return valid, non-empty LazyFrames.
    """

    def setUp(self):
        logger.remove()
        logger.add(
            sys.stdout,
            level="INFO",
            format=(
                "<green>{time:HH:mm:ss}</green> | "
                "<level>{level:<8}</level> | {message}"
            )
        )
        self.connector = RealTimeDBConnectorTwelveData(
            plan="free",
            data_path=_data_path
        )

    def tearDown(self):
        self.connector.clear_temporary_folder()

    def test_get_realtime_price(self):
        """
        Tests get_realtime_price behavior.
        Asserts that it returns a valid, non-empty LazyFrame.
        """
        symbol = "EUR/USD"
        logger.info(f"Fetching live real-time price for {symbol}...")
        result_lf = self.connector.get_realtime_price(symbol=symbol)
        self.assertIsInstance(result_lf, pl.LazyFrame)

        df = result_lf.collect()
        logger.info(f"Received DataFrame:\n{df}")
        self.assertGreater(df.height, 0, "DataFrame should not be empty")
        self.assertIn("timestamp", df.columns)
        self.assertIn("ticker", df.columns)
        self.assertIn("price", df.columns)

    def test_get_data_success(self):
        """
        Tests get_data behavior with a valid start date (January 2026).
        Asserts that it returns a valid, non-empty LazyFrame.
        """
        symbol = "EUR/USD"
        start_date = "2026-01-05 10:00:00"
        end_date = "2026-01-05 10:30:00"
        timeframe = "5min"

        logger.info(
            f"Fetching live historical data for {symbol} starting {start_date}..."
        )
        result_lf = self.connector.get_data(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date
        )
        self.assertIsInstance(result_lf, pl.LazyFrame)

        df = result_lf.collect()
        logger.info(f"Received DataFrame:\n{df}")
        self.assertGreater(df.height, 0, "DataFrame should not be empty")

        # Verify columns match POLARS_DTYPE_DICT.TIME_TF_DTYPE
        expected_schema = list(POLARS_DTYPE_DICT.TIME_TF_DTYPE.keys())
        self.assertEqual(set(df.columns), set(expected_schema))

    def test_get_data_invalid_start_date(self):
        """
        Tests that get_data locally raises a ValueError when the start_date is
        before January 2026.
        This local check does not require a real API key.
        """
        logger.info(
            "Testing local validation check for get_data with start date "
            "before January 2026..."
        )
        invalid_dates = [
            "2025-12-31 23:59:59",
            "2020-01-01 00:00:00",
            "2025-06-15"
        ]

        for invalid_date in invalid_dates:
            with self.assertRaises(ValueError) as ctx:
                self.connector.get_data(
                    symbol="EUR/USD",
                    timeframe="1h",
                    start_date=invalid_date,
                    end_date="2026-01-02"
                )
            self.assertIn("cannot be before January 2026", str(ctx.exception))
            logger.info(
                f"Correctly caught local ValueError for invalid start_date "
                f"'{invalid_date}': {ctx.exception}"
            )

    def test_get_recent_data(self):
        """
        Tests get_recent_data behavior.
        Asserts that it returns a valid, non-empty LazyFrame.
        """
        symbol = "EUR/USD"
        timeframe = "1h"
        interval_window = timedelta(days=1)

        logger.info(
            f"Fetching live recent data for {symbol} within {interval_window} "
            f"window..."
        )
        result_lf = self.connector.get_recent_data(
            symbol=symbol,
            timeframe=timeframe,
            interval_window=interval_window
        )
        self.assertIsInstance(result_lf, pl.LazyFrame)

        df = result_lf.collect()
        logger.info(f"Received DataFrame:\n{df}")
        self.assertGreater(df.height, 0, "DataFrame should not be empty")

        # Assert rows length is at least window/timeframe
        # (24 rows for 1 day window with 1h timeframe)
        expected_rows = int(interval_window / timedelta(hours=1))
        self.assertGreaterEqual(
            df.height,
            expected_rows,
            f"DataFrame should have at least {expected_rows} rows (window/timeframe)"
        )

        expected_schema = list(POLARS_DTYPE_DICT.TIME_TF_DTYPE.keys())
        self.assertEqual(set(df.columns), set(expected_schema))


def main():
    print("=" * 70)
    print("  Twelve Data Real-Time Database Connector — Live Test Suite Runner")
    print("=" * 70)

    suite = unittest.TestLoader().loadTestsFromTestCase(
        TestRealTimeDBConnectorTwelveData
    )
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
