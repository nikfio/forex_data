# -*- coding: utf-8 -*-
"""
Created on Mon May 18 21:40:00 2026

@author: Antigravity
"""

import os
import sys
import unittest
import zoneinfo
from datetime import (
    datetime,
    timedelta
)
from pathlib import Path
from unittest.mock import patch

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
        if not os.environ.get("TWELVE_DATA_API_KEY"):
            self.skipTest("TWELVE_DATA_API_KEY environment variable not set")
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

        utc_tz = zoneinfo.ZoneInfo("UTC")
        ny_tz = zoneinfo.ZoneInfo("America/New_York")

        max_ts = df["timestamp"].max()
        max_ts_utc = max_ts.replace(tzinfo=utc_tz)
        start_ts_utc = max_ts_utc - interval_window

        expected_rows = 0
        current = max_ts_utc
        while current > start_ts_utc:
            current_ny = current.astimezone(ny_tz)
            wkday = current_ny.weekday()  # Mon=0, ..., Sun=6
            is_wknd = (
                (wkday == 5)  # Saturday
                or (wkday == 4 and current_ny.hour >= 17)  # Friday >= 17:00 NY time
                or (wkday == 6 and current_ny.hour < 17)  # Sunday < 17:00 NY time
            )
            if not is_wknd:
                expected_rows += 1
            current -= timedelta(hours=1)

        self.assertGreaterEqual(
            df.height,
            expected_rows,
            f"DataFrame should have at least {expected_rows} rows (window/timeframe)"
        )

        expected_schema = list(POLARS_DTYPE_DICT.TIME_TF_DTYPE.keys())
        self.assertEqual(set(df.columns), set(expected_schema))

    def test_weekend_data_filtering(self):
        """
        Tests that get_data and get_recent_data correctly filter out weekend data.
        Does not require a live API key as we mock the HTTP request response.
        """
        # Mock data representing various UTC times:
        # We need mock data in June (Summer, DST active in NY: NY is UTC-4)
        # And mock data in December (Winter, DST inactive in NY: NY is UTC-5)
        raw_mock = [
            # --- SUMMER (June 2026, NY is UTC-4) ---
            "2026-06-05 20:59:00",  # Friday active -> Keep
            "2026-06-05 21:01:00",  # Friday weekend -> Filter
            "2026-06-06 12:00:00",  # Saturday -> Filter
            "2026-06-07 20:59:00",  # Sunday weekend -> Filter
            "2026-06-07 21:01:00",  # Sunday active -> Keep
            "2026-06-08 09:00:00",  # Monday -> Keep

            # --- WINTER (December 2026, NY is UTC-5) ---
            "2026-12-04 21:59:00",  # Friday active -> Keep
            "2026-12-04 22:01:00",  # Friday weekend -> Filter
            "2026-12-05 12:00:00",  # Saturday -> Filter
            "2026-12-06 21:59:00",  # Sunday weekend -> Filter
            "2026-12-06 22:01:00",  # Sunday active -> Keep
            "2026-12-07 09:00:00",  # Monday -> Keep
        ]
        mock_values = [
            {
                "datetime": dt,
                "open": "1.0850",
                "high": "1.0850",
                "low": "1.0850",
                "close": "1.0850",
                "volume": "100"
            }
            for dt in raw_mock
        ]

        # Initialize the connector directly (no real API key needed)
        with patch.dict("os.environ", {"TWELVE_DATA_API_KEY": "dummy"}):
            connector = RealTimeDBConnectorTwelveData(plan="free", data_path=_data_path)

        with patch.object(
            RealTimeDBConnectorTwelveData,
            "_execute_request",
            return_value={"values": mock_values}
        ):
            # Test get_data
            lf_data = connector.get_data(
                symbol="EUR/USD",
                timeframe="1min",
                start_date="2026-06-01",
                end_date="2026-12-31"
            )
            df_data = lf_data.collect()

            # The timestamps that should be kept
            expected_timestamps = [
                "2026-06-05 20:59:00",
                "2026-06-07 21:01:00",
                "2026-06-08 09:00:00",
                "2026-12-04 21:59:00",
                "2026-12-06 22:01:00",
                "2026-12-07 09:00:00",
            ]

            actual_timestamps = (
                df_data["timestamp"]
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .to_list()
            )
            self.assertEqual(actual_timestamps, expected_timestamps)

            # Test get_recent_data
            # Max timestamp is Dec 7 2026 09:00:00.
            # Cutoff = Max - 30 days = Nov 7 2026. Only Dec dates remain.
            lf_recent = connector.get_recent_data(
                symbol="EUR/USD",
                timeframe="1min",
                interval_window=timedelta(days=30)
            )
            df_recent = lf_recent.collect()

            expected_recent_timestamps = [
                "2026-12-04 21:59:00",
                "2026-12-06 22:01:00",
                "2026-12-07 09:00:00",
            ]
            actual_recent_timestamps = (
                df_recent["timestamp"]
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .to_list()
            )
            self.assertEqual(actual_recent_timestamps, expected_recent_timestamps)

    def test_get_recent_data_weekend_shifting(self):
        """
        Tests that when get_recent_data is called during a weekend,
        the request parameters are correctly shifted to target the last active
        market period (Friday close).
        """
        # Mock current time as Saturday June 6, 2026 12:00:00 NY time (weekend)
        ny_tz = zoneinfo.ZoneInfo("America/New_York")
        mock_now = datetime(2026, 6, 6, 12, 0, 0, tzinfo=ny_tz)

        class MockDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is not None:
                    return mock_now.astimezone(tz)
                return mock_now

        # Set up a mock response from the API
        mock_values = [
            {
                "datetime": "2026-06-05 20:59:00",
                "open": "1.0850",
                "high": "1.0850",
                "low": "1.0850",
                "close": "1.0850",
                "volume": "100"
            }
        ]

        with patch.dict("os.environ", {"TWELVE_DATA_API_KEY": "dummy"}):
            connector = RealTimeDBConnectorTwelveData(plan="free", data_path=_data_path)

        with patch(
            "forex_data.data_management.remoteconnector.datetime", MockDatetime
        ), patch.object(
            RealTimeDBConnectorTwelveData,
            "_execute_request",
            return_value={"values": mock_values}
        ) as mock_execute:
            connector.get_recent_data(
                symbol="EUR/USD",
                timeframe="1min",
                interval_window=timedelta(days=1)
            )

            # Assert that the API was called with shifted start/end dates.
            # June is summer, so Friday 17:00 NY time is Friday 21:00 UTC.
            # start_date = 21:00 UTC - 1 day = June 4, 2026 21:00 UTC.
            called_params = mock_execute.call_args[0][1]
            self.assertEqual(called_params["end_date"], "2026-06-05 21:00:00")
            self.assertEqual(called_params["start_date"], "2026-06-04 21:00:00")


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
