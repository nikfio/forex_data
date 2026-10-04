# -*- coding: utf-8 -*-
"""
Created on Sat Oct 03 16:30:00 2026

@author: Antigravity
"""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
from loguru import logger

from forex_data import (
    TiingoConnector,
    POLARS_DTYPE_DICT,
    COLUMN_NAME
)

_base_path = Path.home() / ".test_database"
_data_path = _base_path / "Realtime_Tiingo"


class TestTiingoConnector(unittest.TestCase):
    """
    Tests for TiingoConnector (Route B real-time data provider).
    Verifies symbol formatting, top-of-book retrieval, price & candle fetching,
    schema adherence, and weekend filtering.
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
        self.connector = TiingoConnector(
            api_key="test_api_key",
            plan="free",
            data_path=_data_path
        )

    def tearDown(self):
        self.connector.clear_temporary_folder()

    def test_format_symbol(self):
        """Tests that symbol strings are properly normalized to Tiingo fx tickers."""
        self.assertEqual(self.connector._format_symbol("EUR/USD"), "eurusd")
        self.assertEqual(self.connector._format_symbol("GBP_USD"), "gbpusd")
        self.assertEqual(self.connector._format_symbol("USD-JPY"), "usdjpy")
        self.assertEqual(self.connector._format_symbol("EURUSD"), "eurusd")

    def test_format_timeframe(self):
        """Tests timeframe mapping to Tiingo resampleFreq strings."""
        self.assertEqual(self.connector._format_timeframe("1m"), "1min")
        self.assertEqual(self.connector._format_timeframe("5m"), "5min")
        self.assertEqual(self.connector._format_timeframe("15m"), "15min")
        self.assertEqual(self.connector._format_timeframe("1h"), "1hour")
        self.assertEqual(self.connector._format_timeframe("4h"), "4hour")
        self.assertEqual(self.connector._format_timeframe("1d"), "1day")

    def test_get_top_of_book(self):
        """Tests fetching top of book quoting structure."""
        mock_response = [
            {
                "ticker": "eurusd",
                "quoteTimestamp": "2026-10-02T16:00:00.000Z",
                "bidPrice": 1.0850,
                "bidSize": 1500000.0,
                "askPrice": 1.08515,
                "askSize": 2000000.0,
                "midPrice": 1.085075
            }
        ]
        with patch.object(
            self.connector, "_execute_request", return_value=mock_response
        ):
            tob = self.connector.get_top_of_book("EUR/USD")
            self.assertEqual(tob["ticker"], "eurusd")
            self.assertEqual(tob["bidPrice"], 1.0850)
            self.assertEqual(tob["askPrice"], 1.08515)
            self.assertEqual(tob["bidSize"], 1500000.0)
            self.assertEqual(tob["askSize"], 2000000.0)

    def test_get_realtime_price(self):
        """Tests get_realtime_price produces LazyFrame with bid/ask prices and sizes."""
        mock_tob = {
            "ticker": "eurusd",
            "quoteTimestamp": "2026-10-02T16:00:00.000Z",
            "bidPrice": 1.0850,
            "bidSize": 1500000.0,
            "askPrice": 1.08515,
            "askSize": 2000000.0,
            "midPrice": 1.085075
        }
        with patch.object(self.connector, "get_top_of_book", return_value=mock_tob):
            result_lf = self.connector.get_realtime_price("EUR/USD")
            self.assertIsInstance(result_lf, pl.LazyFrame)
            df = result_lf.collect()
            self.assertEqual(df.height, 1)
            self.assertIn("timestamp", df.columns)
            self.assertIn("ticker", df.columns)
            self.assertIn("price", df.columns)
            self.assertIn("bid", df.columns)
            self.assertIn("ask", df.columns)
            self.assertIn("bid_volume", df.columns)
            self.assertIn("ask_volume", df.columns)
            self.assertEqual(df["ticker"][0], "EUR/USD")
            self.assertAlmostEqual(df["price"][0], 1.085075)
            self.assertAlmostEqual(df["bid_volume"][0], 1500000.0)
            self.assertAlmostEqual(df["ask_volume"][0], 2000000.0)

    def test_get_data_and_schema(self):
        """
        Tests get_data converts raw Tiingo candle records into the canonical
        POLARS_DTYPE_DICT.TIME_TF_DTYPE schema with derived bid/ask and ask/bid volume.
        """
        mock_candles = [
            {
                "date": "2026-10-02T14:00:00.000Z",
                "open": 1.0850,
                "high": 1.0860,
                "low": 1.0845,
                "close": 1.0855
            },
            {
                "date": "2026-10-02T14:05:00.000Z",
                "open": 1.0855,
                "high": 1.0865,
                "low": 1.0850,
                "close": 1.0860
            }
        ]
        mock_tob = {
            "ticker": "eurusd",
            "bidPrice": 1.0850,
            "askPrice": 1.0852,
            "bidSize": 1500000.0,
            "askSize": 1500000.0
        }

        with patch.object(
            self.connector, "_execute_request", return_value=mock_candles
        ):
            with patch.object(self.connector, "get_top_of_book", return_value=mock_tob):
                result_lf = self.connector.get_data(
                    symbol="EUR/USD",
                    timeframe="5m",
                    start_date="2026-10-02",
                    end_date="2026-10-02"
                )
                self.assertIsInstance(result_lf, pl.LazyFrame)
                df = result_lf.collect()
                self.assertEqual(df.height, 2)

                # Schema validation
                expected_schema = list(POLARS_DTYPE_DICT.TIME_TF_DTYPE.keys())
                self.assertEqual(list(df.columns), expected_schema)
                for col_name, expected_dtype in (
                    POLARS_DTYPE_DICT.TIME_TF_DTYPE.items()
                ):
                    self.assertEqual(
                        df.schema[col_name],
                        expected_dtype,
                        f"Dtype mismatch on {col_name}"
                    )

                # Check ask/bid values (close +/- half_spread)
                # spread = 0.0002 -> half_spread = 0.0001
                self.assertAlmostEqual(
                    df[COLUMN_NAME.ASK][0], 1.0855 + 0.0001, places=5
                )
                self.assertAlmostEqual(
                    df[COLUMN_NAME.BID][0], 1.0855 - 0.0001, places=5
                )
                self.assertAlmostEqual(df[COLUMN_NAME.ASK_VOLUME][0], 1500000.0)
                self.assertAlmostEqual(df[COLUMN_NAME.BID_VOLUME][0], 1500000.0)

    def test_weekend_filtering(self):
        """Tests that weekend candles (Fri 17:00 NY to Sun 17:00 NY) are filtered."""
        # June 2026: NY is EDT (UTC-4)
        mock_candles = [
            # Fri 16:55 NY -> keep
            {"date": "2026-06-05T20:55:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
            # Fri 17:05 NY -> weekend
            {"date": "2026-06-05T21:05:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
            # Sat -> weekend
            {"date": "2026-06-06T12:00:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
            # Sun 16:55 NY -> weekend
            {"date": "2026-06-07T20:55:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
            # Sun 17:05 NY -> keep
            {"date": "2026-06-07T21:05:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
            # Mon -> keep
            {"date": "2026-06-08T09:00:00.000Z", "open": 1.08,
             "high": 1.08, "low": 1.08, "close": 1.08},
        ]

        with patch.object(
            self.connector, "_execute_request", return_value=mock_candles
        ):
            with patch.object(self.connector, "get_top_of_book", return_value={}):
                df = self.connector.get_data(
                    "EUR/USD", "5m", "2026-06-05", "2026-06-08"
                ).collect()
                self.assertEqual(df.height, 3)
                timestamps = df["timestamp"].to_list()
                self.assertEqual(timestamps[0], datetime(2026, 6, 5, 20, 55))
                self.assertEqual(timestamps[1], datetime(2026, 6, 7, 21, 5))
                self.assertEqual(timestamps[2], datetime(2026, 6, 8, 9, 0))

    def test_live_if_api_key_set(self):
        """Optional live test if TIINGO_API_KEY is available in the environment."""
        if not os.environ.get("TIINGO_API_KEY"):
            self.skipTest("TIINGO_API_KEY environment variable not set")

        live_connector = TiingoConnector(plan="free", data_path=_data_path)
        result_lf = live_connector.get_realtime_price("EUR/USD")
        df = result_lf.collect()
        self.assertGreater(df.height, 0)
        logger.info(f"Live Tiingo Top-of-Book:\n{df}")


if __name__ == "__main__":
    unittest.main()
