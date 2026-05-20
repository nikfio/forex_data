# -*- coding: utf-8 -*-
"""
Created on Thu May 21 00:30:00 2026

@author: Antigravity
"""

import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import polars as pl
import pandas as pd

from forex_data import DukascopyConnector
from forex_data.data_management.common import POLARS_DTYPE_DICT


class TestDukascopyConnector(unittest.TestCase):
    """
    Unit tests for DukascopyConnector.
    Uses mocks to test the logic of connection checking, scraper,
    data conversion, volume mapping, and recent data reframing.
    """

    def setUp(self):
        self.connector = DukascopyConnector(
            data_path="./dukascopy_test_path",
            ssl_verify=False
        )

    @patch('requests.Session.head')
    def test_check_connection_success(self, mock_head):
        # Arrange
        mock_head.return_value = MagicMock(status_code=200)

        # Act
        connected = self.connector.check_connection()

        # Assert
        self.assertTrue(connected)
        mock_head.assert_called_once_with("https://datafeed.dukascopy.com/datafeed/", timeout=5)

    @patch('requests.Session.head')
    def test_check_connection_failure(self, mock_head):
        # Arrange
        mock_head.side_effect = Exception("Connection error")

        # Act
        connected = self.connector.check_connection()

        # Assert
        self.assertFalse(connected)

    @patch('requests.Session.get')
    def test_get_available_tickers(self, mock_get):
        # Arrange
        html_content = """
        <html>
            <body>
                <a href="/swiss/english/fx-market-tools/charts/eur-usd/">EUR/USD Chart</a>
                <a href="/swiss/english/fx-market-tools/charts/gbp-usd/">GBP/USD Chart</a>
                <a href="/swiss/english/fx-market-tools/charts/usd-jpy/">USD/JPY Chart</a>
                <a href="/swiss/english/fx-market-tools/charts/xau-usd/">Gold Chart</a>
                <a href="/other/page/">Other link</a>
            </body>
        </html>
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = html_content.encode('utf-8')
        mock_get.return_value = mock_response

        # Force clear cache
        self.connector._tickers_cache = []

        # Act
        tickers = self.connector.get_available_tickers()

        # Assert
        self.assertIn("EURUSD", tickers)
        self.assertIn("GBPUSD", tickers)
        self.assertIn("USDJPY", tickers)
        self.assertIn("XAUUSD", tickers)
        mock_get.assert_called_once_with(
            "https://www.dukascopy.com/swiss/english/fx-market-tools/historical-data/",
            timeout=5
        )

    @patch('tick_vault.download_range')
    @patch('tick_vault.read_tick_data')
    def test_download_month_raw_polars(self, mock_read, mock_download):
        # Arrange
        mock_download.return_value = None

        # Sample tick data
        times = [datetime(2025, 5, 1, 10, 0, i) for i in range(5)]
        mock_df = pd.DataFrame({
            "time": times,
            "ask": [1.0801, 1.0802, 1.0803, 1.0804, 1.0805],
            "bid": [1.0800, 1.0801, 1.0802, 1.0803, 1.0804],
            "ask_volume": [1.5, 2.0, 1.0, 3.0, 2.5],
            "bid_volume": [2.5, 1.0, 2.0, 1.0, 1.5]
        })
        mock_read.return_value = mock_df

        self.connector._tickers_cache = ["EURUSD"]

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=2025,
            month_num=5,
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)
        self.assertListEqual(
            list(result.columns),
            list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())
        )

        # Check volume mapping: vol = ask_volume + bid_volume
        # First row: ask_vol=1.5, bid_vol=2.5 -> vol=4.0
        self.assertEqual(result["vol"][0], 4.0)

        # Check mid price mapping: p = (ask + bid) / 2
        # First row: ask=1.0801, bid=1.0800 -> p=1.08005
        self.assertAlmostEqual(result["p"][0], 1.08005, places=5)

    @patch('tick_vault.download_range')
    @patch('tick_vault.read_tick_data')
    def test_download_month_raw_polars_lazy(self, mock_read, mock_download):
        # Arrange
        mock_download.return_value = None
        times = [datetime(2025, 5, 1, 10, 0, i) for i in range(5)]
        mock_df = pd.DataFrame({
            "time": times,
            "ask": [1.0801] * 5,
            "bid": [1.0800] * 5,
            "ask_volume": [1.0] * 5,
            "bid_volume": [1.0] * 5
        })
        mock_read.return_value = mock_df
        self.connector._tickers_cache = ["EURUSD"]

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=2025,
            month_num=5,
            engine="polars_lazy"
        )

        # Assert
        self.assertIsInstance(result, pl.LazyFrame)
        collected = result.collect()
        self.assertEqual(collected.height, 5)

    @patch('tick_vault.download_range')
    @patch('tick_vault.read_tick_data')
    def test_get_recent_data_tick(self, mock_read, mock_download):
        # Arrange
        mock_download.return_value = None
        times = [datetime.now() - timedelta(minutes=5 - i) for i in range(5)]
        mock_df = pd.DataFrame({
            "time": times,
            "ask": [1.0801] * 5,
            "bid": [1.0800] * 5,
            "ask_volume": [1.0] * 5,
            "bid_volume": [1.0] * 5
        })
        mock_read.return_value = mock_df
        self.connector._tickers_cache = ["EURUSD"]

        # Act
        result = self.connector.get_recent_data(
            symbol="EURUSD",
            timeframe="TICK",
            interval_window=timedelta(hours=1),
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)
        self.assertIn("timestamp", result.columns)

    @patch('tick_vault.download_range')
    @patch('tick_vault.read_tick_data')
    def test_get_recent_data_reframed(self, mock_read, mock_download):
        # Arrange
        mock_download.return_value = None
        # Generate 120 ticks, one every second
        base_time = datetime.now() - timedelta(minutes=5)
        times = [base_time + timedelta(seconds=i) for i in range(120)]
        mock_df = pd.DataFrame({
            "time": times,
            "ask": [1.0800 + 0.0001 * i for i in range(120)],
            "bid": [1.0800 + 0.0001 * i for i in range(120)],
            "ask_volume": [1.0] * 120,
            "bid_volume": [1.0] * 120
        })
        mock_read.return_value = mock_df
        self.connector._tickers_cache = ["EURUSD"]

        # Act
        # Reframe to 1m (1 minute)
        result = self.connector.get_recent_data(
            symbol="EURUSD",
            timeframe="1m",
            interval_window=timedelta(hours=1),
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        # 120 seconds spans over 2 or 3 distinct minutes depending on start alignment
        self.assertIn("open", result.columns)
        self.assertIn("high", result.columns)
        self.assertIn("low", result.columns)
        self.assertIn("close", result.columns)


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDukascopyConnector)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
