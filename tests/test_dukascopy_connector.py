# -*- coding: utf-8 -*-
"""
Created on Thu May 21 00:30:00 2026

@author: Antigravity
"""

import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import timedelta
import polars as pl

from forex_data import DukascopyConnector
from forex_data.data_management.common import (
    POLARS_DTYPE_DICT,
    DEFAULT_PATHS
)

from pathlib import Path

_base_path = Path.home() / ".test_database"
_data_path = _base_path
_counter = 1
while _data_path.exists():
    _data_path = Path.home() / f".test_database_{_counter}"
    _counter += 1

_data_path = Path.home() / ".test_database"


class TestDukascopyConnector(unittest.TestCase):
    """
    Unit tests for DukascopyConnector.
    Uses mocks to test the logic of connection checking, scraper,
    data conversion, volume mapping, and recent data reframing.
    """

    def setUp(self):
        self.connector = DukascopyConnector(
            data_path=_data_path / DEFAULT_PATHS.HIST_DATA_FOLDER / 'dukascopy',
            ssl_verify=True
        )

    def tearDown(self):
        self.connector.clear_temporary_folder()

    @patch('requests.Session.get')
    @patch('requests.Session.head')
    def test_check_connection_success(self, mock_head, mock_get):
        # Arrange
        mock_head.return_value = MagicMock(status_code=200)

        # Act
        connected = self.connector.check_connection()

        # Assert
        self.assertTrue(connected)
        mock_head.assert_called_once_with("https://www.dukascopy.com/swiss/english/marketwatch/historical/", timeout=30)
        mock_get.assert_not_called()

    @patch('requests.Session.get')
    @patch('requests.Session.head')
    def test_check_connection_fallback_success(self, mock_head, mock_get):
        # Arrange
        mock_head.side_effect = Exception("HEAD failed")
        mock_get.return_value = MagicMock(status_code=200)

        # Act
        connected = self.connector.check_connection()

        # Assert
        self.assertTrue(connected)
        mock_head.assert_called_once()
        mock_get.assert_called_once_with("https://www.dukascopy.com/swiss/english/marketwatch/historical/", timeout=30)

    @patch('forex_data.data_management.remoteconnector.logger')
    @patch('requests.Session.get')
    @patch('requests.Session.head')
    def test_check_connection_failure(self, mock_head, mock_get, mock_logger):
        # Arrange
        mock_head.side_effect = Exception("HEAD failed")
        mock_get.side_effect = Exception("GET failed")

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
            "https://www.dukascopy.com/swiss/english/marketwatch/historical/",
            timeout=30
        )

    def test_download_month_raw_polars(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=2024,
            month_num=5,
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        self.assertGreater(result.height, 0)
        self.assertListEqual(
            list(result.columns),
            list(POLARS_DTYPE_DICT.TIME_TICK_DTYPE.keys())
        )

        # Check volume and mid price mapping
        self.assertGreaterEqual(result["vol"][0], 0)
        self.assertAlmostEqual(result["p"][0], (result["ask"][0] + result["bid"][0]) / 2, places=5)

    def test_download_month_raw_polars_lazy(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=2024,
            month_num=5,
            engine="polars_lazy"
        )

        # Assert
        self.assertIsInstance(result, pl.LazyFrame)
        collected = result.collect()
        self.assertGreater(collected.height, 0)

    def test_get_recent_data_tick(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        result = self.connector.get_recent_data(
            symbol="EURUSD",
            timeframe="TICK",
            interval_window=timedelta(hours=1),
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        self.assertGreater(result.height, 0)
        self.assertIn("timestamp", result.columns)

    def test_get_recent_data_reframed(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

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
        self.assertGreater(result.height, 0)
        self.assertIn("open", result.columns)
        self.assertIn("high", result.columns)
        self.assertIn("low", result.columns)
        self.assertIn("close", result.columns)

    def test_fail_safe_configurations(self):
        # Arrange & Act
        from tick_vault.config import CONFIG

        # Assert session user-agent is browser-like
        user_agent = self.connector._session.headers.get("User-Agent")
        self.assertIsNotNone(user_agent)
        self.assertIn("Mozilla", user_agent)
        self.assertIn("Chrome", user_agent)

        # Assert tick_vault CONFIG values
        self.assertEqual(CONFIG.worker_per_proxy, 1)  # overridden in connect()
        self.assertEqual(CONFIG.fetch_max_retry_attempts, 5)  # default modified
        self.assertEqual(CONFIG.request_pacing_min, 0.5)
        self.assertEqual(CONFIG.request_pacing_max, 1.5)
        self.assertIn("Mozilla", CONFIG.user_agent)


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDukascopyConnector)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
