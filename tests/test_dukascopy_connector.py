# -*- coding: utf-8 -*-
"""
Created on Thu May 21 00:30:00 2026

@author: Antigravity
"""

import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import polars as pl

from forex_data import DukascopyConnector
from forex_data.data_management.common import (
    DEFAULT_PATHS
)
from forex_data.data_management.remoteconnector import InterceptHandler
from tick_vault.config import CONFIG, reload_config

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

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

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

    @unittest.skipUnless(
        os.environ.get("RUN_DUCKASCOPY_DOWNLOAD_TESTS") == "1",
        "Skipped by default. Run with RUN_DUCKASCOPY_DOWNLOAD_TESTS=1"
    )
    def test_download_month_raw_polars_lazy(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=2026,
            month_num=5,
            engine="polars_lazy"
        )

        # Assert
        self.assertIsInstance(result, pl.LazyFrame)
        collected = result.collect()
        self.assertGreater(collected.height, 0)

    @unittest.skipUnless(
        os.environ.get("RUN_DOWNLOAD_CURRENT_MONTH_TESTS") == "1",
        "Skipped by default. Run with RUN_DOWNLOAD_CURRENT_MONTH_TESTS=1"
    )
    def test_download_current_month_raw_polars_lazy(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        now = datetime.now()

        # Act
        result = self.connector.download_month_raw(
            ticker="EURUSD",
            year=now.year,
            month_num=now.month,
            engine="polars_lazy"
        )

        # Assert
        self.assertIsInstance(result, pl.LazyFrame)
        collected = result.collect()
        self.assertGreater(collected.height, 0)

    @unittest.skipUnless(
        os.environ.get("RUN_DOWNLOAD_CURRENT_MONTH_TESTS") == "1",
        "Skipped by default. Run with RUN_DOWNLOAD_CURRENT_MONTH_TESTS=1"
    )
    def test_get_recent_data_tick(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        result = self.connector.get_recent_data(
            symbol="EURUSD",
            timeframe="TICK",
            interval_window=timedelta(hours=10),
            engine="polars"
        )

        # Assert
        self.assertIsInstance(result, pl.DataFrame)
        self.assertGreater(result.height, 0)
        # Assert duration spans the interval window (allowing tolerance for first/last tick times)
        time_diff = result["timestamp"].max() - result["timestamp"].min()
        self.assertGreaterEqual(time_diff + timedelta(seconds=10), timedelta(hours=10))

    @unittest.skipUnless(
        os.environ.get("RUN_DOWNLOAD_CURRENT_MONTH_TESTS") == "1",
        "Skipped by default. Run with RUN_DOWNLOAD_CURRENT_MONTH_TESTS=1"
    )
    def test_get_recent_data_reframed(self):
        if not self.connector.check_connection():
            self.skipTest("No network connection to Dukascopy.")

        # Act
        # Reframe to 1m (1 minute)
        result = self.connector.get_recent_data(
            symbol="EURUSD",
            timeframe="1m",
            interval_window=timedelta(hours=10),
            engine="polars_lazy"
        )

        # Assert
        self.assertIsInstance(result, pl.LazyFrame)
        collected = result.collect()
        self.assertGreater(collected.height, 0)
        self.assertIn("open", collected.columns)
        self.assertIn("high", collected.columns)
        self.assertIn("low", collected.columns)
        self.assertIn("close", collected.columns)

        # Assert duration spans the interval window (allowing tolerance for 1-minute bar resolution)
        df_timespan = collected["timestamp"].max() - collected["timestamp"].min()
        # remove 1 min from df_timespan to account for 1 min bar resolution
        self.assertGreaterEqual(df_timespan.total_seconds() + 60, timedelta(hours=10).total_seconds())

    def test_fail_safe_configurations(self):
        # Assert session user-agent is browser-like
        user_agent = self.connector._session.headers.get("User-Agent")
        self.assertIsNotNone(user_agent)
        self.assertIn("Mozilla", user_agent)
        self.assertIn("Chrome", user_agent)

        # Assert tick_vault CONFIG values
        self.assertEqual(
            CONFIG.worker_per_proxy, 3, "worker_per_proxy should be 3"
        )  # overridden in connect()
        self.assertEqual(
            CONFIG.fetch_max_retry_attempts, 3, "fetch_max_retry_attempts should be 5"
        )  # default modified
        self.assertEqual(
            CONFIG.request_pacing_min, 0.5, "request_pacing_min should be 0.5"
        )
        self.assertEqual(
            CONFIG.request_pacing_max, 1.5, "request_pacing_max should be 1.5"
        )
        self.assertIn(
            "Mozilla", CONFIG.user_agent, "user_agent should contain Mozilla"
        )
        self.assertEqual(
            DukascopyConnector.get_default_data_path(),
            Path(DEFAULT_PATHS.BASE_PATH).expanduser().resolve()
            / DEFAULT_PATHS.HIST_DATA_FOLDER
            / DukascopyConnector.DEFAULT_DATA_FOLDER
        )
        self.assertEqual(
            CONFIG.base_directory,
            self.connector.data_path
        )

    def test_tick_vault_logging_intercept_and_routing(self):
        with tempfile.TemporaryDirectory() as td:
            temp_data_path = Path(td)
            test_connector = DukascopyConnector(
                data_path=temp_data_path, ssl_verify=True
            )
            log_file = test_connector.data_path / "log" / "dukascopy.log"

            tv_logger = logging.getLogger("tick_vault")
            self.assertTrue(
                any(isinstance(h, InterceptHandler) for h in tv_logger.handlers)
            )

            unique_msg = (
                "tick_vault download test line "
                + datetime.now().isoformat()
            )
            tv_logger.info(unique_msg)

            self.assertTrue(log_file.exists())
            content = log_file.read_text()
            self.assertIn(unique_msg, content)
            self.assertIn("INFO", content)

            test_connector.clear_temporary_folder()

    def test_tick_vault_logging_reload_config_no_redundant_file_handler(self):
        tv_logger = logging.getLogger("tick_vault")
        self.assertTrue(
            any(isinstance(h, InterceptHandler) for h in tv_logger.handlers)
        )
        reload_config(worker_per_proxy=3)
        has_file_handler = any(
            isinstance(h, logging.FileHandler) for h in tv_logger.handlers
        )
        self.assertFalse(has_file_handler)

    def test_tick_vault_logging_levels(self):
        with tempfile.TemporaryDirectory() as td:
            temp_data_path = Path(td)
            test_connector = DukascopyConnector(
                data_path=temp_data_path, ssl_verify=True
            )
            log_file = test_connector.data_path / "log" / "dukascopy.log"

            tv_logger = logging.getLogger("tick_vault")
            warn_msg = "test_warning_tick_vault_capture"
            err_msg = "test_error_tick_vault_capture"
            tv_logger.warning(warn_msg)
            tv_logger.error(err_msg)

            self.assertTrue(log_file.exists())
            content = log_file.read_text()
            self.assertIn(warn_msg, content)
            self.assertIn("WARNING", content)
            self.assertIn(err_msg, content)
            self.assertIn("ERROR", content)

            test_connector.clear_temporary_folder()

    @patch("forex_data.data_management.remoteconnector.read_tick_data")
    @patch("forex_data.data_management.remoteconnector.download_range")
    def test_download_month_raw_tick_vault_logs_dumped_to_file(
        self, mock_download, mock_read
    ):
        with tempfile.TemporaryDirectory() as td:
            test_connector = DukascopyConnector(
                data_path=Path(td), ssl_verify=True
            )
            test_connector._tickers_cache = ["EURUSD"]
            log_file = test_connector.data_path / "log" / "dukascopy.log"

            async def _fake_download(symbol, start, end):
                tv_log = logging.getLogger("tick_vault")
                tv_log.info(f"Starting download for {symbol}")
                tv_log.info(f"Found 21 chunks to download for {symbol}")
                tv_log.info(f"Successfully downloaded 21 chunks for {symbol}")

            def _fake_read(symbol, start, end, strict=False):
                tv_log = logging.getLogger("tick_vault")
                tv_log.info(f"Reading tick data for {symbol}")
                tv_log.info(f"Decoding 21 chunks for {symbol}")
                df = pd.DataFrame({
                    "time": [datetime(2026, 5, 1, 10, 0, 0)],
                    "ask": [1.0850],
                    "bid": [1.0848],
                    "ask_volume": [100.0],
                    "bid_volume": [100.0]
                })
                tv_log.info(f"Successfully loaded {len(df)} ticks for {symbol}")
                return df

            mock_download.side_effect = _fake_download
            mock_read.side_effect = _fake_read

            res = test_connector.download_month_raw(
                "EURUSD", 2026, 5, engine="polars"
            )
            self.assertEqual(res.height, 1)

            content = log_file.read_text()
            self.assertIn("Starting download for EURUSD", content)
            self.assertIn("Found 21 chunks to download for EURUSD", content)
            self.assertIn(
                "Successfully downloaded 21 chunks for EURUSD", content
            )
            self.assertIn("Reading tick data for EURUSD", content)
            self.assertIn("Decoding 21 chunks for EURUSD", content)
            self.assertIn("Successfully loaded 1 ticks for EURUSD", content)

            test_connector.clear_temporary_folder()


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDukascopyConnector)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
