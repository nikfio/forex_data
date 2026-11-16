# -*- coding: utf-8 -*-
"""Tests for normalized RealtimeData/<connector> folder resolution."""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
from loguru import logger

from forex_data.data_management.common import COLUMN_NAME, DEFAULT_PATHS
from forex_data.data_management.historicaldata import HistoricalManagerDB
from forex_data.data_management.remoteconnector import (
    CTraderConnector,
    TiingoConnector,
    TwelveDataConnector,
)


def _remove_handlers_under(root: Path) -> None:
    """Detach Loguru file sinks inside `root` before temp cleanup."""
    root_str = str(root.resolve())
    to_remove = []
    for handler_id, handler in logger._core.handlers.items():
        sink = getattr(handler, "_sink", None)
        sink_path = getattr(sink, "_path", None)
        if sink_path and str(sink_path).startswith(root_str):
            to_remove.append(handler_id)
    for handler_id in to_remove:
        try:
            logger.remove(handler_id)
        except ValueError:
            pass


class TestRealtimeConnectorPaths(unittest.TestCase):
    """Verify all realtime connectors resolve under `<data_path>/RealtimeData/`."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp_dir = tempfile.TemporaryDirectory()
        cls.root_path = Path(cls._tmp_dir.name).resolve()

    @classmethod
    def tearDownClass(cls) -> None:
        _remove_handlers_under(cls.root_path)
        cls._tmp_dir.cleanup()

    def _build_connector(self, connector_cls, raw_data_path):
        """Instantiate a realtime connector without hitting external APIs."""
        if connector_cls is CTraderConnector:
            return connector_cls(
                client_id="test_cid",
                client_secret="test_csec",
                access_token="test_tok",
                broker_account_id="123456",
                data_path=raw_data_path,
            )
        if connector_cls is TiingoConnector:
            return connector_cls(
                api_key="test_tiingo_key",
                plan="free",
                data_path=raw_data_path,
            )
        return connector_cls(
            api_key="test_twelve_key",
            plan="free",
            data_path=raw_data_path,
        )

    def test_get_default_data_path(self) -> None:
        base_default = Path(DEFAULT_PATHS.BASE_PATH).expanduser().resolve()
        cases = (
            (CTraderConnector, "ctrader"),
            (TiingoConnector, "tiingo"),
            (TwelveDataConnector, "twelvedata"),
        )
        for connector_cls, folder in cases:
            self.assertEqual(connector_cls.DEFAULT_DATA_FOLDER, folder)
            self.assertEqual(
                connector_cls.get_default_data_path(),
                base_default / DEFAULT_PATHS.REALTIME_DATA_FOLDER / folder,
            )

    def test_root_data_path_routes_under_realtimedata(self) -> None:
        cases = (
            (CTraderConnector, "ctrader", "ctrader.log"),
            (TiingoConnector, "tiingo", "tiingo.log"),
            (TwelveDataConnector, "twelvedata", "twelvedata.log"),
        )
        for connector_cls, folder, log_name in cases:
            expected = (
                self.root_path / DEFAULT_PATHS.REALTIME_DATA_FOLDER / folder
            )
            conn = self._build_connector(connector_cls, self.root_path)
            try:
                self.assertEqual(conn.data_path, expected)
                self.assertTrue((expected / "log" / log_name).exists())
            finally:
                conn.clear_temporary_folder()

    def test_pre_nested_and_legacy_paths_do_not_duplicate(self) -> None:
        legacy_cases = (
            (CTraderConnector, "ctrader", "Realtime_cTrader"),
            (TiingoConnector, "tiingo", "Realtime_Tiingo"),
            (TwelveDataConnector, "twelvedata", "Realtime_TwelveData"),
        )
        for connector_cls, folder, legacy_name in legacy_cases:
            rt_root = self.root_path / DEFAULT_PATHS.REALTIME_DATA_FOLDER
            expected = rt_root / folder
            inputs = (
                rt_root,
                expected,
                self.root_path / legacy_name,
                expected / legacy_name,
                self.root_path / "Realtime",
            )
            for candidate in inputs:
                conn = self._build_connector(connector_cls, candidate)
                try:
                    self.assertEqual(conn.data_path, expected)
                finally:
                    conn.clear_temporary_folder()

    @patch("forex_data.data_management.historicaldata.CTraderConnector")
    def test_historical_manager_patch_avoids_historical_ctrader_dir(
        self, mock_ctrader_cls
    ) -> None:
        hm = HistoricalManagerDB(data_path=self.root_path, offline=True)
        tick_dir = (
            hm._db_connector.data_path / "forex" / "EURUSD" / "tick"
        )
        tick_dir.mkdir(parents=True, exist_ok=True)
        tick_name = hm._db_connector._get_filename(
            "forex", "EURUSD", "tick", 2025
        )
        df = pl.DataFrame(
            {
                COLUMN_NAME.TIMESTAMP: [datetime(2025, 1, 2, 10, 0, 0)],
                COLUMN_NAME.ASK: [0.0],
                COLUMN_NAME.BID: [1.0],
                COLUMN_NAME.ASK_VOLUME: [1.0],
                COLUMN_NAME.BID_VOLUME: [1.0],
                COLUMN_NAME.VWMP: [1.0],
            }
        )
        df.write_parquet(tick_dir / tick_name)

        mock_inst = MagicMock()
        mock_inst.download_range_raw.return_value = pl.DataFrame(
            schema={COLUMN_NAME.TIMESTAMP: pl.Datetime("ms")}
        )
        mock_ctrader_cls.return_value = mock_inst

        hm.patch_raw_tick_holes("EURUSD", 2025)
        mock_ctrader_cls.assert_called_once()
        self.assertEqual(
            mock_ctrader_cls.call_args.kwargs.get("data_path"),
            str(hm.data_path),
        )
        self.assertFalse(
            (self.root_path / DEFAULT_PATHS.HIST_DATA_FOLDER / "ctrader").exists()
        )


if __name__ == "__main__":
    unittest.main()
