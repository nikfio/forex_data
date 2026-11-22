# -*- coding: utf-8 -*-
"""Tests for cross-platform graceful shutdown signal trapping and tick flushing."""

from datetime import datetime
import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import pandas as pd
import polars as pl

from forex_data import (
    COLUMN_NAME,
    DownloadInterruptedError,
    DukascopyConnector,
    HistoricalManagerDB,
    POLARS_DTYPE_DICT,
    graceful_shutdown_guard,
)
from forex_data.data_management.common import write_parquet


class TestGracefulShutdownDump(unittest.TestCase):
    """Verify signal traps flush partial tick data cleanly and enable resume."""

    temp_dir: tempfile.TemporaryDirectory

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.root_path = Path(cls.temp_dir.name)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def _make_tick_lf(self, timestamps) -> pl.LazyFrame:
        n = len(timestamps)
        return pl.DataFrame(
            {
                COLUMN_NAME.TIMESTAMP: timestamps,
                COLUMN_NAME.ASK: [1.1002] * n,
                COLUMN_NAME.BID: [1.1000] * n,
                COLUMN_NAME.ASK_VOLUME: [1.5] * n,
                COLUMN_NAME.BID_VOLUME: [1.2] * n,
                COLUMN_NAME.VWMP: [1.1001] * n,
            }
        ).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE).lazy()

    def test_graceful_shutdown_guard_signals_and_restores(self) -> None:
        prev_sigterm = signal.getsignal(signal.SIGTERM)
        received = []

        with graceful_shutdown_guard(
            on_shutdown=lambda name: received.append(name)
        ) as stop_ev:
            self.assertFalse(stop_ev.is_set())
            os.kill(os.getpid(), signal.SIGTERM)
            self.assertTrue(stop_ev.is_set())
            os.kill(os.getpid(), signal.SIGTERM)

        self.assertEqual(received, ["SIGTERM"])
        self.assertIs(signal.getsignal(signal.SIGTERM), prev_sigterm)

        # Safe no-op registration when used inside a background worker thread
        worker_ok = []

        def _worker() -> None:
            with graceful_shutdown_guard() as ev:
                worker_ok.append(ev.is_set())

        t = threading.Thread(target=_worker)
        t.start()
        t.join(timeout=5)
        self.assertEqual(worker_ok, [False])

    def test_atomic_parquet_write_leaves_no_tmp_files(self) -> None:
        target = self.root_path / "atomic_dir" / "sample.parquet"
        df = self._make_tick_lf([datetime(2024, 1, 2, 10, 0)]).collect()
        write_parquet(df, target)

        self.assertTrue(target.exists())
        leftover_tmp = list(target.parent.glob("*.tmp"))
        self.assertEqual(leftover_tmp, [])
        loaded = pl.read_parquet(target)
        self.assertEqual(loaded.height, 1)

    def test_historical_manager_interrupt_flushes_and_resumes(self) -> None:
        db_dir = self.root_path / "hist_interrupt_db"
        call_log = []

        with patch.object(
            DukascopyConnector, "get_available_tickers", return_value=["EURUSD"]
        ):
            manager = HistoricalManagerDB(
                data_path=str(db_dir),
                volume_data=True,
                engine="polars_lazy",
            )

        def _mock_download_month(
            _conn,
            ticker,
            year,
            month_num,
            temp_filepath="",
            engine="polars_lazy",
            start_date=None,
            end_date=None,
            stop_event=None,
        ):
            call_log.append((month_num, start_date))
            if month_num == 1:
                return self._make_tick_lf([datetime(2024, 1, 2, 10, 0)])
            if month_num == 2:
                return self._make_tick_lf([datetime(2024, 2, 2, 10, 0)])
            if month_num == 3 and len(call_log) == 3:
                self.assertIsNotNone(stop_event)
                stop_event.set()
                return self._make_tick_lf([datetime(2024, 3, 4, 12, 0)])
            if month_num == 3:
                return self._make_tick_lf([datetime(2024, 3, 4, 18, 0)])
            return self._make_tick_lf([datetime(2024, 4, 2, 10, 0)])

        try:
            with patch.object(
                DukascopyConnector,
                "get_available_tickers",
                return_value=["EURUSD"],
            ):
                with patch.object(
                    DukascopyConnector,
                    "download_month_raw",
                    autospec=True,
                    side_effect=_mock_download_month,
                ):
                    with self.assertRaises(DownloadInterruptedError):
                        manager.get_data(
                            ticker="EURUSD",
                            timeframe="1D",
                            start=datetime(2024, 1, 1),
                            end=datetime(2024, 4, 30),
                        )

                    self.assertEqual(
                        manager.last_interrupted_timestamp,
                        datetime(2024, 3, 4, 12, 0),
                    )
                    persisted = manager._db_connector.read_data(
                        "forex",
                        "eurusd",
                        "tick",
                        datetime(2024, 1, 1),
                        datetime(2024, 4, 30),
                    ).collect()
                    self.assertEqual(persisted.height, 3)

                    # Clear stop flag and resume: months 1 & 2 must be skipped
                    call_log.clear()
                    manager.clear_stop()
                    resumed = manager.get_data(
                        ticker="EURUSD",
                        timeframe="1D",
                        start=datetime(2024, 1, 1),
                        end=datetime(2024, 4, 30),
                    ).collect()
                    self.assertEqual([m for m, _ in call_log], [3, 4])
                    self.assertEqual(call_log[0][1], datetime(2024, 3, 4, 10, 0))
                    self.assertGreaterEqual(resumed.height, 4)
        finally:
            manager.close()

    def test_dukascopy_partial_month_contiguous_recovery(self) -> None:
        db_dir = self.root_path / "duka_partial_month"
        conn = DukascopyConnector(data_path=str(db_dir))
        stop_ev = threading.Event()
        seen_reads = []

        class _FakeChunk:
            def __init__(self, dt: datetime) -> None:
                self.time = dt

        class _FakeMetadataDB:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def find_not_attempted_chunks(self, symbol, start, end):
                # Hours 00 and 01 finished; hour 02 was first missing chunk
                return [_FakeChunk(datetime(2024, 3, 1, 2, 0))]

        async def _fake_download_range(symbol, start, end, stop_event=None):
            if stop_event is not None:
                stop_event.set()

        def _fake_read_tick_data(symbol, start, end, strict=False):
            seen_reads.append((start, end))
            return pd.DataFrame(
                {
                    "time": [
                        datetime(2024, 3, 1, 0, 15),
                        datetime(2024, 3, 1, 1, 45),
                    ],
                    "ask": [1.1002, 1.1004],
                    "bid": [1.1000, 1.1002],
                    "ask_volume": [2.0, 3.0],
                    "bid_volume": [1.0, 2.0],
                }
            )

        try:
            with patch.object(
                DukascopyConnector, "get_available_tickers", return_value=["EURUSD"]
            ):
                with patch(
                    "forex_data.data_management.remoteconnector.download_range",
                    side_effect=_fake_download_range,
                ):
                    with patch(
                        "forex_data.data_management.remoteconnector.MetadataDB",
                        _FakeMetadataDB,
                    ):
                        with patch(
                            "forex_data.data_management.remoteconnector.read_tick_data",
                            side_effect=_fake_read_tick_data,
                        ):
                            out = conn.download_month_raw(
                                "EURUSD",
                                2024,
                                3,
                                engine="polars",
                                stop_event=stop_ev,
                            )
            self.assertEqual(
                seen_reads,
                [(datetime(2024, 3, 1, 0, 0), datetime(2024, 3, 1, 2, 0))],
            )
            self.assertEqual(out.height, 2)
        finally:
            conn.clear_temporary_folder()

    def test_clear_temporary_folder_preserves_locked_sibling_temp_dir(self) -> None:
        db_dir = self.root_path / "shared_duka_temp_lock"
        conn_a = DukascopyConnector(data_path=str(db_dir))
        conn_b = DukascopyConnector(data_path=str(db_dir))
        orphan_dir = conn_b._temporary_data_path.parent / "stale-unlocked-orphan"
        try:
            marker = conn_a._temporary_data_path / "metadata.db"
            marker.write_text("active", encoding="utf-8")
            old_ts = time.time() - 3600
            os.utime(conn_a._temporary_data_path, (old_ts, old_ts))

            orphan_dir.mkdir(parents=True, exist_ok=True)
            os.utime(orphan_dir, (old_ts, old_ts))

            conn_b.clear_temporary_folder()
            self.assertTrue(conn_a._temporary_data_path.exists())
            self.assertTrue(marker.exists())
            self.assertFalse(orphan_dir.exists())
        finally:
            conn_a.clear_temporary_folder()
            conn_b.clear_temporary_folder()

    def test_mid_year_exception_flushes_completed_months(self) -> None:
        db_dir = self.root_path / "hist_midyear_err"
        with patch.object(
            DukascopyConnector, "get_available_tickers", return_value=["EURUSD"]
        ):
            manager = HistoricalManagerDB(
                data_path=str(db_dir),
                volume_data=True,
                engine="polars_lazy",
            )

        def _failing_month_3(
            _conn,
            ticker,
            year,
            month_num,
            temp_filepath="",
            engine="polars_lazy",
            start_date=None,
            end_date=None,
            stop_event=None,
        ):
            if month_num == 1:
                return self._make_tick_lf([datetime(2024, 1, 2, 10, 0)])
            if month_num == 2:
                return self._make_tick_lf([datetime(2024, 2, 2, 10, 0)])
            raise RuntimeError("simulated mid-year failure")

        try:
            with patch.object(
                DukascopyConnector,
                "get_available_tickers",
                return_value=["EURUSD"],
            ):
                with patch.object(
                    DukascopyConnector,
                    "download_month_raw",
                    autospec=True,
                    side_effect=_failing_month_3,
                ):
                    with self.assertRaises(RuntimeError):
                        manager.get_data(
                            ticker="EURUSD",
                            timeframe="1D",
                            start=datetime(2024, 1, 1),
                            end=datetime(2024, 4, 30),
                        )

                    persisted = manager._db_connector.read_data(
                        "forex",
                        "eurusd",
                        "tick",
                        datetime(2024, 1, 1),
                        datetime(2024, 4, 30),
                    ).collect()
                    self.assertEqual(persisted.height, 2)
        finally:
            manager.close()
