import os
import tempfile
import threading
import time
import unittest
import shutil
import concurrent.futures
import multiprocessing
from pathlib import Path
from datetime import datetime
from unittest.mock import patch
from filelock import FileLock
from loguru import logger
import polars as pl

from forex_data import (
    COLUMN_NAME,
    HistoricalManagerDB,
    POLARS_DTYPE_DICT,
)
from forex_data.data_management import DukascopyConnector

_base_path = Path.home() / ".test_database_concurrent"
_data_path = _base_path
_counter = 1
while _data_path.exists():
    _data_path = Path.home() / f".test_database_concurrent_{_counter}"
    _counter += 1

test_config_yaml = f'''
DATA_PATH: '{_data_path}'
DB_FILES_YEAR_PARTITIONING: True
SSL_VERIFY: True
'''


def worker_task_process(worker_id, config_yaml):
    """Worker task that instantiates its own manager."""
    manager = None
    try:
        manager = HistoricalManagerDB(config=config_yaml)
        data = manager.get_data(
            ticker='EURUSD',
            timeframe='1D',
            start=datetime(2018, 1, 1),
            end=datetime(2018, 1, 31)
        )
        if hasattr(data, 'collect'):
            data = data.collect()

        return len(data)
    except Exception as e:
        logger.exception(f"Worker {worker_id} failed with exception: {e}")
        return e
    finally:
        if manager:
            try:
                manager.close()
            except Exception:
                pass


def worker_task_thread(worker_id, config_yaml):
    """Worker task that uses its own manager instance in threads."""
    manager = None
    try:
        manager = HistoricalManagerDB(config=config_yaml)
        data = manager.get_data(
            ticker='GBPUSD',
            timeframe='1D',
            start=datetime(2019, 1, 1),
            end=datetime(2019, 1, 31)
        )
        if hasattr(data, 'collect'):
            data = data.collect()

        return len(data)
    except Exception as e:
        logger.exception(f"Worker {worker_id} failed with exception: {e}")
        return e
    finally:
        if manager:
            try:
                manager.close()
            except Exception:
                pass


class TestHistoricalManagerConcurrency(unittest.TestCase):
    """Test suite for validating parallel runs of HistManager."""

    @classmethod
    def setUpClass(cls):
        """Clean directory if it accidentally exists and set up shared manager."""
        # Broad cleanup of any legacy test directories matching the pattern
        for p in Path.cwd().glob(".test_database_concurrent*"):
            if p.is_dir():
                try:
                    shutil.rmtree(p)
                except Exception:
                    pass

        # Also ensure our specific current path is clean
        if _data_path.exists():
            shutil.rmtree(_data_path)

        cls.hist_manager = HistoricalManagerDB(config=test_config_yaml)

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures after all tests have run."""
        if hasattr(cls, 'hist_manager') and cls.hist_manager is not None:
            try:
                cls.hist_manager.close()
            except Exception:
                pass

        if _data_path.exists():
            shutil.rmtree(_data_path)

    @unittest.skipUnless(
        os.environ.get("RUN_DUCKASCOPY_DOWNLOAD_TESTS") == "1",
        "Skipped by default. Run with RUN_DUCKASCOPY_DOWNLOAD_TESTS=1"
    )
    def test_01_multiprocessing_get_data(self):
        """Test concurrent get_data using a ProcessPoolExecutor."""
        num_workers = 4
        results = []

        ctx = multiprocessing.get_context('spawn')
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers,
            mp_context=ctx
        ) as executor:
            futures = [
                executor.submit(worker_task_process, i, test_config_yaml)
                for i in range(num_workers)
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        for res in results:
            self.assertFalse(
                isinstance(res, Exception),
                f"Process worker failed with exception: {res}"
            )
            self.assertGreater(
                res, 0, "Dataframe length should be greater than 0"
            )

        # Ensure all returned the same amount of data
        self.assertEqual(
            len(set(results)),
            1,
            "Workers returned different lengths of data!"
        )

    @unittest.skipUnless(
        os.environ.get("RUN_DUCKASCOPY_DOWNLOAD_TESTS") == "1",
        "Skipped by default. Run with RUN_DUCKASCOPY_DOWNLOAD_TESTS=1"
    )
    def test_02_multithreading_get_data(self):
        """Test concurrent get_data using a ThreadPoolExecutor."""
        num_workers = 4
        results = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers
        ) as executor:
            futures = [
                executor.submit(worker_task_thread, i, test_config_yaml)
                for i in range(num_workers)
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        for res in results:
            self.assertFalse(
                isinstance(res, Exception),
                f"Thread worker failed with exception: {res}"
            )
            self.assertGreater(
                res, 0, "Dataframe length should be greater than 0"
            )

        # Ensure all returned the same amount of data
        self.assertEqual(
            len(set(results)),
            1,
            "Workers returned different lengths of data!"
        )

    @staticmethod
    def _sample_jan_2025_ticks(year=2025, month_num=1):
        """Return small weekday tick LazyFrame inside requested year/month."""
        if year != 2025 or month_num != 1:
            return pl.DataFrame(schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE).lazy()
        return pl.DataFrame({
            COLUMN_NAME.TIMESTAMP: [
                datetime(2025, 1, 15, 10, 0, 0),
                datetime(2025, 1, 15, 10, 30, 0),
                datetime(2025, 1, 15, 11, 0, 0),
                datetime(2025, 1, 16, 10, 0, 0),
                datetime(2025, 1, 16, 11, 0, 0),
                datetime(2025, 1, 16, 12, 0, 0),
            ],
            COLUMN_NAME.ASK: [1.1002, 1.1003, 1.1004, 1.1005, 1.1006, 1.1007],
            COLUMN_NAME.BID: [1.1000, 1.1001, 1.1002, 1.1003, 1.1004, 1.1005],
            COLUMN_NAME.ASK_VOLUME: [100.0] * 6,
            COLUMN_NAME.BID_VOLUME: [100.0] * 6,
            COLUMN_NAME.VWMP: [1.1001, 1.1002, 1.1003, 1.1004, 1.1005, 1.1006],
        }).cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE).lazy()

    def test_03_slow_ticker_download_does_not_block_other_ticker(self):
        """A long download on GBPUSD must never block EURUSD updates or reads."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            gbp_started = threading.Event()
            release_gbp = threading.Event()
            gbp_errors = []

            def mock_dl(
                ticker,
                year,
                month_num,
                engine='polars_lazy',
                start_date=None,
                end_date=None,
                **kwargs,
            ):
                if ticker.upper() == 'GBPUSD':
                    gbp_started.set()
                    release_gbp.wait(timeout=10.0)
                return self._sample_jan_2025_ticks(year, month_num)

            with patch.object(
                DukascopyConnector,
                'get_available_tickers',
                return_value=['EURUSD', 'GBPUSD'],
            ), patch.object(
                DukascopyConnector,
                'download_month_raw',
                side_effect=mock_dl,
            ):
                mgr_gbp = HistoricalManagerDB(data_path=tmp_dir, volume_data=True)
                mgr_eur = HistoricalManagerDB(data_path=tmp_dir, volume_data=True)

                def run_gbp():
                    try:
                        df_g = mgr_gbp.get_data(
                            ticker='GBPUSD',
                            timeframe='1h',
                            start=datetime(2025, 1, 15, 0, 0),
                            end=datetime(2025, 1, 16, 23, 59),
                        )
                        if hasattr(df_g, 'collect'):
                            df_g = df_g.collect()
                        self.assertGreater(len(df_g), 0)
                    except Exception as exc:
                        gbp_errors.append(exc)

                t_gbp = threading.Thread(target=run_gbp, daemon=True)
                t_gbp.start()
                self.assertTrue(gbp_started.wait(timeout=5.0))

                # EURUSD must finish quickly while GBPUSD is blocked mid-download
                eur_start_t = time.monotonic()
                df_e = mgr_eur.get_data(
                    ticker='EURUSD',
                    timeframe='1h',
                    start=datetime(2025, 1, 15, 0, 0),
                    end=datetime(2025, 1, 16, 23, 59),
                )
                if hasattr(df_e, 'collect'):
                    df_e = df_e.collect()
                win_e = mgr_eur.get_data_window(
                    ticker='EURUSD',
                    date=datetime(2025, 1, 16, 12, 0),
                    timeframe='1h',
                    periods=2,
                    direction='backward',
                )
                if hasattr(win_e, 'collect'):
                    win_e = win_e.collect()
                eur_elapsed = time.monotonic() - eur_start_t

                self.assertGreater(len(df_e), 0)
                self.assertGreater(len(win_e), 0)
                self.assertLess(eur_elapsed, 5.0)
                self.assertTrue(t_gbp.is_alive())

                release_gbp.set()
                t_gbp.join(timeout=10.0)
                self.assertFalse(t_gbp.is_alive())
                self.assertEqual(gbp_errors, [])

                mgr_eur.close()
                mgr_gbp.close()

                final_mgr = HistoricalManagerDB(data_path=tmp_dir, offline=True)
                loaded = final_mgr._db_connector.load_tickers_years_info()
                self.assertIn('eurusd', loaded)
                self.assertIn('gbpusd', loaded)

    def test_04_metadata_merge_preserves_concurrent_tickers_on_close(self):
        """Stale manager closing for GBPUSD must not erase newer EURUSD metadata."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(
                DukascopyConnector,
                'get_available_tickers',
                return_value=['EURUSD', 'GBPUSD'],
            ), patch.object(
                DukascopyConnector,
                'download_month_raw',
                side_effect=lambda t, y, m, *a, **kw: (
                    self._sample_jan_2025_ticks(y, m)
                ),
            ):
                # Start GBPUSD manager early before EURUSD exists in metadata
                mgr_gbp = HistoricalManagerDB(data_path=tmp_dir, volume_data=True)
                mgr_gbp.get_data(
                    ticker='GBPUSD',
                    timeframe='1h',
                    start=datetime(2025, 1, 15, 0, 0),
                    end=datetime(2025, 1, 16, 23, 59),
                )

                # Separate EURUSD manager updates EURUSD afterwards and closes
                mgr_eur = HistoricalManagerDB(data_path=tmp_dir, volume_data=True)
                mgr_eur.get_data(
                    ticker='EURUSD',
                    timeframe='1h',
                    start=datetime(2025, 1, 15, 0, 0),
                    end=datetime(2025, 1, 16, 23, 59),
                )
                mgr_eur.close()

                # Now older GBPUSD manager (without EURUSD in memory) closes
                mgr_gbp.close()

                on_disk = mgr_eur._db_connector.load_tickers_years_info()
                self.assertIn('eurusd', on_disk)
                self.assertIn('gbpusd', on_disk)

    def test_05_offline_get_data_window_non_blocking_when_same_ticker_locked(self):
        """Offline reads must return without hanging even if ticker lock is held."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(
                DukascopyConnector,
                'get_available_tickers',
                return_value=['EURUSD'],
            ), patch.object(
                DukascopyConnector,
                'download_month_raw',
                side_effect=lambda t, y, m, *a, **kw: (
                    self._sample_jan_2025_ticks(y, m)
                ),
            ):
                writer = HistoricalManagerDB(data_path=tmp_dir, volume_data=True)
                writer.get_data(
                    ticker='EURUSD',
                    timeframe='1h',
                    start=datetime(2025, 1, 15, 0, 0),
                    end=datetime(2025, 1, 16, 23, 59),
                )
                writer.close()

            offline_mgr = HistoricalManagerDB(data_path=tmp_dir, offline=True)
            eur_lock = FileLock(offline_mgr._get_ticker_lock_path('EURUSD'))
            with eur_lock:
                t_start = time.monotonic()
                # Periods span into missing Dec 2024 so _acquire_ticker_lock is hit
                win = offline_mgr.get_data_window(
                    ticker='EURUSD',
                    date=datetime(2025, 1, 16, 12, 0),
                    timeframe='1h',
                    periods=500,
                    direction='backward',
                )
                if hasattr(win, 'collect'):
                    win = win.collect()
                elapsed = time.monotonic() - t_start

            self.assertGreater(len(win), 0)
            self.assertLess(elapsed, 2.0)
