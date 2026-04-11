import unittest
import shutil
import concurrent.futures
import multiprocessing
from pathlib import Path
from datetime import datetime
from loguru import logger

from forex_data import HistoricalManagerDB

_base_path = Path.home() / ".test_database_concurrent"
_data_path = _base_path
_counter = 1
while _data_path.exists():
    _data_path = Path.home() / f".test_database_concurrent_{_counter}"
    _counter += 1

test_config_yaml = f'''
DATA_PATH: '{_data_path}'
DATA_FILETYPE: 'parquet'
ENGINE: 'polars_lazy'
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
