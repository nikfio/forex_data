# -*- coding: utf-8 -*-
"""
Test suite for HistoricalManagerDB class

Based on examples/histdata_db_manager.py, this test suite validates
the functionality of the historical data manager including:
- Initialization and configuration
- Data retrieval across different timeframes
- Database operations
- Data validation
"""


import unittest
from datetime import datetime

from polars import (
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe
)

from forex_data import (
    HistoricalManagerDB,
    TickerNotFoundError,
    is_empty_dataframe
)


__all__ = ['TestHistoricalManagerDB']

# Use a runtime defined config yaml file
test_config_yaml = '''
DATA_FILETYPE: 'parquet'

ENGINE: 'polars_lazy'
'''


class TestHistoricalManagerDB(unittest.TestCase):
    """Test suite for HistoricalManagerDB class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are shared across all tests."""

        # Create manager instance for tests
        cls.hist_manager = HistoricalManagerDB(
            config=test_config_yaml
        )

    def test_01_initialization_with_config(self):
        """Test that HistoricalManagerDB initializes correctly with config."""
        manager = HistoricalManagerDB(
            config=test_config_yaml
        )

        self.assertIsNotNone(manager)
        self.assertEqual(manager.data_type, 'parquet')
        self.assertEqual(manager.engine, 'polars_lazy')

    def test_02_initialization_without_config(self):
        """Test that HistoricalManagerDB initializes with defaults."""
        manager = HistoricalManagerDB()

        self.assertIsNotNone(manager)
        self.assertEqual(manager.data_type, 'parquet')
        self.assertEqual(manager.engine, 'polars_lazy')

    def test_03_data_type_validation(self):
        """Test that invalid data_type raises appropriate error."""
        with self.assertRaises(ValueError):
            HistoricalManagerDB(
                config=test_config_yaml,
                data_type='invalid_type'
            )

    def test_04_engine_validation(self):
        """Test that invalid engine raises appropriate error."""
        with self.assertRaises(ValueError):
            HistoricalManagerDB(
                config=test_config_yaml,
                engine='invalid_engine'
            )

    def test_05_get_data_with_daily_timeframe(self):
        """Test retrieving data with daily (1D) timeframe."""
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 12, 3)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        self.assertIsNotNone(data)
        self.assertIsInstance(data, (polars_dataframe, polars_lazyframe))

        # Collect if lazy frame
        if isinstance(data, polars_lazyframe):
            data = data.collect()

        self.assertGreater(len(data), 0)
        self.assertIn('timestamp', data.columns)

    def test_06_get_data_with_3day_timeframe(self):
        """Test retrieving data with 3-day (3D) timeframe."""
        ticker = 'EURUSD'
        timeframe = '3D'
        start = datetime(2018, 10, 4)
        end = datetime(2020, 12, 1)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        self.assertIsNotNone(data)
        self.assertIsInstance(data, (polars_dataframe, polars_lazyframe))

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        self.assertGreater(len(data), 0)

    def test_07_get_data_date_range_validation(self):
        """Test that data is within requested date range."""
        ticker = 'EURJPY'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 12, 3)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        if len(data) > 0:
            min_date = data['timestamp'].min()
            max_date = data['timestamp'].max()

            self.assertGreaterEqual(min_date, start)
            self.assertLessEqual(max_date, end)

    def test_08_get_data_with_add_timeframe_flag(self):
        """Test get_data with add_timeframe parameter."""
        ticker = 'EURJPY'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 11, 1)

        # Test with add_timeframe=True (default)
        data_with_tf = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            add_timeframe=True
        )

        # Test with add_timeframe=False
        data_without_tf = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            add_timeframe=False
        )

        self.assertIsNotNone(data_with_tf)
        self.assertIsNotNone(data_without_tf)

    def test_09_data_columns_present(self):
        """Test that retrieved data has expected columns."""
        ticker = 'EURJPY'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 10, 15)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        expected_columns = ['timestamp', 'open', 'high', 'low', 'close']
        for col in expected_columns:
            self.assertIn(col, data.columns,
                          msg=f"Column '{col}' missing from data")

    def test_10_data_sorted_by_timestamp(self):
        """Test that data is sorted by timestamp in ascending order."""
        ticker = 'EURJPY'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 10, 20)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        if len(data) > 1:
            timestamps = data['timestamp'].to_list()
            self.assertEqual(timestamps, sorted(timestamps),
                             msg="Data not sorted by timestamp")

    def test_11_no_duplicate_timestamps(self):
        """Test that there are no duplicate timestamps in the data."""
        ticker = 'EURJPY'
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 11, 1)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        if len(data) > 0:
            unique_count = data['timestamp'].n_unique()
            total_count = len(data)
            self.assertEqual(unique_count, total_count,
                             msg="Duplicate timestamps found")

    def test_12_add_timeframe_method(self):
        """Test adding a new timeframe to the manager."""
        new_timeframe = '2D'

        # Add the timeframe
        self.hist_manager.add_timeframe(new_timeframe)

        # Verify it was added (if _tf_list is accessible)
        if hasattr(self.hist_manager, '_tf_list'):
            self.assertIn(new_timeframe, self.hist_manager._tf_list)

    def test_13_multiple_timeframes(self):
        """Test retrieving data for multiple different timeframes."""
        ticker = 'EURJPY'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 11, 1)

        timeframes = ['1D', '2D', '3D']

        for tf in timeframes:
            data = self.hist_manager.get_data(
                ticker=ticker,
                timeframe=tf,
                start=start,
                end=end
            )
            self.assertIsNotNone(data,
                                 msg=f"Failed to retrieve data for timeframe {tf}")

    def test_14_empty_date_range(self):
        """Test behavior with a very narrow date range that might return no data."""
        ticker = 'EURJPY'
        timeframe = '1D'
        # Use same date for start and end
        start = datetime(2018, 10, 4, 0, 0, 0)
        end = datetime(2018, 10, 4, 1, 0, 0)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        # Should return empty or minimal data, not raise an error
        self.assertIsNotNone(data)

    def test_15_future_date_range(self):
        """Test behavior with future dates (should raise ValueError)."""
        ticker = 'EURJPY'
        timeframe = '1D'

        # Use current year + 5 to ensure it's always in the future
        future_year = datetime.now().year + 5
        start = datetime(future_year, 1, 1)
        end = datetime(future_year, 12, 31)

        # Future dates outside available range should raise ValueError
        with self.assertRaises(ValueError):
            self.hist_manager.get_data(
                ticker=ticker,
                timeframe=timeframe,
                start=start,
                end=end
            )

    def test_16_ticker_case_handling(self):
        """Test that ticker symbols are handled consistently."""
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 10, 15)

        # Test with lowercase
        data_lower = self.hist_manager.get_data(
            ticker='eurjpy',
            timeframe=timeframe,
            start=start,
            end=end
        )

        # Test with uppercase
        data_upper = self.hist_manager.get_data(
            ticker='EURJPY',
            timeframe=timeframe,
            start=start,
            end=end
        )

        # Both should return data (or both should be empty)
        self.assertEqual(
            (data_lower is not None),
            (data_upper is not None),
            msg="Ticker case handling inconsistent"
        )

        # Both check dataframe are not empty
        self.assertFalse(is_empty_dataframe(data_lower))
        self.assertFalse(is_empty_dataframe(data_upper))

    def test_17_ticker_not_found_exception(self):
        """Test that TickerNotFoundError is raised for invalid tickers."""
        ticker = "USDNZD"
        timeframe = '1D'
        start = datetime(2018, 10, 4)
        end = datetime(2018, 12, 3)

        # USDNZD is used as an example of a ticker not supported/found
        with self.assertRaises(TickerNotFoundError):
            self.hist_manager.get_data(
                ticker=ticker,
                timeframe=timeframe,
                start=start,
                end=end
            )


if __name__ == '__main__':
    unittest.main()
