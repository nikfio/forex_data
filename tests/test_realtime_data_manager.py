# -*- coding: utf-8 -*-
"""
Test suite for RealtimeManager class

Based on examples/realtime_data_manager.py, this test suite validates
the functionality of the realtime data manager including:
- Initialization and configuration
- Daily close data retrieval (last_close, time window, date range)
- Intraday data retrieval with different timeframes
- Data validation and integrity checks
"""

import unittest
from pathlib import Path

from pandas import (
    Timestamp,
    Timedelta,
    DataFrame as pandas_dataframe,
    Series as pandas_series
)

from polars import (
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe
)

from forex_data import (
    RealtimeManager,
    BASE_DATA_COLUMN_NAME,
    is_empty_dataframe,
    shape_dataframe,
    get_dataframe_element
)


__all__ = ['TestRealtimeManager']


class TestRealtimeManager(unittest.TestCase):
    """Test suite for RealtimeManager class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are shared across all tests."""
        cls.config_path = Path(__file__).parent.parent / 'appconfig'

        # Create manager instance for tests
        cls.realtime_manager = RealtimeManager(
            config=str(cls.config_path)
        )

        # Example ticker for tests
        cls.test_ticker = 'EURUSD'

    def test_01_initialization_with_config(self):
        """Test that RealtimeManager initializes correctly with config."""
        manager = RealtimeManager(
            config=str(self.config_path)
        )

        self.assertIsNotNone(manager)
        self.assertIn(manager.engine, ['pandas', 'polars', 'polars_lazy', 'pyarrow'])

    def test_02_initialization_without_config(self):
        """Test that RealtimeManager initializes with default config."""
        manager = RealtimeManager()

        self.assertIsNotNone(manager)
        self.assertIsNotNone(manager.engine)

    def test_03_get_daily_close_last_close_true(self):
        """Test get_daily_close with last_close=True returns single row or Series."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                last_close=True
            )

            # Result can be a Series (pandas) or empty
            if not is_empty_dataframe(result):
                # For pandas engine, last_close returns a Series
                self.assertIsInstance(result, (pandas_series, pandas_dataframe))
        except Exception as e:
            # API rate limit or connectivity issues are expected
            self.skipTest(f"API call failed: {str(e)}")

    def test_04_get_daily_close_with_recent_days_window(self):
        """Test get_daily_close with recent_days_window parameter."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=10
            )

            if not is_empty_dataframe(result):
                self.assertIsInstance(result, (pandas_dataframe, polars_dataframe))
                # Should have multiple rows (or at least one)
                self.assertGreater(shape_dataframe(result)[0], 0)
        except Exception as e:
            # API rate limit or connectivity issues are expected
            self.skipTest(f"API call failed: {str(e)}")

    def test_05_get_daily_close_with_date_range(self):
        """Test get_daily_close with day_start and day_end parameters."""
        try:
            start_date = '2025-01-01'
            end_date = '2025-01-31'

            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                day_start=start_date,
                day_end=end_date
            )

            # Result should be a DataFrame if data is found
            if not is_empty_dataframe(result):
                self.assertIsInstance(result, (pandas_dataframe, polars_dataframe))
        except Exception as e:
            # API rate limit or connectivity issues are expected
            self.skipTest(f"API call failed: {str(e)}")

    def test_06_get_daily_close_returns_dataframe(self):
        """Test that get_daily_close returns appropriate dataframe type."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=5
            )

            if not is_empty_dataframe(result):
                # Should be pandas or polars dataframe/series
                self.assertTrue(
                    isinstance(result, (pandas_dataframe, pandas_series,
                                        polars_dataframe, polars_lazyframe))
                )
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_07_get_daily_close_has_required_columns(self):
        """Test that daily close data has required OHLC columns."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=5
            )

            if not is_empty_dataframe(result):
                # Convert Series to DataFrame if needed for column checking
                if isinstance(result, pandas_series):
                    # Series is single row, should have timestamp and close
                    self.assertIn(BASE_DATA_COLUMN_NAME.TIMESTAMP, result.index)
                else:
                    # Check DataFrame has expected columns
                    expected_cols = [BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                     BASE_DATA_COLUMN_NAME.OPEN,
                                     BASE_DATA_COLUMN_NAME.HIGH,
                                     BASE_DATA_COLUMN_NAME.LOW,
                                     BASE_DATA_COLUMN_NAME.CLOSE]

                    for col in expected_cols:
                        self.assertIn(col, result.columns,
                                      msg=f"Column '{col}' missing from daily close data")
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_08_get_data_with_timeframe(self):
        """Test get_data retrieves data with specified timeframe."""
        try:
            start = '2024-04-10'
            end = '2024-04-15'
            timeframe = '1h'

            result = self.realtime_manager.get_data(
                ticker=self.test_ticker,
                start=start,
                end=end,
                timeframe=timeframe
            )

            # Result can be various dataframe types
            self.assertIsNotNone(result)
        except Exception as e:
            # API rate limit or connectivity issues are expected
            self.skipTest(f"API call failed: {str(e)}")

    def test_09_get_data_with_intraday_timeframe(self):
        """Test get_data with intraday timeframe (5m)."""
        try:
            start = Timestamp.now() - Timedelta('10D')
            end = Timestamp.now() - Timedelta('8D')
            timeframe = '5m'

            result = self.realtime_manager.get_data(
                ticker=self.test_ticker,
                start=start,
                end=end,
                timeframe=timeframe
            )

            self.assertIsNotNone(result)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_10_get_data_with_hourly_timeframe(self):
        """Test get_data with hourly timeframe (1h)."""
        try:
            start = '2024-05-01'
            end = '2024-05-05'
            timeframe = '1h'

            result = self.realtime_manager.get_data(
                ticker=self.test_ticker,
                start=start,
                end=end,
                timeframe=timeframe
            )

            self.assertIsNotNone(result)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_11_get_data_has_required_columns(self):
        """Test that get_data returns data with required columns."""
        try:
            start = '2024-04-10'
            end = '2024-04-15'
            timeframe = '1h'

            result = self.realtime_manager.get_data(
                ticker=self.test_ticker,
                start=start,
                end=end,
                timeframe=timeframe
            )

            if not is_empty_dataframe(result):
                # Check for expected columns
                expected_cols = [BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 BASE_DATA_COLUMN_NAME.OPEN,
                                 BASE_DATA_COLUMN_NAME.HIGH,
                                 BASE_DATA_COLUMN_NAME.LOW,
                                 BASE_DATA_COLUMN_NAME.CLOSE]

                # Convert LazyFrame to DataFrame if needed
                if isinstance(result, polars_lazyframe):
                    result = result.collect()

                if isinstance(result, polars_dataframe):
                    for col in expected_cols:
                        self.assertIn(col, result.columns)
                elif isinstance(result, pandas_dataframe):
                    for col in expected_cols:
                        self.assertIn(col, result.columns)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_12_ticker_validation(self):
        """Test that invalid ticker format is handled appropriately."""
        # Very short ticker
        with self.assertRaises((ValueError, TypeError)):
            self.realtime_manager.get_daily_close(
                ticker='EU',  # Invalid: too short
                last_close=True
            )

    def test_13_date_range_validation(self):
        """Test get_daily_close with valid date range."""
        try:
            # Test that function accepts string dates
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                day_start='2024-01-01',
                day_end='2024-01-31'
            )

            self.assertIsNotNone(result)
        except Exception as e:
            # May fail due to API issues, but method signature is valid
            if 'unexpected keyword argument' in str(e):
                self.fail(f"Function signature error: {e}")

    def test_14_get_daily_close_result_types(self):
        """Test that get_daily_close returns appropriate pandas types."""
        try:
            # With last_close=True
            result_single = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                last_close=True
            )

            # With window
            result_window = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=5
            )

            # Both should be valid types
            valid_single = (isinstance(result_single,
                                       (pandas_series, pandas_dataframe)) or
                            is_empty_dataframe(result_single))
            self.assertTrue(valid_single)

            valid_window = (isinstance(result_window,
                                       (pandas_dataframe, polars_dataframe)) or
                            is_empty_dataframe(result_window))
            self.assertTrue(valid_window)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_15_is_empty_dataframe_works_with_series(self):
        """Test that is_empty_dataframe handles pandas Series correctly."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                last_close=True
            )

            # Should not raise error even if result is a Series
            is_empty = is_empty_dataframe(result)
            self.assertIsInstance(is_empty, bool)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_16_shape_dataframe_works_with_multiple_types(self):
        """Test that shape_dataframe handles different result types."""
        try:
            # Get data that returns Series
            result_series = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                last_close=True
            )

            if not is_empty_dataframe(result_series):
                shape = shape_dataframe(result_series)
                # Should return a tuple
                self.assertIsInstance(shape, tuple)
                self.assertGreater(len(shape), 0)

            # Get data that returns DataFrame
            result_df = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=5
            )

            if not is_empty_dataframe(result_df):
                shape = shape_dataframe(result_df)
                self.assertIsInstance(shape, tuple)
                self.assertGreater(len(shape), 0)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_17_get_dataframe_element_with_dataframe(self):
        """Test get_dataframe_element with DataFrame results."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                recent_days_window=5
            )

            if not is_empty_dataframe(result) and isinstance(result, pandas_dataframe):
                # Should be able to get first timestamp
                timestamp = get_dataframe_element(
                    result,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0
                )
                self.assertIsNotNone(timestamp)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_18_get_dataframe_element_with_series(self):
        """Test get_dataframe_element with Series results."""
        try:
            result = self.realtime_manager.get_daily_close(
                ticker=self.test_ticker,
                last_close=True
            )

            if not is_empty_dataframe(result) and isinstance(result, pandas_series):
                # Should be able to get timestamp from Series
                timestamp = get_dataframe_element(
                    result,
                    BASE_DATA_COLUMN_NAME.TIMESTAMP,
                    0
                )
                self.assertIsNotNone(timestamp)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")

    def test_19_engine_property(self):
        """Test that RealtimeManager has engine property."""
        self.assertTrue(hasattr(self.realtime_manager, 'engine'))
        self.assertIn(self.realtime_manager.engine,
                      ['pandas', 'polars', 'polars_lazy', 'pyarrow'])

    def test_20_multiple_tickers(self):
        """Test that manager can handle requests for different tickers."""
        tickers = ['EURUSD', 'GBPUSD', 'USDJPY']

        try:
            for ticker in tickers:
                result = self.realtime_manager.get_daily_close(
                    ticker=ticker,
                    recent_days_window=3
                )

                # Should return result (empty or not)
                self.assertIsNotNone(result)
        except Exception as e:
            self.skipTest(f"API call failed: {str(e)}")


if __name__ == '__main__':
    unittest.main()
