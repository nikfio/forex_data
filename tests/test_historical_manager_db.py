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
import random
from loguru import logger

from datetime import (
    datetime,
    timedelta
)

from polars import (
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe,
    col
)

from forex_data import (
    HistoricalManagerDB,
    TickerNotFoundError,
    is_empty_dataframe,
    get_histdata_tickers,
    BASE_DATA_COLUMN_NAME,
    SQL_COMPARISON_OPERATORS,
    SQL_CONDITION_AGGREGATION_MODES,
    YEARS,
    POLARS_DTYPE_DICT,
    business_days_data,
    US_holiday_dates
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

    def test_08_get_data_with_weekly_timeframe(self):
        """Test get_data with weekly (1W) timeframe."""
        ticker = 'EURJPY'
        timeframe = '1W'
        start = datetime(2018, 5, 4)
        end = datetime(2018, 11, 1)

        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        self.assertIsNotNone(data)

        if isinstance(data, polars_lazyframe):
            data = data.collect()

        # assert that between two consecutive candles the timestamp
        # difference is 7 days
        random_candles = data.head(2)
        # random_candles is already sorted by timestamp
        self.assertEqual(
            (
                random_candles[1]['timestamp'][0]
                - random_candles[0]['timestamp'][0]
            ).days,
            7,
            msg=f"Between two random consecutive candles the timestamp difference "
                f"is not 7 days: {random_candles[1]['timestamp'][0]} - "
                f"{random_candles[0]['timestamp'][0]}"
        )

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
        for column in expected_columns:
            self.assertIn(column, data.columns,
                          msg=f"Column '{column}' missing from data")

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

    def test_18_clear_database_and_redownload(self):
        """
        Test that clear_database wipes data for a specific ticker
        and verify that data can be correctly re-downloaded.
        """
        ticker = 'GBPUSD'
        timeframe = '1W'

        # 1. Ensure GBPUSD data is present (pull some data)
        # Use a more recent year to avoid issues with old data on histdata.com
        test_year = 2018
        start = datetime(test_year, 1, 1)
        end = datetime(test_year, 1, 15)

        self.hist_manager.get_data(
            ticker=ticker,
            timeframe='1D',
            start=start,
            end=end
        )

        # Verify it's in the ticker list
        self.assertIn(ticker.lower(), self.hist_manager._get_ticker_list())

        # 2. Clear GBPUSD data
        self.hist_manager.clear_database(filter=ticker)

        # 3. Verify GBPUSD is NOT in the ticker list anymore
        self.assertNotIn(ticker.lower(), self.hist_manager._get_ticker_list())

        # 4. Re-download data for a random 4-month timespan
        # Pick 4 random consecutive months within a valid year
        random_year = random.choice(YEARS)
        random_month = random.randint(1, 12)
        start_date = datetime(random_year, random_month, 1)
        # approx 4 months later
        end_date = start_date + timedelta(days=4 * 30)

        logger.debug(f"Downloading data for {ticker} from {start_date}"
                     f" to {end_date} with timeframe {timeframe}")
        data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start_date,
            end=end_date
        )

        # 5. Verify data is successfully re-downloaded
        self.assertIsNotNone(data)

        self.assertFalse(
            is_empty_dataframe(data),
            msg=f"Data re-downloaded for {ticker} is empty"
        )
        self.assertTrue(dict(data.collect_schema()) == POLARS_DTYPE_DICT.TIME_TF_DTYPE)

        # Final check that ticker is back in the list
        self.assertIn(ticker.lower(), self.hist_manager._get_ticker_list())

    def test_19_get_histdata_tickers(self):
        """Test retrieving available tickers from HistData.com."""
        tickers = get_histdata_tickers()

        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0, msg="No tickers retrieved from HistData")
        self.assertIn('EURUSD', tickers, msg="EURUSD missing from retrieved tickers")

        # Verify ticker format (should be uppercase and at least 6 chars)
        for ticker in tickers[:10]:  # Check first 10 for performance
            self.assertTrue(ticker.isupper(), msg=f"Ticker {ticker} is not uppercase")
            self.assertGreaterEqual(len(ticker), 6, msg=f"Ticker {ticker} is too short")

    def test_20_get_data_with_single_condition_less_than(self):
        """Test get_data with single condition filtering (OPEN < threshold)."""
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2018, 1, 1)
        end = datetime(2018, 12, 31)
        min_open_value = 1.13

        # Get data with condition: open < 1.13
        filtered_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=BASE_DATA_COLUMN_NAME.OPEN,
            check_level=min_open_value,
            comparison_operator=SQL_COMPARISON_OPERATORS.LESS_THAN
        )

        self.assertIsNotNone(filtered_data)
        self.assertIsInstance(filtered_data, (polars_dataframe, polars_lazyframe))

        if isinstance(filtered_data, polars_lazyframe):
            filtered_data = filtered_data.collect()

        # Verify all returned rows have OPEN < threshold
        if len(filtered_data) > 0:
            max_open = filtered_data['open'].max()
            self.assertLess(
                max_open,
                min_open_value,
                msg=f"Found OPEN value {max_open} >= threshold {min_open_value}"
            )

    def test_21_get_data_with_multiple_conditions_or(self):
        """Test get_data with multiple conditions using OR aggregation."""
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2019, 1, 1)
        end = datetime(2019, 12, 31)
        high_threshold = 1.145
        low_threshold = 1.12

        # Get data with multiple conditions: HIGH < threshold OR LOW < threshold
        filtered_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=[
                BASE_DATA_COLUMN_NAME.HIGH,
                BASE_DATA_COLUMN_NAME.LOW
            ],
            check_level=[high_threshold, low_threshold],
            comparison_operator=[
                SQL_COMPARISON_OPERATORS.GREATER_THAN,
                SQL_COMPARISON_OPERATORS.LESS_THAN
            ],
            aggregation_mode=SQL_CONDITION_AGGREGATION_MODES.OR
        )

        self.assertIsNotNone(filtered_data)
        self.assertIsInstance(filtered_data, (polars_dataframe, polars_lazyframe))

        if isinstance(filtered_data, polars_lazyframe):
            filtered_data = filtered_data.collect()

        # Verify at least one condition is met for each row
        if len(filtered_data) > 0:
            for row in filtered_data.iter_rows(named=True):
                high_condition = row['high'] > high_threshold
                low_condition = row['low'] < low_threshold
                self.assertTrue(
                    high_condition or low_condition,
                    msg=f"Row does not satisfy OR condition: "
                        f"HIGH={row['high']}, LOW={row['low']}"
                )

    def test_22_get_data_with_multiple_conditions_and(self):
        """Test get_data with multiple conditions using AND aggregation."""
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2019, 1, 1)
        end = datetime(2019, 12, 31)
        high_threshold = 1.145
        low_threshold = 1.12

        # Get data with multiple conditions: HIGH < threshold AND LOW < threshold
        filtered_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=[
                BASE_DATA_COLUMN_NAME.HIGH,
                BASE_DATA_COLUMN_NAME.LOW
            ],
            check_level=[high_threshold, low_threshold],
            comparison_operator=[
                SQL_COMPARISON_OPERATORS.LESS_THAN,
                SQL_COMPARISON_OPERATORS.GREATER_THAN
            ],
            aggregation_mode=SQL_CONDITION_AGGREGATION_MODES.AND
        )

        self.assertIsNotNone(filtered_data)
        self.assertIsInstance(filtered_data, (polars_dataframe, polars_lazyframe))

        if isinstance(filtered_data, polars_lazyframe):
            filtered_data = filtered_data.collect()

        # Verify both conditions are met for each row
        if len(filtered_data) > 0:
            for row in filtered_data.iter_rows(named=True):
                self.assertLess(
                    row['high'],
                    high_threshold,
                    msg=f"HIGH={row['high']} not less than {high_threshold}"
                )
                self.assertGreater(
                    row['low'],
                    low_threshold,
                    msg=f"LOW={row['low']} not greater than {low_threshold}"
                )

    def test_23_get_data_with_condition_greater_than_or_equal(self):
        """Test get_data with GREATER_THAN_OR_EQUAL condition."""
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2020, 6, 1)
        end = datetime(2020, 6, 30)
        close_threshold = 1.12

        # Get all data (no filter)
        all_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        # Get filtered data (close >= threshold)
        filtered_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=BASE_DATA_COLUMN_NAME.CLOSE,
            check_level=close_threshold,
            comparison_operator=SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL
        )

        self.assertIsNotNone(all_data)
        self.assertIsNotNone(filtered_data)

        if isinstance(all_data, polars_lazyframe):
            all_data = all_data.collect()
        if isinstance(filtered_data, polars_lazyframe):
            filtered_data = filtered_data.collect()

        # Verify filtered data is subset of all data
        self.assertLessEqual(
            len(filtered_data),
            len(all_data),
            msg="Filtered data has more rows than unfiltered data"
        )

        # Verify all rows meet the condition
        if len(filtered_data) > 0:
            min_close = filtered_data['close'].min()
            self.assertGreaterEqual(
                min_close,
                close_threshold,
                msg=f"Found CLOSE value {min_close} < threshold {close_threshold}"
            )

    def test_24_business_days_data_filtering(self):
        """
        Test business_days_data function filters out weekends and
        holidays correctly.
        """
        ticker = 'EURUSD'
        timeframe = '1D'

        # Use a 2-month date range (includes weekends and likely some holidays)
        start = datetime(2019, 11, 1)  # November 2019
        end = datetime(2019, 12, 31)   # Through December 2019

        # Get the raw data (includes weekends and holidays)
        raw_data = self.hist_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        self.assertIsNotNone(raw_data)

        # Collect if lazy frame
        if isinstance(raw_data, polars_lazyframe):
            raw_data = raw_data.collect()

        # Apply business_days_data filter
        filtered_data = business_days_data(raw_data)

        # Collect if lazy frame
        if isinstance(filtered_data, polars_lazyframe):
            filtered_data = filtered_data.collect()

        # Test 1: Filtered data should have <= rows than raw data
        # (weekends/holidays removed, forex data may already be business-days-only)
        self.assertLessEqual(
            len(filtered_data),
            len(raw_data),
            msg="Filtered data should have <= rows after removing weekends/holidays"
        )

        # Test 2: Verify no weekend dates remain in filtered data
        if len(filtered_data) > 0:
            # Get weekday for each timestamp (1=Monday, ..., 7=Sunday)
            weekdays = filtered_data.select(
                col('timestamp').dt.weekday().alias('weekday')
            )['weekday'].to_list()

            # All weekdays should be 1-5 (Monday-Friday)
            for weekday in weekdays:
                self.assertLess(
                    weekday,
                    6,
                    msg=f"Found weekend day (weekday={weekday}) in filtered data"
                )
                self.assertGreater(
                    weekday,
                    0,
                    msg=f"Invalid weekday value: {weekday}"
                )

        # Test 3: Verify no US holidays remain in filtered data
        if len(filtered_data) > 0:
            dates = filtered_data.select(
                col('timestamp').dt.date().alias('date')
            )['date'].to_list()

            for date in dates:
                self.assertNotIn(
                    date,
                    US_holiday_dates,
                    msg=f"Found US holiday {date} in filtered data"
                )

        # Test 4: Verify all remaining dates are valid business days
        # (not weekends and not holidays)
        if len(filtered_data) > 0:
            for row in filtered_data.iter_rows(named=True):
                timestamp = row['timestamp']

                # Check it's not a weekend
                weekday = timestamp.weekday()  # Python's weekday: 0=Monday, 6=Sunday
                self.assertLess(
                    weekday,
                    5,  # 0-4 are Monday-Friday
                    msg=f"Found weekend timestamp {timestamp} (weekday={weekday})"
                )

                # Check it's not a holiday
                date = timestamp.date()
                self.assertNotIn(
                    date,
                    US_holiday_dates,
                    msg=f"Found holiday {date} in filtered data"
                )

        # Test 5: Verify filtered data is not empty
        # (there should be at least some business days in 2 months)
        self.assertGreater(
            len(filtered_data),
            0,
            msg="Filtered data should not be empty for a 2-month period"
        )

        # Test 6: Verify data structure integrity
        # (same columns as original)
        self.assertEqual(
            filtered_data.columns,
            raw_data.columns,
            msg="Filtered data should have same columns as raw data"
        )

    def test_25_close_saves_tickers_years_info(self):
        """
        Test that close() method saves tickers years information correctly.

        This test verifies:
        1. The close() method can be called without errors
        2. It persists the tickers_years_dict to the file system
        3. A new instance can load the saved data
        """
        # Create a new manager instance for this test
        test_manager = HistoricalManagerDB(config=test_config_yaml)

        # Get some data to populate the tickers_years_dict
        ticker = 'EURUSD'
        timeframe = '1D'
        start = datetime(2019, 1, 1)
        end = datetime(2019, 1, 31)

        test_manager.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end
        )

        # Verify tickers_years_dict has data
        self.assertIsNotNone(test_manager._tickers_years_dict)
        self.assertIn(ticker.lower(), test_manager._tickers_years_dict)

        # Save the tickers_years_dict before closing
        tickers_years_before = test_manager._tickers_years_dict.copy()

        # Call close() - this should save the data
        test_manager.close()

        # Create a new instance - it should load the saved data
        new_manager = HistoricalManagerDB(config=test_config_yaml)

        # Verify the new instance loaded the same data
        self.assertIsNotNone(new_manager._tickers_years_dict)
        self.assertEqual(
            new_manager._tickers_years_dict,
            tickers_years_before,
            msg="New instance should load the same tickers_years_dict that was saved"
        )

        # Specifically check the ticker we added
        self.assertIn(
            ticker.lower(),
            new_manager._tickers_years_dict,
            msg=f"Ticker {ticker} should be in loaded tickers_years_dict"
        )


if __name__ == '__main__':
    unittest.main()
