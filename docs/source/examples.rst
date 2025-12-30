========
Examples
========

This page provides comprehensive examples of how to use the forex_data package for various common tasks.

.. contents:: On this page
   :local:
   :depth: 2

Historical Data Examples
========================

Example 1: Basic Data Retrieval
--------------------------------

Get historical data for a specific currency pair and date range:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   from forex_data.config import APPCONFIG_FILE_YAML
   
   # Create manager for NZD/USD
   manager = historical_manager_db(
       ticker='NZDUSD',
       config_file=APPCONFIG_FILE_YAML
   )
   
   # Get hourly data for 2 months
   data = manager.get_data(
       timeframe='1h',
       start='2009-10-04',
       end='2009-12-03'
   )
   
   print(f"Retrieved {len(data)} rows of data")
   print(data.head())

**Expected Output** (with Polars):

.. code-block:: text

   Retrieved 1421 rows of data
   ┌─────────────────────┬─────────┬─────────┬─────────┬─────────┐
   │ timestamp           ┆ open    ┆ high    ┆ low     ┆ close   │
   │ ---                 ┆ ---     ┆ ---     ┆ ---     ┆ ---     │
   │ datetime[ms]        ┆ f32     ┆ f32     ┆ f32     ┆ f32     │
   ╞═════════════════════╪═════════╪═════════╪═════════╪═════════╡
   │ 2009-10-04 17:00:00 ┆ 0.7172  ┆ 0.71735 ┆ 0.7165  ┆ 0.71665 │
   │ 2009-10-04 18:00:00 ┆ 0.71655 ┆ 0.7168  ┆ 0.71555 ┆ 0.71635 │
   │ 2009-10-04 19:00:00 ┆ 0.7164  ┆ 0.71665 ┆ 0.71385 ┆ 0.71445 │
   └─────────────────────┴─────────┴─────────┴─────────┴─────────┘

Example 2: Working with Multiple Timeframes
--------------------------------------------

Aggregate data into different timeframes:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   manager = historical_manager_db(
       ticker='EURUSD',
       config_file='appconfig.yaml'
   )
   
   # Get 1-minute data
   minute_data = manager.get_data(
       timeframe='1m',
       start='2020-01-01 00:00:00',
       end='2020-01-01 23:59:59'
   )
   
   # Add and cache a 15-minute timeframe
   manager.add_timeframe('15m')
   
   # Get 15-minute data
   fifteen_min_data = manager.get_data(
       timeframe='15m',
       start='2020-01-01',
       end='2020-01-07'
   )
   
   # Add daily timeframe
   manager.add_timeframe('1D')
   
   # Get daily data
   daily_data = manager.get_data(
       timeframe='1D',
       start='2020-01-01',
       end='2020-12-31'
   )
   
   print(f"1-minute candles: {len(minute_data)}")
   print(f"15-minute candles: {len(fifteen_min_data)}")
   print(f"Daily candles: {len(daily_data)}")

Example 3: Visualizing Historical Data
---------------------------------------

Create candlestick charts from historical data:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   manager = historical_manager_db(
       ticker='NZDUSD',
       config_file='appconfig.yaml'
   )
   
   # Plot 5 months of daily data
   manager.plot(
       timeframe='1D',
       start_date='2013-02-02',
       end_date='2013-06-23'
   )

This generates an interactive candlestick chart showing price movements.

Example 4: Multi-Year Analysis
-------------------------------

Analyze data across multiple years:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   manager = historical_manager_db(
       ticker='GBPUSD',
       config_file='appconfig.yaml'
   )
   
   # Get 5 years of weekly data
   weekly_data = manager.get_data(
       timeframe='1W',
       start='2015-01-01',
       end='2019-12-31'
   )
   
   # Calculate some basic statistics
   if hasattr(weekly_data, 'describe'):  # Polars
       print(weekly_data.describe())
   else:  # Pandas
       print(weekly_data.describe())
   
   # Find highest and lowest weekly close
   max_close = weekly_data['close'].max()
   min_close = weekly_data['close'].min()
   
   print(f"Highest weekly close: {max_close}")
   print(f"Lowest weekly close: {min_close}")
   
Example 5: Conditional Data Filtering
-------------------------------------

Filter data based on price conditions directly during retrieval:

.. code-block:: python

   from forex_data import (
         HistoricalManagerDB,
         BASE_DATA_COLUMN_NAME,
         SQL_COMPARISON_OPERATORS,
         SQL_CONDITION_AGGREGATION_MODES
      )

   manager = HistoricalManagerDB(config='appconfig.yaml')

   # 1. Simple Filter: Get days where Close >= 1.12
   high_close_data = manager.get_data(
      ticker='EURUSD',
      timeframe='1D',
         start='2020-06-01',
         end='2020-06-30',
         comparison_column_name=BASE_DATA_COLUMN_NAME.CLOSE,
         check_level=1.12,
         comparison_operator=SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL
      )
      
   print(f"Days with Close >= 1.12: {len(high_close_data)}")

   # 2. Complex Filter: Get days where High > 1.145 OR Low < 1.12
   volatile_days = manager.get_data(
      ticker='EURUSD',
      timeframe='1D',
         start='2019-01-01',
         end='2019-12-31',
         comparison_column_name=[
            BASE_DATA_COLUMN_NAME.HIGH,
            BASE_DATA_COLUMN_NAME.LOW
         ],
         check_level=[1.145, 1.12],
         comparison_operator=[
            SQL_COMPARISON_OPERATORS.GREATER_THAN,
            SQL_COMPARISON_OPERATORS.LESS_THAN
         ],
         aggregation_mode=SQL_CONDITION_AGGREGATION_MODES.OR
      )

   print(f"Volatile days found: {len(volatile_days)}")

Real-time Data Examples
=======================

Example 5: Get Latest Market Price
-----------------------------------

Retrieve the most recent closing price:

.. code-block:: python

   from forex_data.data_management import realtime_manager
   
   rt_manager = realtime_manager(
       ticker='NZDUSD',
       config_file='appconfig.yaml'
   )
   
   # Get the latest daily close
   latest_close = rt_manager.get_daily_close(last_close=True)
   
   print("Latest close:")
   print(latest_close)

**Expected Output**:

.. code-block:: text

   Latest close:
   ┌─────────────────────┬─────────┬─────────┬─────────┬────────┐
   │ timestamp           ┆ open    ┆ high    ┆ low     ┆ close  │
   │ ---                 ┆ ---     ┆ ---     ┆ ---     ┆ ---    │
   │ datetime[ms]        ┆ f32     ┆ f32     ┆ f32     ┆ f32    │
   ╞═════════════════════╪═════════╪═════════╪═════════╪════════╡
   │ 2024-04-19 00:00:00 ┆ 0.59022 ┆ 0.59062 ┆ 0.58516 ┆ 0.5886 │
   └─────────────────────┴─────────┴─────────┴─────────┴────────┘

Example 6: Recent Price History
--------------------------------

Get the last N days of market data:

.. code-block:: python

   from forex_data.data_management import realtime_manager
   
   rt_manager = realtime_manager(
       ticker='EURUSD',
       config_file='appconfig.yaml'
   )
   
   # Get last 10 days
   recent_data = rt_manager.get_daily_close(recent_days_window=10)
   
   print("Last 10 days of trading:")
   print(recent_data)

Example 7: Specific Date Range (Real-time)
-------------------------------------------

Query real-time data for a specific date range:

.. code-block:: python

   from forex_data.data_management import realtime_manager
   
   rt_manager = realtime_manager(
       ticker='GBPUSD',
       config_file='appconfig.yaml'
   )
   
   # Get daily closes for March 2024
   march_data = rt_manager.get_daily_close(
       day_start='2024-03-01',
       day_end='2024-03-31'
   )
   
   print(f"Retrieved {len(march_data)} trading days")
   print(march_data)

Example 8: Intraday Real-time Data
-----------------------------------

Get intraday data with specific timeframes:

.. code-block:: python

   from forex_data.data_management import realtime_manager
   
   rt_manager = realtime_manager(
       ticker='USDJPY',
       config_file='appconfig.yaml'
   )
   
   # Get hourly data for a week
   hourly_data = rt_manager.get_data(
       timeframe='1h',
       start='2024-04-10',
       end='2024-04-15'
   )
   
   print(f"Hourly candles: {len(hourly_data)}")
   print(hourly_data.head())

Combined Examples
=================

Example 9: Historical + Real-time Combination
----------------------------------------------

Combine historical data with recent real-time updates:

.. code-block:: python

   from forex_data.data_management import historical_manager_db, realtime_manager
   import polars as pl  # or pandas as pd
   
   ticker = 'NZDUSD'
   config = 'appconfig.yaml'
   
   # Get historical data up to last month
   hist_manager = historical_manager_db(ticker=ticker, config_file=config)
   historical = hist_manager.get_data(
       timeframe='1D',
       start='2020-01-01',
       end='2023-12-31'
   )
   
   # Get recent data
   rt_manager = realtime_manager(ticker=ticker, config_file=config)
   recent = rt_manager.get_daily_close(
       day_start='2024-01-01',
       day_end='2024-12-31'
   )
   
   # Combine datasets (Polars example)
   if isinstance(historical, pl.DataFrame):
       combined = pl.concat([historical, recent])
   else:  # Pandas
       combined = pd.concat([historical, recent])
   
   print(f"Total data points: {len(combined)}")
   print(f"Date range: {combined['timestamp'].min()} to {combined['timestamp'].max()}")

Example 10: Multi-Currency Analysis
------------------------------------

Analyze multiple currency pairs:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   pairs = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD']
   data_dict = {}
   
   for pair in pairs:
       manager = historical_manager_db(
           ticker=pair,
           config_file='appconfig.yaml'
       )
       
       data = manager.get_data(
           timeframe='1D',
           start='2023-01-01',
           end='2023-12-31'
       )
       
       data_dict[pair] = data
       print(f"{pair}: {len(data)} days of data")
   
   # You can now analyze correlations, etc.
   for pair, data in data_dict.items():
       avg_close = data['close'].mean()
       print(f"{pair} average close price: {avg_close:.5f}")

Advanced Examples
=================

Example 11: Custom Data Processing Pipeline
--------------------------------------------

Build a custom processing pipeline:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   import polars as pl
   
   manager = historical_manager_db(
       ticker='EURUSD',
       config_file='appconfig.yaml'
   )
   
   # Get raw data
   data = manager.get_data(
       timeframe='1h',
       start='2023-01-01',
       end='2023-12-31'
   )
   
   # Add custom calculations (Polars example)
   if isinstance(data, pl.DataFrame):
       processed = data.with_columns([
           # Calculate price range
           (pl.col('high') - pl.col('low')).alias('range'),
           
           # Calculate percentage change
           ((pl.col('close') - pl.col('open')) / pl.col('open') * 100).alias('pct_change'),
           
           # Moving average (7-hour)
           pl.col('close').rolling_mean(window_size=7).alias('ma_7')
       ])
   
   print(processed.head())

Example 12: Error Handling and Validation
------------------------------------------

Robust data retrieval with error handling:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   from loguru import logger
   
   def get_forex_data_safely(ticker, timeframe, start, end):
       """Safely retrieve forex data with error handling."""
       try:
           manager = historical_manager_db(
               ticker=ticker,
               config_file='appconfig.yaml'
           )
           
           data = manager.get_data(
               timeframe=timeframe,
               start=start,
               end=end
           )
           
           # Validate data
           if len(data) == 0:
               logger.warning(f"No data retrieved for {ticker}")
               return None
           
           logger.info(f"Successfully retrieved {len(data)} rows for {ticker}")
           return data
           
       except FileNotFoundError:
           logger.error("Configuration file not found")
           return None
       except Exception as e:
           logger.error(f"Error retrieving data: {e}")
           return None
   
   # Use the function
   data = get_forex_data_safely(
       ticker='EURUSD',
       timeframe='1D',
       start='2023-01-01',
       end='2023-12-31'
   )
   
   if data is not None:
       print("Data retrieved successfully")

Example 13: Performance Optimization
-------------------------------------

Optimize for large datasets:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   # Use Polars for best performance
   manager = historical_manager_db(
       ticker='EURUSD',
       config_file='appconfig.yaml',
       engine='polars',  # Explicitly use Polars
       filetype='parquet'  # Use Parquet for speed
   )
   
   # Get large dataset
   import time
   start_time = time.time()
   
   data = manager.get_data(
       timeframe='1m',
       start='2020-01-01',
       end='2023-12-31'
   )
   
   elapsed = time.time() - start_time
   print(f"Retrieved {len(data)} rows in {elapsed:.2f} seconds")
   print(f"Rate: {len(data)/elapsed:.0f} rows/second")

Example 14: Export to Different Formats
----------------------------------------

Export data for use in other tools:

.. code-block:: python

   from forex_data.data_management import historical_manager_db
   
   manager = historical_manager_db(
       ticker='GBPUSD',
       config_file='appconfig.yaml'
   )
   
   data = manager.get_data(
       timeframe='1D',
       start='2023-01-01',
       end='2023-12-31'
   )
   
   # Export to CSV
   if hasattr(data, 'write_csv'):  # Polars
       data.write_csv('gbpusd_2023.csv')
   else:  # Pandas
       data.to_csv('gbpusd_2023.csv', index=False)
   
   # Export to Parquet
   if hasattr(data, 'write_parquet'):  # Polars
       data.write_parquet('gbpusd_2023.parquet')
   else:  # Pandas
       data.to_parquet('gbpusd_2023.parquet', index=False)
   
   print("Data exported successfully")

Testing Examples
================

Example 15: Running the Test Suite
-----------------------------------

The package includes comprehensive tests. Run them with:

.. code-block:: bash

   poetry run pytest

Run specific test files:

.. code-block:: bash

   poetry run pytest tests/test_hist_data_manager.py
   poetry run pytest tests/test_realtime_data_manager.py

Run with verbose output:

.. code-block:: bash

   poetry run pytest -v

Tips and Best Practices
========================

1. **Use Polars for Performance**
   
   .. code-block:: python
   
      # In your appconfig.yaml
      DATA_ENGINE: polars

2. **Cache Timeframes**
   
   .. code-block:: python
   
      # Add common timeframes once
      manager.add_timeframe('15m')
      manager.add_timeframe('1h')
      manager.add_timeframe('1D')

3. **Handle Time Zones**
   
   .. code-block:: python
   
      # Forex data is typically in UTC
      # Be explicit about timezones in your date strings
      data = manager.get_data(
          timeframe='1h',
          start='2023-01-01 00:00:00',  # UTC
          end='2023-01-31 23:59:59'
      )

4. **Monitor API Rate Limits**
   
   .. code-block:: python
   
      from time import sleep
      
      # Add delays between API calls
      for pair in ['EURUSD', 'GBPUSD', 'USDJPY']:
          rt_manager = realtime_manager(ticker=pair, config_file='appconfig.yaml')
          data = rt_manager.get_daily_close(recent_days_window=10)
          sleep(1)  # Respect rate limits

5. **Validate Data Quality**
   
   .. code-block:: python
   
      # Check for missing data
      expected_rows = 365  # For daily data in a year
      if len(data) < expected_rows:
          print(f"Warning: Expected {expected_rows} rows, got {len(data)}")

Next Steps
==========

* Review the :doc:`API reference <modules>` for detailed method documentation
* Check :doc:`configuration` for all available options
* Visit the `GitHub repository <https://github.com/nikfio/forex_data>`_ for more examples
