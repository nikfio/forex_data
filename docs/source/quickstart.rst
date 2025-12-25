===========
Quick Start
===========

This guide will help you get started with forex_data in just a few minutes.

Basic Concepts
==============

The forex_data package provides two main types of data managers:

1. **Historical Data Manager**: For accessing historical Forex data (20+ years)
2. **Real-time Data Manager**: For accessing current and recent market data

Both managers provide a consistent interface and return data in the standard OHLC format:

* ``timestamp`` - Date and time of the candle
* ``open`` - Opening price
* ``high`` - Highest price
* ``low`` - Lowest price
* ``close`` - Closing price

Configuration
=============

Before using the package, it's recommended to create a configuration file.

Create a file ending with ``data_config.yaml``:

.. code-block:: yaml

   ENGINE: polars          # Options: polars, pyarrow, pandas
   DATA_FILETYPE: parquet       # Options: parquet, csv
   
   PROVIDERS_KEY:
     ALPHAVANTAGE: your_api_key_here
     POLYGON: your_api_key_here

.. tip::
   Use **parquet** for better performance and **polars** for the fastest data processing!

Working with Historical Data
==============================

The historical data manager gives you access to years of Forex data.

Basic Usage
-----------

.. code-block:: python

   from forex_data import HistoricalManagerDB
   
   # Create a manager instance
   hist_manager = HistoricalManagerDB(
       config='data_config.yaml'
   )
   
   # Get data for a specific period
   data = hist_manager.get_data(
       ticker='EURUSD',
       timeframe='1h',
       start='2020-01-01',
       end='2020-01-31'
   )
   
   print(data)

This will return a DataFrame with hourly EURUSD data for January 2020.

Changing Timeframes
-------------------

You can easily work with different timeframes:

.. code-block:: python

   # Get daily data
   daily_data = hist_manager.get_data(
       ticker='EURUSD',
       timeframe='1D',
       start='2019-01-01',
       end='2019-12-31'
   )
   
   # Get 15-minute data
   intraday_data = hist_manager.get_data(
       ticker='EURUSD',
       timeframe='15m',
       start='2020-06-01 00:00:00',
       end='2020-06-07 23:59:59'
   )

Supported timeframes include: ``1m``, ``5m``, ``15m``, ``30m``, ``1h``, ``4h``, ``1D``, ``1W``, ``1M``

Adding Custom Timeframes
-------------------------

You can create and cache custom timeframes:

.. code-block:: python

   # Add a weekly timeframe
   hist_manager.add_timeframe('1W')
   
   # Now you can use it
   weekly_data = hist_manager.get_data(
       ticker='EURUSD',
       timeframe='1W',
       start='2020-01-01',
       end='2020-12-31'
   )

Visualizing Data
----------------

Plot candlestick charts directly:

.. code-block:: python

   hist_manager.plot(
       ticker='EURUSD',
       timeframe='1D',
       start_date='2020-01-01',
       end_date='2020-06-30'
   )

This will generate an interactive candlestick chart.

Working with Real-time Data
============================

The real-time data manager provides access to current market data.

.. important::
   You need API keys from Alpha Vantage and/or Polygon.io to use real-time data.
   Add them to your ``appconfig.yaml`` file.

Basic Usage
-----------

.. code-block:: python

   from forex_data import RealtimeManager
   
   # Create a real-time manager
   rt_manager = RealtimeManager(
       config='data_config.yaml'
   )

Get Latest Close Price
----------------------

.. code-block:: python

   # Get the most recent daily close
   latest = rt_manager.get_daily_close(ticker='GBPUSD', last_close=True)
   print(latest)

Get Recent History
------------------

.. code-block:: python

   # Get last 10 days of data
   recent_data = rt_manager.get_daily_close(ticker='GBPUSD', recent_days_window=10)
   print(recent_data)
   
   # Get specific date range
   range_data = rt_manager.get_daily_close(
       ticker='GBPUSD',
       day_start='2024-01-01',
       day_end='2024-01-31'
   )

Get Intraday Data
-----------------

.. code-block:: python

   # Get hourly data
   intraday = rt_manager.get_data(
       ticker='GBPUSD',
       timeframe='1h',
       start='2024-01-15',
       end='2024-01-20'
   )

Complete Example
================

Here's a complete example combining historical and real-time data:

.. code-block:: python

   from forex_data import HistoricalManagerDB, RealtimeManager
   
   # Setup
   ticker = 'NZDUSD'
   config = 'data_config.yaml'
   
   # Get historical data
   hist = HistoricalManagerDB(config=config)
   historical_data = hist.get_data(
       ticker=ticker,
       timeframe='1D',
       start='2020-01-01',
       end='2023-12-31'
   )
   
   # Get recent real-time data
   rt = RealtimeManager(config=config)
   recent_data = rt.get_daily_close(ticker=ticker, recent_days_window=30)
   
   # Combine them for a complete dataset
   print(f"Historical data: {len(historical_data)} rows")
   print(f"Recent data: {len(recent_data)} rows")
   
   # Plot recent data
   hist.plot(
       ticker=ticker,
       timeframe='1D',
       start_date='2023-12-01',
       end_date='2023-12-31'
   )

Data Engine Selection
=====================

The package supports multiple data processing engines:

Polars (Recommended)
--------------------

.. code-block:: yaml

   DATA_ENGINE: polars

* ✅ Fastest performance
* ✅ Modern API
* ✅ Great for large datasets
* ✅ Best memory efficiency

PyArrow
-------

.. code-block:: yaml

   ENGINE: pyarrow

* ✅ Very fast
* ✅ Columnar format
* ✅ Great for analytics

Pandas
------

.. code-block:: yaml

   ENGINE: pandas

* ⚠️ Slower performance
* ✅ Most familiar API
* ✅ Widest ecosystem

.. warning::
   Pandas is significantly slower than Polars and PyArrow, especially for large datasets.
   Use Polars for the best performance.

Next Steps
==========

Now that you know the basics:

* Learn more about :doc:`configuration` options
* Explore detailed :doc:`examples`
* Check the :doc:`API reference <modules>` for all available methods

Need Help?
==========

* Check the :doc:`examples` for more use cases
* Visit the `GitHub repository <https://github.com/nikfio/forex_data>`_
* Report issues on `GitHub Issues <https://github.com/nikfio/forex_data/issues>`_
