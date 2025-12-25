.. forex_data documentation master file

========================================
Welcome to forex_data's documentation!
========================================

**forex_data** is a Python package for aggregating and managing Forex market data in OHLC format.
It provides a unified interface for both historical and real-time data sources, making it easy to
work with multiple data providers and timeframes.

.. note::
   This package combines the power of historical data (going back 20+ years) with real-time updates,
   giving you comprehensive market coverage from a single, easy-to-use API.

Key Features
============

* 📊 **Unified OHLC Format**: All data sources return consistent timestamp, open, high, low, close format
* 🕐 **Multiple Timeframes**: Easily aggregate data into any timeframe (1m, 5m, 1h, 1D, 1W, etc.)
* 📈 **Historical Data**: Access 20+ years of historical data from sources like histdata.com
* ⚡ **Real-time Data**: Get up-to-date market data from Alpha Vantage and Polygon.io
* 🚀 **High Performance**: Built on Polars, PyArrow, or Pandas for lightning-fast data processing
* 💾 **Smart Caching**: Intelligent data caching for optimal performance
* 📉 **Built-in Plotting**: Visualize candlestick charts with ease

Quick Example
=============

.. code-block:: python

   from forex_data import HistoricalManagerDB
   
   # Create a historical data manager
   manager = HistoricalManagerDB(
       config='data_config.yaml'
   )
   
   # Get hourly data for a date range
   data = manager.get_data(
       ticker='NZDUSD',
       timeframe='1h',
       start='2020-01-01',
       end='2020-12-31'
   )
   
   # Plot the data
   manager.plot(
       ticker='NZDUSD',
       timeframe='1D',
       start_date='2020-01-01',
       end_date='2020-12-31'
   )

Documentation Contents
======================

.. toctree::
   :maxdepth: 2
   :caption: User Guide
   
   installation
   quickstart
   configuration
   examples

.. toctree::
   :maxdepth: 2
   :caption: API Reference
   
   modules

.. toctree::
   :maxdepth: 1
   :caption: Additional Information
   
   contributing
   changelog

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
