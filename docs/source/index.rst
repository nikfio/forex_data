.. forex_data documentation master file

=============================================
🚀 Welcome to forex_data Documentation
=============================================

.. image:: https://img.shields.io/badge/python-3.12-blue?style=for-the-badge&logo=python
   :target: https://www.python.org/
   :alt: Python Version

.. image:: https://img.shields.io/badge/Poetry-Package%20Manager-blue?style=for-the-badge&logo=poetry
   :target: https://python-poetry.org/
   :alt: Poetry

.. image:: https://img.shields.io/circleci/build/github/nikfio/forex_data/master?style=for-the-badge&logo=circleci
   :target: https://circleci.com/gh/nikfio/forex_data
   :alt: CI Status

|

**forex_data** is a powerful, professional-grade Python library for aggregating and managing Forex market data in standardized OHLC (Open, High, Low, Close) format.
It provides a unified, elegant interface for both historical and real-time data sources, making it effortless to work with multiple data providers and timeframes.

.. important::
   🎯 **The Perfect Combination**: This package uniquely bridges historical data (20+ years of market history) 
   with real-time updates, giving you comprehensive market coverage through a single, intuitive API.

.. tip::
   **New to forex_data?** Start with our :doc:`quickstart` guide to be up and running in minutes!

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

⚡ Quick Navigation
====================

**Choose your path:**

📥 **Getting Started**
   * :doc:`installation` - Install forex_data and dependencies
   * :doc:`quickstart` - Learn the basics with hands-on examples
   * :doc:`configuration` - Configure engines, file types, and API keys

💡 **Learn More**
   * :doc:`examples` - 15+ comprehensive code examples
   * :doc:`modules` - Complete API reference documentation

🤝 **Community**
   * :doc:`contributing` - Help improve forex_data
   * :doc:`changelog` - Version history and updates

|

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
