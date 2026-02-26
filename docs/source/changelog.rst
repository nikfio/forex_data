=========
Changelog
=========

All notable changes to the forex_data project will be documented here.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

Version 0.1.5 (Current)
=======================

*Release Date: 27/02/2026*

Tested with Python 3.12 and 3.13.
Largely tested with default data parameters: engine 'polars-lazy' and file format 'parquet'.

Added
-----

Core Features
^^^^^^^^^^^^^

* **Historical Data Management**: 

  * Access to 20+ years of Forex historical data
  * Support for histdata.com as historical data source
  * Multiple timeframe support (1m, 5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M)
  * Automatic timeframe aggregation
  * Smart data caching for improved performance
  * Get data function with conditional options

* **Real-time Data Management**: 

  * Current and recent market data access
  * Alpha Vantage integration
  * Polygon.io integration
  * Support for multiple data providers with automatic failover
  * Rate limit management

* **Multi-Engine Support**: 

  * Choose your preferred data processing engine
  * Polars (recommended for performance)
  * PyArrow (columnar data format)
  * Pandas (compatibility)

* **Configuration System**: 

  * Flexible YAML-based configuration
  * Centralized settings management
  * Override capabilities
  * Support for multiple configuration files
  * Template configuration included

Data Management
^^^^^^^^^^^^^^^

* **File Format Support**:
  
  * Parquet (recommended for performance)
  * CSV (for compatibility)

* **Caching System**:
  
  * Intelligent data caching
  * Automatic cache management

* **Plotting Capabilities**:
  
  * Built-in candlestick chart generation
  * Interactive visualizations
  * Customizable date ranges

Developer Experience
^^^^^^^^^^^^^^^^^^^^

* **Type Hints**: Full type annotation support
* **Logging**: Comprehensive logging with loguru
* **Testing**: pytest-based test suite
* **CI/CD**: CircleCI integration
* **Documentation**: Sphinx-based documentation
* **Code Quality**: Flake8 and MyPy integration

Documentation
^^^^^^^^^^^^^

* Installation guide
* Quick start tutorial
* Configuration reference
* Comprehensive examples
* API reference
* Contributing guide

Known Issues
------------

* Data engines, all configurations among engine and file format are not tested yet
* Limited to daily and intraday timeframes for real-time sources
* API rate limits affects data retrieval frequency based on the plan associated with the API key

Future Releases
===============

Planned for v0.2.0
------------------

Database Integration
^^^^^^^^^^^^^^^^^^^^

* Cache data in AWS S3 storage and other major cloud providers persistent storage service

Enhanced Features
^^^^^^^^^^^^^^^^^

* Twelve data provider integration
* Real-time data providers caching as in Historical manager

Performance
^^^^^^^^^^^

* Data engine and format files, all options testing 
* Performance comparisons (Polars vs PyArrow vs Pandas)

Planned for v0.3.0
------------------

* DuckDB integration as engine for data caching
* Robust connection between Historical and Real-time data managers if applicable
* Data types outside raw price data available for query (e.g. volume, fundamentals information and others))
* Caching optimizations (Enhance files organization and segmentation)

Planned for v0.4.0
------------------

* Add Stock Market, Future, Commodities and Option data support

How to Upgrade
==============

Update with Poetry:

.. code-block:: bash

   poetry update forex_data

Or with pip:

.. code-block:: bash

   pip install --upgrade forex_data

Contributing to Changelog
==========================

When contributing changes, please:

1. Add your changes to the "Unreleased" section
2. Categorize under: Added, Changed, Deprecated, Removed, Fixed, or Security
3. Include issue/PR references when applicable
4. Follow the existing format

Example:

.. code-block:: text

   Changed
   -------
   * Updated historical data manager to support custom timeframes (#123)

See Also
========

* :doc:`installation` - How to install forex_data
* :doc:`contributing` - How to contribute to the project
* `GitHub Releases <https://github.com/nikfio/forex_data/releases>`_ - Official releases
* `GitHub Issues <https://github.com/nikfio/forex_data/issues>`_ - Report issues or request features
