============================
Command-Line Interface (CLI)
============================

The ``forex_data`` package includes a full-featured Command-Line Interface (CLI) powered by Typer, located at ``cli/fx_agg_app.py``. It provides commands to generate, download, cache, and plot historical and real-time forex market data directly from your terminal.

Execution Syntax
================

Launch CLI commands using ``uv run python cli/fx_agg_app.py``:

.. code-block:: bash

   uv run python cli/fx_agg_app.py --help

Commands Overview
=================

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Command
     - Description
   * - ``generate-database``
     - Download missing historical forex tick data and generate aggregated timeframes cached in Parquet databases.
   * - ``plot``
     - Render interactive candlestick/line charts for historical ranges or launch a real-time streaming Dash dashboard.

--------------------------------------------------

``generate-database``
=====================

Downloads raw tick data (e.g. from Dukascopy/HistData) for the specified currency pairs and builds local partitioned Parquet tables for all requested timeframes.

Usage
-----

.. code-block:: bash

   uv run python cli/fx_agg_app.py generate-database [ARGUMENTS] [OPTIONS]

Arguments
---------

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Argument
     - Type
     - Description
   * - ``tickers``
     - Text (Required)
     - Currency ticker symbol or comma-separated list of symbols (e.g. ``EURUSD`` or ``EURUSD,GBPUSD,USDJPY``).
   * - ``start_date``
     - Text (Required)
     - Start timestamp formatted as ``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS`` (e.g. ``2020-01-01``).
   * - ``end_date``
     - Text (Optional)
     - End timestamp formatted as ``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS``. Defaults to ``now``.

Options
-------

.. list-table::
   :widths: 25 15 20 40
   :header-rows: 1

   * - Option
     - Flag
     - Default
     - Description
   * - ``--timeframe``
     - ``-t``
     - ``["1D"]``
     - Timeframe interval(s) to aggregate (e.g., ``1m``, ``5m``, ``1h``, ``1D``; minimum resolution: ``1m``). Can be specified multiple times or as comma-separated values. Sub-minute intervals are rejected.
   * - ``--data-path``
     - ``-d``
     - ``~/.database``
     - Absolute or relative database root directory. Automatically defaults to ``DATA_PATH`` in ``appconfig/data_config.yaml`` if present.
   * - ``--offline`` / ``--no-download``
     -
     - ``False``
     - Operates in offline mode without attempting remote HTTP downloads.
   * - ``--config``
     - ``-c``
     - ``""``
     - Path to custom YAML configuration file or inline YAML string.

Examples
--------

**1. Generate Daily and Hourly Data for EURUSD**:

.. code-block:: bash

   uv run python cli/fx_agg_app.py generate-database EURUSD 2020-01-01 2021-01-01 -t 1D -t 1h

**2. Generate Multiple Tickers with Comma-Separated Syntax**:

.. code-block:: bash

   uv run python cli/fx_agg_app.py generate-database EURUSD,GBPUSD,USDJPY 2018-01-01 2018-12-31 -t 1D,1h,15m

**3. Target a Custom Database Directory in Offline Mode**:

.. code-block:: bash

   uv run python cli/fx_agg_app.py generate-database EURUSD 2024-01-01 now -t 1D -d ~/.test_database --offline

--------------------------------------------------

``plot``
========

Renders interactive market charts in your default browser using Plotly, or launches a live-updating Dash dashboard server for streaming prices.

Usage
-----

.. code-block:: bash

   uv run python cli/fx_agg_app.py plot [ARGUMENTS] [OPTIONS]

Arguments
---------

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Argument
     - Type
     - Description
   * - ``ticker``
     - Text (Required)
     - Currency pair symbol (e.g. ``EURUSD``).
   * - ``start_date``
     - Text (Required)
     - Start date/time (e.g. ``2024-01-01`` or ``2024-01-01 00:00:00``).
   * - ``end_date``
     - Text (Optional)
     - End date/time (e.g. ``2024-06-01``), ``now`` (for current UTC time), or ``live`` (to start real-time Dash server). Defaults to ``now``.
   * - ``timeframe``
     - Text (Required)
     - Aggregation interval (e.g. ``1D``, ``1h``, ``15m``, ``5m``, ``1m``). The minimum allowed resolution is 1 minute (``1m``). Sub-minute intervals (e.g., seconds) are not supported.

Options
-------

.. list-table::
   :widths: 25 15 20 40
   :header-rows: 1

   * - Option
     - Flag
     - Default
     - Description
   * - ``--type``
     -
     - ``ohlc``
     - Chart visual type: ``ohlc`` (candlestick chart with volume), ``line`` (close price with VWMP), or ``scatter``.
   * - ``--data-path``
     - ``-d``
     - ``~/.database``
     - Local database root path. Defaults to ``DATA_PATH`` in ``appconfig/data_config.yaml``.
   * - ``--offline`` / ``--no-download``
     -
     - ``False``
     - Read only from local cached parquets without checking remote providers.
   * - ``--config``
     - ``-c``
     - ``""``
     - Path to YAML configuration file.
   * - ``--port``
     -
     - ``8050``
     - HTTP port when running a real-time server (``end_date=live``).
   * - ``--interval``
     -
     - ``1000``
     - Real-time refresh interval in milliseconds (default: 1000 ms).

Examples
--------

**1. Historical Daily Candlestick (OHLC) Chart**:

Renders an interactive candlestick chart for EURUSD from January 2024 to June 2026:

.. code-block:: bash

   uv run python cli/fx_agg_app.py plot EURUSD 2024-01-01 2026-06-06 1D

**2. Offline Static Plot up to Current Time**:

Plots cached EURUSD daily data up to the current moment without fetching remote updates:

.. code-block:: bash

   uv run python cli/fx_agg_app.py plot EURUSD 2024-01-01 now 1D --no-download

**3. Hourly Line Chart with Custom Database Directory**:

Renders an hourly line chart displaying close prices and Volume-Weighted Mean Price (VWMP):

.. code-block:: bash

   uv run python cli/fx_agg_app.py plot EURUSD 2024-01-01 2024-06-01 1h -d ~/.test_database --type line

**4. Continuous Live Streaming Plot (Dash Web Server)**:

Starts a continuous real-time server on port 8050 that updates the chart dynamically:

.. code-block:: bash

   uv run python cli/fx_agg_app.py plot EURUSD 2024-01-01 live 1D --port 8050 --interval 1000
