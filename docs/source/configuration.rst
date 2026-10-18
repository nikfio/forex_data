=============
Configuration
=============

This guide covers all configuration options for the forex_data package.

Configuration File
==================

The forex_data package uses a YAML configuration file to manage settings. 
You can pass directly the absolute path to the YAML file or also a folder.
In the second case, it will look for the configuration file ending with `data_config.yaml` in the specified folder.
Furthermore, any parameter value can be overridden by explicit assignment in object instantion.

Basic Configuration File
------------------------

Create a YAML configuration file named ``data_config.yaml`` with the following structure:

.. code-block:: yaml

   # Path to data directory
   DATA_PATH: 'ABSOLUTE-PATH-TO-DATA-DIRECTORY'

   # Optional GPU acceleration
   DEVICE: auto
   POLARS_GPU_ENGINE: false

Configuration Options
=====================

Default Engine & File Storage Format
------------------------------------

The package internally standardizes on high-performance columnar processing and storage:

* **Engine**: ``polars_lazy`` (default and maintained engine). All queries and aggregations leverage Polars LazyFrame query plans for optimal predicate pushdown and parallel execution.
* **Storage Format**: ``parquet`` (default and maintained format). Binary Snappy-compressed Parquet files provide minimal disk usage and high-speed column scanning.

Because these are built-in defaults, ``ENGINE`` and ``DATA_TYPE`` are no longer required in configuration files.

DATA_PATH
---------

Specifies the absolute or relative directory path where the downloaded data files and local databases will be stored.

**Type**: String

**Default**: ``~/.database/`` (or ``~/.test_database`` in test environments)

**Example**:

.. code-block:: yaml

   DATA_PATH: '~/.test_database'

Real-Time Provider API Keys & Credentials
-----------------------------------------

`forex-data` integrates real-time forex market data providers defined in ``SUPPORTED_REALTIME_DATA_PROVIDERS``:

* **Twelve Data** (``twelvedata``): REST API for live quotes and intraday aggregates.
* **Tiingo** (``tiingo``): FX API for top-of-book prices and resampled candles.
* **Spotware cTrader** (``ctrader``): Open API for institutional broker tick feeds and trendbars via Protocol Buffers over TLS.

To keep your credentials secure and prevent accidentally committing secrets to version control, **API keys and credentials should be saved as system or user environment variables**.

The connectors automatically detect and load credentials from your environment variables if not provided directly in constructor calls.

Supported Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``TWELVE_DATA_API_KEY`` - Twelve Data API key
* ``TIINGO_API_KEY`` - Tiingo API token
* ``CTRADER_CLIENT_ID`` - Spotware cTrader Open API Client ID
* ``CTRADER_CLIENT_SECRET`` - Spotware cTrader Open API Client Secret
* ``CTRADER_ACCESS_TOKEN`` - Spotware cTrader Open API Access Token
* ``CTRADER_ACCOUNT_ID`` - Spotware cTrader Account ID (cTID)

.. important::
   Keep your API keys secure! Never commit plain API keys to version control. Always prefer system or user environment variables.

1. Twelve Data (``twelvedata``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Sign Up & API Key Generation**:
1. Go to `Twelve Data Registration <https://twelvedata.com/>`_ and create a free account.
2. Log in and navigate to the developer **Dashboard -> API Keys**.
3. Copy your API key (the free tier includes 800 API credits/day with 8 requests/minute).

**Credential Storage**:

* **Linux & macOS**:

  .. code-block:: bash

     # Current shell session
     export TWELVE_DATA_API_KEY="your_twelvedata_api_key"

     # Persistent across sessions (append to ~/.zshrc or ~/.bashrc)
     echo 'export TWELVE_DATA_API_KEY="your_twelvedata_api_key"' >> ~/.zshrc
     source ~/.zshrc

* **Windows**:

  .. code-block:: bat

     :: Command Prompt (persistent user variable)
     setx TWELVE_DATA_API_KEY "your_twelvedata_api_key"
     :: Command Prompt (current session)
     set TWELVE_DATA_API_KEY=your_twelvedata_api_key

  .. code-block:: powershell

     # PowerShell (persistent user variable)
     [System.Environment]::SetEnvironmentVariable('TWELVE_DATA_API_KEY', 'your_twelvedata_api_key', 'User')
     # PowerShell (current session)
     $env:TWELVE_DATA_API_KEY="your_twelvedata_api_key"

**Python Usage Example**:

.. code-block:: python

   from forex_data import TwelveDataConnector

   connector = TwelveDataConnector(plan="free")

   # Fetch live real-time price snapshot
   price_lf = connector.get_realtime_price(symbol="EUR/USD")
   print(price_lf.collect())

   # Fetch recent intraday candles (1h timeframe)
   data_lf = connector.get_data(
       symbol="EUR/USD",
       timeframe="1h",
       start_date="2026-01-05 10:00:00",
       end_date="2026-01-05 18:00:00"
   )
   print(data_lf.collect())

**Running Tests & Verification**:

.. code-block:: bash

   # Offline/mocked unit tests (default):
   uv run pytest tests/test_realtime_twelvedata_connector.py --flake8 -v

   # Live integration tests with real API data fetch:
   RUN_REALTIME_CONNECTOR_TEST=1 uv run pytest tests/test_realtime_twelvedata_connector.py --flake8 -v

.. note::
   Live data fetch tests are skipped by default to avoid requiring API keys in CI/CD pipelines. Enable them with ``RUN_REALTIME_CONNECTOR_TEST=1``. If ``TWELVE_DATA_API_KEY`` is missing or rejected when enabled, the test suite raises an actionable error with remediation hints directing you to https://twelvedata.com/.

2. Tiingo (``tiingo``)
^^^^^^^^^^^^^^^^^^^^^^

**Sign Up & API Key Generation**:
1. Go to `Tiingo Registration <https://www.tiingo.com/>`_ and create an account.
2. Visit `Tiingo Account Token <https://www.tiingo.com/account/api/token>`_ to copy your personal API token (the free tier provides access to 500 unique symbols/month and up to 20,000 requests/hour).

**Credential Storage**:

* **Linux & macOS**:

  .. code-block:: bash

     # Current shell session
     export TIINGO_API_KEY="your_tiingo_api_token"

     # Persistent across sessions
     echo 'export TIINGO_API_KEY="your_tiingo_api_token"' >> ~/.zshrc
     source ~/.zshrc

* **Windows**:

  .. code-block:: bat

     :: Command Prompt
     setx TIINGO_API_KEY "your_tiingo_api_token"
     set TIINGO_API_KEY=your_tiingo_api_token

  .. code-block:: powershell

     # PowerShell
     [System.Environment]::SetEnvironmentVariable('TIINGO_API_KEY', 'your_tiingo_api_token', 'User')
     $env:TIINGO_API_KEY="your_tiingo_api_token"

**Python Usage Example**:

.. code-block:: python

   from forex_data import TiingoConnector

   connector = TiingoConnector(plan="free")

   # Fetch top-of-book bid/ask quote
   price_lf = connector.get_realtime_price(symbol="EUR/USD")
   print(price_lf.collect())

   # Fetch resampled 5m candles
   data_lf = connector.get_data(
       symbol="EUR/USD",
       timeframe="5m",
       start_date="2026-10-01",
       end_date="2026-10-02"
   )
   print(data_lf.collect())

**Running Tests & Verification**:

.. code-block:: bash

   # Offline/mocked unit tests (default):
   uv run pytest tests/test_realtime_tiingo_connector.py --flake8 -v

   # Live integration tests with real API data fetch:
   RUN_REALTIME_CONNECTOR_TEST=1 uv run pytest tests/test_realtime_tiingo_connector.py --flake8 -v

.. note::
   Live data fetch tests are skipped by default to avoid requiring API keys in CI/CD pipelines. Enable them with ``RUN_REALTIME_CONNECTOR_TEST=1``. If ``TIINGO_API_KEY`` is missing or invalid when enabled, the test suite fails immediately with detailed remediation hints directing you to generate and export your token.

3. Spotware cTrader Open API (``ctrader``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

cTrader Open API provides direct, high-frequency access to broker market feeds via protocol buffers over TLS.

**Sign Up & Credentials Generation**:
1. Register at `Spotware Open API Portal <https://openapi.ctrader.com/>`_.
2. Create an **Open API Application** (set application type to *Desktop/Other* and redirect URI to ``http://localhost:8080/redirect``).
3. Note your **Client ID** and **Client Secret**.
4. Use the Open API playground or OAuth flow to authorize an account and obtain an **Access Token**.
5. Obtain your broker **Account ID** (cTID Trader Account ID, e.g. from your cTrader desktop/mobile client).

**Credential Storage**:

* **Linux & macOS**:

  .. code-block:: bash

     # Current shell session
     export CTRADER_CLIENT_ID="your_client_id"
     export CTRADER_CLIENT_SECRET="your_client_secret"
     export CTRADER_ACCESS_TOKEN="your_access_token"
     export CTRADER_ACCOUNT_ID="your_account_id"

     # Persistent across sessions (append to ~/.zshrc or ~/.bashrc)
     cat << 'EOF' >> ~/.zshrc
     export CTRADER_CLIENT_ID="your_client_id"
     export CTRADER_CLIENT_SECRET="your_client_secret"
     export CTRADER_ACCESS_TOKEN="your_access_token"
     export CTRADER_ACCOUNT_ID="your_account_id"
     EOF
     source ~/.zshrc

* **Windows**:

  .. code-block:: bat

     :: Command Prompt
     setx CTRADER_CLIENT_ID "your_client_id"
     setx CTRADER_CLIENT_SECRET "your_client_secret"
     setx CTRADER_ACCESS_TOKEN "your_access_token"
     setx CTRADER_ACCOUNT_ID "your_account_id"

  .. code-block:: powershell

     # PowerShell
     $env:CTRADER_CLIENT_ID="your_client_id"
     $env:CTRADER_CLIENT_SECRET="your_client_secret"
     $env:CTRADER_ACCESS_TOKEN="your_access_token"
     $env:CTRADER_ACCOUNT_ID="your_account_id"

**Python Usage Example**:

.. code-block:: python

   import os
   from forex_data import CTraderConnector

   connector = CTraderConnector(
       client_id=os.environ["CTRADER_CLIENT_ID"],
       client_secret=os.environ["CTRADER_CLIENT_SECRET"],
       access_token=os.environ["CTRADER_ACCESS_TOKEN"],
       broker_account_id=os.environ["CTRADER_ACCOUNT_ID"],
   )

   connector.connect()

   # Fetch 1-hour trendbars
   lf = connector.get_data(
       symbol="EURUSD",
       timeframe="1h",
       start_date="2026-10-01 00:00:00",
       end_date="2026-10-05 00:00:00"
   )
   df = lf.collect()
   print(df)

   connector.close()

**Running Tests & Verification**:

.. code-block:: bash

   # Protocol & mock-based tests (default):
   uv run pytest tests/test_ctrader_connector.py --flake8 -v

   # Live connection tests against demo.ctraderapi.com:
   RUN_REALTIME_CONNECTOR_TEST=1 uv run pytest tests/test_ctrader_connector.py --flake8 -v

.. note::
   Live data fetch tests are skipped by default to avoid requiring API keys in CI/CD pipelines. Enable them with ``RUN_REALTIME_CONNECTOR_TEST=1``. If credentials are missing or expired when enabled, clear error messages with renewal steps are displayed.

Using Configuration in Code
============================

Specify Configuration File folder
---------------------------------

You can specify the folder where the configuration file is located, in this case it will automatically loaded the file ending with `data_config.yaml`.

.. code-block:: python

   from forex_data import HistoricalManagerDB
   
   # Automatically uses configuration file ending with data_config.yaml if found
   manager = HistoricalManagerDB(
      config='path/to/data_config_folder/
   )

Explicit Configuration File
----------------------------

You can specify a custom configuration file path:

.. code-block:: python

   manager = HistoricalManagerDB(
       config='path/to/data_config.yaml'
   )

Overriding Configuration
-------------------------

You can override any configuration parameter in code:

.. code-block:: python

   manager = HistoricalManagerDB(
       config='path/to/data_config.yaml',
       data_path='/custom/data/path'
   )

.. note::
   Parameters passed directly to the manager constructor take precedence over 
   configuration file settings.

Advanced Configuration
======================

Template Configuration File
---------------------------

A template configuration file is available in the repository at:

   ``appconfig/appconfig_template.yaml``

You can copy this template and customize it:

.. code-block:: bash

   cp appconfig/appconfig_template.yaml appconfig.yaml
   # Edit appconfig.yaml with your custom settings

Environment Variables
---------------------

For sensitive credentials like API keys, always use environment variables rather than hardcoding them into configuration files:

.. code-block:: python

   import os
   from forex_data import TwelveDataConnector

   # TwelveDataConnector automatically detects TWELVE_DATA_API_KEY from environment:
   connector = TwelveDataConnector(
       api_key=os.environ.get("TWELVE_DATA_API_KEY")
   )

Configuration Best Practices
=============================

1. **Parquet & Polars Defaults** are automatically used for optimal speed and memory efficiency
2. **Keep API keys secure** - don't commit them to version control; save them as user/system environment variables
3. **Document custom configurations** for team members

Example: Production Configuration
==================================

Here's a recommended production configuration:

.. code-block:: yaml

   # Production configuration for forex_data
   
   # Path to data directory
   DATA_PATH: '/var/data/forex'

   # Optional GPU acceleration
   POLARS_GPU_ENGINE: false

Troubleshooting
===============

Configuration Not Found
-----------------------

**Error**: ``Configuration file not found``

**Solution**: Ensure you pass the absolute path of file ``appconfig.yaml``:

.. code-block:: python

   manager = HistoricalManagerDB(
       config='/absolute/path/to/data_config.yaml'
   )

Invalid API Key
---------------

**Error**: ``API key invalid or rate limit exceeded``

**Solutions**:

1. Check that your API key is correct
2. Verify you haven't exceeded rate limits
3. Check provider status pages



Next Steps
==========

* See :doc:`examples` for practical usage scenarios
* Check :doc:`quickstart` for basic usage patterns
* Explore the :doc:`API reference <modules>` for detailed method documentation
