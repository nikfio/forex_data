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

To keep your credentials secure and prevent accidentally committing secrets to version control, **API keys should be saved as system or user environment variables**.

The connectors will automatically detect and load credentials from your environment variables if not provided directly.

Supported Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* ``TWELVE_DATA_API_KEY`` - Twelve Data API key
* ``TIINGO_API_KEY`` - Tiingo API token
* ``CTRADER_CLIENT_ID`` - Spotware cTrader Open API Client ID
* ``CTRADER_CLIENT_SECRET`` - Spotware cTrader Open API Client Secret
* ``CTRADER_ACCESS_TOKEN`` - Spotware cTrader Open API Access Token
* ``CTRADER_ACCOUNT_ID`` - Spotware cTrader Account ID (cTID)

Setting Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Linux & macOS**:

.. code-block:: bash

   # Current shell session
   export TWELVE_DATA_API_KEY="your_api_key_here"
   export TIINGO_API_KEY="your_token_here"

   # Persistent across sessions (append to ~/.bashrc or ~/.zshrc)
   echo 'export TWELVE_DATA_API_KEY="your_api_key_here"' >> ~/.zshrc
   source ~/.zshrc

**Windows**:

.. code-block:: bat

   :: Command Prompt (persistent user variable)
   setx TWELVE_DATA_API_KEY "your_api_key_here"
   setx TIINGO_API_KEY "your_token_here"

   :: Command Prompt (current session)
   set TWELVE_DATA_API_KEY=your_api_key_here

.. code-block:: powershell

   # PowerShell (persistent user variable)
   [System.Environment]::SetEnvironmentVariable('TWELVE_DATA_API_KEY', 'your_api_key_here', 'User')

   # PowerShell (current session)
   $env:TWELVE_DATA_API_KEY="your_api_key_here"

Getting API Keys
^^^^^^^^^^^^^^^^

**1. Twelve Data**
  * Sign up at `Twelve Data <https://twelvedata.com/>`_
  * Get your API key from the developer dashboard
  * Free tier: 8 requests/minute, 800 credits/day

**2. Tiingo**
  * Sign up at `Tiingo <https://www.tiingo.com/>`_
  * Get your API token at `Tiingo Token <https://www.tiingo.com/account/api/token>`_
  * Free tier: 500 unique symbols/month, 20,000 requests/hour

**3. Spotware cTrader Open API**
  * Register at `cTrader Open API <https://openapi.ctrader.com/>`_
  * Create an Open API application to receive Client ID and Client Secret
  * Obtain Access Token and Trader Account ID from broker / demo account

.. important::
   Keep your API keys secure! Never commit plain API keys to version control. Always prefer system or user environment variables.

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
