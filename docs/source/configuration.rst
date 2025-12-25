=============
Configuration
=============

This guide covers all configuration options for the forex_data package.

Configuration File
==================

The forex_data package uses a YAML configuration file to manage settings. By default, it looks for a file named ``appconfig.yaml`` in your project directory.

Basic Configuration File
------------------------

Create a YAML configuration file ending with ``data_config.yaml`` file with the following structure:

.. code-block:: yaml

   # Data processing engine
   ENGINE: polars
   
   # File format for cached data
   DATA_FILETYPE: parquet
   
   # API keys for real-time data providers
   PROVIDERS_KEY:
     ALPHAVANTAGE: your_alphavantage_key
     POLYGON: your_polygon_key

Configuration Options
=====================

ENGINE
------

Specifies which data processing engine to use.

**Type**: String

**Options**: 
  * ``polars`` (Recommended) - Fastest, most efficient
  * ``pyarrow`` - Very fast, columnar format
  * ``pandas`` - Slower but most compatible

**Default**: No default, must be specified

**Example**:

.. code-block:: yaml

   ENGINE: polars

**Performance Comparison**:

.. note::
   Polars is typically 5-10x faster than Pandas for operations like groupby and aggregations.
   PyArrow offers similar performance to Polars with a different API.

**When to use each**:

* **Polars**: Best choice for most use cases. Modern, fast, and memory-efficient.
* **PyArrow**: Use if you need Apache Arrow ecosystem integration.
* **Pandas**: Only if you need compatibility with existing Pandas-based code.

DATA_FILETYPE
-------------

Defines the file format for caching downloaded data.

**Type**: String

**Options**:
  * ``parquet`` (Recommended) - Compressed, fast I/O
  * ``csv`` - Human-readable, widely compatible

**Default**: No default, must be specified

**Example**:

.. code-block:: yaml

   DATA_FILETYPE: parquet

**Comparison**:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Feature
     - Parquet
     - CSV
   * - Read Speed
     - ⚡ Very Fast
     - 🐌 Slow
   * - Write Speed
     - ⚡ Very Fast
     - 🐌 Slow
   * - File Size
     - 📉 Small (compressed)
     - 📈 Large
   * - Compatibility
     - Modern tools
     - Universal
   * - Human Readable
     - ❌ No
     - ✅ Yes

**Recommendation**: Use ``parquet`` unless you specifically need CSV compatibility.

PROVIDERS_KEY
-------------

Contains API keys for real-time data providers.

**Type**: Dictionary

**Supported Providers**:
  * ``ALPHAVANTAGE`` - Alpha Vantage API key
  * ``POLYGON`` - Polygon.io API key

**Example**:

.. code-block:: yaml

   PROVIDERS_KEY:
     ALPHAVANTAGE: YOUR_API_KEY_HERE
     POLYGON: YOUR_API_KEY_HERE

Getting API Keys
^^^^^^^^^^^^^^^^

**Alpha Vantage**

1. Visit `Alpha Vantage <https://www.alphavantage.co/support/#api-key>`_
2. Enter your email and organization (can be personal)
3. Get your free API key instantly
4. Free tier: 25 requests per day

**Polygon.io**

1. Visit `Polygon.io <https://polygon.io/>`_
2. Sign up for a free account
3. Navigate to your dashboard to get your API key
4. Free tier: Limited historical data access

.. important::
   Keep your API keys secure! Don't commit them to version control.
   Consider using environment variables for production deployments.

Using Configuration in Code
============================

Automatic Loading
-----------------

The package automatically looks for ``appconfig.yaml`` in the current directory:

.. code-block:: python

   from forex_data import HistoricalManagerDB
   
   # Automatically uses configuration file ending with data_config.yaml if found
   manager = HistoricalManagerDB()

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
       config='appconfig.yaml',
       engine='polars'  # Overrides ENGINE from config file
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
   # Edit appconfig.yaml with your settings

Multiple Configuration Files
-----------------------------

For different environments (development, testing, production):

.. code-block:: python

   import os
   
   env = os.getenv('ENVIRONMENT', 'development')
   config_file = f'appconfig.{env}.yaml'
   
   manager = HistoricalManagerDB(
       config=config_file
   )

Environment Variables
---------------------

For sensitive data like API keys, consider using environment variables:

.. code-block:: python

   import os
   import yaml
   
   # Load config
   with open('appconfig.yaml', 'r') as f:
       config = yaml.safe_load(f)
   
   # Override with environment variables
   if 'ALPHAVANTAGE_KEY' in os.environ:
       config['PROVIDERS_KEY']['ALPHAVANTAGE'] = os.environ['ALPHAVANTAGE_KEY']

Configuration Best Practices
=============================

1. **Use Parquet files** for better performance and storage efficiency
2. **Use Polars engine** for fastest processing
3. **Keep API keys secure** - don't commit them to version control
4. **Document custom configurations** for team members

Example: Production Configuration
==================================

Here's a recommended production configuration:

.. code-block:: yaml

   # Production configuration for forex_data
   
   # Use Polars for best performance
   ENGINE: polars
   
   # Use Parquet for efficient storage
   DATA_FILETYPE: parquet
   
   # API keys (use environment variables in real production!)
   PROVIDERS_KEY:
     ALPHAVANTAGE: ${ALPHAVANTAGE_KEY}
     POLYGON: ${POLYGON_KEY}

Troubleshooting
===============

Configuration Not Found
-----------------------

**Error**: ``Configuration file not found``

**Solution**: Ensure ``appconfig.yaml`` is in the current working directory or provide full path:

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
