============
Installation
============

This guide will help you install the forex_data package and all its dependencies.

Prerequisites
=============

Before installing forex_data, make sure you have:

* **Python 3.13** installed on your system
* **uv** package manager (recommended)

.. note::
   `uv` is the recommended way to manage this package. If you don't have `uv` installed,
   visit the `uv documentation <https://docs.astral.sh/uv/>`_ for installation instructions.

Installing uv
-------------

To install uv, run:

.. code-block:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

Or on Windows PowerShell:

.. code-block:: powershell

   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

Quick Installation
==================

Using pip or uv (Recommended)
-----------------------------

The easiest way to install forex_data is from PyPI:

.. code-block:: bash

   uv pip install forex-data-aggregator

Or using pip:

.. code-block:: bash

   pip install forex-data-aggregator

This will install the latest stable version with all dependencies.

.. note::
   After installation, you can import the package as ``import forex_data``

Installing from Source
======================

For Development or Latest Features
-----------------------------------

If you want to contribute to development or use the latest features:

1. Clone the Repository
^^^^^^^^^^^^^^^^^^^^^^^

First, clone the forex_data repository:

.. code-block:: bash

   git clone https://github.com/nikfio/forex_data.git -b master forex-data
   cd forex-data

2. Install uv
^^^^^^^^^^^^^

Ensure you have uv installed:

.. code-block:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

3. Install Dependencies
^^^^^^^^^^^^^^^^^^^^^^^

Use uv to synchronize the environment and install all dependencies:

.. code-block:: bash

   uv sync

This will:

* Create a virtual environment at ``.venv``
* Install all required dependencies from ``uv.lock``
* Install the package in editable development mode

4. Verify Installation
^^^^^^^^^^^^^^^^^^^^^^

Run the test suite to ensure everything is working correctly:

.. code-block:: bash

   uv run pytest

If all tests pass, your installation is successful! ✅

Installation Options
====================

Development Installation
------------------------

For development work including documentation tools, install with development dependencies:

.. code-block:: bash

   uv sync --group dev

This includes additional tools for:

* Testing (pytest)
* Linting (flake8, mypy)
* Documentation (sphinx, sphinx-rtd-theme, sphinx-autodoc-typehints)
* Code formatting (autopep8)

Default Installation
--------------------

For a standard environment with core project dependencies:

.. code-block:: bash

   uv sync --no-dev

Requirements
============

The package has the following main dependencies:

* **Data Processing**: polars, pyarrow, or pandas
* **HTTP Requests**: requests
* **Logging**: loguru
* **Configuration**: PyYAML
* **Plotting**: plotly or matplotlib

See ``pyproject.toml`` for the complete list of dependencies.

Next Steps
==========

Now that you have installed forex_data, you can:

* Read the :doc:`quickstart` guide to start using the package
* Learn about :doc:`configuration` options
* Explore :doc:`examples` for common use cases

If you encounter any issues during installation, please check our
`GitHub Issues <https://github.com/nikfio/forex_data/issues>`_ page.
