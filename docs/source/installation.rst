============
Installation
============

This guide will help you install the forex_data package and all its dependencies.

Prerequisites
=============

Before installing forex_data, make sure you have:

* **Python 3.8 or higher** installed on your system
* **Poetry** package manager (recommended)

.. note::
   Poetry is the recommended way to manage this package. If you don't have Poetry installed,
   visit the `Poetry documentation <https://python-poetry.org/docs/>`_ for installation instructions.

Installing Poetry
-----------------

To install Poetry, run:

.. code-block:: bash

   curl -sSL https://install.python-poetry.org | python3 -

Or on Windows PowerShell:

.. code-block:: powershell

   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

Quick Installation
==================

Using pip or Poetry (Recommended)
---------------------------------

The easiest way to install forex_data is from PyPI:

.. code-block:: bash

   pip install forex-data-aggregator

Or using Poetry:

.. code-block:: bash

   poetry add forex-data-aggregator

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

2. Install Poetry
^^^^^^^^^^^^^^^^^

Ensure you have Poetry installed:

.. code-block:: bash

   curl -sSL https://install.python-poetry.org | python3 -

Or on Windows PowerShell:

.. code-block:: powershell

   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

3. Install Dependencies
^^^^^^^^^^^^^^^^^^^^^^^

Use Poetry to install the package and all its dependencies:

.. code-block:: bash

   poetry install

This will:

* Create a virtual environment
* Install all required dependencies
* Install the package in development mode

4. Verify Installation
^^^^^^^^^^^^^^^^^^^^^^

Run the test suite to ensure everything is working correctly:

.. code-block:: bash

   poetry run pytest

If all tests pass, your installation is successful! ✅

Alternative: Using pip from Source
-----------------------------------

You can also install directly from the source directory:

.. code-block:: bash

   pip install -e .

.. warning::
   Using pip directly may not ensure all dependency versions are correctly resolved.
   Poetry is strongly recommended for development.

Installation Options
====================

Development Installation
------------------------

For development work, install with all development dependencies:

.. code-block:: bash

   poetry install --with dev

This includes additional tools for:

* Testing (pytest)
* Linting (flake8, mypy)
* Documentation (sphinx)
* Code formatting

Minimal Installation
--------------------

For a minimal installation with only required dependencies:

.. code-block:: bash

   poetry install --only main

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
