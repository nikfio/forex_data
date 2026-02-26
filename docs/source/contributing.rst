============
Contributing
============

Thank you for your interest in contributing to forex_data! This guide will help you get started.

Getting Started
===============

Development Setup
-----------------

1. **Fork the repository** on GitHub

2. **Clone your fork**:

   .. code-block:: bash

      git clone https://github.com/YOUR_USERNAME/forex_data.git
      cd forex_data

3. **Install Poetry** if you haven't already:

   .. code-block:: bash

      curl -sSL https://install.python-poetry.org | python3 -

4. **Install dependencies**:

   .. code-block:: bash

      poetry install --with dev

5. **Create a branch** for your changes:

   .. code-block:: bash

      git checkout -b feature/your-feature-name

Code Standards
==============

Python Style Guide
------------------

This project follows PEP 8 style guidelines with some modifications:

* Use **type hints** for all function parameters and return values
* Maximum line length: **88 characters** (Black formatter default)
* Use **single quotes** for strings unless double quotes avoid escaping
* Docstrings should follow **Google style**

Linting and Formatting
----------------------

The project uses several tools to maintain code quality:

**Flake8** - Linting:

.. code-block:: bash

   poetry run flake8 forex_data/

**MyPy** - Type checking:

.. code-block:: bash

   poetry run mypy forex_data/

Configuration files are available:

* ``setup.cfg`` - Flake8 configuration
* ``mypy.ini`` - Standard MyPy configuration
* ``mypy_relaxed.ini`` - Relaxed settings for initial development

Running all checks:

.. code-block:: bash

   poetry run flake8 forex_data/
   poetry run mypy --config-file=mypy.ini forex_data/

Testing
=======

Writing Tests
-------------

* All new features should include tests
* Tests should be placed in the ``tests/`` directory
* Use descriptive test names that explain what is being tested
* Follow the existing test structure

Running Tests
-------------

Run the full test suite:

.. code-block:: bash

   poetry run pytest

Run specific test files:

.. code-block:: bash

   poetry run pytest tests/test_hist_data_manager.py

Run with coverage:

.. code-block:: bash

   poetry run pytest --cov=forex_data --cov-report=html

Test with different engines:

.. code-block:: python

   # Configure in appconfig.yaml
   ENGINE: polars  # Test with Polars
   ENGINE: pandas  # Test with Pandas
   ENGINE: pyarrow # Test with PyArrow

Continuous Integration
----------------------

The project uses CircleCI for continuous integration. All PRs must:

* ✅ Pass all tests
* ✅ Pass linting checks
* ✅ Pass type checking
* ✅ Maintain or improve code coverage

See ``.circleci/config.yml`` for CI configuration.

Documentation
=============

Building Documentation
----------------------

Documentation is built using Sphinx. To build locally:

.. code-block:: bash

   cd docs
   poetry run sphinx-build -b html source build/html

View the built documentation:

.. code-block:: bash

   open build/html/index.html  # macOS
   xdg-open build/html/index.html  # Linux
   start build/html/index.html  # Windows

Docstring Format
----------------

Use Google-style docstrings:

.. code-block:: python

   def get_data(self, timeframe: str, start: str, end: str) -> DataFrame:
       """Retrieve OHLC data for specified timeframe and date range.
       
       Args:
           timeframe: The timeframe for candles (e.g., '1h', '1D', '1W')
           start: Start date in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
           end: End date in format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
       
       Returns:
           DataFrame containing OHLC data with columns: timestamp, open, 
           high, low, close
       
       Raises:
           ValueError: If timeframe is not supported
           FileNotFoundError: If data files are not found
       
       Example:
       >>> manager = HistoricalManagerDB(config='data_config.yaml')
       >>> data = manager.get_data(ticker='EURUSD', timeframe='1h', start='2020-01-01', end='2020-01-31')
       """
       pass

Adding Documentation Pages
--------------------------

To add a new documentation page:

1. Create a ``.rst`` file in ``docs/source/``
2. Add the file to the appropriate ``toctree`` in ``index.rst``
3. Build and verify the documentation

Contribution Workflow
=====================

Making Changes
--------------

1. **Make your changes** in your feature branch

2. **Add tests** for new functionality

3. **Update documentation** if needed

4. **Run tests and linting**:

   .. code-block:: bash

      poetry run pytest
      poetry run flake8 forex_data/
      poetry run mypy forex_data/

5. **Commit your changes**:

   .. code-block:: bash

      git add .
      git commit -m "Add feature: brief description"

   Use clear, descriptive commit messages:
   
   * ``Add feature: support for new data provider``
   * ``Fix bug: incorrect timeframe conversion``
   * ``Docs: update installation instructions``
   * ``Test: add tests for real-time manager``

6. **Push to your fork**:

   .. code-block:: bash

      git push origin feature/your-feature-name

Submitting a Pull Request
--------------------------

1. Go to the `forex_data repository <https://github.com/nikfio/forex_data>`_

2. Click "New Pull Request"

3. Select your fork and branch

4. Fill in the PR template with:
   
   * **Description** of changes
   * **Motivation** for the changes
   * **Testing** performed
   * Related **issue numbers** (if applicable)

5. Submit the PR

Your PR will be reviewed and may receive feedback for changes before merging.

What to Contribute
==================

We welcome contributions in many areas:

Features
--------

* 🔌 New data source integrations
* 📊 Additional timeframe support
* 🎨 Enhanced plotting capabilities
* ⚡ Performance optimizations
* 🗄️ Database backends (PostgreSQL, DuckDB, etc.)

Bug Fixes
---------

* 🐛 Fix reported issues
* 🔧 Improve error handling
* 📝 Correct documentation errors

Documentation
-------------

* 📚 Improve existing docs
* ✍️ Add examples and tutorials
* 🌍 Translations (future)

Tests
-----

* ✅ Improve test coverage
* 🧪 Add edge case tests
* 🎯 Integration tests

Development Priorities
======================

Current Focus Areas
-------------------

Based on the project roadmap, priority areas include:

1. **Database Integration**
   
   * DuckDB integration
   * Cache data location in cloud services persistent storage (S3, Google Cloud Storage, etc.)
   
2. **Enhanced Real-time Manager**
   
   * Allow Real-Time manager to cache data as in Historical Manager (and sharing cache folder/location)
   * Interface for Twelve Data provider
   * Automatic failover between providers
   * Smart API call distribution
  
3. **Performance**
   
   * Benchmark additions
   * Optimization opportunities
   * Memory efficiency

See ``README.md`` under "Future developments" for more details.

Code Review Process
===================

What Reviewers Look For
-----------------------

When reviewing your PR, maintainers will check:

* ✅ **Code quality**: Follows style guide, well-structured
* ✅ **Tests**: Adequate test coverage, tests pass
* ✅ **Documentation**: Changes are documented
* ✅ **Type hints**: Proper type annotations
* ✅ **Performance**: No performance regressions
* ✅ **Compatibility**: Works with all engines (Polars, Pandas, PyArrow)

Getting Help
============

Need help with your contribution?

* 💬 Open a discussion on GitHub Discussions (if available)
* 🐛 Reference related issues
* 📧 Contact maintainers through GitHub

Community Guidelines
====================

Be Respectful
-------------

* Treat everyone with respect
* Welcome newcomers
* Be patient and helpful
* Focus on constructive feedback

Code of Conduct
---------------

This project adheres to professional standards:

* Use welcoming and inclusive language
* Respect differing viewpoints
* Accept constructive criticism gracefully
* Focus on what's best for the community

License
=======

By contributing to forex_data, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).

Recognition
===========

Contributors will be:

* Listed in the project contributors
* Acknowledged in release notes for significant contributions
* Appreciated for making forex_data better! 🎉

Thank You!
==========

Every contribution, no matter how small, makes forex_data better. We appreciate your time and effort in improving this project!

.. note::
   Questions about contributing? Open an issue with the "question" label!
