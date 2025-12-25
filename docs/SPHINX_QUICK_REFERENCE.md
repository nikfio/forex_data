# Sphinx Quick Reference Card

## Common reStructuredText Directives

### Admonitions (Colored Boxes)

```rst
.. note::
   This is a note - general information

.. tip::
   This is a helpful tip

.. warning::
   This is a warning - be careful!

.. important::
   This is important information

.. seealso::
   See also: :doc:`related-doc`

.. danger::
   This is dangerous - critical warning!

.. hint::
   Here's a hint to help you
```

### Code Blocks

```rst
Basic code block:

.. code-block:: python

   def example():
       return "Hello World"

With line numbers:

.. code-block:: python
   :linenos:

   def example():
       return "Hello"

With emphasized lines:

.. code-block:: python
   :emphasize-lines: 2,4

   def example():
       x = 42  # highlighted
       y = 10
       return x + y  # highlighted

With caption:

.. code-block:: python
   :caption: example.py

   print("Hello")
```

### Links and Cross-References

```rst
Link to another RST file:
:doc:`installation`

Link with custom text:
:doc:`Install Guide <installation>`

Link to a section in current document:
:ref:`my-section-label`

Link to a Python class:
:class:`forex_data.HistoricalManager`

Link to a Python method:
:meth:`forex_data.HistoricalManager.get_data`

Link to a Python function:
:func:`forex_data.utils.process_data`

Link to a Python module:
:mod:`forex_data.data_management`

External link:
`Python <https://python.org>`_
```

### Section References

```rst
Create a label above a section:

.. _my-custom-label:

Section Title
=============

Reference it elsewhere:
See :ref:`my-custom-label` for details.
```

### Tables

```rst
Simple table:

.. list-table:: Title
   :header-rows: 1
   :widths: 25 25 50

   * - Column 1
     - Column 2
     - Column 3
   * - Row 1, Col 1
     - Row 1, Col 2
     - Row 1, Col 3
   * - Row 2, Col 1
     - Row 2, Col 2
     - Row 2, Col 3

CSV table:

.. csv-table:: Title
   :header: "Name", "Age", "City"
   :widths: 30, 10, 30

   "Alice", "30", "NYC"
   "Bob", "25", "LA"
```

### Images and Figures

```rst
Simple image:

.. image:: _static/logo.png
   :alt: Alternative text
   :width: 200px

Figure with caption:

.. figure:: _static/diagram.png
   :alt: System diagram
   :width: 600px
   :align: center

   This is the caption for the figure.
```

### Lists

```rst
Bullet list:

* Item 1
* Item 2
  
  * Nested item 2.1
  * Nested item 2.2

* Item 3

Numbered list:

1. First item
2. Second item
3. Third item

Definition list:

Term 1
    Definition of term 1

Term 2
    Definition of term 2
```

### Table of Contents

```rst
Inline TOC for current page:

.. contents:: On this page
   :local:
   :depth: 2

TOC tree for multiple pages:

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   configuration
```

### Code Documentation

```rst
Document a module:

.. automodule:: forex_data.data_management
   :members:
   :undoc-members:
   :show-inheritance:

Document a class:

.. autoclass:: forex_data.HistoricalManager
   :members:
   :special-members: __init__
   :show-inheritance:

Document a function:

.. autofunction:: forex_data.utils.process_data
```

### Text Formatting

```rst
*italic text*

**bold text**

``inline code``

`external link <https://example.com>`_

:kbd:`Ctrl+C`  (keyboard shortcut)

:file:`/path/to/file.py`  (file path)

:command:`git commit`  (command)
```

### Nested Content

```rst
.. note::
   
   This note contains:
   
   * A bullet list
   * With multiple items
   
   .. code-block:: python
   
      # And even code!
      print("Hello")
   
   And more text after the code.
```

### Version Information

```rst
.. versionadded:: 0.2.0
   This feature was added in version 0.2.0

.. versionchanged:: 0.3.0
   Behavior changed in version 0.3.0

.. deprecated:: 0.4.0
   This feature is deprecated as of 0.4.0
```

### Custom Containers

```rst
.. container:: custom-class

   Content inside a custom container.
   You can style this with CSS.
```

### Raw HTML (use sparingly)

```rst
.. raw:: html

   <div style="color: red;">
       This is raw HTML
   </div>
```

## Section Heading Levels

```rst
# with overline, for parts
* with overline, for chapters
=============  (for titles/h1)
-------------  (for sections/h2)
^^^^^^^^^^^^^  (for subsections/h3)
"""""""""""""  (for subsubsections/h4)
```

Example:

```rst
Main Title
==========

Section
-------

Subsection
^^^^^^^^^^

Subsubsection
"""""""""""""
```

## Python Docstring Example (Google Style)

```python
def get_data(self, timeframe: str, start: str, end: str) -> DataFrame:
    """Retrieve OHLC data for the specified period.
    
    Longer description here with more details about what this
    function does and how it works.
    
    Args:
        timeframe: Candle timeframe (e.g., '1h', '1D')
        start: Start date in ISO format
        end: End date in ISO format
    
    Returns:
        DataFrame with OHLC data containing columns:
        timestamp, open, high, low, close
    
    Raises:
        ValueError: If timeframe is invalid
        FileNotFoundError: If data files not found
    
    Example:
        >>> manager = HistoricalManager(ticker='EURUSD')
        >>> data = manager.get_data('1h', '2020-01-01', '2020-01-31')
        >>> len(data)
        744
    
    Note:
        First call may be slower as it builds cache.
    
    See Also:
        add_timeframe: Add custom timeframes
        plot: Visualize the data
    """
    pass
```

## Useful Sphinx Roles

```rst
:doc:`document`           - Link to document
:ref:`label`             - Link to section
:class:`ClassName`       - Link to class
:func:`function_name`    - Link to function
:meth:`method_name`      - Link to method
:mod:`module_name`       - Link to module
:attr:`attribute`        - Link to attribute
:exc:`Exception`         - Link to exception
:data:`DATA_CONSTANT`    - Link to data/constant
:term:`glossary-term`    - Link to glossary term
:download:`file.zip`     - Download link
:file:`path/to/file`     - File path
:command:`command`       - Command
:kbd:`Ctrl+C`           - Keyboard shortcut
:option:`--option`      - Command option
```

## Building Documentation

```bash
# Build HTML
cd docs
poetry run sphinx-build -b html source build/html

# Build with clean (removes old files)
poetry run sphinx-build -b html source build/html -E

# Auto-rebuild on changes (requires sphinx-autobuild)
poetry run sphinx-autobuild source build/html

# Build PDF
poetry run sphinx-build -b latex source build/latex
cd build/latex && make

# Check for broken links
poetry run sphinx-build -b linkcheck source build/linkcheck
```

## Common Patterns

### Feature Comparison Table

```rst
.. list-table:: Engine Comparison
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - Feature
     - Polars
     - PyArrow
     - Pandas
     - Notes
   * - Speed
     - ⚡⚡⚡
     - ⚡⚡⚡
     - ⚡
     - Polars fastest
   * - Memory
     - ✅ Low
     - ✅ Low
     - ⚠️ High
     - Arrow formats efficient
```

### Step-by-Step Instructions

```rst
Installation Steps
==================

1. **Clone the repository**

   .. code-block:: bash

      git clone https://github.com/user/repo.git
      cd repo

2. **Install dependencies**

   .. code-block:: bash

      poetry install

3. **Run tests**

   .. code-block:: bash

      poetry run pytest
```

### API Pattern with Examples

```rst
get_data()
----------

Retrieve historical data.

**Signature**:

.. code-block:: python

   def get_data(timeframe: str, start: str, end: str) -> DataFrame

**Parameters**:

* ``timeframe`` (str): Candle timeframe
* ``start`` (str): Start date
* ``end`` (str): End date

**Returns**: DataFrame with OHLC data

**Example**:

.. code-block:: python

   data = manager.get_data('1h', '2020-01-01', '2020-01-31')
```

## Tips

1. **Always use blank lines** before and after directives
2. **Indent content** under directives with 3 spaces
3. **Test your builds** frequently to catch errors early
4. **Use labels** for sections you want to reference
5. **Keep lines under 100 characters** for readability
6. **Preview in browser** to see final formatting
7. **Check warnings** - they often indicate broken links or formatting issues

## Resources

- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [reStructuredText Primer](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)
- [Sphinx Directives](https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html)
- [Read the Docs Theme](https://sphinx-rtd-theme.readthedocs.io/)
