#!/bin/bash

# Build and open the documentation locally

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Building documentation with Sphinx..."

# Build the documentation
# -b html: build HTML
# -E: don't use a saved environment, always read all files
# docs/source: source directory
# docs/build/html: output directory
poetry run sphinx-build -b html docs/source docs/build/html

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "----------------------------------------"
    echo "Documentation built successfully!"
    echo "Opening docs/build/html/index.html..."
    echo "----------------------------------------"
    
    # Open the documentation in the default browser (macOS)
    open docs/build/html/index.html
else
    echo "----------------------------------------"
    echo "Error: Failed to build documentation."
    echo "----------------------------------------"
    exit 1
fi
