#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=================================================="
echo "  Starting CLI Unit Tests"
echo "=================================================="

# 1. Check if virtual environment exists, if not, create it
if [ ! -d ".venv" ]; then
    echo "[*] Creating new Python virtual environment in .venv/"
    python3 -m venv .venv
fi

# 2. Activate the virtual environment
echo "[*] Activating virtual environment..."
source .venv/bin/activate

# 3. Install the CLI in editable mode along with testing tools
echo "[*] Installing CLI and requirements..."
pip install -e . -q
pip install pytest requests-mock -q

# 4. Run the tests
echo "--------------------------------------------------"
echo " Running Pytest..."
echo "--------------------------------------------------"
python -m pytest tests/ -v -s

echo "=================================================="
echo "  CLI Tests Completed Successfully!"
echo "=================================================="