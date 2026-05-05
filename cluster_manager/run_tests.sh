#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=================================================="
echo "  Starting Cluster Manager Unit Tests"
echo "=================================================="

# 1. Check if virtual environment exists, if not, create it
if [ ! -d ".venv" ]; then
    echo "[*] Creating new Python virtual environment in .venv/"
    python3 -m venv .venv
fi

# 2. Activate the virtual environment
echo "[*] Activating virtual environment..."
source .venv/bin/activate

# 3. Install dependencies
echo "[*] Installing requirements..."
pip install -r requirements.txt -q
pip install pytest pytest-asyncio httpx -q

# 4. Run the tests
echo "--------------------------------------------------"
echo " Running Pytest..."
echo "--------------------------------------------------"
# -v for verbose, -s to allow print statements to output to console
python -m pytest tests/ -v -s

echo "=================================================="
echo "  Tests Completed Successfully!"
echo "=================================================="