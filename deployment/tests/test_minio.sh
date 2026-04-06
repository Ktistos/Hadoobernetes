#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/../../.venv/bin/python3"

$VENV_PYTHON "$SCRIPT_DIR/lib/test_minio.py"
