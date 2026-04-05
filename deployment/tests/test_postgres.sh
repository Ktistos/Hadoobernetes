#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/../../.venv/bin/python3"

kubectl port-forward -n mapreduce svc/postgres 5432:5432 &
PF_PID=$!

cleanup() {
    kill $PF_PID 2>/dev/null
}
trap cleanup EXIT

sleep 2
$VENV_PYTHON "$SCRIPT_DIR/lib/test_postgres.py"
