#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -x "./.venv/Scripts/python.exe" ]; then
  PYTHON_BIN="./.venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python interpreter not found. Install Python or create ./.venv first."
  exit 1
fi

"$PYTHON_BIN" -m pytest test/test_notification_socket_interactions.py -q "$@"
