#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default to current ISO week (e.g. 2026-W13). You can override:
#   ./trigger_weekly_report_this_week.sh 2026-W13
WEEK_YEAR="${1:-$(date +%G-W%V)}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8006}"
REPORT_URL="${ORCHESTRATOR_URL}/pipeline/weekly-report"

echo "Generating weekly feedback PDF for: ${WEEK_YEAR}"
echo "Endpoint: ${REPORT_URL}"

response=$(curl -s -X POST "$REPORT_URL" \
  -H "Content-Type: application/json" \
  -d "{\"week_year\":\"${WEEK_YEAR}\"}" \
  --connect-timeout 10 \
  --max-time 180 \
  2>/dev/null) || {
  echo "[ERROR] Failed to reach orchestrator at ${REPORT_URL}"
  echo "[INFO] Make sure services are running: ./run_services.sh --status"
  exit 1
}

echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
