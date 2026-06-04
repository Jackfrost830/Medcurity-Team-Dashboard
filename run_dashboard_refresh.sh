#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
STAMP="$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[$STAMP] Starting dashboard refresh"

# Load env for Salesforce + ClickUp credentials.
set -a
source "$SCRIPT_DIR/.env"
set +a

python3 dashboard_metrics.py
python3 generate_dashboard_preview.py
python3 sync_dashboard_backend.py || true

cp dashboard_preview.html public/dashboard_preview.html
cp dashboard_team_view.html public/dashboard_team_view.html
cp goals_admin.html public/goals_admin.html
cp index.html public/index.html
cp dashboard_metrics_output.json public/dashboard_metrics_output.json

STAMP_END="$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[$STAMP_END] Refresh complete"
