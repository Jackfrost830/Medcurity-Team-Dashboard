import json
import os
from datetime import datetime
from pathlib import Path

import requests
from requests.exceptions import RequestException

from dashboard_metrics import load_dotenv, parse_date


ROOT = Path(__file__).resolve().parent
HISTORY_JSON = ROOT / "dashboard_history.json"
STATE_JSON = ROOT / "dashboard_state.json"
SERVICES_OVERRIDES_JSON = ROOT / "dashboard_services_overrides.json"


def _headers(key: str) -> dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _upsert(base_url: str, key: str, table: str, payload: list[dict] | dict, on_conflict: str) -> None:
    url = f"{base_url.rstrip('/')}/rest/v1/{table}"
    headers = _headers(key)
    headers["Prefer"] = "resolution=merge-duplicates"
    params = {"on_conflict": on_conflict}
    resp = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
    resp.raise_for_status()


def main() -> None:
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / ".env.supabase")
    base_url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base_url or not key:
        print("Supabase not configured, skipping backend sync.")
        return

    history_table = os.getenv("SUPABASE_DASHBOARD_HISTORY_TABLE", "dashboard_history").strip()
    state_table = os.getenv("SUPABASE_DASHBOARD_STATE_TABLE", "dashboard_quarter_state").strip()
    services_overrides_table = os.getenv("SUPABASE_DASHBOARD_SERVICE_OVERRIDES_TABLE", "dashboard_service_overrides").strip()

    if HISTORY_JSON.exists():
        rows = json.loads(HISTORY_JSON.read_text())
        if isinstance(rows, list) and rows:
            payload = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                payload.append(
                    {
                        "quarter": row.get("quarter"),
                        "week_start": row.get("week_start"),
                        "quarter_anchor_date": row.get("quarter_anchor_date"),
                        "snapshot_date": row.get("snapshot_date"),
                        "generated_at_utc": row.get("generated_at_utc"),
                        "metrics": row.get("metrics") if isinstance(row.get("metrics"), dict) else {},
                    }
                )
            if payload:
                _upsert(base_url, key, history_table, payload, "quarter,week_start")
                print(f"Synced {len(payload)} history rows -> {history_table}")

    if STATE_JSON.exists():
        raw = json.loads(STATE_JSON.read_text())
        if isinstance(raw, dict) and raw:
            payload = []
            for quarter, row in raw.items():
                if not isinstance(row, dict):
                    continue
                payload.append(
                    {
                        "quarter": quarter,
                        "goals": row.get("goals") if isinstance(row.get("goals"), dict) else {},
                        "cs_content": row.get("cs_content") if isinstance(row.get("cs_content"), dict) else {},
                        "milestones": row.get("milestones") if isinstance(row.get("milestones"), list) else [],
                    }
                )
            if payload:
                _upsert(base_url, key, state_table, payload, "quarter")
                print(f"Synced {len(payload)} quarter state rows -> {state_table}")
    else:
        # Seed current quarter with default empty state so admin save has a stable row.
        q_anchor = parse_date((json.loads((ROOT / "dashboard_metrics_output.json").read_text()).get("meta", {}) or {}).get("quarter_anchor_date"))
        if q_anchor is not None:
            q = f"Q{((q_anchor.month - 1) // 3) + 1}-{q_anchor.year}"
            row = {
                "quarter": q,
                "goals": {},
                "cs_content": {},
                "milestones": [],
            }
            try:
                _upsert(base_url, key, state_table, row, "quarter")
                print(f"Seeded quarter state row for {q}")
            except RequestException:
                pass

    if SERVICES_OVERRIDES_JSON.exists():
        raw = json.loads(SERVICES_OVERRIDES_JSON.read_text())
        if isinstance(raw, dict) and raw:
            payload = []
            for quarter, row in raw.items():
                if not isinstance(row, dict):
                    continue
                payload.append(
                    {
                        "quarter": quarter,
                        "closed_projects_this_quarter": int(row.get("closed_projects_this_quarter"))
                        if row.get("closed_projects_this_quarter") is not None
                        else None,
                        "avg_project_close_days_this_quarter": float(row.get("avg_project_close_days_this_quarter"))
                        if row.get("avg_project_close_days_this_quarter") is not None
                        else None,
                        "notes": row.get("notes"),
                    }
                )
            if payload:
                _upsert(base_url, key, services_overrides_table, payload, "quarter")
                print(f"Synced {len(payload)} service override rows -> {services_overrides_table}")

    # Log a refresh run marker.
    run_table = "dashboard_refresh_runs"
    run_payload = {
        "started_at": datetime.utcnow().isoformat() + "Z",
        "completed_at": datetime.utcnow().isoformat() + "Z",
        "status": "ok",
        "message": "sync_dashboard_backend.py completed",
        "context": {},
    }
    try:
        _upsert(base_url, key, run_table, run_payload, "id")
    except RequestException:
        pass


if __name__ == "__main__":
    main()
