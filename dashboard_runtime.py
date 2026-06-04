import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import RequestException

from dashboard_metrics import build_metrics, load_dotenv, parse_date, read_json
# Install Supabase CRM provider (no-op when crm.provider != "supabase").
# This monkey-patches dashboard_metrics.crm_client_from_config to
# recognize the new "supabase" provider option in dashboard_config.json.
# See Claude/README.md for full details. Roll back by setting
# crm.provider back to "salesforce" — no code change required.
import Claude.install_supabase_provider  # noqa: F401, E402
from dashboard_state_backend import quarter_state_backend_from_env, safe_load_quarter_state
from generate_dashboard_preview import (
    ARR_HISTORY_POINTS,
    DEFAULT_GOALS,
    NRR_CUSTOMER_HISTORY_POINTS,
    NRR_DOLLAR_HISTORY_POINTS,
    build_history_snapshot,
    quarter_label_from_date,
    snapshot_is_complete,
    upsert_history,
)


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / os.getenv("DASHBOARD_CONFIG_PATH", "dashboard_config.json")
HISTORY_JSON = ROOT / "dashboard_history.json"


def _json(value: Any) -> str:
    return json.dumps(value)


def _replace_js_const(html: str, const_name: str, value: Any) -> str:
    token = f"const {const_name} = "
    start = html.find(token)
    if start == -1:
        return html
    end = html.find(";\n", start)
    if end == -1:
        return html
    replacement = f"{token}{_json(value)};\n"
    return html[:start] + replacement + html[end + 2 :]


def _replace_js_string_const(html: str, const_name: str, value: str) -> str:
    token = f"const {const_name} = "
    start = html.find(token)
    if start == -1:
        return html
    end = html.find(";\n", start)
    if end == -1:
        return html
    replacement = f"{token}{_json(value)};\n"
    return html[:start] + replacement + html[end + 2 :]


def _quarter_sort_key(label: str) -> tuple[int, int]:
    m = re.match(r"^\s*Q([1-4])-(\d{4})\s*$", str(label or ""))
    if not m:
        return (0, 0)
    return (int(m.group(2)), int(m.group(1)))


def _merge_quarter_series(
    baseline_points: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    metric_key: str,
    current_quarter: str,
    current_value: float,
    max_points: int = 8,
) -> list[dict[str, Any]]:
    merged: dict[str, float] = {}

    for p in baseline_points or []:
        q = str((p or {}).get("quarter", "")).strip()
        if not q:
            continue
        try:
            merged[q] = float((p or {}).get("value", 0) or 0)
        except (TypeError, ValueError):
            continue

    # Prefer latest snapshot per quarter from history.
    latest_by_quarter: dict[str, tuple[str, float]] = {}
    for row in history_rows or []:
        if not isinstance(row, dict):
            continue
        q = str(row.get("quarter", "")).strip()
        if not q:
            continue
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        if metric_key not in metrics:
            continue
        try:
            v = float(metrics.get(metric_key, 0) or 0)
        except (TypeError, ValueError):
            continue
        snap = str(row.get("snapshot_date", "") or row.get("week_start", "") or "")
        prev = latest_by_quarter.get(q)
        if prev is None or snap > prev[0]:
            latest_by_quarter[q] = (snap, v)

    for q, (_snap, v) in latest_by_quarter.items():
        merged[q] = v

    merged[current_quarter] = float(current_value or 0)

    items = [{"quarter": q, "value": v} for q, v in merged.items()]
    items.sort(key=lambda x: _quarter_sort_key(str(x.get("quarter", ""))))
    if max_points > 0 and len(items) > max_points:
        items = items[-max_points:]
    return items


class HistoryBackend:
    backend_name = "unknown"

    def load(self) -> list[dict[str, Any]]:
        return []

    def upsert(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        return self.load()


class FileHistoryBackend(HistoryBackend):
    backend_name = "local_file"

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            payload = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return []
        return payload if isinstance(payload, list) else []

    def upsert(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        history = upsert_history(self.load(), snapshot)
        if os.getenv("VERCEL"):
            return history
        self.path.write_text(json.dumps(history, indent=2))
        return history


class SupabaseHistoryBackend(HistoryBackend):
    backend_name = "supabase"

    def __init__(self, url: str, service_key: str, table: str) -> None:
        self.base_url = url.rstrip("/")
        self.service_key = service_key
        self.table = table

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
        }

    def load(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {"select": "*", "order": "quarter.asc,week_start.asc"}
        response = requests.get(url, headers=self._headers(), params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def upsert(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {"on_conflict": "quarter,week_start"}
        headers = self._headers()
        headers["Prefer"] = "resolution=merge-duplicates"
        response = requests.post(url, headers=headers, params=params, json=snapshot, timeout=20)
        response.raise_for_status()
        return self.load()


def history_backend_from_env() -> HistoryBackend:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    table = os.getenv("SUPABASE_DASHBOARD_HISTORY_TABLE", "dashboard_history").strip()
    if supabase_url and service_key:
        return SupabaseHistoryBackend(supabase_url, service_key, table)
    return FileHistoryBackend(HISTORY_JSON)


def load_config() -> dict[str, Any]:
    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / ".env.supabase")
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")
    return read_json(CONFIG_PATH)


def _runtime_anchor_date() -> str:
    forced = str(os.getenv("DASHBOARD_RUNTIME_ANCHOR_DATE", "")).strip()
    if forced and parse_date(forced):
        return forced
    # Default behavior: current date (calendar quarter), not a pinned config date.
    return datetime.now(timezone.utc).date().isoformat()


def current_quarter_label(data: dict[str, Any]) -> str:
    q_anchor = (data.get("meta", {}) or {}).get("quarter_anchor_date")
    parsed = parse_date(q_anchor) or datetime.now(timezone.utc).date()
    return quarter_label_from_date(parsed)


def _snapshot_context() -> tuple[dict[str, Any], dict[str, Any], str, str, Any, Any]:
    config = load_config()
    config["quarter_anchor_date"] = _runtime_anchor_date()
    data = build_metrics(config)
    current_quarter = current_quarter_label(data)
    generated_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    state_backend = quarter_state_backend_from_env(ROOT)
    quarter_state = safe_load_quarter_state(state_backend, current_quarter)
    return data, config, current_quarter, generated_at_utc, state_backend, quarter_state


def lock_current_week_snapshot() -> dict[str, Any]:
    data, _config, current_quarter, generated_at_utc, state_backend, quarter_state = _snapshot_context()
    chart_data = {
        key: ((data.get("salesforce", {}) or {}).get(key, {}) or {}).get("series", [])
        for key in ["new_sales", "total_active_pipeline", "new_customers", "sql", "mql", "renewals_number"]
    }
    live_arr = (((data.get("salesforce", {}) or {}).get("arr", {}) or {}).get("value")
                or ((data.get("dashboard", {}) or {}).get("arr", {}) or {}).get("value")
                or 0)
    live_nrr_customer = (((data.get("salesforce", {}) or {}).get("nrr_customer_pct", {}) or {}).get("value")
                         or ((data.get("dashboard", {}) or {}).get("nrr_customer_pct", {}) or {}).get("value")
                         or 0)
    live_nrr_dollar = (((data.get("salesforce", {}) or {}).get("nrr_dollar_pct", {}) or {}).get("value")
                       or ((data.get("dashboard", {}) or {}).get("nrr_dollar_pct", {}) or {}).get("value")
                       or 0)
    history_backend = history_backend_from_env()
    try:
        history_rows = history_backend.load()
    except RequestException:
        history_rows = FileHistoryBackend(HISTORY_JSON).load()
    except Exception:
        history_rows = FileHistoryBackend(HISTORY_JSON).load()
    arr_history = _merge_quarter_series(
        baseline_points=ARR_HISTORY_POINTS,
        history_rows=history_rows,
        metric_key="arr",
        current_quarter=current_quarter,
        current_value=live_arr,
        max_points=8,
    )
    nrr_customer_history = _merge_quarter_series(
        baseline_points=NRR_CUSTOMER_HISTORY_POINTS,
        history_rows=history_rows,
        metric_key="nrr_customer_pct",
        current_quarter=current_quarter,
        current_value=live_nrr_customer,
        max_points=8,
    )
    nrr_dollar_history = _merge_quarter_series(
        baseline_points=NRR_DOLLAR_HISTORY_POINTS,
        history_rows=history_rows,
        metric_key="nrr_dollar_pct",
        current_quarter=current_quarter,
        current_value=live_nrr_dollar,
        max_points=8,
    )
    default_goals = json.loads(json.dumps(DEFAULT_GOALS))
    goals_store = {current_quarter: quarter_state.goals} if quarter_state.goals else {}

    snapshot = build_history_snapshot(
        data,
        generated_at_utc,
        chart_data=chart_data,
        backend_state={
            "goals_by_quarter": goals_store,
            "cs_content": quarter_state.cs_content,
            "milestones": quarter_state.milestones,
            "state_backend": state_backend.backend_name,
        },
        current_quarter=current_quarter,
        arr_history=arr_history,
        nrr_customer_history=nrr_customer_history,
        nrr_dollar_history=nrr_dollar_history,
        default_goals=default_goals,
    )
    if not snapshot_is_complete(snapshot):
        return {"ok": False, "reason": "snapshot_incomplete", "snapshot": snapshot, "quarter": current_quarter}
    history_backend.upsert(snapshot)
    return {
        "ok": True,
        "quarter": current_quarter,
        "week_start": snapshot.get("week_start"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "history_backend": history_backend.backend_name,
    }


def build_runtime_payload() -> dict[str, Any]:
    data, _config, current_quarter, generated_at_utc, state_backend, quarter_state = _snapshot_context()

    chart_data = {
        key: ((data.get("salesforce", {}) or {}).get(key, {}) or {}).get("series", [])
        for key in ["new_sales", "total_active_pipeline", "new_customers", "sql", "mql", "renewals_number"]
    }

    live_arr = (((data.get("salesforce", {}) or {}).get("arr", {}) or {}).get("value")
                or ((data.get("dashboard", {}) or {}).get("arr", {}) or {}).get("value")
                or 0)
    live_nrr_customer = (((data.get("salesforce", {}) or {}).get("nrr_customer_pct", {}) or {}).get("value")
                         or ((data.get("dashboard", {}) or {}).get("nrr_customer_pct", {}) or {}).get("value")
                         or 0)
    live_nrr_dollar = (((data.get("salesforce", {}) or {}).get("nrr_dollar_pct", {}) or {}).get("value")
                       or ((data.get("dashboard", {}) or {}).get("nrr_dollar_pct", {}) or {}).get("value")
                       or 0)
    history_backend = history_backend_from_env()
    try:
        history_data = history_backend.load()
    except RequestException:
        # Do not 500 the dashboard if Supabase is temporarily unavailable.
        history_data = FileHistoryBackend(HISTORY_JSON).load()
    except Exception:
        history_data = FileHistoryBackend(HISTORY_JSON).load()
    local_history_rows = FileHistoryBackend(HISTORY_JSON).load()
    if not history_data and history_backend.backend_name == "supabase":
        history_data = local_history_rows
    arr_history_rows = history_data
    if history_backend.backend_name == "supabase" and local_history_rows:
        # Enrich ARR quarter points from local snapshots when Supabase legacy rows are sparse.
        arr_history_rows = [*(history_data or []), *local_history_rows]
    arr_history = _merge_quarter_series(
        baseline_points=ARR_HISTORY_POINTS,
        history_rows=arr_history_rows,
        metric_key="arr",
        current_quarter=current_quarter,
        current_value=live_arr,
        max_points=8,
    )
    nrr_customer_history = _merge_quarter_series(
        baseline_points=NRR_CUSTOMER_HISTORY_POINTS,
        history_rows=arr_history_rows,
        metric_key="nrr_customer_pct",
        current_quarter=current_quarter,
        current_value=live_nrr_customer,
        max_points=5,
    )
    nrr_dollar_history = _merge_quarter_series(
        baseline_points=NRR_DOLLAR_HISTORY_POINTS,
        history_rows=arr_history_rows,
        metric_key="nrr_dollar_pct",
        current_quarter=current_quarter,
        current_value=live_nrr_dollar,
        max_points=5,
    )
    default_goals = json.loads(json.dumps(DEFAULT_GOALS))
    goals_store = {current_quarter: quarter_state.goals} if quarter_state.goals else {}
    snapshot = build_history_snapshot(
        data,
        generated_at_utc,
        chart_data=chart_data,
        backend_state={
            "goals_by_quarter": goals_store,
            "cs_content": quarter_state.cs_content,
            "milestones": quarter_state.milestones,
            "state_backend": state_backend.backend_name,
        },
        current_quarter=current_quarter,
        arr_history=arr_history,
        nrr_customer_history=nrr_customer_history,
        nrr_dollar_history=nrr_dollar_history,
        default_goals=default_goals,
    )
    allow_request_snapshot = str(os.getenv("DASHBOARD_ENABLE_REQUEST_SNAPSHOT", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
    }
    if allow_request_snapshot and snapshot_is_complete(snapshot):
        try:
            history_data = history_backend.upsert(snapshot)
        except RequestException:
            history_data = upsert_history(history_data, snapshot)

    data.setdefault("meta", {})
    data["meta"]["generated_at_utc"] = generated_at_utc
    data["meta"]["history_backend"] = history_backend.backend_name
    data["meta"]["history_persistence"] = history_backend.backend_name == "supabase" or not os.getenv("VERCEL")
    data["meta"]["state_backend"] = state_backend.backend_name
    data["meta"]["request_snapshot_enabled"] = allow_request_snapshot
    data["meta"]["history_lock_mode"] = "cron_or_manual"

    return {
        "data": data,
        "chart_data": chart_data,
        "default_goals": default_goals,
        "arr_history": arr_history,
        "nrr_customer_history": nrr_customer_history,
        "nrr_dollar_history": nrr_dollar_history,
        "generated_at": generated_at_utc,
        "current_quarter": current_quarter,
        "history_data": history_data,
        "backend_state": {
            "goals_by_quarter": goals_store,
            "cs_content": quarter_state.cs_content,
            "milestones": quarter_state.milestones,
            "state_backend": state_backend.backend_name,
        },
    }


def render_dashboard_html(template_name: str, payload: dict[str, Any]) -> str:
    html = (ROOT / template_name).read_text()
    html = _replace_js_const(html, "DATA", payload["data"])
    html = _replace_js_const(html, "CHART_DATA", payload["chart_data"])
    html = _replace_js_const(html, "DEFAULT_GOALS", payload["default_goals"])
    html = _replace_js_const(html, "ARR_HISTORY", payload["arr_history"])
    html = _replace_js_const(html, "NRR_DOLLAR_HISTORY", payload["nrr_dollar_history"])
    html = _replace_js_const(html, "NRR_CUSTOMER_HISTORY", payload["nrr_customer_history"])
    html = _replace_js_string_const(html, "GENERATED_AT", payload["generated_at"])
    html = _replace_js_string_const(html, "CURRENT_QUARTER", payload["current_quarter"])
    html = _replace_js_const(html, "BACKEND_STATE", payload.get("backend_state", {}))
    if template_name == "goals_admin.html":
        html = _replace_js_const(html, "HISTORY_DATA", payload["history_data"])
    return html
