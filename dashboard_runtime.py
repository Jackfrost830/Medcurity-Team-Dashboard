import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import RequestException

from dashboard_metrics import build_metrics, load_dotenv, parse_date, read_json
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
    pattern = rf"const {re.escape(const_name)} = .*?;"
    replacement = f"const {const_name} = {_json(value)};"
    return re.sub(pattern, replacement, html, count=1, flags=re.S)


def _replace_js_string_const(html: str, const_name: str, value: str) -> str:
    pattern = rf'const {re.escape(const_name)} = ".*?";'
    replacement = f"const {const_name} = {_json(value)};"
    return re.sub(pattern, replacement, html, count=1)


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
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")
    return read_json(CONFIG_PATH)


def current_quarter_label(data: dict[str, Any]) -> str:
    q_anchor = (data.get("meta", {}) or {}).get("quarter_anchor_date")
    parsed = parse_date(q_anchor)
    if parsed is None:
        parsed = datetime.now(timezone.utc).date()
    return quarter_label_from_date(parsed)


def build_runtime_payload() -> dict[str, Any]:
    config = load_config()
    data = build_metrics(config)
    current_quarter = current_quarter_label(data)
    generated_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

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
    arr_history = [*ARR_HISTORY_POINTS, {"quarter": current_quarter, "value": live_arr}]
    nrr_customer_history = [*NRR_CUSTOMER_HISTORY_POINTS, {"quarter": current_quarter, "value": live_nrr_customer}]
    nrr_dollar_history = [*NRR_DOLLAR_HISTORY_POINTS, {"quarter": current_quarter, "value": live_nrr_dollar}]

    history_backend = history_backend_from_env()
    history_data = history_backend.load()
    snapshot = build_history_snapshot(data, generated_at_utc)
    if snapshot_is_complete(snapshot):
        try:
            history_data = history_backend.upsert(snapshot)
        except RequestException:
            history_data = upsert_history(history_data, snapshot)

    data.setdefault("meta", {})
    data["meta"]["generated_at_utc"] = generated_at_utc
    data["meta"]["history_backend"] = history_backend.backend_name
    data["meta"]["history_persistence"] = history_backend.backend_name == "supabase" or not os.getenv("VERCEL")

    return {
        "data": data,
        "chart_data": chart_data,
        "default_goals": DEFAULT_GOALS,
        "arr_history": arr_history,
        "nrr_customer_history": nrr_customer_history,
        "nrr_dollar_history": nrr_dollar_history,
        "generated_at": generated_at_utc,
        "current_quarter": current_quarter,
        "history_data": history_data,
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
    if template_name == "goals_admin.html":
        html = _replace_js_const(html, "HISTORY_DATA", payload["history_data"])
    return html

