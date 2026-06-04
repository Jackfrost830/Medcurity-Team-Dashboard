import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import RequestException


@dataclass
class DashboardQuarterState:
    quarter: str
    goals: dict[str, Any]
    cs_content: dict[str, Any]
    milestones: list[dict[str, Any]]
    updated_at: str | None = None


class QuarterStateBackend:
    backend_name = "unknown"

    def load(self, quarter: str) -> DashboardQuarterState:
        return DashboardQuarterState(quarter=quarter, goals={}, cs_content={}, milestones=[])

    def save(
        self,
        quarter: str,
        goals: dict[str, Any] | None = None,
        cs_content: dict[str, Any] | None = None,
        milestones: list[dict[str, Any]] | None = None,
    ) -> DashboardQuarterState:
        return self.load(quarter)


class FileQuarterStateBackend(QuarterStateBackend):
    backend_name = "local_file"

    def __init__(self, path: Path) -> None:
        self.path = path

    def _load_store(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return {}
        return raw if isinstance(raw, dict) else {}

    def _save_store(self, store: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(store, indent=2))

    def load(self, quarter: str) -> DashboardQuarterState:
        store = self._load_store()
        row = store.get(quarter, {}) if isinstance(store.get(quarter, {}), dict) else {}
        goals = row.get("goals", {}) if isinstance(row.get("goals", {}), dict) else {}
        cs_content = row.get("cs_content", {}) if isinstance(row.get("cs_content", {}), dict) else {}
        milestones = row.get("milestones", []) if isinstance(row.get("milestones", []), list) else []
        updated_at = row.get("updated_at")
        return DashboardQuarterState(
            quarter=quarter,
            goals=goals,
            cs_content=cs_content,
            milestones=milestones,
            updated_at=updated_at if isinstance(updated_at, str) else None,
        )

    def save(
        self,
        quarter: str,
        goals: dict[str, Any] | None = None,
        cs_content: dict[str, Any] | None = None,
        milestones: list[dict[str, Any]] | None = None,
    ) -> DashboardQuarterState:
        store = self._load_store()
        current = store.get(quarter, {}) if isinstance(store.get(quarter, {}), dict) else {}
        if goals is not None:
            current["goals"] = goals
        if cs_content is not None:
            current["cs_content"] = cs_content
        if milestones is not None:
            current["milestones"] = milestones
        store[quarter] = current
        self._save_store(store)
        return self.load(quarter)


class SupabaseQuarterStateBackend(QuarterStateBackend):
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

    def load(self, quarter: str) -> DashboardQuarterState:
        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {
            "select": "quarter,goals,cs_content,milestones,updated_at",
            "quarter": f"eq.{quarter}",
            "limit": "1",
        }
        resp = requests.get(url, headers=self._headers(), params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        row = payload[0] if isinstance(payload, list) and payload else {}
        goals = row.get("goals", {}) if isinstance(row.get("goals", {}), dict) else {}
        cs_content = row.get("cs_content", {}) if isinstance(row.get("cs_content", {}), dict) else {}
        milestones = row.get("milestones", []) if isinstance(row.get("milestones", []), list) else []
        updated_at = row.get("updated_at")
        return DashboardQuarterState(
            quarter=quarter,
            goals=goals,
            cs_content=cs_content,
            milestones=milestones,
            updated_at=updated_at if isinstance(updated_at, str) else None,
        )

    def save(
        self,
        quarter: str,
        goals: dict[str, Any] | None = None,
        cs_content: dict[str, Any] | None = None,
        milestones: list[dict[str, Any]] | None = None,
    ) -> DashboardQuarterState:
        current = self.load(quarter)
        body: dict[str, Any] = {"quarter": quarter}
        body["goals"] = goals if goals is not None else current.goals
        body["cs_content"] = cs_content if cs_content is not None else current.cs_content
        body["milestones"] = milestones if milestones is not None else current.milestones

        url = f"{self.base_url}/rest/v1/{self.table}"
        params = {"on_conflict": "quarter"}
        headers = self._headers()
        headers["Prefer"] = "resolution=merge-duplicates"
        resp = requests.post(url, headers=headers, params=params, json=body, timeout=20)
        resp.raise_for_status()
        return self.load(quarter)


def quarter_state_backend_from_env(root: Path) -> QuarterStateBackend:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    table = os.getenv("SUPABASE_DASHBOARD_STATE_TABLE", "dashboard_quarter_state").strip()
    if supabase_url and service_key:
        return SupabaseQuarterStateBackend(supabase_url, service_key, table)
    return FileQuarterStateBackend(root / "dashboard_state.json")


def safe_load_quarter_state(backend: QuarterStateBackend, quarter: str) -> DashboardQuarterState:
    try:
        return backend.load(quarter)
    except RequestException:
        return DashboardQuarterState(quarter=quarter, goals={}, cs_content={}, milestones=[])

