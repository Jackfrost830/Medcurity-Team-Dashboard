import json
import hashlib
import os
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from dashboard_metrics import load_dotenv
from dashboard_state_backend import quarter_state_backend_from_env, safe_load_quarter_state


ROOT = Path(__file__).resolve().parent.parent


class handler(BaseHTTPRequestHandler):
    def _key_fp(self) -> dict:
        key = str(os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")).strip()
        if not key:
            return {"present": False}
        return {"present": True, "len": len(key), "sha12": hashlib.sha256(key.encode()).hexdigest()[:12]}

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        load_dotenv(ROOT / ".env")
        load_dotenv(ROOT / ".env.supabase")
        quarter = self._quarter_from_path() or ""
        if not quarter:
            self._json(400, {"ok": False, "error": "Missing quarter query param"})
            return
        backend = quarter_state_backend_from_env(ROOT)
        state = safe_load_quarter_state(backend, quarter)
        self._json(
            200,
            {
                "ok": True,
                "quarter": quarter,
                "backend": backend.backend_name,
                "goals": state.goals,
                "cs_content": state.cs_content,
                "milestones": state.milestones,
                "updated_at": state.updated_at,
            },
        )

    def do_POST(self) -> None:
        load_dotenv(ROOT / ".env")
        load_dotenv(ROOT / ".env.supabase")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._json(400, {"ok": False, "error": "Invalid JSON"})
            return

        quarter = str(payload.get("quarter", "")).strip()
        if not quarter:
            self._json(400, {"ok": False, "error": "quarter is required"})
            return

        goals = payload.get("goals")
        cs_content = payload.get("cs_content")
        milestones = payload.get("milestones")

        if goals is not None and not isinstance(goals, dict):
            self._json(400, {"ok": False, "error": "goals must be an object"})
            return
        if cs_content is not None and not isinstance(cs_content, dict):
            self._json(400, {"ok": False, "error": "cs_content must be an object"})
            return
        if milestones is not None and not isinstance(milestones, list):
            self._json(400, {"ok": False, "error": "milestones must be an array"})
            return

        backend = quarter_state_backend_from_env(ROOT)
        try:
            state = backend.save(
                quarter=quarter,
                goals=goals,
                cs_content=cs_content,
                milestones=milestones,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            hint = None
            if "401" in msg and "supabase.co/rest/v1" in msg:
                hint = "Check Vercel SUPABASE_SERVICE_ROLE_KEY; it must be the service_role key for this Supabase project."
            self._json(500, {"ok": False, "error": msg, "hint": hint, "key_fp": self._key_fp()})
            return

        self._json(
            200,
            {
                "ok": True,
                "quarter": quarter,
                "backend": backend.backend_name,
                "goals": state.goals,
                "cs_content": state.cs_content,
                "milestones": state.milestones,
                "updated_at": state.updated_at,
            },
        )

    def _quarter_from_path(self) -> str | None:
        # e.g. /api/dashboard_state?quarter=Q1-2026
        path = self.path or ""
        if "?" not in path:
            return None
        query = path.split("?", 1)[1]
        for part in query.split("&"):
            if not part:
                continue
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k == "quarter":
                return v
        return None
