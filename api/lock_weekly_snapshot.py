import json
import hashlib
import os
from http.server import BaseHTTPRequestHandler

from dashboard_metrics import load_dotenv
from dashboard_runtime import ROOT, lock_current_week_snapshot


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

    def _authorized(self) -> bool:
        secret = str(os.getenv("CRON_SECRET", "")).strip()
        manual = str(os.getenv("SNAPSHOT_LOCK_TOKEN", "")).strip()
        if not secret and not manual:
            return True
        auth = str(self.headers.get("Authorization", "")).strip()
        if secret and auth == f"Bearer {secret}":
            return True
        if manual and auth == f"Bearer {manual}":
            return True
        return False

    def _run(self) -> None:
        load_dotenv(ROOT / ".env")
        load_dotenv(ROOT / ".env.supabase")
        if not self._authorized():
            self._json(401, {"ok": False, "error": "Unauthorized"})
            return
        try:
            result = lock_current_week_snapshot()
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            hint = None
            if "401" in msg and "supabase.co/rest/v1" in msg:
                hint = "Check Vercel SUPABASE_SERVICE_ROLE_KEY; it must be the project service_role key (not publishable/anon)."
            self._json(500, {"ok": False, "error": msg, "hint": hint, "key_fp": self._key_fp()})
            return
        status = 200 if result.get("ok") else 409
        self._json(status, result)

    def do_GET(self) -> None:
        self._run()

    def do_POST(self) -> None:
        self._run()
