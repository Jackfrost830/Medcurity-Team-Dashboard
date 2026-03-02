import json
from http.server import BaseHTTPRequestHandler

from dashboard_runtime import build_runtime_payload


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = build_runtime_payload()
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

