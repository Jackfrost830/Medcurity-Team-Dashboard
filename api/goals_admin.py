from http.server import BaseHTTPRequestHandler

from api._dashboard_response import html_response


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        status, headers, body = html_response("goals_admin.html")
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)
