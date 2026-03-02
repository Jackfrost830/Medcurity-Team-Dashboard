from dashboard_runtime import build_runtime_payload, render_dashboard_html


def html_response(template_name: str) -> tuple[int, dict[str, str], bytes]:
    payload = build_runtime_payload()
    html = render_dashboard_html(template_name, payload).encode("utf-8")
    headers = {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, max-age=0, must-revalidate",
    }
    return 200, headers, html

