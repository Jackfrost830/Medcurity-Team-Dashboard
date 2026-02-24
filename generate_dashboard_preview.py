import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

INPUT = Path("dashboard_metrics_output.json")
DASHBOARD_OUTPUT = Path("dashboard_preview.html")
TEAM_OUTPUT = Path("dashboard_team_view.html")
INDEX_OUTPUT = Path("index.html")
GOALS_OUTPUT = Path("goals_admin.html")
GOALS_JSON = Path("dashboard_goals.json")
HISTORY_JSON = Path("dashboard_history.json")


DEFAULT_GOALS = {
    "new_sales": {"quarter_goal": 36000, "month_goals": [None, None, None]},
    "total_active_pipeline": {"quarter_goal": 800000, "month_goals": [800000, 800000, 800000]},
    "new_customers": {"quarter_goal": 24, "month_goals": [None, None, None]},
    "arr": {"quarter_goal": 1100000, "month_goals": [None, None, None]},
    "sql": {"quarter_goal": 15, "month_goals": [None, None, None]},
    "mql": {"quarter_goal": 75, "month_goals": [None, None, None]},
    "renewals_number": {"quarter_goal": 150000, "month_goals": [None, None, None]},
    "nrr_customer_pct": {"quarter_goal": 0.90, "month_goals": [None, None, None]},
    "nrr_dollar_pct": {"quarter_goal": 0.90, "month_goals": [None, None, None]},
    "qtd_billing_progress": {"quarter_goal": 350000, "month_goals": [None, None, None]},
}

# Historical rolling-365 ARR quarter endpoints from financial workbook Summary row 23.
# Hardcoded for now per request; current quarter is appended from live Salesforce ARR.
ARR_HISTORY_POINTS = [
    {"quarter": "Q2-2024", "value": 920441.25},
    {"quarter": "Q3-2024", "value": 941425.5},
    {"quarter": "Q4-2024", "value": 957989.0},
    {"quarter": "Q1-2025", "value": 973958.55005},
    {"quarter": "Q2-2025", "value": 991001.15005},
    {"quarter": "Q3-2025", "value": 1003099.65005},
    {"quarter": "Q4-2025", "value": 1005173.65088},
]

# Historical quarterly NRR from spreadsheet-derived values.
# Dollar NRR = 1 - row18; Customer NRR = 1 - row20.
NRR_DOLLAR_HISTORY_POINTS = [
    {"quarter": "Q1-2025", "value": 0.8792408210768073},
    {"quarter": "Q2-2025", "value": 0.8413659273760765},
    {"quarter": "Q3-2025", "value": 0.9333100067635891},
    {"quarter": "Q4-2025", "value": 0.8400561472863652},
]
NRR_CUSTOMER_HISTORY_POINTS = [
    {"quarter": "Q1-2025", "value": 0.8620689655172413},
    {"quarter": "Q2-2025", "value": 0.7441860465116279},
    {"quarter": "Q3-2025", "value": 0.9130434782608696},
    {"quarter": "Q4-2025", "value": 0.8771929824561404},
]


def js(obj: dict) -> str:
    return json.dumps(obj)


def quarter_label_from_date(d: date) -> str:
    q = ((d.month - 1) // 3) + 1
    return f"Q{q}-{d.year}"


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def week_start_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def metric_value(data: dict, key: str) -> float | None:
    dash = data.get("dashboard", {}) if isinstance(data.get("dashboard"), dict) else {}
    sf = data.get("salesforce", {}) if isinstance(data.get("salesforce"), dict) else {}
    if isinstance(dash.get(key), dict) and "value" in dash[key]:
        try:
            return float(dash[key]["value"])
        except (TypeError, ValueError):
            return None
    metric = sf.get(key, {})
    if isinstance(metric, dict):
        source = metric.get("snapshot_value", metric.get("qtd_total", metric.get("value")))
        try:
            return float(source)
        except (TypeError, ValueError):
            return None
    return None


def build_history_snapshot(data: dict, generated_at_utc: str) -> dict:
    meta = data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
    quarter_anchor = parse_iso_date(str(meta.get("quarter_anchor_date", "")).strip()) or date.today()
    snapshot_date = datetime.now(timezone.utc).date()
    services = data.get("services", {}) if isinstance(data.get("services"), dict) else {}
    metrics = {
        "arr": metric_value(data, "arr"),
        "nrr_customer_pct": metric_value(data, "nrr_customer_pct"),
        "nrr_dollar_pct": metric_value(data, "nrr_dollar_pct"),
        "renewals_number": metric_value(data, "renewals_number"),
        "new_sales": metric_value(data, "new_sales"),
        "total_active_pipeline": metric_value(data, "total_active_pipeline"),
        "new_customers": metric_value(data, "new_customers"),
        "lost_customers": metric_value(data, "lost_customers"),
        "sql": metric_value(data, "sql"),
        "mql": metric_value(data, "mql"),
        "services_active_projects": float(services.get("active_projects", 0) or 0),
        "services_closed_projects_this_quarter": float(services.get("closed_projects_this_quarter", 0) or 0),
        "services_avg_project_close_days_this_quarter": float(services.get("avg_project_close_days_this_quarter", 0) or 0),
    }
    return {
        "quarter": quarter_label_from_date(quarter_anchor),
        "quarter_anchor_date": quarter_anchor.isoformat(),
        "snapshot_date": snapshot_date.isoformat(),
        "week_start": week_start_monday(snapshot_date).isoformat(),
        "generated_at_utc": generated_at_utc,
        "metrics": metrics,
    }


def snapshot_is_complete(snapshot: dict) -> bool:
    metrics = snapshot.get("metrics", {}) if isinstance(snapshot.get("metrics"), dict) else {}
    required = [
        "arr",
        "nrr_customer_pct",
        "nrr_dollar_pct",
        "renewals_number",
        "new_sales",
        "total_active_pipeline",
        "new_customers",
        "sql",
        "mql",
    ]
    return all(metrics.get(k) is not None for k in required)


def upsert_history(history_rows: list[dict], snapshot: dict) -> list[dict]:
    key = (snapshot.get("quarter"), snapshot.get("week_start"))
    out: list[dict] = []
    replaced = False
    for row in history_rows:
        row_key = (row.get("quarter"), row.get("week_start"))
        if row_key == key:
            out.append(snapshot)
            replaced = True
        else:
            out.append(row)
    if not replaced:
        out.append(snapshot)
    out.sort(key=lambda r: (str(r.get("quarter", "")), str(r.get("week_start", ""))))
    return out


def main() -> None:
    data = json.loads(INPUT.read_text())
    sf = data.get("salesforce", {})
    dash = data.get("dashboard", {})

    GOALS_JSON.write_text(json.dumps(DEFAULT_GOALS, indent=2))

    chart_data = {
        key: (sf.get(key, {}) or {}).get("series", [])
        for key in ["new_sales", "total_active_pipeline", "new_customers", "sql", "mql", "renewals_number"]
    }
    current_q_label = "Q1-2026"
    q_anchor = (data.get("meta", {}) or {}).get("quarter_anchor_date")
    if isinstance(q_anchor, str) and len(q_anchor) >= 7:
        d = parse_iso_date(q_anchor)
        if d is not None:
            current_q_label = quarter_label_from_date(d)
    live_arr = (sf.get("arr", {}) or {}).get("value") or (dash.get("arr", {}) or {}).get("value") or 0
    arr_history = [*ARR_HISTORY_POINTS, {"quarter": current_q_label, "value": live_arr}]
    generated_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if HISTORY_JSON.exists():
        try:
            history_data = json.loads(HISTORY_JSON.read_text())
            if not isinstance(history_data, list):
                history_data = []
        except json.JSONDecodeError:
            history_data = []
    else:
        history_data = []
    snapshot = build_history_snapshot(data, generated_at_utc)
    if snapshot_is_complete(snapshot):
        history_data = upsert_history(history_data, snapshot)
        HISTORY_JSON.write_text(json.dumps(history_data, indent=2))

    dashboard_html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Medcurity Dashboard Preview</title>
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
  <script src=\"https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2\"></script>
  <style>
    :root {{
      --bg:#f3f7fc; --card:#ffffff; --cardSoft:#fbfdff; --ink:#0f2747; --muted:#5f7190; --line:#d8e4f3;
      --green:#39b36b; --yellow:#d9a620; --red:#d44f4f; --goal:#2b76c9; --arr:#1f7bc6;
      --grid:rgba(15,39,71,.13);
      --bgTop:#f8fbff;
      --glow:rgba(111,170,228,.30);
    }}
    [data-theme=\"dark\"] {{
      --bg:#0d1624; --card:#111f32; --cardSoft:#15263d; --ink:#e7f0fd; --muted:#9fb3ce; --line:#2a3e59;
      --green:#49c67b; --yellow:#e1b63d; --red:#e26a6a; --goal:#6aa9ff; --arr:#69b5ff;
      --grid:rgba(231,240,253,.16);
      --bgTop:#0f1b2d;
      --glow:rgba(35,91,152,.18);
    }}
    * {{ box-sizing: border-box; }}
    html {{ background:var(--bgTop); }}
    body {{ margin:0; font-family: "Avenir Next", "Manrope", "SF Pro Text", "Segoe UI", sans-serif; color:var(--ink); background:radial-gradient(1200px 580px at 90% -14%, var(--glow) 0%, transparent 62%), linear-gradient(180deg,var(--bgTop) 0%, var(--bg) 34%); transition:background .2s ease,color .2s ease; }}
    .wrap {{ max-width:1760px; margin:16px auto; padding:0 20px 22px; }}
    .top {{ display:grid; grid-template-columns:1fr auto 1fr; align-items:center; margin-bottom:12px; column-gap:10px; }}
    h1 {{ margin:0; font-size:34px; letter-spacing:.01em; color:var(--ink); }}
    .top h1 {{ justify-self:start; text-align:left; }}
    .stamp {{
      text-align:center; color:var(--muted); font-size:12px; line-height:1.25;
      border:1px solid var(--line);
      border-radius:999px;
      padding:7px 12px;
      background:var(--cardSoft);
      min-width:340px;
      max-width:340px;
      height:52px;
      box-shadow:0 3px 10px rgba(19,40,68,.07);
      justify-self:center;
    }}
    .stamp-main {{ color:var(--ink); font-weight:700; font-size:12px; letter-spacing:.01em; }}
    .stamp-sub {{ margin-top:2px; font-size:11px; opacity:.92; }}
    .actions {{ display:flex; gap:8px; justify-self:end; }}
    .actions a, .actions button {{ color:var(--ink); text-decoration:none; font-weight:700; border:1px solid var(--line); padding:7px 10px; border-radius:8px; background:var(--card); cursor:pointer; }}
    .sections {{ display:grid; grid-template-columns: 1fr 1fr; gap:16px; align-items:start; }}
    .sec {{ border:1px solid var(--line); border-radius:16px; background:var(--card); padding:12px; box-shadow:0 10px 24px rgba(19,40,68,.10); }}
    .sales {{ grid-column: 1; grid-row: 1; }}
    .marketing {{ grid-column: 1; grid-row: 2; }}
    .cs {{ grid-column: 2; grid-row: 1 / span 2; }}
    .svc {{ grid-column: 1; grid-row: 3; margin-top:0; }}
    .dev {{ grid-column: 2; grid-row: 3; margin-top:0; }}
    .sec h2 {{ margin:2px 2px 10px; font-size:30px; }}
    .kpi-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:8px; }}
    .pair-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:8px; }}
    .pair-col {{ display:grid; grid-template-rows:auto 1fr; gap:8px; }}
    .kpi {{ border:1px solid var(--line); border-radius:10px; padding:10px; background:var(--cardSoft); }}
    .k {{ color:var(--muted); font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }}
    .v {{ margin-top:4px; font-size:28px; font-weight:800; }}
    .chart-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; margin-bottom:8px; }}
    .chart {{ border:1px solid var(--line); border-radius:10px; padding:10px; min-height:300px; background:var(--card); position:relative; }}
    .chart.span2 {{ grid-column:1 / -1; }}
    .chart.tall {{ min-height:350px; }}
    .chart canvas {{ width:100% !important; height:260px !important; }}
    .chart.tall canvas {{ height:310px !important; }}
    .chart.no-data::after {{ content: 'Waiting for Salesforce data'; position:absolute; inset:auto 10px 10px 10px; text-align:center; color:var(--muted); font-size:12px; pointer-events:none; }}
    .chart h3 {{ margin:0 0 4px; font-size:14px; }}
    .small {{ color:var(--muted); font-size:11px; }}
    .list {{ border:1px solid var(--line); border-radius:10px; padding:8px; background:var(--cardSoft); }}
    .cs-extra {{ display:grid; grid-template-columns:1.2fr .8fr; gap:10px; margin-top:8px; }}
    .q-card {{ border:1px solid var(--line); border-radius:10px; padding:10px; background:var(--cardSoft); }}
    .q-title {{ display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:6px; }}
    .q-input, .q-textarea {{ width:100%; border:1px solid var(--line); border-radius:8px; background:var(--card); color:var(--ink); padding:8px; }}
    .q-textarea {{ min-height:92px; resize:vertical; }}
    .team-only {{ display:none; }}
    .view-only .owner-only {{ display:none !important; }}
    .view-only .team-only {{ display:block !important; }}
    .quote-view {{ margin:0; font-size:18px; line-height:1.45; color:var(--ink); }}
    .quote-meta {{ margin-top:8px; color:var(--muted); font-size:13px; }}
    .bill-readonly {{ margin-top:8px; }}
    .bill-row {{ display:flex; justify-content:space-between; align-items:center; margin:8px 0; font-size:13px; color:var(--muted); }}
    .bill-val {{ font-size:28px; font-weight:800; color:var(--yellow); }}
    .svc-layout {{ display:block; }}
    .health-card .v {{ font-size:22px; }}
    .health-wrap {{ margin-top:6px; border-top:1px solid var(--line); padding-top:8px; }}
    .health-wrap canvas {{ width:100% !important; height:150px !important; }}
    .svc-title {{ margin:0 0 8px; font-size:30px; }}
    .svc-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; }}
    .svc-grid .kpi {{ display:flex; flex-direction:column; justify-content:center; align-items:center; text-align:center; min-height:132px; }}
    .svc-grid .kpi .v {{ margin-top:6px; }}
    .health-card {{ align-items:stretch !important; }}
    .health-card .k, .health-card .v {{ text-align:center; }}
    .svc-status {{ margin-top:8px; font-size:13px; color:var(--muted); border:1px solid var(--line); border-radius:10px; padding:8px; background:var(--cardSoft); }}
    .svc-status h3 {{ margin:0 0 6px; font-size:13px; color:var(--ink); }}
    .status-chips {{ display:flex; flex-wrap:wrap; gap:6px; }}
    .status-chip {{ border:1px solid var(--line); border-radius:999px; padding:4px 8px; font-size:12px; background:var(--card); color:var(--ink); }}
    .dev-head {{ display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:8px; }}
    .dev-title {{ margin:0; font-size:30px; }}
    .dev-actions button {{ color:var(--ink); border:1px solid var(--line); background:var(--cardSoft); padding:6px 10px; border-radius:8px; cursor:pointer; }}
    .dev-actions .danger, .m-table .danger {{ border-color: rgba(212,79,79,.35); color: var(--red); }}
    .edit-only {{ display:none; }}
    .m-table {{ width:100%; border-collapse:collapse; }}
    .m-table th, .m-table td {{ border-bottom:1px solid var(--line); padding:8px; font-size:13px; }}
    .m-table th {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.05em; }}
    .m-table input[type=\"text\"], .m-table input[type=\"date\"] {{ width:100%; border:1px solid var(--line); border-radius:8px; background:var(--cardSoft); color:var(--ink); padding:6px 8px; }}
    .badge {{ display:inline-block; border-radius:999px; padding:4px 8px; font-size:11px; font-weight:700; }}
    .b-green {{ background:rgba(57,179,107,.18); color:var(--green); }}
    .b-yellow {{ background:rgba(217,166,32,.18); color:var(--yellow); }}
    .b-red {{ background:rgba(212,79,79,.18); color:var(--red); }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ font-size:12px; text-align:left; padding:6px; border-bottom:1px solid var(--line); }}
    th {{ color:var(--muted); font-size:10px; text-transform:uppercase; letter-spacing:.05em; }}
    @media (max-width: 1300px) {{
      .sections {{ grid-template-columns:1fr; }}
      .sales, .marketing, .cs, .svc, .dev {{ grid-column: auto; grid-row: auto; }}
    }}
    @media (max-width: 980px) {{
      .top {{ display:grid; grid-template-columns:1fr; gap:8px; }}
      .top h1 {{ text-align:center; }}
      .top .actions {{ justify-content:center; }}
      .top .stamp {{ margin:0 auto; max-width:340px; }}
    }}
    @media (max-width: 980px) {{ .svc-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .cs-extra {{ grid-template-columns:1fr; }} }}
    @media (max-width: 780px) {{ .chart-grid, .pair-grid, .svc-grid {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"top\">
      <h1>Team Dashboard</h1>
      <div class=\"stamp\">
        <div class=\"stamp-main\" id=\"stamp-main\"></div>
        <div class=\"stamp-sub\" id=\"stamp-sub\"></div>
      </div>
      <div class=\"actions\"><button id=\"theme-toggle\" type=\"button\">Toggle Theme</button><a href=\"goals_admin.html\">Goals Admin</a></div>
    </div>

    <div class=\"sections\">
      <section class=\"sec sales\">
        <h2>Sales</h2>
        <div class=\"pair-grid\">
          <div class=\"pair-col\">
            <div class=\"kpi\"><div class=\"k\">ARR</div><div class=\"v\" id=\"k-arr\"></div></div>
            <div class=\"chart tall\"><h3>ARR by Quarter <span class=\"small\">Rolling 365-day total</span></h3><canvas id=\"c-arr_history\"></canvas></div>
          </div>
          <div class=\"pair-col\">
            <div class=\"kpi\"><div class=\"k\">New Customers QTD</div><div class=\"v\" id=\"k-new-customers\"></div></div>
            <div class=\"chart\"><h3>New Customers <span class=\"small\">Running total vs Goal</span></h3><canvas id=\"c-new_customers\"></canvas></div>
          </div>
        </div>
        <div class=\"chart-grid\">
          <div class=\"chart\"><h3>New Sales <span class=\"small\">Running total vs Goal</span></h3><canvas id=\"c-new_sales\"></canvas></div>
          <div class=\"chart\"><h3>Total Active Pipeline <span class=\"small\">Current snapshot vs Goal</span></h3><canvas id=\"c-total_active_pipeline\"></canvas></div>
        </div>
      </section>

      <section class=\"sec marketing\">
        <h2>Marketing</h2>
        <div class=\"chart-grid\">
          <div class=\"chart\"><h3>SQL <span class=\"small\">Running total vs Goal</span></h3><canvas id=\"c-sql\"></canvas></div>
          <div class=\"chart\"><h3>MQL <span class=\"small\">Running total vs Goal</span></h3><canvas id=\"c-mql\"></canvas></div>
        </div>
      </section>

      <section class=\"sec cs\">
        <h2>Customer Success</h2>
        <div class=\"kpi-grid\">
          <div class=\"kpi\"><div class=\"k\">NRR by Customer</div><div class=\"v\" id=\"k-nrr-c\"></div></div>
          <div class=\"kpi\"><div class=\"k\">NRR by Dollar</div><div class=\"v\" id=\"k-nrr-d\"></div></div>
        </div>
        <div class=\"chart-grid\">
          <div class=\"chart\"><h3>NRR by Customer <span class=\"small\">Quarter trend</span></h3><canvas id=\"c-nrr_customer_history\"></canvas></div>
          <div class=\"chart\"><h3>NRR by Dollar <span class=\"small\">Quarter trend</span></h3><canvas id=\"c-nrr_dollar_history\"></canvas></div>
          <div class=\"chart span2 tall\"><h3>Renewals <span class=\"small\">Running total amount vs Goal</span></h3><canvas id=\"c-renewals_number\"></canvas></div>
        </div>
        <div class=\"list\">
          <h3 style=\"margin:0 0 2px; font-size:14px\">Lost Customers</h3>
          <div class=\"small\" style=\"margin-bottom:8px\"><span id=\"k-lost\"></span> account(s)</div>
          <table>
            <thead><tr><th>Account</th></tr></thead>
            <tbody id=\"lost-body\"></tbody>
          </table>
        </div>
        <div class=\"cs-extra\">
          <div class=\"q-card\">
            <div class=\"q-title\"><strong>Most Recent Quote <span style=\"color:var(--green)\">10/10</span></strong><span class=\"small\">Editable</span></div>
            <div class=\"owner-only\">
              <textarea id=\"quote-text\" class=\"q-textarea\" placeholder=\"Paste customer quote here\"></textarea>
            </div>
            <div class=\"team-only\" id=\"quote-readonly\">
              <p class=\"quote-view\" id=\"quote-view-text\">None</p>
              <div class=\"quote-meta\" id=\"quote-view-meta\"></div>
            </div>
            <div class=\"owner-only\" style=\"display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:8px;\">
              <input id=\"quote-name\" class=\"q-input\" placeholder=\"Person Name\" />
              <input id=\"quote-org\" class=\"q-input\" placeholder=\"Organization\" />
            </div>
          </div>
          <div class=\"q-card\">
            <div class=\"q-title\"><strong>QTD Billing Progress</strong><span class=\"small\">Manual input</span></div>
            <div class=\"bill-row\"><span>Quarter Goal</span><strong id=\"billing-goal\"></strong></div>
            <div class=\"owner-only\">
            <div class=\"bill-row\"><span>Current Progress</span></div>
            <input id=\"billing-progress\" class=\"q-input\" inputmode=\"numeric\" placeholder=\"Paste current QTD billing\" />
            </div>
            <div class=\"bill-row\"><span>Progress</span><span id=\"billing-pct\" class=\"bill-val\"></span></div>
          </div>
        </div>
      </section>
    <section class=\"sec svc\">
      <h2 class=\"svc-title\">Services</h2>
      <div class=\"svc-layout\">
        <div class=\"svc-grid\">
          <div class=\"kpi\"><div class=\"k\">Active Projects</div><div class=\"v\" id=\"k-svc-active\"></div></div>
          <div class=\"kpi\"><div class=\"k\">Closed This Quarter</div><div class=\"v\" id=\"k-svc-closed\"></div></div>
          <div class=\"kpi\"><div class=\"k\">Avg Close Days (QTD)</div><div class=\"v\" id=\"k-svc-avg\"></div></div>
          <div class=\"kpi health-card\">
            <div class=\"k\">Project Health</div>
            <div class=\"v\" id=\"k-svc-status\"></div>
            <div class=\"health-wrap\"><canvas id=\"c-svc-health\"></canvas></div>
          </div>
        </div>
      </div>
      <div class=\"svc-status\">
        <h3>Project Status Breakdown</h3>
        <div class=\"status-chips\" id=\"k-svc-status-breakdown\"></div>
      </div>
    </section>

    <section class=\"sec dev\">
      <div class=\"dev-head\">
        <h2 class=\"dev-title\">Development</h2>
        <div class=\"dev-actions\"><button id=\"edit-milestones\" type=\"button\">Edit</button><button id=\"remove-milestones\" class=\"danger edit-only\" type=\"button\">Remove Selected</button><button id=\"add-milestone\" class=\"edit-only\" type=\"button\">Add Milestone</button></div>
      </div>
      <table class=\"m-table\">
        <thead><tr><th class=\"edit-only\">Select</th><th>Project</th><th>Completion Date</th><th>Complete</th><th>Status</th></tr></thead>
        <tbody id=\"milestone-body\"></tbody>
      </table>
    </section>
    </div>
  </div>

  <script>
    const DATA = {js(data)};
    const CHART_DATA = {js(chart_data)};
    const DEFAULT_GOALS = {js(DEFAULT_GOALS)};
    const ARR_HISTORY = {js(arr_history)};
    const NRR_DOLLAR_HISTORY = {js([*NRR_DOLLAR_HISTORY_POINTS, {"quarter": current_q_label, "value": (dash.get("nrr_dollar_pct", {}) or {}).get("value", (sf.get("nrr_dollar_pct", {}) or {}).get("value", 0))}])};
    const NRR_CUSTOMER_HISTORY = {js([*NRR_CUSTOMER_HISTORY_POINTS, {"quarter": current_q_label, "value": (dash.get("nrr_customer_pct", {}) or {}).get("value", (sf.get("nrr_customer_pct", {}) or {}).get("value", 0))}])};
    const GENERATED_AT = {js(generated_at_utc)};
    const CURRENT_QUARTER = {js(current_q_label)};
    const VIEW_ONLY = false;
    if (VIEW_ONLY) document.documentElement.classList.add('view-only');
    const THEME_KEY = 'dashboard_theme_v1';
    const savedTheme = localStorage.getItem(THEME_KEY) || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme === 'dark' ? 'dark' : 'light');
    const MILESTONE_KEY = 'dashboard_milestones_v1';
    const CS_CONTENT_KEY = 'dashboard_cs_content_v1';
    const DEFAULT_MILESTONES = [
      {{ title: 'PhishRX', due_date: '2026-03-31', completed: false }},
      {{ title: 'Policy Updates', due_date: '2026-03-01', completed: false }},
      {{ title: 'LMS Build', due_date: '2026-02-16', completed: true }},
      {{ title: 'Small Practice SRA', due_date: '2026-02-21', completed: false }}
    ];

    Chart.register(ChartDataLabels);

    function money(v) {{ return '$' + Number(v || 0).toLocaleString(undefined, {{maximumFractionDigits:0}}); }}
    function pct(v) {{ return (Number(v || 0) * 100).toFixed(1) + '%'; }}
    function num(v) {{ return Number(v || 0).toLocaleString(undefined, {{maximumFractionDigits:0}}); }}

    function normalizeGoalEntry(raw, fallback, metricKey = '') {{
      if (metricKey === 'total_active_pipeline') {{
        return {{ quarter_goal: 800000, month_goals: [800000, 800000, 800000] }};
      }}
      if (Array.isArray(raw)) {{
        const month = raw.map(x => Number(x || 0));
        return {{ quarter_goal: month.reduce((a, b) => a + b, 0), month_goals: month }};
      }}
      const src = raw && typeof raw === 'object' ? raw : fallback;
      const quarter_goal = Number(src?.quarter_goal ?? fallback.quarter_goal ?? 0);
      const month_goals = (src?.month_goals || src?.splits || fallback.month_goals || [null, null, null]).map(v => {{
        if (v === null || v === undefined || v === '') return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      }});
      const isPct = metricKey === 'nrr_customer_pct' || metricKey === 'nrr_dollar_pct';
      const normalizePct = (v) => {{
        if (v === null || v === undefined) return v;
        return Number(v) > 1 ? Number(v) / 100 : Number(v);
      }};
      return {{ quarter_goal, month_goals }};
    }}

    function normalizedGoalValues(entry, metricKey) {{
      const isPct = metricKey === 'nrr_customer_pct' || metricKey === 'nrr_dollar_pct';
      if (!isPct) return entry;
      const q = Number(entry?.quarter_goal || 0);
      const m = Array.isArray(entry?.month_goals) ? entry.month_goals : [null, null, null];
      return {{
        quarter_goal: q > 1 ? q / 100 : q,
        month_goals: m.map(v => (v === null || v === undefined) ? null : (Number(v) > 1 ? Number(v) / 100 : Number(v)))
      }};
    }}

    function defaultGoals() {{
      const out = {{}};
      Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(DEFAULT_GOALS[k], DEFAULT_GOALS[k], k));
      return out;
    }}

    function loadLegacyGoals() {{
      try {{
        const v2 = localStorage.getItem('dashboard_goals_v2');
        if (v2) {{
          const parsed = JSON.parse(v2);
          const out = {{}};
          Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(parsed[k], DEFAULT_GOALS[k], k));
          return out;
        }}
        const v1 = localStorage.getItem('dashboard_goals_v1');
        if (v1) {{
          const parsed = JSON.parse(v1);
          const out = {{}};
          Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(parsed[k], DEFAULT_GOALS[k], k));
          return out;
        }}
      }} catch (e) {{}}
      return null;
    }}

    function loadGoalStore() {{
      try {{
        const raw = localStorage.getItem('dashboard_goals_by_quarter_v1');
        if (!raw) return {{}};
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : {{}};
      }} catch (e) {{
        return {{}};
      }}
    }}

    function loadGoals() {{
      const store = loadGoalStore();
      if (store[CURRENT_QUARTER] && typeof store[CURRENT_QUARTER] === 'object') {{
        const out = {{}};
        Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(store[CURRENT_QUARTER][k], DEFAULT_GOALS[k], k));
        return out;
      }}
      const legacy = loadLegacyGoals();
      if (legacy) {{
        if (!VIEW_ONLY) {{
          store[CURRENT_QUARTER] = legacy;
          localStorage.setItem('dashboard_goals_by_quarter_v1', JSON.stringify(store));
        }}
        return legacy;
      }}
      return defaultGoals();
    }}

    function monthlyGoals(metricKey, mode) {{
      const goals = loadGoals();
      const g = normalizedGoalValues(
        normalizeGoalEntry(goals[metricKey], DEFAULT_GOALS[metricKey], metricKey),
        metricKey
      );
      if (metricKey === 'total_active_pipeline') {{
        const v = Number((g.month_goals && g.month_goals[0]) || g.quarter_goal || 800000);
        return {{ monthly: [v, v, v], cumulative: [v, v, v] }};
      }}
      const cum = [...(g.month_goals || [null, null, null])];
      const targetQuarter = Number(g.quarter_goal || 0);
      cum[2] = (cum[2] === null || cum[2] === undefined) ? targetQuarter : Number(cum[2] || 0);
      cum[2] = targetQuarter;

      if (cum[0] === null && cum[1] === null) {{
        cum[0] = cum[2] / 3;
        cum[1] = (cum[2] * 2) / 3;
      }} else if (cum[0] !== null && cum[1] === null) {{
        cum[1] = (Number(cum[0]) + Number(cum[2])) / 2;
      }} else if (cum[0] === null && cum[1] !== null) {{
        cum[0] = Number(cum[1]) / 2;
      }}

      cum[0] = Number(cum[0] || 0);
      cum[1] = Number(cum[1] || 0);
      cum[2] = Number(cum[2] || 0);
      if (cum[1] < cum[0]) cum[1] = cum[0];
      if (cum[2] < cum[1]) cum[2] = cum[1];

      const monthly = [cum[0], cum[1] - cum[0], cum[2] - cum[1]];
      return {{
        monthly,
        cumulative: [cum[0], cum[1], cum[2]]
      }};
    }}

    function pointStatus(value, goalCum, idx) {{
      if (value === null || value === undefined) return null;
      const v = Number(value || 0);
      const g0 = Number(goalCum?.[0] || 0);
      const g1 = Number(goalCum?.[1] || g0);
      const g2 = Number(goalCum?.[2] || g1);
      if (idx === 0) return v >= g0 ? 'green' : 'red';
      if (idx === 1) {{
        if (v >= g1) return 'green';
        if (v >= g0) return 'yellow';
        return 'red';
      }}
      if (v >= g2) return 'green';
      if (v >= g1) return 'yellow';
      return 'red';
    }}

    function cssVar(name) {{
      return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }}

    function chartUi() {{
      return {{
        text: cssVar('--ink'),
        muted: cssVar('--muted'),
        card: cssVar('--card'),
        line: cssVar('--line'),
        grid: cssVar('--grid'),
        red: cssVar('--red'),
        yellow: cssVar('--yellow'),
        green: cssVar('--green'),
        goal: cssVar('--goal'),
        arr: cssVar('--arr'),
        clear: 'rgba(0,0,0,0)'
      }};
    }}

    function fmtByKind(kind, value) {{
      if (value === null || value === undefined) return '';
      if (kind === 'currency') return money(value);
      if (kind === 'percent') return pct(value);
      return num(value);
    }}

    function dataLabelOptions(ui, kind, seriesType = 'actual', labelMode = 'last_visible', fixedIndex = null, alignOverride = null, plain = false, anchorOverride = null) {{
      const isGoal = seriesType === 'goal';
      return {{
        display: (ctx) => {{
          const values = Array.isArray(ctx.dataset.data) ? ctx.dataset.data : [];
          const v = values[ctx.dataIndex];
          if (v === null || v === undefined) return false;
          if (typeof fixedIndex === 'number') return ctx.dataIndex === fixedIndex;
          if (labelMode === 'all') return true;
          if (labelMode === 'last_visible') {{
            let last = -1;
            for (let i = 0; i < values.length; i++) {{
              if (values[i] !== null && values[i] !== undefined) last = i;
            }}
            return ctx.dataIndex === last;
          }}
          return true;
        }},
        anchor: anchorOverride || 'end',
        align: alignOverride || 'top',
        color: isGoal ? ui.goal : ui.text,
        clamp: true,
        clip: false,
        offset: 3,
        padding: 4,
        borderRadius: 4,
        backgroundColor: plain ? 'rgba(0,0,0,0)' : (isGoal ? 'rgba(43,118,201,.16)' : ui.card),
        borderColor: plain ? 'rgba(0,0,0,0)' : (isGoal ? ui.goal : ui.line),
        borderWidth: plain ? 0 : 1,
        font: {{ size: 9, weight: '700' }},
        formatter: (v) => fmtByKind(kind, v)
      }};
    }}

    function quarterStartFromAnchor(anchorIso) {{
      const iso = String(anchorIso || new Date().toISOString().slice(0, 10));
      const y = Number(iso.slice(0, 4));
      const m = Number(iso.slice(5, 7));
      const qStartMonth = Math.floor((m - 1) / 3) * 3 + 1;
      return new Date(y, qStartMonth - 1, 1);
    }}

    function fallbackQuarterRows(metricKey, sfMetric) {{
      const anchorIso = String((DATA.meta || {{}}).quarter_anchor_date || new Date().toISOString().slice(0, 10));
      const currentYm = anchorIso.slice(0, 7);
      const qStart = quarterStartFromAnchor(anchorIso);
      const rows = [];
      for (let i = 0; i < 3; i++) {{
        const d = new Date(qStart.getFullYear(), qStart.getMonth() + i, 1);
        const monthIso = d.toISOString().slice(0, 7);
        rows.push({{
          month: monthIso,
          label: d.toLocaleString('en-US', {{ month: 'short' }}),
          value: monthIso === currentYm
            ? Number(sfMetric?.qtd_total ?? sfMetric?.snapshot_value ?? sfMetric?.value ?? 0)
            : null
        }});
      }}
      return rows;
    }}

    function setChartState(metricKey, hasData) {{
      const canvas = document.getElementById('c-' + metricKey);
      if (!canvas) return;
      const card = canvas.closest('.chart');
      if (!card) return;
      if (hasData) card.classList.remove('no-data');
      else card.classList.add('no-data');
    }}

    function transformedActualSeries(metricKey, rows, sfMetric, currentYm) {{
      if (!rows.length) return [];
      if (metricKey === 'total_active_pipeline' || sfMetric?.series_mode === 'snapshot') {{
        const snap = Number(sfMetric?.snapshot_value ?? sfMetric?.qtd_total ?? sfMetric?.value ?? 0);
        return rows.map(r => (r.month <= currentYm ? snap : null));
      }}

      const raw = rows.map(r => Number(r.value || 0));
      if (sfMetric?.series_mode === 'cumulative') {{
        return rows.map((r, i) => (r.month <= currentYm ? raw[i] : null));
      }}

      let running = 0;
      return rows.map((r, i) => {{
        if (r.month > currentYm) return null;
        running += raw[i];
        return running;
      }});
    }}

    function drawMetric(metricKey, kind) {{
      const sfMetric = (DATA.salesforce || {{}})[metricKey] || {{}};
      let rows = CHART_DATA[metricKey] || [];
      if (!rows.length) {{
        rows = fallbackQuarterRows(metricKey, sfMetric);
      }}
      setChartState(metricKey, rows.some(r => r && r.value !== null && r.value !== undefined));
      if (!rows.length) return;
      const labels = rows.map(r => r.label);
      const currentYm = String((DATA.meta || {{}}).quarter_anchor_date || new Date().toISOString().slice(0,10)).slice(0,7);
      let currentIndex = -1;
      rows.forEach((r, i) => {{ if (r.month <= currentYm) currentIndex = i; }});
      if (currentIndex < 0) currentIndex = 0;

      const actual = transformedActualSeries(metricKey, rows, sfMetric, currentYm);
      const mode = String(sfMetric?.series_mode || '');
      const goals = monthlyGoals(metricKey, mode).cumulative;
      const statuses = actual.map((v, i) => pointStatus(v, goals, i));
      const ui = chartUi();
      const colorByStatus = (s) => s === 'green' ? ui.green : (s === 'yellow' ? ui.yellow : (s === 'red' ? ui.red : ui.clear));
      const isPipeline = metricKey === 'total_active_pipeline';
      let yMax = isPipeline ? 1000000 : undefined;
      if (metricKey === 'sql') {{
        const top = Math.max(
          ...actual.filter(v => v !== null && v !== undefined).map(v => Number(v)),
          ...goals.filter(v => v !== null && v !== undefined).map(v => Number(v)),
          0
        );
        yMax = top > 0 ? Math.ceil(top * 1.2) : undefined;
      }}
      const layoutPadding = metricKey === 'sql'
        ? {{ top: 20, right: 26, left: 16, bottom: 8 }}
        : {{ top: 14, right: 14, left: 16, bottom: 6 }};

      new Chart(document.getElementById('c-' + metricKey), {{
        type: 'line',
        data: {{
          labels,
          datasets: [
            {{
              label: 'Actual',
              data: actual,
              borderWidth: 3,
              pointRadius: actual.map(v => v === null ? 0 : 4),
              pointHoverRadius: actual.map(v => v === null ? 0 : 5),
              pointHitRadius: 12,
              pointBackgroundColor: statuses.map(s => colorByStatus(s)),
              pointBorderColor: statuses.map(s => colorByStatus(s)),
              segment: {{ borderColor: ctx => colorByStatus(statuses[ctx.p1DataIndex]) }},
              spanGaps: false,
              tension: 0.25,
              fill: false,
              datalabels: dataLabelOptions(
                ui,
                kind,
                'actual',
                'last_visible',
                currentIndex,
                'left',
                isPipeline,
                'center'
              )
            }},
            {{
              label: 'Goal',
              data: goals,
              borderColor: ui.goal,
              borderWidth: 2,
              borderDash: [6,4],
              pointRadius: goals.map((v, i) => (v === null || i !== currentIndex) ? 0 : 3),
              pointHoverRadius: goals.map(v => v === null ? 0 : 5),
              pointHitRadius: 12,
              pointBackgroundColor: ui.goal,
              spanGaps: false,
              tension: 0.15,
              fill: false,
              datalabels: dataLabelOptions(
                ui,
                kind,
                'goal',
                'last_visible',
                currentIndex,
                'right',
                isPipeline,
                'center'
              )
            }}
          ]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          layout: {{ padding: layoutPadding }},
          plugins: {{
            legend: {{ display: true, labels: {{ boxWidth: 10, color: ui.text }} }}
          }},
          scales: {{
            y: {{
              beginAtZero: true,
              max: yMax,
              grid: {{ color: ui.grid }},
              ticks: {{ color: ui.muted, padding: 6, callback: (v) => kind === 'currency' ? money(v) : (kind === 'percent' ? pct(v) : num(v)) }}
            }},
            x: {{
              offset: true,
              grid: {{ color: ui.grid }},
              ticks: {{ color: ui.muted, padding: 6 }}
            }}
          }}
        }}
      }});
    }}

    function drawArrHistory() {{
      const labels = ARR_HISTORY.map(p => p.quarter);
      const values = ARR_HISTORY.map(p => Number(p.value || 0));
      const ui = chartUi();
      new Chart(document.getElementById('c-arr_history'), {{
        type: 'line',
        data: {{
          labels,
          datasets: [{{
            label: 'ARR (Rolling 365)',
            data: values,
            borderColor: ui.arr,
            borderWidth: 3,
            pointRadius: 3,
            pointBackgroundColor: ui.arr,
            tension: 0.2,
            fill: false,
            datalabels: dataLabelOptions(ui, 'currency', 'actual', 'last_visible')
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          layout: {{ padding: {{ top: 14, right: 14, left: 16, bottom: 6 }} }},
          plugins: {{
            legend: {{ display: false }}
          }},
          scales: {{
            y: {{ beginAtZero: false, grid: {{ color: ui.grid }}, ticks: {{ color: ui.muted, padding: 6, callback: (v) => money(v) }} }},
            x: {{ offset: true, grid: {{ color: ui.grid }}, ticks: {{ color: ui.muted, padding: 6 }} }}
          }}
        }}
      }});
    }}

    function drawNrrHistory(canvasId, points, label) {{
      const labels = points.map(p => p.quarter);
      const values = points.map(p => Number(p.value || 0));
      const currentIndex = Math.max(values.length - 1, 0);
      const ui = chartUi();
      const nrrKey = canvasId.includes('customer') ? 'nrr_customer_pct' : 'nrr_dollar_pct';
      const goalValue = Number(
        normalizedGoalValues(
          normalizeGoalEntry(loadGoals()[nrrKey], DEFAULT_GOALS[nrrKey], nrrKey),
          nrrKey
        ).quarter_goal || 0
      );
      const goalSeries = labels.map(() => goalValue);
      const statuses = values.map((v, i) => pointStatus(v, goalSeries, i));
      const colorByStatus = (s) => s === 'green' ? ui.green : (s === 'yellow' ? ui.yellow : (s === 'red' ? ui.red : ui.clear));
      new Chart(document.getElementById(canvasId), {{
        type: 'line',
        data: {{
          labels,
          datasets: [
            {{
              label,
              data: values,
              borderWidth: 3,
              pointRadius: 3,
              pointHoverRadius: 5,
              pointHitRadius: 12,
              pointBackgroundColor: statuses.map(s => colorByStatus(s)),
              pointBorderColor: statuses.map(s => colorByStatus(s)),
              segment: {{ borderColor: ctx => colorByStatus(statuses[ctx.p1DataIndex]) }},
              tension: 0.2,
              fill: false,
              datalabels: dataLabelOptions(ui, 'percent', 'actual', 'last_visible', currentIndex, 'left', false, 'center')
            }},
            {{
              label: 'Goal',
              data: goalSeries,
              borderColor: ui.goal,
              borderWidth: 2,
              borderDash: [6,4],
              pointRadius: labels.map((_, i) => i === currentIndex ? 3 : 0),
              pointHoverRadius: labels.map((_, i) => i === currentIndex ? 5 : 0),
              pointHitRadius: 12,
              pointBackgroundColor: ui.goal,
              tension: 0,
              fill: false,
              datalabels: dataLabelOptions(ui, 'percent', 'goal', 'last_visible', currentIndex, 'right', false, 'center')
            }}
          ]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          layout: {{ padding: {{ top: 14, right: 14, left: 16, bottom: 6 }} }},
          plugins: {{
            legend: {{ display: true, labels: {{ boxWidth: 10, color: ui.text }} }}
          }},
          scales: {{
            y: {{ beginAtZero: false, min: 0.6, max: 1.0, grid: {{ color: ui.grid }}, ticks: {{ color: ui.muted, padding: 6, callback: (v) => pct(v) }} }},
            x: {{ offset: true, grid: {{ color: ui.grid }}, ticks: {{ color: ui.muted, padding: 6 }} }}
          }}
        }}
      }});
    }}

    function drawServicesHealthChart() {{
      const services = DATA.services || {{}};
      const flagged = Array.isArray(services.projects_over_red_threshold) ? services.projects_over_red_threshold.length : 0;
      const total = Number(services.active_projects || 0);
      const healthy = Math.max(total - flagged, 0);
      const atRisk = Math.max(flagged, 0);
      const ui = chartUi();
      new Chart(document.getElementById('c-svc-health'), {{
        type: 'doughnut',
        data: {{
          labels: ['Healthy', 'At Risk'],
          datasets: [{{
            data: (healthy + atRisk) > 0 ? [healthy, atRisk] : [1, 0],
            backgroundColor: [ui.green, ui.red],
            borderWidth: 0
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          cutout: '68%',
          plugins: {{
            legend: {{ position: 'bottom', labels: {{ color: ui.text, boxWidth: 10 }} }},
            datalabels: {{
              color: ui.text,
              formatter: (v, ctx) => ctx.dataIndex === 0 ? num(v) : (v ? num(v) : ''),
              font: {{ size: 10, weight: '700' }}
            }}
          }}
        }}
      }});
    }}

    function loadCsContent() {{
      try {{
        const raw = localStorage.getItem(CS_CONTENT_KEY)
          || localStorage.getItem('dashboard_cs_content_shared_v1')
          || localStorage.getItem('dashboard_cs_content_v1');
        if (!raw) return {{ quote_text: '', quote_name: '', quote_org: '', billing_progress: '' }};
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') return {{ quote_text: '', quote_name: '', quote_org: '', billing_progress: '' }};
        return {{
          quote_text: String(parsed.quote_text || ''),
          quote_name: String(parsed.quote_name || ''),
          quote_org: String(parsed.quote_org || ''),
          billing_progress: String(parsed.billing_progress || '')
        }};
      }} catch (e) {{
        return {{ quote_text: '', quote_name: '', quote_org: '', billing_progress: '' }};
      }}
    }}

    function saveCsContent(content) {{
      localStorage.setItem(CS_CONTENT_KEY, JSON.stringify(content));
      localStorage.setItem('dashboard_cs_content_shared_v1', JSON.stringify(content));
      localStorage.setItem('dashboard_cs_content_v1', JSON.stringify(content));
    }}

    function loadMilestones() {{
      try {{
        const raw = localStorage.getItem(MILESTONE_KEY);
        if (!raw) return DEFAULT_MILESTONES;
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return DEFAULT_MILESTONES;
        return parsed;
      }} catch (e) {{
        return DEFAULT_MILESTONES;
      }}
    }}

    function saveMilestones(items) {{
      localStorage.setItem(MILESTONE_KEY, JSON.stringify(items));
    }}

    function milestoneStatus(item) {{
      if (item.completed) return {{ cls: 'b-green', label: 'Complete' }};
      if (!item.due_date) return {{ cls: 'b-green', label: 'On Track' }};
      const today = new Date();
      today.setHours(0,0,0,0);
      const due = new Date(item.due_date + 'T00:00:00');
      const diffDays = Math.floor((due - today) / 86400000);
      if (diffDays < 0) return {{ cls: 'b-red', label: 'Past Due' }};
      if (diffDays <= 7) return {{ cls: 'b-yellow', label: 'Due Soon' }};
      return {{ cls: 'b-green', label: 'On Track' }};
    }}

    function renderMilestones() {{
      const body = document.getElementById('milestone-body');
      const items = loadMilestones();
      body.innerHTML = items.map((item, idx) => {{
        const status = milestoneStatus(item);
        const title = String(item.title || '').replace(/\"/g, '&quot;');
        const due = String(item.due_date || '');
        return `<tr>`
          + `<td class=\"edit-only\"><input type=\"checkbox\" data-ms=\"selected\" data-i=\"${{idx}}\" ${{item.selected ? 'checked' : ''}} /></td>`
          + `<td><input type=\"text\" data-ms=\"title\" data-i=\"${{idx}}\" value=\"${{title}}\" /></td>`
          + `<td><input type=\"date\" data-ms=\"due_date\" data-i=\"${{idx}}\" value=\"${{due}}\" /></td>`
          + `<td><input type=\"checkbox\" data-ms=\"completed\" data-i=\"${{idx}}\" ${{item.completed ? 'checked' : ''}} /></td>`
          + `<td><span class=\"badge ${{status.cls}}\">${{status.label}}</span></td>`
          + `</tr>`;
      }}).join('');
    }}

    const sf = DATA.salesforce || {{}};
    const dash = DATA.dashboard || {{}};
    const services = DATA.services || {{}};
    const stampMainEl = document.getElementById('stamp-main');
    const stampSubEl = document.getElementById('stamp-sub');
    const now = new Date();
    if (stampMainEl) {{
      stampMainEl.textContent = 'Updated ' + now.toLocaleDateString(undefined, {{ month: 'short', day: 'numeric', year: 'numeric' }});
    }}
    if (stampSubEl) {{
      stampSubEl.textContent = 'Salesforce sync: ' + GENERATED_AT;
    }}
    document.getElementById('k-arr').textContent = money((dash.arr || {{}}).value || (sf.arr || {{}}).value || 0);
    document.getElementById('k-new-customers').textContent = num((sf.new_customers || {{}}).qtd_total || 0);
    document.getElementById('k-nrr-c').textContent = pct((dash.nrr_customer_pct || {{}}).value || (sf.nrr_customer_pct || {{}}).value || 0);
    document.getElementById('k-nrr-d').textContent = pct((dash.nrr_dollar_pct || {{}}).value || (sf.nrr_dollar_pct || {{}}).value || 0);
    document.getElementById('k-lost').textContent = num((sf.lost_customers || {{}}).value || 0);
    document.getElementById('k-svc-active').textContent = num(services.active_projects || 0);
    document.getElementById('k-svc-closed').textContent = num(services.closed_projects_this_quarter || 0);
    document.getElementById('k-svc-avg').textContent = num(Math.round(Number(services.avg_project_close_days_this_quarter || 0)));
    const svcStatus = String(services.overall_project_status || 'green').toLowerCase();
    const svcBadge = svcStatus === 'red' ? '<span class=\"badge b-red\">Red</span>' : '<span class=\"badge b-green\">Green</span>';
    document.getElementById('k-svc-status').innerHTML = svcBadge;
    const breakdownEl = document.getElementById('k-svc-status-breakdown');
    const breakdown = (Array.isArray(services.status_breakdown) ? services.status_breakdown : [])
      .filter(item => String(item.status || '').trim().toLowerCase() !== 'completed');
    if (breakdownEl) {{
      if (!breakdown.length) {{
        breakdownEl.innerHTML = '<span class=\"status-chip\">No status data</span>';
      }} else {{
        breakdownEl.innerHTML = breakdown
          .map(item => `<span class=\"status-chip\">${{item.status}}: ${{num(item.count || 0)}}</span>`)
          .join('');
      }}
    }}
    drawServicesHealthChart();

    const lostBody = document.getElementById('lost-body');
    const lostRows = (sf.lost_customers || {{}}).accounts || [];
    if (!lostRows.length) {{
      lostBody.innerHTML = '<tr><td>None</td></tr>';
    }} else {{
      lostBody.innerHTML = lostRows.map(r => `<tr><td>${{r.account_name || ''}}</td></tr>`).join('');
    }}

    const csContent = loadCsContent();
    const quoteText = document.getElementById('quote-text');
    const quoteName = document.getElementById('quote-name');
    const quoteOrg = document.getElementById('quote-org');
    const quoteViewText = document.getElementById('quote-view-text');
    const quoteViewMeta = document.getElementById('quote-view-meta');
    const billingProgress = document.getElementById('billing-progress');
    if (quoteText) quoteText.value = csContent.quote_text;
    if (quoteName) quoteName.value = csContent.quote_name;
    if (quoteOrg) quoteOrg.value = csContent.quote_org;
    if (billingProgress) billingProgress.value = csContent.billing_progress;
    if (quoteViewText) quoteViewText.textContent = csContent.quote_text || 'None';
    if (quoteViewMeta) {{
      const metaBits = [csContent.quote_name, csContent.quote_org].filter(Boolean);
      quoteViewMeta.textContent = metaBits.length ? ('- ' + metaBits.join(' @ ')) : '';
    }}
    const billingGoal = Number(normalizeGoalEntry(loadGoals().qtd_billing_progress, DEFAULT_GOALS.qtd_billing_progress, 'qtd_billing_progress').quarter_goal || 0);
    const goalEl = document.getElementById('billing-goal');
    if (goalEl) goalEl.textContent = money(billingGoal);
    const progressNum = Number(String(csContent.billing_progress || '0').replace(/[^0-9.-]/g, '')) || 0;
    const pctEl = document.getElementById('billing-pct');
    if (pctEl) {{
      const ratio = billingGoal > 0 ? (progressNum / billingGoal) : 0;
      pctEl.textContent = money(progressNum) + ' (' + (ratio * 100).toFixed(1) + '%)';
    }}
    // On owner view, normalize persisted content keys so team view can read the same payload.
    if (!VIEW_ONLY) {{
      saveCsContent({{
        quote_text: csContent.quote_text || '',
        quote_name: csContent.quote_name || '',
        quote_org: csContent.quote_org || '',
        billing_progress: csContent.billing_progress || ''
      }});
    }}

    drawMetric('new_sales', 'currency');
    drawMetric('total_active_pipeline', 'currency');
    drawMetric('new_customers', 'count');
    drawMetric('sql', 'count');
    drawMetric('mql', 'count');
    drawMetric('renewals_number', 'currency');
    drawArrHistory();
    drawNrrHistory('c-nrr_customer_history', NRR_CUSTOMER_HISTORY, 'NRR by Customer');
    drawNrrHistory('c-nrr_dollar_history', NRR_DOLLAR_HISTORY, 'NRR by Dollar');
    renderMilestones();

    let milestoneEditMode = false;
    function setMilestoneEditMode(enabled) {{
      milestoneEditMode = !!enabled;
      document.querySelectorAll('.edit-only').forEach(el => {{
        if (el.tagName === 'TH' || el.tagName === 'TD') {{
          el.style.display = milestoneEditMode ? '' : 'none';
        }} else {{
          el.style.display = milestoneEditMode ? 'inline-block' : 'none';
        }}
      }});
      const editBtn = document.getElementById('edit-milestones');
      if (editBtn) editBtn.textContent = milestoneEditMode ? 'Done' : 'Edit';
    }}

    const editMilestonesBtn = document.getElementById('edit-milestones');
    if (editMilestonesBtn) {{
      editMilestonesBtn.onclick = () => setMilestoneEditMode(!milestoneEditMode);
    }}

    const addMilestoneBtn = document.getElementById('add-milestone');
    if (addMilestoneBtn) {{
      addMilestoneBtn.onclick = () => {{
        const items = loadMilestones();
        items.push({{ title: '', due_date: '', completed: false, selected: false }});
        saveMilestones(items);
        renderMilestones();
        setMilestoneEditMode(milestoneEditMode);
      }};
    }}

    document.getElementById('milestone-body')?.addEventListener('change', (e) => {{
      const t = e.target;
      const idx = Number(t.getAttribute('data-i'));
      const key = t.getAttribute('data-ms');
      if (!Number.isFinite(idx) || !key) return;
      const items = loadMilestones();
      if (!items[idx]) return;
      if (key === 'completed') {{
        items[idx].completed = !!t.checked;
      }} else if (key === 'selected') {{
        items[idx].selected = !!t.checked;
      }} else {{
        items[idx][key] = t.value;
      }}
      saveMilestones(items);
      renderMilestones();
      setMilestoneEditMode(milestoneEditMode);
    }});

    const removeBtn = document.getElementById('remove-milestones');
    if (removeBtn) {{
      removeBtn.onclick = () => {{
        const items = loadMilestones();
        const kept = items.filter(i => !i.selected);
        saveMilestones(kept.map(i => ({{ ...i, selected: false }})));
        renderMilestones();
        setMilestoneEditMode(milestoneEditMode);
      }};
    }}

    const handleCsContentUpdate = () => {{
      saveCsContent({{
        quote_text: quoteText?.value || '',
        quote_name: quoteName?.value || '',
        quote_org: quoteOrg?.value || '',
        billing_progress: billingProgress?.value || ''
      }});
      const newProgress = Number(String(billingProgress?.value || '0').replace(/[^0-9.-]/g, '')) || 0;
      const ratio = billingGoal > 0 ? (newProgress / billingGoal) : 0;
      if (pctEl) pctEl.textContent = money(newProgress) + ' (' + (ratio * 100).toFixed(1) + '%)';
      if (quoteViewText) quoteViewText.textContent = quoteText?.value || 'None';
      if (quoteViewMeta) {{
        const metaBits = [quoteName?.value || '', quoteOrg?.value || ''].filter(Boolean);
        quoteViewMeta.textContent = metaBits.length ? ('- ' + metaBits.join(' @ ')) : '';
      }}
    }};
    [quoteText, quoteName, quoteOrg, billingProgress].forEach((el) => {{
      if (!el) return;
      el.addEventListener('change', handleCsContentUpdate);
      el.addEventListener('input', handleCsContentUpdate);
    }});

    function applyViewMode() {{
      if (!VIEW_ONLY) return;
      const goalsLink = document.querySelector('a[href="goals_admin.html"]');
      if (goalsLink) goalsLink.remove();
      const editBtn = document.getElementById('edit-milestones');
      const addBtn = document.getElementById('add-milestone');
      const removeBtn = document.getElementById('remove-milestones');
      if (editBtn) editBtn.style.display = 'none';
      if (addBtn) addBtn.style.display = 'none';
      if (removeBtn) removeBtn.style.display = 'none';
      document.querySelectorAll('#milestone-body input').forEach(el => {{
        el.disabled = true;
      }});
      // Team view stays read-only.
      [quoteText, quoteName, quoteOrg, billingProgress].forEach(el => {{
        if (!el) return;
        el.readOnly = true;
        el.disabled = true;
      }});
    }}

    setMilestoneEditMode(false);
    applyViewMode();

    const toggle = document.getElementById('theme-toggle');
    if (toggle) {{
      toggle.textContent = document.documentElement.getAttribute('data-theme') === 'dark' ? 'Light Mode' : 'Dark Mode';
      toggle.onclick = () => {{
        const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        localStorage.setItem(THEME_KEY, next);
        document.documentElement.setAttribute('data-theme', next);
        location.reload();
      }};
    }}
  </script>
</body>
</html>
"""

    goals_html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Dashboard Goals Admin</title>
  <style>
    body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background:#f3f7fc; color:#0f2747; margin:0; }}
    .wrap {{ max-width:1200px; margin:20px auto; padding:0 14px; }}
    h1 {{ margin:0 0 8px; }}
    .card {{ background:#fff; border:1px solid #d8e4f3; border-radius:10px; padding:12px; margin-bottom:10px; }}
    .tabs {{ display:flex; gap:8px; margin-bottom:10px; }}
    .tab {{ padding:8px 12px; border-radius:8px; border:1px solid #bfd3ec; background:#fff; cursor:pointer; }}
    .tab.active {{ background:#0f2747; color:#fff; border-color:#0f2747; }}
    .controls {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }}
    .controls label {{ font-size:12px; color:#5f7190; }}
    .controls select {{ padding:8px; border:1px solid #c8d8ee; border-radius:8px; background:#fff; }}
    .row {{ display:grid; grid-template-columns: 220px 170px 1fr 1fr 1fr; gap:8px; align-items:center; margin-bottom:8px; }}
    input {{ padding:8px; border:1px solid #c8d8ee; border-radius:8px; width:100%; }}
    .btns {{ display:flex; gap:8px; margin-top:8px; }}
    button, a {{ padding:8px 12px; border-radius:8px; border:1px solid #bfd3ec; background:#fff; cursor:pointer; text-decoration:none; color:#0f2747; }}
    .hint {{ color:#5f7190; font-size:12px; margin-bottom:10px; }}
    .head {{ font-size:11px; color:#5f7190; text-transform:uppercase; letter-spacing:.05em; margin-bottom:6px; display:grid; grid-template-columns: 220px 170px 1fr 1fr 1fr; gap:8px; }}
    .pane {{ display:none; }}
    .pane.active {{ display:block; }}
    .table-wrap {{ overflow:auto; border:1px solid #d8e4f3; border-radius:8px; }}
    table {{ width:100%; border-collapse:collapse; min-width:980px; }}
    th, td {{ border-bottom:1px solid #e6eef8; padding:8px; font-size:12px; text-align:left; white-space:nowrap; }}
    th {{ color:#5f7190; font-size:11px; text-transform:uppercase; letter-spacing:.05em; background:#f8fbff; }}
    .muted {{ color:#7085a4; }}
    @media (max-width: 980px) {{ .row, .head {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Goals Admin</h1>
    <div class=\"tabs\">
      <button class=\"tab active\" id=\"tab-goals\" type=\"button\">Goals</button>
      <button class=\"tab\" id=\"tab-history\" type=\"button\">Historical</button>
    </div>

    <div class=\"pane active\" id=\"pane-goals\">
      <div class=\"hint\">Quarter goals are stored per quarter. M3 is forced to quarter goal. If M1/M2 are blank, they auto-fill evenly. Active Pipeline remains fixed at 800,000 each month.</div>
      <div class=\"controls\">
        <label for=\"goal-quarter\">Goal Quarter</label>
        <select id=\"goal-quarter\"></select>
      </div>
      <div class=\"card\">
        <div class=\"head\">
          <div>Metric</div><div>Quarter Goal</div><div id=\"m1h\">Month1 Goal</div><div id=\"m2h\">Month2 Goal</div><div id=\"m3h\">Month3 Goal</div>
        </div>
        <div id=\"form\"></div>
      </div>
      <div class=\"btns\">
        <button id=\"save\">Save Goals</button>
        <button id=\"reset\">Reset Quarter Defaults</button>
        <a href=\"dashboard_preview.html\">Back To Dashboard</a>
      </div>
    </div>

    <div class=\"pane\" id=\"pane-history\">
      <div class=\"hint\">Weekly snapshots are captured on dashboard generation runs. Use this to compare quarter progress week-over-week.</div>
      <div class=\"controls\">
        <label for=\"history-quarter\">History Quarter</label>
        <select id=\"history-quarter\"></select>
      </div>
      <div class=\"table-wrap\">
        <table>
          <thead>
            <tr>
              <th>Week Start</th><th>Snapshot Date</th><th>ARR</th><th>New Sales</th><th>Pipeline</th><th>New Customers</th><th>SQL</th><th>MQL</th><th>Renewals</th><th>NRR Cust</th><th>NRR $</th><th>Δ vs prior week</th>
            </tr>
          </thead>
          <tbody id=\"history-body\"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const LABELS = {{
      new_sales:'New Sales',
      total_active_pipeline:'Total Active Pipeline',
      new_customers:'New Customers',
      arr:'ARR',
      sql:'SQL',
      mql:'MQL',
      renewals_number:'Renewals',
      nrr_customer_pct:'NRR by Customer',
      nrr_dollar_pct:'NRR by Dollar',
      qtd_billing_progress:'QTD Billing Goal'
    }};
    const DEFAULT_GOALS = {js(DEFAULT_GOALS)};
    const CURRENT_QUARTER = {js(current_q_label)};
    const HISTORY_DATA = {js(history_data)};
    const GOALS_BY_QUARTER_KEY = 'dashboard_goals_by_quarter_v1';

    function money(v) {{ return '$' + Number(v || 0).toLocaleString(undefined, {{maximumFractionDigits:0}}); }}
    function num(v) {{ return Number(v || 0).toLocaleString(undefined, {{maximumFractionDigits:0}}); }}
    function pct(v) {{ return (Number(v || 0) * 100).toFixed(1) + '%'; }}

    function quarterMonths(qLabel) {{
      const m = /^Q([1-4])-(\\d{{4}})$/.exec(String(qLabel || '').trim());
      if (!m) return ['Month1','Month2','Month3'];
      const q = Number(m[1]);
      const year = Number(m[2]);
      const startMonth = (q - 1) * 3;
      const names = [];
      for (let i = 0; i < 3; i++) {{
        names.push(new Date(year, startMonth + i, 1).toLocaleString(undefined, {{ month: 'short' }}));
      }}
      return names;
    }}

    function normalizeGoalEntry(raw, fallback, metricKey = '') {{
      if (metricKey === 'total_active_pipeline') {{
        return {{ quarter_goal: 800000, month_goals: [800000, 800000, 800000] }};
      }}
      if (Array.isArray(raw)) {{
        const monthly = raw.map(x => Number(x || 0));
        return {{ quarter_goal: monthly.reduce((a,b)=>a+b,0), month_goals: monthly }};
      }}
      const src = raw && typeof raw === 'object' ? raw : fallback;
      const month_goals = (src?.month_goals || src?.splits || fallback.month_goals || [null, null, null]).map(v => {{
        if (v === null || v === undefined || v === '') return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      }});
      return {{
        quarter_goal: Number(src?.quarter_goal ?? fallback.quarter_goal ?? 0),
        month_goals
      }};
    }}

    function defaultGoals() {{
      const out = {{}};
      Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(DEFAULT_GOALS[k], DEFAULT_GOALS[k], k));
      return out;
    }}

    function loadGoalStore() {{
      try {{
        const raw = localStorage.getItem(GOALS_BY_QUARTER_KEY);
        if (raw) {{
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed === 'object') return parsed;
        }}
      }} catch (e) {{}}
      const migrated = {{}};
      try {{
        const v2 = localStorage.getItem('dashboard_goals_v2');
        if (v2) {{
          migrated[CURRENT_QUARTER] = JSON.parse(v2);
          localStorage.setItem(GOALS_BY_QUARTER_KEY, JSON.stringify(migrated));
          return migrated;
        }}
      }} catch (e) {{}}
      return {{}};
    }}

    function getQuarterGoals(quarter) {{
      const store = loadGoalStore();
      const src = (store[quarter] && typeof store[quarter] === 'object') ? store[quarter] : defaultGoals();
      const out = {{}};
      Object.keys(DEFAULT_GOALS).forEach(k => out[k] = normalizeGoalEntry(src[k], DEFAULT_GOALS[k], k));
      return out;
    }}

    function saveQuarterGoals(quarter, goals) {{
      const store = loadGoalStore();
      store[quarter] = goals;
      localStorage.setItem(GOALS_BY_QUARTER_KEY, JSON.stringify(store));
      localStorage.setItem('dashboard_goals_v2', JSON.stringify(goals));
    }}

    function availableQuarters() {{
      const set = new Set([CURRENT_QUARTER]);
      HISTORY_DATA.forEach(row => {{ if (row && row.quarter) set.add(String(row.quarter)); }});
      const store = loadGoalStore();
      Object.keys(store).forEach(q => set.add(q));
      const parseQuarter = (q) => {{
        const m = /^Q([1-4])-(\\d{{4}})$/.exec(String(q || ''));
        if (!m) return [0, 0];
        return [Number(m[2]), Number(m[1])];
      }};
      return Array.from(set).sort((a,b) => {{
        const [ay, aq] = parseQuarter(a);
        const [by, bq] = parseQuarter(b);
        return ay === by ? aq - bq : ay - by;
      }});
    }}

    function renderGoals() {{
      const qSel = document.getElementById('goal-quarter');
      const quarter = qSel.value || CURRENT_QUARTER;
      const goals = getQuarterGoals(quarter);
      const months = quarterMonths(quarter);
      document.getElementById('m1h').textContent = months[0] + ' Goal';
      document.getElementById('m2h').textContent = months[1] + ' Goal';
      document.getElementById('m3h').textContent = months[2] + ' Goal';
      const form = document.getElementById('form');
      form.innerHTML = Object.keys(LABELS).map(key => {{
        const g = goals[key] || DEFAULT_GOALS[key];
        const locked = key === 'total_active_pipeline';
        const m0 = g.month_goals?.[0];
        const m1 = g.month_goals?.[1];
        const m2 = g.month_goals?.[2];
        const ro = locked ? 'readonly' : '';
        return `<div class=\"row\">`
          + `<div><strong>${{LABELS[key]}}</strong></div>`
          + `<input data-k=\"${{key}}\" data-f=\"quarter_goal\" value=\"${{g.quarter_goal}}\" ${{ro}} />`
          + `<input data-k=\"${{key}}\" data-f=\"month_goal\" data-i=\"0\" value=\"${{m0 === null || m0 === undefined ? '' : m0}}\" ${{ro}} />`
          + `<input data-k=\"${{key}}\" data-f=\"month_goal\" data-i=\"1\" value=\"${{m1 === null || m1 === undefined ? '' : m1}}\" ${{ro}} />`
          + `<input data-k=\"${{key}}\" data-f=\"month_goal\" data-i=\"2\" value=\"${{m2 === null || m2 === undefined ? '' : m2}}\" ${{ro}} />`
          + `</div>`;
      }}).join('');
    }}

    function fmtDelta(curr, prev, kind) {{
      if (curr === null || curr === undefined || prev === null || prev === undefined) return 'n/a';
      const d = Number(curr) - Number(prev);
      if (!Number.isFinite(d)) return 'n/a';
      if (kind === 'currency') return (d >= 0 ? '+' : '-') + money(Math.abs(d));
      if (kind === 'percent') return (d >= 0 ? '+' : '') + (d * 100).toFixed(1) + 'pp';
      return (d >= 0 ? '+' : '') + num(d);
    }}

    function renderHistory() {{
      const q = document.getElementById('history-quarter').value || CURRENT_QUARTER;
      const rows = HISTORY_DATA
        .filter(r => String(r.quarter || '') === q)
        .slice()
        .sort((a,b) => String(b.week_start || '').localeCompare(String(a.week_start || '')));
      const body = document.getElementById('history-body');
      if (!rows.length) {{
        body.innerHTML = '<tr><td colspan=\"12\" class=\"muted\">No snapshots yet for this quarter.</td></tr>';
        return;
      }}
      body.innerHTML = rows.map((row, idx) => {{
        const prev = rows[idx + 1];
        const m = row.metrics || {{}};
        const pm = prev ? (prev.metrics || {{}}) : null;
        const deltaBits = pm ? [
          `Sales ${{fmtDelta(m.new_sales, pm.new_sales, 'currency')}}`,
          `SQL ${{fmtDelta(m.sql, pm.sql, 'count')}}`,
          `MQL ${{fmtDelta(m.mql, pm.mql, 'count')}}`,
          `ARR ${{fmtDelta(m.arr, pm.arr, 'currency')}}`
        ].join(' | ') : 'Baseline week';
        return `<tr>`
          + `<td>${{row.week_start || ''}}</td>`
          + `<td>${{row.snapshot_date || ''}}</td>`
          + `<td>${{money(m.arr)}}</td>`
          + `<td>${{money(m.new_sales)}}</td>`
          + `<td>${{money(m.total_active_pipeline)}}</td>`
          + `<td>${{num(m.new_customers)}}</td>`
          + `<td>${{num(m.sql)}}</td>`
          + `<td>${{num(m.mql)}}</td>`
          + `<td>${{money(m.renewals_number)}}</td>`
          + `<td>${{pct(m.nrr_customer_pct)}}</td>`
          + `<td>${{pct(m.nrr_dollar_pct)}}</td>`
          + `<td class=\"muted\">${{deltaBits}}</td>`
          + `</tr>`;
      }}).join('');
    }}

    const goalQuarter = document.getElementById('goal-quarter');
    const histQuarter = document.getElementById('history-quarter');
    const quarters = availableQuarters();
    goalQuarter.innerHTML = quarters.map(q => `<option value=\"${{q}}\">${{q}}</option>`).join('');
    histQuarter.innerHTML = quarters.map(q => `<option value=\"${{q}}\">${{q}}</option>`).join('');
    goalQuarter.value = CURRENT_QUARTER;
    histQuarter.value = CURRENT_QUARTER;

    goalQuarter.onchange = renderGoals;
    histQuarter.onchange = renderHistory;

    document.getElementById('save').onclick = () => {{
      const goals = getQuarterGoals(goalQuarter.value);
      document.querySelectorAll('input[data-k]').forEach(inp => {{
        const k = inp.getAttribute('data-k');
        if (!goals[k]) goals[k] = {{ quarter_goal: 0, month_goals: [null, null, null] }};
        if (k === 'total_active_pipeline') {{
          goals[k] = {{ quarter_goal: 800000, month_goals: [800000, 800000, 800000] }};
          return;
        }}
        const f = inp.getAttribute('data-f');
        if (f === 'quarter_goal') {{
          goals[k].quarter_goal = Number(inp.value || 0);
        }} else {{
          const i = Number(inp.getAttribute('data-i'));
          const raw = inp.value;
          goals[k].month_goals[i] = raw === '' ? null : Number(raw || 0);
        }}
      }});
      saveQuarterGoals(goalQuarter.value, goals);
      alert('Goals saved for ' + goalQuarter.value + '.');
    }};

    document.getElementById('reset').onclick = () => {{
      saveQuarterGoals(goalQuarter.value, defaultGoals());
      renderGoals();
    }};

    function setTab(name) {{
      const goalsPane = document.getElementById('pane-goals');
      const historyPane = document.getElementById('pane-history');
      const goalsTab = document.getElementById('tab-goals');
      const historyTab = document.getElementById('tab-history');
      const goalsActive = name === 'goals';
      goalsPane.classList.toggle('active', goalsActive);
      historyPane.classList.toggle('active', !goalsActive);
      goalsTab.classList.toggle('active', goalsActive);
      historyTab.classList.toggle('active', !goalsActive);
    }}

    document.getElementById('tab-goals').onclick = () => setTab('goals');
    document.getElementById('tab-history').onclick = () => setTab('history');

    renderGoals();
    renderHistory();
  </script>
</body>
</html>
"""

    team_view_html = (
        dashboard_html
        .replace("<title>Medcurity Dashboard Preview</title>", "<title>Medcurity Team Dashboard</title>")
        .replace("const VIEW_ONLY = false;", "const VIEW_ONLY = true;")
        .replace('<a href="goals_admin.html">Goals Admin</a>', '')
        .replace('><a href=\\"goals_admin.html\\">Goals Admin</a>', '>')
    )

    DASHBOARD_OUTPUT.write_text(dashboard_html)
    TEAM_OUTPUT.write_text(team_view_html)
    INDEX_OUTPUT.write_text(team_view_html)
    GOALS_OUTPUT.write_text(goals_html)
    print(f"Wrote {DASHBOARD_OUTPUT}")
    print(f"Wrote {TEAM_OUTPUT}")
    print(f"Wrote {INDEX_OUTPUT}")
    print(f"Wrote {GOALS_OUTPUT}")
    print(f"Wrote {GOALS_JSON}")


if __name__ == "__main__":
    main()
