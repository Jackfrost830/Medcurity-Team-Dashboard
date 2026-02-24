import json
import os
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from simple_salesforce import Salesforce
import requests
from requests.exceptions import RequestException


NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main", "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}


class ConfigError(Exception):
    pass


class CRMReportClient(Protocol):
    provider: str

    def fetch_report(self, report_id: str, include_details: bool = True) -> dict[str, Any]:
        ...


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass
class XlsxCell:
    value: Any
    formula: str | None


class XlsxReader:
    def __init__(self, workbook_path: Path):
        self.workbook_path = workbook_path
        self._shared_strings: list[str] = []
        self._sheet_name_to_target: dict[str, str] = {}

    def __enter__(self):
        self._zip = ZipFile(self.workbook_path)
        self._load_shared_strings()
        self._load_sheet_map()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._zip.close()

    def _load_shared_strings(self) -> None:
        if "xl/sharedStrings.xml" not in self._zip.namelist():
            self._shared_strings = []
            return
        root = ET.fromstring(self._zip.read("xl/sharedStrings.xml"))
        out: list[str] = []
        for si in root.findall("a:si", NS):
            texts = [t.text or "" for t in si.findall(".//a:t", NS)]
            out.append("".join(texts))
        self._shared_strings = out

    def _load_sheet_map(self) -> None:
        wb = ET.fromstring(self._zip.read("xl/workbook.xml"))
        rels = ET.fromstring(self._zip.read("xl/_rels/workbook.xml.rels"))
        rid_to_target = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        for sheet in wb.find("a:sheets", NS):
            name = sheet.attrib["name"]
            rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            self._sheet_name_to_target[name] = rid_to_target[rid]

    def _sheet_xml(self, sheet_name: str) -> ET.Element:
        target = self._sheet_name_to_target.get(sheet_name)
        if not target:
            raise ConfigError(f"Sheet not found: {sheet_name}")
        return ET.fromstring(self._zip.read(f"xl/{target}"))

    def get_cell(self, sheet_name: str, cell_ref: str) -> XlsxCell:
        sheet = self._sheet_xml(sheet_name)
        cell = sheet.find(f".//a:c[@r='{cell_ref}']", NS)
        if cell is None:
            return XlsxCell(value=None, formula=None)
        return self._parse_cell(cell)

    def get_row_cells(self, sheet_name: str, row_number: int) -> list[tuple[str, XlsxCell]]:
        sheet = self._sheet_xml(sheet_name)
        row = sheet.find(f".//a:sheetData/a:row[@r='{row_number}']", NS)
        if row is None:
            return []
        out: list[tuple[str, XlsxCell]] = []
        for cell in row.findall("a:c", NS):
            ref = cell.attrib.get("r", "")
            out.append((ref, self._parse_cell(cell)))
        return out

    def _parse_cell(self, cell: ET.Element) -> XlsxCell:
        raw_type = cell.attrib.get("t")
        v = cell.find("a:v", NS)
        f = cell.find("a:f", NS)
        value = v.text if v is not None else None
        formula = f.text if f is not None else None

        if raw_type == "s" and value is not None:
            idx = int(value)
            return XlsxCell(
                value=self._shared_strings[idx] if 0 <= idx < len(self._shared_strings) else None,
                formula=formula,
            )

        if value is None:
            return XlsxCell(value=None, formula=formula)

        try:
            if "." in value:
                return XlsxCell(value=float(value), formula=formula)
            return XlsxCell(value=int(value), formula=formula)
        except ValueError:
            return XlsxCell(value=value, formula=formula)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required env var: {name}")
    return value


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def parse_path_tokens(path_expr: str) -> list[Any]:
    # Supports dotted fields with optional [index], e.g. factMap.T!T.aggregates[0].value
    tokens: list[Any] = []
    for part in path_expr.split("."):
        if not part:
            continue
        m = re.match(r"^([^[\]]+)(?:\[(\d+)\])?$", part)
        if not m:
            raise ConfigError(f"Invalid path token: {part}")
        key = m.group(1)
        idx = m.group(2)
        tokens.append(key)
        if idx is not None:
            tokens.append(int(idx))
    return tokens


def get_by_path(data: Any, path_expr: str) -> Any:
    cur = data
    for token in parse_path_tokens(path_expr):
        if isinstance(token, int):
            if not isinstance(cur, list) or token >= len(cur):
                raise ConfigError(f"Path index missing: {token} in {path_expr}")
            cur = cur[token]
            continue
        if not isinstance(cur, dict) or token not in cur:
            raise ConfigError(f"Path key missing: {token} in {path_expr}")
        cur = cur[token]
    return cur


def salesforce_client_from_env() -> Salesforce:
    return Salesforce(
        username=require_env("SF_USERNAME"),
        password=require_env("SF_PASSWORD"),
        security_token=require_env("SF_SECURITY_TOKEN"),
        domain=os.getenv("SF_DOMAIN", "login"),
    )


def fetch_salesforce_report(sf: Salesforce, report_id: str, include_details: bool = True) -> dict[str, Any]:
    response = sf.restful(
        f"analytics/reports/{report_id}",
        params={"includeDetails": str(include_details).lower()},
    )
    if not isinstance(response, dict):
        raise ConfigError(f"Unexpected Salesforce report response for report {report_id}")
    return response


class SalesforceReportClient:
    provider = "salesforce"

    def __init__(self) -> None:
        self._sf = salesforce_client_from_env()

    def fetch_report(self, report_id: str, include_details: bool = True) -> dict[str, Any]:
        return fetch_salesforce_report(self._sf, report_id=report_id, include_details=include_details)


class PipedriveReportClient:
    provider = "pipedrive"

    def __init__(self, config: dict[str, Any]) -> None:
        # Pipedrive does not have a Salesforce-style Analytics Reports API.
        # During migration, implement provider-specific metric extractors or
        # provide report snapshots as JSON at this path.
        self._path = Path(str(config.get("pipedrive_reports_json_path", "")).strip()).expanduser()
        self._cache: dict[str, Any] = {}
        if self._path and self._path.exists():
            raw = json.loads(self._path.read_text())
            if isinstance(raw, dict):
                self._cache = raw

    def fetch_report(self, report_id: str, include_details: bool = True) -> dict[str, Any]:
        if report_id in self._cache and isinstance(self._cache[report_id], dict):
            return self._cache[report_id]
        raise ConfigError(
            "Pipedrive provider selected but report mapping is not configured. "
            "Set pipedrive_reports_json_path with objects keyed by report_id."
        )


def crm_client_from_config(config: dict[str, Any]) -> CRMReportClient:
    crm_cfg = config.get("crm", {}) if isinstance(config.get("crm"), dict) else {}
    provider = str(crm_cfg.get("provider", "salesforce")).strip().lower()
    if provider == "salesforce":
        return SalesforceReportClient()
    if provider == "pipedrive":
        return PipedriveReportClient(crm_cfg)
    raise ConfigError(f"Unsupported CRM provider: {provider}")


def extract_report_aggregate_value(report_data: dict[str, Any], aggregate_index: int = 0) -> float:
    fact_map = report_data.get("factMap", {}) if isinstance(report_data.get("factMap"), dict) else {}
    candidates: list[Any] = []
    preferred = fact_map.get("T!T")
    if isinstance(preferred, dict):
        candidates.extend(preferred.get("aggregates", []) or [])
    for key, bucket in fact_map.items():
        if key == "T!T" or not isinstance(bucket, dict):
            continue
        candidates.extend(bucket.get("aggregates", []) or [])
    if not candidates:
        return 0.0
    # Aggregates often repeat in grouped reports; prefer the primary index and max numeric value.
    values: list[float] = []
    for item in candidates:
        if isinstance(item, dict):
            raw = item.get("value")
            values.append(parse_number(raw))
    if not values:
        return 0.0
    return max(values)


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def parse_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        for k in ("amount", "value"):
            if k in value:
                return parse_number(value.get(k))
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_epoch_ms_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def parse_epoch_ms_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def parse_iso_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def find_row_value(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def extract_tabular_report_rows(report_data: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = report_data.get("reportMetadata", {})
    extended = report_data.get("reportExtendedMetadata", {})
    detail_cols = metadata.get("detailColumns", []) or []
    detail_info = (extended.get("detailColumnInfo") or {}) if isinstance(extended, dict) else {}

    fact_map = report_data.get("factMap", {}) if isinstance(report_data.get("factMap"), dict) else {}
    # Tabular reports commonly use T!T. Grouped reports often store detail rows in keys like 0!T, 1!T, 0_0!T.
    row_buckets: list[list[dict[str, Any]]] = []
    t_bucket = fact_map.get("T!T", {})
    t_rows = t_bucket.get("rows", []) if isinstance(t_bucket, dict) else []
    if t_rows:
        row_buckets.append(t_rows)
    else:
        for key, bucket in fact_map.items():
            if key == "T!T" or not isinstance(bucket, dict):
                continue
            rows = bucket.get("rows", []) or []
            if rows:
                row_buckets.append(rows)

    rows = [row for bucket in row_buckets for row in bucket]
    seen_signatures: set[str] = set()
    output: list[dict[str, Any]] = []

    for row in rows:
        # Deduplicate rows that can appear in multiple grouped fact buckets.
        signature_parts: list[str] = []
        for cell in (row.get("dataCells") or []):
            signature_parts.append(str(cell.get("value")))
            signature_parts.append(str(cell.get("label")))
        signature = "|".join(signature_parts)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        cells = row.get("dataCells", []) if isinstance(row, dict) else []
        mapped: dict[str, Any] = {}
        for idx, api_col in enumerate(detail_cols):
            if idx >= len(cells):
                continue
            cell = cells[idx]
            value = cell.get("value")
            label = cell.get("label")
            col_info = detail_info.get(api_col, {}) if isinstance(detail_info, dict) else {}
            ui_label = col_info.get("label", api_col)

            mapped[api_col] = value if value is not None else label
            mapped[normalize_key(api_col)] = mapped[api_col]
            mapped[normalize_key(ui_label)] = mapped[api_col]

            # Label can carry cleaner date/number formatting in some report outputs.
            if label not in (None, ""):
                mapped[f"{normalize_key(api_col)}_label"] = label
                mapped[f"{normalize_key(ui_label)}_label"] = label
        output.append(mapped)
    return output


def compute_financial_metrics_from_report(report_data: dict[str, Any], owner_filter: str = "Consolidated") -> dict[str, Any]:
    rows = extract_tabular_report_rows(report_data)
    if not rows:
        raise ConfigError("Report has no detail rows. Ensure includeDetails=true and report is tabular.")

    today = date.today()
    cutoff = today - timedelta(days=365)
    owner_filter_norm = owner_filter.strip().lower()
    renewal_sources = {
        "renewal - influence partner",
        "renewal - strategic partner",
        "renewal- direct",
    }

    arr_amount = 0.0
    won_count = 0
    lost_amount = 0.0
    lost_count = 0

    for row in rows:
        stage = str(
            find_row_value(
                row,
                [normalize_key("Stage"), normalize_key("Opportunity.StageName"), "stage"],
            )
            or ""
        ).strip()
        owner = str(
            find_row_value(
                row,
                [normalize_key("Opportunity Owner"), normalize_key("Owner"), "owner"],
            )
            or ""
        ).strip()
        one_time = parse_bool(
            find_row_value(
                row,
                [normalize_key("One Time Project"), normalize_key("One_Time_Project__c"), "onetimeproject"],
            )
        )
        amount = parse_number(
            find_row_value(
                row,
                [normalize_key("Amount"), normalize_key("Opportunity.Amount"), "amount"],
            )
        )

        close_raw = find_row_value(
            row,
            [
                f"{normalize_key('Close Date')}_label",
                normalize_key("Close Date"),
                normalize_key("Opportunity.CloseDate"),
                "closedate",
            ],
        )
        close_date = parse_date(close_raw)
        if close_date is None or close_date <= cutoff:
            continue

        if owner_filter_norm != "consolidated" and owner.strip().lower() != owner_filter_norm:
            continue

        if stage == "Closed Won" and not one_time:
            arr_amount += amount
            won_count += 1

        lead_source = str(
            find_row_value(
                row,
                [normalize_key("Lead Source"), normalize_key("Opportunity.LeadSource"), "leadsource"],
            )
            or ""
        ).strip().lower()

        if stage == "Closed Lost" and not one_time and lead_source in renewal_sources:
            lost_amount += amount
            lost_count += 1

    nrr_dollar_pct = 1.0 - (lost_amount / arr_amount) if arr_amount else 0.0
    nrr_customer_pct = 1.0 - (lost_count / won_count) if won_count else 0.0
    return {
        "arr": arr_amount,
        "nrr_dollar_pct": nrr_dollar_pct,
        "nrr_customer_pct": nrr_customer_pct,
        "window_start": cutoff.isoformat(),
        "window_end": today.isoformat(),
        "won_count_rolling_365": won_count,
        "lost_count_rolling_365": lost_count,
        "lost_amount_rolling_365": lost_amount,
    }


def quarter_bounds(anchor: date) -> tuple[date, date, list[date]]:
    quarter_start_month = ((anchor.month - 1) // 3) * 3 + 1
    q_start = date(anchor.year, quarter_start_month, 1)
    if quarter_start_month == 10:
        q_end = date(anchor.year + 1, 1, 1)
    else:
        q_end = date(anchor.year, quarter_start_month + 3, 1)
    month_starts = [date(anchor.year, quarter_start_month + i, 1) for i in range(3)]
    return q_start, q_end, month_starts


def last_monday_window(anchor: date) -> tuple[date, date]:
    # If run on Monday, "last Monday" means 7 days ago.
    if anchor.weekday() == 0:
        start = anchor - timedelta(days=7)
    else:
        start = anchor - timedelta(days=anchor.weekday())
    end_exclusive = anchor + timedelta(days=1)
    return start, end_exclusive


def month_label(d: date) -> str:
    return d.strftime("%b")


def quarter_label(anchor: date) -> str:
    q_num = ((anchor.month - 1) // 3) + 1
    return f"Q{q_num}-{anchor.year}"


def clickup_headers() -> dict[str, str]:
    token = require_env("CLICKUP_API_TOKEN")
    return {"Authorization": token, "Content-Type": "application/json"}


def fetch_clickup_list_tasks(list_id: str, include_closed: bool = True) -> list[dict[str, Any]]:
    headers = clickup_headers()
    page = 0
    out: list[dict[str, Any]] = []
    while True:
        params = {"page": page, "include_closed": str(include_closed).lower()}
        r = requests.get(f"https://api.clickup.com/api/v2/list/{list_id}/task", headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        tasks = data.get("tasks", []) if isinstance(data, dict) else []
        out.extend(tasks)
        if data.get("last_page") is True or not tasks:
            break
        page += 1
    return out


def task_custom_field_value(task: dict[str, Any], field_key_or_name: str) -> Any:
    needle = str(field_key_or_name).strip().lower()
    if not needle:
        return None
    for field in task.get("custom_fields", []) or []:
        fid = str(field.get("id", "")).strip().lower()
        fname = str(field.get("name", "")).strip().lower()
        if needle in {fid, fname}:
            return field.get("value")
    return None


def _quarter_sort_key(label: str) -> tuple[int, int]:
    m = re.match(r"^\s*(\d{4})\s+Q([1-4])\s*$", str(label or ""))
    if not m:
        return (0, 0)
    return (int(m.group(1)), int(m.group(2)))


def _date_to_datetime(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, datetime.min.time())


def _first_metric_date(metrics: dict[str, Any], keys: list[str]) -> date | None:
    for key in keys:
        parsed = parse_date(metrics.get(key))
        if parsed is not None:
            return parsed
    return None


def _load_admin_dashboard_rows(db_path: Path) -> list[dict[str, Any]]:
    query = """
    SELECT sf_id, task_name, task_status, task_created_at, task_closed_at, metrics_json, source_updated_at
    FROM client_status
    """
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(query).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        raw_metrics = row[5] or "{}"
        try:
            metrics = json.loads(raw_metrics)
            if not isinstance(metrics, dict):
                metrics = {}
        except json.JSONDecodeError:
            metrics = {}
        output.append(
            {
                "sf_id": str(row[0] or ""),
                "task_name": str(row[1] or ""),
                "task_status": str(row[2] or ""),
                "task_created_at": str(row[3] or ""),
                "task_closed_at": str(row[4] or ""),
                "metrics": metrics,
                "source_updated_at": str(row[6] or ""),
            }
        )
    return output


def _admin_dashboard_projects(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    projects: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("task_status", "")).strip().lower()
        is_completed = status == "completed"
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        anchor_dt: datetime | None = None

        if is_completed:
            sra_final = _date_to_datetime(
                _first_metric_date(metrics, ["sra.present_final_sra_report.date", "sra.present_final_sra_report.acd"])
            )
            nva_final = _date_to_datetime(
                _first_metric_date(metrics, ["nva.present_final_nva_report.date", "nva.present_final_nva_report.acd"])
            )
            if sra_final is not None and nva_final is not None:
                anchor_dt = max(sra_final, nva_final)
            else:
                anchor_dt = sra_final or nva_final
            if anchor_dt is None:
                anchor_dt = parse_iso_datetime(row.get("task_closed_at")) or parse_iso_datetime(row.get("source_updated_at"))
        else:
            anchor_dt = parse_iso_datetime(row.get("task_created_at")) or parse_iso_datetime(row.get("source_updated_at"))

        projects.append(
            {
                "task_name": str(row.get("task_name", "")),
                "task_status": status,
                "period_label": quarter_label(anchor_dt.date()) if anchor_dt is not None else "",
            }
        )

    completed = [p for p in projects if p["task_status"] == "completed"]
    active = [p for p in projects if p["task_status"] != "completed"]

    # Mirror the temporary quarter bucketing in the admin dashboard app.
    for project in completed:
        name_key = project["task_name"].strip().lower()
        if name_key == "acs":
            project["period_label"] = "Q1-2026"
        elif project["period_label"] == "Q1-2026":
            project["period_label"] = "Q4-2025"

    return active, completed


def _admin_dashboard_avg_close_days(db_path: Path, quarter_lbl: str) -> float:
    query = """
    SELECT quarter_label, close_days
    FROM historical_close_metrics
    """
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(query).fetchall()
    by_quarter: dict[str, list[float]] = {}
    for quarter_raw, close_days_raw in rows:
        q = str(quarter_raw or "").strip()
        if not q:
            continue
        by_quarter.setdefault(q, []).append(parse_number(close_days_raw))
    if not by_quarter:
        return 0.0
    if quarter_lbl in by_quarter and by_quarter[quarter_lbl]:
        vals = by_quarter[quarter_lbl]
        return sum(vals) / len(vals)
    latest_quarter = sorted(by_quarter.keys(), key=_quarter_sort_key)[-1]
    vals = by_quarter.get(latest_quarter, [])
    return (sum(vals) / len(vals)) if vals else 0.0


def _display_status(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return "Unknown"
    text = re.sub(r"[_-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.title()


def compute_services_from_admin_dashboard(config: dict[str, Any], anchor_date: date) -> dict[str, Any]:
    services_cfg = config.get("services", {}) if isinstance(config.get("services"), dict) else {}
    raw_db_path = str(services_cfg.get("admin_dashboard_db_path", "")).strip() or str(
        os.getenv("ADMIN_DASHBOARD_DB_PATH", "")
    ).strip()
    if not raw_db_path:
        return {
            "status": "pending_config",
            "message": "Set services.admin_dashboard_db_path or ADMIN_DASHBOARD_DB_PATH.",
        }
    db_path = Path(raw_db_path).expanduser()
    if not db_path.exists():
        return {"status": "error", "message": f"Admin dashboard DB not found: {db_path}"}

    rows = _load_admin_dashboard_rows(db_path)
    active, completed = _admin_dashboard_projects(rows)
    status_counts = Counter(_display_status(row.get("task_status")) for row in rows)
    status_breakdown = [
        {"status": status, "count": count}
        for status, count in sorted(status_counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    q_label = quarter_label(anchor_date)
    closed_this_quarter = sum(1 for p in completed if p.get("period_label") == q_label)
    avg_close_days = _admin_dashboard_avg_close_days(db_path, q_label)
    return {
        "status": "ok",
        "source": "admin_dashboard_db",
        "quarter": q_label,
        "active_projects": len(active),
        "closed_projects_this_quarter": closed_this_quarter,
        "avg_project_close_days_this_quarter": avg_close_days,
        "overall_project_status": "green",
        "projects_over_red_threshold": [],
        "status_breakdown": status_breakdown,
        "task_count": len(active) + len(completed),
    }


def compute_services_from_clickup(config: dict[str, Any], anchor_date: date) -> dict[str, Any]:
    services_cfg = config.get("services", {}) if isinstance(config.get("services"), dict) else {}
    clickup_cfg = services_cfg.get("clickup", {}) if isinstance(services_cfg.get("clickup"), dict) else {}
    list_id = str(clickup_cfg.get("list_id", "")).strip() or str(os.getenv("CLICKUP_LIST_ID", "")).strip()
    if not list_id:
        return {"status": "pending_config", "message": "Set services.clickup.list_id or CLICKUP_LIST_ID."}

    q_start, q_end, _ = quarter_bounds(anchor_date)
    closed_statuses = {s.strip().lower() for s in clickup_cfg.get("closed_statuses", ["complete", "completed", "closed", "done", "cancelled", "canceled"])}
    red_threshold = int(clickup_cfg.get("red_item_threshold", 3))
    red_field = str(clickup_cfg.get("red_items_field", "")).strip()
    use_admin_json = str(services_cfg.get("admin_dashboard_json", "")).strip()

    tasks = fetch_clickup_list_tasks(list_id, include_closed=True)
    active_count = 0
    closed_this_quarter = 0
    close_day_values: list[float] = []
    projects_over_threshold: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()

    for task in tasks:
        status_name = str((task.get("status") or {}).get("status", "")).strip().lower()
        status_counts[_display_status(status_name)] += 1
        is_closed = status_name in closed_statuses
        if not is_closed:
            active_count += 1

        created_dt = parse_epoch_ms_datetime(task.get("date_created"))
        closed_dt = parse_epoch_ms_datetime(task.get("date_closed"))
        if is_closed and closed_dt is not None and q_start <= closed_dt.date() < q_end:
            closed_this_quarter += 1
            if created_dt is not None and closed_dt >= created_dt:
                close_day_values.append((closed_dt - created_dt).total_seconds() / 86400.0)

        if red_field:
            red_val = parse_number(task_custom_field_value(task, red_field))
            if red_val > red_threshold:
                projects_over_threshold.append(
                    {
                        "project": task.get("name", ""),
                        "red_items": red_val,
                        "source": "clickup",
                    }
                )

    if use_admin_json:
        p = Path(use_admin_json).expanduser()
        if p.exists():
            try:
                obj = json.loads(p.read_text())
                for proj in obj.get("projects", []) if isinstance(obj, dict) else []:
                    red_items = parse_number(proj.get("red_items"))
                    if red_items > red_threshold:
                        projects_over_threshold.append(
                            {
                                "project": str(proj.get("name", "")),
                                "red_items": red_items,
                                "source": str(p),
                            }
                        )
            except json.JSONDecodeError:
                pass

    avg_close_days = sum(close_day_values) / len(close_day_values) if close_day_values else 0.0
    overall_status = "green" if not projects_over_threshold else "red"
    status_breakdown = [
        {"status": status, "count": count}
        for status, count in sorted(status_counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    return {
        "status": "ok",
        "source": "clickup",
        "quarter": quarter_label(anchor_date),
        "active_projects": active_count,
        "closed_projects_this_quarter": closed_this_quarter,
        "avg_project_close_days_this_quarter": avg_close_days,
        "overall_project_status": overall_status,
        "projects_over_red_threshold": projects_over_threshold,
        "status_breakdown": status_breakdown,
        "red_item_threshold": red_threshold,
        "task_count": len(tasks),
    }


def row_date_from_candidates(row: dict[str, Any], date_keys: list[str]) -> date | None:
    default_candidates = [
        "closedate",
        "createddate",
        "accountcreateddate",
        "contactmqlc",
        "leadmqlc",
        "mql",
        "date",
    ]
    norm_keys = [normalize_key(k) for k in date_keys] if date_keys else default_candidates
    candidates: list[str] = []
    for key in norm_keys:
        candidates.extend([f"{key}_label", key])
    for key in candidates:
        parsed = parse_date(row.get(key))
        if parsed is not None:
            return parsed
    return None


def row_matches_filters(row: dict[str, Any], filters: dict[str, Any]) -> bool:
    if not filters:
        return True
    for key, expected in filters.items():
        norm_key = normalize_key(str(key))
        actual = find_row_value(row, [norm_key, f"{norm_key}_label"])
        actual_text = str(actual or "").strip().lower()
        if isinstance(expected, list):
            options = {str(v).strip().lower() for v in expected}
            if actual_text not in options:
                return False
        else:
            if actual_text != str(expected).strip().lower():
                return False
    return True


def row_value(row: dict[str, Any], aggregation: str) -> float:
    if aggregation == "amount":
        return parse_number(
            find_row_value(
                row,
                [normalize_key("Amount"), normalize_key("Opportunity.Amount"), "amount", "amount_label"],
            )
        )
    return 1.0


def compute_quarter_metric_from_reports(
    crm_client: CRMReportClient,
    metric_name: str,
    metric_cfg: dict[str, Any],
    anchor_date: date,
) -> dict[str, Any]:
    report_ids = metric_cfg.get("report_ids")
    if not report_ids:
        single_id = str(metric_cfg.get("report_id", "")).strip()
        report_ids = [single_id] if single_id else []
    report_ids = [str(r).strip() for r in report_ids if str(r).strip()]
    if not report_ids:
        return {"status": "pending_config", "message": "Set report_id(s)"}

    aggregation = str(metric_cfg.get("aggregation", "count")).strip().lower()
    date_keys = metric_cfg.get("date_keys", ["Close Date", "Created Date", "Date"])
    filters = metric_cfg.get("filters", {})
    include_details = bool(metric_cfg.get("include_details", True))
    window_mode = str(metric_cfg.get("window_mode", "quarter_monthly")).strip().lower()
    q_start, q_end, month_starts = quarter_bounds(anchor_date)
    month_index = {m.strftime("%Y-%m"): idx for idx, m in enumerate(month_starts)}
    month_values = [0.0, 0.0, 0.0]
    period_total = 0.0
    snapshot_total = 0.0
    account_counts: dict[tuple[str, str], int] = {}

    for report_id in report_ids:
        report_data = crm_client.fetch_report(report_id=report_id, include_details=include_details)
        if window_mode == "current_snapshot":
            snapshot_total += extract_report_aggregate_value(report_data, aggregate_index=0)
            continue
        rows = extract_tabular_report_rows(report_data)
        for row in rows:
            if not row_matches_filters(row, filters):
                continue

            dt = row_date_from_candidates(row, date_keys)
            if dt is None:
                continue

            val = row_value(row, aggregation)
            if window_mode == "last_week":
                start, end_exclusive = last_monday_window(anchor_date)
                if not (start <= dt < end_exclusive):
                    continue
                period_total += val
            else:
                if not (q_start <= dt < q_end):
                    continue
                month_key = dt.strftime("%Y-%m")
                idx = month_index.get(month_key)
                if idx is None:
                    continue
                month_values[idx] += val

            if bool(metric_cfg.get("list_accounts", False)):
                acct_name = str(
                    find_row_value(
                        row,
                        [
                            f"{normalize_key('Account Name')}_label",
                            f"{normalize_key('Account.Name')}_label",
                            normalize_key("Account Name"),
                            normalize_key("Account.Name"),
                            "accountname",
                        ],
                    )
                    or "Unknown"
                ).strip()
                acct_num = str(
                    find_row_value(
                        row,
                        [normalize_key("Account Number"), normalize_key("Account.Account_Number__c"), "accountnumber"],
                    )
                    or ""
                ).strip()
                account_counts[(acct_name, acct_num)] = account_counts.get((acct_name, acct_num), 0) + 1

    output = {"report_ids": report_ids, "aggregation": aggregation, "window_mode": window_mode}
    if window_mode == "current_snapshot":
        elapsed = 0
        for m in month_starts:
            if m.strftime("%Y-%m") <= anchor_date.strftime("%Y-%m"):
                elapsed += 1
        series = []
        for i, m in enumerate(month_starts):
            value = snapshot_total if i < elapsed else None
            series.append({"month": m.strftime("%Y-%m"), "label": month_label(m), "value": value})
        output.update(
            {
                "series": series,
                "snapshot_value": snapshot_total,
                "qtd_total": snapshot_total,
                "quarter_start": q_start.isoformat(),
                "quarter_end_exclusive": q_end.isoformat(),
                "series_mode": "snapshot",
            }
        )
    elif window_mode == "last_week":
        start, end_exclusive = last_monday_window(anchor_date)
        output.update(
            {
                "value": period_total,
                "last_week_start": start.isoformat(),
                "last_week_end_exclusive": end_exclusive.isoformat(),
            }
        )
    else:
        series = [
            {
                "month": month_starts[i].strftime("%Y-%m"),
                "label": month_label(month_starts[i]),
                "value": month_values[i],
            }
            for i in range(3)
        ]
        series_mode = str(metric_cfg.get("series_mode", "monthly")).strip().lower()
        if series_mode == "cumulative":
            running = 0.0
            for point in series:
                running += float(point.get("value", 0) or 0)
                point["value"] = running

        output.update(
            {
                "series": series,
                "qtd_total": sum(month_values),
                "quarter_start": q_start.isoformat(),
                "quarter_end_exclusive": q_end.isoformat(),
                "series_mode": series_mode,
            }
        )
    if account_counts:
        accounts = [
            {"account_name": name, "account_number": num, "lost_customer_count": count}
            for (name, num), count in sorted(account_counts.items(), key=lambda x: (-x[1], x[0][0].lower()))
        ]
        output["accounts"] = accounts
        output["account_count"] = len(accounts)
    return output


def build_metrics(config: dict[str, Any]) -> dict[str, Any]:
    workbook_raw = str(config.get("financial_workbook", "")).strip()
    workbook_path = Path(workbook_raw).expanduser() if workbook_raw else None

    output: dict[str, Any] = {"excel": {}, "salesforce": {}, "services": {}, "dashboard": {}, "meta": {}}

    excel_cfg = config.get("excel", {})
    sheet = excel_cfg.get("sheet", "Summary")
    excel_enabled = workbook_path is not None and workbook_path.exists()
    if excel_enabled:
        with XlsxReader(workbook_path) as xlsx:
            # ARR = latest populated numeric value in Revenue row (row 23).
            revenue_row = int(excel_cfg.get("arr_row", 23))
            row_cells = xlsx.get_row_cells(sheet, revenue_row)
            latest_numeric = None
            latest_ref = None
            for ref, cell in row_cells:
                if isinstance(cell.value, (int, float)):
                    latest_numeric = float(cell.value)
                    latest_ref = ref
            if latest_numeric is not None:
                output["excel"]["arr"] = {
                    "value": latest_numeric,
                    "source": {"sheet": sheet, "cell": latest_ref, "row": revenue_row},
                }

            for metric_name, cell_ref in excel_cfg.get("cells", {}).items():
                cell = xlsx.get_cell(sheet, cell_ref)
                output["excel"][metric_name] = {
                    "value": cell.value,
                    "source": {"sheet": sheet, "cell": cell_ref},
                }
    else:
        output["excel"]["status"] = "disabled"
        output["excel"]["message"] = "Workbook not configured or not found; using Salesforce-only mode."

    sf_reports = config.get("salesforce_reports", {})
    sf_quarter_metrics = config.get("salesforce_quarter_metrics", {})
    crm_client: CRMReportClient | None = None
    crm_provider = str((config.get("crm", {}) or {}).get("provider", "salesforce")).strip().lower()
    output["meta"]["crm_provider"] = crm_provider

    def get_crm_client() -> CRMReportClient:
        nonlocal crm_client
        if crm_client is None:
            crm_client = crm_client_from_config(config)
        return crm_client
    financial_cfg = config.get("salesforce_financial_model", {})
    financial_report_id = str(financial_cfg.get("report_id", "")).strip()
    if financial_report_id:
        try:
            client = get_crm_client()
            financial_report = client.fetch_report(report_id=financial_report_id, include_details=True)
            financial_metrics = compute_financial_metrics_from_report(
                financial_report,
                owner_filter=str(financial_cfg.get("owner_filter", "Consolidated")),
            )
            for metric_name in ("arr", "nrr_customer_pct", "nrr_dollar_pct"):
                output["salesforce"][metric_name] = {
                    "value": financial_metrics[metric_name],
                    "report_id": financial_report_id,
                    "model": "financial_sheet_equivalent",
                }
            output["salesforce"]["financial_model_meta"] = {
                "report_id": financial_report_id,
                "owner_filter": str(financial_cfg.get("owner_filter", "Consolidated")),
                "window_start": financial_metrics["window_start"],
                "window_end": financial_metrics["window_end"],
                "won_count_rolling_365": financial_metrics["won_count_rolling_365"],
                "lost_count_rolling_365": financial_metrics["lost_count_rolling_365"],
                "lost_amount_rolling_365": financial_metrics["lost_amount_rolling_365"],
            }
        except (RequestException, ConfigError) as exc:
            output["salesforce"]["financial_model_meta"] = {
                "report_id": financial_report_id,
                "status": "error",
                "message": str(exc),
            }

    quarter_anchor = parse_date(config.get("quarter_anchor_date")) or date.today()
    if sf_quarter_metrics:
        try:
            client = get_crm_client()
            for metric_name, metric_cfg in sf_quarter_metrics.items():
                try:
                    metric_out = compute_quarter_metric_from_reports(
                        crm_client=client,
                        metric_name=metric_name,
                        metric_cfg=metric_cfg or {},
                        anchor_date=quarter_anchor,
                    )
                    output["salesforce"][metric_name] = metric_out
                except (RequestException, ConfigError) as exc:
                    output["salesforce"][metric_name] = {
                        "status": "error",
                        "message": str(exc),
                    }
        except (RequestException, ConfigError) as exc:
            for metric_name in sf_quarter_metrics.keys():
                output["salesforce"][metric_name] = {
                    "status": "error",
                    "message": str(exc),
                }

    services_cfg = config.get("services", {})
    if isinstance(services_cfg, dict) and services_cfg.get("enabled", True):
        try:
            services_source = str(services_cfg.get("source", "clickup")).strip().lower()
            if services_source == "admin_dashboard_db":
                output["services"] = compute_services_from_admin_dashboard(config=config, anchor_date=quarter_anchor)
            else:
                output["services"] = compute_services_from_clickup(config=config, anchor_date=quarter_anchor)
        except (RequestException, ConfigError) as exc:
            output["services"] = {"status": "error", "message": str(exc)}

    runnable_reports = {
        metric_name: report_cfg
        for metric_name, report_cfg in sf_reports.items()
        if str(report_cfg.get("report_id", "")).strip()
        and str(report_cfg.get("value_path", "")).strip()
    }

    for metric_name, report_cfg in sf_reports.items():
        report_id = str(report_cfg.get("report_id", "")).strip()
        value_path = str(report_cfg.get("value_path", "")).strip()
        if metric_name in {"arr", "nrr_customer_pct", "nrr_dollar_pct"} and financial_report_id:
            continue
        if not report_id or not value_path:
            output["salesforce"][metric_name] = {
                "status": "pending_config",
                "message": "Set report_id and value_path",
            }

    if runnable_reports:
        client = get_crm_client()
        for metric_name, report_cfg in runnable_reports.items():
            report_id = report_cfg["report_id"].strip()
            value_path = report_cfg["value_path"].strip()
            try:
                report_data = client.fetch_report(
                    report_id=report_id,
                    include_details=bool(report_cfg.get("include_details", True)),
                )
                value = get_by_path(report_data, value_path)
                output["salesforce"][metric_name] = {
                    "value": value,
                    "report_id": report_id,
                    "value_path": value_path,
                }
            except (RequestException, ConfigError) as exc:
                output["salesforce"][metric_name] = {
                    "status": "error",
                    "message": str(exc),
                    "report_id": report_id,
                    "value_path": value_path,
                }

    preferred_source = str(config.get("preferred_source", "salesforce_then_excel"))
    tracked_metrics = config.get(
        "dashboard_metrics",
        ["arr", "nrr_customer_pct", "nrr_dollar_pct"],
    )
    for metric_name in tracked_metrics:
        sf_metric = output["salesforce"].get(metric_name)
        excel_metric = output["excel"].get(metric_name)
        sf_has_value = isinstance(sf_metric, dict) and ("value" in sf_metric or "qtd_total" in sf_metric)
        excel_has_value = isinstance(excel_metric, dict) and "value" in excel_metric
        sf_value = None
        if isinstance(sf_metric, dict):
            if "value" in sf_metric:
                sf_value = sf_metric["value"]
            elif "qtd_total" in sf_metric:
                sf_value = sf_metric["qtd_total"]

        if preferred_source == "excel_then_salesforce":
            if excel_has_value:
                output["dashboard"][metric_name] = {"value": excel_metric["value"], "source": "excel"}
            elif sf_has_value:
                output["dashboard"][metric_name] = {"value": sf_value, "source": "salesforce"}
            else:
                output["dashboard"][metric_name] = {"status": "missing"}
        else:
            if sf_has_value:
                output["dashboard"][metric_name] = {"value": sf_value, "source": "salesforce"}
            elif excel_has_value:
                output["dashboard"][metric_name] = {"value": excel_metric["value"], "source": "excel"}
            else:
                output["dashboard"][metric_name] = {"status": "missing"}

    if isinstance(output.get("services"), dict) and output["services"].get("status") == "ok":
        output["dashboard"]["services_active_projects"] = {
            "value": output["services"].get("active_projects", 0),
            "source": "services",
        }
        output["dashboard"]["services_closed_projects_this_quarter"] = {
            "value": output["services"].get("closed_projects_this_quarter", 0),
            "source": "services",
        }
        output["dashboard"]["services_avg_project_close_days_this_quarter"] = {
            "value": output["services"].get("avg_project_close_days_this_quarter", 0.0),
            "source": "services",
        }
        output["dashboard"]["services_overall_project_status"] = {
            "value": output["services"].get("overall_project_status", "green"),
            "source": "services",
        }

    output["meta"] = {
        "workbook": str(workbook_path) if workbook_path else None,
        "summary_sheet": sheet,
        "preferred_source": preferred_source,
        "quarter_anchor_date": quarter_anchor.isoformat(),
    }
    return output


def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))

    config_path = Path(os.getenv("DASHBOARD_CONFIG_PATH", "dashboard_config.json"))
    if not config_path.exists():
        raise SystemExit(
            f"Missing config file: {config_path}. Create it from dashboard_config.template.json."
        )

    config = read_json(config_path)
    metrics = build_metrics(config)

    output_path = Path(config.get("output_path", "dashboard_metrics_output.json"))
    output_path.write_text(json.dumps(metrics, indent=2))
    print(f"Wrote dashboard metrics -> {output_path}")


if __name__ == "__main__":
    main()
