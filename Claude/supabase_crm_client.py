"""
Supabase CRM Report Client (v2 — fixed value/label semantics)
============================================================

Drop-in adapter that lets dashboard_metrics.py read from the Medcurity
CRM (Supabase) instead of Salesforce, while keeping the existing
SF-shaped factMap return contract so downstream code (factMap parsers,
quarter aggregators) does not change.

CRITICAL CONTRACT NOTES
-----------------------
The dashboard's downstream parsers (in dashboard_metrics.py) call
extract_tabular_report_rows() which builds a flat dict keyed by the
NORMALIZED column name (lowercase, alphanum only) — e.g. "Close Date"
becomes "closedate". They then look up dates by trying the label
variant FIRST (e.g. "closedate_label") then the raw value. The
label is what `parse_date()` actually parses, since SF returns nicely
formatted strings there ("2026-04-15") while the raw value can be an
unparseable epoch.

So for date columns we MUST set BOTH cell.value AND cell.label to a
date string parse_date() understands ("YYYY-MM-DD" works).

For boolean columns (one_time_project, etc.), parse_bool() handles
"true"/"True"/"1"/"yes". Keep cell.value as a real bool; cell.label
as the same.

For amounts, parse_number() handles raw numbers + "$1,234.56" style
strings. Use a real number in cell.value.

Stage values: the CRM uses lowercase enums ('closed_won', 'closed_lost').
The financial-metrics computer expects "Closed Won" / "Closed Lost"
SF-style display strings. Map them on the way out.

Lead source: the financial-metrics computer matches against a fixed
set of strings in lowercase. The CRM's lead_source enum already
uses lowercase, so just pass through.

Two Supabase projects — DO NOT confuse them
-------------------------------------------
This adapter reads from the CRM project (white-flower) using:
    CRM_SUPABASE_URL / CRM_SUPABASE_ANON_KEY

The dashboard's own state DB stays pointed at:
    SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY  (a DIFFERENT project)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone, timedelta
from typing import Any, Callable
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

# Stage enum → SF-style display name. The financial-metrics computer
# does string-equals matching, so we have to mirror SF's exact casing.
_STAGE_DISPLAY = {
    "closed_won":              "Closed Won",
    "closed_lost":             "Closed Lost",
    "details_analysis":        "Details Analysis",
    "demo":                    "Demo",
    "proposal_and_price_quote":"Proposal and Price Quote",
    "proposal_conversation":   "Proposal Conversation",
    "lead":                    "Lead",
    "qualified":               "Qualified",
    "proposal":                "Proposal",
    "verbal_commit":           "Verbal Commit",
}


def _stage_label(stage: Any) -> str:
    if not stage:
        return ""
    return _STAGE_DISPLAY.get(str(stage).strip().lower(), str(stage))


def _date_str(value: Any) -> str:
    """Return YYYY-MM-DD or empty string. Used for both .value AND .label."""
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return ""
    # Already YYYY-MM-DD?
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Try to parse and re-emit
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _money(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _empty_factmap() -> dict[str, Any]:
    return {
        "factMap": {"T!T": {"aggregates": [{"value": 0.0, "label": "0"}], "rows": []}},
        "reportMetadata": {"detailColumns": []},
        "reportExtendedMetadata": {"detailColumnInfo": {}},
    }


def _build_factmap(
    aggregate_value: float,
    rows: list[dict[str, Any]],
    columns: list[tuple[str, str]],
) -> dict[str, Any]:
    """
    Compose a SF-shaped factMap.

    columns: ordered list of (api_name, ui_label, kind) tuples where
    kind is one of:
      - 'date'    → both value + label become YYYY-MM-DD
      - 'money'   → value is float, label is "$X,XXX.XX"-ish string
      - 'bool'    → value is True/False, label is "true"/"false"
      - 'string'  → value + label are str()
      - 'raw'     → value passes through as-is, label is str()
    For backward compat, columns may be plain (api_name, ui_label) — kind
    defaults to 'string'.

    rows: list of dicts keyed by api_name → raw value.
    """
    api_names: list[str] = []
    ui_labels: list[str] = []
    kinds: list[str] = []
    for col in columns:
        if len(col) == 3:
            api, ui, kind = col
        else:
            api, ui = col  # type: ignore[misc]
            kind = "string"
        api_names.append(api)
        ui_labels.append(ui)
        kinds.append(kind)

    data_rows: list[dict[str, Any]] = []
    for r in rows:
        cells: list[dict[str, Any]] = []
        for api, kind in zip(api_names, kinds):
            v = r.get(api)
            if kind == "date":
                ds = _date_str(v)
                cells.append({"value": ds, "label": ds})
            elif kind == "money":
                amt = _money(v)
                cells.append({"value": amt, "label": f"${amt:,.2f}"})
            elif kind == "bool":
                b = bool(v)
                cells.append({"value": b, "label": "true" if b else "false"})
            elif kind == "raw":
                cells.append({"value": v, "label": "" if v is None else str(v)})
            else:  # 'string'
                cells.append({"value": v, "label": "" if v is None else str(v)})
        data_rows.append({"dataCells": cells})

    return {
        "factMap": {
            "T!T": {
                "aggregates": [{"value": aggregate_value, "label": f"{aggregate_value:,.2f}"}],
                "rows": data_rows,
            }
        },
        "reportMetadata": {"detailColumns": api_names},
        "reportExtendedMetadata": {
            "detailColumnInfo": {
                api: {"label": ui, "dataType": "string"}
                for api, ui in zip(api_names, ui_labels)
            }
        },
    }


# ---------------------------------------------------------------------
# Transformers per report
# ---------------------------------------------------------------------

def _t_arr_base_dataset(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """
    SF financial model report (00O5w000009E4ZyEAK).

    UPDATED 2026-04-27: now reads from the `opportunities` table directly
    (NOT v_arr_base_dataset) so the financial computer sees EVERY opp,
    not just the ones the view exposes. v_arr_base_dataset filters out
    one_time_project=true and name='Customer Service' which the
    spreadsheet formula does NOT — that mismatch was causing ARR to
    show ~$576K instead of the expected ~$997K.

    The downstream computer (compute_financial_metrics_from_report)
    mirrors the spreadsheet's column-X formula:
        IF one_time_project → skip
        IF stage='Closed Won' AND close_date in last 365 days → += amount
        Owner filter applied per dashboard_config "owner_filter"

    Rows arrive from PostgREST with embedded owner via the
    `owner:user_profiles` syntax (see _fetch_view).
    """
    columns = [
        ("Close Date",       "Close Date",       "date"),
        ("Stage",            "Stage",            "string"),
        ("Opportunity Owner","Opportunity Owner","string"),
        ("Amount",           "Amount",           "money"),
        ("One Time Project", "One Time Project", "bool"),
        ("Lead Source",      "Lead Source",      "string"),
        ("Account Name",     "Account Name",     "string"),
        ("Opportunity Name", "Opportunity Name", "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    total = 0.0
    for r in rows:
        amt = _money(r.get("amount"))
        total += amt
        # Owner can come from a join (owner.full_name) or a flat field
        owner = "Unassigned"
        owner_obj = r.get("owner")
        if isinstance(owner_obj, dict) and owner_obj.get("full_name"):
            owner = owner_obj["full_name"]
        elif r.get("opportunity_owner"):
            owner = r["opportunity_owner"]
        # Account name from join
        acct_name = None
        acct_obj = r.get("account")
        if isinstance(acct_obj, dict):
            acct_name = acct_obj.get("name")
        if not acct_name:
            acct_name = r.get("account_name")
        out_rows.append({
            "Close Date":        _date_str(r.get("close_date")),
            "Stage":             _stage_label(r.get("stage")),
            "Opportunity Owner": owner,
            "Amount":            amt,
            "One Time Project":  bool(r.get("one_time_project")),
            "Lead Source":       (r.get("lead_source") or "").lower(),
            "Account Name":      acct_name,
            "Opportunity Name":  r.get("name") or r.get("opportunity_name"),
        })
    return _build_factmap(total, out_rows, columns)


def _t_dashboard_arr_financial(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Pre-aggregated single-row view from
    supabase/migrations/20260427000001_dashboard_arr_view.sql

    Returns a SF-shaped factMap that signals to the patched
    compute_financial_metrics_from_report (in install_supabase_provider.py)
    to skip row iteration and read the scalars from
    reportMetadata.precomputed_metrics.
    """
    if not rows:
        precomputed: dict[str, Any] = {}
    else:
        precomputed = rows[0]
    arr_amount = _money(precomputed.get("arr"))
    return {
        "factMap": {
            "T!T": {
                "aggregates": [{"value": arr_amount, "label": f"${arr_amount:,.2f}"}],
                "rows": [],
            }
        },
        "reportMetadata": {
            "detailColumns": [],
            "source": "supabase_financial_view",
            "precomputed_metrics": precomputed,
        },
        "reportExtendedMetadata": {"detailColumnInfo": {}},
    }


def _row_owner(r: dict[str, Any]) -> str:
    """All CRM views expose owner via opportunity_owner (or lead_owner /
    account_owner for non-opp views). Try the most common keys."""
    return (
        r.get("opportunity_owner")
        or r.get("lead_owner")
        or r.get("account_owner")
        or r.get("owner_name")
        or "Unassigned"
    )


def _row_full_name(r: dict[str, Any]) -> str:
    """For MQL contacts/leads — combine first + last when present."""
    fn = (r.get("first_name") or "").strip()
    ln = (r.get("last_name") or "").strip()
    if fn or ln:
        return f"{fn} {ln}".strip()
    return (
        r.get("account_name")
        or r.get("contact_name")
        or r.get("lead_name")
        or r.get("name")
        or ""
    )


def _t_renewals_qtd(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """SF renewals → v_renewals_qtd. aggregation=amount, date=close_date."""
    columns = [
        ("Close Date",       "Close Date",       "date"),
        ("Account Name",     "Account Name",     "string"),
        ("Opportunity Name", "Opportunity Name", "string"),
        ("Amount",           "Amount",           "money"),
        ("Opportunity Owner","Opportunity Owner","string"),
        ("Stage",            "Stage",            "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    total = 0.0
    for r in rows:
        amt = _money(r.get("amount"))
        total += amt
        out_rows.append({
            "Close Date":        _date_str(r.get("close_date")),
            "Account Name":      r.get("account_name"),
            "Opportunity Name":  r.get("opportunity_name"),
            "Amount":            amt,
            "Opportunity Owner": _row_owner(r),
            "Stage":             _stage_label(r.get("stage")),
        })
    return _build_factmap(total, out_rows, columns)


def _t_new_sales(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """v_new_customers_qtd has no `stage` column (already filtered).

    NOTE: include the opportunity `id` as the first column so that
    extract_tabular_report_rows()'s row-signature dedup never collapses two
    distinct opps that happen to share Close Date + Account + Owner + Amount
    + Lead Source. Without this the chart's count diverged from the SQL-side
    KPI tile (count(*) from v_new_customers_qtd).
    """
    columns = [
        ("Id",               "Id",               "string"),
        ("Close Date",       "Close Date",       "date"),
        ("Account Name",     "Account Name",     "string"),
        ("Opportunity Name", "Opportunity Name", "string"),
        ("Amount",           "Amount",           "money"),
        ("Opportunity Owner","Opportunity Owner","string"),
        ("Lead Source",      "Lead Source",      "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    total = 0.0
    for r in rows:
        amt = _money(r.get("amount"))
        total += amt
        out_rows.append({
            "Id":                r.get("id"),
            "Close Date":        _date_str(r.get("close_date")),
            "Account Name":      r.get("account_name"),
            "Opportunity Name":  r.get("opportunity_name"),
            "Amount":            amt,
            "Opportunity Owner": _row_owner(r),
            "Lead Source":       (r.get("lead_source") or "").lower(),
        })
    return _build_factmap(total, out_rows, columns)


def _t_lost_customers_qtd(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """v_lost_customers_qtd has no account_number column."""
    columns = [
        ("Close Date",       "Close Date",       "date"),
        ("Account Name",     "Account Name",     "string"),
        ("Opportunity Name", "Opportunity Name", "string"),
        ("Amount",           "Amount",           "money"),
        ("Opportunity Owner","Opportunity Owner","string"),
        ("Stage",            "Stage",            "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        out_rows.append({
            "Close Date":        _date_str(r.get("close_date")),
            "Account Name":      r.get("account_name"),
            "Opportunity Name":  r.get("opportunity_name"),
            "Amount":            _money(r.get("amount")),
            "Opportunity Owner": _row_owner(r),
            "Stage":             _stage_label(r.get("stage")),
        })
    return _build_factmap(float(len(out_rows)), out_rows, columns)


def _t_active_pipeline(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """current_snapshot mode — downstream uses the aggregate scalar."""
    columns = [
        ("Close Date",       "Close Date",       "date"),
        ("Account Name",     "Account Name",     "string"),
        ("Opportunity Name", "Opportunity Name", "string"),
        ("Amount",           "Amount",           "money"),
        ("Opportunity Owner","Opportunity Owner","string"),
        ("Stage",            "Stage",            "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    total = 0.0
    for r in rows:
        amt = _money(r.get("amount"))
        total += amt
        out_rows.append({
            "Close Date":        _date_str(r.get("close_date")),
            "Account Name":      r.get("account_name"),
            "Opportunity Name":  r.get("opportunity_name"),
            "Amount":            amt,
            "Opportunity Owner": _row_owner(r),
            "Stage":             _stage_label(r.get("stage")),
        })
    return _build_factmap(total, out_rows, columns)


def _t_sql_accounts(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """v_sql_accounts: not yet introspected — guess at columns and fall
    back to whatever's there. The downstream just counts rows where the
    sql_date is in the current quarter. Tries common column names."""
    columns = [
        ("SQL",          "SQL",          "date"),
        ("Created Date", "Created Date", "date"),
        ("Account Name", "Account Name", "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        sql_dt = _date_str(
            r.get("sql_date")
            or r.get("first_sql_date")
            or r.get("earliest_sql_date")
            or r.get("mql_date")  # if SQL view doesn't exist, mql_date is closest fallback
        )
        out_rows.append({
            "SQL":          sql_dt,
            "Created Date": sql_dt,  # fallback so date_keys lookup succeeds
            "Account Name": r.get("account_name"),
        })
    return _build_factmap(float(len(out_rows)), out_rows, columns)


def _t_mql_combined(rows: list[dict[str, Any]], _cfg: dict[str, Any]) -> dict[str, Any]:
    """v_mql_contacts / v_mql_leads_qtd. No created_at — use mql_date
    for both date keys to satisfy any path the downstream tries."""
    columns = [
        ("MQL",            "MQL",            "date"),
        ("Created Date",   "Created Date",   "date"),
        ("Contact.MQL__c", "Contact.MQL__c", "date"),
        ("Lead.MQL__c",    "Lead.MQL__c",    "date"),
        ("Name",           "Name",           "string"),
    ]
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        mql_dt = _date_str(r.get("mql_date") or r.get("first_mql_date") or r.get("earliest_mql_date"))
        out_rows.append({
            "MQL":             mql_dt,
            "Created Date":    mql_dt,
            "Contact.MQL__c":  mql_dt,
            "Lead.MQL__c":     mql_dt,
            "Name":            _row_full_name(r),
        })
    return _build_factmap(float(len(out_rows)), out_rows, columns)


# ---------- Mapping table --------------------------------------------
# SF report ID → (supabase view OR raw table with select expr,
#                 transformer,
#                 query_params dict)
#
# For the financial model report we hit the underlying `opportunities`
# table directly (NOT v_arr_base_dataset) so we get every opp the
# spreadsheet would have seen — including 'Customer Service' and
# anything else the view filters out. The spreadsheet formula does
# its own one-time-project filter inside the X column logic, which
# the downstream financial computer faithfully replicates.
SF_REPORT_TO_SUPABASE: dict[str, tuple[str, Callable, dict[str, str]]] = {
    # ARR + NRR come from a dedicated single-row view that mirrors the
    # financial spreadsheet formula. We bypass the row-by-row financial
    # computer entirely and return the scalar directly. See
    # supabase/migrations/20260427000001_dashboard_arr_view.sql in the
    # CRM repo.
    "00O5w000009E4ZyEAK": (
        "v_dashboard_arr_financial",
        lambda rows, _cfg: _t_dashboard_arr_financial(rows),
        {},
    ),
    "00ORO00000CN3nu2AD": ("v_renewals_qtd",         _t_renewals_qtd,     {}),
    "00ORO00000CMuW82AL": ("v_new_customers_qtd",    _t_new_sales,        {}),
    "00O5w000008XOz1EAG": ("v_active_pipeline",      _t_active_pipeline,  {}),
    "00ORO00000FHOX92AP": ("v_new_customers_qtd",    _t_new_sales,        {}),
    "00O5w000009FLTdEAO": ("v_lost_customers_qtd",   _t_lost_customers_qtd, {}),
    "00ORO000002i21m2AA": ("v_sql_accounts",         _t_sql_accounts,     {}),
    "00O5w000009F5BNEA0": ("v_mql_contacts",         _t_mql_combined,     {}),
    "00O5w000009E9WWEA0": ("v_mql_leads_qtd",        _t_mql_combined,     {}),
}


# ---------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------

class SupabaseCRMReportClient:
    provider = "supabase"

    # PostgREST default page size is 1000. Some views have more rows
    # (v_arr_base_dataset on prod has ~2000 opps). Use a large limit.
    PAGE_LIMIT = 5000

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        cfg = (config or {}).get("crm", {}) if config else {}
        self._url = (cfg.get("supabase_url") or os.environ.get("CRM_SUPABASE_URL") or "").rstrip("/")
        self._key = cfg.get("supabase_anon_key") or os.environ.get("CRM_SUPABASE_ANON_KEY") or ""

        if not self._url or not self._key:
            raise RuntimeError(
                "SupabaseCRMReportClient requires CRM_SUPABASE_URL + CRM_SUPABASE_ANON_KEY. "
                "These point at the Medcurity CRM project, NOT the dashboard's own state DB."
            )

    def fetch_report(
        self,
        report_id: str,
        include_details: bool = True,  # noqa: ARG002
        standard_date_column: str | None = None,  # noqa: ARG002
        standard_start_date: str | None = None,  # noqa: ARG002
        standard_end_date: str | None = None,  # noqa: ARG002
    ) -> dict[str, Any]:
        mapping = SF_REPORT_TO_SUPABASE.get(report_id)
        if not mapping:
            logger.warning("Supabase CRM: no mapping for SF report id %s", report_id)
            return _empty_factmap()

        view_name, transformer, params = mapping
        try:
            rows = self._fetch_view(view_name, params)
        except Exception:  # noqa: BLE001
            logger.exception("Supabase CRM: fetch failed for report %s (view %s)", report_id, view_name)
            return _empty_factmap()

        try:
            result = transformer(rows, {})
            logger.info(
                "Supabase CRM report %s (view %s): %d rows, aggregate=%s",
                report_id, view_name, len(rows),
                result.get("factMap", {}).get("T!T", {}).get("aggregates", [{}])[0].get("value"),
            )
            return result
        except Exception:  # noqa: BLE001
            logger.exception("Supabase CRM: transformer failed for report %s", report_id)
            return _empty_factmap()

    def _fetch_view(self, view_name: str, params: dict[str, str]) -> list[dict[str, Any]]:
        # Caller can override the default `select=*` by including a
        # `select` key in params (e.g. for embeds against raw tables).
        # Other params become PostgREST filters: amount=gte.0,
        # stage=eq.closed_won, etc.
        select_expr = params.get("select", "*")
        query = f"select={quote(select_expr, safe=',():!')}&limit={self.PAGE_LIMIT}"
        for k, v in params.items():
            if k == "select":
                continue
            query += f"&{quote(k)}={quote(v, safe='.,:')}"
        url = f"{self._url}/rest/v1/{view_name}?{query}"

        req = Request(url, headers={
            "apikey": self._key,
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/json",
            "Accept-Profile": "public",
        })

        try:
            with urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8")
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase view {view_name} HTTP {e.code}: {body[:300]}") from e
        except URLError as e:
            raise RuntimeError(f"Supabase view {view_name} network error: {e}") from e

        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Supabase view {view_name} non-JSON response: {body[:300]}") from e

        if not isinstance(data, list):
            raise RuntimeError(f"Supabase view {view_name} returned non-list: {type(data).__name__}")
        return data
