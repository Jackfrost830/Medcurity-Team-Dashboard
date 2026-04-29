"""
Install the Supabase CRM provider into dashboard_metrics without
editing the original file.

WHY this approach:
- Keeps dashboard_metrics.py 100% as Codex shipped it. If we ever need
  to roll back to Salesforce, no file diff to revert.
- Importing this module mutates dashboard_metrics.crm_client_from_config
  to recognize provider="supabase" in addition to "salesforce" and
  "pipedrive".

USAGE:
- Import this module ONCE somewhere on the dashboard's import path
  before crm_client_from_config is called.
- The recommended place is at the top of api/dashboard_data.py (or any
  other api/* entry point) — see Claude/api_patch_example.py for the
  canonical 3-line snippet.

Activation is config-driven. To switch the dashboard to read from the
Medcurity CRM Supabase project, set in dashboard_config.json:

    "crm": {
      "provider": "supabase"
    }

To roll back to Salesforce, set provider back to "salesforce" — no
code changes required.
"""

from __future__ import annotations

import logging
from datetime import date

import dashboard_metrics  # the original, unmodified file
from Claude.supabase_crm_client import SupabaseCRMReportClient  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Patch 1: crm_client_from_config — recognize provider="supabase"
# ---------------------------------------------------------------------
_original_crm_client_from_config = dashboard_metrics.crm_client_from_config


def _patched_crm_client_from_config(config):
    crm_cfg = config.get("crm", {}) if isinstance(config.get("crm"), dict) else {}
    provider = str(crm_cfg.get("provider", "salesforce")).strip().lower()
    if provider == "supabase":
        logger.info("crm_client_from_config: using Supabase CRM provider")
        return SupabaseCRMReportClient(config)
    return _original_crm_client_from_config(config)


dashboard_metrics.crm_client_from_config = _patched_crm_client_from_config


# ---------------------------------------------------------------------
# Patch 2: compute_financial_metrics_from_report — when the report
# came from the Supabase financial view, the rows are pre-aggregated.
# Read scalars off the rows[0] dict directly instead of running the
# row-iteration logic (which would always return 0).
# ---------------------------------------------------------------------
_original_compute_financial = dashboard_metrics.compute_financial_metrics_from_report


def _patched_compute_financial_metrics_from_report(
    report_data, owner_filter="Consolidated", as_of_date=None
):
    """
    Detect Supabase pre-aggregated payload by checking for a marker in
    reportMetadata (the SF flow won't have it). When present, return
    the scalars directly. Otherwise fall through to the original SF
    row-iteration logic.
    """
    metadata = report_data.get("reportMetadata", {}) if isinstance(report_data, dict) else {}
    if metadata.get("source") == "supabase_financial_view":
        precomputed = metadata.get("precomputed_metrics", {})
        today = as_of_date or date.today()
        result = {
            "arr":                       float(precomputed.get("arr", 0) or 0),
            "nrr_dollar_pct":            (float(precomputed.get("nrr_dollar_pct") or 0) / 100.0)
                                          if precomputed.get("nrr_dollar_pct") is not None else 0.0,
            "nrr_customer_pct":          (float(precomputed.get("nrr_customer_pct") or 0) / 100.0)
                                          if precomputed.get("nrr_customer_pct") is not None else 0.0,
            "window_start":              str(precomputed.get("window_start") or ""),
            "window_end":                str(precomputed.get("window_end") or today.isoformat()),
            "won_count_rolling_365":     int(precomputed.get("won_count_rolling_365", 0) or 0),
            "lost_count_rolling_365":    int(precomputed.get("lost_count_rolling_365", 0) or 0),
            "lost_amount_rolling_365":   float(precomputed.get("lost_amount_rolling_365", 0) or 0),
        }
        logger.info(
            "compute_financial_metrics: using Supabase precomputed scalars — ARR=%s, NRR$=%s%%, NRR#=%s%%",
            result["arr"], precomputed.get("nrr_dollar_pct"), precomputed.get("nrr_customer_pct"),
        )
        return result
    return _original_compute_financial(report_data, owner_filter, as_of_date)


dashboard_metrics.compute_financial_metrics_from_report = _patched_compute_financial_metrics_from_report

logger.debug("Supabase CRM provider patches installed: crm_client_from_config + compute_financial_metrics_from_report")
