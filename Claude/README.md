# Claude / Supabase CRM provider for the Team Dashboard

This folder swaps the dashboard's data source from **Salesforce reports** to the **Medcurity CRM Supabase project** (the one at `crm.medcurity.com`). Originals stay untouched — this is purely additive.

If anything goes wrong, you can switch back to Salesforce in a single config flag flip with no code changes.

---

## Files in this folder

| File | Purpose |
|---|---|
| `supabase_crm_client.py` | The actual adapter. Maps each SF report ID to a Supabase view + transforms the response into the SF-shaped factMap that `dashboard_metrics.py` expects. |
| `install_supabase_provider.py` | One-import shim that monkey-patches `dashboard_metrics.crm_client_from_config` to recognize `provider = "supabase"`. Keeps the original SF code in `dashboard_metrics.py` 100% untouched. |
| `README.md` | This file. |

---

## How the swap works

1. The original `dashboard_metrics.py` has a `crm_client_from_config()` function that picks between `SalesforceReportClient` and `PipedriveReportClient` based on `dashboard_config.json`'s `crm.provider`.
2. `install_supabase_provider.py` patches that function in-memory to also recognize `crm.provider = "supabase"`.
3. When called with `"supabase"`, it returns a `SupabaseCRMReportClient` that hits the CRM Supabase REST API for each SF report ID and returns rows shaped exactly like SF would.

Downstream code (factMap parsers, quarter aggregators, runtime renderers) does **not change** — they see the same shape they always did.

---

## Two Supabase projects — DO NOT confuse them

This dashboard talks to TWO separate Supabase projects:

| Project | Used for | Env var prefix |
|---|---|---|
| **Team Dashboard Supabase** (`pvdxaokyithabjqaaewj`) | Editable state (quote, billing, weekly snapshots). Tables: `dashboard_quarter_state`, `dashboard_history`, etc. | `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` |
| **Medcurity CRM Supabase** (white-flower / prod CRM) | Sales metrics from CRM views (`v_dashboard_metrics`, `v_active_pipeline`, etc.) | `CRM_SUPABASE_URL`, `CRM_SUPABASE_ANON_KEY` |

The new env vars are explicitly prefixed `CRM_*` so they can never collide with the existing dashboard state vars.

---

## Activation steps

### 1. Add the two new env vars to Vercel

In the Vercel dashboard for the team-dashboard project, go to **Settings → Environment Variables** and add:

```
CRM_SUPABASE_URL       = https://<crm-prod-project-ref>.supabase.co
CRM_SUPABASE_ANON_KEY  = <anon key from the CRM Supabase project>
```

You can find both in the **CRM** Supabase dashboard → **Project Settings → API**. **Use the anon key, not the service-role key.** RLS protects the data; anon is safe in browsers (and we only call it from server-side API routes anyway).

Set the env vars for **Production**, **Preview**, AND **Development**.

### 2. Add the import to your API entry points

At the **top** of these files (or wherever else `crm_client_from_config` is called), add ONE line:

```python
import Claude.install_supabase_provider  # noqa: F401  – installs the Supabase CRM provider
```

The files that need it:
- `api/dashboard_data.py`
- `api/dashboard_team_view.py`
- `api/dashboard_preview.py`
- `api/lock_weekly_snapshot.py`

(If you'd rather not edit the api/ files at all, you can instead add the import to `dashboard_runtime.py` near the top — every API entry point goes through it. Either way, the import only needs to happen ONCE per process.)

### 3. Flip the config flag

Edit `dashboard_config.json`:

```json
{
  ...,
  "crm": {
    "provider": "supabase"
  }
}
```

(If `"crm"` doesn't exist yet, just add it at the top level.)

### 4. Deploy

```bash
npx --yes vercel deploy --prod --yes
```

Or push to whatever branch Vercel auto-deploys from.

---

## Verify

After deploy:

1. Hit `https://medcurity-team-dashboard-site.vercel.app/dashboard_data` (or whatever your data endpoint is) — should return JSON with non-zero ARR / pipeline / etc.
2. Hit the team view page — should render with current numbers from the CRM, not Salesforce.
3. The hardcoded `arr` / `total_active_pipeline` overrides in `dashboard_config.json`'s `hardcoded_overrides` block should be unnecessary now. Numbers should match what you see in the CRM at `crm.medcurity.com/reports/standard/dashboard-metrics`.

---

## Roll back to Salesforce

Set in `dashboard_config.json`:

```json
"crm": {
  "provider": "salesforce"
}
```

Re-deploy. That's it. The Supabase provider stays installed but dormant.

---

## SF report ID → Supabase view mapping

| SF report ID | Supabase view | What it drives |
|---|---|---|
| `00O5w000009E4ZyEAK` | `v_arr_base_dataset` | Financial workbook ARR computation |
| `00ORO00000CN3nu2AD` | `v_renewals_qtd` | Renewals YTD chart |
| `00ORO00000CMuW82AL` | `v_new_customers_qtd` | New Sales (amount) |
| `00O5w000008XOz1EAG` | `v_active_pipeline` | Total Active Pipeline ($) |
| `00ORO00000FHOX92AP` | `v_new_customers_qtd` | New Customers (count) |
| `00O5w000009FLTdEAO` | `v_lost_customers_qtd` | Lost Customers list |
| `00ORO000002i21m2AA` | `v_sql_accounts` | SQL count |
| `00O5w000009F5BNEA0` | `v_mql_contacts` | MQL contacts |
| `00O5w000009E9WWEA0` | `v_mql_leads_qtd` | MQL leads |

Single-scalar metrics (NRR percentages currently sourced from Excel) can also be pulled from `v_dashboard_metrics` if you want to retire the Excel dependency — see commented examples in `supabase_crm_client.py`.

---

## What if a metric is wrong?

The mapping table lives in **`supabase_crm_client.py`** under `SF_REPORT_TO_SUPABASE`. Update the view name or transformer there. No need to touch `dashboard_metrics.py`.

If a CRM view itself returns wrong data, that's a CRM-side fix in the `medcurity-crm` repo (under `supabase/migrations/`). Both are versioned in git.

---

## Failure mode

If the Supabase call fails (network, 500, view doesn't exist), the adapter logs the error and returns an empty factMap. Downstream code reads that as zero rather than crashing the whole dashboard. The dashboard will continue to render with that metric showing 0 / empty until the Supabase issue is resolved.
