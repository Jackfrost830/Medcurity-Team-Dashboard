# Salesforce Dashboard Data Pipeline

Builds a live JSON payload plus browser preview pages for the weekly dashboard.

## Run data refresh
```bash
python3 dashboard_metrics.py
```

Output: `dashboard_metrics_output.json`

## Generate preview visuals
```bash
python3 generate_dashboard_preview.py
```

Outputs:
- `dashboard_preview.html`
- `goals_admin.html`
- `dashboard_goals.json` (default goals seed)

## Supabase Backend (single production project)

This repo now supports Supabase-backed persistence for:
- weekly snapshot history (`dashboard_history`)
- quarter state (`dashboard_quarter_state`): goals, quote/billing content, milestones
- services quarter overrides (`dashboard_service_overrides`): historical closed-project and avg-close-days corrections

### 1) Fill env
Add these to `.env`:
```bash
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_DASHBOARD_HISTORY_TABLE=dashboard_history
SUPABASE_DASHBOARD_STATE_TABLE=dashboard_quarter_state
SUPABASE_DASHBOARD_SERVICE_OVERRIDES_TABLE=dashboard_service_overrides
```

### 2) Apply SQL
Run the migration in Supabase SQL editor:
- `supabase_dashboard_backend.sql`
or
- `medcurity-crm/supabase/migrations/20260401_dashboard_backend.sql`

### 3) Runtime routes
Vercel rewrites now point to dynamic API-rendered pages:
- `/` -> team view
- `/dashboard_preview`
- `/goals_admin`

### 4) Daily refresh sync
`run_dashboard_refresh.sh` now calls:
```bash
python3 sync_dashboard_backend.py
```
so local snapshots/state can be pushed to Supabase when env keys are configured.

For historical quarter service metrics, edit:
- `dashboard_services_overrides.json`

Current seed includes:
- `Q1-2026`: `closed_projects_this_quarter=8`, `avg_project_close_days_this_quarter=52`

## Current behavior
- CRM provider is config-driven (`crm.provider` in `dashboard_config.json`):
  - `salesforce` (default, current production path)
  - `pipedrive` scaffold via `crm.pipedrive_reports_json_path` (JSON report-map input)
- Financial model from Salesforce report `00O5w000009E4ZyEAK`:
  - `arr`, `nrr_customer_pct`, `nrr_dollar_pct`
- Quarter monthly metrics (Jan/Feb/Mar style):
  - `new_sales`, `total_active_pipeline`, `new_customers`, `sql`, `mql`, `renewals_number`
- Lost customers metric:
  - `lost_customers` now uses `window_mode: last_week`
  - returns count + account list for the window from last Monday through today
- Renewals chart now uses amount values (not count)

## Goals and color logic
Open `goals_admin.html` to set month1/month2/month3 goals. Dashboard line segments + dots auto-color:
- Red: below required target
- Yellow: month 2/3 only, when current month beats prior month goal but not current month goal
- Green: current month goal achieved

## Sections in preview
- Sales: ARR, New Sales, Total Active Pipeline, New Customers
- Marketing: SQL, MQL
- Customer Success: NRR by Customer, NRR by Dollar, Renewals amount, Lost customers last week
