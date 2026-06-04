# Claude Code Handoff - Medcurity Team Dashboard

Last updated: 2026-04-27 (America/Los_Angeles)

## 1) What this project is
This repo runs the Medcurity dashboard experience (team view + admin preview + goals admin) with:
- live Salesforce/ClickUp refresh on current views
- Supabase-backed quarter state and weekly historical snapshots
- Vercel API-first rendering (not static HTML-only in production)

Primary production URL:
- `https://medcurity-team-dashboard-site.vercel.app`

## 2) Source of truth and architecture
There are 3 core layers.

1. Metrics layer
- File: `dashboard_metrics.py`
- Purpose: pull and normalize Salesforce + ClickUp + optional spreadsheet data.
- Main output: `dashboard_metrics_output.json`

2. Runtime/render layer
- File: `dashboard_runtime.py`
- Purpose: build runtime payload, merge quarter history, apply snapshot behavior, and render HTML templates.
- API handlers call this at request time.

3. Persistence/state layer
- File: `dashboard_state_backend.py`
- Purpose: quarter state read/write to Supabase (or local fallback).
- Tables used: `dashboard_history`, `dashboard_quarter_state`, `dashboard_refresh_runs`, `dashboard_service_overrides`.

## 3) Key file map (where everything is)
### Runtime + generation
- `dashboard_metrics.py` -> core data pull + metric logic
- `generate_dashboard_preview.py` -> local HTML generation and chart logic
- `dashboard_runtime.py` -> production request-time payload + snapshot logic
- `dashboard_config.json` -> report IDs, metric windows, source behavior

### API routes (Vercel)
- `api/dashboard_team_view.py`
- `api/dashboard_preview.py`
- `api/goals_admin.py`
- `api/dashboard_data.py`
- `api/dashboard_state.py`
- `api/lock_weekly_snapshot.py`
- shared helper: `api/_dashboard_response.py`

### Persistence + backend sync
- `dashboard_state_backend.py`
- `sync_dashboard_backend.py`
- `supabase_dashboard_backend.sql`
- `dashboard_services_overrides.json`

### Deployment and scheduling
- `vercel.json` (routes + cron)
- `run_dashboard_refresh.sh` (local refresh/generate/sync workflow)
- `com.medcurity.dashboard-refresh.plist` (local launchd schedule)

### Working docs
- `DASHBOARD_HANDOFF.md`
- `DASHBOARD_CHANGELOG.md`
- `README.md`

## 4) Routing contract (critical)
Production must stay API-first.

`vercel.json` routes:
- `/` and `/index.html` -> `/api/dashboard_team_view`
- `/dashboard_team_view(.html)` -> `/api/dashboard_team_view`
- `/dashboard_preview(.html)` -> `/api/dashboard_preview`
- `/goals_admin(.html)` -> `/api/goals_admin`
- `/dashboard_data` -> `/api/dashboard_data`
- `/dashboard_state` -> `/api/dashboard_state`
- `/lock_weekly_snapshot` -> `/api/lock_weekly_snapshot`

If this breaks, stale static files can shadow live runtime behavior.

## 5) Snapshot behavior (current contract)
- Current views (team/admin): live refresh from Salesforce + ClickUp on each load.
- Historical snapshots: locked weekly and immutable in intent.
- Cron lock is configured in `vercel.json`:
  - `59 6 * * 2` UTC, equivalent to Monday 11:59 PM PT.

Historical mode uses query params:
- `snapshot_quarter=Qx-YYYY`
- `snapshot_week_start=YYYY-MM-DD`

## 6) Supabase setup required for full functionality
Environment variables (local + Vercel):
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DASHBOARD_HISTORY_TABLE` (default: `dashboard_history`)
- `SUPABASE_DASHBOARD_STATE_TABLE` (default: `dashboard_quarter_state`)
- `SUPABASE_DASHBOARD_SERVICE_OVERRIDES_TABLE` (default: `dashboard_service_overrides`)
- Optional auth guards:
  - `CRON_SECRET`
  - `SNAPSHOT_LOCK_TOKEN`

Apply SQL schema from:
- `supabase_dashboard_backend.sql`

## 7) Known business logic decisions already in place
- Pipeline uses month-end snapshot behavior from Salesforce report config.
- Q1-2026 hardcoded correction exists in `dashboard_config.json` under `hardcoded_overrides` for:
  - `arr = 1001208`
  - `total_active_pipeline = 859436`
- Services overrides file currently seeds avg close-day behavior:
  - `dashboard_services_overrides.json`

## 8) Known pain points / likely improvement opportunities
1. Replace hardcoded corrections with auditable override records in Supabase.
2. Add stronger input validation and sanitization for admin text fields (quote/billing/milestones).
3. Add automated regression checks for:
- live vs snapshot isolation
- quarter goal loading
- route shadowing
- history upsert integrity
4. Add structured logging + alerting for failed Salesforce/ClickUp pulls.
5. Reduce duplication between local static generation and API runtime code paths.

## 9) Improvement roadmap (recommended)
P0 (stability)
- Add automated smoke test script against `/dashboard_data`, `/goals_admin`, `/dashboard_team_view`.
- Add schema validation for runtime payload before render.
- Add explicit fallback markers in UI when backend/source data is partial.

P1 (maintainability)
- Move report IDs and metric transforms into a typed config layer.
- Centralize quarter logic into one module shared by metrics + runtime.
- Remove legacy static shadow risks and unused files where possible.

P2 (new CRM migration)
- Introduce provider adapter interface:
  - `get_arr()`
  - `get_pipeline_series()`
  - `get_new_customers_series()`
  - `get_marketing_series()`
  - `get_nrr()`
  - `get_renewals()`
- Keep downstream payload shape unchanged so UI/pages do not need a redesign.

## 10) Quick commands
Local refresh and build:
```bash
python3 dashboard_metrics.py
python3 generate_dashboard_preview.py
python3 sync_dashboard_backend.py
```

Local all-in-one script:
```bash
./run_dashboard_refresh.sh
```

Deploy:
```bash
npx --yes vercel deploy --prod --yes
```

## 11) External related code the team has referenced
Not in this repo, but used in recent work:
- `/Users/braydenfrost/Library/CloudStorage/OneDrive-Medcurity,Inc/Code/medcurity-crm/.claude/worktrees/quirky-lehmann/scripts/migration/recover_prod_opportunity_amounts.sql`
- `/Users/braydenfrost/Library/CloudStorage/OneDrive-Medcurity,Inc/Code/medcurity-crm/.claude/worktrees/quirky-lehmann/scripts/migration/recover_staging_opportunity_amounts.sql`

These are recovery/ops scripts for CRM/Supabase data correction, not dashboard runtime files.

## 12) Handoff rule for future agents
When changing dashboard behavior, always update both:
- `DASHBOARD_CHANGELOG.md`
- `DASHBOARD_HANDOFF.md`

This file (`CLAUDE_CODE_HANDOFF.md`) should also be updated when architecture or source map changes.
