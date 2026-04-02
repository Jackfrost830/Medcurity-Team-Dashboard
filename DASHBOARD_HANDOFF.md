# Medcurity Dashboard Handoff

## Scope
This repo powers the Medcurity dashboard stack:
- `dashboard_preview.html` (owner/admin view)
- `dashboard_team_view.html` (team-safe view)
- `goals_admin.html` (goals + historical navigation)
- Supabase-backed historical/state persistence
- Salesforce + ClickUp metric refresh pipeline

## Current Production URL
- Team base: `https://medcurity-team-dashboard-site.vercel.app`

## Key Data Flows
1. Metrics refresh
- `dashboard_metrics.py` pulls Salesforce + ClickUp data.
- `dashboard_metrics_output.json` is generated.

2. HTML generation
- `generate_dashboard_preview.py` consumes metrics + history + state and emits:
  - `dashboard_preview.html`
  - `dashboard_team_view.html`
  - `goals_admin.html`
  - `index.html`

3. Historical snapshots
- Weekly snapshots are stored in `dashboard_history.json` and Supabase table `dashboard_history`.
- Snapshot rows are keyed by `(quarter, week_start)`.
- Snapshot payload includes frozen chart/data state and manual content.

4. Backend state
- Supabase `dashboard_quarter_state` stores:
  - goals
  - quote/QTD billing content
  - development milestones

## Supabase Tables
- `dashboard_history`
- `dashboard_quarter_state`
- `dashboard_refresh_runs`
- `dashboard_service_overrides`

## Critical Env Vars
Set in Vercel and local `.env.supabase`:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DASHBOARD_HISTORY_TABLE` (default `dashboard_history`)
- `SUPABASE_DASHBOARD_STATE_TABLE` (default `dashboard_quarter_state`)
- `SUPABASE_DASHBOARD_SERVICE_OVERRIDES_TABLE` (default `dashboard_service_overrides`)

## Operational Runbook
1. Refresh metrics locally:
- `python3 dashboard_metrics.py`

2. Regenerate dashboards:
- `python3 generate_dashboard_preview.py`

3. Sync static files into Vercel-served folder:
- `cp dashboard_preview.html dashboard_team_view.html goals_admin.html index.html dashboard_goals.json public/`

4. Deploy:
- `npx --yes vercel deploy --prod --yes`

## Historical Behavior (Important)
- Historical views use URL params:
  - `snapshot_quarter`
  - `snapshot_week_start`
- For frozen snapshots, render uses stored frozen payload.
- For legacy non-frozen snapshots, charts are rebuilt from historical rows up to selected week.

## Development Data Retention
- Weekly snapshots now capture `metrics.milestones`.
- Snapshot merge logic preserves existing milestones if incoming run omits them.
- Historical snapshot rendering now reads milestone payload from snapshot metrics.

## Notes For Any New Agent (Claude/Codex)
- Do not rewrite quarter timelines from current date while in snapshot mode.
- Preserve manual weekly fields on upsert:
  - `quote_text`, `quote_name`, `quote_org`, `billing_progress`, `milestones`
- Treat `dashboard_history` as source of truth for historical rendering.

## Documentation Discipline (Required)
For every dashboard logic/data/deploy change:
1. Update `DASHBOARD_CHANGELOG.md` with what changed and why.
2. Update this handoff file if architecture, runbook, env vars, or behavior contracts changed.
3. Include deployment result/target URL in the assistant summary.

This is mandatory so a different agent (including Claude Code) can resume work without missing context.

## Live vs Historical Contract (Verified 2026-04-02)
- Live pages are request-time rendered via API (`api/_dashboard_response.py` + `dashboard_runtime.py`).
- They pull current Salesforce/ClickUp data each load (subject only to upstream source freshness).
- Weekly historical is lock-based only:
  - Cron: `59 6 * * 2` UTC (`vercel.json`) == Monday 11:59 PM Pacific.
  - Manual: `/lock_weekly_snapshot` endpoint.
- Request-time auto-snapshot is disabled (`DASHBOARD_ENABLE_REQUEST_SNAPSHOT=false`).
- Q2 goals are sourced from Supabase `dashboard_quarter_state` and injected into runtime payload.

## Routing Contract (Critical)
- Vercel must route dashboard pages to API handlers first:
  - `/` -> `/api/dashboard_team_view`
  - `/dashboard_team_view` -> `/api/dashboard_team_view`
  - `/dashboard_preview` -> `/api/dashboard_preview`
  - `/goals_admin` -> `/api/goals_admin`
- Static HTML files can exist in repo for local generation, but production must not allow them to shadow live API pages.
- If quarter/goals look stale in production, inspect `vercel.json` routing first.

## Snapshot Isolation Rule (Critical)
- Historical pages must never read current/local admin state for quote/QTD/milestones.
- In `applySnapshotFromQuery`, do not use localStorage/current BACKEND_STATE to fill missing historical fields.
- Historical data source of truth is `dashboard_history.metrics` (and its frozen payload when present).
- Any fallback in historical mode must come from historical rows only, never from current quarter state.

## URL Consistency Rule
- Always use extensionless URLs in-app:
  - `/dashboard_team_view`
  - `/dashboard_preview`
  - `/goals_admin`
- Keep `.html` route aliases mapped to the same API handlers for backward compatibility and old bookmarks.
- If goals appear default in incognito while dashboard looks correct, verify requested path isn't bypassing API runtime.
## Latest update (2026-04-02)

- Dashboard action bar now has:
  - `Toggle Theme`
  - `Print HQ PDF`
  - `Goals Admin` (owner/admin view)
- `Download HD PNG` was intentionally removed from generated output.
