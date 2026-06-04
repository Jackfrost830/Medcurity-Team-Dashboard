# Dashboard Change Log

## 2026-04-27 - Claude migration handoff package

- Added `CLAUDE_CODE_HANDOFF.md` with:
  - full source map
  - architecture/data flow
  - Supabase + routing contracts
  - snapshot behavior contract
  - prioritized improvement roadmap for next iterations
- Updated `DASHBOARD_HANDOFF.md` to explicitly point new agents to `CLAUDE_CODE_HANDOFF.md` first.

## 2026-04-01

### Historical snapshot stability
- Fixed historical snapshot rendering to avoid wiping manual fields when frozen snapshots had partial state.
- Added fallback handling for legacy historical rows.

### Quote + QTD retention
- Patched weekly merge behavior to preserve existing non-empty values when incoming snapshot fields are blank.
- Locked `Q1-2026 / week_start 2026-03-30` with explicit quote + QTD billing values.

### Historical quarter rendering
- Fixed snapshot mode goal lookup to use the selected snapshot quarter (not current quarter).
- Rebuilt chart rows for non-frozen historical views from in-quarter weekly rows, preventing Apr/May/Jun leakage into Q1 snapshots.

### Development historical tracking
- Added `metrics.milestones` into weekly snapshot payload.
- Added merge-preserve behavior for `milestones` across weekly snapshot upserts.
- Historical snapshot apply now reads and applies milestones from snapshot metrics for development section consistency.

### Documentation
- Added `DASHBOARD_HANDOFF.md` with architecture, runbook, env vars, and handoff guidance.
- Added this `DASHBOARD_CHANGELOG.md` to track major behavior fixes.

## 2026-04-01 (continued)

### Historical development fallback
- Historical snapshot milestone loader now falls back to default milestones when a legacy snapshot has an empty milestone payload.

### Legacy month consistency fix
- For non-frozen historical weeks, month series now use the quarter's best frozen baseline and only override the selected snapshot month.
- Prevents January values from dropping to zero in February/March historical views when January snapshots were never captured.

### Ops commitment
- Documentation files are now treated as required deliverables for every meaningful dashboard change and deploy.

## 2026-04-02

### Production verification (live vs historical)
- Verified `/dashboard_data` on production is now Q2/April live:
  - `quarter_anchor_date=2026-04-02`
  - `current_quarter=Q2-2026`
  - `history_backend=supabase`
  - `state_backend=supabase`
  - `history_lock_mode=cron_or_manual`
  - `request_snapshot_enabled=false`
- Confirmed Q2 goals are loading from Supabase state (`goals_by_quarter['Q2-2026']` present with expected values).

### Behavior contract confirmed
- Current views (`/dashboard_team_view`, `/dashboard_preview`) refresh live on each load from Salesforce/ClickUp.
- Historical snapshots remain fixed to Monday 11:59 PM PT lock (cron/manual) and are not mutated by daily live refreshes.

## 2026-04-02 (routing fix)

### API-first routing fix for live current/admin/team views
- Replaced `rewrites` with explicit `routes` in `vercel.json` so `/`, `/dashboard_team_view`, `/dashboard_preview`, and `/goals_admin` resolve to API handlers before static file fallback.
- This prevents stale static HTML (Q1 constants) from shadowing dynamic runtime output.

### Impact
- Current view now reflects live quarter/month context (Q2 Apr/May/Jun) from runtime payload.
- Admin writes now target the runtime quarter correctly, so goals/state persistence aligns with current quarter.

## 2026-04-02 (historical isolation fix)

### Historical immutability fix (quotes/QTD/dev no longer bleed from current edits)
- Removed snapshot-time fallbacks that hydrated historical pages from current local/admin state.
- Historical rendering now uses only locked snapshot data (`dashboard_history`) plus explicit historical-only fallback logic.
- This separates current-quarter admin edits from previously locked historical weeks.

### Intentional behavior
- Current/admin edits update current quarter state only.
- Historical links remain fixed to what was captured in that week’s snapshot.

## 2026-04-02 (goals admin URL consistency)

### Goals Admin mismatch in incognito fixed
- Root issue: mixed static vs API URLs (`*.html` vs extensionless routes) could hit different data paths.
- Updated dashboard links to use API-backed extensionless paths (`/goals_admin`, `/dashboard_preview`).
- Added explicit Vercel route mappings for both extensionless and `.html` variants:
  - `/`, `/index.html`
  - `/dashboard_team_view`, `/dashboard_team_view.html`
  - `/dashboard_preview`, `/dashboard_preview.html`
  - `/goals_admin`, `/goals_admin.html`
- Result: goals/admin/team views now resolve to the same live Supabase-backed runtime regardless of browser mode.
## 2026-04-02 - UI action cleanup

- Removed the `Download HD PNG` action from dashboard UI actions.
- Kept `Print HQ PDF` intact for one-page export workflow.
- Regenerated dashboard outputs (`dashboard_preview.html`, `dashboard_team_view.html`, `index.html`) and synced `public/` copies.

## 2026-04-02 - Admin 500 fallback + live deploy

- Fixed admin/runtime hard-fail path when Supabase history load errors.
  - `dashboard_runtime.py` now falls back to local `dashboard_history.json` on history backend read exceptions instead of returning HTTP 500.
- Deployed directly to production via Vercel CLI:
  - Deployment ID: `dpl_GxWErKhxYLv9GNgcidLePREB9Vp4`
  - Production alias: `https://medcurity-team-dashboard-site.vercel.app`
- Verified live endpoints respond:
  - `/goals_admin` -> `200`
  - `/dashboard_team_view` -> `200`

## 2026-04-02 - Services avg close days seeded baseline

- Updated services metric logic so QTD avg close days can start from a trusted seed value and adjust as new closures occur.
  - `dashboard_metrics.py`
    - Added `close_day_sample_sum` and `close_day_sample_count` from ClickUp quarter-close samples.
    - Propagated these through hybrid services output.
    - Enhanced `apply_services_quarter_override()` to support seeded average blending:
      - `avg_project_close_days_this_quarter` + `avg_seed_closed_projects_count`.
- Added Q2 seed in `dashboard_services_overrides.json`:
  - `Q2-2026.avg_project_close_days_this_quarter = 52`
  - `Q2-2026.avg_seed_closed_projects_count = 8`
- Deployed to production:
  - Deployment ID: `dpl_3f4Kti47unv6QCjcZPMzSt4bW8L6`
  - Alias: `https://medcurity-team-dashboard-site.vercel.app`
- Live verification:
  - `data.services.avg_project_close_days_this_quarter = 52.0`
  - `data.services.override_source = supabase_or_file`

## 2026-04-06 - MQL Q2 date-window alignment

- Updated MQL config to disable Salesforce standard date override for this metric:
  - `dashboard_config.json` -> `salesforce_quarter_metrics.mql.use_standard_date_override = false`
  - `dashboard_config.template.json` updated to match.
- Deployed to production:
  - Deployment ID: `dpl_7a3K2uaQLNDKx3zX9sztHT8FwYZ4`
  - Alias: `https://medcurity-team-dashboard-site.vercel.app`
- Post-deploy verification (2026-04-06, Q2 window Apr 1-Jun 30):
  - MQL now computed as 39 based on current report rows:
    - `00O5w000009F5BNEA0` = 11
    - `00O5w000009E9WWEA0` = 28

## 2026-04-06 - Month 1 goal color rule update

- Updated goal status coloring logic for month 1 in chart rendering:
  - Month 1 is `yellow` while month 1 is current and below month-1 goal.
  - Month 1 turns `green` when month-1 goal is hit/exceeded.
  - Month 1 turns `red` only after month 2 starts if month-1 goal was missed.
- Month 2 and month 3 behavior left as-is:
  - `yellow` when prior month goal is surpassed but current month goal not yet hit.
  - `green` when current month goal is hit/exceeded.
  - `red` when below required threshold.
- Files updated:
  - `generate_dashboard_preview.py` (source logic)
  - regenerated HTML outputs and synced `public/`.
- Deployed to production:
  - Deployment ID: `dpl_5AZAFYRiX3UsBYyeaqsXQWJn6vxD`

## 2026-04-20 - ClickUp closed-project tracking fix

- Fixed services hybrid closed-project logic to track live ClickUp quarter closures directly.
  - `dashboard_metrics.py`
    - `compute_services_from_clickup()` now returns `closed_projects_this_quarter_names`.
    - `compute_services_hybrid()` now uses ClickUp quarter closed count as canonical and passes through name lists.
    - `apply_services_quarter_override()` now supports seeded cumulative close count:
      - `closed_seed_projects_count`
      - `exclude_closed_project_names`
- Updated `dashboard_services_overrides.json` for `Q2-2026`:
  - `closed_seed_projects_count: 8`
  - `exclude_closed_project_names`: includes Flourish Collective variants.
- Deployed to production:
  - Deployment ID: `dpl_Cfy5sw55uJkwzXkBAh4KTsvu3qEX`
  - Alias: `https://medcurity-team-dashboard-site.vercel.app`
- Verified live:
  - `closed_projects_this_quarter = 12`

## 2026-04-20 - NRR visible range trim

- Adjusted current dashboard NRR visible history window to 5 points, dropping oldest visible point (`Q1-2025`) while keeping historical snapshots retained.
- `dashboard_runtime.py`:
  - `nrr_customer_history` and `nrr_dollar_history` now use `max_points=5` in current runtime payload.
- Deployed to production:
  - Deployment ID: `dpl_7n5TnE1GJn6jgf3XgLGYz97UgxGs`
- Verified live arrays now start at `Q2-2025` and include `Q1-2026`, `Q2-2026`.
