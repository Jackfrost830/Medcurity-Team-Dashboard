# Dashboard Change Log

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
