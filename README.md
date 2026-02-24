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

## Current behavior
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
