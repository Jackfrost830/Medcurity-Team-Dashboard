-- Dashboard backend storage for goals/manual content/history.
-- Run in Supabase SQL editor for your single production project.

create table if not exists public.dashboard_history (
  quarter text not null,
  week_start date not null,
  quarter_anchor_date date,
  snapshot_date date,
  generated_at_utc text,
  metrics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  primary key (quarter, week_start)
);

create table if not exists public.dashboard_quarter_state (
  quarter text primary key,
  goals jsonb not null default '{}'::jsonb,
  cs_content jsonb not null default '{}'::jsonb,
  milestones jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.dashboard_refresh_runs (
  id bigint generated always as identity primary key,
  started_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz,
  status text not null default 'running',
  message text,
  context jsonb not null default '{}'::jsonb
);

create table if not exists public.dashboard_service_overrides (
  quarter text primary key,
  closed_projects_this_quarter integer,
  avg_project_close_days_this_quarter numeric,
  notes text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create or replace function public.dashboard_set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists trg_dashboard_history_updated_at on public.dashboard_history;
create trigger trg_dashboard_history_updated_at
before update on public.dashboard_history
for each row execute function public.dashboard_set_updated_at();

drop trigger if exists trg_dashboard_quarter_state_updated_at on public.dashboard_quarter_state;
create trigger trg_dashboard_quarter_state_updated_at
before update on public.dashboard_quarter_state
for each row execute function public.dashboard_set_updated_at();

drop trigger if exists trg_dashboard_service_overrides_updated_at on public.dashboard_service_overrides;
create trigger trg_dashboard_service_overrides_updated_at
before update on public.dashboard_service_overrides
for each row execute function public.dashboard_set_updated_at();

alter table public.dashboard_history enable row level security;
alter table public.dashboard_quarter_state enable row level security;
alter table public.dashboard_refresh_runs enable row level security;
alter table public.dashboard_service_overrides enable row level security;

drop policy if exists "dashboard_history_service_role_only" on public.dashboard_history;
create policy "dashboard_history_service_role_only"
on public.dashboard_history
for all
to authenticated
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "dashboard_state_service_role_only" on public.dashboard_quarter_state;
create policy "dashboard_state_service_role_only"
on public.dashboard_quarter_state
for all
to authenticated
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "dashboard_refresh_runs_service_role_only" on public.dashboard_refresh_runs;
create policy "dashboard_refresh_runs_service_role_only"
on public.dashboard_refresh_runs
for all
to authenticated
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "dashboard_service_overrides_service_role_only" on public.dashboard_service_overrides;
create policy "dashboard_service_overrides_service_role_only"
on public.dashboard_service_overrides
for all
to authenticated
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');
