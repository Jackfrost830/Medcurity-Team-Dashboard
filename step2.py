from simple_salesforce import Salesforce
import os
import requests
import signal
import socket
import time
from pathlib import Path

# ======================
# CONFIG (from .env)
# ======================

def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(Path(__file__).with_name(".env"))

SF_USERNAME = os.getenv("SF_USERNAME", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN", "")
SF_DOMAIN = os.getenv("SF_DOMAIN", "login")  # or "test"

CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN", "")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID", "")
CLICKUP_SF_ID_FIELD_ID = os.getenv("CLICKUP_SF_ID_FIELD_ID", "")  # ClickUp field labeled "SF ID"

def _require_env(name: str, value: str) -> None:
    if not value:
        raise SystemExit(f"Missing required env var: {name}. Set it in .env.")


_require_env("SF_USERNAME", SF_USERNAME)
_require_env("SF_PASSWORD", SF_PASSWORD)
_require_env("SF_SECURITY_TOKEN", SF_SECURITY_TOKEN)
_require_env("CLICKUP_API_TOKEN", CLICKUP_API_TOKEN)
_require_env("CLICKUP_LIST_ID", CLICKUP_LIST_ID)
_require_env("CLICKUP_SF_ID_FIELD_ID", CLICKUP_SF_ID_FIELD_ID)

# ======================
# Salesforce query
# ======================

SF_QUERY = """
SELECT Id, Name, AccountId, Account.Name, Account.Account_Number__c
FROM Opportunity
WHERE StageName = 'Closed Won'
"""

# ======================
# Setup APIs
# ======================

def _timeout_handler(signum, frame):
    raise TimeoutError("Operation timed out")


def _run_with_timeout(seconds, fn, *args, **kwargs):
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(seconds)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)

CLICKUP_HEADERS = {
    "Authorization": CLICKUP_API_TOKEN,
    "Content-Type": "application/json",
}

REQUEST_TIMEOUT = 15
socket.setdefaulttimeout(REQUEST_TIMEOUT)

# ======================
# Helper functions
# ======================

def fetch_clickup_tasks_by_name():
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    page = 0
    tasks_by_name = {}
    while True:
        params = {"page": page}
        print(f"Fetching ClickUp tasks page {page}...", flush=True)
        r = requests.get(url, headers=CLICKUP_HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        tasks = data.get("tasks", [])
        for task in tasks:
            name = task.get("name")
            if name:
                tasks_by_name.setdefault(name, []).append(task)
        if data.get("last_page") is True or not tasks:
            break
        page += 1
    return tasks_by_name


def update_clickup_salesforce_id(task_id, account_number):
    url = f"https://api.clickup.com/api/v2/task/{task_id}/field/{CLICKUP_SF_ID_FIELD_ID}"
    payload = {"value": str(account_number)}
    r = requests.post(url, headers=CLICKUP_HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

def fetch_task(task_id):
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    r = requests.get(url, headers=CLICKUP_HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_custom_field_value(task, field_id):
    for field in task.get("custom_fields", []):
        if field.get("id") == field_id:
            return field.get("value")
    return None

# ======================
# Main logic
# ======================

def main():
    start_ts = time.time()
    print("Starting sync...", flush=True)
    print("Logging in to Salesforce...", flush=True)
    sf = _run_with_timeout(
        55,
        Salesforce,
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN,
        domain=SF_DOMAIN,
    )
    print("Querying Salesforce...", flush=True)
    records = _run_with_timeout(55, sf.query_all, SF_QUERY)["records"]
    print(f"Salesforce returned {len(records)} opportunity records.", flush=True)
    print("Fetching ClickUp tasks...", flush=True)
    tasks_by_name = _run_with_timeout(55, fetch_clickup_tasks_by_name)
    clickup_task_count = sum(len(v) for v in tasks_by_name.values())
    clickup_names = set(tasks_by_name.keys())
    print(f"ClickUp returned {clickup_task_count} tasks.", flush=True)

    for record in records:
        account = record.get("Account") or {}
        account_name = account.get("Name")
        account_number = account.get("Account_Number__c")

        if not account_name:
            print("Skipping: missing Account Name for Opportunity", record.get("Id"))
            continue

        if not account_number:
            print(f"Skipping: no Account Number for {account_name}")
            continue
        if account_name not in clickup_names:
            continue

        matching_tasks = tasks_by_name.get(account_name, [])
        if not matching_tasks:
            print(f"No ClickUp task found for: {account_name}", flush=True)
            continue
        if len(matching_tasks) > 1:
            print(f"Multiple ClickUp tasks found for: {account_name}; skipping", flush=True)
            continue
        task = matching_tasks[0]

        existing_value = get_custom_field_value(task, CLICKUP_SF_ID_FIELD_ID)
        if existing_value not in (None, ""):
            print(f"Skipping {account_name}: SF ID already set", flush=True)
            continue

        update_clickup_salesforce_id(task["id"], account_number)
        verify = fetch_task(task["id"])
        verified_value = get_custom_field_value(verify, CLICKUP_SF_ID_FIELD_ID)
        print(
            f"Updated {account_name} -> {account_number} (verified: {verified_value})",
            flush=True,
        )

    elapsed = time.time() - start_ts
    print(f"Done in {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
