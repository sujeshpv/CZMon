"""Monitors Nutanix PC for failed and stuck tasks.

This script reads PC endpoints from a config file, connects to the
Nutanix Prism API (v4) to gather task status, and prints pure JSON.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_UNAME = "admin"
DEF_PWD = "Nutanix.123"
PENDING_TASK_THRESHOLD_HOURS = 2.0
TASK_LOOKBACK_HOURS = 24.0  # Collect tasks created in the last X hours

logger = logging.getLogger(__name__)

def get_tasks_from_pc(
  pc_ip: str,
  username: str,
  password: str,
  pending_threshold_hours: float,
  lookback_hours: float,
) -> dict:
  """Fetches paginated tasks from a Prism Central instance.

  Filters for failed tasks and pending tasks created within the lookback window.

  Args:
    pc_ip (str): The Prism Central IP address.
    username (str): The Prism Central username.
    password (str): The Prism Central password.
    pending_threshold_hours (float): Hours threshold for pending tasks.
    lookback_hours (float): Hours lookback window for task collection.

  Returns:
    dict: A dictionary with "Failed" and "Pending" task lists, or "error".
  """
  url = f"https://{pc_ip}:9440/api/prism/v4.0/config/tasks"
  headers = {"Accept": "application/json"}

  now_utc = datetime.now(timezone.utc)
  pending_cutoff = now_utc - timedelta(hours=pending_threshold_hours)
  lookback_cutoff = now_utc - timedelta(hours=lookback_hours)

  failed_tasks = []
  pending_tasks = []

  page = 0
  limit = 100
  has_more = True

  while has_more:
    params = {"$page": page, "$limit": limit}
    try:
      response = requests.get(
        url,
        auth=(username, password),
        headers=headers,
        params=params,
        verify=False,
        timeout=15,
      )
      response.raise_for_status()
      response_json = response.json()

      tasks_list = response_json.get("data", [])
      if not tasks_list:
        break

      stop_pagination = False
      for task in tasks_list:
        task_uuid = task.get("extId")
        status = task.get("status")
        title = task.get("operationDescription") or task.get(
          "operation", "Unknown Task"
        )
        created_time_str = task.get("createdTime")

        if created_time_str:
          clean_time_str = created_time_str.replace("Z", "+00:00")
          created_time = datetime.fromisoformat(clean_time_str)

          # Stop checking tasks older than the lookback window
          if created_time < lookback_cutoff:
            stop_pagination = True
            break

          if status == "FAILED":
            error_detail = "Unknown error"
            error_msgs = task.get("errorMessages", [])
            if error_msgs:
              error_detail = error_msgs[0].get("message", "Unknown error")
            else:
              completion_details = task.get("completionDetails", [])
              if completion_details:
                error_detail = completion_details[0].get(
                  "value", "Unknown error"
                )
            failed_tasks.append({task_uuid: error_detail})

          elif status in ["RUNNING", "QUEUED", "PENDING"]:
            if created_time < pending_cutoff:
              pending_tasks.append({task_uuid: title})

      if stop_pagination:
        break

      page += 1
      if len(tasks_list) < limit:
        has_more = False

    except requests.exceptions.RequestException as e:
      logger.error(f"API Request Failed for {pc_ip}: {e}")
      return {"error": f"API Request Failed: {str(e)}"}

  return {"Failed": failed_tasks, "Pending": pending_tasks}

def collect_all_pc_tasks(config_path: str = None) -> None:
  """Reads endpoints, collects tasks for PC clusters, and prints JSON.

  Args:
    config_path (str, optional): Path to the endpoints JSON config file.
  """
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, "r") as f:
      config_data = json.load(f)
  except Exception as e:
    logger.error(f"Failed to load config at {config_path}: {e}")
    sys.exit(1)

  pc_endpoints = []
  if "pcs" in config_data:
    pc_endpoints = config_data.get("pcs", [])
  else:
    for zone, entries in config_data.items():
      if isinstance(entries, list):
        for entry in entries:
          if entry.get("type", "").upper() == "PC":
            pc_endpoints.append(entry)

  final_results = {}

  for endpoint in pc_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})
    user = creds.get("username", creds.get("user", DEF_UNAME))
    pwd = creds.get("password", DEF_PWD)

    if not ip:
      continue

    logger.info(f"Checking tasks for PC: {ip}...")
    final_results[ip] = get_tasks_from_pc(
      ip,
      user,
      pwd,
      PENDING_TASK_THRESHOLD_HOURS,
      TASK_LOOKBACK_HOURS,
    )

  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  collect_all_pc_tasks()

