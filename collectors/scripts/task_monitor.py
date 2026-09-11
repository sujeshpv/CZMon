"""Monitors Nutanix PC for failed and stuck tasks.

This script reads PC endpoints from a config file, connects to the
Nutanix Prism API (v4) to gather task status using time-windowed
server-side filters, and prints pure JSON.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_UNAME = os.getenv("CZMON_PC_USERNAME", "admin")
DEF_PWD = os.getenv("CZMON_PC_PASSWORD")
PENDING_TASK_THRESHOLD_HOURS = float(
  os.getenv("CZMON_PENDING_TASK_THRESHOLD_HOURS", "2.0")
)
TASK_LOOKBACK_HOURS = float(os.getenv("CZMON_TASK_LOOKBACK_HOURS", "12.0"))
MAX_PAGE_RETRIES = 2
TASK_PAGE_LIMIT = 50
TASK_WINDOW_MINUTES = 15
MAX_CONSECUTIVE_WINDOW_FAILURES = 3
ACTIVE_TASK_STATUSES = ("RUNNING", "QUEUED", "CANCELING", "SUSPENDED")

logger = logging.getLogger(__name__)

def get_tasks_from_pc(
  pc_ip: str,
  username: str,
  password: str,
  pending_threshold_hours: float,
  lookback_hours: float,
) -> dict:
  """Fetches paginated tasks from a Prism Central instance.

  Returns failed tasks from the lookback window and active tasks older than the
  configured pending threshold.

  Args:
    pc_ip (str): The Prism Central IP address.
    username (str): The Prism Central username.
    password (str): The Prism Central password.
    pending_threshold_hours (float): Age after which an active task is overdue.
    lookback_hours (float): Hours lookback window for task collection.

  Returns:
    dict: ``Failed`` and overdue ``Pending`` task lists, or an ``error`` value.
  """
  url = f"https://{pc_ip}:9440/api/prism/v4.0/config/tasks"
  headers = {"Accept": "application/json"}

  now_utc = datetime.now(timezone.utc).replace(microsecond=0)
  lookback_cutoff = (now_utc - timedelta(hours=lookback_hours)).replace(
    microsecond=0
  )
  pending_cutoff = (
    now_utc - timedelta(hours=pending_threshold_hours)
  ).replace(microsecond=0)

  status_groups = {
    "Failed": [],
    "Pending": [],
  }
  active_statuses = set(ACTIVE_TASK_STATUSES)
  limit = TASK_PAGE_LIMIT
  seen_task_ids = set()
  warnings = []
  successful_windows = 0
  consecutive_window_failures = 0

  def record_task(task):
    task_uuid = task.get("extId")
    status = str(task.get("status") or "").upper()
    title = task.get("operationDescription") or task.get(
      "operation", "Unknown Task"
    )
    created_time_str = str(task.get("createdTime") or "")
    dedupe_key = task_uuid or f"{created_time_str}|{status}|{title}"
    if dedupe_key in seen_task_ids:
      return

    try:
      created_time = datetime.fromisoformat(
        created_time_str.replace("Z", "+00:00")
      )
      if created_time.tzinfo is None:
        created_time = created_time.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
      created_time = None

    task_identifier = task_uuid or "Unknown UUID"
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
      status_groups["Failed"].append({task_identifier: error_detail})
      seen_task_ids.add(dedupe_key)
    elif (
      status in active_statuses
      and created_time
      and created_time <= pending_cutoff
    ):
      status_groups["Pending"].append({task_identifier: title})
      seen_task_ids.add(dedupe_key)
  pending_windows = []
  cursor = lookback_cutoff
  while cursor < now_utc:
    window_end = min(
      cursor + timedelta(minutes=TASK_WINDOW_MINUTES), now_utc
    )
    pending_windows.append((cursor, window_end))
    cursor = window_end

  while pending_windows:
    window_start, window_end = pending_windows.pop(0)
    start_text = window_start.isoformat().replace("+00:00", "Z")
    end_text = window_end.isoformat().replace("+00:00", "Z")
    page = 0
    window_succeeded = False

    while True:
      params = {
        "$page": page,
        "$limit": limit,
        "$filter": f"createdTime ge {start_text} and createdTime lt {end_text}",
        "$orderby": "createdTime desc",
        "$select": (
          "extId,status,operationDescription,operation,createdTime,"
          "errorMessages,completionDetails"
        ),
      }
      try:
        response = None
        for attempt in range(MAX_PAGE_RETRIES):
          try:
            response = requests.get(
              url,
              auth=(username, password),
              headers=headers,
              params=params,
              verify=False,
              timeout=(5, 15),
            )
            break
          except requests.exceptions.Timeout:
            if attempt == MAX_PAGE_RETRIES - 1:
              raise
            time.sleep(2 ** attempt)

        response.raise_for_status()
        tasks_list = response.json().get("data", [])
      except requests.exceptions.RequestException as e:
        logger.error(
          "Task API failed for %s window %s to %s page %s: %s",
          pc_ip,
          start_text,
          end_text,
          page,
          e,
        )
        warnings.append(
          f"{start_text} to {end_text} stopped at page {page}: {e}"
        )
        break

      if not tasks_list:
        window_succeeded = True
        break

      for task in tasks_list:
        record_task(task)

      page += 1
      if len(tasks_list) < limit:
        window_succeeded = True
        break

    if window_succeeded:
      successful_windows += 1
      consecutive_window_failures = 0
    else:
      consecutive_window_failures += 1
      if (
        consecutive_window_failures
        >= MAX_CONSECUTIVE_WINDOW_FAILURES
      ):
        warnings.append(
          "Stopped recent-task queries after "
          f"{consecutive_window_failures} consecutive failed windows."
        )
        break

  if successful_windows == 0:
    return {
      "error": (
        f"Task API failed for every {lookback_hours:g}-hour collection window."
      )
    }

  if warnings:
    logger.warning(
      "Task collection for %s was partial: %s", pc_ip, " | ".join(warnings)
    )
  return status_groups

def collect_all_endpoints(config_path: str = None) -> None:
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
    user = (
      endpoint.get("username")
      or endpoint.get("user")
      or creds.get("username")
      or creds.get("user")
      or DEF_UNAME
    )
    pwd = endpoint.get("password") or creds.get("password") or DEF_PWD

    if not ip:
      continue
    if not user or not pwd:
      final_results[ip] = {
        "error": (
          "Missing PC credentials. Configure endpoint credentials or set "
          "CZMON_PC_USERNAME and CZMON_PC_PASSWORD."
        )
      }
      logger.error("Skipping %s due to missing credentials.", ip)
      continue

    logger.info("Checking tasks for PC: %s...", ip)
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
  collect_all_endpoints()
