"""Module to track expired system-retained snapshots (ENG-924183).

This script connects to Nutanix Prism Central via REST API to retrieve
all VM recovery points, compares their expiration times against the current
time, and prints pure JSON
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Default Credentials ---
DEF_USER = "admin"
DEF_PWD = "Nutanix.123"

logger = logging.getLogger(__name__)

def make_api_call(
  ip: str,
  method: str,
  endpoint: str,
  payload: dict = None,
  user: str = DEF_USER,
  pwd: str = DEF_PWD
) -> dict:
  """Executes a direct REST API call to Prism Central.

  Args:
    ip (str): The target Prism Central IP address.
    method (str): The HTTP method (GET, POST, PUT, DELETE).
    endpoint (str): The API path to hit.
    payload (dict, optional): Request payload dictionary. Defaults to None.
    user (str, optional): Username for authentication. Defaults to DEF_USER.
    pwd (str, optional): Password for authentication. Defaults to DEF_PWD.

  Returns:
    dict: The parsed JSON response.
  """
  url = f"https://{ip}:9440{endpoint}"
  auth = (user, pwd)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  resp = requests.request(
    method=method,
    url=url,
    auth=auth,
    json=payload,
    headers=headers,
    verify=False,
    timeout=60
  )
  resp.raise_for_status()
  return resp.json()

def verify_expired_snapshots(ip: str, user: str, password: str) -> tuple:
  """Executes API calls to retrieve and filter expired recovery points.

  Args:
    ip (str): The target IP address to connect to.
    user (str): The username for authentication.
    password (str): The password for authentication.

  Returns:
    tuple: A tuple containing a summary dictionary, a list of expired snapshots,
           and a boolean indicating success.
  """
  try:
    offset = 0
    length = 100
    expired_list = []

    # Get the current time in UTC formatted identically to Nutanix timestamps
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
      payload = {
        "kind": "vm_recovery_point",
        "offset": offset,
        "length": length
      }
      data = make_api_call(
        ip, "POST", "/api/nutanix/v3/vm_recovery_points/list",
        payload, user, password
      )

      entities = data.get("entities", [])
      if not entities:
        break

      for entity in entities:
        status = entity.get("status", {})
        metadata = entity.get("metadata", {})
        resources = status.get("resources", {})

        expired_at = resources.get("expiration_time")

        # Check if the snapshot has an expiration time and if it has passed
        if expired_at and expired_at < current_time:
          parent_vm = resources.get("parent_vm_reference", {})

          expired_list.append({
            "vm_name": parent_vm.get("name", "Unknown"),
            "entity_type": "VM",
            "entity_uuid": parent_vm.get("uuid", "Unknown"),
            "snapshot_uuid": metadata.get("uuid", "Unknown"),
            "rp_name": status.get("name", ""),
            "created_at": metadata.get("creation_time", "Unknown"),
            "expired_at": expired_at
          })

      offset += length
      # Break the pagination loop if we received fewer entities than requested
      if len(entities) < length:
        break

    summary = {
      "total_expired_snapshots": len(expired_list),
      "oldest_snapshot_date": expired_list[0]["created_at"] if expired_list else "N/A",
      "status": "CRITICAL" if len(expired_list) > 30 else "OK"
    }
    return summary, expired_list, True

  except Exception as e:
    logger.error(f"API execution failed for {ip}: {e}")
    summary = {"error": f"API Request failed: {e}"}
    return summary, [], False

def run_snapshot_collection(config_path: str = None) -> dict:
  """Reads endpoints config, gathers expired snapshots, and returns results.

  Args:
    config_path (str, optional): Path to the configuration JSON file.
                                 Defaults to None (auto-resolves path).

  Returns:
    dict: A dictionary containing the collection results mapped by IP address.
  """
  if not config_path:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, "r") as f:
      config_data = json.load(f)
  except Exception as e:
    return {"error": f"Failed to load config: {str(e)}"}

  pcs = config_data.get("pcs", [])
  results = {}

  for pc in pcs:
    ip = pc.get("ip") or pc.get("virtual_ip")
    if not ip:
      continue

    creds = pc.get("credentials", {})
    user = pc.get("user", creds.get("username", creds.get("user", DEF_USER)))
    pwd = pc.get("password", creds.get("password", DEF_PWD))

    summary_data, expired_list, is_successful = verify_expired_snapshots(
      ip, user, pwd
    )

    results[ip] = {
      "is_successful": is_successful,
      "summary_data": summary_data,
      "snapshots_data": expired_list
    }

  return results

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  final_results = run_snapshot_collection()
  print(json.dumps(final_results, indent=2))

