"""Module to track Recovery Point storage usage (Ticket ENG-924185).

This script connects to Nutanix Prism Element via REST API to retrieve
storage usage statistics and calculate the recovery point usage percentage.
It outputs pure JSON for the CZMon framework.
"""

import json
import logging
import os
import sys
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Default Credentials ---
DEF_USER = "admin"
DEF_PWD = "Nutanix.123"

logger = logging.getLogger(__name__)

def get_recovery_point_usage(ip: str, user: str, password: str) -> dict:
  """Connects to Nutanix Prism Element API to get snapshot usage stats.

  Args:
    ip (str): The Prism Element Cluster IP address.
    user (str): Username for authentication.
    password (str): Password for authentication.

  Returns:
    dict: A dictionary containing the recovery point usage percentage.
  """
  url = f"https://{ip}:9440/PrismGateway/services/rest/v2.0/cluster/"
  auth = (user, password)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  try:
    resp = requests.get(
      url, auth=auth, headers=headers, verify=False, timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    stats = data.get("usage_stats", {})
    snap_bytes = int(stats.get("storage.snapshot_reclaimable_bytes", 0))

    # Extract total capacity correctly from the usage_stats dictionary.
    # Fallback to root capacity_bytes or 1 byte to prevent division by zero.
    total_capacity = int(stats.get("storage.capacity_bytes", data.get("capacity_bytes", 1)))

    if total_capacity > 0:
      usage_pct = round((snap_bytes / total_capacity) * 100, 2)
    else:
      usage_pct = 0.0

    return {"recovery_point_usage": usage_pct}

  except Exception as e:
    logger.error(f"API Request Failed for cluster {ip}: {e}")
    # Return None on failure so the UI knows the cluster is unreachable
    return {"recovery_point_usage": None}

def run_snapshot_tracker(config_path: str = None) -> None:
  """Reads endpoints config, gathers snapshot usage stats, and prints JSON.

  Args:
    config_path (str, optional): Path to endpoints.json. Defaults to None
                                 (auto-resolves path).
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

  # Recovery points are tracked on the PE cluster dashboards
  pes = config_data.get("pes", [])

  # Fallback: if 'pes' is empty, search for PEs inside Zone layout
  if not pes:
    for key, nodes in config_data.items():
      if isinstance(nodes, list):
        for node in nodes:
          if node.get("type") == "PE" or "PE" in node.get("name", ""):
            pes.append(node)

  final_results = {}

  for pe in pes:
    ip = pe.get("ip") or pe.get("virtual_ip")
    cluster_name = pe.get("name", ip)

    if not ip:
      continue

    # Credential fallback to DEF_USER and DEF_PWD
    creds = pe.get("credentials", pe)
    user = pe.get("user", creds.get("username", creds.get("user", DEF_USER)))
    pwd = pe.get("password", creds.get("password", DEF_PWD))

    logger.info(f"Tracking snapshot usage for cluster: {cluster_name} ({ip})...")
    # MUST USE IP AS KEY FOR UI DROPDOWN MATCHING
    final_results[ip] = get_recovery_point_usage(ip, user, pwd)

  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_snapshot_tracker()

