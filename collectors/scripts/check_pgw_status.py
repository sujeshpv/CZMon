"""Checks Prism Gateway (PGW) heartbeat status for Nutanix clusters.

This script connects to the Nutanix Prism API to verify PGW status.
It is designed to run independently and print pure JSON for the CZMon framework.
"""

import json
import logging
import os
import sys
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_UNAME = "admin"
DEF_PWD = "Nutanix.123"

logger = logging.getLogger(__name__)

def verify_pgw_status(
  ip: str, username: str, password: str
) -> dict:
  """Connects to Nutanix Prism Gateway API to check heartbeat status.

  Args:
    ip (str): The Prism Element or Prism Central Cluster IP / virtual IP.
    username (str): The Username for authentication.
    password (str): The Password for authentication.

  Returns:
    dict: A dictionary containing online status and heartbeat data or error message.
  """
  url = f"https://{ip}:9440/PrismGateway/services/rest/v1/heartbeat"
  auth = (username, password)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  try:
    resp = requests.get(
      url, auth=auth, headers=headers, verify=False, timeout=15
    )
    resp.raise_for_status()
    heartbeat_data = resp.json()

    logger.info(f"PGW status check successful for {ip}")
    return {
      "is_online": True,
      "status_data": heartbeat_data
    }

  except requests.exceptions.RequestException as e:
    logger.error(f"API Request Failed for {ip}: {e}")
    return {
      "is_online": False,
      "error_message": str(e)
    }

def run_pgw_collection(config_path: str = None) -> None:
  """Reads endpoints, collects PGW status, and prints JSON.

  Args:
    config_path (str, optional): Path to the endpoints JSON config file.
                                 Defaults to None (auto-resolves path).
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

  # Handle BOTH endpoints.json formats automatically
  all_endpoints = []
  if "pes" in config_data or "pcs" in config_data:
    all_endpoints.extend(config_data.get("pes", []))
    all_endpoints.extend(config_data.get("pcs", []))
  else:
    for zone, entries in config_data.items():
      if isinstance(entries, list):
        all_endpoints.extend(entries)

  final_results = {}

  for endpoint in all_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})

    # Use the global variables as fallback credentials
    user = creds.get("username", creds.get("user", DEF_UNAME))
    pwd = creds.get("password", DEF_PWD)

    if not ip:
      continue

    logger.info(f"Checking PGW status for cluster: {ip}...")
    final_results[ip] = verify_pgw_status(ip, user, pwd)

  # Print pure JSON to stdout so local_processor.py can parse it
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  # Python's logging module writes to stderr by default.
  # This keeps our logs separate from the printed JSON on stdout.
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_pgw_collection()

