"""Checks if a Nutanix cluster is underutilized based on Memory usage.

This script connects to the Nutanix Prism API to gather stats.
It is designed to run independently and print pure JSON for the CZMon framework.
"""

import os
import sys
import json
import logging
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Threshold for underutilization alert
MEMORY_UNDERUTILIZATION_THRESHOLD_PCT = 20.0

logger = logging.getLogger(__name__)

def check_cluster_utilization(
  cluster_ip: str, username: str, password: str
) -> dict:
  """Connects to Nutanix Prism API to get utilization stats."""
  base_url = f"https://{cluster_ip}:9440/api/nutanix/v2.0"
  auth = (username, password)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  try:
    resp = requests.get(
      f"{base_url}/cluster", auth=auth, headers=headers, verify=False, timeout=15
    )
    resp.raise_for_status()
    cluster_data = resp.json()
    stats = cluster_data.get("stats", {})

    # ppm is parts per million. Divide by 10,000 to get percentage.
    cpu_ppm = float(stats.get("hypervisor_cpu_usage_ppm", 0))
    mem_ppm = float(stats.get("memory_usage_ppm", 0))

    cpu_pct = round(cpu_ppm / 10000.0, 2)
    mem_pct = round(mem_ppm / 10000.0, 2)
    iops = round(float(stats.get("controller_num_iops", 0)), 2)

    is_underutilized = mem_pct < MEMORY_UNDERUTILIZATION_THRESHOLD_PCT
    if is_underutilized:
      logger.warning(f"ALERT: Cluster memory on {cluster_ip} low ({mem_pct}%)!")

    return {
      "cluster_ip": cluster_ip,
      "cpu_usage_percent": cpu_pct,
      "memory_usage_percent": mem_pct,
      "iops": iops,
      "is_underutilized": is_underutilized
    }

  except requests.exceptions.RequestException as e:
    logger.error(f"API Request Failed for {cluster_ip}: {e}")
    return {"cluster_ip": cluster_ip, "error_message": str(e)}

def run_utilization_check(config_path: str = None) -> None:
  """Reads endpoints, collects utilization stats, and prints JSON."""
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, 'r') as f:
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
    user = creds.get("username", creds.get("user", "admin"))
    pwd = creds.get("password", "Nutanix.123")

    if not ip:
      continue

    logger.info(f"Checking utilization for cluster: {ip}...")
    final_results[ip] = check_cluster_utilization(ip, user, pwd)

  # Print pure JSON to stdout so local_processor.py can parse it
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  # Python's logging module writes to stderr by default.
  # This keeps our logs separate from the printed JSON on stdout.
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_utilization_check()

