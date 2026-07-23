"""
Checks the powered on/off VM counts and affinity status per node.

This script connects to the Nutanix Prism API to map each VM to its
corresponding host and tally the power states and affinity settings.
It is designed to run independently and print JSON for the framework.
"""

import json
import logging
import os
import sys

import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

def fetch_vm_count_per_node(
  cluster_ip: str,
  username: str,
  password: str,
) -> dict:
  """Connects to Nutanix Prism API to get VM counts and affinity status.

  Args:
    cluster_ip (str): The Prism Element Cluster IP or FQDN.
    username (str): The Prism Element Username for authentication.
    password (str): The Prism Element Password for authentication.

  Returns:
    dict: A dictionary containing the cluster name and counts per node.
  """
  base_url = f"https://{cluster_ip}:9440/api/nutanix/v2.0"
  auth = (username, password)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  try:
    cluster_resp = requests.get(
      f"{base_url}/cluster", auth=auth, headers=headers, verify=False, timeout=15
    )
    cluster_resp.raise_for_status()
    cluster_name = cluster_resp.json().get("name", "Unknown Cluster")

    hosts_resp = requests.get(
      f"{base_url}/hosts", auth=auth, headers=headers, verify=False, timeout=15
    )
    hosts_resp.raise_for_status()
    hosts_data = hosts_resp.json().get("entities", [])

    host_map = {}
    counts = {}

    for host in hosts_data:
      host_uuid = host.get("uuid") or host.get("host_uuid") or host.get("id")
      svm_ip = (
        host.get("service_vm_external_ip")
        or host.get("controller_vm_external_ip")
        or host.get("service_vm_ip")
        or host.get("controller_vm_ip")
      )

      if host_uuid and svm_ip:
        host_map[host_uuid] = svm_ip
        counts[svm_ip] = {
          "powered_on": 0, "powered_off": 0, "affinity_enabled_count": 0
        }

    vms_resp = requests.get(
      f"{base_url}/vms", auth=auth, headers=headers, verify=False, timeout=15
    )
    vms_resp.raise_for_status()
    vms_data = vms_resp.json().get("entities", [])

    if "Unassigned_Host" not in counts:
      counts["Unassigned_Host"] = {
        "powered_on": 0, "powered_off": 0, "affinity_enabled_count": 0
      }

    for vm in vms_data:
      host_uuid = vm.get("host_uuid")
      power_state = vm.get("power_state", "").lower()
      affinity = vm.get("affinity")

      target_svm = host_map.get(host_uuid, "Unassigned_Host")

      if power_state in ["on", "powered_on"]:
        counts[target_svm]["powered_on"] += 1
      elif power_state in ["off", "powered_off"]:
        counts[target_svm]["powered_off"] += 1

      if affinity:
        counts[target_svm]["affinity_enabled_count"] += 1

    if (
      counts["Unassigned_Host"]["powered_on"] == 0
      and counts["Unassigned_Host"]["powered_off"] == 0
      and counts["Unassigned_Host"]["affinity_enabled_count"] == 0
    ):
      del counts["Unassigned_Host"]

    output = {"Cluster_name": cluster_name}
    output.update(counts)
    return output

  except requests.exceptions.RequestException as e:
    logger.error(f"API Request Failed for {cluster_ip}: {e}")
    return {"error": f"API Request Failed: {str(e)}"}

def collect_all_endpoints(config_path: str = None) -> None:
  """Reads endpoints, collects data for all PE clusters, and prints JSON."""

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
  pe_endpoints = []
  if "pes" in config_data:
    pe_endpoints = config_data.get("pes", [])
  else:
    for zone, entries in config_data.items():
      if isinstance(entries, list):
        for entry in entries:
          if entry.get("type", "").upper() == "PE":
            pe_endpoints.append(entry)

  final_results = {}

  for endpoint in pe_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})
    user = creds.get("user", "admin")
    pwd = creds.get("password", "Nutanix.123")

    if not ip:
      continue

    logger.info(f"Checking VM count per node for cluster: {ip}...")
    final_results[ip] = fetch_vm_count_per_node(ip, user, pwd)

  # Print pure JSON to stdout so local_processor.py can parse it
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  # Python's logging module writes to stderr by default.
  # This keeps our logs separate from the printed JSON on stdout.
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )

  collect_all_endpoints()

