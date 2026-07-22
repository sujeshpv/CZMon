"""Gets the powered on/off VM counts and affinity status per host in a Nutanix cluster.

This script connects to the Nutanix Prism API to map each VM to its
corresponding host and tally the power states and affinity settings.
"""

import argparse
import getpass
import json
import logging
import os
import sys

import requests
import urllib3
import django

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

def get_vm_count_per_host(
  cluster_ip: str,
  username: str,
  password: str,
) -> str:
  """Connects to Nutanix Prism API to get VM counts and affinity status per host."""
  base_url = f"https://{cluster_ip}:9440/api/nutanix/v2.0"
  auth = (username, password)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  try:
    # 1. Get cluster info to extract the cluster name
    cluster_resp = requests.get(
      f"{base_url}/cluster",
      auth=auth,
      headers=headers,
      verify=False,
      timeout=15,
    )
    cluster_resp.raise_for_status()
    cluster_name = cluster_resp.json().get("name", "Unknown Cluster")

    # 2. Get hosts to map host_uuid to SVM IP
    hosts_resp = requests.get(
      f"{base_url}/hosts",
      auth=auth,
      headers=headers,
      verify=False,
      timeout=15,
    )
    hosts_resp.raise_for_status()
    hosts_data = hosts_resp.json().get("entities", [])

    host_map = {}  # Mapping of host_uuid -> svm_ip
    counts = {}    # Output dictionary for holding the SVM counts

    for host in hosts_data:
      host_uuid = host.get("uuid") or host.get("host_uuid") or host.get("id")
      svm_ip = (
        host.get("service_vm_external_ip")
        or host.get("controller_vm_external_ip")
        or host.get("service_vm_ip")
        or host.get("controller_vm_ip")
        or host.get("hypervisor_address")
        or host.get("name")
        or host_uuid
      )

      if host_uuid and svm_ip:
        host_map[host_uuid] = svm_ip
        counts[svm_ip] = {
          "powered_on": 0, 
          "powered_off": 0, 
          "affinity_enabled_count": 0
        }

    # 3. Get all VMs
    vms_resp = requests.get(
      f"{base_url}/vms",
      auth=auth,
      headers=headers,
      verify=False,
      timeout=15,
    )
    vms_resp.raise_for_status()
    vms_data = vms_resp.json().get("entities", [])

    # 4. Count the VMs per SVM based on power state and affinity
    if "Unassigned_Host" not in counts:
      counts["Unassigned_Host"] = {
        "powered_on": 0, 
        "powered_off": 0, 
        "affinity_enabled_count": 0
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

    # 5. Format the final output
    output = {"Cluster_name": cluster_name}
    output.update(counts)

    return json.dumps(output, indent=2)

  except requests.exceptions.RequestException as e:
    logger.error(f"API Request Failed for {cluster_ip}: {e}")
    return json.dumps({"error": f"API Request Failed: {str(e)}"}, indent=2)

def fetch_vm_count_collection(config_path: str = None) -> None:
  """Reads endpoints from config and persists VM Count per host to DB."""
  from django.conf import settings
  from coreapp.models import VmCountPerHost

  if not config_path:
    config_path = os.path.join(
      settings.BASE_DIR, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, 'r') as f:
      config_data = json.load(f)
  except Exception as e:
    logger.error(f"Failed to load config: {e}")
    return

  all_endpoints = [entry for zone in config_data.values() for entry in zone]

  for endpoint in all_endpoints:
    if endpoint.get("type", "").upper() != "PE":
      continue    

    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})
    user = creds.get("user", "admin")
    pwd = creds.get("password", "Nutanix.123")

    if not ip:
      continue

    logger.info(f"Checking VM count per host for cluster: {ip}...")

    try:
      result_json_str = get_vm_count_per_host(ip, user, pwd)
      result_dict = json.loads(result_json_str)
    except Exception as e:
      logger.error(f"Failed to process {ip}: {e}")
      result_dict = {"error": f"Data Processing Failed: {str(e)}"}

    cluster_name = result_dict.get("Cluster_name", ip)

    try:
      obj, created = VmCountPerHost.objects.update_or_create(
        cluster_ip=ip,
        defaults={
          "cluster_name": cluster_name,
          "status_data": result_dict,
        }
      )
      action = "Created" if created else "Updated"
      logger.info(f"{action} DB record for VM count on {cluster_name}")
    except Exception as e:
      logger.error(f"Failed to save DB record for {ip}: {e}")


if __name__ == "__main__":
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
  django.setup()
  
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )

  fetch_vm_count_collection()
