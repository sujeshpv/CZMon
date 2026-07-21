"""Checks partition usage on AHV hosts.

This script utilizes a two-tier connection strategy to securely gather 
partition metrics across AHV hosts and persists the data to SQLite.
"""

import json
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

import django
import paramiko
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

# --- Global Configurations ---
DEFAULT_CVM_USER = "nutanix"

# The interactive shell needs time to render the prompt and menu over the network
SHELL_PROMPT_DELAY = 2  
# Running 'hostssh' triggers execution across every node in the cluster, which takes time
HOSTSSH_EXECUTION_DELAY = 15  
# Polling delay while waiting for the output buffer to fill
BUFFER_POLL_DELAY = 1

def get_cluster_info(cluster_ip: str, username: str, password: str) -> Tuple[Optional[str], Dict[str, str]]:
  """Fetches the cluster name and host mapping from the Prism API."""
  auth_tuple = (username, password)
  cluster_name = None

  try:
    url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/cluster"
    response = requests.get(url, auth=auth_tuple, verify=False, timeout=10)
    response.raise_for_status()
    cluster_name = response.json().get("name")

    if not cluster_name:
      logger.error(f"Cluster name missing in API response for {cluster_ip}")
      return None, {}
  except requests.exceptions.RequestException as e:
    logger.error(f"Failed to fetch cluster info for {cluster_ip}: {e}")
    return None, {}

  hosts_map = {}
  try:
    url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/hosts/"
    response = requests.get(url, auth=auth_tuple, verify=False, timeout=10)
    response.raise_for_status()
    data = response.json()
    for entity in data.get("entities", []):
      name = entity.get("name")
      ip = entity.get("hypervisor_address")
      if name and ip:
        hosts_map[ip] = name
    return cluster_name, hosts_map
  except requests.exceptions.RequestException as e:
    logger.error(f"Error fetching hosts from API for {cluster_ip}: {e}")
    return cluster_name, {}

def run_cvm_ssh_strategy(cluster_ip: str, pe_user: str, passwords: List[str], hosts_map: Dict[str, str]) -> Optional[str]:
  """Tier 1: Gathers partition data by invoking an interactive shell on the CVM."""
  if not hosts_map:
    return None

  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  for user in [DEFAULT_CVM_USER, pe_user]:
    for pwd in passwords:
      try:
        client.connect(cluster_ip, username=user, password=pwd, timeout=15, auth_timeout=15)

        shell = client.invoke_shell()
        # Wait for the initial shell prompt or NuService Menu to render
        time.sleep(SHELL_PROMPT_DELAY)

        out = ""
        if shell.recv_ready():
          out = shell.recv(8192).decode('utf-8')

        # Bypass the restricted NuService Menu if present
        if "Choice:" in out:
          shell.send("3\n")
          # Wait for the restricted menu to transition to the Admin Shell
          time.sleep(SHELL_PROMPT_DELAY)
          if shell.recv_ready():
            shell.recv(8192)

        # Execute hostssh to check ALL partitions
        shell.send("hostssh 'df -P -h'\n")
        # Wait briefly to see if the CVM prompts for a sudo password
        time.sleep(SHELL_PROMPT_DELAY)

        out = ""
        if shell.recv_ready():
          out = shell.recv(8192).decode('utf-8')

        if "password" in out.lower():
          shell.send(pwd + "\n")

        # Wait for the hostssh command to execute across all hypervisors
        time.sleep(HOSTSSH_EXECUTION_DELAY)

        output = ""
        while shell.recv_ready():
          output += shell.recv(8192).decode("utf-8")
          # Poll briefly to ensure the buffer is fully drained
          time.sleep(BUFFER_POLL_DELAY)

        client.close()

        if "%" in output and "not allowed" not in output:
          return output
      except Exception as e:
        logger.error(f"CVM SSH strategy failed for user {user} on {cluster_ip}: {e}")
        continue

  return None

def get_direct_ahv_usage(ip: str, passwords: List[str]) -> Optional[Dict[str, Dict[str, str]]]:
  """Tier 2: Fallback to direct AHV SSH if the CVM strategy fails."""
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  for user, port in [("root", 22), ("nutant", 2223)]:
    for pwd in passwords:
      try:
        client.connect(ip, username=user, password=pwd, port=port, timeout=5, auth_timeout=5)
        _, stdout, _ = client.exec_command("df -P -h")
        output = stdout.read().decode('utf-8').strip()
        client.close()

        partitions = {}
        for line in output.splitlines():
          parts = line.split()
          if len(parts) >= 6 and "%" in parts[-2] and not line.startswith("Filesystem"):
            mount_point = parts[-1]
            partitions[mount_point] = {
              "total": parts[-5],
              "available": parts[-3],
              "usage": parts[-2]
            }

        if partitions:
          return partitions
      except Exception as e:
        logger.error(f"Direct AHV SSH failed for {ip} with user {user}: {e}")
  return None

def parse_cvm_output(output: str, hosts_map: Dict[str, str]) -> List[Dict]:
  """Parses raw terminal df output into a structured dictionary."""
  results = []
  current_host = None
  single_node_host = list(hosts_map.values())[0] if len(hosts_map) == 1 else None

  # Clean ANSI escape sequences
  output = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', output)
  host_data = {}

  for line in output.splitlines():
    line = line.strip()
    if not line:
      continue

    match = re.search(r"=============\s*([\d.]+)\s*============", line)
    if match:
      ip = match.group(1)
      current_host = hosts_map.get(ip, ip)
      continue

    if line.startswith("=============") and not match:
      ip_match = re.search(r"([\d.]+)", line)
      if ip_match:
        ip = ip_match.group(1)
        current_host = hosts_map.get(ip, ip)
      continue

    # Strictly parse valid df output lines
    if "hostssh" not in line and "Permission denied" not in line and not line.startswith("Filesystem"):
      parts = line.split()

      # Using exact negative indices to grab any partition
      if len(parts) >= 6 and "%" in parts[-2]:
        mount_point = parts[-1]
        usage_data = {
          "total": parts[-5],
          "available": parts[-3],
          "usage": parts[-2],
        }

        target_host = current_host if current_host else single_node_host
        if target_host:
          if target_host not in host_data:
            host_data[target_host] = {}
          host_data[target_host][mount_point] = usage_data

  for host, partitions in host_data.items():
    results.append({host: partitions})

  return results

def process_cluster(cluster_ip: str, pe_user: str, pe_pass: str) -> Dict:
  """Orchestrates data collection for a single cluster."""
  passwords = list(dict.fromkeys([pe_pass, "nutanix/4u", "Nutanix.123", "RDMCluster.123"]))
  cluster_name, hosts_map = get_cluster_info(cluster_ip, pe_user, pe_pass)

  if not cluster_name or not hosts_map:
    # Fallback to IP if name resolution entirely failed
    fallback_name = cluster_name if cluster_name else cluster_ip
    logger.error(f"Halting processing for {fallback_name} due to missing cluster metadata.")
    return {fallback_name: [{"error": "Could not fetch cluster info or hosts map from API"}]}

  host_results = []
  cvm_output = run_cvm_ssh_strategy(cluster_ip, pe_user, passwords, hosts_map)

  if cvm_output:
    host_results = parse_cvm_output(cvm_output, hosts_map)
  else:
    for ip, name in hosts_map.items():
      usage = get_direct_ahv_usage(ip, passwords)
      if usage:
        host_results.append({name: usage})

  found_hostnames = {list(d.keys())[0] for d in host_results if d and isinstance(d, dict) and "error" not in list(d.values())[0]}
  for name in hosts_map.values():
    if name not in found_hostnames:
      host_results.append({name: {"error": "Could not fetch usage data"}})

  return {cluster_name: host_results}

def run_ahv_home_usage(config_path: Optional[str] = None) -> None:
  """Reads endpoints from config and persists usage data to DB."""
  from coreapp.models import AhvHomeUsage
  from django.conf import settings

  if not config_path:
    config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

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

    logger.info(f"Checking AHV partition usage for cluster: {ip}...")
    result = process_cluster(ip, user, pwd)

    for cluster_name, hosts_data in result.items():
      obj, created = AhvHomeUsage.objects.update_or_create(
        cluster_name=cluster_name,
        defaults={
          "status_data": hosts_data,
        }
      )
      action = "Created" if created else "Updated"
      logger.info(f"{action} DB record for AHV usage on {cluster_name}")

if __name__ == "__main__":
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
  django.setup()

  # Configure local logging for manual execution
  logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

  run_ahv_home_usage()
