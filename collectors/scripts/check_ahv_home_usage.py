"""Checks partition usage on AHV hosts.

This script utilizes a two-tier connection strategy to securely gather 
partition metrics across AHV hosts. It is designed to run independently
and print pure JSON for the CZMon framework.
"""

import json
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

import paramiko
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

# --- Global Configurations ---
DEF_UNAME = "admin"
DEF_PWD = "Nutanix.123"
SHELL_PROMPT_DELAY = 2
HOSTSSH_EXECUTION_DELAY = 15
BUFFER_POLL_DELAY = 1

def get_cluster_info(
  cluster_ip: str, username: str, password: str
) -> Tuple[Optional[str], Dict[str, str]]:
  """Fetches the cluster name and host mapping from the Prism API.

  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    username (str): The Prism API username.
    password (str): The Prism API password.

  Returns:
    Tuple[Optional[str], Dict[str, str]]: A tuple containing the cluster name
      (or None) and a dictionary mapping hypervisor IPs to their hostnames.
  """
  prism_auth = (username, password)
  cluster_name = None

  try:
    url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/cluster"
    response = requests.get(url, auth=prism_auth, verify=False, timeout=10)
    response.raise_for_status()
    cluster_name = response.json().get("name")
  except (requests.exceptions.RequestException, ValueError) as e:
    logger.error(f"Failed to fetch cluster info for {cluster_ip}: {e}")

  hosts_map = {}
  try:
    url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/hosts/"
    response = requests.get(url, auth=prism_auth, verify=False, timeout=10)
    response.raise_for_status()
    data = response.json()
    for entity in data.get("entities", []):
      name = entity.get("name")
      ip = entity.get("hypervisor_address")
      if name and ip:
        hosts_map[ip] = name
    return cluster_name, hosts_map
  except (requests.exceptions.RequestException, ValueError) as e:
    logger.error(f"Error fetching hosts from API for {cluster_ip}: {e}")
    return cluster_name, {}

def execute_ssh_command(
  ip: str,
  port: int,
  username: str,
  password: str,
  command: str,
  is_cvm: bool,
) -> str:
  """Establishes an SSH connection and executes a command.

  Args:
    ip (str): Target IP address to connect to.
    port (int): SSH port.
    username (str): SSH username.
    password (str): SSH password.
    command (str): Command to execute on the remote machine.
    is_cvm (bool): Flag indicating if connection is to a Controller VM,
      requiring interactive PTY menu handling.

  Returns:
    str: The string output of the command.
  """
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  try:
    client.connect(
      ip,
      username=username,
      password=password,
      port=port,
      timeout=15,
      auth_timeout=15,
    )
    if is_cvm:
      shell = client.invoke_shell()
      time.sleep(SHELL_PROMPT_DELAY)
      out = ""
      if shell.recv_ready():
        out = shell.recv(8192).decode("utf-8")

      if "Choice:" in out:
        shell.send("3\n")
        time.sleep(SHELL_PROMPT_DELAY)
        if shell.recv_ready():
          shell.recv(8192)

      shell.send(command + "\n")
      time.sleep(SHELL_PROMPT_DELAY)

      out = ""
      if shell.recv_ready():
        out = shell.recv(8192).decode("utf-8")

      if "password" in out.lower():
        shell.send(password + "\n")

      time.sleep(HOSTSSH_EXECUTION_DELAY)

      output = ""
      while shell.recv_ready():
        output += shell.recv(8192).decode("utf-8")
        time.sleep(BUFFER_POLL_DELAY)
    else:
      _, stdout, _ = client.exec_command(command)
      output = stdout.read().decode("utf-8").strip()

    return output
  finally:
    client.close()

def fetch_usage_data_from_host_via_cvm(
  cluster_ip: str, pe_user: str, pe_pass: str, hosts_map: Dict[str, str]
) -> str:
  """Gathers partition data by invoking an interactive shell on the CVM.

  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    pe_user (str): The Prism Element admin user.
    pe_pass (str): The Prism Element admin password.
    hosts_map (Dict[str, str]): Map of hypervisor IPs to hostnames.

  Returns:
    str: The raw text output from the hostssh command, or an empty string.
  """
  if not hosts_map:
    return ""

  try:
    output = execute_ssh_command(
      cluster_ip, 22, pe_user, pe_pass, "hostssh 'df -P -h'", is_cvm=True
    )
    if "%" in output and "not allowed" not in output:
      return output
  except Exception as e:
    logger.error(f"CVM SSH strategy failed for {cluster_ip}: {e}")

  return ""

def get_host_partition_info(ip: str, password: str) -> Dict[str, any]:
  """Connects directly to an AHV host via SSH to gather partition info.

  Args:
    ip (str): The IP address of the AHV host.
    password (str): The password to authenticate with (root).

  Returns:
    Dict[str, any]: A dictionary of partition metrics, or an error dictionary.
  """
  try:
    output = execute_ssh_command(
      ip, 22, "root", password, "df -P -h", is_cvm=False
    )
    partitions = {}
    for line in output.splitlines():
      parts = line.split()
      if (
        len(parts) >= 6
        and "%" in parts[-2]
        and not line.startswith("Filesystem")
      ):
        mount_point = parts[-1]
        partitions[mount_point] = {
          "total": parts[-5],
          "available": parts[-3],
          "usage": parts[-2],
        }
    if partitions:
      return partitions
  except Exception as e:
    logger.error(f"Direct AHV SSH failed for {ip} with root: {e}")
    return {"error": f"SSH Collection Failed: {e}"}

  return {"error": "SSH Collection Failed: No valid output returned"}

def parse_cvm_output(output: str, hosts_map: Dict[str, str]) -> List[Dict]:
  """Parses raw terminal df output into a structured dictionary.

  Args:
    output (str): The raw multi-line string output from hostssh.
    hosts_map (Dict[str, str]): Map of hypervisor IPs to hostnames.

  Returns:
    List[Dict]: A structured list mapping each host to its partition metrics.
  """
  results = []
  current_host = None

  single_node = list(hosts_map.values())[0] if len(hosts_map) == 1 else None
  output = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", output)
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

    if (
      "hostssh" not in line
      and "Permission denied" not in line
      and not line.startswith("Filesystem")
    ):
      parts = line.split()

      if len(parts) >= 6 and "%" in parts[-2]:
        mount_point = parts[-1]
        usage_data = {
          "total": parts[-5],
          "available": parts[-3],
          "usage": parts[-2],
        }
        target_host = current_host if current_host else single_node
        if target_host:
          if target_host not in host_data:
            host_data[target_host] = {}
          host_data[target_host][mount_point] = usage_data

  for host, partitions in host_data.items():
    results.append({host: partitions})

  return results

def collect_cluster_partition_usage(
  cluster_ip: str, pe_user: str, pe_pass: str
) -> Dict:
  """Orchestrates partition data collection for hosts within a cluster.

  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    pe_user (str): The Prism Element admin username.
    pe_pass (str): The Prism Element admin password.

  Returns:
    Dict: A dictionary containing collected host partition usage metrics.
  """
  cluster_name, hosts_map = get_cluster_info(cluster_ip, pe_user, pe_pass)

  if not cluster_name or not hosts_map:
    fallback = cluster_name if cluster_name else cluster_ip
    logger.error(f"Halting processing for {fallback} - missing metadata.")
    return {fallback: [{"error": "Could not fetch hosts map from API"}]}

  host_results = []
  cvm_output = fetch_usage_data_from_host_via_cvm(
    cluster_ip, pe_user, pe_pass, hosts_map
  )

  if cvm_output:
    host_results = parse_cvm_output(cvm_output, hosts_map)
  else:
    for ip, name in hosts_map.items():
      usage = get_host_partition_info(ip, pe_pass)
      host_results.append({name: usage})

  found_hostnames = {
    list(d.keys())[0] for d in host_results if d and isinstance(d, dict)
  }

  for name in hosts_map.values():
    if name not in found_hostnames:
      host_results.append({name: {"error": "Host missing from hostssh"}})

  return {cluster_name: host_results}

def collect_all_ahv_partition_usage(config_path: Optional[str] = None) -> None:
  """Fetches endpoints from config and triggers partition data collection.

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
    logger.error(f"Failed to load config: {e}")
    sys.exit(1)

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
    user = creds.get("username", creds.get("user", DEF_UNAME))
    pwd = creds.get("password", DEF_PWD)

    if not ip:
      continue

    logger.info(f"Fetching AHV host partition usage for IP: {ip}...")
    final_results[ip] = collect_cluster_partition_usage(ip, user, pwd)

  # CRITICAL: Print pure JSON to stdout so local_processor.py can parse it
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  # Silent on standard framework runs to avoid false stderr pollution
  logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  collect_all_ahv_partition_usage()

