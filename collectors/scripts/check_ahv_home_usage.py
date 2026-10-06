"""Checks partition usage on AHV hosts.

Always collects through the SVM: CZMon SSHes to the CVM, then the CVM
reaches each AHV host with hostssh or nested SSH.
"""

import json
import logging
import os
import re
import shlex
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
DEF_PWD = "CZNutanix.1234"
DEF_CVM_USER = "nutanix"
CVM_SSH_KEY_PATH = os.path.join(
  os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
  "ssh", "keys", "nutanix"
)
SHELL_PROMPT_DELAY = 2
HOSTSSH_EXECUTION_DELAY = 15
BUFFER_POLL_DELAY = 1
HOST_DF_COMMAND = "df -P -h"


def _cvm_usernames(cvm_user: str) -> List[str]:
  """Build a list of usernames to try when SSHing to the CVM.
  
  Args:
    cvm_user (str): The configured CVM SSH username.
    
  Returns:
    List[str]: A list of unique usernames to attempt, with the configured
      CVM user first, followed by the default 'nutanix' OS user as fallback.
  """
  users = []
  for user in (cvm_user, "nutanix"):
    if user and user not in users:
      users.append(user)
  return users


def _parse_df_output(output: str) -> Dict[str, any]:
  """Parse df -P -h output lines into structured partition metrics.
  
  Extracts partition information from POSIX-format df output, capturing
  the mount point, total size, available space, and usage percentage.
  
  Args:
    output (str): Raw output from 'df -P -h' command.
    
  Returns:
    Dict[str, any]: A dictionary mapping mount points to their metrics.
      Each entry contains 'total', 'available', and 'usage' keys.
      Example: {'/': {'total': '100G', 'available': '20G', 'usage': '80%'}}
  """
  partitions = {}
  for line in (output or "").splitlines():
    parts = line.split()
    if (
      len(parts) >= 6
      and "%" in parts[-2]
      and not line.startswith("Filesystem")
    ):
      partitions[parts[-1]] = {
        "total": parts[-5],
        "available": parts[-3],
        "usage": parts[-2],
      }
  return partitions


def get_cluster_info(
  cluster_ip: str, username: str, password: str
) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
  """Fetches the cluster name, host mapping, and a CVM IP from the Prism API.

  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    username (str): The Prism API username.
    password (str): The Prism API password.

  Returns:
    Tuple[Optional[str], Dict[str, str], Optional[str]]: Cluster name (or None),
      map of hypervisor IPs to hostnames, and a CVM IP for SSH access.
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
  cvm_ip = None
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
      
      # Get the first CVM IP for SSH access
      if not cvm_ip:
        cvm_ip = (
          entity.get("service_vmexternal_ip") or
          entity.get("controller_vm_backplane_ip") or
          entity.get("service_vm_external_ip") or
          entity.get("ipmi_address")
        )
    
    if cvm_ip:
      logger.info(f"Using CVM IP {cvm_ip} for SSH to cluster {cluster_ip}")
    else:
      logger.warning(f"No CVM IP found for {cluster_ip}, will use cluster VIP")
    
    return cluster_name, hosts_map, cvm_ip
  except (requests.exceptions.RequestException, ValueError) as e:
    logger.error(f"Error fetching hosts from API for {cluster_ip}: {e}")
    return cluster_name, {}, None

def execute_ssh_command(
  ip: str,
  port: int,
  username: str,
  password: str = None,
  command: str = None,
  is_cvm: bool = False,
  key_filename: str = None,
) -> str:
  """Establishes an SSH connection and executes a command.

  Args:
    ip (str): Target IP address to connect to.
    port (int): SSH port.
    username (str): SSH username.
    password (str, optional): SSH password. Not used if key_filename is provided.
    command (str): Command to execute on the remote machine.
    is_cvm (bool): Flag indicating if connection is to a Controller VM,
      requiring interactive PTY menu handling.
    key_filename (str, optional): Path to SSH private key file for key-based auth.

  Returns:
    str: The string output of the command.
  """
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  try:
    connect_kwargs = {
      "hostname": ip,
      "username": username,
      "port": port,
      "timeout": 60,
      "banner_timeout": 60,
      "auth_timeout": 30,
    }
    
    # Use SSH key if provided, otherwise use password
    if key_filename and os.path.exists(key_filename):
      connect_kwargs["key_filename"] = key_filename
    elif password:
      connect_kwargs["password"] = password
    else:
      raise ValueError("Either password or key_filename must be provided")
    
    client.connect(**connect_kwargs)
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
      _, stdout, stderr = client.exec_command(command, timeout=30)
      output = stdout.read().decode("utf-8").strip()
      error_output = stderr.read().decode("utf-8").strip()
      
      if error_output:
        logger.warning(f"SSH command stderr for {ip}: {error_output}")
      
      if not output and error_output:
        logger.error(f"SSH command failed for {ip}. Stderr: {error_output}")

    return output
  finally:
    client.close()


def run_command_on_cvm(
  cluster_ip: str, cvm_user: str, remote_command: str
) -> str:
  """SSH to the CVM (SVM) and execute a command using SSH key authentication.
  
  Args:
    cluster_ip (str): The virtual IP address of the cluster (CVM target).
    cvm_user (str): The CVM SSH username (typically 'nutanix').
    remote_command (str): The command to execute on the CVM.
    
  Returns:
    str: The command output, or empty string if all attempts fail.
  """
  # Source Nutanix profile and run the command
  # Use full path for hostssh if it's in the command
  if "hostssh" in remote_command and not remote_command.startswith("/usr/local/nutanix"):
    cmd_to_run = remote_command.replace("hostssh", "/usr/local/nutanix/cluster/bin/hostssh", 1)
  else:
    cmd_to_run = remote_command
  
  # Wrap command to source profile first, then run
  wrapped = f"source /etc/profile.d/nutanix_env.sh 2>/dev/null; {cmd_to_run}"
  
  last_error = None
  for username in _cvm_usernames(cvm_user):
    # Try with exec_command (cleaner output)
    try:
      output = execute_ssh_command(
        ip=cluster_ip,
        port=22,
        username=username,
        command=wrapped,
        is_cvm=False,
        key_filename=CVM_SSH_KEY_PATH
      )
      if output and "not allowed" not in output.lower():
        return output
    except Exception as err:
      last_error = err
      logger.error(f"CVM exec_command failed for {cluster_ip} as {username}: {err}")
    
    # Fallback: Try with interactive shell
    try:
      output = execute_ssh_command(
        ip=cluster_ip,
        port=22,
        username=username,
        command=cmd_to_run,
        is_cvm=True,
        key_filename=CVM_SSH_KEY_PATH
      )
      if output and "not allowed" not in output.lower():
        return output
    except Exception as err:
      last_error = err
      logger.error(f"CVM interactive SSH failed for {cluster_ip} as {username}: {err}")
  
  if last_error:
    logger.error(f"All CVM SSH attempts failed for {cluster_ip}: {last_error}")
  return ""


def fetch_usage_data_from_host_via_cvm(
  cluster_ip: str, cvm_user: str, hosts_map: Dict[str, str]
) -> str:
  """Gather partition data on all AHV hosts from the SVM with hostssh.

  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    cvm_user (str): The CVM SSH username.
    hosts_map (Dict[str, str]): Map of hypervisor IPs to hostnames.

  Returns:
    str: The raw text output from the hostssh command, or an empty string.
  """
  if not hosts_map:
    return ""

  output = run_command_on_cvm(
    cluster_ip, cvm_user, f"hostssh {shlex.quote(HOST_DF_COMMAND)}"
  )
  if "%" in output:
    return output
  return ""


def get_host_partition_info_via_svm(
  cluster_ip: str, cvm_user: str, host_ip: str
) -> Dict[str, any]:
  """Collect partition info from a single AHV host via nested SSH through the CVM.
  
  This function performs a two-hop SSH connection:
  1. SSH to the CVM using SSH key
  2. SSH from the CVM to the target AHV host (using passwordless SSH keys)
  3. Run 'df -P -h' on the AHV host
  
  Args:
    cluster_ip (str): The virtual IP address of the cluster (CVM).
    cvm_user (str): The CVM SSH username.
    host_ip (str): The IP address of the target AHV host.
    
  Returns:
    Dict[str, any]: A dictionary containing 'partitions' (parsed metrics)
      and 'raw_output' (complete df output), or an 'error' key if collection fails.
  """
  nested = (
    "ssh -o BatchMode=yes -o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "
    f"root@{host_ip} {shlex.quote(HOST_DF_COMMAND)}"
  )
  try:
    output = run_command_on_cvm(cluster_ip, cvm_user, nested)
    partitions = _parse_df_output(output)
    if partitions:
      return {
        "partitions": partitions,
        "raw_output": output
      }
    return {
      "error": "SVM-to-host SSH returned no df data",
      "raw_output": output
    }
  except Exception as err:
    logger.error(f"SVM-to-host SSH failed for {host_ip} via {cluster_ip}: {err}")
    return {"error": f"SVM-to-host SSH failed: {err}"}


def parse_cvm_output(output: str, hosts_map: Dict[str, str]) -> Tuple[List[Dict], str]:
  """Parses raw terminal df output into a structured dictionary.

  Args:
    output (str): The raw multi-line string output from hostssh.
    hosts_map (Dict[str, str]): Map of hypervisor IPs to hostnames.

  Returns:
    Tuple[List[Dict], str]: A structured list mapping each host to its partition 
      metrics, and the raw output string.
  """
  results = []
  current_host = None

  single_node = list(hosts_map.values())[0] if len(hosts_map) == 1 else None
  cleaned_output = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", output)
  host_data = {}

  for line in cleaned_output.splitlines():
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
    results.append({host: {"partitions": partitions}})

  return results, output

def collect_cluster_partition_usage(
  cluster_ip: str, pe_user: str, pe_pass: str, cvm_user: str, explicit_cvm_ip: str = None
) -> Dict:
  """Orchestrates partition data collection for hosts within a cluster.

  Args:
    cluster_ip (str): The virtual IP address of the cluster (for API access).
    pe_user (str): The Prism Element admin username (for REST API).
    pe_pass (str): The Prism Element admin password (for REST API).
    cvm_user (str): The CVM SSH username (uses SSH key from ssh/keys/nutanix).
    explicit_cvm_ip (str, optional): Explicitly configured CVM IP (overrides API lookup).

  Returns:
    Dict: A dictionary containing collected host partition usage metrics.
  """
  cluster_name, hosts_map, cvm_ip = get_cluster_info(cluster_ip, pe_user, pe_pass)

  if not cluster_name or not hosts_map:
    fallback = cluster_name if cluster_name else cluster_ip
    logger.error(f"Halting processing for {fallback} - missing metadata.")
    return {fallback: [{"error": "Could not fetch hosts map from API"}]}
  
  # Use explicitly configured CVM IP, or API-discovered CVM IP, or fallback to cluster VIP
  ssh_target = explicit_cvm_ip or cvm_ip or cluster_ip
  
  if ssh_target == cluster_ip and not explicit_cvm_ip:
    logger.warning(f"No CVM IP found, falling back to cluster VIP {cluster_ip} (SSH may fail)")

  host_results = []
  raw_hostssh_output = ""
  cvm_output = fetch_usage_data_from_host_via_cvm(
    ssh_target, cvm_user, hosts_map
  )

  if cvm_output:
    host_results, raw_hostssh_output = parse_cvm_output(cvm_output, hosts_map)

  found_hostnames = {
    list(d.keys())[0] for d in host_results if d and isinstance(d, dict)
  }

  for ip, name in hosts_map.items():
    if name in found_hostnames:
      continue
    host_results.append({
      name: get_host_partition_info_via_svm(
        ssh_target, cvm_user, ip
      )
    })

  result = {cluster_name: host_results}
  if raw_hostssh_output:
    result["_raw_hostssh_output"] = raw_hostssh_output
  
  return result

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
    
    # Extract Prism UI credentials (for REST API)
    user = (
      creds.get("username") or 
      creds.get("user") or 
      endpoint.get("user") or 
      DEF_UNAME
    )
    pwd = creds.get("password") or endpoint.get("password") or DEF_PWD
    
    # Extract CVM SSH username (uses SSH key from ssh/keys/nutanix)
    cvm_user = (
      creds.get("cvm_user") or 
      endpoint.get("cvm_user") or 
      DEF_CVM_USER
    )
    
    # Optional: explicitly specified CVM IP in config (overrides API lookup)
    explicit_cvm_ip = endpoint.get("cvm_ip") or creds.get("cvm_ip")

    if not ip:
      continue

    logger.info(f"Fetching AHV host partition usage for cluster: {ip}...")
    final_results[ip] = collect_cluster_partition_usage(
      ip, user, pwd, cvm_user, explicit_cvm_ip
    )

  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  # Silent on standard framework runs to avoid false stderr pollution
  logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  collect_all_ahv_partition_usage()

