"""Module to audit clusters for process segmentation faults (SIGSEGV).

This script connects to Nutanix Prism Element CVMs via SSH and utilizes the 
'allssh' command to scan log directories across all CVMs in the cluster 
for SIGSEGV crashes simultaneously. It outputs pure JSON for the CZMon framework.
"""

import json
import logging
import os
import sys
import paramiko
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Default Credentials ---
DEF_USER = "nutanix"
DEF_API_USER = "admin"
DEF_API_PWD = "CZNutanix.1234"
# SSH key for CVM authentication
CVM_SSH_KEY_PATH = os.path.join(
  os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
  "ssh", "keys", "nutanix"
)

logger = logging.getLogger(__name__)

def get_cvm_ip(cluster_ip: str, username: str, password: str) -> str:
  """Fetches a CVM IP from the Prism API for SSH access.
  
  Args:
    cluster_ip (str): The virtual IP address of the cluster.
    username (str): The Prism API username.
    password (str): The Prism API password.
    
  Returns:
    str: A CVM IP address, or the cluster_ip if API call fails.
  """
  try:
    url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/hosts/"
    response = requests.get(url, auth=(username, password), verify=False, timeout=10)
    response.raise_for_status()
    data = response.json()
    
    for entity in data.get("entities", []):
      cvm_ip = entity.get("service_vmexternal_ip")
      if cvm_ip:
        logger.info(f"Using CVM IP {cvm_ip} for SSH to cluster {cluster_ip}")
        return cvm_ip
    
    logger.warning(f"No CVM IP found for cluster {cluster_ip}, will try cluster VIP")
    return cluster_ip
  except Exception as e:
    logger.error(f"Failed to fetch CVM IP for {cluster_ip}: {e}")
    return cluster_ip

def audit_cvm_sigsegv(cvm_ip: str, user: str, max_retries: int = 3) -> dict:
  """Connects to a CVM and scans all CVM logs for SIGSEGV crashes using SSH key.

  Args:
    cvm_ip (str): The CVM IP address for SSH access.
    user (str): SSH username for CVM access (typically 'nutanix').
    max_retries (int): Maximum number of connection retry attempts.

  Returns:
    dict: A dictionary containing crash findings, detailed logs, and status.
  """
  all_cvm_details = []
  any_crash_found = False

  try:
    # Open SSH connection to CVM using SSH key
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    if os.path.exists(CVM_SSH_KEY_PATH):
      client.connect(
        hostname=cvm_ip, 
        username=user, 
        key_filename=CVM_SSH_KEY_PATH, 
        timeout=60,
        banner_timeout=60,
        auth_timeout=30
      )
      logger.info(f"Connected to CVM {cvm_ip} using SSH key")
    else:
      logger.error(f"SSH key not found: {CVM_SSH_KEY_PATH}")
      return {
        "error": f"SSH key not found: {CVM_SSH_KEY_PATH}",
        "status": "ERROR"
      }

  if not client:
    return {
      "error": f"Failed to establish SSH connection: {last_error}",
      "status": "ERROR"
    }

  try:
    # Use a login shell to load aliases, then run allssh to scan every CVM instantly
    cmd = 'bash -lc \'allssh "grep -r -l -I \\"SIGSEGV\\" /home/nutanix/data/logs/"\''
    stdin, stdout, stderr = client.exec_command(cmd)

    cvm_output = stdout.read().decode("utf-8").strip()
    client.close()

    # The allssh command outputs the CVM IPs alongside their grep results.
    # If the output contains the search string, a crash log was found.
    if "SIGSEGV" in cvm_output or "/home/nutanix/data/logs/" in cvm_output:
      any_crash_found = True
      all_cvm_details.append(f"CVM {cvm_ip} output:\n{cvm_output}")

    return {
      "crash_found": any_crash_found,
      "details": (
        "\n".join(all_cvm_details) if any_crash_found else "No SIGSEGV found."
      ),
      "status": "FAIL" if any_crash_found else "PASS"
    }

  except Exception as e:
    logger.error(f"Failed cluster audit for {cvm_ip}: {e}")
    return {
      "error": f"Paramiko SSH failed: {e}",
      "status": "ERROR"
    }

def run_crash_audit(config_path: str = None) -> None:
  """Reads endpoints config, audits clusters for crashes, and prints JSON.

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

  logger.info(f"Loading endpoints configuration from: {config_path}")
  try:
    with open(config_path, "r") as f:
      config_data = json.load(f)
  except Exception as e:
    logger.error(f"Failed to load config at {config_path}: {e}")
    sys.exit(1)

  # Target Prism Element endpoints from config
  pes = config_data.get("pes", [])

  # Fallback: if 'pes' is empty, search for PEs inside Zone layout
  if not pes:
    for key, nodes in config_data.items():
      if isinstance(nodes, list):
        for node in nodes:
          if node.get("type") == "PE" or "PE" in node.get("name", ""):
            pes.append(node)

  logger.info(f"Found {len(pes)} PE(s) to audit in {config_path}")
  if not pes:
    logger.warning("No PEs found in configuration - nothing to audit")
  
  final_results = {}

  for pe in pes:
    cluster_ip = pe.get("ip") or pe.get("virtual_ip")
    cluster_name = pe.get("name", cluster_ip)

    if not cluster_ip:
      continue

    # Get API credentials for fetching CVM IP
    creds = pe.get("credentials", pe)
    api_user = creds.get("username", creds.get("user", DEF_API_USER))
    api_pwd = creds.get("password", DEF_API_PWD)
    
    # Get SSH username (uses SSH key, no password)
    ssh_user = pe.get("cvm_user", creds.get("cvm_user", DEF_USER))

    logger.info(f"Auditing CVMs for SIGSEGV crashes: {cluster_name} ({cluster_ip})...")

    # Get actual CVM IP for SSH (cluster_ip is VIP for API only)
    cvm_ip = get_cvm_ip(cluster_ip, api_user, api_pwd)
    
    # MUST USE CLUSTER IP AS KEY FOR UI DROPDOWN MATCHING
    final_results[cluster_ip] = audit_cvm_sigsegv(cvm_ip, ssh_user)

  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_crash_audit()

