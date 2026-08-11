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

# --- Global Default Credentials ---
DEF_USER = "admin"
DEF_PWD = "Nutanix.123"

logger = logging.getLogger(__name__)

def audit_cvm_sigsegv(cluster_ip: str, user: str, password: str) -> dict:
  """Connects to a cluster and scans all CVM logs for SIGSEGV crashes.

  Args:
    cluster_ip (str): The primary CVM IP address for the cluster.
    user (str): SSH username for CVM access.
    password (str): SSH password for CVM access.

  Returns:
    dict: A dictionary containing crash findings, detailed logs, and status.
  """
  all_cvm_details = []
  any_crash_found = False

  try:
    # Open a single SSH connection to the cluster
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=cluster_ip, username=user, password=password, timeout=15)

    # Use a login shell to load aliases, then run allssh to scan every CVM instantly
    cmd = 'bash -lc \'allssh "grep -r -l -I \\"SIGSEGV\\" /home/nutanix/data/logs/"\''
    stdin, stdout, stderr = client.exec_command(cmd)

    cvm_output = stdout.read().decode("utf-8").strip()
    client.close()

    # The allssh command outputs the CVM IPs alongside their grep results.
    # If the output contains the search string, a crash log was found.
    if "SIGSEGV" in cvm_output or "/home/nutanix/data/logs/" in cvm_output:
      any_crash_found = True
      all_cvm_details.append(f"Cluster {cluster_ip} output:\n{cvm_output}")

    return {
      "crash_found": any_crash_found,
      "details": (
        "\n".join(all_cvm_details) if any_crash_found else "No SIGSEGV found."
      ),
      "status": "FAIL" if any_crash_found else "PASS"
    }

  except Exception as e:
    logger.error(f"Failed cluster audit for {cluster_ip}: {e}")
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

  try:
    with open(config_path, "r") as f:
      config_data = json.load(f)
  except Exception as e:
    logger.error(f"Failed to load config at {config_path}: {e}")
    sys.exit(1)

  # Target Prism Element endpoints from config
  pes = config_data.get("pes", [])
  final_results = {}

  for pe in pes:
    ip = pe.get("ip") or pe.get("virtual_ip")
    cluster_name = pe.get("name", ip)

    if not ip:
      continue

    creds = pe.get("credentials", {})
    user = pe.get("ssh_user", creds.get("username", creds.get("user", DEF_USER)))
    pwd = pe.get("ssh_password", creds.get("password", DEF_PWD))

    logger.info(f"Auditing CVMs for SIGSEGV crashes: {cluster_name} ({ip})...")
    final_results[cluster_name] = audit_cvm_sigsegv(ip, user, pwd)

  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_crash_audit()

