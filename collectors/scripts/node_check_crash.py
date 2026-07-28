"""Module to audit clusters for process segmentation faults (SIGSEGV).

This script connects to Nutanix Prism Element CVMs via SSH, retrieves the list
of all SVM IPs in the cluster, and scans log directories on each CVM for
SIGSEGV crashes. It outputs pure JSON for the CZMon framework.
"""

import json
import logging
import os
import sys
import paramiko

# --- Global Default Credentials ---
DEF_USER = "nutanix"
DEF_PWD = "Pitadmin@1234"

logger = logging.getLogger(__name__)

def audit_node_sigsegv(cluster_ip: str, user: str, password: str) -> dict:
  """Connects to CVMs in a cluster and scans logs for SIGSEGV crashes.

  Args:
    cluster_ip (str): The primary CVM IP address for the cluster.
    user (str): SSH username for CVM access.
    password (str): SSH password for CVM access.

  Returns:
    dict: A dictionary containing crash findings, detailed logs, and status.
  """
  all_node_details = []
  any_crash_found = False

  try:
    # Connect to primary CVM to get list of all CVM IPs in the cluster
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=cluster_ip, username=user, password=password, timeout=15)

    stdin, stdout, stderr = client.exec_command("svmips")
    svm_ips = stdout.read().decode("utf-8").strip().split()
    client.close()

    if not svm_ips:
      svm_ips = [cluster_ip]

    # Iterate through every individual CVM node in the cluster
    for node_ip in svm_ips:
      try:
        node_client = paramiko.SSHClient()
        node_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        node_client.connect(hostname=node_ip, username=user, password=password, timeout=15)

        cmd = 'grep -r -l -I "SIGSEGV" /home/nutanix/data/logs/'
        stdin, stdout, stderr = node_client.exec_command(cmd)
        node_output = stdout.read().decode("utf-8").strip()
        node_client.close()

        if node_output:
          any_crash_found = True
          all_node_details.append(f"NODE {node_ip}:\n{node_output}")

      except Exception as node_err:
        logger.error(f"Failed SSH check on CVM node {node_ip}: {node_err}")
        all_node_details.append(f"NODE {node_ip} error: {node_err}")

    return {
      "crash_found": any_crash_found,
      "details": (
        "\n".join(all_node_details) if any_crash_found else "No SIGSEGV found."
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

    # Credential fallback to DEF_USER and DEF_PWD
    creds = pe.get("credentials", {})
    user = pe.get("ssh_user", creds.get("username", creds.get("user", DEF_USER)))
    pwd = pe.get("ssh_password", creds.get("password", DEF_PWD))

    logger.info(f"Auditing cluster for SIGSEGV crashes: {cluster_name} ({ip})...")
    final_results[cluster_name] = audit_node_sigsegv(ip, user, pwd)

  # Print pure JSON to stdout for local_processor.py to capture
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_crash_audit()

