"""Module for Cassandra node consistency check (Ticket ENG-925555)."""

import json
import logging
import os
import re
import sys
import paramiko

# --- Global Default Credentials ---
DEF_USER = "nutanix"
DEF_PWD = "Pitadmin@1234"

logger = logging.getLogger(__name__)

def check_node_consistency(ip: str, user: str, password: str) -> dict:
  """Runs svmips and nodetool ring check to verify Cassandra consistency.

  Args:
    ip (str): The IP address of a CVM in the Prism Element cluster.
    user (str): SSH username for CVM access.
    password (str): SSH password for CVM access.

  Returns:
    dict: A dictionary containing nodetool output, svmips, and consistency status.
  """
  try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=ip, username=user, password=password, timeout=15)

    # Run both commands in a single login shell channel to load aliases and bypass CVM limits
    cmd = "bash -lc 'svmips && echo \"---SPLIT---\" && nodetool -h 0 ring'"
    stdin, stdout, stderr = client.exec_command(cmd)
    combined_out = stdout.read().decode("utf-8")
    client.close()

    # Split the outputs safely
    if "---SPLIT---" in combined_out:
      svmips_out, nodetool_out = combined_out.split("---SPLIT---", 1)
    else:
      svmips_out = combined_out
      nodetool_out = ""

    svmips_out = svmips_out.strip()
    nodetool_out = nodetool_out.strip()

    # Retrieve all IP addresses present in the Cassandra ring table
    nodetool_ips = re.findall(r"\d+\.\d+\.\d+\.\d+", nodetool_out)

    # Protect against false positives if commands fail or return empty strings
    if not nodetool_ips or not svmips_out:
      check_pass = False
    else:
      check_pass = all(ip in nodetool_ips for ip in svmips_out.split())

    return {
      "nodetool_op": nodetool_out,
      "svmips": svmips_out,
      "svms_check": check_pass
    }

  except Exception as e:
    logger.error(f"SSH execution failed for cluster CVM {ip}: {e}")
    return {"error": f"Paramiko SSH failed: {e}"}

def run_node_tool_check(config_path: str = None) -> None:
  """Reads endpoints config, executes nodetool checks on PEs, and prints JSON.

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

  # Cassandra ring metrics reside directly on Prism Element (pes) CVM nodes
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

    logger.info(f"Checking Cassandra consistency: {cluster_name} ({ip})...")
    final_results[cluster_name] = check_node_consistency(ip, user, pwd)

  # Print pure JSON output to stdout for local_processor.py to capture
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_node_tool_check()

