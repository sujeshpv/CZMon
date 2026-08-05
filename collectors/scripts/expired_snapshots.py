"""Module to Track expired system-retained snapshots ENG-924183"""

import json
import os
import sys
import paramiko

DEF_USER = 'nutanix'
DEF_PWD = 'Nutanix.123'

def parse_raw_output(raw_text: str) -> list:
  """Parses the pipe-delimited text table into a list of dictionaries.

  Args:
    raw_text (str): The raw string output returned from the command.

  Returns:
    list: A list of dictionaries containing structured snapshot information.
  """
  parsed_rows = []
  if not raw_text:
    return parsed_rows

  lines = raw_text.strip().split('\n')
  if len(lines) <= 1:
    return parsed_rows

  for line in lines[1:]:
    parts = [p.strip() for p in line.split('|')]
    if len(parts) >= 7:
      parsed_rows.append({
        "vm_name": parts[0],
        "entity_type": parts[1],
        "entity_uuid": parts[2],
        "snapshot_uuid": parts[3],
        "rp_name": parts[4],
        "created_at": parts[5],
        "expired_at": parts[6]
      })

  return parsed_rows

def verify_expired_snapshots(ip: str, user: str, password: str) -> tuple:
  """Executes the snapshot utility via SSH to grab expired recovery points.

  Args:
    ip (str): The target IP address to connect to.
    user (str): The SSH username for authentication.
    password (str): The SSH password for authentication.

  Returns:
    tuple: A tuple containing a summary dictionary, a list of expired snapshots, 
           and a boolean indicating success.
  """
  try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=ip, username=user, password=password, timeout=15)

    cmd = (
      'python /home/nutanix/ncc/bin/long_expiry_time_snapshot_util.py '
      '--expired_rp list_recovery_point --show_datetime'
    )
    stdin, stdout, stderr = client.exec_command(cmd)
    raw_output = stdout.read().decode('utf-8')
    client.close()

    expired_list = parse_raw_output(raw_output)

    summary = {
      "total_expired_snapshots": len(expired_list),
      "oldest_snapshot_date": expired_list[0]['created_at'] if expired_list else "N/A",
      "status": "CRITICAL" if len(expired_list) > 30 else "OK"
    }
    return summary, expired_list, True

  except Exception as e:
    summary = {"error": f"Paramiko SSH failed: {e}"}
    return summary, [], False

def run_snapshot_collection(config_path=None):
  """Reads endpoints from the config file and returns expired snapshots.

  Args:
    config_path (str, optional): Path to the configuration JSON file.
                                 Defaults to None (auto-resolves path).

  Returns:
    dict: A dictionary containing the collection results mapped by IP address.
  """
  
  if not config_path:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    config_path = os.path.join(base_dir, "static", "configurations", "endpoints.json")

  try:
    with open(config_path, 'r') as f:
      config_data = json.load(f)
  except Exception as e:
    return {"error": f"Failed to load config: {str(e)}"}

  pcs = config_data.get('pcs', [])
  results = {}

  for pc in pcs:
    ip = pc.get("ip")
    if not ip:
      continue

    # Use the global default variables requested in the PR
    user = pc.get('ssh_user', DEF_USER)
    pwd = pc.get('ssh_password', DEF_PWD)

    summary_data, expired_list, is_successful = verify_expired_snapshots(ip, user, pwd)

    results[ip] = {
      "is_successful": is_successful,
      "summary_data": summary_data,
      "snapshots_data": expired_list
    }

  return results

if __name__ == "__main__":
  final_results = run_snapshot_collection()
  print(json.dumps(final_results, indent=2))

