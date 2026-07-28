"""Module to verify Expired Snapshots across all PCs."""

import json
import os
import sys
import paramiko

# STEP 5: Define global credentials as requested
DEF_USER = 'nutanix'
DEF_PWD = 'Pitadmin@1234'

def parse_raw_output(raw_text: str) -> list:
  """Parses the pipe-delimited text table into a list of dictionaries."""
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
  """Executes the snapshot utility via SSH to grab expired recovery points."""
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
  """Reads endpoints from the config file and returns expired snapshots."""

  # STEP 6: Read config without using Django settings
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

!
    results[ip] = {
      "is_successful": is_successful,
      "summary_data": summary_data,
      "snapshots_data": expired_list
    }

  return results

if __name__ == "__main__":
  # The framework will automatically catch this printed JSON and save it to the DB!
  final_results = run_snapshot_collection()
  print(json.dumps(final_results, indent=2))

