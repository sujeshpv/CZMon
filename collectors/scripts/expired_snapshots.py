"""Module to verify Expired Snapshots across all PCs and save to DB."""

import json
import paramiko

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
  """Executes the snapshot utility via SSH."""
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
  """Reads endpoints from config and persists expired snapshots to DB."""

  # 1. SETUP DJANGO ENVIRONMENT
  import os
  import sys
  import django

  script_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(os.path.dirname(script_dir))
  if base_dir not in sys.path:
    sys.path.append(base_dir)

  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")

  from django.apps import apps
  if not apps.ready:
    django.setup()

  # 2. IMPORT MODELS
  from django.conf import settings
  from coreapp.models import ExpiredSnapshot

  if not config_path:
    config_path = os.path.join(
      settings.BASE_DIR, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, 'r') as f:
      config_data = json.load(f)
  except Exception as e:
    print(f"Failed to load config: {str(e)}")
    return

  pcs = config_data.get('pcs', [])

  for pc in pcs:
    ip = pc.get("ip")
    if not ip:
      continue

    user = pc.get('ssh_user', 'nutanix')
    pwd = pc.get('ssh_password', 'Pitadmin@1234')

    summary_data, expired_list, is_successful = verify_expired_snapshots(
      ip, user, pwd
    )

    obj, created = ExpiredSnapshot.objects.update_or_create(
      ip_address=ip,
      defaults={
        "is_successful": is_successful,
        "summary_data": summary_data,
        "snapshots_data": expired_list,
      }
    )

    action = "Created" if created else "Updated"
    status_text = "Success" if is_successful else "Failed"
    print(f"{action} DB record for {ip} -> {status_text}")

if __name__ == "__main__":
  run_snapshot_collection()

