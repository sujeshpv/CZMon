"""Module to verify Expired Snapshots across all PCs and save to DB."""

import json
import paramiko

def parse_raw_output(raw_text: str) -> list:
  """Parses the pipe-delimited text table into a list of dictionaries.

  Args:
    raw_text: The raw text string returned from the SSH command.

  Returns:
    A list of dictionaries containing the parsed snapshot data.
  """
  parsed_rows = []

  # Bail out early if we got nothing back from the command
  if not raw_text:
    return parsed_rows

  lines = raw_text.strip().split('\n')

  # If it's just the header row or completely empty, return the empty list
  if len(lines) <= 1:
    return parsed_rows

  # Skip the first line since it's just the table header
  for line in lines[1:]:
    # Clean up the whitespace around the pipe delimiters
    parts = [p.strip() for p in line.split('|')]

    # Make sure we actually have all the columns before trying to map them
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
    ip: The IP address of the target cluster.
    user: SSH username (typically 'nutanix').
    password: SSH password for the cluster.

  Returns:
    A tuple containing (summary_dict, expired_list, is_successful).
  """
  try:
    client = paramiko.SSHClient()
    # Auto-accept unknown keys so the script doesn't hang waiting for user input
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=ip, username=user, password=password, timeout=15)

    # Run the NCC utility to grab the snapshot info
    cmd = (
      'python /home/nutanix/ncc/bin/long_expiry_time_snapshot_util.py '
      '--expired_rp list_recovery_point --show_datetime'
    )
    stdin, stdout, stderr = client.exec_command(cmd)
    raw_output = stdout.read().decode('utf-8')
    client.close()

    expired_list = parse_raw_output(raw_output)

    # Build a quick summary for the UI. Flag as CRITICAL if we have over 30.
    summary = {
      "total_expired_snapshots": len(expired_list),
      "oldest_snapshot_date": expired_list[0]['created_at'] if expired_list else "N/A",
      "status": "CRITICAL" if len(expired_list) > 30 else "OK"
    }

    return summary, expired_list, True

  except Exception as e:
    # Catch network timeouts or bad creds safely without breaking the runner
    summary = {"error": f"Paramiko SSH failed: {e}"}
    return summary, [], False

def run_snapshot_collection(config_path=None):
  """Reads endpoints from the config file and persists expired snapshots to the DB.

  Args:
    config_path: Optional path to endpoints.json. If None, it automatically 
                 finds it in the Django settings directory.
  """

  # Hook into the Django environment so we can use the DB models directly
  import os
  import sys
  import django

  script_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(os.path.dirname(script_dir))

  if base_dir not in sys.path:
    sys.path.append(base_dir)

  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")

  from django.apps import apps
  # Only call setup if Django isn't already running (like when triggered by the runner)
  if not apps.ready:
    django.setup()

  from django.conf import settings
  from coreapp.models import ExpiredSnapshot

  # Fallback to the default config location if a custom one wasn't passed in
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

  # We only care about Prism Central IPs for this script
  pcs = config_data.get('pcs', [])

  for pc in pcs:
    ip = pc.get("ip")
    if not ip:
      continue

    # Grab creds, falling back to the defaults if they aren't in the JSON
    user = pc.get('ssh_user', 'nutanix')
    pwd = pc.get('ssh_password', 'Pitadmin@1234')

    summary_data, expired_list, is_successful = verify_expired_snapshots(
      ip, user, pwd
    )

    # Update the existing record so we don't flood the database with duplicates
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
  # Allows testing the script standalone from the terminal
  run_snapshot_collection()

