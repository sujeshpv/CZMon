"""Module to verify Expired Snapshots across all PCs (Ticket ENG-924183)."""

import json
import os

from typing import Any, Dict, List

from collectors.api_processor import ApiProcessor
from common.connection.ssh_connect import Ssh

class ExpiredSnapshotsVerification:
  """Verifies system-retained recovery points via long_expiry_time_snapshot_util.

  Attributes:
    api: Instance of ApiProcessor for configuration and credentials.
    testbed_config: Dictionary containing loaded endpoints.
  """

  def __init__(self):
    """Initializes the check environment and loads cluster config."""
    self.api = ApiProcessor()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(
      base_dir, 'static', 'configurations', 'endpoints.json'
    )
    self.testbed_config = self.api.load_config(config_path)
    self.api.config = self.testbed_config

  def _parse_raw_output(self, raw_text: str) -> List[Dict[str, Any]]:
    """Parses the pipe-delimited text table into a list of dictionaries.

    Args:
      raw_text: The raw string output from the CVM command.

    Returns:
      A list of formatted snapshot objects.
    """
    parsed_rows = []
    lines = raw_text.strip().split('\n')

    if len(lines) <= 1:
      return parsed_rows

    for line in lines[1:]:
      # Split by pipe and remove extra whitespace
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

  def run_verification(self) -> Dict[str, Any]:
    """Executes the snapshot utility and returns structured JSON.

    Returns:
      A dictionary with a summary and a list of parsed expired snapshots.
    """
    results = {}
    pcs = self.testbed_config.get('pcs', [])

    for pc in pcs:
      ip_addr = pc.get('ip')
      try:
        creds = self.api.get_credentials(ip_addr)
        ssh = Ssh(
          remote_ip=ip_addr,
          username=creds['user'],
          password=creds['password']
        )

        # Execute command
        cmd = (
          'python /home/nutanix/ncc/bin/long_expiry_time_snapshot_util.py '
          '--expired_rp list_recovery_point --show_datetime'
        )
        raw_output = ssh.execute(cmd)

        expired_list = self._parse_raw_output(raw_output)

        results[ip_addr] = {
          "summary": {
            "total_expired_snapshots": len(expired_list),
            "oldest_snapshot_date": expired_list[0]['created_at'] if expired_list else "N/A",
            "status": "CRITICAL" if len(expired_list) > 30 else "OK"
          },
          "expired_snapshots": expired_list
        }

        ssh.close_session()

      except Exception as e:
        print(f'Failed snapshot verification for PC {ip_addr}: {e}')
        results[ip_addr] = {"error": str(e)}

    return results

if __name__ == '__main__':
  verifier = ExpiredSnapshotsVerification()
  print(json.dumps(verifier.run_verification(), indent=2))
