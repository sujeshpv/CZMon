"""Module to audit clusters for process segmentation faults (SIGSEGV)."""

import json
import os
from typing import Any, Dict

from collectors.api_processor import ApiProcessor
from common.connection.ssh_connect import Ssh

class SigsegvAudit:
  """Audits cluster logs for SIGSEGV crashes by connecting to nodes directly.

  Attributes:
    api: Instance of ApiProcessor for configuration and credentials.
    testbed_config: Dictionary containing loaded cluster endpoints.
  """

  def __init__(self):
    """Initializes the audit environment and loads cluster config."""
    self.api = ApiProcessor()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(
      base_dir, 'static', 'configurations', 'endpoints.json'
    )
    self.testbed_config = self.api.load_config(config_path)
    self.api.config = self.testbed_config

  def run_audit(self) -> Dict[str, Any]:
    """Scans all nodes for SIGSEGV by connecting to each CVM directly.

    Returns:
      A dictionary mapping cluster names to crash audit results.
    """
    results = {}
    # Target the pe
    pes = self.testbed_config.get('pes', [])

    for pe in pes:
      cluster_ip = pe.get('ip')
      cluster_name = pe.get('name', cluster_ip)

      all_node_details = []
      any_crash_found = False

      try:
        creds = self.api.get_credentials(cluster_ip)
        primary_ssh = Ssh(
          remote_ip=cluster_ip,
          username=creds['user'],
          password=creds['password']
        )
        svm_ips = primary_ssh.execute("svmips").strip().split()
        primary_ssh.close_session()

        # Loop through every individual CVM IP found
        for node_ip in svm_ips:
          try:
            node_ssh = Ssh(
              remote_ip=node_ip,
              username=creds['user'],
              password=creds['password']
            )

            cmd = 'grep -r -l -I "SIGSEGV" /home/nutanix/data/logs/'
            node_output = node_ssh.execute(cmd).strip()

            if node_output:
              any_crash_found = True
              all_node_details.append(f"NODE {node_ip}:\n{node_output}")

            node_ssh.close_session()
          except Exception as node_err:
            all_node_details.append(f"NODE {node_ip} error: {node_err}")

        results[cluster_name] = {
          "crash_found": any_crash_found,
          "details": "\n".join(all_node_details) if any_crash_found else "No SIGSEGV found.",
          "status": "FAIL" if any_crash_found else "PASS"
        }

      except Exception as e:
        results[cluster_name] = {"error": str(e), "status": "ERROR"}

    return results

if __name__ == '__main__':
  auditor = SigsegvAudit()
  print(json.dumps(auditor.run_audit(), indent=2))
