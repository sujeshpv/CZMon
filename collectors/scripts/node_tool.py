"""Module for Cassandra node consistency check (Ticket ENG-925555)."""

import json
import os
import re
from typing import Any, Dict

from collectors.api_processor import ApiProcessor
from common.connection.ssh_connect import Ssh

class NodetoolCheck:
  """Verifies Cassandra ring consistency against svmips.

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

  def run_check(self) -> Dict[str, Any]:
    """Runs the node check on all clusters found in config.

    Returns:
      A dictionary mapping cluster names to the consistency results.
    """
    results = {}
    pcs = self.testbed_config.get('pcs', [])

    for pc in pcs:
      ip_addr = pc.get('ip')
      cluster_name = pc.get('name', ip_addr)

      try:
        creds = self.api.get_credentials(ip_addr)
        ssh = Ssh(
          remote_ip=ip_addr,
          username=creds['user'],
          password=creds['password']
        )

        svmips_out = ssh.execute('svmips').strip()
        nodetool_out = ssh.execute('nodetool -h 0 ring')

        nodetool_ips = re.findall(r'\d+\.\d+\.\d+\.\d+', nodetool_out)
        check_pass = all(ip in nodetool_ips for ip in svmips_out.split())

        results[cluster_name] = {
          'nodetool_op': nodetool_out,
          'svmips': svmips_out,
          'svms_check': check_pass
        }
        ssh.close_session()
      except Exception as e:
        print(f'Consistency check failed for {cluster_name}: {e}')

    return results

if __name__ == '__main__':
  checker = NodetoolCheck()
  print(json.dumps(checker.run_check(), indent=2))
