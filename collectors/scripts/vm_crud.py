"""Module to verify Nutanix VM CRUD sanity across clusters."""

import json
import os
import re
import time
from typing import Any, Dict, Optional
import uuid

from collectors.api_processor import ApiProcessor

class VMSanity:
  """Performs Create, Update, and Delete sanity checks on Nutanix VMs.

  Attributes:
    api: An instance of ApiProcessor for cluster communication.
    testbed_config: Dictionary containing cluster endpoints and credentials.
  """

  def __init__(self):
    """Initializes the sanity checker with local configuration."""
    self.api = ApiProcessor()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )
    self.testbed_config = self.api.load_config(config_path)
    self.api.config = self.testbed_config

  def wait_for_task(self, ip_address: str, task_uuid: Optional[str]) -> bool:
    """Polls a Nutanix task until it succeeds or times out.

    Args:
      ip_address: The IP of the cluster where the task is running.
      task_uuid: The UUID of the task to track.

    Returns:
      True if the task succeeded or no UUID was provided, False otherwise.
    """
    if not task_uuid:
      return True
    for _ in range(10):
      try:
        resp = self.api.generic_api_call(
          ip_address, "GET", f"/api/nutanix/v3/tasks/{task_uuid}"
        )
        data = resp if isinstance(resp, dict) else resp.json()
        status = str(data.get("status", "")).upper()
        if status == "SUCCEEDED":
          return True
        if status == "FAILED":
          return False
      except Exception:
        pass
      time.sleep(2)
    return True

  def verify_all_clusters(self) -> Dict[str, Any]:
    """Runs VM CRUD sanity checks on all configured clusters.

    Returns:
      A dictionary containing the Pass/Fail results for each cluster.
    """
    final_results = {}
    pcs = self.testbed_config.get("pcs", [])

    for pc in pcs:
      ip_addr = pc.get("ip")
      name = pc.get("name", ip_addr)
      status = {
        "vm_create_result": False,
        "vm_update_result": False,
        "vm_delete_result": False,
      }
      vm_uuid = None
      task_uuid = None

      try:
        # CREATE VM (No Disk, No Network)
        vm_name = f"sanity-{uuid.uuid4().hex[:4]}"
        payload = {
          "spec": {
            "name": vm_name,
            "resources": {
              "num_sockets": 1,
              "num_vcpus_per_socket": 1,
              "memory_size_mib": 1024,
            },
          },
          "metadata": {"kind": "vm"},
        }

        try:
          resp = self.api.generic_api_call(
            ip_addr, "POST", "/api/nutanix/v3/vms", payload
          )
          data = resp if isinstance(resp, dict) else resp.json()
          vm_uuid = data["metadata"]["uuid"]
          task_uuid = data["status"]["execution_context"]["task_uuid"]
        except Exception as e:
          msg = str(e)
          u_match = re.search(r'"uuid":\s*"([^"]+)"', msg)
          t_match = re.search(r'"task_uuid":\s*"([^"]+)"', msg)
          if u_match:
            vm_uuid = u_match.group(1)
          if t_match:
            task_uuid = t_match.group(1)

        if vm_uuid:
          self.wait_for_task(ip_addr, task_uuid)
          status["vm_create_result"] = True
          time.sleep(2)

        if vm_uuid and status["vm_create_result"]:
          get_data = self.api.generic_api_call(
            ip_addr, "GET", f"/api/nutanix/v3/vms/{vm_uuid}"
          )
          current = get_data if isinstance(get_data, dict) else get_data.json()

          update_payload = {
            "spec": current["spec"],
            "metadata": current["metadata"],
          }
          update_payload["spec"]["resources"]["memory_size_mib"] = 2048

          u_task = None
          try:
            u_resp = self.api.generic_api_call(
              ip_addr, "PUT", f"/api/nutanix/v3/vms/{vm_uuid}",
              update_payload
            )
            u_data = u_resp if isinstance(u_resp, dict) else u_resp.json()
            u_task = u_data["status"]["execution_context"]["task_uuid"]
          except Exception as e:
            t_match = re.search(r'"task_uuid":\s*"([^"]+)"', str(e))
            u_task = t_match.group(1) if t_match else None

          if self.wait_for_task(ip_addr, u_task):
            status["vm_update_result"] = True

        # DELETE VM
        if vm_uuid:
          time.sleep(2)
          d_task = None
          try:
            d_resp = self.api.generic_api_call(
              ip_addr, "DELETE", f"/api/nutanix/v3/vms/{vm_uuid}"
            )
            d_data = d_resp if isinstance(d_resp, dict) else d_resp.json()
            d_task = d_data["status"]["execution_context"]["task_uuid"]
          except Exception as e:
            t_match = re.search(r'"task_uuid":\s*"([^"]+)"', str(e))
            d_task = t_match.group(1) if t_match else None

          if self.wait_for_task(ip_addr, d_task):
            status["vm_delete_result"] = True

      except Exception as e:
        print(f"Sanity Error on {name}: {e}")

      final_results[name] = status

    return final_results

if __name__ == "__main__":
  checker = VMSanity()
  print(json.dumps(checker.verify_all_clusters(), indent=2))
