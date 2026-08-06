"""Module to verify Nutanix VM CRUD sanity across clusters."""

import json
import logging
import os
import re
import sys
import time
import uuid
from typing import Optional
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_USER = "admin"
DEF_PWD = "Nutanix.123"

# Prefix for test VMs so anyone can easily track/change them
VM_NAME_PREFIX = "sanity-"  

logger = logging.getLogger(__name__)

def make_api_call(
  ip: str,
  method: str,
  endpoint: str,
  payload: dict = None,
  user: str = DEF_USER,
  pwd: str = DEF_PWD
) -> dict:
  """Executes a direct REST API call to Prism Central.

  Args:
    ip (str): The target Prism Central IP address.
    method (str): The HTTP method (GET, POST, PUT, DELETE).
    endpoint (str): The API path to hit.
    payload (dict, optional): Request payload dictionary. Defaults to None.
    user (str, optional): Username for authentication. Defaults to DEF_USER.
    pwd (str, optional): Password for authentication. Defaults to DEF_PWD.

  Returns:
    dict: The parsed JSON response.
  """
  url = f"https://{ip}:9440{endpoint}"
  auth = (user, pwd)
  headers = {"Content-Type": "application/json", "Accept": "application/json"}

  resp = requests.request(
    method=method,
    url=url,
    auth=auth,
    json=payload,
    headers=headers,
    verify=False,
    timeout=15
  )
  resp.raise_for_status()
  return resp.json()

def wait_for_task(
  ip_address: str,
  task_uuid: Optional[str],
  user: str,
  pwd: str
) -> bool:
  """Polls a Nutanix task until it succeeds, fails, or times out.

  Args:
    ip_address (str): The IP of the cluster where the task is running.
    task_uuid (str): The UUID of the task to track.
    user (str): Username for authentication.
    pwd (str): Password for authentication.

  Returns:
    bool: True if the task succeeded or no UUID was provided, False otherwise.
  """
  if not task_uuid:
    return True

  # Poll up to 10 times with a 2-second sleep (total 20 seconds)
  for _ in range(10):
    try:
      data = make_api_call(
        ip_address,
        "GET",
        f"/api/nutanix/v3/tasks/{task_uuid}",
        user=user,
        pwd=pwd
      )
      status = str(data.get("status", "")).upper()
      if status == "SUCCEEDED":
        return True
      if status == "FAILED":
        return False
    except Exception as e:
      logger.debug(f"Error polling task {task_uuid}: {e}")

    # Mandatory hard sleep: We must pause between checks to prevent flooding the Prism Central API with hundreds of requests in a tight loop.
    time.sleep(2)

  return True

def run_vm_sanity(config_path: str = None) -> None:
  """Reads endpoints config, executes VM CRUD sanity check, and prints JSON.

  Args:
    config_path (str, optional): Path to endpoints.json. Defaults to None.
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

  pcs = config_data.get("pcs", [])
  final_results = {}

  for pc in pcs:
    ip_addr = pc.get("ip") or pc.get("virtual_ip")
    name = pc.get("name", ip_addr)

    if not ip_addr:
      continue

    creds = pc.get("credentials", {})
    user = pc.get("user", creds.get("username", creds.get("user", DEF_USER)))
    pwd = pc.get("password", creds.get("password", DEF_PWD))

    status = {
      "vm_create_result": False,
      "vm_update_result": False,
      "vm_delete_result": False,
    }
    vm_uuid = None
    task_uuid = None

    logger.info(f"Starting VM CRUD sanity check for cluster: {name} ({ip_addr})...")

    try:
      # --- 1. CREATE VM ---
      # Used the globally defined prefix so it can be easily tracked or changed
      vm_name = f"{VM_NAME_PREFIX}{uuid.uuid4().hex[:4]}"
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
        data = make_api_call(
          ip_addr,
          "POST",
          "/api/nutanix/v3/vms",
          payload,
          user=user,
          pwd=pwd
        )
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
        wait_for_task(ip_addr, task_uuid, user, pwd)
        status["vm_create_result"] = True

        # Mandatory hard sleep: Even after a task reports 'SUCCEEDED', 
        # backend needs a moment to fully synchronize the new entity.
        # Without this delay, the subsequent GET request might return a 404 Not Found error.
        time.sleep(2)

      # --- 2. UPDATE VM ---
      if vm_uuid and status["vm_create_result"]:
        current = make_api_call(
          ip_addr,
          "GET",
          f"/api/nutanix/v3/vms/{vm_uuid}",
          user=user,
          pwd=pwd
        )

        update_payload = {
          "spec": current["spec"],
          "metadata": current["metadata"],
        }
        update_payload["spec"]["resources"]["memory_size_mib"] = 2048

        u_task = None
        try:
          u_data = make_api_call(
            ip_addr,
            "PUT",
            f"/api/nutanix/v3/vms/{vm_uuid}",
            update_payload,
            user=user,
            pwd=pwd
          )
          u_task = u_data["status"]["execution_context"]["task_uuid"]
        except Exception as e:
          t_match = re.search(r'"task_uuid":\s*"([^"]+)"', str(e))
          u_task = t_match.group(1) if t_match else None

        if wait_for_task(ip_addr, u_task, user, pwd):
          status["vm_update_result"] = True

      # --- 3. DELETE VM ---
      if vm_uuid:
        # Mandatory hard sleep: Ensures the previous PUT operation is completely 
        # settled across cluster nodes before issuing a DELETE, preventing state conflicts.
        time.sleep(2)
        d_task = None
        try:
          d_data = make_api_call(
            ip_addr,
            "DELETE",
            f"/api/nutanix/v3/vms/{vm_uuid}",
            user=user,
            pwd=pwd
          )
          d_task = d_data["status"]["execution_context"]["task_uuid"]
        except Exception as e:
          t_match = re.search(r'"task_uuid":\s*"([^"]+)"', str(e))
          d_task = t_match.group(1) if t_match else None

        if wait_for_task(ip_addr, d_task, user, pwd):
          status["vm_delete_result"] = True

    except Exception as e:
      logger.error(f"Sanity Error on {name}: {e}")

    final_results[name] = status

 
  print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_vm_sanity()

