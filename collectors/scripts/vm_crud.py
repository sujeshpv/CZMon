"""Module to verify Nutanix VM CRUD sanity across clusters."""

import json
import logging
import os
import sys
import time
import uuid
from typing import Optional, Tuple
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_USER = "admin"
DEF_PWD = "Nutanix.123"

# Prefix for test VMs so anyone can easily track/change them
VM_NAME_PREFIX = "sanity-"

# Interval in seconds between API polling requests
POLL_INTERVAL_SECS = 2

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
  pwd: str,
  timeout_secs: int = 20
) -> bool:
  """Polls a Nutanix task until it succeeds, fails, or times out.

  Args:
    ip_address (str): The IP of the cluster where the task is running.
    task_uuid (str): The UUID of the task to track.
    user (str): Username for authentication.
    pwd (str): Password for authentication.
    timeout_secs (int, optional): Total time to wait. Defaults to 20.

  Returns:
    bool: True if task succeeded or no UUID was provided, False otherwise.
  """
  if not task_uuid:
    return True

  iterations = max(1, int(timeout_secs / POLL_INTERVAL_SECS))

  for _ in range(iterations):
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

    # Polling interval
    time.sleep(POLL_INTERVAL_SECS)

  logger.error(f"Task {task_uuid} timed out after {timeout_secs} seconds.")
  return False

def wait_for_entity(
  ip: str,
  vm_uuid: str,
  user: str,
  pwd: str,
  expected_state: str = "AVAILABLE",
  timeout_secs: int = 20
) -> bool:
  """Polls the VM API endpoint until it reaches the expected state.

  Args:
    ip (str): Cluster IP address.
    vm_uuid (str): The UUID of the VM to track.
    user (str): Username for authentication.
    pwd (str): Password for authentication.
    expected_state (str): 'AVAILABLE' for 200 OK, 'DELETED' for 404 Not Found.
    timeout_secs (int): Total seconds to wait. Defaults to 20.

  Returns:
    bool: True if the expected state is reached, False otherwise.
  """
  iterations = max(1, int(timeout_secs / POLL_INTERVAL_SECS))

  for _ in range(iterations):
    try:
      make_api_call(
        ip, "GET", f"/api/nutanix/v3/vms/{vm_uuid}", user=user, pwd=pwd
      )
      # If no exception is raised, the entity is available (200 OK)
      if expected_state == "AVAILABLE":
        return True
    except requests.exceptions.HTTPError as e:
      # If we get a 404, the entity is not found or has been fully deleted
      if expected_state == "DELETED" and e.response.status_code == 404:
        return True
      else:
        # Log unexpected HTTP errors 
        logger.error(f"HTTPError checking entity {vm_uuid}: {e}")
    except Exception as e:
      # Log any other unexpected exception
      logger.error(f"Error checking entity {vm_uuid}: {e}")

    # Polling interval
    time.sleep(POLL_INTERVAL_SECS)

  logger.error(f"Entity {vm_uuid} state check timed out.")
  return False

def create_vm(ip: str, user: str, pwd: str) -> Tuple[Optional[str], bool]:
  """Creates a test VM and waits for it to become available in the API.

  Args:
    ip (str): Cluster IP address.
    user (str): API username.
    pwd (str): API password.

  Returns:
    tuple: (vm_uuid (str or None), success (bool)).
  """
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
    data = make_api_call(ip, "POST", "/api/nutanix/v3/vms", payload, user, pwd)
    vm_uuid = data.get("metadata", {}).get("uuid")
    task_uuid = data.get("status", {}).get("execution_context", {}).get("task_uuid")
  except Exception as e:
    logger.error(f"API call failed during VM creation on {ip}: {e}")
    return None, False

  if vm_uuid and task_uuid:
    task_ok = wait_for_task(ip, task_uuid, user, pwd)
    if task_ok:
      # Dynamically wait for the 404 to clear
      is_avail = wait_for_entity(ip, vm_uuid, user, pwd, "AVAILABLE")
      return vm_uuid, is_avail

  return vm_uuid, False

def update_vm(ip: str, vm_uuid: str, user: str, pwd: str) -> bool:
  """Updates the memory of the test VM and waits for completion.

  Args:
    ip (str): Cluster IP address.
    vm_uuid (str): The UUID of the VM to update.
    user (str): API username.
    pwd (str): API password.

  Returns:
    bool: True if the update succeeded and settled, False otherwise.
  """
  try:
    current = make_api_call(
      ip, "GET", f"/api/nutanix/v3/vms/{vm_uuid}", user=user, pwd=pwd
    )

    update_payload = {
      "spec": current["spec"],
      "metadata": current["metadata"],
    }
    update_payload["spec"]["resources"]["memory_size_mib"] = 2048

    u_data = make_api_call(
      ip, "PUT", f"/api/nutanix/v3/vms/{vm_uuid}", update_payload, user, pwd
    )
    u_task = u_data.get("status", {}).get("execution_context", {}).get("task_uuid")

    if wait_for_task(ip, u_task, user, pwd):
      # Wait for the API to confirm the entity is settled
      return wait_for_entity(ip, vm_uuid, user, pwd, "AVAILABLE")
    return False

  except Exception as e:
    logger.error(f"API call failed during VM update on {ip}: {e}")
    return False

def delete_vm(ip: str, vm_uuid: str, user: str, pwd: str) -> bool:
  """Deletes the test VM and polls until the API returns 404 Not Found.

  Args:
    ip (str): Cluster IP address.
    vm_uuid (str): The UUID of the VM to delete.
    user (str): API username.
    pwd (str): API password.

  Returns:
    bool: True if deletion succeeded, False otherwise.
  """
  try:
    d_data = make_api_call(
      ip, "DELETE", f"/api/nutanix/v3/vms/{vm_uuid}", user=user, pwd=pwd
    )
    d_task = d_data.get("status", {}).get("execution_context", {}).get("task_uuid")

    if wait_for_task(ip, d_task, user, pwd):
      # Poll until the API throws a 404 Not Found
      return wait_for_entity(ip, vm_uuid, user, pwd, "DELETED")
    return False

  except Exception as e:
    logger.error(f"API call failed during VM deletion on {ip}: {e}")
    return False

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

    logger.info(
      f"Starting VM CRUD sanity check for cluster: {name} ({ip_addr})..."
    )

    try:
      vm_uuid, create_ok = create_vm(ip_addr, user, pwd)
      status["vm_create_result"] = create_ok

      if vm_uuid and create_ok:
        status["vm_update_result"] = update_vm(ip_addr, vm_uuid, user, pwd)

      if vm_uuid:
        status["vm_delete_result"] = delete_vm(ip_addr, vm_uuid, user, pwd)

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

