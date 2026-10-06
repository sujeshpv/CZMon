"""
Checks Nutanix Guest Tools (NGT) status for VMs on each Prism Element.

This script pages through the Prism v3 VM list API so large clusters
(3000+ VMs) are collected without a per-VM GET. Timeouts are retried.
It is designed to run independently and print JSON for the framework.
"""

import json
import logging
import os
import sys
import time

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Configurations ---
DEF_UNAME = "admin"
DEF_PWD = "CZNutanix.1234"
VM_LIST_PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)
MAX_RETRIES = 3
RETRY_BACKOFF_SECS = 2

logger = logging.getLogger(__name__)


def _build_session(username: str, password: str) -> requests.Session:
  """Create a session with auth, headers, and transport-level retries."""
  session = requests.Session()
  session.auth = (username, password)
  session.headers.update(
    {"Content-Type": "application/json", "Accept": "application/json"}
  )
  retry = Retry(
    total=MAX_RETRIES,
    connect=MAX_RETRIES,
    read=MAX_RETRIES,
    backoff_factor=1.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
  )
  adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
  session.mount("https://", adapter)
  session.mount("http://", adapter)
  return session


def _request_json(
  session: requests.Session,
  method: str,
  url: str,
  **kwargs,
) -> dict:
  """Send an HTTP request with timeout retries and return parsed JSON."""
  last_error = None
  for attempt in range(1, MAX_RETRIES + 1):
    try:
      resp = session.request(
        method, url, verify=False, timeout=REQUEST_TIMEOUT, **kwargs
      )
      resp.raise_for_status()
      return resp.json()
    except (
      requests.exceptions.Timeout,
      requests.exceptions.ConnectionError,
    ) as exc:
      last_error = exc
      logger.warning(
        "Timeout/connection on attempt %s/%s for %s: %s",
        attempt,
        MAX_RETRIES,
        url,
        exc,
      )
      if attempt < MAX_RETRIES:
        time.sleep(RETRY_BACKOFF_SECS * attempt)
    except requests.exceptions.HTTPError as exc:
      status = exc.response.status_code if exc.response is not None else None
      # 401/403 and other client errors will not succeed on retry.
      if status and 400 <= status < 500 and status != 429:
        raise
      last_error = exc
      logger.warning(
        "Request failed on attempt %s/%s for %s: %s",
        attempt,
        MAX_RETRIES,
        url,
        exc,
      )
      if attempt < MAX_RETRIES:
        time.sleep(RETRY_BACKOFF_SECS * attempt)
    except requests.exceptions.RequestException as exc:
      last_error = exc
      logger.warning(
        "Request failed on attempt %s/%s for %s: %s",
        attempt,
        MAX_RETRIES,
        url,
        exc,
      )
      if attempt < MAX_RETRIES:
        time.sleep(RETRY_BACKOFF_SECS * attempt)

  raise last_error or requests.exceptions.RequestException(
    f"Request failed after {MAX_RETRIES} attempts: {url}"
  )


def _extract_ngt(vm_payload: dict) -> dict:
  """Pull NGT fields from a v3 VM list/GET entity."""
  status = vm_payload.get("status") or {}
  spec = vm_payload.get("spec") or {}
  metadata = vm_payload.get("metadata") or {}
  status_resources = status.get("resources") or {}
  spec_resources = spec.get("resources") or {}

  guest_tools = (
    (status_resources.get("guest_tools") or {}).get("nutanix_guest_tools")
    or (spec_resources.get("guest_tools") or {}).get("nutanix_guest_tools")
    or {}
  )

  ngt_state = (guest_tools.get("ngt_state") or "").upper()
  enabled_state = (guest_tools.get("state") or "").upper()
  is_reachable = guest_tools.get("is_reachable")
  if is_reachable is None:
    is_reachable = guest_tools.get("is_reachability_updated")

  return {
    "name": status.get("name") or spec.get("name") or "Unknown",
    "uuid": metadata.get("uuid") or "",
    "power_state": (status_resources.get("power_state") or "").upper(),
    "ngt_state": ngt_state or "NOT_INSTALLED",
    "ngt_enabled_state": enabled_state or "NOT_CONFIGURED",
    "version": guest_tools.get("version") or "",
    "available_version": guest_tools.get("available_version") or "",
    "iso_mount_state": guest_tools.get("iso_mount_state") or "",
    "is_reachable": bool(is_reachable) if is_reachable is not None else False,
    "guest_os_version": guest_tools.get("guest_os_version") or "",
    "enabled_capability_list": guest_tools.get("enabled_capability_list") or [],
  }


def _list_vm_entities(session: requests.Session, cluster_ip: str) -> tuple:
  """Page through all VMs on the cluster and return (entities, incomplete).

  Uses v3 /vms/list with offset pagination. Falls back to paginated v2 /vms
  if the v3 list API is unavailable. Does not GET each VM individually.
  """
  base_v3 = f"https://{cluster_ip}:9440/api/nutanix/v3"
  entities = []
  offset = 0
  incomplete = False

  try:
    while True:
      payload = {"kind": "vm", "offset": offset, "length": VM_LIST_PAGE_SIZE}
      try:
        data = _request_json(
          session, "POST", f"{base_v3}/vms/list", json=payload
        )
      except requests.exceptions.RequestException as exc:
        if not entities:
          logger.warning(
            "v3 VM list failed for %s (%s); falling back to v2 /vms",
            cluster_ip,
            exc,
          )
          return _list_vm_entities_v2(session, cluster_ip)
        logger.error(
          "v3 VM list failed for %s at offset %s after %s VM(s): %s",
          cluster_ip,
          offset,
          len(entities),
          exc,
        )
        incomplete = True
        break

      page = data.get("entities") or []
      entities.extend(page)
      metadata = data.get("metadata") or {}
      total_matches = metadata.get("total_matches")
      logger.info(
        "Listed %s/%s VMs from %s (offset %s)",
        len(entities),
        total_matches if total_matches is not None else "?",
        cluster_ip,
        offset,
      )

      if not page:
        break
      if total_matches is not None and len(entities) >= int(total_matches):
        break
      if len(page) < VM_LIST_PAGE_SIZE:
        break
      offset += VM_LIST_PAGE_SIZE

    if entities:
      return entities, incomplete
  except requests.exceptions.RequestException as exc:
    logger.warning(
      "v3 VM list failed for %s (%s); falling back to v2 /vms",
      cluster_ip,
      exc,
    )

  return _list_vm_entities_v2(session, cluster_ip)


def _list_vm_entities_v2(session: requests.Session, cluster_ip: str) -> tuple:
  """Page through v2 /vms and return (entities, incomplete)."""
  base_v2 = f"https://{cluster_ip}:9440/api/nutanix/v2.0/vms"
  entities = []
  page = 1
  incomplete = False

  while True:
    try:
      data = _request_json(
        session,
        "GET",
        f"{base_v2}/?count={VM_LIST_PAGE_SIZE}&page={page}",
      )
    except requests.exceptions.RequestException as exc:
      logger.error(
        "Timed out or failed listing v2 VMs on %s at page %s: %s",
        cluster_ip,
        page,
        exc,
      )
      incomplete = True
      break

    page_entities = data.get("entities") or []
    # Wrap v2 entities so _extract_ngt can still read uuid/name/power.
    for vm in page_entities:
      entities.append(
        {
          "metadata": {"uuid": vm.get("uuid") or ""},
          "status": {
            "name": vm.get("name") or "Unknown",
            "resources": {
              "power_state": vm.get("power_state") or "",
              "guest_tools": {
                "nutanix_guest_tools": {
                  "ngt_state": (
                    "INSTALLED"
                    if str(vm.get("guest_os") or "").strip()
                    else ""
                  ),
                  "state": "",
                  "is_reachable": False,
                  "version": "",
                }
              },
            },
          },
          "spec": {"name": vm.get("name") or "Unknown"},
        }
      )

    metadata = data.get("metadata") or {}
    total = (
      metadata.get("grand_total_entities")
      or metadata.get("total_entities")
    )
    logger.info(
      "Listed %s/%s VMs from %s via v2 (page %s)",
      len(entities),
      total if total is not None else "?",
      cluster_ip,
      page,
    )

    if not page_entities:
      break
    if total is not None and len(entities) >= int(total):
      break
    if len(page_entities) < VM_LIST_PAGE_SIZE:
      break
    page += 1

  return entities, incomplete


def fetch_ngt_status(
  cluster_ip: str,
  username: str,
  password: str,
) -> dict:
  """Connects to Prism and returns NGT status for every VM.

  Args:
    cluster_ip (str): The Prism Element Cluster IP or FQDN.
    username (str): The Prism Element Username for authentication.
    password (str): The Prism Element Password for authentication.

  Returns:
    dict: Cluster name, NGT counts, and per-VM guest-tools details.
  """
  base_v2 = f"https://{cluster_ip}:9440/api/nutanix/v2.0"

  try:
    session = _build_session(username, password)

    try:
      cluster_data = _request_json(session, "GET", f"{base_v2}/cluster")
      cluster_name = cluster_data.get("name", "Unknown Cluster")
    except requests.exceptions.RequestException as exc:
      logger.error("Failed to read cluster name for %s: %s", cluster_ip, exc)
      cluster_name = "Unknown Cluster"

    entities, incomplete = _list_vm_entities(session, cluster_ip)
    vms = []
    ngt_installed = 0
    ngt_not_installed = 0
    ngt_enabled = 0
    ngt_disabled = 0
    ngt_reachable = 0

    for entity in entities:
      vm_info = _extract_ngt(entity)
      if not vm_info.get("uuid"):
        continue

      if vm_info["ngt_state"] == "INSTALLED":
        ngt_installed += 1
      else:
        ngt_not_installed += 1

      if vm_info["ngt_enabled_state"] == "ENABLED":
        ngt_enabled += 1
      elif vm_info["ngt_enabled_state"] == "DISABLED":
        ngt_disabled += 1

      if vm_info["is_reachable"]:
        ngt_reachable += 1

      vms.append(vm_info)

    result = {
      "Cluster_name": cluster_name,
      "total_vms": len(vms),
      "ngt_installed": ngt_installed,
      "ngt_not_installed": ngt_not_installed,
      "ngt_enabled": ngt_enabled,
      "ngt_disabled": ngt_disabled,
      "ngt_reachable": ngt_reachable,
      "vms": vms,
    }
    if incomplete:
      result["fetch_incomplete"] = True
      result["warning"] = (
        f"Timed out while listing VMs on {cluster_ip}; "
        f"returning {len(vms)} VM(s) collected so far."
      )
      logger.warning(result["warning"])
    return result

  except requests.exceptions.RequestException as e:
    logger.error("API Request Failed for %s: %s", cluster_ip, e)
    return {"error": f"API Request Failed: {str(e)}"}


def collect_all_ngt_status(config_path: str = None) -> None:
  """Reads endpoints, collects NGT status for all PE clusters, and prints JSON.

  Args:
    config_path (str, optional): Path to the endpoints JSON config file.
                                 Defaults to None (auto-resolves path).
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
    logger.error("Failed to load config at %s: %s", config_path, e)
    sys.exit(1)

  # Handle BOTH endpoints.json formats automatically
  pe_endpoints = []
  if "pes" in config_data:
    pe_endpoints = config_data.get("pes", [])
  else:
    for zone, entries in config_data.items():
      if isinstance(entries, list):
        for entry in entries:
          if entry.get("type", "").upper() == "PE":
            pe_endpoints.append(entry)

  final_results = {}

  for endpoint in pe_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})

    # Use nested credentials, then top-level fields, then defaults
    user = (
      creds.get("username")
      or creds.get("user")
      or endpoint.get("user")
      or DEF_UNAME
    )
    pwd = creds.get("password") or endpoint.get("password") or DEF_PWD

    if not ip:
      continue

    logger.info("Checking NGT status for cluster: %s...", ip)
    final_results[ip] = fetch_ngt_status(ip, user, pwd)

  # Print pure JSON to stdout so local_processor.py can parse it
  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  # Python's logging module writes to stderr by default.
  # This keeps our logs separate from the printed JSON on stdout.
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )

  collect_all_ngt_status()
