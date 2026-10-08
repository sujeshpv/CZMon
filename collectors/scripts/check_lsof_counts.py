"""
Collects `sudo lsof -u nutanix | wc -l` from every CVM in each PE cluster.

The chart value is the maximum count observed on any CVM in the cluster.
Per-CVM counts are retained in the result details for investigation.
The script prints JSON for the CZMon local collector framework.
"""

import json
import logging
import os
import re
import subprocess
import sys
from typing import Dict, List, Optional, Tuple


ALLSSH_COMMAND = "sudo lsof -u nutanix | wc -l"
ALLSSH_TIMEOUT_SECS = int(os.environ.get("CZMON_ALLSSH_TIMEOUT_SECS", "120"))
HOST_HEADER_RE = re.compile(r"={5,}\s*(.*?)\s*={5,}")
COUNT_RE = re.compile(r"^\s*(\d+)\s*$")

logger = logging.getLogger(__name__)


def _load_pe_endpoints(config_path: str) -> List[dict]:
  """Load PE endpoints from either supported endpoints.json format."""
  with open(config_path, "r", encoding="utf-8") as config_file:
    config_data = json.load(config_file)

  if isinstance(config_data, dict) and isinstance(config_data.get("pes"), list):
    return config_data["pes"]

  endpoints = []
  if isinstance(config_data, dict):
    for entries in config_data.values():
      if not isinstance(entries, list):
        continue
      endpoints.extend(
        entry for entry in entries
        if isinstance(entry, dict) and entry.get("type", "").upper() == "PE"
      )
  return endpoints


def _parse_allssh_output(output: str, fallback_host: str) -> List[dict]:
  """Parse allssh host sections and their single numeric wc output."""
  results: List[dict] = []
  current_host: Optional[str] = None

  for line in output.splitlines():
    header = HOST_HEADER_RE.search(line)
    if header:
      current_host = header.group(1).strip() or fallback_host
      continue

    count_match = COUNT_RE.match(line)
    if not count_match:
      continue

    host = current_host or fallback_host
    results.append(
      {
        "host": host,
        "lsof_count": int(count_match.group(1)),
        "status": "SUCCESS",
      }
    )
    current_host = None

  # A host header without a numeric result means the command failed or lsof
  # was unavailable on that CVM. Preserve the host in the details.
  if current_host:
    results.append(
      {
        "host": current_host,
        "lsof_count": None,
        "status": "ERROR",
        "error": "No numeric lsof count returned",
      }
    )

  if not results:
    results.append(
      {
        "host": fallback_host,
        "lsof_count": None,
        "status": "ERROR",
        "error": "No allssh host output or numeric lsof count returned",
      }
    )

  # Avoid duplicate records if allssh repeats a host header in its output.
  unique: Dict[str, dict] = {}
  for result in results:
    unique[result["host"]] = result
  return list(unique.values())


def _collect_cluster(endpoint: dict) -> dict:
  """Run the lsof count across one PE cluster's CVMs."""
  cluster_ip = endpoint.get("ip") or endpoint.get("virtual_ip") or "Unknown"
  cluster_name = (
    endpoint.get("name")
    or endpoint.get("cluster_name")
    or cluster_ip
  )

  try:
    completed = subprocess.run(
      ["allssh", ALLSSH_COMMAND],
      capture_output=True,
      text=True,
      timeout=ALLSSH_TIMEOUT_SECS,
      check=False,
    )
  except FileNotFoundError:
    return {
      "cluster_name": cluster_name,
      "lsof_count": None,
      "host_count": 0,
      "successful_hosts": 0,
      "failed_hosts": 0,
      "status": "ERROR",
      "command": ALLSSH_COMMAND,
      "host_results": [],
      "error": "allssh command was not found",
      "summary": f"Unable to collect lsof counts for {cluster_name}: allssh not found",
    }
  except subprocess.TimeoutExpired:
    return {
      "cluster_name": cluster_name,
      "lsof_count": None,
      "host_count": 0,
      "successful_hosts": 0,
      "failed_hosts": 0,
      "status": "ERROR",
      "command": ALLSSH_COMMAND,
      "host_results": [],
      "error": f"allssh timed out after {ALLSSH_TIMEOUT_SECS} seconds",
      "summary": f"Unable to collect lsof counts for {cluster_name}: allssh timed out",
    }

  host_results = _parse_allssh_output(completed.stdout, cluster_ip)
  if completed.returncode != 0:
    error_text = (completed.stderr or completed.stdout).strip()
    for result in host_results:
      if result["status"] == "SUCCESS":
        result["status"] = "ERROR"
        result["error"] = error_text[-500:] or f"allssh exited with {completed.returncode}"

  successful = [
    result for result in host_results
    if result["status"] == "SUCCESS" and result["lsof_count"] is not None
  ]
  failed = [result for result in host_results if result["status"] != "SUCCESS"]
  maximum = max((result["lsof_count"] for result in successful), default=None)
  status = "SUCCESS" if successful and not failed else "ERROR"

  summary = (
    f"Collected lsof counts from {len(successful)} CVM(s); "
    f"maximum count: {maximum if maximum is not None else 'N/A'}"
  )
  if failed:
    summary += f"; failed CVM(s): {len(failed)}"

  return {
    "cluster_name": cluster_name,
    "lsof_count": maximum,
    "host_count": len(host_results),
    "successful_hosts": len(successful),
    "failed_hosts": len(failed),
    "status": status,
    "command": ALLSSH_COMMAND,
    "host_results": host_results,
    "summary": summary,
  }


def collect_all_lsof_counts(config_path: Optional[str] = None) -> None:
  """Collect lsof counts for every PE endpoint and print framework JSON."""
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    endpoints = _load_pe_endpoints(config_path)
  except (OSError, ValueError) as exc:
    logger.error("Failed to load PE endpoints from %s: %s", config_path, exc)
    print(json.dumps({"error": f"Failed to load endpoints: {exc}"}, indent=2))
    return

  results = {}
  for endpoint in endpoints:
    cluster_ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    if not cluster_ip:
      continue
    logger.info("Collecting lsof counts for PE cluster %s", cluster_ip)
    results[cluster_ip] = _collect_cluster(endpoint)

  print(json.dumps(results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  collect_all_lsof_counts()
