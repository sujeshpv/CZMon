"""Collect CVM/PCVM file descriptor counts from /proc/sys/fs/file-nr.

"""

import json
import logging
import os
import re
import shlex
import sys

from common.connection.ssh_connect import Ssh
from library.const import NUTANIX

HEALTHY_THRESHOLD = 20000
CRITICAL_THRESHOLD = 60000
TOP_PROCESS_FD_THRESHOLD = 500
TOP_PROCESS_LIMIT = 20
HERMES_FD_LIMIT = 100
FILE_NR_COMMAND = "cat /proc/sys/fs/file-nr"
HOST_MARKER_RE = re.compile(r"=+\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s*=+")
FILE_NR_LINE_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s*$")
TOP_PROCESS_RE = re.compile(r"^TOP_PROCESS\s+(\d+)(?:\s+(.+))?$")
HERMES_MISSING_RE = re.compile(r"^HERMES\s+missing$", re.IGNORECASE)
HERMES_RE = re.compile(r"^HERMES\s+(\d+)(?:\s+(\d+))?")
FILE_DESCRIPTOR_REMOTE_SCRIPT = (
  "cat /proc/sys/fs/file-nr; "
  "{ "
  "for pid in /proc/[0-9]*; do "
  "count=$(ls -1 \"$pid/fd\" 2>/dev/null | wc -l); "
  f"if [ \"$count\" -gt {TOP_PROCESS_FD_THRESHOLD} ]; then "
  "echo $count $(ps -p ${pid##*/} -o comm= 2>/dev/null); "
  "fi; "
  "done; "
  f"}} | sort -nr | head -{TOP_PROCESS_LIMIT} | while read count name; do "
  "echo TOP_PROCESS $count $name; "
  "done; "
  "hp=; "
  "for cand in $(pgrep -x hermes 2>/dev/null); do "
  "if ls -l /proc/$cand/exe 2>/dev/null | grep -q hermes; then "
  "hp=$cand; break; "
  "fi; "
  "done; "
  "if [ -z \"$hp\" ]; then hp=$(pgrep -x hermes 2>/dev/null | head -1); fi; "
  "if [ -n \"$hp\" ]; then "
  "hc=$(ls -1 /proc/$hp/fd 2>/dev/null | wc -l); "
  "if [ \"$hc\" -eq 0 ]; then "
  "hc=$(sudo -n ls -1 /proc/$hp/fd 2>/dev/null | wc -l); "
  "fi; "
  "echo HERMES $hc $hp; "
  "else echo HERMES missing; "
  "fi"
)

logger = logging.getLogger(__name__)


def classify_fd_status(allocated: int) -> str:
  """Classify allocated file descriptor count against known risk thresholds.

  Args:
    allocated (int): Allocated file descriptor count from file-nr.

  Returns:
    str: HEALTHY (< 20k), WARNING (20k-60k), or CRITICAL (>= 60k).
  """
  if allocated >= CRITICAL_THRESHOLD:
    return "CRITICAL"
  if allocated >= HEALTHY_THRESHOLD:
    return "WARNING"
  return "HEALTHY"


def classify_hermes_status(fd_count) -> str:
  """Classify ANC Hermes FD count. Values above 100 cause flow issues.

  Args:
    fd_count: Hermes open-file count, or None if Hermes is not running.

  Returns:
    str: HEALTHY (<= 100), CRITICAL (> 100), or UNKNOWN if missing.
  """
  if fd_count is None:
    return "UNKNOWN"
  if int(fd_count) > HERMES_FD_LIMIT:
    return "CRITICAL"
  return "HEALTHY"


def _empty_fd_row(host: str) -> dict:
  """Return a per-host FD row with default process and Hermes fields."""
  return {
    "host": host,
    "allocated": None,
    "unused": None,
    "max": None,
    "status": "ERROR",
    "top_processes": [],
    "hermes": None,
  }


def parse_file_nr_output(output: str, default_host: str = "") -> list:
  """Parse file-nr, top-process, and Hermes FD lines into per-CVM rows.

  Args:
    output (str): Raw command output, optionally grouped by host markers.
    default_host (str): Host to use when no marker is present.

  Returns:
    list: Dictionaries with host, allocated, unused, max, status,
    top_processes, and hermes.
  """
  rows_by_host = {}
  host_order = []
  current_host = default_host or "unknown"
  cleaned = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", output or "")

  def row_for(host):
    if host not in rows_by_host:
      rows_by_host[host] = _empty_fd_row(host)
      host_order.append(host)
    return rows_by_host[host]

  for raw_line in cleaned.splitlines():
    line = raw_line.strip()
    if not line:
      continue
    marker = HOST_MARKER_RE.search(line)
    if marker:
      current_host = marker.group(1)
      continue
    match = FILE_NR_LINE_RE.match(line)
    if match:
      allocated = int(match.group(1))
      unused = int(match.group(2))
      maximum = int(match.group(3))
      row = row_for(current_host)
      row["allocated"] = allocated
      row["unused"] = unused
      row["max"] = maximum
      row["status"] = classify_fd_status(allocated)
      continue
    top_match = TOP_PROCESS_RE.match(line)
    if top_match:
      row = row_for(current_host)
      row["top_processes"].append({
        "fd_count": int(top_match.group(1)),
        "name": (top_match.group(2) or "unknown").strip() or "unknown",
      })
      continue
    if HERMES_MISSING_RE.match(line):
      row_for(current_host)["hermes"] = {
        "pid": None,
        "fd_count": None,
        "status": "UNKNOWN",
      }
      continue
    hermes_match = HERMES_RE.match(line)
    if hermes_match:
      fd_count = int(hermes_match.group(1))
      pid = int(hermes_match.group(2)) if hermes_match.group(2) else None
      row_for(current_host)["hermes"] = {
        "pid": pid,
        "fd_count": fd_count,
        "status": classify_hermes_status(fd_count),
      }

  rows = [rows_by_host[host] for host in host_order]
  for row in rows:
    row["top_processes"].sort(
      key=lambda item: item.get("fd_count") or 0,
      reverse=True,
    )
    row["top_processes"] = row["top_processes"][:TOP_PROCESS_LIMIT]
  return resolve_cluster_hermes(rows)


def resolve_cluster_hermes(rows: list) -> list:
  """Mark the single PCVM that runs ANC /usr/bin/hermes; others are standby.

  Flow ANC Hermes runs on one PCVM in a 3-instance PC. Sibling VMs without
  that process are expected and must not look like a failure.
  """
  if not any(isinstance(row, dict) and row.get("hermes") for row in rows):
    return rows

  active_rows = []
  for row in rows:
    if not isinstance(row, dict):
      continue
    hermes = row.get("hermes") or {}
    if hermes.get("pid") is not None and hermes.get("fd_count") is not None:
      active_rows.append(row)
  active_rows.sort(
    key=lambda row: (row.get("hermes") or {}).get("fd_count") or 0,
    reverse=True,
  )
  active_host = (active_rows[0].get("host") if active_rows else None)

  for row in rows:
    if not isinstance(row, dict):
      continue
    hermes = dict(row.get("hermes") or {
      "pid": None,
      "fd_count": None,
      "status": "UNKNOWN",
    })
    if active_host and row.get("host") == active_host:
      hermes["role"] = "active"
      hermes["status"] = classify_hermes_status(hermes.get("fd_count"))
    elif active_host:
      hermes["role"] = "standby"
      hermes["status"] = "STANDBY"
    else:
      hermes["role"] = "missing"
      hermes["status"] = "UNKNOWN"
    row["hermes"] = hermes
  return rows


def _worst_status(statuses: list) -> str:
  """Return the most severe status from a list of CVM statuses."""
  order = {"CRITICAL": 3, "WARNING": 2, "HEALTHY": 1, "ERROR": 0}
  if not statuses:
    return "ERROR"
  return max(statuses, key=lambda status: order.get(status, 0))


def build_fanout_command(command: str = FILE_NR_COMMAND) -> str:
  """Build the svmips SSH fan-out used by CliProcessor and this collector.

  Args:
    command (str): Remote command to run on each SVM. file-nr commands
      expand to FILE_DESCRIPTOR_REMOTE_SCRIPT.

  Returns:
    str: A login-shell command that SSHes to every SVM IP.
  """
  remote = str(command or "")
  if "file-nr" in remote:
    remote = FILE_DESCRIPTOR_REMOTE_SCRIPT
  fanout = (
    "for i in $(svmips); do "
    'echo "================== $i ================="; '
    f"ssh $i {shlex.quote(remote)}; "
    "done"
  )
  return "bash -lc " + shlex.quote(fanout)


def collect_cluster_file_descriptors(cluster_ip: str) -> dict:
  """SSH to a PE/PC VM with the nutanix key and collect file-nr on every SVM.

  Args:
    cluster_ip (str): Prism Element / Prism Central IP used as the SSH target.

  Returns:
    dict: Cluster-level FD counts, per-CVM rows, and health status.
  """
  cmd = build_fanout_command(FILE_NR_COMMAND)
  last_error = None
  try:
    ssh_obj = Ssh(cluster_ip, NUTANIX)
    cvm_output = ssh_obj.execute(cmd)
    rows = parse_file_nr_output(cvm_output, default_host=cluster_ip)
    if not rows:
      last_error = "No file-nr rows parsed from svmips fan-out output."
    else:
      allocated_values = [
        row["allocated"] for row in rows if row.get("allocated") is not None
      ]
      max_allocated = max(allocated_values) if allocated_values else None
      hermes_values = [
        (row.get("hermes") or {}).get("fd_count")
        for row in rows
        if (row.get("hermes") or {}).get("fd_count") is not None
      ]
      status = _worst_status(
        [row["status"] for row in rows]
        + [(row.get("hermes") or {}).get("status") for row in rows]
      )
      return {
        "status": status,
        "max_allocated": max_allocated,
        "healthy_threshold": HEALTHY_THRESHOLD,
        "critical_threshold": CRITICAL_THRESHOLD,
        "top_process_threshold": TOP_PROCESS_FD_THRESHOLD,
        "hermes_fd_limit": HERMES_FD_LIMIT,
        "max_hermes_fds": max(hermes_values) if hermes_values else None,
        "cvms": rows,
        "raw_output": (cvm_output or "").strip(),
      }
  except Exception as err:
    last_error = str(err)
    logger.error(
      "File descriptor collection failed for %s as %s: %s",
      cluster_ip,
      NUTANIX,
      err,
    )

  return {
    "error": f"Failed collecting file descriptors: {last_error}",
    "status": "ERROR",
    "max_allocated": None,
    "healthy_threshold": HEALTHY_THRESHOLD,
    "critical_threshold": CRITICAL_THRESHOLD,
    "cvms": [],
  }


def run_file_descriptor_collection(config_path: str = None) -> None:
  """Read endpoints, collect FD counts with SSH keys, and print JSON.

  Args:
    config_path (str, optional): Path to endpoints.json. Defaults to None
      (auto-resolves path).
  """
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, "r") as handle:
      config_data = json.load(handle)
  except Exception as err:
    logger.error(f"Failed to load config at {config_path}: {err}")
    sys.exit(1)

  all_endpoints = []
  if "pes" in config_data or "pcs" in config_data:
    all_endpoints.extend(config_data.get("pes", []))
    all_endpoints.extend(config_data.get("pcs", []))
  else:
    for _, entries in config_data.items():
      if isinstance(entries, list):
        all_endpoints.extend(entries)

  final_results = {}
  for endpoint in all_endpoints:
    if not isinstance(endpoint, dict):
      continue
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    cluster_name = endpoint.get("name", ip)
    if not ip:
      continue
    logger.info(f"Collecting file descriptor counts: {cluster_name} ({ip})...")
    final_results[ip] = collect_cluster_file_descriptors(ip)

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
  )
  run_file_descriptor_collection()
