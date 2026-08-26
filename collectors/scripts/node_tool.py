"""Cassandra node consistency helpers used by the CLI collector.

Compares unique SVM IPs from `svmips` with unique nodes in
`nodetool -h 0 ring`. CliProcessor stores one JSON record per SVM.
"""

import json
import logging
import os
import re
import sys
import time

import paramiko

DEF_UNAME = "admin"
DEF_PWD = "Nutanix.123"
SHELL_PROMPT_DELAY = 2
COMMAND_DELAY = 3
RING_COMMAND_DELAY = 8
LOGGER = logging.getLogger(__name__)
IPV4_RE = re.compile(r"\d+\.\d+\.\d+\.\d+")
RING_LINE_RE = re.compile(
  r"^(?P<ip>\d+\.\d+\.\d+\.\d+)\s+"
  r"(?:(?P<rack>(?!Up|Down)\S+)\s+)?"
  r"(?P<status>Up|Down)\s+"
  r"(?P<state>\S+)\s+"
  r"(?P<load>[\d.]+\s+\S+)"
)
LOAD_UNITS = {
  "b": 1 / (1024 ** 3),
  "bytes": 1 / (1024 ** 3),
  "kb": 1 / (1024 ** 2),
  "kib": 1 / (1024 ** 2),
  "mb": 1 / 1024,
  "mib": 1 / 1024,
  "gb": 1.0,
  "gib": 1.0,
  "tb": 1024.0,
  "tib": 1024.0,
}


def load_to_gib(load_text):
  """Convert a nodetool Load value to GiB for charting."""
  parts = str(load_text or "").strip().split()
  if not parts:
    return 0.0
  try:
    number = float(parts[0])
  except ValueError:
    return 0.0
  unit = parts[1].lower().rstrip("s") if len(parts) > 1 else "gib"
  return round(number * LOAD_UNITS.get(unit, 1.0), 4)


def parse_svmips(output):
  """Return unique SVM IPs from `svmips` output, preserving order."""
  seen = set()
  ips = []
  for ip in IPV4_RE.findall(output or ""):
    if ip not in seen:
      seen.add(ip)
      ips.append(ip)
  return ips


def parse_nodetool_ring(output):
  """Parse unique Cassandra nodes and their Load from `nodetool ring`."""
  nodes = {}
  for line in (output or "").splitlines():
    match = RING_LINE_RE.match(line.strip())
    if not match:
      continue
    ip = match.group("ip")
    if ip in nodes:
      continue
    nodes[ip] = {
      "status": match.group("status"),
      "state": match.group("state"),
      "usage_raw": match.group("load"),
      "usage": load_to_gib(match.group("load")),
    }
  return nodes


def _read_shell(shell, wait):
  """Read available SSH shell output after waiting briefly."""
  time.sleep(wait)
  output = ""
  while shell.recv_ready():
    output += shell.recv(8192).decode("utf-8", errors="replace")
    time.sleep(0.3)
  return output


def run_cvm_commands(cluster_ip, user, password):
  """Run svmips and nodetool on a CVM using password SSH.

  Tries a direct exec first, then the CVM login menu used by other collectors.
  """
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(
    hostname=cluster_ip,
    username=user,
    password=password,
    timeout=15,
    look_for_keys=False,
    allow_agent=False,
  )
  try:
    def exec_cmd(command):
      _, stdout, stderr = client.exec_command(command)
      output = stdout.read().decode("utf-8", errors="replace").strip()
      error_text = stderr.read().decode("utf-8", errors="replace").strip()
      return output or error_text

    try:
      svmips_out = exec_cmd("svmips")
      nodetool_out = exec_cmd("nodetool -h 0 ring")
      if parse_svmips(svmips_out) or parse_nodetool_ring(nodetool_out):
        return svmips_out, nodetool_out
    except Exception as error:
      LOGGER.info("Direct exec failed for %s: %s", cluster_ip, error)

    LOGGER.info("Using interactive CVM shell for %s", cluster_ip)
    shell = client.invoke_shell()
    banner = _read_shell(shell, SHELL_PROMPT_DELAY)
    if "Choice:" in banner:
      shell.send("3\n")
      banner = _read_shell(shell, COMMAND_DELAY)
    if "password" in banner.lower():
      shell.send(password + "\n")
      _read_shell(shell, SHELL_PROMPT_DELAY)
    shell.send("svmips\n")
    svmips_out = _read_shell(shell, COMMAND_DELAY)
    shell.send("nodetool -h 0 ring\n")
    nodetool_out = _read_shell(shell, RING_COMMAND_DELAY)
    return svmips_out, nodetool_out
  finally:
    client.close()


def build_svm_records(cluster_ip, svmips_out, nodetool_out):
  """Build per-SVM payloads from svmips and nodetool ring output."""
  expected_svms = parse_svmips(svmips_out)
  ring_nodes = parse_nodetool_ring(nodetool_out)
  seen_svms = list(ring_nodes)
  expected_count = len(expected_svms)
  seen_count = len(seen_svms)
  missing_svms = [ip for ip in expected_svms if ip not in ring_nodes]
  extra_svms = [ip for ip in seen_svms if ip not in expected_svms]
  mismatch = expected_count != seen_count or bool(missing_svms) or bool(extra_svms)

  all_svms = list(dict.fromkeys(expected_svms + seen_svms)) or [cluster_ip]
  results = {}
  for svm_ip in all_svms:
    ring = ring_nodes.get(svm_ip, {})
    results[svm_ip] = {
      "cluster_ip": cluster_ip,
      "svm_ip": svm_ip,
      "usage": ring.get("usage", 0),
      "usage_raw": ring.get("usage_raw", "0 GiB"),
      "status": ring.get("status", "Missing"),
      "state": ring.get("state", "Unknown"),
      "expected_nodes": expected_count,
      "seen_nodes": seen_count,
      "mismatch": mismatch,
      "missing_svms": missing_svms,
      "extra_svms": extra_svms,
      "svmips": expected_svms,
      "nodetool_svms": seen_svms,
    }
  return results


def check_cluster_nodes(cluster_ip, user, password):
  """Compare svmips with nodetool ring and return per-SVM records.

  Args:
    cluster_ip (str): PE virtual IP.
    user (str): Endpoint username.
    password (str): Endpoint password.

  Returns:
    dict: Mapping of SVM IP to status payload.
  """
  try:
    svmips_out, nodetool_out = run_cvm_commands(cluster_ip, user, password)
  except Exception as error:
    LOGGER.error("Node tool check failed for %s: %s", cluster_ip, error)
    return {
      cluster_ip: {
        "cluster_ip": cluster_ip,
        "error": str(error),
        "mismatch": False,
        "expected_nodes": 0,
        "seen_nodes": 0,
        "usage": 0,
      }
    }
  return build_svm_records(cluster_ip, svmips_out, nodetool_out)


def _is_pe_endpoint(endpoint):
  """Return True when an endpoint is a Prism Element cluster."""
  if not isinstance(endpoint, dict):
    return False
  endpoint_type = str(endpoint.get("type") or "").strip().upper()
  return endpoint_type == "PE" or not endpoint_type


def load_endpoints(config_data):
  """Return PE endpoints only. Node tool is not applicable to PC."""
  if "pes" in config_data or "pcs" in config_data:
    return [
      endpoint
      for endpoint in config_data.get("pes", [])
      if _is_pe_endpoint(endpoint)
    ]

  endpoints = []
  for entries in config_data.values():
    if not isinstance(entries, list):
      continue
    for entry in entries:
      if str(entry.get("type") or "").upper() == "PE":
        endpoints.append(entry)
  return endpoints


def run_node_tool_collection(config_path=None):
  """Read endpoints, run the check, and print per-SVM JSON."""
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, "r", encoding="utf-8") as config_file:
      config_data = json.load(config_file)
  except Exception as error:
    LOGGER.error("Failed to load config at %s: %s", config_path, error)
    sys.exit(1)

  final_results = {}
  for endpoint in load_endpoints(config_data):
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})
    user = (
      endpoint.get("user")
      or creds.get("username")
      or creds.get("user")
      or DEF_UNAME
    )
    password = endpoint.get("password") or creds.get("password") or DEF_PWD
    if not ip:
      continue
    LOGGER.info("Checking Cassandra ring for cluster: %s", ip)
    final_results.update(check_cluster_nodes(ip, user, password))

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  run_node_tool_collection()
