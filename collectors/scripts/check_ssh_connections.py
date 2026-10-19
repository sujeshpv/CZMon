"""Counts ESTABLISHED SSH connections on CVM port 22 across a PE cluster.

Connects to each Prism Element via Paramiko using the CZMon SSH private key
(``ssh/keys/nutanix``), runs allssh with ss, and prints JSON for the CZMon
local_cli framework to persist in metrics.db.

Healthy: fewer than 10 ESTABLISHED SSH sessions per CVM.

Authentication: key-only as user ``nutanix``. PE ``ssh_user`` / ``ssh_password``
fields in endpoints.json are not used.
"""

import json
import logging
import os
import re
import sys

import paramiko

SSH_USER = "nutanix"
HEALTHY_MAX = 10
ALERT_MIN = 500

BEGIN_MARK = "__CZMON_SSH_BEGIN__"
END_MARK = "__CZMON_SSH_END__"

_BASE_DIR = os.path.dirname(
  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
SSH_KEY_PATH = os.path.join(_BASE_DIR, "ssh", "keys", "nutanix")

# bash -lic so allssh (alias/function) is available on Nutanix CVMs.
COUNT_CMD = (
  f"echo {BEGIN_MARK}; "
  'bash -lic \'allssh "ss -ant state established sport = :22 | '
  'tail -n +2 | wc -l"\'; '
  f"echo {END_MARK}"
)

logger = logging.getLogger(__name__)
HOST_BANNER_RE = re.compile(r"=+\s*([\d.]+)\s*=+")


def _extract_marked_section(output: str) -> str:
  """
  Return text between BEGIN/END markers when present.

  Args:
    output (str): Raw remote command output.

  Returns:
    str: Substring between ``BEGIN_MARK`` and ``END_MARK``, or the full
      output when markers are missing.
  """
  if BEGIN_MARK in output and END_MARK in output:
    return output.split(BEGIN_MARK, 1)[1].split(END_MARK, 1)[0]
  return output


def parse_allssh_counts(output: str) -> dict:
  """
  Parse allssh host banners into a per-CVM connection count map.

  Accepts the first integer-only line after each host banner so warning
  text between the banner and ``wc -l`` is ignored.

  Args:
    output (str): Combined allssh stdout .

  Returns:
    dict: Mapping of CVM IP address to ESTABLISHED SSH count.
  """
  section = _extract_marked_section(output)
  per_cvm = {}
  current_host = None

  for raw_line in section.splitlines():
    line = raw_line.strip()
    if not line:
      continue
    match = HOST_BANNER_RE.search(line)
    if match:
      current_host = match.group(1)
      continue
    if current_host is None:
      continue
    if line.isdigit():
      per_cvm[current_host] = int(line)
      current_host = None
  return per_cvm


def _status_for_count(max_count: int) -> str:
  """
  Map the highest per-CVM count to a status string.

  Args:
    max_count (int): Highest ESTABLISHED SSH count across CVMs.

  Returns:
    str: ``OK`` (under 10), ``WARN`` (10 or more), or ``ALERT`` (500 or more).
  """
  if max_count >= ALERT_MIN:
    return "ALERT"
  if max_count >= HEALTHY_MAX:
    return "WARN"
  return "OK"


def _connect_client(cluster_ip: str) -> paramiko.SSHClient:
  """
  Open an SSH client to a CVM using the CZMon nutanix private key.

  Args:
    cluster_ip (str): Target CVM / PE virtual IP.

  Returns:
    paramiko.SSHClient: Connected client ready for ``exec_command``.

  Raises:
    FileNotFoundError: If the private key file is missing.
    Exception: If key authentication fails.
  """
  if not os.path.isfile(SSH_KEY_PATH):
    raise FileNotFoundError(f"SSH private key not found: {SSH_KEY_PATH}")

  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(
    hostname=cluster_ip,
    username=SSH_USER,
    key_filename=SSH_KEY_PATH,
    timeout=20,
    auth_timeout=20,
    banner_timeout=20,
    allow_agent=False,
    look_for_keys=False,
  )
  logger.info("SSH key auth succeeded for %s as %s", cluster_ip, SSH_USER)
  return client


def _execute_on_cvm(cluster_ip: str, command: str) -> str:
  """
  SSH to a CVM with the nutanix key and run a command; return stdout only.

  Args:
    cluster_ip (str): Target CVM / PE virtual IP.
    command (str): Remote shell command to execute.

  Returns:
    str: Remote command stdout with surrounding whitespace stripped.
      Stderr is logged but not returned.
  """
  client = _connect_client(cluster_ip)
  try:
    _stdin, stdout, stderr = client.exec_command(command, timeout=180)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if err:
      logger.warning("Remote stderr on %s: %s", cluster_ip, err[:500])
    return out.strip()
  finally:
    client.close()


def collect_ssh_connections(cluster_ip: str) -> dict:
  """
  Run ss via allssh and return per-CVM ESTABLISHED SSH counts.

  Args:
    cluster_ip (str): Prism Element VIP used as the SSH jump target.

  Returns:
    dict: Structured result with:
      - ``highest_established`` (int): Max count on any single CVM.
      - ``total_established`` (int): Sum of counts across CVMs.
      - ``per_cvm`` (dict): Map of CVM IP to count.
      - ``status`` (str): ``OK``, ``WARN``, ``ALERT``, or ``ERROR``.
      - ``error`` / ``raw_count_output`` on failure.
  """
  try:
    raw_output = _execute_on_cvm(cluster_ip, COUNT_CMD)
    per_cvm = parse_allssh_counts(raw_output)
    counts = list(per_cvm.values())
    highest = max(counts) if counts else 0
    total = sum(counts)
    status = _status_for_count(highest) if per_cvm else "ERROR"

    if status in ("WARN", "ALERT"):
      logger.warning(
        "SSH connections elevated on %s: highest=%s total=%s status=%s",
        cluster_ip,
        highest,
        total,
        status,
      )

    result = {
      "highest_established": highest,
      "total_established": total,
      "per_cvm": per_cvm,
      "status": status,
    }
    if not per_cvm:
      result["raw_count_output"] = raw_output[:2000]
      result["error"] = "Unable to parse allssh ss counts"
    return result

  except Exception as exc:
    logger.error("Failed SSH connection check for %s: %s", cluster_ip, exc)
    return {
      "error": f"Paramiko SSH failed: {exc}",
      "highest_established": 0,
      "total_established": 0,
      "per_cvm": {},
      "status": "ERROR",
    }


def run_ssh_connection_check(config_path: str = None) -> None:
  """
  Read endpoints, collect SSH connection counts, and print JSON.

  Args:
    config_path (str, optional): Path to endpoints.json.

  Returns:
    None: Prints a JSON object keyed by PE IP to stdout for LocalProcessor.
  """
  if not config_path:
    config_path = os.path.join(
      _BASE_DIR, "static", "configurations", "endpoints.json"
    )

  try:
    with open(config_path, "r", encoding="utf-8") as handle:
      config_data = json.load(handle)
  except Exception as exc:
    logger.error("Failed to load config at %s: %s", config_path, exc)
    sys.exit(1)

  pes = list(config_data.get("pes", []))
  if not pes:
    for _key, nodes in config_data.items():
      if isinstance(nodes, list):
        for node in nodes:
          if node.get("type") == "PE" or "PE" in str(node.get("name", "")):
            pes.append(node)

  final_results = {}
  for pe in pes:
    ip = pe.get("ip") or pe.get("virtual_ip")
    if not ip:
      continue
    logger.info(
      "Checking SSH connection counts: %s (%s)...", pe.get("name", ip), ip
    )
    final_results[ip] = collect_ssh_connections(ip)

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  run_ssh_connection_check()
