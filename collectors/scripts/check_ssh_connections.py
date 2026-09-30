"""Counts ESTABLISHED SSH connections on CVM port 22 across a PE cluster.

Connects to each Prism Element via Paramiko, runs allssh with ss, and prints
JSON for the CZMon local_cli framework to persist in metrics.db.

Healthy: fewer than 10 ESTABLISHED SSH sessions per CVM.
Prefer ``ssh_user`` / ``ssh_password`` on the PE entry in endpoints.json.
"""

import json
import logging
import os
import re
import sys

import paramiko

DEF_USER = "nutanix"
DEF_PWD = "Nutanix.123"
HEALTHY_MAX = 10
ALERT_MIN = 500

BEGIN_MARK = "__CZMON_SSH_BEGIN__"
END_MARK = "__CZMON_SSH_END__"

# bash -lic so allssh (alias/function) is available on Nutanix CVMs.
COUNT_CMD = (
  f"echo {BEGIN_MARK}; "
  'bash -lic \'allssh "ss -ant state established sport = :22 | '
  'tail -n +2 | wc -l"\'; '
  f"echo {END_MARK}"
)

logger = logging.getLogger(__name__)
HOST_BANNER_RE = re.compile(r"=+\s*([\d.]+)\s*=+")


def _resolve_ssh_credentials(pe: dict) -> tuple:
  """Resolve SSH login: ssh_* first, then credentials, then UI user/password."""
  creds = pe.get("credentials") if isinstance(pe.get("credentials"), dict) else {}
  user = (
    pe.get("ssh_user")
    or creds.get("ssh_user")
    or creds.get("username")
    or pe.get("user")
    or creds.get("user")
    or DEF_USER
  )
  password = (
    pe.get("ssh_password")
    or creds.get("ssh_password")
    or creds.get("password")
    or pe.get("password")
    or DEF_PWD
  )
  return user, password


def _extract_marked_section(output: str) -> str:
  """Return text between BEGIN/END markers when present."""
  if BEGIN_MARK in output and END_MARK in output:
    return output.split(BEGIN_MARK, 1)[1].split(END_MARK, 1)[0]
  return output


def parse_allssh_counts(output: str) -> dict:
  """Parse allssh banners into {cvm_ip: count} (first integer line per host)."""
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
  if max_count >= ALERT_MIN:
    return "ALERT"
  if max_count >= HEALTHY_MAX:
    return "WARN"
  return "OK"


def _execute_on_cvm(
  cluster_ip: str, user: str, password: str, command: str
) -> str:
  """SSH to CVM and run command via exec_command; return stdout only."""
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.connect(
    hostname=cluster_ip,
    username=user,
    password=password,
    timeout=20,
    auth_timeout=20,
    banner_timeout=20,
    allow_agent=False,
    look_for_keys=False,
  )
  try:
    _stdin, stdout, stderr = client.exec_command(command, timeout=180)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if err:
      logger.warning("Remote stderr on %s: %s", cluster_ip, err[:500])
    return out.strip()
  finally:
    client.close()


def collect_ssh_connections(cluster_ip: str, user: str, password: str) -> dict:
  """Run ss via allssh and return per-CVM ESTABLISHED SSH counts."""
  try:
    raw_output = _execute_on_cvm(cluster_ip, user, password, COUNT_CMD)
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
  """Read endpoints, collect SSH connection counts, and print JSON."""
  if not config_path:
    base_dir = os.path.dirname(
      os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
      base_dir, "static", "configurations", "endpoints.json"
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
    user, pwd = _resolve_ssh_credentials(pe)
    logger.info(
      "Checking SSH connection counts: %s (%s)...", pe.get("name", ip), ip
    )
    final_results[ip] = collect_ssh_connections(ip, user, pwd)

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  run_ssh_connection_check()
