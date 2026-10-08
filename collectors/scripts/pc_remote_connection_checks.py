"""Checks Prism Central PC-PC remote connection health via nuclei.

Connects to each Prism Central via Paramiko using the CZMon SSH private key
(``ssh/keys/nutanix``), runs ``nuclei remote_connection.list_all``, then
``nuclei remote_connection.health_check <uuid>`` for every listed connection.
Prints JSON for the CZMon local_cli framework to persist in metrics.db.

Authentication: key-only as user ``nutanix``. PC ``user`` / ``password``
fields in endpoints.json are not used.
"""

import json
import logging
import os
import re
import shlex
import sys

import paramiko

SSH_USER = "nutanix"

BEGIN_MARK = "__CZMON_RC_BEGIN__"
END_MARK = "__CZMON_RC_END__"

LIST_CMD = "nuclei remote_connection.list_all"
HEALTH_CMD = "nuclei remote_connection.health_check {uuid}"

_BASE_DIR = os.path.dirname(
  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
SSH_KEY_PATH = os.path.join(_BASE_DIR, "ssh", "keys", "nutanix")

logger = logging.getLogger(__name__)

UUID_RE = re.compile(
  r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
  r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
ROW_RE = re.compile(
  r"^(?P<name>.+?)\s+(?P<uuid>" + UUID_RE.pattern + r")\s*$"
)
# Success is only this nuclei sentence, plus the completion line.
# Version JSON spacing varies, and some lines end with a literal \n
# before the closing quote.
HEALTH_OK_RE = re.compile(
  r"^Health check returned OK for given RC which has uuid:"
  r"(?P<uuid>" + UUID_RE.pattern + r")"
  r"\s+for\s+\{.*\}(?:\\n)?\s*$"
)
HEALTH_COMPLETE = "Health check complete"


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


def _wrapped_command(command: str) -> str:
  """
  Wrap a nuclei command so the login-shell PATH and motd stay out of the parse.

  The ``nutanix`` user on a PC can run nuclei without extra credentials.
  A login shell keeps ``nuclei`` on PATH, and the begin/end markers keep
  motd out of the parsed output.

  Args:
    command (str): Remote nuclei command to run inside bash -lc.

  Returns:
    str: Shell command safe to pass to ``exec_command``.
  """
  inner = f"echo {BEGIN_MARK}; {command}; echo {END_MARK}"
  return f"bash -lc {shlex.quote(inner)}"


def parse_remote_connections(output: str) -> list:
  """
  Parse ``nuclei remote_connection.list_all`` into name/UUID pairs.

  Skips the informational banner and the Name/UUID header. A row is any
  line whose last token is a UUID.

  Args:
    output (str): Raw list_all stdout.

  Returns:
    list: Dicts with ``name`` and ``uuid`` keys, in output order.
  """
  section = _extract_marked_section(output)
  connections = []
  seen = set()
  for raw_line in section.splitlines():
    line = raw_line.strip().strip('"')
    if not line or line.lower().startswith("name"):
      continue
    match = ROW_RE.match(line)
    if not match:
      continue
    uuid = match.group("uuid")
    if uuid in seen:
      continue
    seen.add(uuid)
    connections.append({
      "name": match.group("name").strip(),
      "uuid": uuid,
    })
  return connections


def _health_status(output: str, uuid: str) -> str:
  """
  Classify health-check text as OK or ERROR.

  A check passes only when nuclei prints both of these lines:

  ``Health check returned OK for given RC which has uuid:<uuid> for {...}``
  ``Health check complete``

  The UUID in the OK line must be the connection that was checked. Compact
  version JSON and a trailing literal ``\\n`` still count as success. Any
  other text, including an empty result, is a failure.

  Args:
    output (str): Raw ``remote_connection.health_check`` stdout.
    uuid (str): Remote-connection UUID passed to the health check.

  Returns:
    str: ``OK`` when the success message matches, otherwise ``ERROR``.
  """
  ok_for_uuid = False
  complete = False
  for raw_line in (output or "").splitlines():
    line = raw_line.strip().strip('"')
    match = HEALTH_OK_RE.match(line)
    if match and match.group("uuid").lower() == uuid.lower():
      ok_for_uuid = True
    if line == HEALTH_COMPLETE:
      complete = True
  if ok_for_uuid and complete:
    return "OK"
  return "ERROR"


def _connect_client(pc_ip: str) -> paramiko.SSHClient:
  """
  Open an SSH client to a PC using the CZMon nutanix private key.

  Args:
    pc_ip (str): Target Prism Central virtual IP.

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
    hostname=pc_ip,
    username=SSH_USER,
    key_filename=SSH_KEY_PATH,
    timeout=20,
    auth_timeout=20,
    banner_timeout=20,
    allow_agent=False,
    look_for_keys=False,
  )
  logger.info("SSH key auth succeeded for %s as %s", pc_ip, SSH_USER)
  return client


def _execute(client: paramiko.SSHClient, pc_ip: str, command: str) -> str:
  """
  Run a command on an open PC session and return stdout only.

  Args:
    client (paramiko.SSHClient): Connected PC session.
    pc_ip (str): Prism Central IP, used only for log context.
    command (str): Remote shell command to execute.

  Returns:
    str: Remote command stdout with surrounding whitespace stripped.
      Stderr is logged but not returned.
  """
  _stdin, stdout, stderr = client.exec_command(
    _wrapped_command(command), timeout=180
  )
  out = stdout.read().decode("utf-8", errors="replace")
  out = out.replace("\r\n", "\n").replace("\r", "\n")
  err = stderr.read().decode("utf-8", errors="replace").strip()
  if err:
    logger.warning("Remote stderr on %s: %s", pc_ip, err[:500])
  return _extract_marked_section(out).strip()


def collect_remote_connection_health(pc_ip: str) -> dict:
  """
  List PC-PC remote connections and health-check each UUID.

  Args:
    pc_ip (str): Prism Central VIP used as the SSH target.

  Returns:
    dict: Structured result with:
      - ``connection_count`` (int): Number of parsed remote connections.
      - ``connections`` (list): Per-connection name, UUID, health status,
        and raw health-check output.
      - ``status`` (str): ``OK`` when every check passes, ``WARN`` when any
        check fails, or ``ERROR`` when listing itself fails.
      - ``error`` / ``raw_list_output`` on failure.
  """
  client = None
  try:
    client = _connect_client(pc_ip)
    raw_list = _execute(client, pc_ip, LIST_CMD)
    connections = parse_remote_connections(raw_list)
    if not connections and "uuid" not in raw_list.lower():
      return {
        "connection_count": 0,
        "connections": [],
        "status": "ERROR",
        "error": "Unable to parse remote_connection.list_all output",
        "raw_list_output": raw_list[:2000],
      }

    checked = []
    for connection in connections:
      uuid = connection["uuid"]
      logger.info(
        "Health-checking remote connection %s (%s) on %s",
        connection["name"],
        uuid,
        pc_ip,
      )
      health_output = _execute(
        client, pc_ip, HEALTH_CMD.format(uuid=uuid)
      )
      checked.append({
        "name": connection["name"],
        "uuid": uuid,
        "health_status": _health_status(health_output, uuid),
        "health_output": health_output[:2000],
      })

    failed = [
      item for item in checked if item["health_status"] != "OK"
    ]
    status = "WARN" if failed else "OK"
    if failed:
      logger.warning(
        "Remote connection health degraded on %s: %s of %s failed",
        pc_ip,
        len(failed),
        len(checked),
      )
    return {
      "connection_count": len(checked),
      "connections": checked,
      "status": status,
    }

  except Exception as exc:
    logger.error(
      "Failed remote connection check for %s: %s", pc_ip, exc
    )
    return {
      "error": f"Paramiko SSH failed: {exc}",
      "connection_count": 0,
      "connections": [],
      "status": "ERROR",
    }
  finally:
    if client is not None:
      client.close()


def _pc_endpoints(config_data: dict) -> list:
  """
  Collect Prism Central endpoint dicts from either config layout.

  Args:
    config_data (dict): Parsed endpoints.json.

  Returns:
    list: Endpoint objects whose type is PC, or entries under ``pcs``.
  """
  if "pcs" in config_data:
    return list(config_data.get("pcs", []))

  pcs = []
  for _zone, entries in config_data.items():
    if not isinstance(entries, list):
      continue
    for entry in entries:
      if str(entry.get("type", "")).upper() == "PC":
        pcs.append(entry)
  return pcs


def run_remote_connection_checks(config_path: str = None) -> None:
  """
  Read endpoints, health-check PC remote connections, and print JSON.

  Args:
    config_path (str, optional): Path to endpoints.json.

  Returns:
    None: Prints a JSON object keyed by PC IP to stdout for LocalProcessor.
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

  final_results = {}
  for pc in _pc_endpoints(config_data):
    ip = pc.get("ip") or pc.get("virtual_ip")
    if not ip:
      continue
    logger.info(
      "Checking remote connections: %s (%s)...", pc.get("name", ip), ip
    )
    final_results[ip] = collect_remote_connection_health(ip)

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  run_remote_connection_checks()
