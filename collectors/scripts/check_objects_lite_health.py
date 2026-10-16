"""Checks Objects Lite pod health on Prism Central.

SSHes to each PCVM and runs kubectl to read the objects-lite pod
phase. Prints JSON for the CZMon local CLI framework.
"""

import json
import logging
import os
import shlex
import sys

from common.connection.ssh_connect import Ssh

# --- Global Configurations ---
# PCVM SSH credentials (not Prism admin). kubectl on PC requires this login.
DEF_UNAME = "nutanix"
DEF_PWD = "nutanix/4u"
OBJECTS_LITE_NAMESPACE = "pc-platform-nci"
OBJECTS_LITE_POD = "objects-lite-0"
POD_PHASES = ("Pending", "Running", "Succeeded", "Failed")
PHASE_MARKER = "__CZMON_PHASE__"

logger = logging.getLogger(__name__)


def get_pods(password: str) -> str:
  """Run both operator commands with sudo on the PCVM.

  1) sudo kubectl get pods -A | grep objects-lite  -> READY/STATUS/RESTARTS/AGE
  2) sudo kubectl get pod objects-lite-0 -n pc-platform-nci
     -o=jsonpath='{.status.phase}{"\\n"}'          -> pod state

  Args:
    password (str): PCVM password used for sudo -S when passwordless sudo
      is not available.

  Returns:
    str: Remote shell script that runs both sudo kubectl commands.
  """
  list_plain = "sudo kubectl get pods -A | grep objects-lite"
  phase_plain = (
    f"sudo kubectl get pod {OBJECTS_LITE_POD} -n {OBJECTS_LITE_NAMESPACE} "
    """-o=jsonpath='{.status.phase}{"\\n"}'"""
  )
  sudo_s = f"printf '%s\\n' {shlex.quote(password)} | sudo -S -p ''"
  list_auth = f"{sudo_s} kubectl get pods -A | grep objects-lite"
  phase_auth = (
    f"{sudo_s} kubectl get pod {OBJECTS_LITE_POD} "
    f"-n {OBJECTS_LITE_NAMESPACE} "
    """-o=jsonpath='{.status.phase}{"\\n"}'"""
  )
  return (
    "if sudo -n true >/dev/null 2>&1; then "
    f"{list_plain}; echo {PHASE_MARKER}; {phase_plain}; "
    f"else {list_auth}; echo {PHASE_MARKER}; {phase_auth}; fi"
  )


def _clean_remote_output(raw_output: str) -> str:
  """Drop sudo password prompts and other SSH/PTY noise.

  Args:
    raw_output (str): Combined stdout or stderr from the remote SSH command.

  Returns:
    str: Output with empty lines and sudo password prompts removed.
  """
  cleaned = []
  for line in (raw_output or "").splitlines():
    stripped = line.strip()
    lower = stripped.lower()
    if not stripped:
      continue
    if lower.startswith("[sudo]") or "password for" in lower:
      continue
    if "sorry, try again" in lower:
      continue
    cleaned.append(line)
  return "\n".join(cleaned)


def _run_ssh_command(ip: str, username: str, password: str, command: str) -> str:
  """Run a remote command on the PCVM using the shared SSH helper.

  Args:
    ip (str): Prism Central / PCVM IP to connect to.
    username (str): SSH username, typically nutanix.
    password (str): SSH password for the PCVM user.
    command (str): Shell command to run on the remote host.

  Returns:
    str: Cleaned remote stdout from Ssh.execute.
  """
  ssh_obj = Ssh(ip, username, password, key_path=None)
  try:
    wrapped = f"bash -lc {shlex.quote(command)}"
    return _clean_remote_output(ssh_obj.execute(wrapped))
  finally:
    ssh_obj.close_session()


def get_pods_status(raw_output: str) -> list:
  """Parse `kubectl get pods -A | grep objects-lite` into pod dicts.

  Args:
    raw_output (str): kubectl get pods listing filtered to objects-lite.

  Returns:
    list: Pod dicts with namespace, name, ready, status, restarts, age,
      and running_for.
  """
  pods = []
  for line in (raw_output or "").splitlines():
    line = line.strip()
    if not line or "objects-lite" not in line:
      continue
    if line.lower().startswith("namespace"):
      continue
    parts = line.split()
    if len(parts) < 4:
      continue
    age = parts[-1] if len(parts) >= 6 else ""
    restarts = " ".join(parts[4:-1]) if len(parts) >= 6 else (parts[4] if len(parts) > 4 else "")
    pods.append({
      "namespace": parts[0],
      "name": parts[1],
      "ready": parts[2],
      "status": parts[3],
      "restarts": restarts,
      "age": age,
      "running_for": age,
    })
  return pods


def check_objects_lite_health(pc_ip: str, username: str = None, password: str = None) -> dict:
  """SSH to a PCVM and collect objects-lite pod phase/status.

  PCVM SSH always uses nutanix / nutanix/4u. Prism admin credentials from
  endpoints.json are ignored because kubectl on PC is not reachable that way.

  Args:
    pc_ip (str): Prism Central IP.
    username (str): Unused; SSH user is DEF_UNAME.
    password (str): Unused; SSH password is DEF_PWD.

  Returns:
    dict: Pod list, kubectl phase/status, running_for, ready, and restarts.
      Includes an error key when SSH/kubectl fails.
  """
  ssh_user = DEF_UNAME
  ssh_password = DEF_PWD
  last_error = None
  combined_output = ""

  try:
    combined_output = _run_ssh_command(
      pc_ip, ssh_user, ssh_password, get_pods(ssh_password)
    )
  except Exception as exc:
    last_error = exc
    logger.warning(
      "Objects Lite sudo kubectl failed on %s as %s: %s",
      pc_ip,
      ssh_user,
      exc,
    )

  if last_error:
    return {
      "error": f"SSH/sudo kubectl failed: {last_error}",
      "phase": "Unknown",
      "status": "Unknown",
      "running_for": "",
      "pods": [],
    }

  list_output, _, phase_block = combined_output.partition(PHASE_MARKER)
  list_output = list_output.strip()
  phase_output = phase_block.strip()
  pods = get_pods_status(list_output)
  phase = (phase_output or "").strip().splitlines()
  phase = next(
    (line.strip() for line in reversed(phase) if line.strip() in POD_PHASES),
    "",
  )
  if phase not in POD_PHASES:
    if pods:
      phase = pods[0].get("status") or "Unknown"
    else:
      phase = "Unknown"

  for pod in pods:
    if pod.get("name") == OBJECTS_LITE_POD and not pod.get("phase"):
      pod["phase"] = phase
    else:
      pod.setdefault("phase", pod.get("status") or "")

  if not pods and phase in POD_PHASES:
    pods.append({
      "namespace": OBJECTS_LITE_NAMESPACE,
      "name": OBJECTS_LITE_POD,
      "ready": "",
      "status": phase,
      "restarts": "",
      "age": "",
      "running_for": "",
      "phase": phase,
    })

  primary = next(
    (pod for pod in pods if pod.get("name") == OBJECTS_LITE_POD),
    pods[0] if pods else {},
  )
  running_for = primary.get("age") or primary.get("running_for") or ""
  return {
    "phase": phase,
    "status": phase,
    "running_for": running_for,
    "ready": primary.get("ready") or "",
    "restarts": primary.get("restarts") or "",
    "namespace": primary.get("namespace") or OBJECTS_LITE_NAMESPACE,
    "pod_name": primary.get("name") or OBJECTS_LITE_POD,
    "pods": pods,
    "pod_list_output": list_output,
    "phase_output": phase_output,
  }


def collect_all_objects_lite_health(config_path: str = None) -> None:
  """Read endpoints, check Objects Lite on every PC, and print JSON.

  Args:
    config_path (str, optional): Path to the endpoints JSON config file.
      Defaults to None (auto-resolves path).

  Returns:
    None: Prints a JSON object of Objects Lite results keyed by PC IP to stdout.
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
  except Exception as exc:
    logger.error("Failed to load config at %s: %s", config_path, exc)
    sys.exit(1)

  pc_endpoints = []
  if "pcs" in config_data:
    pc_endpoints = config_data.get("pcs", [])
  else:
    for _zone, entries in config_data.items():
      if isinstance(entries, list):
        for entry in entries:
          if str(entry.get("type", "")).upper() == "PC":
            pc_endpoints.append(entry)

  final_results = {}
  for endpoint in pc_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    if not ip:
      continue
    logger.info("Checking Objects Lite health on PC: %s as %s...", ip, DEF_UNAME)
    result = check_objects_lite_health(ip)
    result["pc_name"] = endpoint.get("name") or ip
    final_results[ip] = result

  print(json.dumps(final_results, indent=2))


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
  )
  collect_all_objects_lite_health()
