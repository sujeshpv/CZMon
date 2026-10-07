"""Curator Full Scan status for the ``curator_full_scan`` CLI metric.

``python runner.py --run-type cli`` runs the ``curator_full_scan`` command of
cli_metrics_catalog.json on every PE and stores its raw output in the
curator_full_scan table, like every other CLI metric. This module turns one
stored row into the Curator Full Scan result document shown on the Stats
page: it reads the CVM marker, exit code and cluster VIP that the catalog
command prints, selects only the table whose ``Job Name`` is exactly
``Full Scan`` and classifies the result. Start and end times are reported
exactly as curator_cli prints them.
"""

import re

METRIC_NAME = "curator_full_scan"
COMMAND = "curator_cli get_last_successful_scans"
FULL_SCAN_JOB_NAME = "Full Scan"
STATUS_SUCCEEDED = "Succeeded"
SUCCESS_STATUS_VALUES = ("0", "succeeded")
SCAN_STATUS_UNKNOWN = "UnknownStatus"
_SCAN_STATUS_ALIASES = {
  "running": "Running",
  "succeeded": STATUS_SUCCEEDED,
  "failed": "Failed",
  "canceled": "Canceled",
  "cancelled": "Canceled",
}

COLLECTION_SUCCESS = "success"
COLLECTION_UNAVAILABLE = "unavailable"
COLLECTION_FAILED = "failed"

EXECUTION_COMPLETED = "completed"
EXECUTION_FAILED = "failed"

SSH_EXECUTION_FAILED = "SSH_EXECUTION_FAILED"
COMMAND_FAILED = "COMMAND_FAILED"
NO_CURATOR_MASTER = "NO_CURATOR_MASTER"
SCAN_STAT_PROTO_NOT_FOUND = "SCAN_STAT_PROTO_NOT_FOUND"
EMPTY_OUTPUT = "EMPTY_OUTPUT"
MALFORMED_OUTPUT = "MALFORMED_OUTPUT"
FULL_SCAN_NOT_FOUND = "FULL_SCAN_NOT_FOUND"
FULL_SCAN_NOT_SUCCEEDED = "FULL_SCAN_NOT_SUCCEEDED"
CLUSTER_ID_UNRESOLVED = "CLUSTER_ID_UNRESOLVED"

_FAILED_CODES = {
  SSH_EXECUTION_FAILED, COMMAND_FAILED, EMPTY_OUTPUT, MALFORMED_OUTPUT,
}
_CURATOR_CONDITION_CODES = {NO_CURATOR_MASTER, SCAN_STAT_PROTO_NOT_FOUND}
_EXECUTION_ERRORS = {
  SSH_EXECUTION_FAILED: (
    EXECUTION_FAILED,
    "'{command}' could not be run on {ip}: {detail}",
    "Check that the PE CVMs can reach each other over SSH and that the "
    "curator_full_scan command in cli_metrics_catalog.json is unchanged.",
  ),
}

_FIELDS = {
  "job name": "job_name",
  "job id": "job_id",
  "execution id": "execution_id",
  "master handle": "master_handle",
  "incarnation id": "incarnation_id",
  "status": "status",
  "start time": "start_time",
  "end time": "end_time",
}
_INTEGER_FIELDS = ("job_id", "execution_id", "incarnation_id")
_NON_EMPTY_FIELDS = (
  "job_name", "job_id", "execution_id", "incarnation_id",
  "status", "start_time", "end_time",
)

_BORDER_RE = re.compile(r"^\s*\+[-=+]+\+\s*$")
_ROW_RE = re.compile(r"^\s*\|(?P<key>[^|]*)\|(?P<value>[^|]*)\|?\s*$")
_MASTER_RE = re.compile(r"using curator master:\s*(?P<master>\S+)", re.IGNORECASE)
_NO_MASTER_RE = re.compile(r"no curator master", re.IGNORECASE)
_PROTO_NOT_FOUND_RE = re.compile(
  r"curator scan stat proto not found", re.IGNORECASE
)


def _issue(code, message, action):
  """
  Build one entry of a result's ``errors`` or ``warnings`` list.

  Parameters
  ----------
  code : str
    Issue code, e.g. ``COMMAND_FAILED``.
  message : str
    What went wrong.
  action : str
    What the operator should check.

  Returns
  -------
  dict
  """
  return {"code": code, "message": message, "action": action}


def parse_scan_tables(output):
  """
  Split ``curator_cli get_last_successful_scans`` output into scan tables.

  A table is a run of ``| key | value |`` rows. Tables are delimited by
  ``+---+`` borders of any length, by non-row lines, or by a repeated
  ``Job Name`` row. Keys are lower-cased with internal whitespace collapsed;
  values are stripped of padding.

  Parameters
  ----------
  output : str
    Raw command stdout.

  Returns
  -------
  list
    One list of ``(key, value)`` tuples per table, in output order.
  """
  tables = []
  current = []
  for line in (output or "").splitlines():
    if _BORDER_RE.match(line):
      if current:
        tables.append(current)
        current = []
      continue
    match = _ROW_RE.match(line)
    if not match:
      if current:
        tables.append(current)
        current = []
      continue
    key = " ".join(match.group("key").split()).lower()
    value = match.group("value").strip()
    if key == "job name" and any(k == "job name" for k, _ in current):
      tables.append(current)
      current = []
    current.append((key, value))
  if current:
    tables.append(current)
  return tables


def _job_name(table):
  """
  Return the ``Job Name`` value of a parsed scan table.

  Parameters
  ----------
  table : list
    ``(key, value)`` tuples of one table from parse_scan_tables.

  Returns
  -------
  str or None
  """
  return next((value for key, value in table if key == "job name"), None)


def normalize_status(status_raw):
  """
  Map Curator success values (``0`` or ``Succeeded``) to ``Succeeded``.

  Any other value is returned stripped but otherwise unchanged.

  Parameters
  ----------
  status_raw : str or None
    ``Status`` value exactly as printed by curator_cli.

  Returns
  -------
  str or None
  """
  if status_raw is None:
    return None
  status = status_raw.strip()
  if status.lower() in SUCCESS_STATUS_VALUES:
    return STATUS_SUCCEEDED
  return status


def scan_status_category(status_raw):
  """
  Map a raw Curator job status to a Stats page scan status category.

  Accepts plain (``Succeeded``), numeric (``0``) and enum-style
  (``kFailed``) values. Anything unrecognised, including a missing Full Scan,
  maps to ``UnknownStatus``.

  Parameters
  ----------
  status_raw : str or None
    ``Status`` value exactly as printed by curator_cli.

  Returns
  -------
  str
    One of Succeeded, Running, Failed, Canceled or UnknownStatus.
  """
  if not status_raw or not status_raw.strip():
    return SCAN_STATUS_UNKNOWN
  value = status_raw.strip()
  if value.lower() in SUCCESS_STATUS_VALUES:
    return STATUS_SUCCEEDED
  if len(value) > 1 and value[0] == "k" and value[1].isupper():
    value = value[1:]
  return _SCAN_STATUS_ALIASES.get(value.lower(), SCAN_STATUS_UNKNOWN)


def _to_int(value):
  """
  Convert a table value to int.

  Parameters
  ----------
  value : str or None
    Value to convert.

  Returns
  -------
  int or None
    None when the value is not an integer.
  """
  try:
    return int(str(value).strip())
  except (TypeError, ValueError):
    return None


def _build_full_scan(table):
  """
  Build the ``full_scan`` object from a parsed Full Scan table.

  Parameters
  ----------
  table : list
    ``(key, value)`` tuples of the Full Scan table from parse_scan_tables.

  Returns
  -------
  tuple
    ``(full_scan, errors)``
  """
  errors = []
  raw = {}
  duplicates = []
  for key, value in table:
    field = _FIELDS.get(key)
    if field is None:
      continue
    if field in raw:
      duplicates.append(field)
      continue
    raw[field] = value

  missing = [field for field in _FIELDS.values() if field not in raw]
  empty = [field for field in _NON_EMPTY_FIELDS if field in raw and not raw[field]]
  not_integer = [
    field for field in _INTEGER_FIELDS
    if raw.get(field) and _to_int(raw[field]) is None
  ]
  problems = []
  if missing:
    problems.append(f"missing fields {missing}")
  if empty:
    problems.append(f"empty fields {empty}")
  if not_integer:
    problems.append(f"non-integer fields {not_integer}")
  if duplicates:
    problems.append(f"duplicate fields {sorted(set(duplicates))}")
  if problems:
    errors.append(_issue(
      MALFORMED_OUTPUT,
      "Full Scan table is incomplete or malformed: " + "; ".join(problems) + ".",
      "Inspect command_result.raw_output; the curator_cli output format may "
      "have changed or the output was truncated.",
    ))

  status_raw = raw.get("status")
  full_scan = {
    "job_name": raw.get("job_name"),
    "job_id": _to_int(raw.get("job_id")),
    "execution_id": _to_int(raw.get("execution_id")),
    "master_handle": raw.get("master_handle"),
    "incarnation_id": _to_int(raw.get("incarnation_id")),
    "status_raw": status_raw,
    "status": normalize_status(status_raw),
    "start_time": raw.get("start_time"),
    "end_time": raw.get("end_time"),
  }

  execution_id = full_scan["execution_id"]
  if status_raw and (
      full_scan["status"] != STATUS_SUCCEEDED
      or (execution_id is not None and execution_id < 0)):
    errors.append(_issue(
      FULL_SCAN_NOT_SUCCEEDED,
      f"Curator reports no successful Full Scan (status_raw={status_raw!r}, "
      f"execution_id={execution_id}).",
      "Check Curator health and scan history on the Curator master "
      "(http://<curator-master>:2010) and curator logs in ~/data/logs.",
    ))
  return full_scan, errors


def _collection_status(errors, full_scan):
  """
  Classify a result as success, unavailable or failed.

  Parameters
  ----------
  errors : list
    Issues collected by build_result.
  full_scan : dict or None
    Parsed Full Scan object, or None when no Full Scan table was parsed.

  Returns
  -------
  str
  """
  codes = {issue["code"] for issue in errors}
  hard_failures = codes & _FAILED_CODES
  if codes & _CURATOR_CONDITION_CODES:
    hard_failures -= {COMMAND_FAILED, EMPTY_OUTPUT}
  if hard_failures:
    return COLLECTION_FAILED
  if codes:
    return COLLECTION_UNAVAILABLE
  return COLLECTION_SUCCESS if full_scan is not None else COLLECTION_FAILED


def build_result(
    cluster_ip,
    exit_code,
    stdout,
    stderr="",
    cluster_id=None,
    cluster_name=None,
    cluster_vip=None,
    command=COMMAND,
    collected_at=None,
    execution_error=None,
    execution_error_code=SSH_EXECUTION_FAILED,
    cluster_info_error=None,
):
  """
  Build the ``curator_full_scan`` JSON document for one cluster.

  ``collection_status`` is ``success`` only when the command completed, a
  single well-formed Full Scan table was found and its status is successful.
  It is ``unavailable`` when Curator answered but has no usable Full Scan
  (no master, no scan stats, missing or unsuccessful Full Scan) and
  ``failed`` for execution or parsing failures. An exit code of zero alone is
  never treated as success.

  Parameters
  ----------
  cluster_ip : str
    PE IP from endpoints.json the command ran against (VIP or CVM IP).
  exit_code : int or None
    Remote exit code; None when the command did not complete.
  stdout : str
    Complete command stdout; stored unmodified in raw_output.
  stderr : str
    Complete command stderr.
  cluster_id : str, optional
    Cluster UUID from the framework's clusters table.
  cluster_name : str, optional
    Cluster name from the framework's clusters table.
  cluster_vip : str, optional
    Cluster virtual IP read from the CVM; equals cluster_ip when the
    endpoint is configured by virtual IP.
  command : str
    Command that was executed.
  collected_at : str, optional
    When the row was stored (the table's ``created_at``).
  execution_error : str, optional
    Error detail when the command could not be run to completion.
  execution_error_code : str
    Issue code for execution_error; SSH_EXECUTION_FAILED.
  cluster_info_error : str, optional
    Why the cluster UUID could not be resolved.

  Returns
  -------
  dict
  """
  stdout = stdout if stdout is not None else ""
  stderr = stderr if stderr is not None else ""
  combined = f"{stdout}\n{stderr}"
  errors = []
  warnings = []
  full_scan = None

  if execution_error is not None:
    execution_status, message, action = _EXECUTION_ERRORS[execution_error_code]
    errors.append(_issue(
      execution_error_code,
      message.format(command=command, ip=cluster_ip, detail=execution_error),
      action,
    ))
  elif exit_code == 0:
    execution_status = EXECUTION_COMPLETED
  else:
    execution_status = EXECUTION_FAILED
    errors.append(_issue(
      COMMAND_FAILED,
      f"'{command}' exited with code {exit_code} on {cluster_ip}.",
      "Run the command manually on a CVM of the cluster and check stderr "
      "and the Curator service status.",
    ))

  if _NO_MASTER_RE.search(combined):
    errors.append(_issue(
      NO_CURATOR_MASTER,
      "Curator reported no master; scan status is unavailable.",
      "Check the Curator service with 'cluster status | grep -i curator' "
      "and review ~/data/logs/curator*.",
    ))
  if _PROTO_NOT_FOUND_RE.search(combined):
    errors.append(_issue(
      SCAN_STAT_PROTO_NOT_FOUND,
      "Curator has no scan statistics recorded (scan stat proto not found).",
      "Curator may not have completed a scan since it started; wait for the "
      "next scheduled Full Scan and review ~/data/logs/curator*.",
    ))

  if execution_error is None:
    known_condition = any(
      issue["code"] in _CURATOR_CONDITION_CODES for issue in errors
    )
    if not stdout.strip():
      errors.append(_issue(
        EMPTY_OUTPUT,
        f"'{command}' produced no stdout.",
        "Run the command manually on a CVM; confirm curator_cli is on the "
        "login shell PATH and Curator is running.",
      ))
    else:
      tables = parse_scan_tables(stdout)
      full_tables = [t for t in tables if _job_name(t) == FULL_SCAN_JOB_NAME]
      if not tables:
        if not known_condition:
          errors.append(_issue(
            MALFORMED_OUTPUT,
            "No scan tables found in the command output.",
            "Inspect command_result.raw_output; the curator_cli output "
            "format may have changed.",
          ))
      elif not full_tables:
        errors.append(_issue(
          FULL_SCAN_NOT_FOUND,
          "No table with Job Name 'Full Scan' in the output; found "
          f"{[_job_name(t) for t in tables]}. Partial and Selective scans "
          "are not used as a substitute.",
          "Confirm Curator has completed at least one Full Scan on this "
          "cluster.",
        ))
      else:
        if len(full_tables) > 1:
          errors.append(_issue(
            MALFORMED_OUTPUT,
            f"Found {len(full_tables)} Full Scan tables; expected exactly one.",
            "Inspect command_result.raw_output for duplicated output.",
          ))
        full_scan, scan_errors = _build_full_scan(full_tables[0])
        errors.extend(scan_errors)

  if not cluster_id:
    warnings.append(_issue(
      CLUSTER_ID_UNRESOLVED,
      f"Cluster UUID for {cluster_ip} could not be resolved: "
      f"{cluster_info_error or 'not found in the clusters table'}.",
      "Run the collector again so the clusters table is populated for this "
      "PE, and check that the cluster has a virtual IP configured.",
    ))

  master = _MASTER_RE.search(stdout)
  full_scan_available = bool(
    full_scan
    and full_scan["execution_id"] is not None
    and full_scan["execution_id"] >= 0
  )
  return {
    "metric_name": METRIC_NAME,
    "parser": METRIC_NAME,
    "endpoint_type": "PE",
    "cluster_id": cluster_id or None,
    "cluster_name": cluster_name or None,
    "cluster_ip": cluster_ip,
    "cluster_vip": cluster_vip or None,
    "collection_status": _collection_status(errors, full_scan),
    "command_execution_status": execution_status,
    "full_scan_available": full_scan_available,
    "collected_at": collected_at,
    "command": command,
    "curator_master": master.group("master") if master else None,
    "full_scan": full_scan,
    "errors": errors,
    "warnings": warnings,
    "command_result": {
      "exit_code": exit_code,
      "stderr": stderr,
      "raw_output": stdout,
    },
  }



SSH_FAILED_EXIT_CODE = 255
_HOST_MARKER_RE = re.compile(r"^=+\s*(?P<ip>\d+\.\d+\.\d+\.\d+)\s*=+\s*$")
_EXIT_CODE_RE = re.compile(r"^CZMON_EXIT_CODE=(?P<code>-?\d+)\s*$")
_CLUSTER_VIP_RE = re.compile(
  r'^CZMON_cluster_external_ip:\s*"?(?P<ip>[^"\s]+)"?\s*$'
)


def _split_cvm_blocks(output):
  """
  Split stored CLI output into the per-CVM blocks printed by the svmips loop.

  Parameters
  ----------
  output : str
    Output stored by CliProcessor for the curator_full_scan command.

  Returns
  -------
  list
    ``(cvm_ip, lines)`` tuples in output order; cvm_ip is None for output
    before the first marker.
  """
  blocks = []
  cvm_ip, lines = None, []
  for line in (output or "").splitlines():
    marker = _HOST_MARKER_RE.match(line.strip())
    if marker:
      if cvm_ip is not None or lines:
        blocks.append((cvm_ip, lines))
      cvm_ip, lines = marker.group("ip"), []
      continue
    lines.append(line)
  if cvm_ip is not None or lines:
    blocks.append((cvm_ip, lines))
  return blocks


def parse_cli_output(output):
  """
  Read the curator_cli answer and the CZMON_* lines from stored CLI output.

  The catalog command stops at the first CVM it can reach over SSH, so the
  last block holds the curator_cli answer; earlier blocks are CVMs that
  could not be reached.

  Parameters
  ----------
  output : str
    Output stored by CliProcessor for the curator_full_scan command.

  Returns
  -------
  dict
    ``cvm_ip``, ``stdout`` (curator_cli output without the CZMON_* lines),
    ``exit_code`` (int or None when no CZMON_EXIT_CODE line was printed),
    ``cluster_vip`` and ``unreachable_cvms``.
  """
  blocks = _split_cvm_blocks(output)
  parsed = {
    "cvm_ip": None, "stdout": "", "exit_code": None,
    "cluster_vip": None, "unreachable_cvms": [],
  }
  if not blocks:
    return parsed
  for cvm_ip, lines in blocks[:-1]:
    if cvm_ip:
      parsed["unreachable_cvms"].append(cvm_ip)
  cvm_ip, lines = blocks[-1]
  parsed["cvm_ip"] = cvm_ip
  stdout_lines = []
  for line in lines:
    stripped = line.strip()
    exit_code = _EXIT_CODE_RE.match(stripped)
    vip = _CLUSTER_VIP_RE.match(stripped)
    if exit_code:
      parsed["exit_code"] = int(exit_code.group("code"))
    elif vip:
      parsed["cluster_vip"] = vip.group("ip")
    elif not stripped.startswith("CZMON_"):
      stdout_lines.append(line)
  parsed["stdout"] = "\n".join(stdout_lines)
  return parsed


def build_result_from_cli_row(row, clusters=None):
  """
  Build the result document for one row of the curator_full_scan table.

  Parameters
  ----------
  row : dict
    Row stored by CliProcessor; uses ``ip``, ``output``, ``cluster_name``
    and ``created_at``.
  clusters : dict, optional
    Cluster virtual IP -> ``{"uuid": ..., "name": ...}`` from the
    framework's clusters table.

  Returns
  -------
  dict
    The build_result document, plus ``cvm_ip`` and ``unreachable_cvms``.
  """
  clusters = clusters or {}
  ip = row.get("ip")
  parsed = parse_cli_output(row.get("output"))
  vip = parsed["cluster_vip"]
  identity = clusters.get(vip) or clusters.get(ip) or {}

  execution_error = None
  if parsed["exit_code"] is None:
    execution_error = "the output has no CZMON_EXIT_CODE line"
  elif parsed["exit_code"] == SSH_FAILED_EXIT_CODE:
    execution_error = (
      f"SSH from the PE to its CVMs failed (last tried {parsed['cvm_ip']})"
    )

  result = build_result(
    cluster_ip=ip,
    exit_code=parsed["exit_code"],
    stdout=parsed["stdout"],
    cluster_id=identity.get("uuid"),
    cluster_name=identity.get("name") or row.get("cluster_name"),
    cluster_vip=vip,
    collected_at=row.get("created_at"),
    execution_error=execution_error,
  )
  result["cvm_ip"] = parsed["cvm_ip"]
  result["unreachable_cvms"] = parsed["unreachable_cvms"]
  return result
