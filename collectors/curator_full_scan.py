"""
Curator full-scan status parser for the ``curator_full_scan`` CLI metric.

Parses the output of ``curator_cli get_last_successful_scans``, selects only
the table whose ``Job Name`` is exactly ``Full Scan`` and builds the JSON
document that CliProcessor stores in the metric table's ``output_json``
column and the Stats page renders.
"""

import ipaddress
import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

METRIC_NAME = "curator_full_scan"
COMMAND = "curator_cli get_last_successful_scans"
CLUSTER_VIP_COMMAND = "zeus_config_printer | grep ^cluster_external_ip:"
FULL_SCAN_JOB_NAME = "Full Scan"
STATUS_SUCCEEDED = "Succeeded"
SUCCESS_STATUS_VALUES = ("0", "succeeded")
TIMESTAMP_FORMAT = "%Y %b %d %H:%M:%S"
SCAN_STATUS_UNKNOWN = "UnknownStatus"
_SCAN_STATUS_ALIASES = {
  "running": "Running",
  "succeeded": STATUS_SUCCEEDED,
  "failed": "Failed",
  "canceled": "Canceled",
  "cancelled": "Canceled",
}
RECORD_KEY_COLUMNS = ("metric_name", "cluster_key", "ip")
RESULT_IDENTITY_COLUMNS = (
  "collection_status", "execution_id", "full_scan_status_raw", "error_codes",
)
DEFAULT_RETENTION_DAYS = 90
DEFAULT_RETENTION_COUNT = 180

COLLECTION_SUCCESS = "success"
COLLECTION_UNAVAILABLE = "unavailable"
COLLECTION_FAILED = "failed"

EXECUTION_COMPLETED = "completed"
EXECUTION_FAILED = "failed"
EXECUTION_TIMED_OUT = "timed_out"
EXECUTION_NOT_EXECUTED = "not_executed"

TARGET_UNREACHABLE = "TARGET_UNREACHABLE"
SSH_EXECUTION_FAILED = "SSH_EXECUTION_FAILED"
COMMAND_TIMEOUT = "COMMAND_TIMEOUT"
COMMAND_FAILED = "COMMAND_FAILED"
NO_CURATOR_MASTER = "NO_CURATOR_MASTER"
SCAN_STAT_PROTO_NOT_FOUND = "SCAN_STAT_PROTO_NOT_FOUND"
EMPTY_OUTPUT = "EMPTY_OUTPUT"
MALFORMED_OUTPUT = "MALFORMED_OUTPUT"
FULL_SCAN_NOT_FOUND = "FULL_SCAN_NOT_FOUND"
FULL_SCAN_NOT_SUCCEEDED = "FULL_SCAN_NOT_SUCCEEDED"
TIMEZONE_UNKNOWN = "TIMEZONE_UNKNOWN"
CLUSTER_ID_UNRESOLVED = "CLUSTER_ID_UNRESOLVED"

_FAILED_CODES = {
  TARGET_UNREACHABLE, SSH_EXECUTION_FAILED, COMMAND_TIMEOUT,
  COMMAND_FAILED, EMPTY_OUTPUT, MALFORMED_OUTPUT,
}
_CURATOR_CONDITION_CODES = {NO_CURATOR_MASTER, SCAN_STAT_PROTO_NOT_FOUND}
_EXECUTION_ERRORS = {
  TARGET_UNREACHABLE: (
    EXECUTION_NOT_EXECUTED,
    "Could not connect to target {ip}: {detail}",
    "Verify the PE is reachable on port 22 and the framework SSH key "
    "(ssh/keys/nutanix) is authorized for the nutanix user on its CVMs.",
  ),
  SSH_EXECUTION_FAILED: (
    EXECUTION_FAILED,
    "Remote execution of '{command}' failed on {ip}: {detail}",
    "Verify the SSH session to the CVM and re-run the collector.",
  ),
  COMMAND_TIMEOUT: (
    EXECUTION_TIMED_OUT,
    "'{command}' did not finish on {ip}: {detail}",
    "Check Curator responsiveness on the cluster or raise timeout_secs for "
    "curator_full_scan in cli_metrics_catalog.json.",
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
_CLUSTER_EXTERNAL_IP_RE = re.compile(
  r'^\s*cluster_external_ip:\s*"?(?P<ip>[^"\s]+)"?\s*$', re.MULTILINE
)


def parse_cluster_external_ip(output):
  """
  Return the cluster virtual IP from ``zeus_config_printer`` output, or None.
  """
  match = _CLUSTER_EXTERNAL_IP_RE.search(output or "")
  if not match:
    return None
  try:
    return str(ipaddress.ip_address(match.group("ip")))
  except ValueError:
    return None


def _utc_now_iso():
  return datetime.now(timezone.utc).isoformat()


def _issue(code, message, action):
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
  return next((value for key, value in table if key == "job name"), None)


def normalize_status(status_raw):
  """
  Map Curator success values (``0`` or ``Succeeded``) to ``Succeeded``.

  Any other value is returned stripped but otherwise unchanged.
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
  """
  if not status_raw or not status_raw.strip():
    return SCAN_STATUS_UNKNOWN
  value = status_raw.strip()
  if value.lower() in SUCCESS_STATUS_VALUES:
    return STATUS_SUCCEEDED
  if len(value) > 1 and value[0] == "k" and value[1].isupper():
    value = value[1:]
  return _SCAN_STATUS_ALIASES.get(value.lower(), SCAN_STATUS_UNKNOWN)


def _resolve_timezone(timezone_name):
  if not timezone_name:
    return None
  try:
    return ZoneInfo(timezone_name)
  except (ZoneInfoNotFoundError, ValueError):
    return None


def parse_timestamp(raw, timezone_name):
  """
  Convert a Curator timestamp (``2026 Sep 24 13:40:08``) to ISO-8601 UTC.

  Curator prints cluster-local wall-clock time without a zone, so the value
  is only converted when the cluster's IANA timezone is known.

  Parameters
  ----------
  raw : str
    Timestamp exactly as printed by curator_cli.
  timezone_name : str or None
    IANA timezone of the cluster, e.g. ``UTC`` or ``Asia/Kolkata``.

  Returns
  -------
  str or None
    ISO-8601 timestamp in UTC, or None if the timezone is unknown or the
    raw value cannot be parsed.
  """
  tzinfo = _resolve_timezone(timezone_name)
  if not raw or tzinfo is None:
    return None
  try:
    local = datetime.strptime(raw.strip(), TIMESTAMP_FORMAT)
  except ValueError:
    return None
  return local.replace(tzinfo=tzinfo).astimezone(timezone.utc).isoformat()


def _is_valid_timestamp(raw):
  try:
    datetime.strptime((raw or "").strip(), TIMESTAMP_FORMAT)
    return True
  except ValueError:
    return False


def _to_int(value):
  try:
    return int(str(value).strip())
  except (TypeError, ValueError):
    return None


def _build_full_scan(table, cluster_timezone):
  """
  Build the ``full_scan`` object from a parsed Full Scan table.

  Returns
  -------
  tuple
    ``(full_scan, errors, warnings)``
  """
  errors = []
  warnings = []
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
  bad_timestamps = [
    field for field in ("start_time", "end_time")
    if raw.get(field) and not _is_valid_timestamp(raw[field])
  ]
  problems = []
  if missing:
    problems.append(f"missing fields {missing}")
  if empty:
    problems.append(f"empty fields {empty}")
  if not_integer:
    problems.append(f"non-integer fields {not_integer}")
  if bad_timestamps:
    problems.append(
      f"timestamps not in '{TIMESTAMP_FORMAT}' format {bad_timestamps}"
    )
  if duplicates:
    problems.append(f"duplicate fields {sorted(set(duplicates))}")
  if problems:
    errors.append(_issue(
      MALFORMED_OUTPUT,
      "Full Scan table is incomplete or malformed: " + "; ".join(problems) + ".",
      "Inspect command_result.raw_output; the curator_cli output format may "
      "have changed or the output was truncated.",
    ))

  timezone_known = _resolve_timezone(cluster_timezone) is not None
  if not timezone_known:
    warnings.append(_issue(
      TIMEZONE_UNKNOWN,
      f"Cluster timezone {cluster_timezone!r} is unknown or invalid; "
      "start_time/end_time are left null and only the raw cluster-local "
      "values are reported.",
      "Run the api collector so the clusters table records the Prism "
      "cluster timezone for this cluster.",
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
    "start_time_raw": raw.get("start_time"),
    "end_time_raw": raw.get("end_time"),
    "start_time": parse_timestamp(raw.get("start_time"), cluster_timezone),
    "end_time": parse_timestamp(raw.get("end_time"), cluster_timezone),
    "timestamp_timezone": cluster_timezone if timezone_known else None,
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
  return full_scan, errors, warnings


def _collection_status(errors, full_scan):
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
    cluster_timezone=None,
    cluster_vip=None,
    command=COMMAND,
    collected_at=None,
    execution_error=None,
    execution_error_code=SSH_EXECUTION_FAILED,
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
    Framework target IP the command ran against.
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
  cluster_timezone : str, optional
    IANA timezone of the cluster; timestamps stay null when unknown.
  cluster_vip : str, optional
    Cluster virtual IP the identity was resolved from; equals cluster_ip
    when the endpoint is configured by virtual IP.
  command : str
    Catalog command that was executed.
  collected_at : str, optional
    ISO-8601 collection time; defaults to now (UTC).
  execution_error : str, optional
    Error detail when the command could not be run to completion.
  execution_error_code : str
    TARGET_UNREACHABLE, SSH_EXECUTION_FAILED or COMMAND_TIMEOUT.

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
        full_scan, scan_errors, scan_warnings = _build_full_scan(
          full_tables[0], cluster_timezone
        )
        errors.extend(scan_errors)
        warnings.extend(scan_warnings)

  if not cluster_id:
    warnings.append(_issue(
      CLUSTER_ID_UNRESOLVED,
      f"No cluster UUID found in the clusters table for {cluster_ip} "
      f"(cluster virtual IP: {cluster_vip or 'unknown'}); the record is keyed "
      "by target IP instead.",
      "Run the api collector so the clusters table is populated, and check "
      "that the cluster has a virtual IP configured.",
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
    "collected_at": collected_at or _utc_now_iso(),
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


def to_db_row(result):
  """
  Map a ``build_result`` document to a CLI metric table row.

  Keeps the ``command``/``output``/``output_json``/``ip``/``cluster_name``
  columns used by the other CLI metrics and adds queryable copies of the
  status fields. ``cluster_key`` is the framework cluster UUID, or
  ``ip:<target>`` when the UUID cannot be resolved; together with
  ``metric_name`` it groups one cluster's collection history.
  """
  full_scan = result.get("full_scan") or {}
  command_result = result["command_result"]
  execution_id = full_scan.get("execution_id")
  exit_code = command_result["exit_code"]
  return {
    "metric_name": result["metric_name"],
    "cluster_key": result["cluster_id"] or f"ip:{result['cluster_ip']}",
    "cluster_id": result["cluster_id"],
    "cluster_name": result["cluster_name"],
    "ip": result["cluster_ip"],
    "cluster_vip": result["cluster_vip"],
    "command": result["command"],
    "collection_status": result["collection_status"],
    "command_execution_status": result["command_execution_status"],
    "exit_code": str(exit_code) if exit_code is not None else None,
    "full_scan_available": "true" if result["full_scan_available"] else "false",
    "full_scan_status": full_scan.get("status"),
    "full_scan_status_raw": full_scan.get("status_raw"),
    "execution_id": str(execution_id) if execution_id is not None else None,
    "full_scan_end_time_raw": full_scan.get("end_time_raw"),
    "error_codes": ",".join(issue["code"] for issue in result["errors"]),
    "output": command_result["raw_output"],
    "stderr": command_result["stderr"],
    "output_json": json.dumps(result),
    "collected_at": result["collected_at"],
  }
