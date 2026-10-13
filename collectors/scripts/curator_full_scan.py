"""Curator Full Scan status for the ``curator_full_scan`` CLI metric.

``python runner.py --run-type cli`` runs ``curator_cli
get_last_successful_scans`` (the ``curator_full_scan`` entry of
cli_metrics_catalog.json) once per PE, on the CVM CliProcessor logs in to
(``build_command``), and stores its raw output in the curator_full_scan
table, like every other CLI metric. This module turns one stored row into the
Curator Full Scan result document shown on the Stats page: it selects only
the table whose ``Job Name`` is exactly ``Full Scan`` and classifies the
result. Start and end times are reported exactly as curator_cli prints them.
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

COMMAND_FAILED = "COMMAND_FAILED"
NO_CURATOR_MASTER = "NO_CURATOR_MASTER"
SCAN_STAT_PROTO_NOT_FOUND = "SCAN_STAT_PROTO_NOT_FOUND"
EMPTY_OUTPUT = "EMPTY_OUTPUT"
MALFORMED_OUTPUT = "MALFORMED_OUTPUT"
FULL_SCAN_NOT_FOUND = "FULL_SCAN_NOT_FOUND"
FULL_SCAN_NOT_SUCCEEDED = "FULL_SCAN_NOT_SUCCEEDED"
CLUSTER_ID_UNRESOLVED = "CLUSTER_ID_UNRESOLVED"

_FAILED_CODES = {COMMAND_FAILED, EMPTY_OUTPUT, MALFORMED_OUTPUT}
_CURATOR_CONDITION_CODES = {NO_CURATOR_MASTER, SCAN_STAT_PROTO_NOT_FOUND}

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
_HOST_MARKER_RE = re.compile(r"^=+\s*(?P<ip>\d+\.\d+\.\d+\.\d+)\s*=+\s*$")


def build_issue(code, message, action):
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


def get_job_name(table):
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


def normalize_scan_status(status_raw):
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


def get_scan_status_category(status_raw):
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


def convert_to_int(value):
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


def build_full_scan_details(table):
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
    if raw.get(field) and convert_to_int(raw[field]) is None
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
    errors.append(build_issue(
      MALFORMED_OUTPUT,
      "Full Scan table is incomplete or malformed: " + "; ".join(problems) + ".",
      "Inspect command_result.raw_output; the curator_cli output format may "
      "have changed or the output was truncated.",
    ))

  status_raw = raw.get("status")
  full_scan = {
    "job_name": raw.get("job_name"),
    "job_id": convert_to_int(raw.get("job_id")),
    "execution_id": convert_to_int(raw.get("execution_id")),
    "master_handle": raw.get("master_handle"),
    "incarnation_id": convert_to_int(raw.get("incarnation_id")),
    "status_raw": status_raw,
    "status": normalize_scan_status(status_raw),
    "start_time": raw.get("start_time"),
    "end_time": raw.get("end_time"),
  }

  execution_id = full_scan["execution_id"]
  if status_raw and (
      full_scan["status"] != STATUS_SUCCEEDED
      or (execution_id is not None and execution_id < 0)):
    errors.append(build_issue(
      FULL_SCAN_NOT_SUCCEEDED,
      f"Curator reports no successful Full Scan (status_raw={status_raw!r}, "
      f"execution_id={execution_id}).",
      "Check Curator health and scan history on the Curator master "
      "(http://<curator-master>:2010) and curator logs in ~/data/logs.",
    ))
  return full_scan, errors


def get_collection_status(errors, full_scan):
  """
  Classify a result as success, unavailable or failed.

  Parameters
  ----------
  errors : list
    Issues collected by build_full_scan_result.
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


def build_full_scan_result(
    cluster_ip,
    exit_code,
    stdout,
    stderr="",
    cluster_id=None,
    cluster_name=None,
    command=COMMAND,
    collected_at=None,
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
  command : str
    Command that was executed.
  collected_at : str, optional
    When the row was stored (the table's ``created_at``).

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

  if exit_code == 0:
    execution_status = EXECUTION_COMPLETED
  else:
    execution_status = EXECUTION_FAILED
    errors.append(build_issue(
      COMMAND_FAILED,
      f"'{command}' exited with code {exit_code} on {cluster_ip}.",
      "Run the command manually on a CVM of the cluster and check stderr "
      "and the Curator service status.",
    ))

  if _NO_MASTER_RE.search(combined):
    errors.append(build_issue(
      NO_CURATOR_MASTER,
      "Curator reported no master; scan status is unavailable.",
      "Check the Curator service with 'cluster status | grep -i curator' "
      "and review ~/data/logs/curator*.",
    ))
  if _PROTO_NOT_FOUND_RE.search(combined):
    errors.append(build_issue(
      SCAN_STAT_PROTO_NOT_FOUND,
      "Curator has no scan statistics recorded (scan stat proto not found).",
      "Curator may not have completed a scan since it started; wait for the "
      "next scheduled Full Scan and review ~/data/logs/curator*.",
    ))

  known_condition = any(
    issue["code"] in _CURATOR_CONDITION_CODES for issue in errors
  )
  if not stdout.strip():
    errors.append(build_issue(
      EMPTY_OUTPUT,
      f"'{command}' produced no stdout.",
      "Run the command manually on a CVM; confirm curator_cli is on the "
      "login shell PATH and Curator is running.",
    ))
  else:
    tables = parse_scan_tables(stdout)
    full_tables = [t for t in tables if get_job_name(t) == FULL_SCAN_JOB_NAME]
    if not tables:
      if not known_condition:
        errors.append(build_issue(
          MALFORMED_OUTPUT,
          "No scan tables found in the command output.",
          "Inspect command_result.raw_output; the curator_cli output "
          "format may have changed.",
        ))
    elif not full_tables:
      errors.append(build_issue(
        FULL_SCAN_NOT_FOUND,
        "No table with Job Name 'Full Scan' in the output; found "
        f"{[get_job_name(t) for t in tables]}. Partial and Selective scans "
        "are not used as a substitute.",
        "Confirm Curator has completed at least one Full Scan on this "
        "cluster.",
      ))
    else:
      if len(full_tables) > 1:
        errors.append(build_issue(
          MALFORMED_OUTPUT,
          f"Found {len(full_tables)} Full Scan tables; expected exactly one.",
          "Inspect command_result.raw_output for duplicated output.",
        ))
      full_scan, scan_errors = build_full_scan_details(full_tables[0])
      errors.extend(scan_errors)

  if not cluster_id:
    warnings.append(build_issue(
      CLUSTER_ID_UNRESOLVED,
      f"Cluster UUID for {cluster_ip} could not be resolved: not found in "
      "the clusters table.",
      "Configure the PE by its cluster virtual IP in endpoints.json and run "
      "the collector again so the clusters table is populated.",
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
    "collection_status": get_collection_status(errors, full_scan),
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

def build_command(command):
  """
  Shell command CliProcessor runs on the PE for the catalog command.

  curator_cli gives the same cluster-wide answer on every CVM, so it runs
  once, on the CVM CliProcessor logged in to (the endpoint IP from
  endpoints.json: the cluster virtual IP or a CVM IP). It runs in a login
  shell because curator_cli needs PATH and ZOOKEEPER_HOST_PORT_LIST from
  /etc/profile; without them newer AOS versions abort it.

  Parameters
  ----------
  command : str
    The curator_full_scan catalog command.

  Returns
  -------
  str
    The command to run over SSH.
  """
  return f"bash -lc '{command}'"


def split_output_by_cvm(output):
  """
  Split stored CLI output into per-CVM blocks.

  Rows stored before ``build_command`` existed hold the svmips loop output,
  with a marker line per CVM; current rows have no markers.

  Parameters
  ----------
  output : str
    Output stored by CliProcessor for the curator_full_scan command.

  Returns
  -------
  list
    ``(cvm_ip, text)`` tuples in output order; cvm_ip is None for output
    before the first marker.
  """
  blocks = []
  cvm_ip, lines = None, []
  for line in (output or "").splitlines():
    marker = _HOST_MARKER_RE.match(line.strip())
    if marker:
      if cvm_ip is not None or lines:
        blocks.append((cvm_ip, "\n".join(lines)))
      cvm_ip, lines = marker.group("ip"), []
      continue
    lines.append(line)
  if cvm_ip is not None or lines:
    blocks.append((cvm_ip, "\n".join(lines)))
  return blocks


def is_curator_answer(text):
  """
  Return whether a CVM block holds a curator_cli answer.

  Parameters
  ----------
  text : str
    Output of one CVM.

  Returns
  -------
  bool
    True when the block has a scan table or a Curator status message.
  """
  return bool(
    parse_scan_tables(text)
    or _MASTER_RE.search(text)
    or _NO_MASTER_RE.search(text)
    or _PROTO_NOT_FOUND_RE.search(text)
  )


def get_curator_answer(output):
  """
  Pick the curator_cli answer out of the stored output.

  Current rows hold one curator_cli answer. Older rows hold one block per
  CVM; for those the answer used is the first CVM whose output has a Full
  Scan table, else the first CVM with any Curator answer, else the first CVM
  with any output.

  Parameters
  ----------
  output : str
    Output stored by CliProcessor for the curator_full_scan command.

  Returns
  -------
  dict
    ``cvm_ip`` (the CVM whose answer is used; None when the output has no
    CVM markers) and ``stdout`` (that CVM's output).
  """
  blocks = [(cvm_ip, text) for cvm_ip, text in split_output_by_cvm(output)
            if text.strip()]
  has_full_scan = [
    block for block in blocks
    if any(get_job_name(t) == FULL_SCAN_JOB_NAME for t in parse_scan_tables(block[1]))
  ]
  answers = [block for block in blocks if is_curator_answer(block[1])]
  chosen = (has_full_scan or answers or blocks or [(None, "")])[0]
  return {"cvm_ip": chosen[0], "stdout": chosen[1]}


def build_full_scan_result_from_row(row, clusters=None):
  """
  Build the result document for one row of the curator_full_scan table.

  CliProcessor stores a row only when the command exited with 0, so the exit
  code of a stored row is 0.

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
    The build_full_scan_result document, plus ``cvm_ip`` (the CVM that answered; the
    endpoint IP for rows without CVM markers).
  """
  ip = row.get("ip")
  parsed = get_curator_answer(row.get("output"))
  identity = (clusters or {}).get(ip) or {}
  result = build_full_scan_result(
    cluster_ip=ip,
    exit_code=0,
    stdout=parsed["stdout"],
    cluster_id=identity.get("uuid"),
    cluster_name=identity.get("name") or row.get("cluster_name"),
    collected_at=row.get("created_at"),
  )
  result["cvm_ip"] = parsed["cvm_ip"] or (ip if parsed["stdout"] else None)
  return result
