from common.connection.sqliteworker import Sqlite3Worker
from common.connection.ssh_connect import Ssh
from common.logger.logger import EntryExit, setup_logger
from collectors.api_processor import ApiProcessor
from collectors import curator_full_scan
from common.exceptions.exceptions import *
from library.const import NUTANIX
from library.urls import CLUSTER
import os
import re
import json
from datetime import datetime, timedelta, timezone
from numbers import Number

LOGGER = setup_logger(__name__)


class CliProcessor:
  """
  CliProcessor is responsible for executing CLI commands on
  remote systems (PC/PE) via SSH and storing results in DB.
  """
  def __init__(self):
    """
    Initialize DB worker for storing CLI results.
    """
    try:
      base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
      db_path = os.path.join(base_dir, "metrics.db")
      self.db_worker = Sqlite3Worker(db_path)
      self.api_processor = ApiProcessor()
    except Exception as err:
      error = CZMonError(
        "Failed initializing CliProcessor",
        cause=err
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def _split_output_by_host(self, output):
    """
    Split command output into host-wise blocks based on markers:
    ================== <ip> =================
    """
    blocks = []
    current_host = "unknown"
    current_lines = []
    marker_re = re.compile(r"^=+\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s*=+$")

    for raw_line in (output or "").splitlines():
      line = raw_line.rstrip("\n")
      marker = marker_re.match(line.strip())
      if marker:
        if current_lines:
          blocks.append({
            "host": current_host,
            "lines": current_lines
          })
          current_lines = []
        current_host = marker.group(1)
        continue
      if line.strip():
        current_lines.append(line)

    if current_lines:
      blocks.append({
        "host": current_host,
        "lines": current_lines
      })

    return blocks

  @EntryExit
  def _normalize_header(self, header):
    """
    Normalize table header names into JSON-friendly keys.
    """
    key = (header or "").strip().lower()
    key = key.replace("%", "_pct")
    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
    return key or "col"

  @EntryExit
  def _coerce_value(self, header_key, value):
    """
    Try to coerce known numeric forms while preserving strings otherwise.
    """
    raw = (value or "").strip()
    if raw.endswith("%"):
      num = raw.rstrip("%").strip()
      if num.isdigit():
        return int(num)
    if header_key.endswith("_pct") and raw.isdigit():
      return int(raw)
    return raw

  @EntryExit
  def _parse_table_like_output(self, output):
    """
    Generic parser for table-like command output grouped by host.
    """
    rows = []
    host_blocks = self._split_output_by_host(output)
    for block in host_blocks:
      host = block.get("host", "unknown")
      lines = block.get("lines", [])
      header_idx = -1
      header_parts = []
      for idx, line in enumerate(lines):
        stripped = line.strip()
        # choose first line that looks like a tabular header
        if stripped and len(stripped.split()) >= 2:
          header_idx = idx
          header_parts = stripped.split()
          break
      if header_idx == -1 or not header_parts:
        continue

      headers = [self._normalize_header(h) for h in header_parts]
      for line in lines[header_idx + 1:]:
        stripped = line.strip()
        if not stripped:
          continue
        parts = stripped.split()
        if len(parts) < len(headers):
          continue
        # If row has more columns than headers, fold remainder into last column.
        if len(parts) > len(headers):
          parts = parts[:len(headers) - 1] + [" ".join(parts[len(headers) - 1:])]

        row = {"host": host}
        for idx, key in enumerate(headers):
          row[key] = self._coerce_value(key, parts[idx])
        rows.append(row)
    return rows

  @EntryExit
  def _parse_recovery_scalar_output(self, command, output):
    """
    Parse host-wise scalar recovery usage output (e.g. 27.33TB / 65%).
    """
    rows = []

    for block in self._split_output_by_host(output):
      host = block.get("host", "unknown")
      lines = block.get("lines", [])
      for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
          continue
        match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*(%|[kKmMgGtTpP][bB])$", line)
        if not match:
          continue
        value = float(match.group(1))
        unit = match.group(2).upper()
        metric_key = "recovery_points_usage_pct" if unit == "%" else "recovery_points_usage"
        rows.append(
          {
            "host": host,
            metric_key: value,
            "dimension": unit,
          }
        )
        break
    return rows

  @EntryExit
  def normalize_output(self, command, output):
    """
    Normalize command output for downstream UI/graph usage.
    """
    rows = self._parse_table_like_output(output)
    if rows:
      return {
        "parser": "table_like",
        "command": command,
        "rows": rows
      }
    recovery_rows = self._parse_recovery_scalar_output(command, output)
    if recovery_rows:
      return {
        "parser": "recovery_points_scalar",
        "command": command,
        "rows": recovery_rows
      }
    return {
      "parser": "raw_host_blocks",
      "command": command,
      "blocks": self._split_output_by_host(output)
    }

  @EntryExit
  def _extract_timeseries_rows(
      self,
      table_name,
      command,
      normalized_output,
      source_ip,
      source_cluster
  ):
    """
    Convert normalized CLI output into generic time-series rows.
    """
    timeseries_rows = []
    if not isinstance(normalized_output, dict):
      return timeseries_rows
    if normalized_output.get("parser") not in ("table_like", "recovery_points_scalar"):
      return timeseries_rows

    for row in normalized_output.get("rows", []):
      if not isinstance(row, dict):
        continue

      host_ip = str(row.get("host") or source_ip or "").strip()
      non_numeric_dimensions = {}
      for key, value in row.items():
        if key == "host":
          continue
        if isinstance(value, Number):
          continue
        text = str(value).strip()
        if text:
          non_numeric_dimensions[key] = text

      dimension_key = ""
      dimension_value = ""
      for preferred in ("mount", "filesystem", "device", "name"):
        if preferred in non_numeric_dimensions:
          dimension_key = preferred
          dimension_value = non_numeric_dimensions[preferred]
          break
      if not dimension_key and non_numeric_dimensions:
        dimension_key = next(iter(non_numeric_dimensions.keys()))
        dimension_value = non_numeric_dimensions[dimension_key]

      for metric_name, metric_value in row.items():
        if metric_name == "host" or not isinstance(metric_value, Number):
          continue
        timeseries_rows.append({
          "source_table": table_name,
          "command": command,
          "source_ip": source_ip,
          "source_cluster": source_cluster,
          "host_ip": host_ip,
          "metric_name": metric_name,
          "metric_value": float(metric_value),
          "dimension_key": dimension_key,
          "dimension_value": dimension_value,
        })
    return timeseries_rows

  @EntryExit
  def _persist_timeseries_rows(self, rows):
    """
    Persist extracted time-series rows to cli_timeseries table.
    """
    if not rows:
      return
    table_name = "cli_timeseries"
    for row in rows:
      self.db_worker.ensure_schema(table_name, row)
      self.db_worker.insert_row(table_name, row)

  @EntryExit
  def _build_remote_command(self, command, fan_out=True):
    """
    Wrap a catalog command for remote execution.

    With fan_out the command runs on every SVM returned by svmips, each
    block prefixed with a host marker. Without it the command runs once in a
    login shell on the target, for cluster-wide commands such as curator_cli.
    """
    if fan_out:
      return (f"bash -lc 'for i in $(svmips); "
              f"do echo \"================== $i "
              f"=================\"; ssh $i "
              f"{command}; done'")
    return f"bash -lc '{command}'"

  @EntryExit
  def _get_cluster_timezone(self, ip):
    """
    Return the IANA timezone recorded for the cluster at ip, or None.
    """
    if "timezone" not in self.db_worker.get_columns(CLUSTER):
      return None
    return self.api_processor.fetch_dynamic_values(
      f"$({CLUSTER}#timezone#clusterExternalIPAddress={ip})"
    )

  @EntryExit
  def _lookup_cluster_identity(self, cluster_ip):
    """
    Return the cluster UUID and name recorded for cluster_ip in the clusters
    table; values are None when no cluster has that external IP.
    """
    return self.api_processor.fetch_dynamic_values({
      "cluster_id": f"$({CLUSTER}#uuid#clusterExternalIPAddress={cluster_ip})",
      "cluster_name": f"$({CLUSTER}#name#clusterExternalIPAddress={cluster_ip})",
    })

  @EntryExit
  def _cluster_vip_from_cvm(self, ssh_obj, ip, timeout=None):
    """
    Read the cluster virtual IP from the CVM's cluster configuration.

    Used when the configured IP is not a cluster external IP (for example a
    CVM IP). Returns None when it cannot be determined.
    """
    try:
      exit_code, stdout, _ = ssh_obj.execute_with_status(
        self._build_remote_command(
          curator_full_scan.CLUSTER_VIP_COMMAND, fan_out=False
        ),
        timeout=timeout,
      )
    except Exception as err:
      LOGGER.error(CZMonError(
        "Failed reading cluster virtual IP from CVM",
        cause=err,
        context={"ip": ip}
      ))
      return None
    if exit_code != 0:
      return None
    return curator_full_scan.parse_cluster_external_ip(stdout)

  @EntryExit
  def _latest_matching_curator_row(self, table_name, key_values, row):
    """
    Return the rowid of the cluster's latest Curator row if it holds the same
    result as row (same scan, statuses and errors), otherwise None.
    """
    where = " AND ".join(f"{col} IS ?" for col in key_values)
    columns = curator_full_scan.RESULT_IDENTITY_COLUMNS
    latest = self.db_worker.execute(
      f"SELECT rowid, {', '.join(columns)} FROM {table_name} "
      f"WHERE {where} ORDER BY rowid DESC LIMIT 1",
      list(key_values.values()),
    )
    if not latest:
      return None
    rowid, *stored = latest[0]
    if list(stored) != [row[col] for col in columns]:
      return None
    return rowid

  @EntryExit
  def _expire_curator_rows(self, table_name, key_values, retention_days):
    """
    Delete this cluster's Curator rows written more than retention_days ago.
    """
    cutoff = (
      datetime.now(timezone.utc) - timedelta(days=float(retention_days))
    ).isoformat()
    where = " AND ".join(f"{col} IS ?" for col in key_values)
    self.db_worker.execute(
      f"DELETE FROM {table_name} WHERE {where} AND created_at < ?",
      list(key_values.values()) + [cutoff],
    )

  @EntryExit
  def _refresh_curator_row(self, table_name, rowid, row):
    """
    Overwrite an existing Curator row with the latest collection of the same
    result, refreshing created_at so time-range filters see it.
    """
    values = dict(row)
    values["created_at"] = datetime.now(timezone.utc).isoformat()
    assignments = ", ".join(f"{col} = ?" for col in values)
    self.db_worker.execute(
      f"UPDATE {table_name} SET {assignments} WHERE rowid = ?",
      list(values.values()) + [rowid],
    )

  @EntryExit
  def _collect_curator_full_scan(
      self, ssh_obj, ip, command, table_name, fan_out=False, timeout=None,
      connect_error=None,
      retention_count=curator_full_scan.DEFAULT_RETENTION_COUNT,
      retention_days=curator_full_scan.DEFAULT_RETENTION_DAYS):
    """
    Collect Curator Full Scan status for one cluster and store it.

    A result that differs from the cluster's latest row is inserted as a new
    row; a result identical to the latest row (same scan, statuses and
    errors) refreshes that row instead, so repeated collections of one scan
    do not create duplicate entries. Afterwards this cluster's rows older
    than retention_days are deleted and at most retention_count are kept.

    Parameters
    ----------
    ssh_obj : Ssh or None
      Connected session, or None when connect_error is set.
    ip : str
      Framework target IP.
    command : str
      Catalog command.
    table_name : str
      Metric table.
    fan_out : bool
      Passed to _build_remote_command.
    timeout : float, optional
      Seconds to wait for the command.
    connect_error : str, optional
      Error text when the framework could not connect to the target.
    retention_count : int
      Maximum number of rows to keep per cluster.
    retention_days : int
      Rows written more than this many days ago are deleted.

    Returns
    -------
    dict
      The curator_full_scan result document.
    """
    exit_code, stdout, stderr = None, "", ""
    execution_error = connect_error
    execution_error_code = curator_full_scan.TARGET_UNREACHABLE
    if connect_error is None:
      full_command = self._build_remote_command(command, fan_out)
      try:
        exit_code, stdout, stderr = ssh_obj.execute_with_status(
          full_command, timeout=timeout
        )
        LOGGER.info(
          "Output for command '%s' on %s (exit=%s): %s",
          full_command, ip, exit_code, stdout
        )
      except CZMonTimeoutError as err:
        execution_error = str(err)
        execution_error_code = curator_full_scan.COMMAND_TIMEOUT
      except Exception as err:
        execution_error = str(err)
        execution_error_code = curator_full_scan.SSH_EXECUTION_FAILED
    cluster_vip = ip
    identity = self._lookup_cluster_identity(ip)
    if not identity.get("cluster_id"):
      cluster_vip = None
      if ssh_obj is not None:
        cluster_vip = self._cluster_vip_from_cvm(ssh_obj, ip, timeout)
      if cluster_vip and cluster_vip != ip:
        identity = self._lookup_cluster_identity(cluster_vip)
    result = curator_full_scan.build_result(
      cluster_ip=ip,
      exit_code=exit_code,
      stdout=stdout,
      stderr=stderr,
      cluster_id=identity.get("cluster_id"),
      cluster_name=identity.get("cluster_name"),
      cluster_timezone=self._get_cluster_timezone(cluster_vip or ip),
      cluster_vip=cluster_vip,
      command=command,
      execution_error=execution_error,
      execution_error_code=execution_error_code,
    )
    row = curator_full_scan.to_db_row(result)
    key_values = {
      col: row[col] for col in curator_full_scan.RECORD_KEY_COLUMNS
    }
    self.db_worker.ensure_schema(table_name, row)
    latest_rowid = self._latest_matching_curator_row(table_name, key_values, row)
    if latest_rowid is None:
      self.db_worker.insert_row(table_name, row)
    else:
      self._refresh_curator_row(table_name, latest_rowid, row)
    self._expire_curator_rows(table_name, key_values, retention_days)
    self.db_worker.trim_rows(table_name, key_values, retention_count)
    if result["collection_status"] != curator_full_scan.COLLECTION_SUCCESS:
      LOGGER.error(CZMonError(
        "Curator full scan not collected successfully",
        context={
          "ip": ip,
          "collection_status": result["collection_status"],
          "errors": [issue["code"] for issue in result["errors"]],
        }
      ))
    return result

  @EntryExit
  def process_data(self, config):
    """
    Execute CLI commands for given configuration and persist results.

    Parameters
    ----------
    config : dict
      Configuration containing table name, endpoint type,
      and list of commands to execute.
    """
    try:
      for table_name, entity_data in config.items():
        endpoint_type = entity_data.get("endpoint_type")
        commands = entity_data.get("command", [])
        fan_out = entity_data.get("fan_out", True)
        parser = entity_data.get("parser")
        timeout = entity_data.get("timeout_secs")
        retention_count = entity_data.get(
          "retention_count", curator_full_scan.DEFAULT_RETENTION_COUNT
        )
        retention_days = entity_data.get(
          "retention_days", curator_full_scan.DEFAULT_RETENTION_DAYS
        )
        ip_endpoints = [
          (ip, et)
          for et in endpoint_type
          for ip in os.environ.get(f"{et}_IPS", "").split(",")
          if ip
        ]
        for ip, current_endpoint_type in ip_endpoints:
          try:
            ssh_obj = Ssh(ip, NUTANIX)
            for command in commands:
              try:
                if parser == curator_full_scan.METRIC_NAME:
                  self._collect_curator_full_scan(
                    ssh_obj, ip, command, table_name, fan_out, timeout,
                    retention_count=retention_count,
                    retention_days=retention_days
                  )
                  continue
                values = {}
                full_command = self._build_remote_command(command, fan_out)
                output = ssh_obj.execute(full_command)
                LOGGER.info(
                  "Output for command '%s' on %s: %s",
                  full_command, ip, output
                )
                normalized_output = self.normalize_output(command, output)
                values["command"] = command
                values["output"] = output
                values["output_json"] = json.dumps(
                  normalized_output
                )
                values["ip"] = ip
                values["cluster_name"] = (
                  f"$({CLUSTER}#name#clusterExternalIPAddress={ip})"
                )
                values = self.api_processor.fetch_dynamic_values(values)
                self.db_worker.ensure_schema(table_name, values)
                self.db_worker.insert_row(table_name, values)
                timeseries_rows = self._extract_timeseries_rows(
                  table_name=table_name,
                  command=command,
                  normalized_output=normalized_output,
                  source_ip=ip,
                  source_cluster=values.get("cluster_name", "")
                )
                self._persist_timeseries_rows(timeseries_rows)
              except Exception as cmd_err:
                error = CZMonError(
                  "Command execution failed",
                  cause=cmd_err,
                  context={
                    "ip": ip,
                    "command": command,
                    "table": table_name
                  }
                )
                LOGGER.error(error)
                continue
          except Exception as ssh_err:
            error = CZMonError(
              "SSH connection failed",
              cause=ssh_err,
              context={
                "ip": ip,
                "endpoint_type": endpoint_type
              }
            )
            LOGGER.error(error)
            if parser == curator_full_scan.METRIC_NAME:
              for command in commands:
                try:
                  self._collect_curator_full_scan(
                    None, ip, command, table_name, fan_out, timeout,
                    connect_error=str(ssh_err),
                    retention_count=retention_count,
                    retention_days=retention_days
                  )
                except Exception as persist_err:
                  LOGGER.error(CZMonError(
                    "Failed recording unreachable target",
                    cause=persist_err,
                    context={"ip": ip, "table": table_name}
                  ))
            continue
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "CLI processing failed",
        cause=err
      )
      LOGGER.error(error)
      raise error
