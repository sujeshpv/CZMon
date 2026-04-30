from common.connection.sqliteworker import Sqlite3Worker
from common.connection.ssh_connect import Ssh
from common.logger.logger import EntryExit, setup_logger
from collectors.api_processor import ApiProcessor
from common.exceptions.exceptions import *
from library.const import NUTANIX
from library.urls import CLUSTER
import os
import re
import json
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
        system_config = {
          "ips": os.environ.get(f"{endpoint_type}_IPS", "").split(",")
        }
        for ip in system_config.get("ips"):
          try:
            ssh_obj = Ssh(ip, NUTANIX)
            for command in commands:
              try:
                values = {}
                full_command = (f"bash -lc 'for i in $(svmips); "
                                f"do echo \"================== $i "
                                f"=================\"; ssh $i "
                                f"{command}; done'")
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
