"""
Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands
and executes them locally on the system, parsing and routing output to the DB.
"""

import os
import json
import subprocess
import sys
from common.logger.logger import EntryExit, setup_logger
from common.exceptions.exceptions import CZMonError
from common.connection.sqliteworker import Sqlite3Worker
from collectors.api_processor import ApiProcessor

LOGGER = setup_logger(__name__)

class LocalProcessor:
  """
  LocalProcessor is responsible for executing local CLI commands
  and persisting the output into a SQLite database using Sqlite3Worker.
  """
  def __init__(self):
    """
    Initialize LocalProcessor with database worker and configurations.
    """
    try:
      self.base_dir = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
      )
      db_path = os.path.join(self.base_dir, "metrics.db")
      self.db_worker = Sqlite3Worker(db_path)
      self.api_processor = ApiProcessor()
      self.config_path = os.path.join(
        self.base_dir, "static", "configurations", "local_cli_catalog.json"
      )
    except Exception as err:
      error = CZMonError(
        "Failed initializing LocalProcessor",
        cause=err
      )
      LOGGER.error(error)
      raise error

  def _looks_like_python(self, value):
    """Return True when a catalog command part names a Python interpreter."""
    name = os.path.basename(str(value)).lower()
    return name in {"python", "python3"} or name.startswith("python3.")

  def _resolve_path(self, value):
    """Resolve a catalog path against this checkout when the stored path is missing."""
    if os.path.exists(value):
      return value
    if os.path.isabs(value):
      script_candidate = os.path.join(
        self.base_dir, "collectors", "scripts", os.path.basename(value)
      )
      if os.path.exists(script_candidate):
        return script_candidate
    relative_candidate = os.path.join(self.base_dir, value)
    if os.path.exists(relative_candidate):
      return relative_candidate
    return value

  def _resolve_command(self, command):
    """
    Bind catalog commands to the current repo and interpreter.

    Absolute paths like /home/nutanix/CZMon/... only work on one layout.
    Missing interpreter or script paths fall back to sys.executable and
    collectors/scripts under this checkout.
    """
    if not command:
      return []

    parts = list(command)
    if self._looks_like_python(parts[0]):
      interpreter = parts[0] if os.path.isfile(parts[0]) else sys.executable
      rest = parts[1:]
    else:
      interpreter = sys.executable
      rest = parts

    return [interpreter] + [self._resolve_path(part) for part in rest]

  @EntryExit
  def process_data(self):
    """
    Execute commands from catalog and persist output to DB dynamically.
    """
    try:
      catalog = self.api_processor.load_config(self.config_path)
      if not catalog:
        return

      custom_env = os.environ.copy()
      custom_env["PYTHONPATH"] = self.base_dir

      for table_name, task_info in catalog.items():
        command = self._resolve_command(task_info.get("command", []))
        desc = task_info.get("description", "No description")

        LOGGER.info(f"Running Task: {table_name} ({desc})")
        LOGGER.info(f"Resolved command: {command}")
        try:
          result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=self.base_dir,
            env=custom_env
          )

          stdout_text = result.stdout.strip() if result.stdout else ""
          stderr_text = result.stderr.strip() if result.stderr else ""

          if stderr_text:
            LOGGER.info(f"Script Logs/Errors:\n{stderr_text}")

          if stdout_text:
            LOGGER.info(f"Script Output:\n{stdout_text}")

            try:
              data = json.loads(stdout_text)
              if isinstance(data, dict):
                for ip_key, payload in data.items():
                  values = {
                    "ip_address": ip_key,
                    "status_data": (
                      json.dumps(payload) 
                      if isinstance(payload, dict) else str(payload)
                    )
                  }
                  self.db_worker.ensure_schema(table_name, values)
                  self.db_worker.insert_row(table_name, values)
              else:
                raise ValueError("Parsed JSON is not a dictionary.")
            except Exception:
              # Fallback for plain text or differently shaped outputs
              values = {
                "command": str(command),
                "output_json": stdout_text,
                "error_msg": stderr_text
              }
              self.db_worker.ensure_schema(table_name, values)
              self.db_worker.insert_row(table_name, values)

        except Exception as cmd_err:
          error = CZMonError(
            "Local command execution failed",
            cause=cmd_err,
            context={"command": command, "table": table_name}
          )
          LOGGER.error(error)
          continue

    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError("Local CLI processing failed", cause=err)
      LOGGER.error(error)
      raise error

def run_local_commands():
  """
  Wrapper function to instantiate and run the LocalProcessor class.
  Allows seamless integration with runner.py.
  """
  try:
    processor = LocalProcessor()
    processor.process_data()
  except Exception as err:
    error = CZMonError("Fatal error executing local commands", cause=err)
    LOGGER.error(error)
    raise error

if __name__ == "__main__":
  run_local_commands()

