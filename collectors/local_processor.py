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
    """Initialize LocalProcessor with database worker and configurations.
    
    Sets up the SQLite database connection, API processor for config loading,
    and resolves paths to the local CLI catalog configuration file.
    
    Raises:
      CZMonError: If initialization of database worker or config paths fails.
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
    """Check if a command part represents a Python interpreter.
    
    Identifies Python interpreter references like 'python', 'python3', or
    version-specific variants like 'python3.9', 'python3.11', etc.
    
    Args:
      value: The command part to check (typically the first element).
      
    Returns:
      bool: True if the value appears to be a Python interpreter name,
        False otherwise.
    """
    name = os.path.basename(str(value)).lower()
    return name in {"python", "python3"} or name.startswith("python3.")

  def _resolve_path(self, value):
    """Resolve a catalog path to work with the current checkout location.
    
    Makes paths portable across different server checkouts by:
    1. Returning the path as-is if it already exists
    2. For absolute paths (like /home/nutanix/CZMon/...), trying to find
       the script basename under collectors/scripts/
    3. Treating the value as relative to the base directory
    4. Falling back to the original value if none of the above work
    
    Args:
      value: The path string from the catalog configuration.
      
    Returns:
      str: The resolved absolute path, or the original value if resolution fails.
    """
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
    """Bind catalog commands to the current repository and Python interpreter.
    
    Transforms catalog commands to work on any server checkout by:
    1. Detecting if the command starts with a Python interpreter reference
    2. Resolving the interpreter to sys.executable if it's missing
    3. Resolving all script paths to the current checkout location
    
    This makes commands portable - catalog entries like:
      ["python3", "collectors/scripts/check_pgw_status.py"]
    work on any checkout location, not just hardcoded paths like
      ["/home/nutanix/CZMon/venv/bin/python3", "/home/nutanix/CZMon/..."]
    
    Args:
      command (list): The command array from the catalog configuration.
      
    Returns:
      list: A resolved command array with absolute paths bound to the
        current checkout, ready for subprocess execution.
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
    """Execute all commands from the local CLI catalog and persist results to the database.
    
    Loads the local_cli_catalog.json configuration, executes each command defined
    in the catalog, captures stdout/stderr, parses JSON output, and dynamically
    creates database tables with appropriate schemas to store the results.
    
    The function handles two output formats:
    1. Structured JSON: Creates rows with ip_address and status_data columns
    2. Plain text/errors: Creates rows with command, output_json, error_msg columns
    
    Each catalog entry becomes a database table, with the table name matching
    the catalog key (e.g., 'check_pgw_status' table for that catalog entry).
    
    Raises:
      CZMonError: If catalog loading fails or critical processing errors occur.
        Individual command failures are logged but don't stop processing.
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
  """Execute local CLI commands defined in the catalog configuration.
  
  Entry point function that instantiates LocalProcessor and triggers
  command execution. This function is called by runner.py when
  --run-type local_cli is specified.
  
  The function provides a clean interface for the runner module and
  ensures proper error handling and logging for the local command
  execution workflow.
  
  Raises:
    CZMonError: If LocalProcessor initialization or execution fails.
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

