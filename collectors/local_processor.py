"""
Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands
and executes them locally on the system, parsing and routing output to the DB.
"""

import os
import json
import subprocess
from common.logger.logger import EntryExit, setup_logger
from common.exceptions.exceptions import CZMonError
from common.connection.sqliteworker import Sqlite3Worker

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

  @EntryExit
  def load_config(self, config_path):
    """
    Load configuration from JSON file.

    Args:
      config_path (str): Path to configuration JSON file.

    Returns:
      dict: Parsed configuration.
    """
    try:
      with open(config_path, "r") as f:
        return json.load(f)
    except Exception as err:
      error = CZMonError(
        "Failed loading config file",
        cause=err,
        context={"config_path": config_path}
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def process_data(self):
    """
    Execute commands from catalog and persist output to DB dynamically.
    """
    try:
      catalog = self.load_config(self.config_path)
      if not catalog:
        return

      custom_env = os.environ.copy()
      custom_env["PYTHONPATH"] = self.base_dir

      for table_name, task_info in catalog.items():
        command = task_info.get("command", [])
        desc = task_info.get("description", "No description")

        LOGGER.info(f"Running Task: {table_name} ({desc})")
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

