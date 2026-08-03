"""Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands,
executes them locally as subprocesses, captures their stdout/stderr, and 
dynamically routes the resulting JSON payload into a SQLite database.
"""

import json
import os
import subprocess

from common.connection.sqliteworker import Sqlite3Worker
from common.exceptions.exceptions import CZMonError
from common.logger.logger import EntryExit, setup_logger

LOGGER = setup_logger(__name__)

class LocalProcessor:
  """Executes local CLI commands and persists output dynamically via Sqlite3Worker.

  Attributes:
    base_dir (str): The root directory of the CZMon project.
    config_path (str): The absolute path to the local CLI catalog JSON.
    db_worker (Sqlite3Worker): The worker managing SQLite database operations.
  """

  def __init__(self):
    """Initializes the LocalProcessor with database workers and config paths.

    Raises:
      CZMonError: If paths cannot be resolved or the database worker fails to start.
    """
    try:
      # Dynamically determine the base directory of the project (e.g., ~/CZMon)
      self.base_dir = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
      )

      db_path = os.path.join(self.base_dir, "metrics.db")
      self.db_worker = Sqlite3Worker(db_path)

      self.config_path = os.path.join(
        self.base_dir, "static", "configurations", "local_cli_catalog.json"
      )
    except Exception as err:
      error = CZMonError("Failed initializing LocalProcessor", cause=err)
      LOGGER.error(error)
      raise error

  @EntryExit
  def load_config(self, config_path: str) -> dict:
    """Loads and parses a JSON configuration file.

    Args:
      config_path (str): The absolute path to the JSON configuration file.

    Returns:
      dict: The parsed JSON configuration data.

    Raises:
      CZMonError: If the file is missing or contains invalid JSON.
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
    """Executes commands from the catalog and persists output to the database.

    This iterates over every registered task, executes it in a subprocess, 
    captures the standard output, and dynamically ensures a database schema 
    exists before inserting the parsed JSON results.
    """
    try:
      catalog = self.load_config(self.config_path)
      if not catalog:
        return

      # Embed the project root into the PYTHONPATH so subprocesses can resolve modules
      custom_env = os.environ.copy()
      custom_env["PYTHONPATH"] = self.base_dir

      for table_name, task_info in catalog.items():
        command = task_info.get("command", [])
        desc = task_info.get("description", "No description provided")

        LOGGER.info(f"Running Task: {table_name} ({desc})")

        try:
          # Execute the command dynamically from the project root
          result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=self.base_dir,
            env=custom_env
          )

          stdout_text = result.stdout.strip() if result.stdout else ""
          stderr_text = result.stderr.strip() if result.stderr else ""

          # Log standard errors or debug info (like our Python logging.info prints)
          if stderr_text:
            LOGGER.info(f"Script Logs/Errors:\n{stderr_text}")

          # Process the actual JSON payload returned by the script
          if stdout_text:
            LOGGER.info(f"Script Output:\n{stdout_text}")

            try:
              data = json.loads(stdout_text)

              # If the output is a clean dictionary, insert each IP as a distinct row
              if isinstance(data, dict):
                for ip_key, payload in data.items():
                  values = {
                    "ip_address": ip_key,
                    "status_data": (
                      json.dumps(payload)
                      if isinstance(payload, dict) else str(payload)
                    )
                  }
                  # Dynamically build the table and insert the data
                  self.db_worker.ensure_schema(table_name, values)
                  self.db_worker.insert_row(table_name, values)
              else:
                raise ValueError("Parsed JSON payload is not a dictionary.")

            except Exception:
              # Fallback for plain text or unexpectedly shaped outputs
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
  """Wrapper function to instantiate and execute the LocalProcessor.

  This provides a clean entry point for seamless integration with runner.py
  or for standalone execution from the command line.
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

