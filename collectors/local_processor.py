"""
Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands
and executes them locally on the system. It handles both standard shell
commands and local Python scripts, capturing their standard output and error.
"""

import json
import subprocess
import os
import logging

# Ensure you import and configure the LOGGER as done in other processor files.
# This is a standard fallback if there's no custom logger module.
LOGGER = logging.getLogger(__name__)

# Dynamically determine the base directory of the project (e.g., ~/CZMon)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "static", "configurations", "local_cli_catalog.json")

def run_local_commands():
  LOGGER.info(f"Reading local catalog from {CONFIG_PATH}...")
  try:
    with open(CONFIG_PATH, 'r') as f:
      catalog = json.load(f)
  except Exception as e:
    LOGGER.error(f"Failed to load catalog: {e}")
    catalog = None

  if catalog:
    # Add the main project folder to PYTHONPATH so the commands can find Django
    custom_env = os.environ.copy()
    custom_env["PYTHONPATH"] = BASE_DIR

    for task_name, task_info in catalog.items():
      command = task_info.get("command", [])
      desc = task_info.get("description", "No description")

      LOGGER.info(f"\n--- Running Task: {task_name} ({desc}) ---")
      try:
        # Execute the command dynamically from the BASE_DIR
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=BASE_DIR,
            env=custom_env
        )
        if result.stdout:
          LOGGER.info(result.stdout.strip())
        if result.stderr:
          LOGGER.error(f"ERROR: {result.stderr.strip()}")
      except Exception as e:
        LOGGER.error(f"Failed to execute command: {e}")

def main():
  run_local_commands()

if __name__ == "__main__":
  main()


