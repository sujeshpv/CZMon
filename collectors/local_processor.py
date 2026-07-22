"""
Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands
and executes them locally on the system. It handles both standard shell
commands and local Python scripts, capturing standard output and error.
"""

import os
import json
import subprocess
from common.logger.logger import setup_logger
from common.exceptions.exceptions import CZMonError

import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
django.setup()
from coreapp.models import VmCountPerHost

LOGGER = setup_logger(__name__)

# Dynamically determine the base directory of the project (e.g., ~/CZMon)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(
  BASE_DIR, "static", "configurations", "local_cli_catalog.json"
)

def run_local_commands():
  LOGGER.info(f"Reading local catalog from {CONFIG_PATH}...")
  catalog = None
  try:
    with open(CONFIG_PATH, 'r') as f:
      catalog = json.load(f)
  except Exception as e:
    LOGGER.error(f"Failed to load catalog: {e}")

  if not catalog:
    return

  # Add main project folder to PYTHONPATH so commands can find Django
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

      # Script errors or logger outputs go to stderr
      if result.stderr:
        LOGGER.info(f"Script Logs/Errors:\n{result.stderr.strip()}")

      # Script JSON data goes to stdout
      if result.stdout:
        stdout_text = result.stdout.strip()
        LOGGER.info(f"Script Output:\n{stdout_text}")

        # --- ADDED: Database Saving Logic for our script ---
        target_tasks = ["check_vm_power_states", "get_vm_count"]
        if task_name in target_tasks:
          try:
            data = json.loads(stdout_text)
            for ip, result_dict in data.items():
              cluster_name = result_dict.get("Cluster_name", ip)
              VmCountPerHost.objects.update_or_create(
                cluster_ip=ip,
                defaults={
                  "cluster_name": cluster_name,
                  "status_data": result_dict,
                }
              )
            LOGGER.info(f"Saved {task_name} data to the database.")
          except json.JSONDecodeError:
            LOGGER.error(
              f"Failed to parse JSON from {task_name}. "
              "Ensure the script prints pure JSON."
            )
          except Exception as db_err:
            LOGGER.error(
              f"Database error saving {task_name}: {db_err}"
            )

    except Exception as e:
      LOGGER.error(f"Failed to execute command: {e}")

if __name__ == "__main__":
  try:
    run_local_commands()
  except Exception as err:
    error = CZMonError(
      "Fatal error executing local commands",
      cause=err
    )
    LOGGER.error(error)
    raise error

