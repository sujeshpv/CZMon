"""
Executes local commands defined in the local CLI catalog configuration.

This module reads a JSON configuration file containing a catalog of commands
and executes them locally on the system, parsing and routing output to the DB.
"""

import os
import json
import subprocess
from common.logger.logger import setup_logger
from common.exceptions.exceptions import CZMonError

LOGGER = setup_logger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(
  BASE_DIR, "static", "configurations", "local_cli_catalog.json"
)

def save_to_db_generically(db_model_str, data):
  """Dynamically loads Django and saves data without hardcoding imports."""
  import django
  import django.apps
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
  if not django.apps.apps.ready:
    django.setup()

  # Dynamically fetch the model class using its string name!
  from django.apps import apps
  ModelClass = apps.get_model('coreapp', db_model_str)

  for ip, result_dict in data.items():
    if db_model_str == "VmCountPerHost":
      ModelClass.objects.update_or_create(
        cluster_ip=ip,
        defaults={
          "cluster_name": result_dict.get("Cluster_name", ip),
          "status_data": result_dict,
        }
      )
    elif db_model_str == "AhvHomeUsage":
      for cluster_name, hosts_data in result_dict.items():
        ModelClass.objects.update_or_create(
          cluster_name=cluster_name,
          defaults={"status_data": hosts_data}
        )

def run_local_commands():
  LOGGER.info(f"Reading local catalog from {CONFIG_PATH}...")
  try:
    with open(CONFIG_PATH, 'r') as f:
      catalog = json.load(f)
  except Exception as e:
    LOGGER.error(f"Failed to load catalog: {e}")
    return

  if not catalog:
    return

  custom_env = os.environ.copy()
  custom_env["PYTHONPATH"] = BASE_DIR

  for task_name, task_info in catalog.items():
    command = task_info.get("command", [])
    desc = task_info.get("description", "No description")
    db_model_str = task_info.get("db_model")

    LOGGER.info(f"\n--- Running Task: {task_name} ({desc}) ---")
    try:
      result = subprocess.run(
        command, capture_output=True, text=True, cwd=BASE_DIR, env=custom_env
      )

      if result.stderr:
        LOGGER.info(f"Script Logs/Errors:\n{result.stderr.strip()}")

      if result.stdout:
        stdout_text = result.stdout.strip()
        LOGGER.info(f"Script Output:\n{stdout_text}")

        # --- Generic Database Saving ---
        if db_model_str:
          try:
            save_to_db_generically(db_model_str, json.loads(stdout_text))
            LOGGER.info(f"Saved {task_name} data to {db_model_str} table.")
          except Exception as db_err:
            LOGGER.error(f"Database error saving {task_name}: {db_err}")
        # -------------------------------

    except Exception as e:
      LOGGER.error(f"Failed to execute command: {e}")

if __name__ == "__main__":
  try:
    run_local_commands()
  except Exception as err:
    error = CZMonError("Fatal error executing local commands", cause=err)
    LOGGER.error(error)
    raise error

