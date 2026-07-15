import json
import subprocess
import os

CONFIG_PATH = "/home/nutanix/CZMon/static/configurations/local_cli_catalog.json"

def run_local_scripts():
    print(f"Reading local catalog from {CONFIG_PATH}...")
    try:
        with open(CONFIG_PATH, 'r') as f:
            catalog = json.load(f)
    except Exception as e:
        print(f"Failed to load catalog: {e}")
        return

    # Add the main project folder to PYTHONPATH so the scripts can find Django!
    custom_env = os.environ.copy()
    custom_env["PYTHONPATH"] = "/home/nutanix/CZMon"

    for task_name, task_info in catalog.items():
        command = task_info.get("command", [])
        desc = task_info.get("description", "No description")

        print(f"\n--- Running Task: {task_name} ({desc}) ---")
        try:
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                cwd="/home/nutanix/CZMon",
                env=custom_env  # <-- This passes the PYTHONPATH fix to the script
            )
            if result.stdout:
                print(result.stdout.strip())
            if result.stderr:
                print(f"ERROR: {result.stderr.strip()}")
        except Exception as e:
            print(f"Failed to execute command: {e}")

if __name__ == "__main__":
    run_local_scripts()
