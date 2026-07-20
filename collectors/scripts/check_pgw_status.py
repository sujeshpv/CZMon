"""Verifies Prism Gateway (PGW) status and saves to the database."""

import json
import requests
import urllib3

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def verify_pgw_status(ip: str, username: str, password: str) -> tuple:
  """Checks the Prism Gateway heartbeat status.
  Returns a tuple of (status_data_dict, is_online_boolean).
  """
  url = f"https://{ip}:9440/PrismGateway/services/rest/v1/heartbeat"
  auth = (username, password)

  try:
    response = requests.get(
      url,
      auth=auth,
      verify=False,
      timeout=10,
      headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()
    # Return the JSON data, and True (online)
    return response.json(), True
  except Exception as e:
    # Return the error string, and False (offline)
    return {"error": str(e)}, False

def run_pgw_collection(config_path=None):
  """Reads endpoints from config and persists PGW status to DB."""

  # MOVE THE IMPORTS HERE!
  import os
  from django.conf import settings
  from coreapp.models import PrismGatewayStatus

  # Automatically find the endpoints.json file in the Django project
  if not config_path:
    config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

  try:
    with open(config_path, 'r') as f:
      config_data = json.load(f)
  except Exception as e:
    print(f"Failed to load config: {str(e)}")
    return

  # Extract all endpoints from the zones (Matches the new endpoints.json format!)
  all_endpoints = [entry for zone in config_data.values() for entry in zone]

  for endpoint in all_endpoints:
    ip = endpoint.get("ip") or endpoint.get("virtual_ip")
    creds = endpoint.get("credentials", {})
    user = creds.get("user", "admin")
    pwd = creds.get("password", "Nutanix.123")

    if not ip:
      continue

    # Get the data from the API
    status_data, is_online = verify_pgw_status(ip, user, pwd)

    # Save or update the data in the Django database table
    obj, created = PrismGatewayStatus.objects.update_or_create(
      ip_address=ip,
      defaults={
        "is_online": is_online,
        "status_data": status_data,
      }
    )

    action = "Created" if created else "Updated"
    status_text = "Online" if is_online else "Offline"
    print(f"{action} DB record for {ip} -> {status_text}")

if __name__ == "__main__":
  # Setup Django environment so it can run standalone
  import os
  import django
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
  django.setup()

  run_pgw_collection()

