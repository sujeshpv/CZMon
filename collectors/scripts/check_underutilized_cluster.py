"""Checks if a Nutanix cluster is underutilized based on Memory usage.

This script reads cluster endpoints from a config file, connects to the 
Nutanix Prism API to gather CPU, Memory, and IOPS statistics, and raises 
an alert if memory utilization is exceptionally low.
"""

import argparse
import json
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Threshold for underutilization alert
MEMORY_UNDERUTILIZATION_THRESHOLD_PCT = 20.0 

def check_cluster_utilization(cluster_ip: str, username: str, password: str) -> dict:
    """Connects to Nutanix Prism API to get utilization stats.

    Returns a dictionary with cpu_usage_percent, memory_usage_percent, and iops.
    """
    base_url = f"https://{cluster_ip}:9440/api/nutanix/v2.0"
    auth = (username, password)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        resp = requests.get(
            f"{base_url}/cluster", auth=auth, headers=headers, verify=False, timeout=15
        )
        resp.raise_for_status()
        cluster_data = resp.json()
        stats = cluster_data.get("stats", {})

        # ppm is parts per million. Divide by 10,000 to get percentage.
        cpu_ppm = float(stats.get("hypervisor_cpu_usage_ppm", 0))
        mem_ppm = float(stats.get("memory_usage_ppm", 0))

        cpu_pct = round(cpu_ppm / 10000.0, 2)
        mem_pct = round(mem_ppm / 10000.0, 2)
        iops = round(float(stats.get("controller_num_iops", 0)), 2)

        return {
            "cluster_ip": cluster_ip,
            "cpu_usage_percent": cpu_pct,
            "memory_usage_percent": mem_pct,
            "iops": iops
        }

    except requests.exceptions.RequestException as e:
        return {"cluster_ip": cluster_ip, "error": str(e)}

def run_utilization_check(config_path=None):
    """Reads endpoints from config and persists utilization stats to DB."""
    import os
    from django.conf import settings
    from coreapp.models import ClusterUtilization

    # Automatically find the endpoints.json file
    if not config_path:
        config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"Failed to load config: {str(e)}")
        return

    # Extract endpoints based on the framework's Zone structure
    all_endpoints = [entry for zone in config_data.values() for entry in zone]

    for endpoint in all_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        creds = endpoint.get("credentials", {})
        user = creds.get("user", "admin")
        pwd = creds.get("password", "Nutanix.123")

        if not ip:
            continue

        print(f"Checking utilization for cluster: {ip}...")

        # Run your core logic
        result = check_cluster_utilization(ip, user, pwd)

        # Extract variables
        error_msg = result.get("error")
        cpu = result.get("cpu_usage_percent")
        mem = result.get("memory_usage_percent")
        iops = result.get("iops")

        # Calculate alert logic
        is_underutilized = False
        if not error_msg and mem is not None:
            is_underutilized = mem < MEMORY_UNDERUTILIZATION_THRESHOLD_PCT
            if is_underutilized:
                print(f"ALERT: Cluster memory on {ip} is underutilized ({mem}%)!")

        # Add the alert status into the JSON dictionary
        result["is_underutilized"] = is_underutilized        

        # Save to database
        obj, created = ClusterUtilization.objects.update_or_create(
            cluster_ip=ip,
            defaults={
                "cpu_usage_percent": cpu,
                "memory_usage_percent": mem,
                "iops": iops,
                "is_underutilized": is_underutilized,
                "error_message": error_msg,
                "status_data": result,
            }
        )

        action = "Created" if created else "Updated"
        print(f"{action} DB record for {ip}. Underutilized: {is_underutilized}\n")

if __name__ == "__main__":
    # Setup Django environment so it can run standalone
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
    django.setup()

    run_utilization_check()

