import argparse
import json
import sys
import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_pe_timezone(cluster_ip: str, username: str, password: str) -> dict[str, dict[str, str]]:
    """Retrieves the timezone for a Prism Element (PE) cluster and its SVMs."""
    cluster_url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/cluster"
    response = requests.get(cluster_url, auth=(username, password), verify=False, timeout=10)
    response.raise_for_status()
    cluster_data = response.json()

    cluster_name = cluster_data.get("name", "Unknown_PE_Cluster")
    timezone = cluster_data.get("timezone", "Unknown")

    hosts_url = f"https://{cluster_ip}:9440/PrismGateway/services/rest/v2.0/hosts"
    hosts_response = requests.get(hosts_url, auth=(username, password), verify=False, timeout=10)
    hosts_response.raise_for_status()
    hosts_data = hosts_response.json()

    result: dict[str, dict[str, str]] = {cluster_name: {}}
    for host in hosts_data.get("entities", []):
        cvm_ip = host.get("service_vm_external_ip", host.get("controller_vm_backplane_ip"))
        if cvm_ip:
            result[cluster_name][cvm_ip] = timezone
    return result

def get_pc_timezone(pc_ip: str, username: str, password: str) -> dict[str, dict[str, str]]:
    """Retrieves the timezone for a Prism Central (PC) cluster."""
    url = f"https://{pc_ip}:9440/api/nutanix/v3/clusters/list"
    payload = {"kind": "cluster"}
    response = requests.post(url, auth=(username, password), json=payload, verify=False, timeout=10)
    response.raise_for_status()
    clusters = response.json().get("entities", [])

    result: dict[str, dict[str, str]] = {}
    for cluster in clusters:
        cluster_name = cluster.get("spec", {}).get("name", "Unknown_PC_Cluster")
        timezone = cluster.get("spec", {}).get("resources", {}).get("timezone", "Unknown")
        result[cluster_name] = {pc_ip: str(timezone)}
    return result

def process_cluster(ip, username, password, ctype):
    utc_variants = ["UTC", "UTC+00:00", "Etc/UTC", "GMT"]
    try:
        if ctype == "PE":
            data = get_pe_timezone(ip, username, password)
        else:
            data = get_pc_timezone(ip, username, password)

        is_utc = True
        messages = []
        for cluster, svms in data.items():
            for svm, tz in svms.items():
                if tz not in utc_variants:
                    is_utc = False
                    messages.append(f"[FAIL] SVM {svm} in '{cluster}' is set to: {tz} (Expected: UTC)")
                else:
                    messages.append(f"[PASS] SVM {svm} in '{cluster}' is correctly set to UTC.")

        return {
            "status": "PASSED" if is_utc else "FAILED",
            "details": data,
            "messages": messages
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "details": {},
            "messages": [f"Error connecting to {ip}: {str(e)}"]
        }

def run_timezone_health_check(config_path=None):
    """Reads endpoints from config and persists timezone health status to DB."""
    import os
    from django.conf import settings
    from coreapp.models import TimezoneHealthCheck

    # Automatically find the endpoints.json file
    if not config_path:
        config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"Failed to load config: {str(e)}")
        return

    # Extract all endpoints from the zones (matches the framework's structure)
    all_endpoints = [entry for zone in config_data.values() for entry in zone]

    for endpoint in all_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        endpoint_type = endpoint.get("type", "PE").upper()
        creds = endpoint.get("credentials", {})
        user = creds.get("user", "admin")
        pwd = creds.get("password", "Nutanix.123")

        if not ip:
            continue

        print(f"Running Timezone Health Check for {endpoint_type} cluster: {ip}...")

        # Execute your core logic
        res = process_cluster(ip, user, pwd, endpoint_type)

        status = res.get("status", "ERROR")
        details = res.get("details", {})
        messages = res.get("messages", [])

        # Print the console output
        for msg in messages:
            print(f"  -> {msg}")

        # Save to database
        obj, created = TimezoneHealthCheck.objects.update_or_create(
            cluster_ip=ip,
            defaults={
                "endpoint_type": endpoint_type,
                "status": status,
                "details_data": details,
                "messages_data": messages,
            }
        )

        action = "Created" if created else "Updated"
        print(f"{action} DB record for {ip}. Status: {status}\n")

if __name__ == "__main__":
    # Setup Django environment so it can run standalone
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
    django.setup()

    run_timezone_health_check()

