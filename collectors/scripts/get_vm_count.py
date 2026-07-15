"""Gets the powered on/off VM counts and affinity status per host in a Nutanix cluster.

This script connects to the Nutanix Prism API to map each VM to its
corresponding host and tally the power states and affinity settings.
"""

import argparse
import getpass
import json

import requests
import urllib3

# Disable insecure request warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_vm_count_per_host(
    cluster_ip: str,
    username: str,
    password: str,
) -> str:
    """Connects to Nutanix Prism API to get VM counts and affinity status per host.

    Args:
        cluster_ip: The Prism Element Cluster IP or FQDN.
        username: The Prism Element Username.
        password: The Prism Element Password.

    Returns:
        A JSON-formatted string containing the cluster name and a count
        of powered_on, powered_off, and affinity_enabled VMs per host/SVM. 
        Returns a JSON error string if the request fails.
    """
    base_url = f"https://{cluster_ip}:9440/api/nutanix/v2.0"
    auth = (username, password)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        # 1. Get cluster info to extract the cluster name
        cluster_resp = requests.get(
            f"{base_url}/cluster",
            auth=auth,
            headers=headers,
            verify=False,
            timeout=15,
        )
        cluster_resp.raise_for_status()
        cluster_name = cluster_resp.json().get("name", "Unknown Cluster")

        # 2. Get hosts to map host_uuid to SVM IP
        hosts_resp = requests.get(
            f"{base_url}/hosts",
            auth=auth,
            headers=headers,
            verify=False,
            timeout=15,
        )
        hosts_resp.raise_for_status()
        hosts_data = hosts_resp.json().get("entities", [])

        host_map = {}  # Mapping of host_uuid -> svm_ip
        counts = {}    # Output dictionary for holding the SVM counts

        for host in hosts_data:
            # Check multiple known keys for the host ID
            host_uuid = host.get("uuid") or host.get("host_uuid") or host.get("id")

            # Check multiple known keys for the CVM/SVM IP
            svm_ip = (
                host.get("service_vm_external_ip")
                or host.get("controller_vm_external_ip")
                or host.get("service_vm_ip")
                or host.get("controller_vm_ip")
                or host.get("hypervisor_address")
                or host.get("name")
                or host_uuid
            )

            if host_uuid and svm_ip:
                host_map[host_uuid] = svm_ip
                counts[svm_ip] = {
                    "powered_on": 0, 
                    "powered_off": 0, 
                    "affinity_enabled_count": 0
                }

        # 3. Get all VMs
        vms_resp = requests.get(
            f"{base_url}/vms",
            auth=auth,
            headers=headers,
            verify=False,
            timeout=15,
        )
        vms_resp.raise_for_status()
        vms_data = vms_resp.json().get("entities", [])

        # 4. Count the VMs per SVM based on power state and affinity
        if "Unassigned_Host" not in counts:
            counts["Unassigned_Host"] = {
                "powered_on": 0, 
                "powered_off": 0, 
                "affinity_enabled_count": 0
            }

        for vm in vms_data:
            host_uuid = vm.get("host_uuid")
            power_state = vm.get("power_state", "").lower()
            # Affinity contains the host UUID if the VM is pinned
            affinity = vm.get("affinity")

            # Map VM to an SVM IP or drop into Unassigned
            target_svm = host_map.get(host_uuid, "Unassigned_Host")

            # Update Power State counts
            if power_state in ["on", "powered_on"]:
                counts[target_svm]["powered_on"] += 1
            elif power_state in ["off", "powered_off"]:
                counts[target_svm]["powered_off"] += 1

            # Update Affinity count if VM is pinned to a specific host
            if affinity:
                counts[target_svm]["affinity_enabled_count"] += 1

        # Clean up Unassigned_Host if it's completely empty
        if (
            counts["Unassigned_Host"]["powered_on"] == 0
            and counts["Unassigned_Host"]["powered_off"] == 0
            and counts["Unassigned_Host"]["affinity_enabled_count"] == 0
        ):
            del counts["Unassigned_Host"]

        # 5. Format the final output
        output = {"Cluster_name": cluster_name}
        output.update(counts)

        return json.dumps(output, indent=4)

    except requests.exceptions.RequestException as e:
        return json.dumps({"error": str(e)}, indent=4)

def run_vm_count_collection(config_path=None):
    """Reads endpoints from config and persists VM Count per host to DB."""
    import os
    from django.conf import settings

    # Import the database model we just created
    from coreapp.models import VmCountPerHost

    # Automatically find the endpoints.json file in the Django project
    if not config_path:
        config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"Failed to load config: {str(e)}")
        return

    # Extract all endpoints from the zones
    all_endpoints = [entry for zone in config_data.values() for entry in zone]

    for endpoint in all_endpoints:
        # Check if it's a PE before running!
        if endpoint.get("type", "").upper() != "PE":
            continue    
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        creds = endpoint.get("credentials", {})
        user = creds.get("user", "admin")
        pwd = creds.get("password", "Nutanix.123")

        if not ip:
            continue

        print(f"Processing cluster: {ip}...")

        # Run your core logic
        result_json_str = get_vm_count_per_host(ip, user, pwd)
        result_dict = json.loads(result_json_str)

        # Extract the cluster name or error
        cluster_name = result_dict.get("Cluster_name", "Error")

        # Save to database
        obj, created = VmCountPerHost.objects.update_or_create(
            cluster_ip=ip,
            defaults={
                "cluster_name": cluster_name,
                "status_data": result_dict,
            }
        )

        action = "Created" if created else "Updated"
        print(f"{action} DB record for {ip} ({cluster_name})")

if __name__ == "__main__":
    # Setup Django environment so it can run standalone
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
    django.setup()

    run_vm_count_collection()

