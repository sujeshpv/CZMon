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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get powered on/off VM counts and affinity status per host/SVM."
    )
    parser.add_argument("-i", "--ip", required=True, help="Prism Element Cluster IP or FQDN")
    parser.add_argument("-u", "--username", required=True, help="Prism Element Username")
    parser.add_argument("-p", "--password", help="Prism Element Password (will prompt if not provided)")

    args = parser.parse_args()

    cli_password = args.password if args.password else getpass.getpass(prompt="Password: ")

    print(get_vm_count_per_host(args.ip, args.username, cli_password))
    