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

def main() -> None:
    parser = argparse.ArgumentParser(description="Health check for Nutanix Cluster Timezone (UTC) via config JSON")
    parser.add_argument("-c", "--config", default="static/configurations/endpoints.json", help="Path to config JSON")
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(json.dumps({"error": str(e)}, indent=4))
        sys.exit(1)

    payload = {
        "section": "observability_health_check",
        "checks": {
            "cluster_time_utc": {}
        }
    }

    # Handle standard endpoints structure
    pe_endpoints = config_data.get("pe", [])
    pc_endpoints = config_data.get("pc", [])

    all_passed = True
    for endpoint in pe_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        if not ip: continue
        res = process_cluster(ip, endpoint.get("username", "admin"), endpoint.get("password", "Nutanix.123"), "PE")
        payload["checks"]["cluster_time_utc"][ip] = res
        if res["status"] != "PASSED": all_passed = False

    for endpoint in pc_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        if not ip: continue
        res = process_cluster(ip, endpoint.get("username", "admin"), endpoint.get("password", "Nutanix.123"), "PC")
        payload["checks"]["cluster_time_utc"][ip] = res
        if res["status"] != "PASSED": all_passed = False

    print(json.dumps(payload, indent=4))
    if not all_passed:
        sys.exit(1)

if __name__ == "__main__":
    main()
