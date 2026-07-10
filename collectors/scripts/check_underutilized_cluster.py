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
        cpu_ppm = stats.get("hypervisor_cpu_usage_ppm", 0)
        mem_ppm = stats.get("memory_usage_ppm", 0)

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check memory/CPU underutilization by reading a config file."
    )
    parser.add_argument(
        "-c", "--config",
        default="static/configurations/endpoints.json",
        help="Path to the endpoints JSON configuration file"
    )
    args = parser.parse_args()

    # Load configuration
    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {args.config}")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {args.config}")
        exit(1)

    pe_endpoints = config_data.get("pe", []) if isinstance(config_data, dict) else config_data

    for endpoint in pe_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        username = endpoint.get("username", "admin")
        password = endpoint.get("password", "Nutanix.123")

        if not ip:
            continue

        print("--- Cluster Utilization Metrics ---")
        result = check_cluster_utilization(ip, username, password)
        print(json.dumps(result, indent=4))

        # Check if we should raise an alert
        if "error" not in result:
            if result["memory_usage_percent"] < MEMORY_UNDERUTILIZATION_THRESHOLD_PCT:
                print(f"ALERT: Cluster memory on {ip} is underutilized!\n")
            else:
                print("\n")
