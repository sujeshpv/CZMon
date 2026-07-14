"""Verifies Prism Gateway (PGW) status for all configured clusters.

This script connects to the Prism Gateway heartbeat endpoint for every 
PC and PE defined in the configuration and returns the nosVersion, 
clusterFunction, and siteType.
"""

import argparse
import json
import requests
import urllib3

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def verify_pgw_status(ip: str, username: str, password: str) -> dict:
    """Checks the Prism Gateway heartbeat status."""
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
        return {"pg_status": response.json()}
    except Exception as e:
        return {"pg_status": {"error": str(e)}}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Prism Gateway status.")
    parser.add_argument(
        "-c", "--config",
        default="static/configurations/endpoints.json",
        help="Path to the setup config JSON file"
    )
    args = parser.parse_args()

    # Load configuration
    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(json.dumps({"error": f"Failed to load config: {str(e)}"}, indent=4))
        exit(1)

    final_results = {}

    # Extract all endpoints (both PC and PE)
    # Adjust keys based on your specific endpoints.json structure
    pe_list = config_data.get("pe", [])
    pc_list = config_data.get("pc", [])
    all_endpoints = pe_list + pc_list

    for endpoint in all_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        user = endpoint.get("username", "admin")
        pwd = endpoint.get("password", "Nutanix.123")

        if not ip:
            continue

        final_results[ip] = verify_pgw_status(ip, user, pwd)

    print(json.dumps(final_results, indent=4))
