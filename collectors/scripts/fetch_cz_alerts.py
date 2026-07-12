import requests
import json
import urllib3
import argparse
import sys
import re
from datetime import datetime, timezone, timedelta

# Suppress InsecureRequestWarning for unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Specific warnings we want to track
TARGET_ALERTS = [
    "Remote Site Is Unhealthy",
    "VM Recovery Point Replication Failed",
    "VM Recovery Point Creation Failed.",
    "snapshot_chain_height_check",
    "Nearsync Replication is lagging for recovery point",
    "recovery point lag",
    "Sync replication paused",
    "Recovery point lag",
    "VSS snapshot not supported for VMs",
    "nearsync failed"
]

# Global cache for Cluster UUID to Name mapping
CLUSTER_NAME_CACHE = {}

def get_cluster_name(pc_ip, username, password, cluster_uuid):
    """Fetches the cluster name using the cluster UUID via v3 API."""
    if not cluster_uuid:
        return "Unknown_Cluster"
    if cluster_uuid in CLUSTER_NAME_CACHE:
        return CLUSTER_NAME_CACHE[cluster_uuid]
    url = f"https://{pc_ip}:9440/api/nutanix/v3/clusters/{cluster_uuid}"
    try:
        response = requests.get(url, auth=(username, password), verify=False, timeout=10)
        if response.status_code == 200:
            cluster_name = response.json().get("spec", {}).get("name", "Unknown_Cluster")
            CLUSTER_NAME_CACHE[cluster_uuid] = cluster_name
            return cluster_name
    except Exception:
        pass
    return "Unknown_Cluster"

def hydrate_message(alert):
    """Replaces {placeholders} in message with actual values from parameters."""
    message = alert.get("message", "")
    parameters = alert.get("parameters", [])
    if not message or not parameters:
        return message or alert.get("title", "")

    param_map = {}
    for param in parameters:
        name = param.get("paramName")
        val_obj = param.get("paramValue", {})
        val = val_obj.get("stringValue")
        if name and val:
            param_map[name] = val

    def replace_match(match):
        key = match.group(1)
        return param_map.get(key, match.group(0))

    return re.sub(r"{(.*?)}", replace_match, message)

def get_unresolved_alerts(pc_ip, username, password, hours, pc_az_name):
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    url = f"https://{pc_ip}:9440/api/monitoring/v4.3/serviceability/alerts"
    params = {
        "$filter": "isResolved eq false",
        "$limit": 100,
        "$orderby": "lastUpdatedTime desc"
    }

    try:
        response = requests.get(url, auth=(username, password), params=params, verify=False, timeout=30)
        if response.status_code != 200:
            return {pc_az_name: {"error": f"API Error: {response.status_code}"}}

        alerts = response.json().get("data", [])
        pc_data = {}

        for alert in alerts:
            last_updated_str = alert.get("lastUpdatedTime")
            if last_updated_str:
                dt = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00"))
                if dt < cutoff_time:
                    continue

            hydrated_msg = hydrate_message(alert)
            title = alert.get("title", "")

            is_target = any(w.lower() in hydrated_msg.lower() or w.lower() in title.lower() for w in TARGET_ALERTS)
            if not is_target:
                continue

            policy_id = alert.get("alertType", "Unknown_Policy")
            source_cluster = alert.get("clusterName") or get_cluster_name(pc_ip, username, password, alert.get("originatingClusterUUID"))

            if policy_id not in pc_data:
                pc_data[policy_id] = {}
            if source_cluster not in pc_data[policy_id]:
                pc_data[policy_id][source_cluster] = []

            pc_data[policy_id][source_cluster].append(hydrated_msg)

        return {pc_az_name: pc_data}
    except Exception as e:
        return {pc_az_name: {"error": str(e)}}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch unresolved alerts using config JSON")
    parser.add_argument("-c", "--config", default="static/configurations/endpoints.json")
    parser.add_argument("--hours", type=int, default=48)
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(json.dumps({"error": f"Config load failed: {str(e)}"}, indent=4))
        sys.exit(1)

    final_results = {}
    pc_endpoints = config_data.get("pc", [])

    for pc in pc_endpoints:
        ip = pc.get("ip") or pc.get("virtual_ip")
        if not ip: continue

        name = pc.get("pc_name") or pc.get("name") or f"PC_{ip}"
        user = pc.get("username", "admin")
        pwd = pc.get("password", "Nutanix.123")

        pc_alerts = get_unresolved_alerts(ip, user, pwd, args.hours, name)
        final_results.update(pc_alerts)

    print(json.dumps(final_results, indent=4))
