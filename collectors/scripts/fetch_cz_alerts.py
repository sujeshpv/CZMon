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
        response = requests.get(url, auth=(username, password), verify=False, timeout=60)
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

def run_alert_collection(config_path=None, hours=48):
    """Reads PC endpoints from config and persists active alerts to DB."""
    import os
    from django.conf import settings
    from coreapp.models import CZAlert

    # Automatically find the endpoints.json file
    if not config_path:
        config_path = os.path.join(settings.BASE_DIR, "static", "configurations", "endpoints.json")

    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"Failed to load config: {str(e)}")
        return

    # Extract PC endpoints from the zones (matches framework structure)
    all_endpoints = [entry for zone in config_data.values() for entry in zone]
    pc_endpoints = [e for e in all_endpoints if e.get("type", "").upper() == "PC"]

    if not pc_endpoints:
        print("No 'PC' (Prism Central) endpoints found in configuration.")
        return

    for endpoint in pc_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        creds = endpoint.get("credentials", {})
        user = creds.get("user", "admin")
        pwd = creds.get("password", "Nutanix.123")
        name = endpoint.get("name") or f"PC_{ip}"

        if not ip:
            continue

        print(f"Fetching active alerts for Prism Central: {ip}...")

        # Execute your core logic
        result = get_unresolved_alerts(ip, user, pwd, hours, name)

        # Parse the output
        pc_data = result.get(name, {})

        if "error" in pc_data:
            # Save the error state to the DB
            obj, created = CZAlert.objects.update_or_create(
                pc_name_or_ip=name,
                alert_policy_id="API_ERROR",
                source_cluster="Unknown",
                defaults={"error_message": pc_data["error"]}
            )
            print(f"  -> Failed: {pc_data['error']}")
            continue

        # If there are no alerts
        if not pc_data:
            print(f"  -> No target alerts found for {name} in the last {hours} hours.")
            continue

        # Save each Policy ID and Cluster combination to the DB
        for policy_id, clusters in pc_data.items():
            for cluster_name, messages in clusters.items():
                obj, created = CZAlert.objects.update_or_create(
                    pc_name_or_ip=name,
                    alert_policy_id=policy_id,
                    source_cluster=cluster_name,
                    defaults={
                        "alert_messages": messages,
                        "error_message": None,
                    }
                )
                action = "Created" if created else "Updated"
                print(f"  -> {action} DB record for {cluster_name} (Policy: {policy_id}) with {len(messages)} alerts.")

if __name__ == "__main__":
    # Setup Django environment so it can run standalone
    import os
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "czmon.settings")
    django.setup()

    run_alert_collection()

