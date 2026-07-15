"""Checks the /home partition usage on all AHV hosts in a Nutanix cluster.

This script utilizes a two-tier connection strategy. It first attempts to 
retrieve the data via the CVM to handle large clusters efficiently. If the CVM 
is unresponsive or restricted by AOS bugs, it falls back to querying the 
hypervisors directly.
"""

import argparse
import json
import re
import sys
import paramiko
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_cluster_info(vip: str, username: str, password: str) -> tuple[str, dict[str, str]]:
    cluster_name = vip
    hosts_map = {}

    try:
        url = f"https://{vip}:9440/PrismGateway/services/rest/v2.0/cluster"
        response = requests.get(url, auth=(username, password), verify=False, timeout=10)
        response.raise_for_status()
        cluster_name = response.json().get("name", vip)
    except requests.exceptions.RequestException:
        pass

    try:
        url = f"https://{vip}:9440/PrismGateway/services/rest/v2.0/hosts/"
        response = requests.get(url, auth=(username, password), verify=False, timeout=10)
        response.raise_for_status()
        data = response.json()
        for entity in data.get("entities", []):
            name = entity.get("name")
            ip = entity.get("hypervisor_address")
            if name and ip:
                hosts_map[ip] = name
        return cluster_name, hosts_map
    except requests.exceptions.RequestException as e:
        print(f"Error fetching hosts from API for {vip}: {e}", file=sys.stderr)
        return cluster_name, {}

def run_cvm_ssh_strategy(vip: str, pe_user: str, passwords: list, hosts_map: dict[str, str]) -> str | None:
    """Tier 1: Attempt to gather data via the CVM."""
    if not hosts_map:
        return None

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ahv_ips = " ".join(hosts_map.keys())
    native_ssh_cmd = f"""
for ip in {ahv_ips}; do
  echo "============= $ip ============"
  out=$(ssh -q -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i /home/nutanix/.ssh/id_rsa -p 22 root@$ip "df -P -h /home | tail -n 1" 2>/dev/null)
  if [ -z "$out" ]; then
    out=$(ssh -q -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i /home/nutanix/.ssh/id_rsa -p 2223 nutant@$ip "df -P -h /home | tail -n 1" 2>/dev/null)
  fi
  echo "$out"
done
"""

    strategies = []
    for pwd in passwords:
        strategies.append(("nutanix", pwd, native_ssh_cmd, False))
        strategies.append((pe_user, passwords[0], 'sudo -S hostssh "df -P -h /home | tail -n 1"', True))

    for user, pwd, cmd, use_sudo in strategies:
        try:
            client.connect(vip, username=user, password=pwd, timeout=5, auth_timeout=5)
            stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)

            if use_sudo:
                stdin.write(pwd + "\n")
                stdin.flush()

            output = stdout.read().decode("utf-8")
            client.close()

            if "/home" in output and "Check failed" not in output:
                return output
        except Exception:
            continue

    return None

def get_direct_ahv_usage(ip: str, passwords: list) -> dict | None:
    """Tier 2: Fallback to directly SSHing the AHV host from the local machine."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for user, port in [("root", 22), ("nutant", 2223)]:
        for pwd in passwords:
            try:
                client.connect(ip, username=user, password=pwd, port=port, timeout=4, auth_timeout=4)
                stdin, stdout, stderr = client.exec_command("df -P -h /home | tail -n 1")
                output = stdout.read().decode('utf-8').strip()
                client.close()

                if output.endswith("/home"):
                    parts = output.split()
                    if len(parts) >= 6:
                        return {
                            "home_total": parts[1],
                            "home_available": parts[3],
                            "home_usage": parts[4]
                        }
            except Exception:
                pass
    return None

def parse_cvm_output(output: str, hosts_map: dict[str, str]) -> list[dict]:
    results = []
    current_host = None
    single_node_host = list(hosts_map.values())[0] if len(hosts_map) == 1 else None

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue

        match = re.search(r"=============\s*([\d.]+)\s*============", line)
        if match:
            ip = match.group(1)
            current_host = hosts_map.get(ip, ip)
            continue

        if line.endswith("/home"):
            parts = line.split()
            if len(parts) >= 6:
                usage_data = {
                    "home_total": parts[1],
                    "home_available": parts[3],
                    "home_usage": parts[4],
                }

                target_host = current_host if current_host else single_node_host
                if target_host:
                    results.append({target_host: usage_data})
                current_host = None

    return results

def process_cluster(vip: str, pe_user: str, pe_pass: str) -> dict:
    passwords = [pe_pass, "nutanix/4u", "Nutanix.123", "RDMCluster.123"]
    passwords = list(dict.fromkeys(passwords))

    cluster_name, hosts_map = get_cluster_info(vip, pe_user, pe_pass)

    if not hosts_map:
        return {cluster_name: [{"error": "Could not fetch hosts map from API"}]}

    host_results = []
    cvm_output = run_cvm_ssh_strategy(vip, pe_user, passwords, hosts_map)

    if cvm_output:
        host_results = parse_cvm_output(cvm_output, hosts_map)
    else:
        for ip, name in hosts_map.items():
            usage = get_direct_ahv_usage(ip, passwords)
            if usage:
                host_results.append({name: usage})

    found_hostnames = {list(d.keys())[0] for d in host_results if d and isinstance(d, dict) and not "error" in d}
    for name in hosts_map.values():
        if name not in found_hostnames:
            host_results.append({name: {"error": "Could not fetch usage data"}})

    return {cluster_name: host_results}

def main() -> None:
    parser = argparse.ArgumentParser(description="Check /home usage on AHV hosts.")
    parser.add_argument(
        "-c", "--config",
        default="static/configurations/endpoints.json",
        help="Path to the endpoints JSON configuration file"
    )
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(json.dumps({"error": f"Configuration file not found: {args.config}"}, indent=4))
        sys.exit(1)
    except json.JSONDecodeError:
        print(json.dumps({"error": f"Invalid JSON format in file: {args.config}"}, indent=4))
        sys.exit(1)

    final_results = {}
    pe_endpoints = config_data.get("pe", []) if isinstance(config_data, dict) else config_data

    for endpoint in pe_endpoints:
        ip = endpoint.get("ip") or endpoint.get("virtual_ip")
        username = endpoint.get("username", "admin")
        password = endpoint.get("password", "Nutanix.123")

        if not ip:
            continue

        cluster_result = process_cluster(ip, username, password)
        final_results.update(cluster_result)

    print(json.dumps(final_results, indent=2))

if __name__ == "__main__":
    main()
