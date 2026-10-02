CREATE TABLE cluster_version (
        created_at TEXT not null
      , command TEXT, output TEXT, output_json TEXT, ip TEXT, cluster_name TEXT);
CREATE TABLE clusters (
        created_at TEXT not null
      , uuid TEXT, name TEXT, clusterExternalIPAddress TEXT, fullVersion TEXT, pe_ips TEXT, timezone TEXT);
CREATE TABLE curator_full_scan (
        created_at TEXT not null
      , metric_name TEXT, cluster_key TEXT, cluster_id TEXT, cluster_name TEXT, ip TEXT, command TEXT, collection_status TEXT, command_execution_status TEXT, exit_code TEXT, full_scan_available TEXT, full_scan_status TEXT, full_scan_status_raw TEXT, execution_id TEXT, full_scan_end_time_raw TEXT, error_codes TEXT, output TEXT, stderr TEXT, output_json TEXT, collected_at TEXT, cluster_vip TEXT);
CREATE TABLE snapshot_usage (
        created_at TEXT not null
      , snapshot_reclaimable_bytes TEXT, output TEXT);
