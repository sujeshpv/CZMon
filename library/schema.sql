CREATE TABLE cluster_version (
        created_at TEXT not null
      , command TEXT, output TEXT, output_json TEXT, ip TEXT, cluster_name TEXT);
CREATE TABLE clusters (
        created_at TEXT not null
      , uuid TEXT, name TEXT, clusterExternalIPAddress TEXT, fullVersion TEXT, pe_ips TEXT, timezone TEXT);
CREATE TABLE curator_full_scan (
        created_at TEXT not null
      , command TEXT, output TEXT, output_json TEXT, ip TEXT, cluster_name TEXT);
CREATE TABLE snapshot_usage (
        created_at TEXT not null
      , snapshot_reclaimable_bytes TEXT, output TEXT);
