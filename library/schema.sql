CREATE TABLE cluster_version (
        created_at TEXT not null
      , command TEXT, output TEXT, ip TEXT, cluster_name TEXT);

CREATE TABLE clusters (
        created_at TEXT not null
      , uuid TEXT, name TEXT, clusterExternalIPAddress TEXT, fullVersion TEXT, pe_ips TEXT);

CREATE TABLE snapshot_usage (
        created_at TEXT not null
      , name TEXT, id TEXT, ipv4 TEXT, version TEXT, targetVersion TEXT, externalSubnet TEXT, internalSubnet TEXT, uuid TEXT, output TEXT);

CREATE TABLE check_ahv_home_usage (
        created_at TEXT not null
      , ip_address TEXT, status_data TEXT);

CREATE TABLE check_pgw_status (
        created_at TEXT not null
      , ip_address TEXT, status_data TEXT);

CREATE TABLE check_underutilized_cluster (
        created_at TEXT not null
      , ip_address TEXT, status_data TEXT);

CREATE TABLE check_vm_power_states (
        created_at TEXT not null
      , ip_address TEXT, status_data TEXT);

CREATE TABLE task_monitor (
        created_at TEXT not null
      , ip_address TEXT, status_data TEXT);
