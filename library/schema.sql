CREATE TABLE clusters (
        created_at TEXT not null
      , uuid TEXT, name TEXT, clusterExternalIPAddress TEXT, fullVersion TEXT, pe_ips TEXT);
CREATE TABLE pgw_status (
        created_at TEXT not null
      , status_output TEXT, output TEXT, nos_version TEXT, site_type TEXT);
CREATE TABLE snapshot_usage (
        created_at TEXT not null
      , snapshot_reclaimable_bytes TEXT, output TEXT);
CREATE TABLE vm_count (
        created_at TEXT not null
      , total_vms TEXT, output TEXT);
