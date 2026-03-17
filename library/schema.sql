CREATE TABLE cluster_version (
                created_at TEXT not null
            , command TEXT, output TEXT, ip TEXT);
CREATE TABLE clusters (
                created_at TEXT not null
            , uuid TEXT, name TEXT, clusterExternalIPAddress TEXT, fullVersion TEXT);
CREATE TABLE snapshot_usage (
                created_at TEXT not null
            , name TEXT, id TEXT, ipv4 TEXT, version TEXT, targetVersion TEXT, externalSubnet TEXT, internalSubnet TEXT, uuid TEXT);
