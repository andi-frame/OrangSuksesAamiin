#!/opt/patroni/bin/python3
"""Start PostgreSQL under Patroni using pod identity and mounted credentials."""
import os
from pathlib import Path

import yaml

env = os.environ
ip = env["POD_IP"]
data_dir = Path("/var/lib/postgresql/data/pgdata")
# kubelet can reapply fsGroup permissions on a remount after a node reboot.
# PostgreSQL requires its own data directory to be private at every startup.
if data_dir.is_dir():
    data_dir.chmod(0o700)
config = {
    "scope": env["SHARD_NAME"],
    "name": env["POD_NAME"],
    "restapi": {"listen": "0.0.0.0:8008", "connect_address": f"{ip}:8008"},
    "kubernetes": {
        "namespace": env["POD_NAMESPACE"],
        "labels": {"app": "canonical-shard", "shard": env["SHARD_NAME"]},
        "scope_label": "shard",
        "role_label": "db-role",
        "use_endpoints": True,
        "pod_ip": ip,
        "ports": [{"name": "postgres", "port": 5432}],
    },
    "bootstrap": {
        "dcs": {
            "ttl": 20,
            "loop_wait": 3,
            "retry_timeout": 5,
            "check_timeline": True,
            "synchronous_mode": True,
            "synchronous_node_count": 1,
            "postgresql": {"use_pg_rewind": True},
        },
        "initdb": ["data-checksums"],
        "pg_hba": [
            "local all all peer",
            "host replication replicator 0.0.0.0/0 scram-sha-256",
            "host all all 0.0.0.0/0 scram-sha-256",
        ],
        "post_bootstrap": "/usr/local/bin/init-shard",
    },
    "postgresql": {
        "listen": "0.0.0.0:5432",
        "connect_address": f"{ip}:5432",
        "data_dir": str(data_dir),
        "parameters": {"argus.node": env["NODE_NAME"]},
        "basebackup": {"checkpoint": "fast"},
        "authentication": {
            "superuser": {"username": "postgres", "password": env["POSTGRES_ADMIN_PASSWORD"]},
            "replication": {"username": "replicator", "password": env["POSTGRES_REPLICATION_PASSWORD"]},
        },
    },
}
os.umask(0o077)
path = Path("/tmp/patroni.yaml")
path.write_text(yaml.safe_dump(config))
os.execvp("patroni", ["patroni", str(path)])
