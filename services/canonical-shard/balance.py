#!/opt/patroni/bin/python3
"""Move this shard's primary role to its preferred node once that is safe.

Runs next to Patroni in every shard member (started by start-shard). Only the current leader acts, and only through
its own Patroni REST API on localhost:

  * PREFERRED_PRIMARY_NODE names the node that should carry this shard's write load (scripts/k8s/up.sh spreads
    the shards' preferred nodes over the storage nodes, see ConfigMap shard-placement);
  * when the leader is elsewhere and a member on the preferred node has been a *synchronous* standby with zero
    replication lag for BALANCE_STABLE_SEC, the leader asks Patroni for a planned switchover to it;
  * a switchover is planned (no data loss: the candidate is synchronous), at most once per BALANCE_COOLDOWN_SEC.

This is what makes a node that returns with an empty disk take its share of the writes again without an operator
(P1 criterion 5): Patroni rebuilds its copy with a base backup, the copy becomes synchronous, and this loop hands
the primary role back.
"""
import json
import os
import sys
import time
import urllib.request

API = "http://127.0.0.1:8008"
me = os.environ["POD_NAME"]
preferred = os.environ.get("PREFERRED_PRIMARY_NODE", "")
interval = float(os.environ.get("BALANCE_INTERVAL_SEC", "10"))
stable_for = float(os.environ.get("BALANCE_STABLE_SEC", "60"))
cooldown = float(os.environ.get("BALANCE_COOLDOWN_SEC", "300"))


def log(msg, **fields):
    print(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "component": "balance-shard",
                      "member": me, "msg": msg, **fields}), file=sys.stderr, flush=True)


def get(path):
    with urllib.request.urlopen(API + path, timeout=5) as r:
        return json.loads(r.read() or b"{}")


def switchover(leader, candidate):
    body = json.dumps({"leader": leader, "candidate": candidate}).encode()
    req = urllib.request.Request(API + "/switchover", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode(errors="replace")


def main():
    if not preferred:
        return
    healthy_since = None
    last_switch = 0.0
    while True:
        time.sleep(interval)
        try:
            cluster = get("/cluster")
        except Exception:  # Patroni not up yet, or restarting
            healthy_since = None
            continue
        if cluster.get("pause"):
            healthy_since = None
            continue
        members = cluster.get("members", [])
        leader = next((m for m in members if m.get("role") == "leader"), None)
        if leader is None or leader.get("name") != me:
            healthy_since = None  # only the leader acts
            continue
        if (leader.get("tags") or {}).get("node") == preferred:
            healthy_since = None  # already where it should be
            continue
        candidate = next((m for m in members
                          if m.get("name") != me and (m.get("tags") or {}).get("node") == preferred
                          and m.get("role") == "sync_standby" and m.get("state") == "streaming"
                          and m.get("lag") == 0), None)
        if candidate is None:
            healthy_since = None
            continue
        now = time.monotonic()
        healthy_since = healthy_since or now
        if now - healthy_since < stable_for or now - last_switch < cooldown:
            continue
        try:
            status, text = switchover(me, candidate["name"])
            log("switchover requested", candidate=candidate["name"], preferred_node=preferred,
                status=status, response=text[:200])
        except Exception as e:  # e.g. 412/503 while Patroni is busy; try again after the cooldown
            log("switchover failed", candidate=candidate["name"], error=str(e)[:200])
        last_switch = now
        healthy_since = None


if __name__ == "__main__":
    main()
