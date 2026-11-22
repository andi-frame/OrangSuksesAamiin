#!/usr/bin/env python3
"""Measure how late and how out of order Feed Satelit detections arrive (P4 criterion 2: the lateness bound).

Reads the SSE stream from an offset for a while (read-only; a second connection of the group may be refused with
429 while the Aggregator is connected, so run it before the deployment or with the consumer stopped) and reports:

  * out-of-order distance: newest acquisition time seen so far minus this detection's acquisition time. This is
    exactly what the Aggregator's watermark (newest acquisition time - SATELLITE_LATENESS_SEC) compares against,
    so its maximum is the smallest lateness bound that classifies no detection of this sample as late;
  * arrival delay: the feed's virtual clock (heartbeat virtual_now, else /v1/session) minus acquisition time;
  * counts per event type.

  SATELLITE_FEED_TOKEN=... python3 tools/satellite/probe.py [--from 0] [--seconds 120] [--url https://...]
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.request


def acq_time(d):
    hhmm = str(d["acq_time"]).zfill(4)
    return dt.datetime.strptime(f'{d["acq_date"]} {hhmm}', "%Y-%m-%d %H%M").replace(tzinfo=dt.timezone.utc)


def parse_ts(v):
    return dt.datetime.fromisoformat(v.replace("Z", "+00:00"))


def quantiles(xs):
    if not xs:
        return None
    xs = sorted(xs)
    q = lambda p: xs[min(len(xs) - 1, max(0, int(round(p * len(xs) + 0.5)) - 1))]
    return {"n": len(xs), "p50": q(0.5), "p95": q(0.95), "p99": q(0.99), "max": xs[-1]}


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--url", default=os.environ.get("SATELLITE_FEED_URL", "https://satellite.naufarrel.dev"))
    a.add_argument("--from", dest="offset", type=int, default=0)
    a.add_argument("--seconds", type=float, default=120)
    args = a.parse_args()
    token = os.environ.get("SATELLITE_FEED_TOKEN") or sys.exit("SATELLITE_FEED_TOKEN is required")
    hdr = {"Authorization": f"Bearer {token}", "Accept": "text/event-stream", "User-Agent": "argus-probe/1.0"}

    def get(path, timeout=15):
        return urllib.request.urlopen(urllib.request.Request(args.url.rstrip("/") + path, headers=hdr), timeout=timeout)

    session = json.loads(get("/v1/session").read())
    print("session:", json.dumps({k: session.get(k) for k in ("group", "profile", "speed", "virtual_now", "latest_offset")}))
    virtual_now = parse_ts(session["virtual_now"]) if session.get("virtual_now") else None
    newest = None
    out_of_order, delay = [], []
    kinds = {}
    first = last = None
    deadline = time.monotonic() + args.seconds
    stream = get(f"/v1/stream?from={args.offset}", timeout=60)
    kind, data = None, []
    for raw in stream:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if line.startswith("event:"):
            kind = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
        elif line == "" and data:
            try:
                body = json.loads("\n".join(data))
            except ValueError:
                body = {}
            kinds[kind] = kinds.get(kind, 0) + 1
            off = body.get("offset")
            first = off if first is None else first
            last = off
            if kind == "heartbeat" and body.get("virtual_now"):
                virtual_now = parse_ts(body["virtual_now"])
            det = body.get("detection") or (body if kind == "detection" else None)
            if kind == "detection" and det and det.get("acq_date"):
                t = acq_time(det)
                newest = t if newest is None or t > newest else newest
                out_of_order.append((newest - t).total_seconds() / 60)
                if virtual_now:
                    delay.append((virtual_now - t).total_seconds() / 60)
            kind, data = None, []
            if time.monotonic() > deadline:
                break
    print("offsets:", first, "->", last, "events:", json.dumps(kinds))
    print("out_of_order_minutes:", json.dumps(quantiles(out_of_order)))
    print("arrival_delay_minutes (virtual clock - acquisition):", json.dumps(quantiles(delay)))


if __name__ == "__main__":
    main()
