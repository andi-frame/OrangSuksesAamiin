#!/usr/bin/env python3
"""Earthquake catalog tooling for P1 (stdlib only).

  generate      a synthetic historical catalog with the natural skew of Indonesian seismicity (most events on the
                Sunda arc), as newline-delimited BMKG SeismicEvent JSON (contracts/schemas/seismic-event.json)
  spike         a burst of events in one region (criterion 2: "lonjakan gempa di satu wilayah")
  convert       a CSV catalog (e.g. the assistants' dataset) to the same NDJSON; columns are matched by name
  distribution  offline comparison of placement strategies on the same input: hash of hazard_id (what the
                Aggregator does: FNV-1a 64 mod N, services/aggregator/internal/adapters/sharded), region ranges and
                time ranges; prints rows per shard and the max/mean imbalance

Usage:
  python3 tools/catalog/catalog.py generate --count 5000 > catalog.ndjson
  python3 tools/catalog/catalog.py spike --region "Sumatera Barat" --count 1000 > spike.ndjson
  python3 tools/catalog/catalog.py convert dataset.csv > catalog.ndjson
  python3 tools/catalog/catalog.py distribution catalog.ndjson [spike.ndjson ...] --shards 3
"""
import argparse
import csv
import datetime as dt
import json
import random
import sys
from collections import Counter

# (region_name, weight, lat range, lon range): weights follow the rough share of BMKG-recorded events; the Sunda
# arc (Sumatra-Java) dominates, which is the skew the problem statement describes.
REGIONS = [
    ("Aceh", 9, (2.0, 5.8), (94.5, 97.5)),
    ("Sumatera Utara", 9, (0.5, 3.5), (96.5, 99.5)),
    ("Sumatera Barat", 12, (-3.0, 0.5), (98.5, 101.0)),
    ("Bengkulu", 9, (-5.5, -2.5), (100.5, 103.0)),
    ("Lampung", 5, (-6.5, -4.5), (102.5, 105.5)),
    ("Banten", 6, (-7.5, -6.0), (104.5, 106.5)),
    ("Jawa Barat", 9, (-8.5, -6.5), (106.0, 108.8)),
    ("Jawa Tengah", 4, (-8.8, -7.0), (108.8, 111.5)),
    ("Jawa Timur", 6, (-9.5, -7.5), (111.5, 114.5)),
    ("Bali", 3, (-9.3, -8.0), (114.5, 115.8)),
    ("Nusa Tenggara Barat", 4, (-9.5, -8.0), (115.8, 119.0)),
    ("Nusa Tenggara Timur", 5, (-10.5, -8.0), (119.0, 125.0)),
    ("Sulawesi Tengah", 4, (-2.5, 1.0), (119.5, 123.0)),
    ("Sulawesi Utara", 4, (0.5, 4.5), (123.0, 127.0)),
    ("Maluku", 6, (-8.0, -2.5), (125.0, 132.0)),
    ("Maluku Utara", 4, (-1.0, 3.0), (125.5, 129.5)),
    ("Papua", 3, (-4.5, -1.0), (134.0, 141.0)),
    ("Kalimantan Timur", 1, (-1.0, 2.0), (116.0, 118.5)),
]


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def magnitude(rng):
    # Gutenberg-Richter with b = 1 above M 2.5, capped at 8.5
    return round(min(8.5, 2.5 + rng.expovariate(2.302585)), 1)


def event(rng, idx, prefix, region, when):
    name, _, (la0, la1), (lo0, lo1) = region
    mag = magnitude(rng)
    depth = round(rng.choice([rng.uniform(5, 40), rng.uniform(5, 70), rng.uniform(70, 300)]), 1)
    return {
        "event_id": f"{prefix}{idx:07d}",
        "magnitude": mag,
        "depth_km": depth,
        "epicenter_lat": round(rng.uniform(la0, la1), 4),
        "epicenter_lon": round(rng.uniform(lo0, lo1), 4),
        "region_name": name,
        "occurred_at": iso(when),
        "potential_tsunami": mag >= 7.0 and depth < 70,
    }


def generate(a):
    rng = random.Random(a.seed)
    start = dt.datetime.fromisoformat(a.start).replace(tzinfo=dt.timezone.utc)
    end = dt.datetime.fromisoformat(a.end).replace(tzinfo=dt.timezone.utc)
    span = (end - start).total_seconds()
    weights = [r[1] for r in REGIONS]
    times = sorted(start + dt.timedelta(seconds=rng.uniform(0, span)) for _ in range(a.count))
    for i, t in enumerate(times):
        print(json.dumps(event(rng, i, a.prefix, rng.choices(REGIONS, weights)[0], t)))


def spike(a):
    rng = random.Random(a.seed)
    region = next((r for r in REGIONS if r[0].lower() == a.region.lower()), None)
    if region is None:
        sys.exit(f"unknown region {a.region!r}; one of: " + ", ".join(r[0] for r in REGIONS))
    start = (dt.datetime.fromisoformat(a.start).replace(tzinfo=dt.timezone.utc) if a.start
             else dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1))
    for i in range(a.count):
        t = start + dt.timedelta(seconds=i * a.every_sec)
        print(json.dumps(event(rng, i, a.prefix, region, t)))


ALIASES = {
    "event_id": ["event_id", "eventid", "id", "event", "eventname"],
    "occurred_at": ["occurred_at", "time", "datetime", "origin_time", "tgl_waktu", "waktu", "date_time"],
    "date": ["date", "tgl", "tanggal"],
    "clock": ["ot", "clock", "jam"],
    "epicenter_lat": ["epicenter_lat", "lat", "latitude", "lintang"],
    "epicenter_lon": ["epicenter_lon", "lon", "lng", "long", "longitude", "bujur"],
    "depth_km": ["depth_km", "depth", "kedalaman"],
    "magnitude": ["magnitude", "mag", "magnitudo"],
    "region_name": ["region_name", "region", "remark", "place", "wilayah", "lokasi", "flinn_engdahl"],
    "potential_tsunami": ["potential_tsunami", "tsunami", "potensi_tsunami"],
}


def parse_time(value):
    v = value.strip().replace(" ", "T")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    t = dt.datetime.fromisoformat(v)
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return iso(t.astimezone(dt.timezone.utc))


def convert(a):
    with open(a.csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = {c.lower().strip(): c for c in reader.fieldnames or []}
        pick = {k: next((cols[n] for n in names if n in cols), None) for k, names in ALIASES.items()}
        missing = [k for k in ("epicenter_lat", "epicenter_lon", "magnitude") if not pick[k]]
        if missing or not (pick["occurred_at"] or pick["date"]):
            sys.exit(f"cannot map columns {missing or ['occurred_at']}; found {reader.fieldnames}")
        written = skipped = 0
        for i, row in enumerate(reader):
            try:
                when = (parse_time(row[pick["occurred_at"]]) if pick["occurred_at"]
                        else parse_time(row[pick["date"]] + "T" + (row[pick["clock"]] if pick["clock"] else "00:00:00")))
                mag = float(row[pick["magnitude"]])
                depth = float(row[pick["depth_km"]]) if pick["depth_km"] and row[pick["depth_km"]] else 10.0
                tsu = row.get(pick["potential_tsunami"] or "", "")
                out = {
                    # the original identifier is kept, so re-imports are idempotent (only new IDs are inserted)
                    "event_id": (row[pick["event_id"]].strip() if pick["event_id"] and row[pick["event_id"]].strip()
                                 else f"{a.prefix}{i:07d}"),
                    "magnitude": mag,
                    "depth_km": max(0.0, depth),
                    "epicenter_lat": float(row[pick["epicenter_lat"]]),
                    "epicenter_lon": float(row[pick["epicenter_lon"]]),
                    "region_name": (row[pick["region_name"]].strip() if pick["region_name"] else "") or "Tidak diketahui",
                    "occurred_at": when,
                    "potential_tsunami": str(tsu).strip().lower() in ("1", "true", "yes", "ya", "y"),
                }
                print(json.dumps(out))
                written += 1
            except (ValueError, KeyError, TypeError) as e:
                skipped += 1
                print(f"line {i + 2}: skipped ({e})", file=sys.stderr)
        print(f"converted {written}, skipped {skipped}", file=sys.stderr)


def fnv1a64(data):
    h = 0xCBF29CE484222325
    for b in data:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def imbalance(counter, shards):
    rows = [counter.get(s, 0) for s in range(shards)]
    mean = sum(rows) / shards or 1
    return rows, max(rows) / mean


def distribution(a):
    events = []
    for path in a.inputs:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    events.append(json.loads(line))
    n = a.shards
    # region ranges: regions sorted by name and cut into n contiguous groups, the usual way a "per region" range
    # partitioning is set up when the distribution is not known in advance
    names = sorted({e["region_name"] for e in events})
    region_shard = {name: min(n - 1, i * n // max(1, len(names))) for i, name in enumerate(names)}
    times = sorted(e["occurred_at"] for e in events)
    cuts = [times[min(len(times) - 1, (i + 1) * len(times) // n)] for i in range(n - 1)]

    def time_shard(t):
        return sum(1 for c in cuts if t >= c)

    strategies = {
        "hash(hazard_id) mod N (chosen)": Counter(fnv1a64(("bmkg-" + e["event_id"]).encode()) % n for e in events),
        "range(region_name)": Counter(region_shard[e["region_name"]] for e in events),
        "range(occurred_at), quantile cuts on this input": Counter(time_shard(e["occurred_at"]) for e in events),
    }
    if a.recent_hours:
        newest = max(times)
        t0 = iso(dt.datetime.fromisoformat(newest.replace("Z", "+00:00")) - dt.timedelta(hours=a.recent_hours))
        recent = [e for e in events if e["occurred_at"] >= t0]
        strategies[f"writes of the last {a.recent_hours} h, hash"] = Counter(
            fnv1a64(("bmkg-" + e["event_id"]).encode()) % n for e in recent)
        strategies[f"writes of the last {a.recent_hours} h, range(region_name)"] = Counter(
            region_shard[e["region_name"]] for e in recent)
        strategies[f"writes of the last {a.recent_hours} h, range(occurred_at)"] = Counter(
            time_shard(e["occurred_at"]) for e in recent)
    top = Counter(e["region_name"] for e in events).most_common(3)
    print(f"events: {len(events)}, shards: {n}, top regions: " + ", ".join(f"{r} {c}" for r, c in top))
    print(f"{'strategy':52} " + " ".join(f"{'shard-' + str(s):>8}" for s in range(n)) + "   max/mean")
    for name, counter in strategies.items():
        rows, ratio = imbalance(counter, n)
        print(f"{name:52} " + " ".join(f"{r:>8}" for r in rows) + f"   {ratio:8.2f}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--count", type=int, default=5000)
    g.add_argument("--seed", type=int, default=4031)
    g.add_argument("--start", default="2016-01-01T00:00:00")
    g.add_argument("--end", default="2026-11-01T00:00:00")
    g.add_argument("--prefix", default="cat-")
    g.set_defaults(fn=generate)
    s = sub.add_parser("spike")
    s.add_argument("--region", required=True)
    s.add_argument("--count", type=int, default=1000)
    s.add_argument("--seed", type=int, default=7)
    s.add_argument("--start", default="")
    s.add_argument("--every-sec", type=float, default=2.0)
    s.add_argument("--prefix", default="spike-")
    s.set_defaults(fn=spike)
    c = sub.add_parser("convert")
    c.add_argument("csv")
    c.add_argument("--prefix", default="cat-")
    c.set_defaults(fn=convert)
    d = sub.add_parser("distribution")
    d.add_argument("inputs", nargs="+")
    d.add_argument("--shards", type=int, default=3)
    d.add_argument("--recent-hours", type=int, default=0)
    d.set_defaults(fn=distribution)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
