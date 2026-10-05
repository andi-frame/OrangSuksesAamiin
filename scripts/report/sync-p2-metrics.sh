#!/usr/bin/env bash
# Embed k6 summary into docs/report/02-p2.md (Orang 2 laporan).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SUM="$ROOT/docs/evidence/p2/k6-summary.json"
REPORT="$ROOT/docs/report/02-p2.md"
MARK_START="<!-- k6-metrics:auto:start -->"
MARK_END="<!-- k6-metrics:auto:end -->"

[ -f "$SUM" ] || exit 0

export SUM REPORT MARK_START MARK_END
python3 <<'PY'
import json
import os
from datetime import datetime, timezone

sum_path = os.environ["SUM"]
report_path = os.environ["REPORT"]
start = os.environ["MARK_START"]
end = os.environ["MARK_END"]

payload = open(sum_path, encoding="utf-8").read().strip()
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
block = (
    f"{start}\n\n"
    f"Snapshot dari `make demo-p2` ({ts}):\n\n"
    f"```json\n{payload}\n```\n\n"
    f"{end}\n"
)

text = open(report_path, encoding="utf-8").read()
if start in text and end in text:
    pre, rest = text.split(start, 1)
    _, post = rest.split(end, 1)
    new = pre + block + post.lstrip("\n")
else:
    needle = "## Metrik k6\n"
    if needle not in text:
        raise SystemExit(0)
    pre, post = text.split(needle, 1)
    lines = post.splitlines(keepends=True)
    i = 0
    while i < len(lines) and not (lines[i].startswith("## ") and i > 0):
        i += 1
    new = pre + needle + "\n" + block + "".join(lines[i:])

open(report_path, "w", encoding="utf-8").write(new)
PY
