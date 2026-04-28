#!/usr/bin/env python3

import json
import re
import sys
from pathlib import Path


METRIC_RE = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")


def parse_metrics(text: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in text.splitlines():
        if "messages=" not in line:
            continue
        row: dict[str, object] = {}
        for key, value in METRIC_RE.findall(line):
            try:
                if "." in value:
                    row[key] = float(value)
                else:
                    row[key] = int(value)
            except ValueError:
                row[key] = value
        if row:
            rows.append(row)
    return rows


def main() -> int:
    if len(sys.argv) > 2:
        print(f"Usage: {Path(sys.argv[0]).name} [bench_output_file]", file=sys.stderr)
        return 1

    if len(sys.argv) == 2:
        text = Path(sys.argv[1]).read_text(encoding="utf-8")
    else:
        text = sys.stdin.read()

    rows = parse_metrics(text)
    if not rows:
        print("[]")
        return 0

    print(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
