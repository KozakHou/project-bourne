"""A deterministic, generic file-to-file workload for Bourne documentation."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: demo_simulation.py CONFIG OUTPUT", file=sys.stderr)
        return 2

    config_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    config = json.loads(config_path.read_text(encoding="utf-8"))
    scale = float(config["scale"])
    samples = [float(value) for value in config["samples"]]

    with output_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["sample", "value"])
        for index, sample in enumerate(samples):
            writer.writerow([index, f"{sample * scale:.6f}"])
    print(f"wrote {len(samples)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
