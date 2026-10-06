"""Split prepared ATLAS Parquet files into disjoint files for the scale-up test."""

import argparse
import json
from pathlib import Path

import awkward as ak


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--shards-per-file", type=int, default=640)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    manifest = []
    for path in sorted(args.data_dir.glob("*.parquet")):
        events = ak.from_parquet(path)
        for index in range(args.shards_per_file):
            start = len(events) * index // args.shards_per_file
            stop = len(events) * (index + 1) // args.shards_per_file
            output = args.output_dir / f"{path.stem}-{index:04d}.parquet"
            ak.to_parquet(events[start:stop], output)
            manifest.append({"file": output.name, "source": path.name, "start": start, "stop": stop})
        print(f"{path.name}: {len(events)} events, {args.shards_per_file} shards", flush=True)
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
