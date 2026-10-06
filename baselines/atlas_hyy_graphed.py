"""Run the existing ATLAS plan on prepared Parquet shards and save comparable results."""

import argparse
import json
import time
from pathlib import Path

import graphed_histogram as gh
from graphed_executors.local import ProcessPoolExecutor
from graphed_executors.taskvine_backend import TaskVineExecutor

from examples.atlas_hyy import build_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backend", choices=["processes", "taskvine"], default="processes")
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--manager-name", default="graphed-hyy-scale")
    parser.add_argument("--wait-for-workers", type=int, default=1)
    parser.add_argument("--port", type=int, default=9123)
    args = parser.parse_args()
    paths = sorted(args.data_dir.resolve().glob("*.parquet"))
    start = time.perf_counter()
    plan = build_plan(paths)
    build_s = time.perf_counter() - start
    if args.backend == "taskvine":
        executor = TaskVineExecutor(
            manager_name=args.manager_name,
            port=args.port,
            libcores=args.cores,
            wait_for_workers=args.wait_for_workers,
            work_dir=args.output.with_suffix(".work"),
            run_info_path=str(args.output.with_suffix(".logs")),
        )
    else:
        executor = ProcessPoolExecutor(max_workers=args.cores)
    with executor:
        start = time.perf_counter()
        result = executor.run(plan)
        run_s = time.perf_counter() - start
        histogram = gh.unpack(result.value)["diphoton_mass"]
        report = {
            "backend": args.backend,
            "data_dir": str(args.data_dir.resolve()),
            "n_partitions": result.n_partitions,
            "n_combines": result.n_combines,
            "cores_per_worker": args.cores,
            "build_s": build_s,
            "run_s": run_s,
            "bins_with_flow": histogram.values(flow=True).tolist(),
            "selected_in_range": int(histogram.sum()),
        }
        if args.backend == "taskvine":
            report["manager_status"] = executor.manager.status("manager")[0]
            report["worker_status"] = executor.manager.status("workers")
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
