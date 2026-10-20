"""Run the existing ATLAS plan on prepared Parquet shards and save comparable results."""

import argparse
import contextlib
import json
import time
from pathlib import Path

import graphed_histogram as gh
from graphed_executors.local import ProcessPoolExecutor

from examples.atlas_hyy import build_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backend", choices=["processes", "vine_graph"], default="processes")
    parser.add_argument("--cores", type=int, default=16, help="processes of the process pool")
    parser.add_argument("--manager-name", default="graphed-hyy-scale")
    parser.add_argument("--wait-for-workers", type=int, default=1)
    parser.add_argument("--port", type=int, default=9123)
    args = parser.parse_args()
    paths = sorted(args.data_dir.resolve().glob("*.parquet"))
    start = time.perf_counter()
    plan = build_plan(paths)
    build_s = time.perf_counter() - start
    with contextlib.ExitStack() as stack:
        if args.backend == "vine_graph":
            from graphed_executors.vine_graph_backend import vine_graph_runner
            from ndcctools.taskvine import vine_graph as vg

            executor = stack.enter_context(
                vg.Executor(
                    args.port,
                    args.manager_name,
                    params={"wait-for-workers": args.wait_for_workers},
                    run_info_path=str(args.output.with_suffix(".logs")),
                )
            )
            runner = stack.enter_context(vine_graph_runner(executor))
        else:
            runner = stack.enter_context(ProcessPoolExecutor(max_workers=args.cores))
        start = time.perf_counter()
        result = runner.run(plan)
        run_s = time.perf_counter() - start
        histogram = gh.unpack(result.value)["diphoton_mass"]
        report = {
            "backend": args.backend,
            "data_dir": str(args.data_dir.resolve()),
            "n_partitions": result.n_partitions,
            "n_combines": result.n_combines,
            "build_s": build_s,
            "run_s": run_s,
            "bins_with_flow": histogram.values(flow=True).tolist(),
            "selected_in_range": int(histogram.sum()),
        }
        if args.backend == "vine_graph":
            report["vine_graph"] = executor.stats()  # makespan, tasks, recovery tasks
            report["workers"] = runner.backend.n_workers()
            report["worker_cores"] = runner.backend.task_slots()
        else:
            report["processes"] = args.cores
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
