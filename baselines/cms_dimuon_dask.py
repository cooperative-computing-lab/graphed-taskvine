"""Traditional dask-awkward baseline for the CMS dimuon quick start.

Same 2,304-event Zmumu.root selection as examples/cms_dimuon.py (opposite-sign muon pair,
invariant mass by hand, 60-bin 60-120 GeV histogram), computed with a plain local dask-awkward
pipeline instead of graphed on vine_graph: no distributed cluster, no vine_graph, just
``uproot.dask`` + ``dask.compute(scheduler="threads")``. Not wired into the package's own
requirements; run it with an environment that has dask-awkward and uproot installed (this repo's
graphed env does not include dask-awkward, since the executor and its examples never need it).

    python baselines/cms_dimuon_dask.py
    python baselines/cms_dimuon_dask.py --partitions 5   # match graphed's steps_per_file=5
"""

import argparse
import shutil
import time
import urllib.request
from pathlib import Path

import awkward as ak
import boost_histogram as bh
import dask
import numpy as np
import uproot

DATA_URL = "https://scikit-hep.org/uproot3/examples/Zmumu.root"
BRANCHES = ["E1", "px1", "py1", "pz1", "Q1", "E2", "px2", "py2", "pz2", "Q2"]


def prepare_data(directory):
    directory.mkdir(parents=True, exist_ok=True)
    root_path = directory / "Zmumu.root"
    if not root_path.exists():
        download_path = root_path.with_suffix(".root.part")
        request = urllib.request.Request(
            DATA_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; graphed-taskvine-example/1.0)"},
        )
        with urllib.request.urlopen(request) as response, download_path.open("wb") as target:
            shutil.copyfileobj(response, target)
        download_path.replace(root_path)
    return root_path


def build_and_compute(root_path, n_partitions, scheduler):
    t_import_done = time.perf_counter()

    events = uproot.dask(f"{root_path}:events", library="ak", steps_per_file=n_partitions)
    events = events[BRANCHES]

    opposite_sign = events[events.Q1 * events.Q2 < 0]
    mass2 = (
        (opposite_sign.E1 + opposite_sign.E2) ** 2
        - (opposite_sign.px1 + opposite_sign.px2) ** 2
        - (opposite_sign.py1 + opposite_sign.py2) ** 2
        - (opposite_sign.pz1 + opposite_sign.pz2) ** 2
    )
    mass = mass2**0.5

    t_build_done = time.perf_counter()

    n_graph_keys = len(mass.__dask_graph__())
    (mass_computed,) = dask.compute(mass, scheduler=scheduler)

    t_compute_done = time.perf_counter()

    histogram = bh.Histogram(bh.axis.Regular(60, 60.0, 120.0), storage=bh.storage.Int64())
    histogram.fill(ak.to_numpy(mass_computed))

    return {
        "histogram": histogram,
        "n_graph_keys": n_graph_keys,
        "build_s": t_build_done - t_import_done,
        "compute_s": t_compute_done - t_build_done,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("cms-dimuon-data"))
    parser.add_argument("--partitions", type=int, default=1, help="uproot.dask steps_per_file")
    parser.add_argument("--scheduler", default="threads", choices=["sync", "threads", "processes"])
    args = parser.parse_args()

    t_cold_start = time.perf_counter()

    root_path = prepare_data(args.data_dir)
    result = build_and_compute(root_path, args.partitions, args.scheduler)

    t_cold_end = time.perf_counter()

    histogram = result["histogram"]
    peak = histogram.axes[0].centers[np.argmax(histogram.values())]
    print(f"events in 60-120 GeV: {int(histogram.sum())}")
    print(f"highest bin center: {peak:.1f} GeV")
    print(f"dask graph keys: {result['n_graph_keys']} (partitions requested: {args.partitions})")
    print(f"graph build: {result['build_s']:.4f}s, compute: {result['compute_s']:.4f}s")
    print(f"cold wall time (data prep through histogram fill): {t_cold_end - t_cold_start:.4f}s")


if __name__ == "__main__":
    main()
