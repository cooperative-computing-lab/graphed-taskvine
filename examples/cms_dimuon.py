"""Reconstruct the Z -> mu mu mass peak from a small CMS open-data ROOT file."""

import argparse
import shutil
import urllib.request
from pathlib import Path

import awkward as ak
import boost_histogram as bh
import graphed_histogram as gh
import uproot
from graphed import Session
from graphed.awkward import AwkwardBackend, from_parquet
from graphed_executors.taskvine_backend import TaskVineExecutor

DATA_URL = "https://scikit-hep.org/uproot3/examples/Zmumu.root"
BRANCHES = ["E1", "px1", "py1", "pz1", "Q1", "E2", "px2", "py2", "pz2", "Q2"]


def prepare_data(directory):
    directory.mkdir(parents=True, exist_ok=True)
    root_path = directory / "Zmumu.root"
    parquet_path = directory / "Zmumu.parquet"
    if not root_path.exists():
        download_path = root_path.with_suffix(".root.part")
        request = urllib.request.Request(
            DATA_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; graphed-taskvine-example/1.0)"},
        )
        with urllib.request.urlopen(request) as response, download_path.open("wb") as target:
            shutil.copyfileobj(response, target)
        download_path.replace(root_path)
    if not parquet_path.exists():
        events = uproot.open(f"{root_path}:events").arrays(BRANCHES)
        ak.to_parquet(events, parquet_path)
    return parquet_path


def build_plan(parquet_path):
    session = Session(AwkwardBackend())
    events = from_parquet(session, "events", str(parquet_path))
    opposite_sign = events[events.Q1 * events.Q2 < 0]
    mass2 = (
        (opposite_sign.E1 + opposite_sign.E2) ** 2
        - (opposite_sign.px1 + opposite_sign.px2) ** 2
        - (opposite_sign.py1 + opposite_sign.py2) ** 2
        - (opposite_sign.pz1 + opposite_sign.pz2) ** 2
    )
    mass = mass2**0.5
    histogram = gh.boost.Histogram(
        bh.axis.Regular(60, 60.0, 120.0, metadata="dimuon mass [GeV]"),
        storage=bh.storage.Int64(),
    )
    histogram.fill(mass)
    return gh.plan({"dimuon_mass": histogram}, steps_per_file=5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--distributed", action="store_true", help="use a TaskVine worker")
    parser.add_argument("--data-dir", type=Path, default=Path("cms-dimuon-data"))
    args = parser.parse_args()

    plan = build_plan(prepare_data(args.data_dir))
    with TaskVineExecutor(
        manager_name="graphed-dimuon",
        port=(9100, 9199) if args.distributed else 0,
        local=not args.distributed,
        libcores=4,
        wait_for_workers=1 if args.distributed else 0,
    ) as executor:
        result = executor.run(plan)

    histogram = gh.unpack(result.value)["dimuon_mass"]
    peak = histogram.axes[0].centers[histogram.values().argmax()]
    print(f"events in 60-120 GeV: {int(histogram.sum())}")
    print(f"highest bin center: {peak:.1f} GeV")
    print(f"process/combine tasks: {result.n_partitions}/{result.n_combines}")


if __name__ == "__main__":
    main()
