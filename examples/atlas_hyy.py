"""Reconstruct the H -> gamma gamma diphoton mass peak from real ATLAS 13 TeV open data.

Downloads the 16 public GamGam ROOT files from the "2025e-13tev-beta" ATLAS Open Data release
(https://opendata.cern.ch, ~9.86 GB total) through atlasopenmagic/fsspec, applies the same photon
selection as the ATLAS Open Data H->yy notebook (tight ID, pT, isolation, eta transition veto,
diphoton mass window), and fills a 100-160 GeV histogram through this executor.
"""

import argparse
from pathlib import Path

import awkward as ak
import boost_histogram as bh
import graphed_histogram as gh
import numpy as np
import uproot
from graphed import Session
from graphed.awkward import AwkwardBackend, from_parquet
from graphed_executors.taskvine_backend import TaskVineExecutor

RELEASE = "2025e-13tev-beta"
TREE_NAME = "analysis"
BRANCHES = ["photon_pt", "photon_eta", "photon_phi", "photon_e", "photon_isTightID", "photon_ptcone20"]


def _download(directory):
    import aiohttp
    import atlasopenmagic as atom
    import fsspec

    atom.set_release(RELEASE)
    urls = atom.get_urls("data", "GamGam", protocol="https", cache=True)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=120, sock_read=3600)
    root_paths = []
    for index, url in enumerate(urls, 1):
        print(f"downloading {index}/{len(urls)}", flush=True)
        cached = fsspec.open_local(
            url,
            simplecache={"cache_storage": str(directory), "same_names": True},
            https={"client_kwargs": {"timeout": timeout}},
        )
        root_paths.append(Path(cached))
    return root_paths


def _flatten_leading_two(root_path, parquet_path):
    events = uproot.open(f"{root_path}:{TREE_NAME}").arrays(BRANCHES)
    events = events[ak.num(events.photon_pt, axis=1) >= 2]
    flat = {}
    for branch in BRANCHES:
        name = branch.removeprefix("photon_")
        flat[f"{name}_0"] = ak.to_numpy(events[branch][:, 0])
        flat[f"{name}_1"] = ak.to_numpy(events[branch][:, 1])
    ak.to_parquet(ak.Array(flat), parquet_path)


def prepare_data(directory):
    directory.mkdir(parents=True, exist_ok=True)
    root_paths = _download(directory)
    parquet_paths = []
    for root_path in root_paths:
        parquet_path = root_path.with_suffix(".parquet")
        if not parquet_path.exists():
            _flatten_leading_two(root_path, parquet_path)
        parquet_paths.append(parquet_path)
    return parquet_paths


def build_plan(parquet_paths):
    session = Session(AwkwardBackend())
    events = from_parquet(session, "events", [str(p) for p in parquet_paths])

    tight = events.isTightID_0 & events.isTightID_1
    pt_cut = (events.pt_0 > 50) & (events.pt_1 > 30)
    isolation = (events.ptcone20_0 / events.pt_0 < 0.055) & (events.ptcone20_1 / events.pt_1 < 0.055)

    def in_transition_veto(eta):
        return (abs(eta) < 1.52) | (abs(eta) > 1.37)

    eta_ok = in_transition_veto(events.eta_0) & in_transition_veto(events.eta_1)

    selected = events[tight & pt_cut & isolation & eta_ok]

    px0, py0, pz0 = (
        selected.pt_0 * np.cos(selected.phi_0),
        selected.pt_0 * np.sin(selected.phi_0),
        selected.pt_0 * np.sinh(selected.eta_0),
    )
    px1, py1, pz1 = (
        selected.pt_1 * np.cos(selected.phi_1),
        selected.pt_1 * np.sin(selected.phi_1),
        selected.pt_1 * np.sinh(selected.eta_1),
    )
    mass2 = (selected.e_0 + selected.e_1) ** 2 - (px0 + px1) ** 2 - (py0 + py1) ** 2 - (pz0 + pz1) ** 2
    mass = mass2**0.5

    with_mass = selected[mass != 0]
    mass_selected = mass[mass != 0]
    iso_mass = (with_mass.pt_0 / mass_selected > 0.35) & (with_mass.pt_1 / mass_selected > 0.35)
    final_mass = mass_selected[iso_mass]

    histogram = gh.boost.Histogram(
        bh.axis.Regular(60, 100.0, 160.0, metadata="diphoton mass [GeV]"),
        storage=bh.storage.Int64(),
    )
    histogram.fill(final_mass)
    return gh.plan({"diphoton_mass": histogram})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--distributed", action="store_true", help="use a TaskVine worker")
    parser.add_argument("--data-dir", type=Path, default=Path("atlas-hyy-data"))
    args = parser.parse_args()

    plan = build_plan(prepare_data(args.data_dir))
    with TaskVineExecutor(
        manager_name="graphed-hyy",
        port=(9100, 9199) if args.distributed else 0,
        local=not args.distributed,
        libcores=4,
        wait_for_workers=1 if args.distributed else 0,
    ) as executor:
        result = executor.run(plan)

    histogram = gh.unpack(result.value)["diphoton_mass"]
    peak = histogram.axes[0].centers[histogram.values().argmax()]
    print(f"events in 100-160 GeV: {int(histogram.sum())}")
    print(f"highest bin center: {peak:.1f} GeV")
    print(f"process/combine tasks: {result.n_partitions}/{result.n_combines}")


if __name__ == "__main__":
    main()
