"""Traditional local-Dask baseline for the ATLAS H -> gamma gamma diphoton workflow.

Reads the identical per-file flat Parquet files that examples/atlas_hyy.py's vine_graph run
path produces and consumes (leading-two-photon pt/eta/phi/e/isTightID/ptcone20 columns, one
partition per input ROOT file), applies the identical selection and by-hand invariant-mass
formula, and fills the identical 100-160 GeV histogram -- but with a plain local
dask.dataframe pipeline instead of graphed + TaskVine. dask.dataframe (not dask_awkward) is
used because the data here is already flattened to fixed leading-two-photon columns, so it
is an ordinary flat/columnar table, not a jagged array; dask.dataframe is the natural traditional
tool for that shape and mirrors the row-wise vectorized style a physicist would actually write.

    python -m benchmarks.atlas_hyy_dask --data-dir examples/atlas-hyy-data
"""

import argparse
import time
from pathlib import Path

import boost_histogram as bh
import dask.dataframe as dd
import numpy as np

from examples.atlas_hyy import prepare_data


def in_transition_veto(eta):
    return (eta.abs() < 1.52) | (eta.abs() > 1.37)


def build_graph(parquet_paths):
    ddf = dd.read_parquet([str(p) for p in parquet_paths])

    tight = ddf.isTightID_0 & ddf.isTightID_1
    pt_cut = (ddf.pt_0 > 50) & (ddf.pt_1 > 30)
    isolation = (ddf.ptcone20_0 / ddf.pt_0 < 0.055) & (ddf.ptcone20_1 / ddf.pt_1 < 0.055)
    eta_ok = in_transition_veto(ddf.eta_0) & in_transition_veto(ddf.eta_1)
    selected = ddf[tight & pt_cut & isolation & eta_ok]

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
    selected = selected.assign(mass=mass2**0.5)

    with_mass = selected[selected.mass != 0]
    iso_mass = (with_mass.pt_0 / with_mass.mass > 0.35) & (with_mass.pt_1 / with_mass.mass > 0.35)
    return with_mass[iso_mass].mass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("atlas-hyy-data"))
    parser.add_argument(
        "--workers", type=int, default=4, help="processes, as many as the cores of one vine_graph worker"
    )
    parser.add_argument("--scheduler", default="processes", choices=["processes", "threads", "sync"])
    args = parser.parse_args()

    t0 = time.perf_counter()
    parquet_paths = prepare_data(args.data_dir)
    t1 = time.perf_counter()

    mass = build_graph(parquet_paths)
    n_graph_keys = len(dict(mass.__dask_graph__()))
    t2 = time.perf_counter()

    final_mass = mass.compute(scheduler=args.scheduler, num_workers=args.workers)
    t3 = time.perf_counter()

    histogram = bh.Histogram(
        bh.axis.Regular(60, 100.0, 160.0, metadata="diphoton mass [GeV]"), storage=bh.storage.Int64()
    )
    histogram.fill(final_mass.to_numpy())
    peak = histogram.axes[0].centers[histogram.view().argmax()]

    print(f"data prep (download/convert, cached where possible): {t1 - t0:.2f} s")
    print(f"graph build: {t2 - t1:.2f} s")
    print(f"compute ({args.scheduler}, {args.workers} workers): {t3 - t2:.2f} s")
    print(f"dask graph keys: {n_graph_keys}")
    print(f"events in 100-160 GeV: {int(histogram.sum())}")
    print(f"highest bin center: {peak:.1f} GeV")


if __name__ == "__main__":
    main()
