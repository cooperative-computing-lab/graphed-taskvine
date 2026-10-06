"""ATLAS H -> gamma gamma with Coffea virtual arrays and a process-pool map/reduce."""

import argparse
import json
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import boost_histogram as bh
import numpy as np
from coffea.nanoevents import BaseSchema, NanoEventsFactory
from coffea.processor import ProcessorABC


class DiphotonProcessor(ProcessorABC):
    def process(self, events):
        tight = events.isTightID_0 & events.isTightID_1
        pt_cut = (events.pt_0 > 50) & (events.pt_1 > 30)
        isolation = (events.ptcone20_0 / events.pt_0 < 0.055) & (events.ptcone20_1 / events.pt_1 < 0.055)
        eta_ok = ((abs(events.eta_0) < 1.52) | (abs(events.eta_0) > 1.37)) & (
            (abs(events.eta_1) < 1.52) | (abs(events.eta_1) > 1.37)
        )
        selected = events[tight & pt_cut & isolation & eta_ok]
        px = selected.pt_0 * np.cos(selected.phi_0) + selected.pt_1 * np.cos(selected.phi_1)
        py = selected.pt_0 * np.sin(selected.phi_0) + selected.pt_1 * np.sin(selected.phi_1)
        pz = selected.pt_0 * np.sinh(selected.eta_0) + selected.pt_1 * np.sinh(selected.eta_1)
        mass = ((selected.e_0 + selected.e_1) ** 2 - px**2 - py**2 - pz**2) ** 0.5
        selected, mass = selected[mass != 0], mass[mass != 0]
        mass = mass[(selected.pt_0 / mass > 0.35) & (selected.pt_1 / mass > 0.35)]
        histogram = bh.Histogram(
            bh.axis.Regular(60, 100.0, 160.0, metadata="diphoton mass [GeV]"),
            storage=bh.storage.Int64(),
        )
        histogram.fill(np.asarray(mass))
        return histogram

    def postprocess(self, accumulator):
        return accumulator


def process_file(path):
    events = NanoEventsFactory.from_parquet(str(path), schemaclass=BaseSchema, mode="virtual").events()
    return DiphotonProcessor().process(events)


def run(paths, workers):
    with ProcessPoolExecutor(max_workers=workers) as pool:
        partials = pool.map(process_file, paths)
        histogram = next(partials)
        for partial in partials:
            histogram += partial
    return histogram


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True, help="prepared flat Parquet files")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = sorted(args.data_dir.resolve().glob("*.parquet"))
    start = time.perf_counter()
    histogram = run(paths, args.workers)
    result = {
        "backend": "coffea-virtual-process-pool",
        "data_dir": str(args.data_dir.resolve()),
        "n_partitions": len(paths),
        "workers": args.workers,
        "run_s": time.perf_counter() - start,
        "bins_with_flow": histogram.values(flow=True).tolist(),
        "selected_in_range": int(histogram.sum()),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
