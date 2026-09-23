"""DV5: the ECF-calculator H->gamma gamma PFNano skim, recorded in graphed.

A port of the Dask/coffea ``ecf_calculator.py::analysis`` behind the DAGVine/SC26 hero run. The 22 GB
input dataset (800 ROOT files, one ``--copy-count 1`` copy of the DAGVine reproducibility archive) and
the original analysis come from
`JinZhou5042/sc26-dagvine-reproducibility <https://github.com/JinZhou5042/sc26-dagvine-reproducibility>`_.

What graphed records (projected, optimized, one IR per run):
    the softdrop-fix event cut, trigger OR, muon/electron/tau counting with tau-lepton dR cleaning,
    b-tag counting, Higgs gen matching (``nearest`` with a 0.2 threshold), the fat-jet selection,
    the event mask, and the final skim record.

What stays an External (opaque boundary) node:
    jet substructure -- building each good jet's PF constituents from ``FatJet_nConstituents`` slices
    of FatJetPFCands, fastjet C/A clustering, soft-drop grooming, energy correlators and the color
    ring. fastjet cannot run on awkward typetracers, so it is exactly the kind of node graphed treats
    as an External. Its output form is taken from running it once on a tiny synthetic event.

No coffea on the graphed side: NanoEvents schema behavior is replaced by explicit column access and
the vector formulas coffea uses (scikit-hep ``vector``).

This example needs ``fastjet``, ``vector``, and ``scipy`` in addition to the base requirements; they
are imported lazily (only where the substructure External actually runs), so importing this module
does not require them, but running the analysis does:

    python -m pip install fastjet vector scipy

It also needs ``uproot.graphed``, a source constructor from the
`graphed-org/uproot5-graphed-mvp <https://github.com/graphed-org/uproot5-graphed-mvp>`_ development
fork rather than the released ``uproot`` PyPI package this repository's ``requirements.txt`` installs.
Install it (over the released ``uproot``) before running this example:

    python -m pip install --no-deps \
        "uproot @ git+https://github.com/graphed-org/uproot5-graphed-mvp.git"

By default this downloads nothing and processes the first few files of ``--data-dir`` (downloading
the full ~22 GB dataset from Zenodo on first use, since it is only published as one archive -- there
is no way to fetch just a handful of files). Point ``--data-dir`` at an existing directory of ROOT
files (for example a previously extracted ``hgg_0``) to skip the download entirely:

    python -m examples.dv5 --data-dir /path/to/hgg_0 --files 5
"""

from __future__ import annotations

import subprocess
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import awkward as ak
import numpy as np
from graphed_executors.taskvine_backend import TaskVineExecutor

# The 2017 trigger list from the original analysis's triggers.json (single "2017" entry).
TRIGGERS_2017 = ["PFHT1050", "AK8PFJet400_TrimMass30", "AK8PFHT800_TrimMass50", "PFJet500", "AK8PFJet500"]

ZENODO_RECORD = "18665133"
ZENODO_FILE = "hgg.tar.zst"

# statusFlags bits (coffea GenParticle.FLAGS): fromHardProcess = 8, isLastCopy = 13
_FROM_HARD_PROCESS_AND_LAST_COPY = (1 << 8) | (1 << 13)

NEEDED_BRANCHES = sorted(
    {
        "FatJet_pt",
        "FatJet_eta",
        "FatJet_phi",
        "FatJet_msoftdrop",
        "FatJet_nConstituents",
        "FatJet_particleNet_HbbvsQCD",
        "FatJet_particleNetMD_QCD",
        "FatJetPFCands_pFCandsIdx",
        "PFCands_pt",
        "PFCands_eta",
        "PFCands_phi",
        "PFCands_mass",
        "PFCands_puppiWeight",
        "Muon_pt",
        "Muon_eta",
        "Muon_phi",
        "Muon_pfRelIso04_all",
        "Muon_looseId",
        "Electron_pt",
        "Electron_eta",
        "Electron_phi",
        "Electron_cutBased",
        "Tau_pt",
        "Tau_eta",
        "Tau_phi",
        "Tau_rawIso",
        "Tau_idDeepTau2017v2p1VSjet",
        "Jet_pt",
        "Jet_eta",
        "Jet_btagDeepFlavB",
        "GenPart_pdgId",
        "GenPart_statusFlags",
        "GenPart_eta",
        "GenPart_phi",
        *(f"HLT_{t}" for t in TRIGGERS_2017),
    }
)


# ---- data prep: download the DV5 hgg dataset from Zenodo if --data-dir is empty -------------------
def prepare_data(directory: Path, n_files: int) -> list[str]:
    directory.mkdir(parents=True, exist_ok=True)
    root_files = sorted(str(p) for p in directory.glob("*.root"))
    if not root_files:
        _download_and_extract(directory)
        root_files = sorted(str(p) for p in directory.glob("*.root"))
    if not root_files:
        raise FileNotFoundError(f"no ROOT files in {directory} after download")
    return root_files if n_files == 0 else root_files[:n_files]


def _download_and_extract(directory: Path) -> None:
    archive = directory / ZENODO_FILE
    if not archive.exists():
        url = f"https://zenodo.org/api/records/{ZENODO_RECORD}/files/{ZENODO_FILE}/content"
        print(f"downloading {url} (~22 GB, one copy of the DV5 hgg dataset)...", flush=True)
        subprocess.run(["curl", "-fL", url, "-o", str(archive)], check=True)
    print(f"extracting {archive} into {directory}...", flush=True)
    subprocess.run(
        ["tar", "--zstd", "-xf", str(archive), "-C", str(directory), "--strip-components=1"], check=True
    )
    archive.unlink()


# ---- kinematics (coffea/vector formulas, on recorded arrays) --------------------------------------
def delta_phi(a: Any, b: Any) -> Any:
    return (a - b + np.pi) % (2 * np.pi) - np.pi


def delta_r(eta1: Any, phi1: Any, eta2: Any, phi2: Any) -> Any:
    return np.sqrt((eta1 - eta2) ** 2 + delta_phi(phi1, phi2) ** 2)


def _pair_dr(gak: Any, a: Any, b: Any) -> Any:
    """coffea ``metric_table``: dR over the nested cartesian product (shape: a x b per event)."""
    pairs = gak.cartesian([a, b], nested=True)
    return delta_r(pairs["0"].eta, pairs["0"].phi, pairs["1"].eta, pairs["1"].phi)


# ---- the recorded analysis -------------------------------------------------------------------------
def record_skim(g: Any, *, ecf_upper_bound: int = 3, puppi_constituents: bool = True) -> Any:
    """Record DV5's skim over the uproot.graphed source ``g``; returns the deferred skim record."""
    from graphed.awkward import gak

    available = set(gak.fields(g))

    # events = events[ak.all(ak.num(FatJet.constituents.pf, axis=2) > 0, axis=1)]
    # (constituents are FatJet_nConstituents slices, so the count IS nConstituents)
    softdrop_fix = gak.all(g["FatJet_nConstituents"] > 0, axis=1)

    def ev(name: str) -> Any:
        return g[name][softdrop_fix]

    fj_pt, fj_eta, fj_phi, fj_msd = (
        ev("FatJet_pt"),
        ev("FatJet_eta"),
        ev("FatJet_phi"),
        ev("FatJet_msoftdrop"),
    )

    trigger = gak.zeros_like(gak.firsts(fj_pt), dtype="bool")
    for t in TRIGGERS_2017:
        if f"HLT_{t}" in available:
            trigger = trigger | ev(f"HLT_{t}")
    trigger = gak.fill_none(trigger, False)

    goodmuon = (
        (ev("Muon_pt") > 10)
        & (abs(ev("Muon_eta")) < 2.4)
        & (ev("Muon_pfRelIso04_all") < 0.25)
        & ev("Muon_looseId")
    )
    nmuons = gak.sum(goodmuon, axis=1)

    goodelectron = (ev("Electron_pt") > 10) & (abs(ev("Electron_eta")) < 2.5) & (ev("Electron_cutBased") >= 2)
    nelectrons = gak.sum(goodelectron, axis=1)

    tau = gak.zip({"eta": ev("Tau_eta"), "phi": ev("Tau_phi")})
    muons = gak.zip({"eta": ev("Muon_eta"), "phi": ev("Muon_phi")})[goodmuon]
    electrons = gak.zip({"eta": ev("Electron_eta"), "phi": ev("Electron_phi")})[goodelectron]
    ntaus = gak.sum(
        (
            (ev("Tau_pt") > 20)
            & (abs(ev("Tau_eta")) < 2.3)
            & (ev("Tau_rawIso") < 5)
            & (ev("Tau_idDeepTau2017v2p1VSjet"))  # uint8: `bool & uint8` is bitwise, as in the original
            & gak.all(_pair_dr(gak, tau, muons) > 0.4, axis=2)
            & gak.all(_pair_dr(gak, tau, electrons) > 0.4, axis=2)
        ),
        axis=1,
    )
    onemuon = (nmuons == 1) & (nelectrons == 0) & (ntaus == 0)
    region = onemuon

    jet_pt, jet_eta = ev("Jet_pt"), ev("Jet_eta")
    btag_count = gak.sum(ev("Jet_btagDeepFlavB")[(jet_pt > 20) & (abs(jet_eta) < 2.4)] > 0.3040, axis=1)

    flags = ev("GenPart_statusFlags")
    is_higgs = (ev("GenPart_pdgId") == 25) & (
        (flags & _FROM_HARD_PROCESS_AND_LAST_COPY) == _FROM_HARD_PROCESS_AND_LAST_COPY
    )
    genhiggs = gak.zip({"eta": ev("GenPart_eta"), "phi": ev("GenPart_phi")})[is_higgs]
    # coffea nearest(threshold=0.2): argmin dR, firsts, mask by metric <= threshold
    fatjets = gak.zip({"eta": fj_eta, "phi": fj_phi})
    pairs = gak.cartesian([fatjets, genhiggs], nested=True)
    mval = delta_r(pairs["0"].eta, pairs["0"].phi, pairs["1"].eta, pairs["1"].phi)
    mmin = gak.argmin(mval, axis=2, keepdims=True)
    nearest_eta = gak.firsts(pairs["1"].eta[mmin], axis=2)
    metric = gak.firsts(mval[mmin], axis=2)
    parents = gak.mask(nearest_eta, metric <= 0.2)
    gen_match = ~gak.is_none(parents, axis=1)

    fatjet_select = (fj_pt > 400) & (abs(fj_eta) < 2.4) & (fj_msd > 40) & (fj_msd < 200) & region & trigger
    has_goodjet = ~gak.is_none(gak.firsts(fj_pt[fatjet_select]))

    def good(x: Any) -> Any:
        return x[fatjet_select][has_goodjet]

    substructure = record_substructure(
        g.session,
        good(gak.local_index(fj_pt, axis=1)),
        ev("FatJet_nConstituents")[has_goodjet],
        ev("FatJetPFCands_pFCandsIdx")[has_goodjet],
        [ev(f"PFCands_{v}")[has_goodjet] for v in ("pt", "eta", "phi", "mass")]
        + ([ev("PFCands_puppiWeight")[has_goodjet]] if puppi_constituents else []),
        JetSubstructure(ecf_upper_bound=ecf_upper_bound, puppi=puppi_constituents),
    )

    return gak.zip(
        {
            "Color_Ring": substructure["Color_Ring"],
            "ECFs": substructure["ECFs"],
            "msoftdrop": good(fj_msd),
            "pt": good(fj_pt),
            "btag_ak4s": btag_count[has_goodjet],
            "pn_HbbvsQCD": good(ev("FatJet_particleNet_HbbvsQCD")),
            "pn_md": good(ev("FatJet_particleNetMD_QCD")),
            "matching": gen_match[has_goodjet],
        },
        depth_limit=1,
    )


# ---- the External: PF constituents + fastjet ----------------------------------------------------
def ecf_names(ecf_upper_bound: int) -> list[str]:
    from scipy.special import binom

    return [
        f"{v}e{n}^{b / 10}"
        for n in range(2, ecf_upper_bound + 1)
        for v in range(1, int(binom(n, 2)) + 1)
        for b in range(5, 45, 5)
    ]


def goodjet_constituents(goodidx, ncons, pfidx, pt, eta, phi, mass, puppi_weight=None):
    """``ak.flatten(goodjets.constituents.pf, axis=1)``: one list of PF candidates per good jet.

    coffea's PFNano schema makes jet j's constituents the contiguous FatJetPFCands slice given by
    FatJet_nConstituents (counts2nestedindex), then follows pFCandsIdx into PFCands."""
    import vector

    n_events = len(goodidx)
    ncons_flat = np.asarray(ak.flatten(ncons), dtype=np.int64)
    fj_offsets = np.concatenate([[0], np.cumsum(np.asarray(ak.num(ncons, axis=1), dtype=np.int64))])
    cons_cumsum = np.concatenate([[0], np.cumsum(ncons_flat)])
    fjpfc_offsets = np.concatenate([[0], np.cumsum(np.asarray(ak.num(pfidx, axis=1), dtype=np.int64))])
    pf_offsets = np.concatenate([[0], np.cumsum(np.asarray(ak.num(pt, axis=1), dtype=np.int64))])

    goods_per_event = np.asarray(ak.num(goodidx, axis=1), dtype=np.int64)
    good_event = np.repeat(np.arange(n_events), goods_per_event)
    good_flat = fj_offsets[good_event] + np.asarray(ak.flatten(goodidx), dtype=np.int64)

    counts = ncons_flat[good_flat]
    local_start = cons_cumsum[good_flat] - cons_cumsum[fj_offsets[good_event]]
    starts = fjpfc_offsets[good_event] + local_start
    jet_of = np.repeat(np.arange(len(counts)), counts)
    within = np.arange(int(counts.sum())) - np.repeat(np.cumsum(counts) - counts, counts)
    fjpfc_pos = starts[jet_of] + within

    pf_local = np.asarray(ak.flatten(pfidx), dtype=np.int64)[fjpfc_pos]
    pf_global = pf_offsets[good_event[jet_of]] + pf_local

    def take(arr):
        return np.asarray(ak.flatten(arr))[pf_global]

    cand_pt = take(pt) if puppi_weight is None else take(pt) * take(puppi_weight)
    cands = ak.zip(
        {"pt": cand_pt, "eta": take(eta), "phi": take(phi), "mass": take(mass)},
        with_name="Momentum4D",
        behavior=vector.backends.awkward.behavior,
    )
    return ak.unflatten(cands, counts), goods_per_event


def color_ring(pf, cluster_val):
    """``variable_functions.color_ring`` with ``fatjet.constituents.pf`` already flattened to ``pf``
    and coffea's LorentzVector replaced by scikit-hep vector's Momentum4D (the same compute kernels)."""
    import fastjet
    import vector

    jetdef = fastjet.JetDefinition(fastjet.cambridge_algorithm, cluster_val)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    subjets = cluster.inclusive_jets()
    vec = ak.zip(
        {"x": subjets.px, "y": subjets.py, "z": subjets.pz, "t": subjets.E},
        with_name="Momentum4D",
        behavior=vector.backends.awkward.behavior,
    )
    vec = ak.pad_none(vec, 3)
    vec["norm3"] = np.sqrt(vec.dot(vec))
    vec["idx"] = ak.local_index(vec)
    i, j = ak.unzip(ak.combinations(vec, 2))
    best = ak.argmax(abs((i + j).pt), axis=1, keepdims=True)
    order_check = ak.concatenate([i[best].pt, j[best].pt], axis=1)
    leading = ak.argmax(order_check, axis=1, keepdims=True)
    subleading = ak.argmin(order_check, axis=1, keepdims=True)
    leading_particles = ak.concatenate([i[best], j[best]], axis=1)
    cut = (vec.idx != ak.firsts(leading_particles.idx)) & (
        vec.idx != ak.firsts(ak.sort(leading_particles.idx, ascending=False))
    )
    everything_else = vec[cut]
    total_pt = ak.sum(vec.pt, axis=1)
    everything_else["momentum_fraction"] = (everything_else.pt) / total_pt
    everything_else["weighted_eta"] = everything_else.eta * everything_else.momentum_fraction
    everything_else["weighted_phi"] = everything_else.phi * everything_else.momentum_fraction
    weighted_average_eta = ak.sum(everything_else.weighted_eta, axis=1) / ak.num(everything_else, axis=1)
    weighted_average_phi = ak.sum(everything_else.weighted_phi, axis=1) / ak.num(everything_else, axis=1)
    leg1 = ak.firsts(leading_particles[leading])
    leg2 = ak.firsts(leading_particles[subleading])
    a13 = (((leg1.eta * (leg1.pt / total_pt)) - weighted_average_eta) ** 2) + (
        ((leg1.phi * (leg1.pt / total_pt)) - weighted_average_phi) ** 2
    )
    a23 = (((leg2.eta * (leg2.pt / total_pt)) - weighted_average_eta) ** 2) + (
        ((leg2.phi * (leg2.pt / total_pt)) - weighted_average_phi) ** 2
    )
    a12 = (((leg1.eta * (leg1.pt / total_pt)) - (leg2.eta * (leg2.pt / total_pt))) ** 2) + (
        ((leg1.phi * (leg1.pt / total_pt)) - (leg2.phi * (leg2.pt / total_pt))) ** 2
    )
    return (a13 + a23) / (a12)


def energy_correlators(pf, ecf_upper_bound):
    import fastjet
    from scipy.special import binom

    jetdef = fastjet.JetDefinition(fastjet.cambridge_algorithm, 0.8)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    ecfs = {}
    for n in range(2, ecf_upper_bound + 1):
        for v in range(1, int(binom(n, 2)) + 1):
            for b in range(5, 45, 5):
                # normalized=False: the DV5 hero run called this on dask-awkward arrays, whose
                # fastjet wrapper defaults to normalized=False (the eager API defaults to True).
                ecfs[f"{v}e{n}^{b / 10}"] = softdrop_cluster.exclusive_jets_energy_correlator(
                    func="generic", npoint=n, angles=v, beta=b / 10, normalized=False
                )
    return ecfs


@dataclass(frozen=True)
class JetSubstructure:
    """External evaluator: per-event lists of good jets -> {Color_Ring, ECFs{...}} per good jet."""

    ecf_upper_bound: int = 3
    puppi: bool = True

    def __call__(self, goodidx, ncons, pfidx, pt, eta, phi, mass, puppi_weight=None):
        pf, goods_per_event = goodjet_constituents(goodidx, ncons, pfidx, pt, eta, phi, mass, puppi_weight)
        if len(pf) == 0:
            per_jet = self._empty()
        else:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", "invalid value")
                warnings.filterwarnings("ignore", "divide by zero")
                warnings.filterwarnings("ignore", "dcut")
                ring = color_ring(pf, cluster_val=0.4)
                ecfs = energy_correlators(pf, self.ecf_upper_bound)
            per_jet = ak.zip(
                {"Color_Ring": ring, "ECFs": ak.zip({k: ak.Array(v) for k, v in ecfs.items()})},
                depth_limit=1,
            )
        return ak.unflatten(per_jet, goods_per_event)

    def _empty(self):
        ring = ak.mask(ak.Array(np.zeros(0, dtype=np.float64)), np.zeros(0, dtype=bool))
        ecfs = ak.zip({k: np.zeros(0, dtype=np.float64) for k in ecf_names(self.ecf_upper_bound)})
        return ak.zip({"Color_Ring": ring, "ECFs": ecfs}, depth_limit=1)


def _synthetic_inputs(puppi: bool):
    """One event, one good jet with six spread-out constituents: enough for fastjet to run."""
    rng = np.random.default_rng(5)
    n = 6
    cols = [
        ak.Array([[0]]),
        ak.Array([np.array([n], dtype=np.uint8)]),
        ak.Array([np.arange(n, dtype=np.int32)]),
        ak.Array([np.array([120, 90, 60, 40, 25, 15], dtype=np.float32)]),
        ak.Array([rng.normal(0.0, 0.3, n).astype(np.float32)]),
        ak.Array([rng.normal(0.0, 0.3, n).astype(np.float32)]),
        ak.Array([np.full(n, 0.14, dtype=np.float32)]),
    ]
    if puppi:
        cols.append(ak.Array([np.ones(n, dtype=np.float32)]))
    return cols


def record_substructure(session, goodidx, ncons, pfidx, pf_columns, evaluator: JetSubstructure):
    from graphed.awkward.backend import AwkwardForm
    from graphed.awkward.payloads import opaque_callable_descriptor

    sample = evaluator(*_synthetic_inputs(evaluator.puppi))
    form = AwkwardForm(ak.Array(sample.layout.to_typetracer(forget_length=True)))
    name = f"dv5.JetSubstructure(ecf_upper_bound={evaluator.ecf_upper_bound},puppi={int(evaluator.puppi)})"
    return session.record_external(
        "map",
        evaluator,
        [goodidx, ncons, pfidx, *pf_columns],
        {"fn": name},
        descriptor=opaque_callable_descriptor(name),
        form=form,
    )


# ---- plan: one IR over the input files, dispatched by dataset at reduce time ----------------------
def first_output(values):
    return values[0]


def _no_value():
    return None


def _concat(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return ak.concatenate([a, b])


def dataset_of(uri: str) -> str:
    return Path(uri).parent.name


@dataclass(frozen=True)
class PerDataset:
    """Wraps the compiled per-partition reduction: keys the partial by dataset."""

    inner: Any

    def __call__(self, partition, resources):
        skim = self.inner(partition, resources)
        ds = dataset_of(partition.uri)
        return {ds: {"n_selected": len(skim), "skim": skim}}


def merge_datasets(a, b):
    out = dict(a)
    for ds, entry in b.items():
        if ds not in out:
            out[ds] = entry
            continue
        prev = out[ds]
        out[ds] = {
            "n_selected": prev["n_selected"] + entry["n_selected"],
            "skim": _concat(prev["skim"], entry["skim"]),
        }
    return out


def empty_datasets():
    return {}


def build_plan(files: list[str], *, ecf_upper_bound=3, puppi_constituents=True, steps_per_file=1):
    """Record + compile DV5 and return ``(plan, info)``; ``info`` carries the build timings."""
    import graphed
    import uproot
    from graphed.core import GraphStore, Partition
    from graphed.core.execution import Plan, Task

    if not files:
        raise ValueError("no input files")
    info: dict[str, Any] = {"n_files": len(files), "steps_per_file": steps_per_file}

    t0 = time.perf_counter()
    # Record against the first file only; every other file is handed a blind Partition (never
    # opened while planning). This is why graphed's build time stays flat regardless of file count.
    g = uproot.graphed([f"{files[0]}:Events"], library="ak")
    t1 = time.perf_counter()
    skim = record_skim(g, ecf_upper_bound=ecf_upper_bound, puppi_constituents=puppi_constituents)
    t2 = time.perf_counter()
    partitions = [
        Partition.blind(f, "Events", s, steps_per_file) for f in files for s in range(steps_per_file)
    ]
    base = graphed.aggregate_plan(
        skim, reduce=first_output, combine=_concat, empty=_no_value, partitions=partitions
    )
    t3 = time.perf_counter()
    plan = Plan(
        process=PerDataset(base.process),
        combine=merge_datasets,
        empty=empty_datasets,
        tasks=tuple(Task(i, t.partition) for i, t in enumerate(base.tasks)),
    )

    kinds: dict[str, int] = {}
    for node in GraphStore.deserialize(bytes(base.process.ir)).nodes():
        kinds[node["kind"]] = kinds.get(node["kind"], 0) + 1
    info.update(
        source_open_s=t1 - t0,
        record_s=t2 - t1,
        compile_s=t3 - t2,
        build_total_s=t3 - t0,
        n_recorded_nodes=len(g.session._store.nodes()),
        ir_nodes_by_kind=kinds,
        n_tasks=len(plan.tasks),
    )
    return plan, info


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distributed", action="store_true", help="use a TaskVine worker")
    parser.add_argument("--data-dir", type=Path, default=Path("dv5-data"))
    parser.add_argument("--files", type=int, default=5, help="first N ROOT files to process; 0 = all")
    parser.add_argument("--libcores", type=int, default=16)
    args = parser.parse_args()

    files = prepare_data(args.data_dir, args.files)
    plan, info = build_plan(files)

    with TaskVineExecutor(
        manager_name="graphed-dv5",
        port=(9100, 9199) if args.distributed else 0,
        local=not args.distributed,
        libcores=args.libcores,
        wait_for_workers=1 if args.distributed else 0,
    ) as executor:
        t0 = time.perf_counter()
        result = executor.run(plan)
        run_s = time.perf_counter() - t0

    n_selected = sum(entry["n_selected"] for entry in result.value.values())
    n_ir_nodes = sum(info["ir_nodes_by_kind"].values())
    print(f"files processed: {len(files)}")
    print(f"recorded nodes -> IR nodes: {info['n_recorded_nodes']} -> {n_ir_nodes}")
    print(f"selected events: {n_selected}")
    print(f"process/combine tasks: {result.n_partitions}/{result.n_combines}")
    print(f"build/run seconds: {info['build_total_s']:.2f}/{run_s:.2f}")


if __name__ == "__main__":
    main()
