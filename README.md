# graphed-taskvine

This repository is the experimental home for Graphed + TaskVine HEP workflows, comparison
pipelines, baselines, and measured results. New experiments belong under [`baselines/`](baselines/).

The TaskVine executor is maintained in
[`JinZhou5042/graphed-executors`](https://github.com/JinZhou5042/graphed-executors/tree/vine-graph/src/graphed_executors/taskvine_backend)
(branch `vine-graph`). Every workflow and test here imports its public
`graphed_executors.taskvine_backend.TaskVineExecutor`; this repository keeps no executor of its own.
A `graphed` `Plan` runs on [TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) through
vine_graph (`ndcctools.taskvine.vine_graph`), the CCTools task-graph executor.

## Architecture

```text
                              DRIVER

  analysis code
       |
       | record once
       v
  graphed operation graph
       |
       | optimize: DCE / CSE / rewrites / stage fusion
       v
  compiled IR ----------------------------------------------------+
       |                                                          |
       | aggregate_plan                                           |
       v                                                          |
  Plan                                                            |
    process(partition, resources)  <--- shared compiled IR --------+
    tasks = [Task(key, Partition), ...]
    combine(left, right)
    empty()
       |
       | TaskVineExecutor.run: graphed-executors' SubmitRunner
       v
  vine_graph Executor (a TaskVine manager)
       |
       +-- leaf 0: process(partition 0) --+
       +-- leaf 1: process(partition 1) --+-- fixed binary combine tree --> result
       +-- leaf 2: process(partition 2) --+
       +-- ...

                              WORKER

  one leaf task
       |
       +-- read one data partition
       +-- evaluate every fused IR stage for that partition
       +-- evaluate any External nodes
       +-- return one partial result
```

There are two distinct granularities:

- graphed fuses ordinary operation subgraphs into local IR stages;
- this executor runs the complete per-partition IR as one TaskVine task.

TaskVine does not receive one task per recorded array operation. For `N` fixed partitions it sees
`N` leaf tasks and `N - 1` binary combine tasks, or `2N - 1` tasks. This is intentionally coarser
than a Dask graph that exposes many array, schema, and I/O keys per partition.

vine_graph moves each partial result from the worker that made it to the worker that combines it,
keeps a copy in the manager's vault, and recomputes a result whose workers were lost, so a run
survives worker churn without restarting. Each core of a worker runs one task at a time.

## Requirements

- Python 3.11 or newer;
- a CCTools build that provides `ndcctools.taskvine.vine_graph` (below);
- the graphed release and the TaskVine executor commit pinned in [`requirements.txt`](requirements.txt).

## Install vine_graph

vine_graph is a CCTools development interface, so the released `ndcctools` Conda package does not
include it. Build the `vine-graph-graphed` branch in its own Conda environment:

```bash
git clone --branch vine-graph-graphed --single-branch https://github.com/JinZhou5042/cctools.git cctools-src
cd cctools-src
unset PYTHONPATH
conda env create -y -f environment.yml
conda install -y -n cctools-dev --override-channels -c conda-forge --strict-channel-priority python=3.13
conda activate cctools-dev
./configure --with-base-dir "$CONDA_PREFIX" --prefix "$CONDA_PREFIX"
make -j4
make install
```

Verify both the Python interface and the worker executable:

```bash
python -c "from ndcctools.taskvine import vine_graph as vg; print(vg.Executor)"
vine_worker --version
```

The branch's
[vine_graph guide](https://github.com/JinZhou5042/cctools/blob/vine-graph-graphed/doc/manuals/taskvine/vine-graph.md)
covers local runs, workers, factories, and settings.

## Install Graphed and the experimental workflows

Inside the active `cctools-dev` environment:

```bash
git clone https://github.com/cooperative-computing-lab/graphed-taskvine.git
cd graphed-taskvine
python -m pip install -r requirements.txt
python -c "import graphed; from graphed_executors.taskvine_backend import TaskVineExecutor"
```

Workers need the same packages: `graphed`, `graphed-executors`, `graphed-histogram`, and whatever
the analysis imports (`awkward`, `uproot`, ...). With a factory, pack this environment
(`poncho_package_create` or `conda-pack`) and pass it with `--poncho-env`. vine_graph's library
refuses to start on a worker whose vine_graph Python code differs from the manager's, and says so.

## Quick start

The example downloads 16 public GamGam ROOT files from the `2025e-13tev-beta`
[ATLAS Open Data](https://opendata.cern.ch) 13 TeV release (about 9.86 GB), records the same
event selection as the ATLAS Open Data H->yy notebook with Graphed, and fills a histogram through
this executor. One partition is created per input ROOT file; once the flat Parquet files exist,
nothing is downloaded again.

Run the complete workflow in this process (vine_graph's local mode, for a quick check):

```bash
python -m examples.atlas_hyy
```

Expected summary (from an actual run against the full 16-file dataset):

```text
events in 100-160 GeV: 251659
highest bin center: 100.5 GeV
process/combine tasks: 16/15
```

The core analysis in [`examples/atlas_hyy.py`](examples/atlas_hyy.py) is ordinary deferred Graphed
code operating on the flattened leading-two-photon columns:

```python
import graphed_histogram as gh
from graphed import Session
from graphed.awkward import AwkwardBackend, from_parquet

from graphed_executors.taskvine_backend import TaskVineExecutor

session = Session(AwkwardBackend())
events = from_parquet(session, "events", [str(p) for p in parquet_paths])  # one file per partition

tight = events.isTightID_0 & events.isTightID_1
pt_cut = (events.pt_0 > 50) & (events.pt_1 > 30)
isolation = (events.ptcone20_0 / events.pt_0 < 0.055) & (events.ptcone20_1 / events.pt_1 < 0.055)
eta_ok = in_transition_veto(events.eta_0) & in_transition_veto(events.eta_1)
selected = events[tight & pt_cut & isolation & eta_ok]

mass = diphoton_mass(selected)  # by-hand 4-vector algebra, same formula as examples/cms_dimuon.py
histogram = gh.boost.Histogram(bh.axis.Regular(60, 100.0, 160.0), storage=bh.storage.Int64())
histogram.fill(mass)
plan = gh.plan({"diphoton_mass": histogram})

with TaskVineExecutor(manager_name="graphed-hyy", wait_for_workers=1) as executor:
    result = executor.run(plan)
```

To run the same plan on TaskVine workers, start workers that connect to the manager by name, by
hand or through a factory, and pass `--distributed`. The input files must be readable by the
workers at the same paths (a shared file system):

```bash
# terminal 1: a worker by hand ...
vine_worker -M graphed-hyy --cores 4
# ... or 50 workers from HTCondor
vine_factory -T condor -M graphed-hyy --min-workers 50 --max-workers 50 --cores 4

# terminal 2, from this repository
python -m examples.atlas_hyy --distributed --workers 1
```

Every example takes the same options ([`examples/vine_options.py`](examples/vine_options.py)):
`--distributed`, `--manager-name`, `--port` (a port or a range `low:high`, default `9100:9199`),
`--workers` (workers to wait for), `--param name=value` (any vine_graph setting, such as
`max-retries=3`; repeatable), and `--run-info` (the directory of the manager's logs).
[`examples/cms_dimuon.py`](examples/cms_dimuon.py) is a much smaller and faster (single-file,
2,304-event) smoke test of the same pattern, and [`examples/dv5.py`](examples/dv5.py) the large one.

## Execution guarantees

- One leaf task is created for each `Plan.tasks` entry, and leaves are ordered by unique `Task.key`
  values.
- Fixed plans use graphed's deterministic binary tree (`plan_tree`), so a float result is bit for
  bit what graphed's local executors return from the same leaf values. A fixed plan with `N`
  partitions performs exactly `N - 1` combines.
- Worker exceptions are re-raised on the driver as their original type when serializable, with the
  worker's traceback attached as an exception note; otherwise `TaskVineWorkerError` carries it.
- Relative local partition paths that exist on the driver are resolved before tasks run.
- Adaptive plans (`next_tasks`) run batch by batch on the same workers: `ExecContext` is updated as
  partitions complete, and `StopCondition` is checked as they do, so a run may stop inside a batch.
- A worker that is lost (evicted, killed, out of memory) costs only the tasks it held: vine_graph
  runs them again elsewhere, and recomputes a partial result that existed only on that worker. A task
  whose workers keep dying fails the run with an error naming it.

Task keys must be unique for the entire run. `process`, `combine`, and `empty` must follow the
graphed `Plan` contract. In particular, `combine` must be associative and commutative, and task
bodies should be safe to retry because TaskVine may re-execute work after worker loss.

Leaf results depend on the CPU that computes them when the analysis uses floating-point math whose
library picks code by CPU features: on a cluster with AVX-512 and AVX2 machines, an event near a cut
or a bin edge may land differently from one run to the next. Compare such histograms within a small
tolerance, or pin numpy's code paths as [`baselines/README.md`](baselines/README.md) does.

## Integration interface

```python
TaskVineExecutor(
    manager=None,                 # an open vine_graph.Executor to run on; the caller keeps owning it
    manager_name="graphed-taskvine",
    port=(9100, 9199),            # a port, or a range
    local=False,                  # run each task in this process, without a manager or workers
    wait_for_workers=0,
    work_dir=None,                # directory of file results (default ./vine-graph-work)
    ship=(),                      # files or directories every worker gets, importable there
    params=None,                  # vine_graph settings, such as {"max-retries": 3}
    run_info_path=None,           # where the manager writes its logs
    run_info_template=None,
    monitor=None, retries=3, max_in_flight=2, services=None, control=None,  # SubmitRunner options
    libcores=None,                # accepted and ignored: each worker core runs one task
)
```

The executor accepts `graphed.core.execution.Plan` (and graphed-executors' staged `DurablePlanV2`)
and returns `graphed.core.execution.ExecResult`. The manager is created when the first plan with
tasks runs, or when `.manager` is read: an empty plan returns `plan.empty()`, and a plan whose task
keys repeat raises `ValueError`, without starting one. `close()` or the context manager releases an
executor-owned manager.

The public interface is owned by `graphed-executors`: `run(plan) -> ExecResult`, `close()`,
`__enter__()`, `__exit__()`, `manager`, `port`, `last_stats` (a `RunStats` with `n_tasks`,
`n_graph_nodes`, `n_rounds`, `lower_s`, `vine_run_s`, `total_s`, and `rounds`), and
`TaskVineWorkerError`. `lower(plan)` is an inspection hook: it returns the same reduction as a
vine_graph `Graph` and its root node, which `executor.manager.run(graph, [root])` computes.

Plans and shipped modules are trusted executable inputs. TaskVine may retry leaves or combines, so
external writes must be idempotent or content-addressed.

## graphed vs. traditional Dask/coffea, across three workflows

Each example here runs two ways: through `TaskVineExecutor`, and through a plain local
Dask/coffea-style pipeline doing the same selection on the same files. CMS dimuon and ATLAS H→γγ
have their traditional side in this repo, at
[`baselines/cms_dimuon_dask.py`](baselines/cms_dimuon_dask.py) and
[`baselines/atlas_hyy_dask.py`](baselines/atlas_hyy_dask.py): plain `uproot.dask` /
`dask.dataframe`, no coffea, no cluster. DV5's traditional side is the original, unmodified
`ecf_calculator.py` from
[`JinZhou5042/sc26-dagvine-reproducibility`](https://github.com/JinZhou5042/sc26-dagvine-reproducibility).
It isn't duplicated here, since it needs coffea, dask-awkward, and fastjet, and has to stay
byte-identical to the DAGVine/SC26 hero-run reference.

### Overview

| workflow | scale | graphed + TaskVine (wall) | traditional (wall) | speedup | correctness |
|---|---|---|---|---|---|
| [CMS dimuon](#quick-start) | 1 file, 2,304 events (toy) | 1.79 s | 1.77–1.89 s | ~1x (noise) | exact match |
| [ATLAS H→γγ](#quick-start) | 16 files, 9.86 GB | 9.9 s (5.35 s execution) | 52.25 s | ~5.3x | exact match |
| [DV5](#dv5-run-it-yourself) | 800 files, 22 GB | 499.7 s (~8.3 min) | 1,316.1 s (~21.9 min) | ~2.6x | bit-for-bit identical |

All three ran on one machine, locally, no distributed factory, on the earlier VineGraph executor
(2026-10-05). On vine_graph with HTCondor workers through a factory (2026-10-20): ATLAS H→γγ over
10,240 Parquet shards in 6.6 s of makespan (251,659 events, exact), and DV5 over all 800 files in
35 s (275 events).

### CMS dimuon (toy scale)

Single 2,304-event file, opposite-sign muon pairs, invariant mass, 60-bin 60–120 GeV histogram.
[`examples/cms_dimuon.py`](examples/cms_dimuon.py) vs.
[`baselines/cms_dimuon_dask.py`](baselines/cms_dimuon_dask.py) (`uproot.dask` + plain
`dask.compute`, 5 partitions to match graphed's `steps_per_file=5`):

| | graphed + TaskVine | traditional dask-awkward |
|---|---|---|
| events in 60–120 GeV | 2004 | 2004 |
| peak bin center | 90.5 GeV | 90.5 GeV |
| scheduled units | 9 TaskVine tasks (5 process + 4 combine) | 135 dask graph keys |
| cold wall time | 1.789 s | 1.77–1.89 s |

No real speedup here, and that's expected: the actual work is well under a second on either side
(graphed's own makespan is 0.107 s, the dask baseline's build+compute is 0.46–0.66 s), so the
~1.8 s wall time on both sides is Python/import startup, not scheduling. At this size neither
framework has anything to prove. This example is a fast, low-dependency smoke test, not a
performance case.

### ATLAS H→γγ (medium scale, real Open Data)

16 real ATLAS Open Data ROOT files (~9.86 GB), diphoton selection and mass reconstruction, 60-bin
100–160 GeV histogram. [`examples/atlas_hyy.py`](examples/atlas_hyy.py) vs.
[`baselines/atlas_hyy_dask.py`](baselines/atlas_hyy_dask.py) (`dask.dataframe.read_parquet` over
the same per-file flat Parquets, `scheduler="processes"`, 4 workers, matching graphed's
`libcores=4`):

| | graphed + TaskVine | traditional Dask |
|---|---|---|
| events in 100–160 GeV | 251659 | 251659 |
| peak bin center | 100.5 GeV | 100.5 GeV |
| scheduled units | 31 TaskVine tasks (16 process + 15 combine) | 1,232 dask graph keys |
| graph build | n/a | 0.13 s |
| execution / compute | 5.35 s makespan | 50.79 s |
| total wall time | 9.9 s | 52.25 s |

~5.3x faster wall-to-wall, with no metadata-scan trick involved. Both sides read the same
already-flattened Parquet files, so the gap is scheduling granularity alone: one TaskVine task
runs a whole file's fused IR, while Dask spreads the same work over 1,232 keys, each carrying its
own dispatch overhead.

### DV5 (large scale, real HEP production workload)

DV5 is a real HEP event-selection and jet-substructure analysis, the ECF-calculator skim behind
the DAGVine/SC26 hero run. [`examples/dv5.py`](examples/dv5.py) records the full event selection
in graphed. Its heaviest step, jet substructure via `fastjet`, is one External node, since fastjet
can't run on awkward typetracers, so graphed gets its output form by running it once on a tiny
synthetic event. No coffea on the graphed side. The 22 GB `hgg_0` dataset (800 ROOT files, one
`--copy-count 1` copy of the DAGVine reproducibility archive) comes from the repo cited above.

<a id="dv5-run-it-yourself"></a>Run it yourself (this needs `fastjet`, `vector`, `scipy`, and the
`uproot.graphed`-providing `graphed-org/uproot5-graphed-mvp` development fork in place of released
`uproot` -- see the module docstring):

```bash
python -m examples.dv5 --data-dir dv5-data --files 5
```

By default this downloads the ~22 GB Zenodo archive on first use and processes the first 5 of its
800 files. Point `--data-dir` at an existing extraction (for example a previously downloaded
`hgg_0`) and pass `--files 0` to reproduce the full-dataset numbers below with local execution.
`--distributed` runs it on TaskVine workers like the other examples: on 100-400 HTCondor workers
through a factory, the full 800-file dataset (1,599 tasks) took 35 s and selected the same 275
events (2026-10-20). The local numbers below were produced with the underlying analysis run
directly against a real `vine_worker`.

Both systems ran the full 22 GB, 800-file `hgg_0` dataset locally on the same machine, capped at
16 cores (graphed: one local `vine_worker --cores 16`, Dask/coffea: `dask.compute(..., scheduler=
"processes", num_workers=16)`), with no `samples_ready.json` cache available for either side:

| stage | graphed + TaskVine | traditional Dask/coffea |
|---|---|---|
| fileset metadata scan | ~0 s (blind partitions: one file opened for schema, `N` for data) | 684.4 s (0.86 s/file × 800, single-threaded `uproot.open`) |
| graph build | 2.19 s (`source_open` 1.02 s + `record` 1.16 s + `compile` 0.008 s) | 2.4 s (`apply_to_fileset`) + 0.003 s (key count) |
| scheduled units | 1,599 TaskVine tasks (800 leaves + 799 combines) | 264,826 graph keys (331/file) |
| execution / compute | 495.4 s makespan (real local worker, 16 cores) | 629.3 s (`scheduler=processes`, 16 workers) |
| **total wall time** | **≈ 499.7 s (~8.3 min)** | **≈ 1,316.1 s (~21.9 min)** |
| selected events (of 800 files) | 275 | 275 |

Correctness: both sides selected the same 275 of 275 events, and all 39 compared leaves
(`Color_Ring`, 32 ECFs, `msoftdrop`, `pt`, `btag_ak4s`, `pn_HbbvsQCD`, `pn_md`, `matching`) matched
with `max_rel_diff = 0` (`compare.py`). Both ran on the same CPU this time, so none of the
cross-microarchitecture fastjet drift from the multi-machine condor runs below shows up.

## Development

In an environment that already contains a compatible TaskVine build:

```bash
python -m pip install -r requirements.txt
ruff check --target-version py311 --line-length 110 --select E,F,I,UP,B,SIM,C4,RUF --ignore E501 tests examples baselines
ruff format --check --target-version py311 --line-length 110 tests examples baselines
python -m pytest -q tests
```

The default test suite uses vine_graph's local mode. Run the worker integration path with:

```bash
GTV_WORKER=1 python -m pytest -q tests/test_executor.py
```

## License

MIT. See [LICENSE](LICENSE).

### Non-Dask Coffea comparison

The [Coffea virtual-array baseline and Factory scale-up commands](baselines/README.md)
compare the ATLAS H→γγ analysis on identical Parquet inputs, with exact histogram-bin checks.
The baseline uses a Coffea processor and a Python process pool, without a Dask scheduler.
