# Design and measured results

Details behind the [README](README.md): how a graphed `Plan` maps to TaskVine tasks, what the backend guarantees, and
the measured comparisons with Dask/coffea.

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
       | vine_graph_runner(executor).run: graphed-executors' SubmitRunner
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

## Execution guarantees

- One leaf task is created for each `Plan.tasks` entry, and leaves are ordered by unique `Task.key`
  values.
- Fixed plans use graphed's deterministic binary tree (`plan_tree`), so a float result is bit for
  bit what graphed's local executors return from the same leaf values. A fixed plan with `N`
  partitions performs exactly `N - 1` combines.
- Worker exceptions are re-raised on the driver as their original type when serializable, with the
  worker's traceback attached as an exception note; otherwise the worker's error message is raised as a `vg.TaskError`.
- Each task runs in a sandbox of its own, so partitions name their files by absolute path (the
  examples resolve `--data-dir`).
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
tolerance, or pin numpy's code paths as [`benchmarks/README.md`](benchmarks/README.md) does.

## Integration interface

```python
from ndcctools.taskvine import vine_graph as vg
from graphed_executors.vine_graph_backend import vine_graph_runner

with vg.Executor(port, name, **options) as executor, vine_graph_runner(executor) as runner:
    result = runner.run(plan)
```

- `vg.Executor` is the TaskVine manager, which the caller creates and closes. Its options include
  `local=True` (run each task in this process, without workers), `library_files` (files or
  directories every worker gets in its sandbox, importable there), `params` (vine_graph settings,
  such as `{"wait-for-workers": 10, "max-retries": 3}`), and `run_info_path` (the manager's logs).
  `executor.stats()` reports the makespan, the tasks, and the recovery tasks.
- `vine_graph_runner(executor, *, monitor=None, retries=3, max_in_flight=2, services=None,
  control=None)` returns graphed-executors' `SubmitRunner` over `VineGraphBackend`, a graphed
  `Executor`: `run(plan)` takes a `Plan` or a staged `DurablePlanV2` and returns an `ExecResult`.
  Closing the runner leaves the Executor open, so one Executor serves any number of runners.
- `runner.backend.n_workers()` and `runner.backend.task_slots()` give the connected workers and
  cores.

Plans and shipped modules are trusted executable inputs. TaskVine may retry leaves or combines, so
external writes must be idempotent or content-addressed.

## graphed vs. traditional Dask/coffea, across three workflows

Each example here runs two ways: through vine_graph, and through a plain local
Dask/coffea-style pipeline doing the same selection on the same files. CMS dimuon and ATLAS H→γγ
have their traditional side in this repo, at
[`benchmarks/cms_dimuon_dask.py`](benchmarks/cms_dimuon_dask.py) and
[`benchmarks/atlas_hyy_dask.py`](benchmarks/atlas_hyy_dask.py): plain `uproot.dask` /
`dask.dataframe`, no coffea, no cluster. DV5's traditional side is the original, unmodified
`ecf_calculator.py` from
[`JinZhou5042/sc26-dagvine-reproducibility`](https://github.com/JinZhou5042/sc26-dagvine-reproducibility).
It isn't duplicated here, since it needs coffea, dask-awkward, and fastjet, and has to stay
byte-identical to the DAGVine/SC26 hero-run reference.

### Overview

| workflow | scale | graphed + TaskVine (wall) | traditional (wall) | speedup | correctness |
|---|---|---|---|---|---|
| [CMS dimuon](README.md#2-quick-check) | 1 file, 2,304 events (toy) | 1.79 s | 1.77–1.89 s | ~1x (noise) | exact match |
| [ATLAS H→γγ](README.md#2-quick-check) | 16 files, 9.86 GB | 9.9 s (5.35 s execution) | 52.25 s | ~5.3x | exact match |
| [DV5](#dv5-run-it-yourself) | 800 files, 22 GB | 499.7 s (~8.3 min) | 1,316.1 s (~21.9 min) | ~2.6x | bit-for-bit identical |

All three ran on one machine, locally, no distributed factory, on the earlier VineGraph executor
(2026-10-05). On vine_graph with HTCondor workers through a factory (2026-10-20): ATLAS H→γγ over
10,240 Parquet shards in 6.6 s of makespan (251,659 events, exact), and DV5 over all 800 files in
35 s (275 events).

### CMS dimuon (toy scale)

Single 2,304-event file, opposite-sign muon pairs, invariant mass, 60-bin 60–120 GeV histogram.
[`examples/cms_dimuon.py`](examples/cms_dimuon.py) vs.
[`benchmarks/cms_dimuon_dask.py`](benchmarks/cms_dimuon_dask.py) (`uproot.dask` + plain
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
[`benchmarks/atlas_hyy_dask.py`](benchmarks/atlas_hyy_dask.py) (`dask.dataframe.read_parquet` over
the same per-file flat Parquets, `scheduler="processes"`, 4 workers, matching the 4 cores graphed's worker
used):

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


### Non-Dask Coffea comparison

The [Coffea virtual-array baseline and Factory scale-up commands](benchmarks/README.md)
compare the ATLAS H→γγ analysis on identical Parquet inputs, with exact histogram-bin checks.
The baseline uses a Coffea processor and a Python process pool, without a Dask scheduler.
