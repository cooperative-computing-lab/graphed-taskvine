# ATLAS H → γγ: Coffea baseline and TaskVine scale test

This run uses the existing ATLAS example's selection and histogram unchanged. The added
reference is a Coffea `ProcessorABC` with virtual NanoEvents and Python process-pool map/reduce,
without a Dask scheduler. This is an executor comparison, not a new physics validation.

## Inputs and timing

ATLAS Open Data `2025e-13tev-beta`, `GamGam`: 16 prepared Parquet files, 36,564,144 events
with at least two photons, 1,257,329,519 Parquet bytes. ROOT download and conversion are outside
the timers. Each source is split into 640 disjoint nonempty shards for the scale test:
10,240 leaf tasks plus 10,239 binary combines = **20,479 tasks**, with each event processed once.
`inputs.json` records source row counts and SHA-256 hashes. `provenance.json` records commits,
manager hardware, and the shard manifest hash; `pip-freeze.txt` records the environment.

The local manager is `condorfe.crc.nd.edu` (AMD EPYC 7451); both local runners use 16 processes.
OpenBLAS, OpenMP, and MKL threads are limited to one. `run_s` includes execution, pool/library
startup, and reads; Graphed plan construction is separate. TaskVine `run_s` also includes waiting
for workers. VineGraph makespan, reported separately below, starts after worker admission.
No caches were explicitly flushed. Local and cluster nodes differ, so local-versus-cluster times
are not a controlled hardware scaling comparison.

## Original-file baseline (three runs)

| Runner | Run times (s) | Median run (s) | Median plan build (s) |
| --- | --- | --- | --- |
| Coffea virtual, 16 processes | 1.549, 1.174, 1.028 | 1.174 | included |
| Graphed, 16 processes | 7.187, 2.191, 2.168 | 2.191 | 0.326 |

Order was Coffea/Graphed, Graphed/Coffea, Coffea/Graphed. **Coffea is faster in this workload.**
All 62 bins match exactly, including underflow and overflow: 251,659 events in [100,160),
224,056 underflow, and 77,742 overflow. Raw results are `full-*.json`.

## Sharded comparison

| Runner | Workers × cores | Run including startup/admission (s) | VineGraph makespan (s) |
| --- | --- | --- | --- |
| Coffea virtual process pool | 1 × 16 | 43.441 | — |
| Graphed process pool | 1 × 16 | 47.673 | — |
| Graphed TaskVine / Factory | 1 × 16 | 433.639 | 173.553 |
| Graphed TaskVine / Factory | 40 × 16 | 104.498 | 29.000 |

The two Factory runs completed all 20,479 tasks with zero failures/recoveries and exact equality
to every reference bin. The 40-worker run connected **40 workers / 640 cores across 39 hosts**;
all 40 workers completed tasks. The measured execution-only speedup is **5.98×** over the
1-worker control. Local Coffea remains the fastest end-to-end choice for this small analysis.
The 1-worker total includes an initial Condor match that did not start and subsequent scheduling
wait; use makespan rather than that total to interpret execution scaling. The 40-worker total
includes about 75.5 seconds outside graph execution. Both factories were stopped after their
managers exited; no submitted jobs remained.

`scale-v3-*.json` contains manager/worker task and resource counters. The saved Condor job
snapshot confirms 40 jobs each requesting 16 CPUs, 16,000 MB memory, and about 8 GB disk.
The command, submit, event, and timing files record how they were launched and completed.

Sharding deliberately supplies enough tasks to exercise the cluster, but the small tasks add
substantial overhead compared with the original 16-file workload. These are single scale trials,
not a claim of stable speedup or optimal partition size.

## Cross-CPU numerical reproducibility

The first 1-worker run completed every task without failure but failed exact histogram equality:
underflow was two events lower, and two pairs of adjacent bins differed by one event.
The raw failing result is retained as `scale-1-native-simd.json`. The analysis uses float32
inputs; the manager and worker chose different NumPy SIMD paths. Restricting NumPy 2.4.6 with
`NPY_DISABLE_CPU_FEATURES=X86_V4,AVX512_ICL,AVX512_SPR` made the full-file worker control
(`cpu-v3.json`) match the reference exactly. The final sharded cluster runs use this setting too.
The manager does not support these extensions, so its original timing runs already used the
same available path. No selection changes or histogram tolerance were introduced.
See NumPy's [runtime dispatch documentation](https://numpy.org/doc/stable/reference/simd/build-options.html).

## Reproduction and checks

Follow [the baseline commands](../../README.md), including environment packaging and SIMD/thread
settings. For the 1-worker control, replace the manager wait count and Factory min/max/cycle count
with 1; cores remain 16. Inputs are shared through `/groups` and the installed Conda environment
is shipped with Poncho. Saved command files record the exact invocations.

- Local experiment suite: 19 passed.
- Real-worker executor suite (`GTV_WORKER=1`): 11 passed.
- Installed contract + Coffea regression tests: 8 passed.
- Ruff lint/format and `pip check`: passed.
- Python 3.11/3.12/3.13 CI: [run 37408916487](https://github.com/cooperative-computing-lab/graphed-taskvine/actions/runs/37408916487), all passed.
- Cluster acceptance: all 62 bins exactly equal; 20,479 tasks done; 40 workers / 640 cores; zero failures; all workers did work.
