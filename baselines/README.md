# HEP experiments and baselines

This directory is the experimental area for new HEP workflows, comparison pipelines, benchmark
inputs, and measured results. Keep each result next to the workflow and command that produced it,
with the input dataset, software revisions, and machine or cluster configuration recorded.
For a new analysis, use a named subdirectory here for its Graphed workflow, reference pipeline,
and result notes. The existing `cms_dimuon_dask.py` and `atlas_hyy_dask.py` files are comparison
baselines for the examples already in this repository.

Graphed + TaskVine workflows here must import `TaskVineExecutor` from
`graphed_executors.taskvine_backend`. The executor implementation and its public API belong to
[`JinZhou5042/graphed-executors`](https://github.com/JinZhou5042/graphed-executors/tree/taskvine/src/graphed_executors/taskvine_backend);
do not copy backend code into this repository.

## Coffea virtual-array baseline and scale-up

Install `python -m pip install -r baselines/requirements.txt` in the VineGraph environment.
`atlas_hyy_coffea.py` uses a Coffea `ProcessorABC`, `NanoEventsFactory` in `virtual` mode,
and a standard Python process pool. It reads the same prepared flat Parquet files and applies
exactly the existing ATLAS example's cuts and histogram definition. No Dask scheduler is used.

Prepare the original ATLAS dataset (about 9.86 GB of ROOT files), then compare both paths on 16 processes:

```bash
export PYTHONNOUSERSITE=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export NPY_DISABLE_CPU_FEATURES=X86_V4,AVX512_ICL,AVX512_SPR
python -c "from pathlib import Path; from examples.atlas_hyy import prepare_data; prepare_data(Path('atlas-hyy-data'))"
python -m baselines.atlas_hyy_coffea --data-dir atlas-hyy-data --workers 16 --output coffea.json
python -m baselines.atlas_hyy_graphed --data-dir atlas-hyy-data --cores 16 --output graphed.json
```

For this NumPy 2.4 environment, the SIMD setting keeps the AVX-512 workers on the same
calculation path as the AVX2 manager. Without it, a few float32 boundary events moved bins
in the cluster run; see the [measured results](results/atlas-hyy-2026-10-06/README.md).

Both JSON files contain all 62 histogram bins (including underflow and overflow). Compare
`bins_with_flow` for exact equality. Timings exclude downloading and ROOT-to-Parquet conversion;
Graphed reports plan construction separately, while the Coffea time includes pool startup and reads.

For more tasks, split the same events into disjoint Parquet files. This avoids repeatedly reading
an entire original file for each small logical slice. The manifest records every source entry range:

```bash
python -m baselines.prepare_atlas_shards --data-dir atlas-hyy-data \
  --output-dir atlas-hyy-shards --shards-per-file 640
```

The 16-file dataset produces 10,240 leaf tasks and 10,239 combine tasks. Use this same shard
directory for every runner in the scale-up comparison. Start the manager:

```bash
python -m baselines.atlas_hyy_graphed --data-dir atlas-hyy-shards --backend taskvine \
  --cores 16 --wait-for-workers 40 --manager-name graphed-hyy-scale --output scale40.json
```

Package the environment with a regular (not editable) installation of `graphed-executors`, then
start the Factory in a second terminal with the same thread-limit environment variables:

```bash
poncho_package_create "$CONDA_PREFIX" graphed-env.tar.gz
vine_factory -T condor -M graphed-hyy-scale --min-workers 40 --max-workers 40 \
  --workers-per-cycle 40 --cores 16 --memory 16000 --disk 8000 \
  --poncho-env graphed-env.tar.gz --env PYTHONNOUSERSITE=1 \
  --env OPENBLAS_NUM_THREADS=1 --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --env NPY_DISABLE_CPU_FEATURES=X86_V4,AVX512_ICL,AVX512_SPR
```

Stop the Factory after the run. Input paths must be accessible on the workers. `run_s` includes
worker admission and library startup; manager/worker status and TaskVine logs provide the actual
worker count and task execution evidence. Compare against a 1-worker, 16-core run using the same
files before interpreting scaling.
