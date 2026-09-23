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
