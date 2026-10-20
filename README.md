# graphed-taskvine

HEP analyses written with [graphed](https://github.com/graphed-org/graphed), run on
[TaskVine](https://cctools.readthedocs.io/en/latest/taskvine/) through vine_graph. Each data partition becomes
one TaskVine task, and partial results are combined in a binary tree. If a worker is lost, its tasks run again
on another worker.

The backend is `vine_graph_runner` from
[`graphed_executors.vine_graph_backend`](https://github.com/JinZhou5042/graphed-executors/tree/vine-graph/src/graphed_executors/vine_graph_backend).
This repository holds examples and benchmarks, and no backend code of its own.

## 1. Install

vine_graph is not in the released `ndcctools` package yet, so build CCTools from the `vine-graph` branch in its
own Conda environment:

```bash
git clone --branch vine-graph --single-branch https://github.com/JinZhou5042/cctools.git cctools-src
cd cctools-src
unset PYTHONPATH
conda env create -y -f environment.yml
conda install -y -n cctools-dev --override-channels -c conda-forge --strict-channel-priority python=3.13
conda activate cctools-dev
./configure --with-base-dir "$CONDA_PREFIX" --prefix "$CONDA_PREFIX"
make -j4 && make install
cd ..
```

Then install graphed and this repository's requirements in the same environment:

```bash
git clone https://github.com/cooperative-computing-lab/graphed-taskvine.git
cd graphed-taskvine
python -m pip install -r requirements.txt
python -c "from graphed_executors.vine_graph_backend import vine_graph_runner; print('ok')"
```

## 2. Quick check

The CMS dimuon example downloads one small file (2,304 events) and runs in a few seconds. Without
`--distributed`, every task runs in this process, so no workers are needed:

```bash
python -m examples.cms_dimuon
```

Expected output:

```text
events in 60-120 GeV: 2004
highest bin center: 90.5 GeV
process/combine tasks: 5/4
```

## 3. Run on TaskVine workers

Start a worker, then run the same example with `--distributed`. Workers must read the input files at the same
paths, so use a shared file system:

```bash
# terminal 1
vine_worker -M graphed-dimuon --cores 4

# terminal 2, in this repository
python -m examples.cms_dimuon --distributed
```

On a cluster, a factory starts the workers. They need the same Python packages as the driver, so pack the
environment and pass it to the factory:

```bash
poncho_package_create "$CONDA_PREFIX" graphed-env.tar.gz
vine_factory -T condor -M graphed-dimuon --min-workers 10 --max-workers 10 --cores 4 --poncho-env graphed-env.tar.gz
python -m examples.cms_dimuon --distributed --workers 10
```

Each example takes the same options: `--manager-name` (the name workers connect to), `--port` (a port or a range
`low:high`, default `9100:9199`), `--workers` (workers to wait for before starting), `--param name=value` (any
vine_graph setting, such as `max-retries=3`), and `--run-info` (where the manager's logs go).

## 4. Larger examples

| example | data | command | expected result |
|---|---|---|---|
| ATLAS H→γγ | 16 ATLAS Open Data files, 9.86 GB, downloaded on first run | `python -m examples.atlas_hyy` | `events in 100-160 GeV: 251659`, `process/combine tasks: 16/15` |
| DV5 | 800 ROOT files, 22 GB, downloaded on first run | `python -m examples.dv5 --files 5` (`--files 0` for all 800) | all 800 files: `selected events: 275`, `process/combine tasks: 800/799` |

DV5 also needs `fastjet`, `vector`, `scipy`, and a development fork of uproot; the top of
[`examples/dv5.py`](examples/dv5.py) explains how to install them. For more data and more tasks, DV5 takes several
directories and splits each file into steps (`--data-dir hgg_0 hgg_1 ... --steps-per-file 4`), and ATLAS can be cut
into shards (see [`benchmarks/README.md`](benchmarks/README.md)).

## Use it in your own analysis

```python
from ndcctools.taskvine import vine_graph as vg
from graphed_executors.vine_graph_backend import vine_graph_runner

plan = ...  # a graphed Plan, for example graphed_histogram.plan({...})
with vg.Executor(9123, "my-analysis") as executor, vine_graph_runner(executor) as runner:
    result = runner.run(plan)  # workers: vine_worker -M my-analysis
```

`vg.Executor(..., local=True)` runs every task in the calling process. An exception raised on a worker reaches
the driver as its original type, with the worker's traceback attached.

## More

- [`RESULTS.md`](RESULTS.md): how a Plan becomes TaskVine tasks, what the backend guarantees, and measured
  comparisons with Dask and coffea (ATLAS 5.3x faster, DV5 2.6x, identical results).
- [`benchmarks/`](benchmarks/): the Dask and coffea pipelines used for those comparisons.
- The [vine_graph guide](https://github.com/JinZhou5042/cctools/blob/vine-graph/doc/manuals/taskvine/vine-graph.md)
  covers workers, factories, and settings.

Tests (`GTV_WORKER=1` also runs them on a real worker):

```bash
python -m pytest -q tests
```

## License

MIT. See [LICENSE](LICENSE).
