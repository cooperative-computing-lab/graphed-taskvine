"""graphed plans on vine_graph against graphed's reference executors.

pytest tests/test_vine_graph.py                    # vine_graph local mode (in-process)
GTV_WORKER=1 pytest tests/test_vine_graph.py       # a real vine_worker on this host
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import awkward as ak
import boost_histogram as bh
import numpy as np
import pytest

pytest.importorskip(
    "ndcctools.taskvine.vine_graph",
    reason="integration tests require a CCTools build with vine_graph",
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import toy_plans  # noqa: E402
from graphed.core.execution import SequentialRunner, StopReason  # noqa: E402
from graphed_executors.local import ThreadExecutor  # noqa: E402
from graphed_executors.vine_graph_backend import vine_graph_runner  # noqa: E402
from ndcctools.taskvine import vine_graph as vg  # noqa: E402

WORKER_MODE = os.environ.get("GTV_WORKER") == "1"


@pytest.fixture(scope="module")
def runner(tmp_path_factory):
    work = tmp_path_factory.mktemp("vine")
    executor = vg.Executor(
        0,
        f"gtv-test-{os.getpid()}",
        local=not WORKER_MODE,
        output_dir=str(work / "outputs"),
        library_files={str(HERE / "toy_plans.py"): "toy_plans.py"},
        params={"wait-for-workers": 1} if WORKER_MODE else {},
        run_info_path=str(work / "logs"),
    )
    worker = None
    worker_output = None
    if WORKER_MODE:
        vine_worker = shutil.which("vine_worker", path=os.path.dirname(sys.executable)) or "vine_worker"
        env = dict(os.environ, PATH=f"{os.path.dirname(sys.executable)}:{os.environ['PATH']}")
        worker_output = open(work / "worker.out", "w")  # noqa: SIM115 -- closed after worker exit
        worker = subprocess.Popen(
            [
                vine_worker,
                "--cores",
                "2",
                "--timeout",
                "600",
                "-s",
                str(work / "worker"),
                "localhost",
                str(executor.port),
            ],
            env=env,
            stdout=worker_output,
            stderr=subprocess.STDOUT,
        )
    with executor, vine_graph_runner(executor) as runner:
        yield runner
    if worker is not None:
        worker.terminate()
        worker.wait(timeout=30)
    if worker_output is not None:
        worker_output.close()


def test_width_matches_reference(runner):
    plan = toy_plans.width_plan(10)
    expected = SequentialRunner().run(plan).value
    result = runner.run(plan)
    assert result.value == expected
    assert (result.n_partitions, result.n_combines, result.stopped) == (10, 9, StopReason.EXHAUSTED)


def test_float_reduction_is_bitwise_identical_to_thread_pool(runner):
    plan = toy_plans.float_plan(9)
    ours = runner.run(plan).value
    reference = ThreadExecutor(max_workers=4).run(plan).value
    assert ours.tobytes() == reference.tobytes()


def test_empty_and_single(runner):
    empty = toy_plans.width_plan(0)
    assert runner.run(empty).value == 0
    single = toy_plans.width_plan(1)
    result = runner.run(single)
    assert (result.value, result.n_partitions, result.n_combines) == (1, 1, 0)


def test_blind_partitions(runner):
    assert runner.run(toy_plans.blind_plan(4)).value == 10


def test_open_once_and_order(runner):
    toy_plans.OPEN_COUNT["n"] = 0
    value = runner.run(toy_plans.label_plan()).value
    assert value == ["ALPHA:0", "BETA:1", "ALPHA:2"]
    if not WORKER_MODE:  # the counter lives in the process that ran the tasks
        assert toy_plans.OPEN_COUNT["n"] <= 2


def test_worker_error_reaches_driver_intact(runner):
    with pytest.raises(ValueError, match="boom at entry 3") as info:
        runner.run(toy_plans.failing_plan(6))
    assert any("TaskVine worker" in note for note in getattr(info.value, "__notes__", []))


def test_adaptive_rounds(runner):
    plan = toy_plans.adaptive_plan(n=9, size=3)
    result = runner.run(plan)
    assert result.value == 90 and result.n_partitions == 9
    assert result.n_combines == 8


def test_adaptive_stop_condition(runner):
    plan = toy_plans.adaptive_plan(n=30, size=3, target_events=50)
    result = runner.run(plan)
    assert result.stopped == StopReason.TARGET_EVENTS
    # The stop is checked as partitions complete, so the run may stop inside a batch, as graphed's own runners
    # stop at different points too. Each partition holds 10 events, and nothing past the target batch runs.
    assert result.value == 10 * result.n_partitions
    assert 50 <= result.value <= 60 and result.n_combines == result.n_partitions - 1


def test_real_graphed_histogram_plan(runner, tmp_path):
    """A real graphed-histogram plan over parquet: TaskVine == thread pool, bin for bin."""
    import graphed_histogram as gh
    from graphed import Session
    from graphed.awkward import AwkwardBackend, from_parquet

    rng = np.random.default_rng(1)
    counts = rng.integers(0, 6, size=5000)
    pt = ak.unflatten(rng.exponential(40.0, size=int(counts.sum())), counts)
    path = tmp_path / "events.parquet"
    ak.to_parquet(ak.Array({"pt": pt}), path)

    def build():
        session = Session(AwkwardBackend())
        events = from_parquet(session, "events", str(path), steps_per_file=7)
        h = gh.boost.Histogram(bh.axis.Regular(20, 0.0, 200.0), storage=bh.storage.Weight())
        h.fill(events.pt[events.pt > 20.0])
        return gh.plan({"jet_pt": h})

    ours = gh.unpack(runner.run(build()).value)["jet_pt"]
    reference = gh.unpack(ThreadExecutor(max_workers=3).run(build()).value)["jet_pt"]
    assert np.array_equal(ours.view(flow=True), reference.view(flow=True))
    assert ours.sum().value == pytest.approx(float(ak.sum(pt[pt > 20.0] < 200.0)))
