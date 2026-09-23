"""TaskVineExecutor against graphed's reference executors.

pytest tests/test_executor.py                    # VineGraph local-execute (in-process)
GTV_WORKER=1 pytest tests/test_executor.py       # a real vine_worker on this host
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
    reason="integration tests require a TaskVine build with VineGraph",
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import toy_plans  # noqa: E402
from graphed.core.execution import SequentialRunner, StopReason  # noqa: E402
from graphed_executors.local import ThreadExecutor  # noqa: E402
from graphed_executors.taskvine_backend import TaskVineExecutor  # noqa: E402

WORKER_MODE = os.environ.get("GTV_WORKER") == "1"


@pytest.fixture(scope="module")
def executor(tmp_path_factory):
    work = tmp_path_factory.mktemp("vine")
    ex = TaskVineExecutor(
        local=not WORKER_MODE,
        manager_name=f"gtv-test-{os.getpid()}",
        port=0,
        libcores=2,
        wait_for_workers=1 if WORKER_MODE else 0,
        work_dir=work,
        run_info_path=str(work / "logs"),
        run_info_template="test",
        ship=[HERE / "toy_plans.py"],
    )
    worker = None
    worker_output = None
    if WORKER_MODE:
        port = ex.manager.port
        vine_worker = shutil.which("vine_worker", path=os.path.dirname(sys.executable)) or "vine_worker"
        env = dict(os.environ, PATH=f"{os.path.dirname(sys.executable)}:{os.environ['PATH']}")
        worker_output = open(work / "worker.out", "w")  # noqa: SIM115 -- closed after worker exit
        worker = subprocess.Popen(
            [
                vine_worker,
                "--cores",
                "2",
                "--memory",
                "4000",
                "--disk",
                "8000",
                "--timeout",
                "600",
                "-s",
                str(work / "worker"),
                "localhost",
                str(port),
            ],
            env=env,
            stdout=worker_output,
            stderr=subprocess.STDOUT,
        )
    yield ex
    if worker is not None:
        worker.terminate()
        worker.wait(timeout=30)
    if worker_output is not None:
        worker_output.close()
    ex.close()


def test_width_matches_reference(executor):
    plan = toy_plans.width_plan(10)
    expected = SequentialRunner().run(plan).value
    result = executor.run(plan)
    assert result.value == expected
    assert (result.n_partitions, result.n_combines, result.stopped) == (10, 9, StopReason.EXHAUSTED)
    assert executor.last_stats.n_graph_nodes == 19


def test_float_reduction_is_bitwise_identical_to_thread_pool(executor):
    plan = toy_plans.float_plan(9)
    ours = executor.run(plan).value
    reference = ThreadExecutor(max_workers=4).run(plan).value
    assert ours.tobytes() == reference.tobytes()


def test_empty_and_single(executor):
    empty = toy_plans.width_plan(0)
    assert executor.run(empty).value == 0
    with pytest.raises(ValueError, match="cannot lower an empty task set"):
        executor.lower(empty)
    single = toy_plans.width_plan(1)
    result = executor.run(single)
    assert (result.value, result.n_partitions, result.n_combines) == (1, 1, 0)


def test_blind_partitions(executor):
    assert executor.run(toy_plans.blind_plan(4)).value == 10


def test_open_once_and_order(executor):
    toy_plans.OPEN_COUNT["n"] = 0
    value = executor.run(toy_plans.label_plan()).value
    assert value == ["ALPHA:0", "BETA:1", "ALPHA:2"]
    if not WORKER_MODE:  # the counter lives in the process that ran the tasks
        assert toy_plans.OPEN_COUNT["n"] <= 2


def test_relative_partition_uris_resolve_against_driver_cwd(executor, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    for name in ("a", "b"):
        (tmp_path / "data" / f"{name}.txt").write_text(name.upper())
    assert executor.run(toy_plans.relative_file_plan(["data/a.txt", "data/b.txt"])).value == ["A", "B"]


def test_worker_error_reaches_driver_intact(executor):
    with pytest.raises(ValueError, match="boom at entry 3") as info:
        executor.run(toy_plans.failing_plan(6))
    assert any("TaskVine worker" in note for note in getattr(info.value, "__notes__", []))


def test_adaptive_rounds(executor):
    plan = toy_plans.adaptive_plan(n=9, size=3)
    result = executor.run(plan)
    assert result.value == 90 and result.n_partitions == 9
    assert result.n_combines == 8
    assert executor.last_stats.n_rounds == 3


def test_adaptive_stop_condition(executor):
    plan = toy_plans.adaptive_plan(n=30, size=3, target_events=50)
    result = executor.run(plan)
    assert result.stopped == StopReason.TARGET_EVENTS
    assert result.value == 60 and result.n_partitions == 6


def test_duplicate_task_keys_are_rejected(executor):
    with pytest.raises(ValueError, match="task keys must be unique"):
        executor.run(toy_plans.duplicate_key_plan())


def test_real_graphed_histogram_plan(executor, tmp_path):
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

    ours = gh.unpack(executor.run(build()).value)["jet_pt"]
    reference = gh.unpack(ThreadExecutor(max_workers=3).run(build()).value)["jet_pt"]
    assert np.array_equal(ours.view(flow=True), reference.view(flow=True))
    assert ours.sum().value == pytest.approx(float(ak.sum(pt[pt > 20.0] < 200.0)))
