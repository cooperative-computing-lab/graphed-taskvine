from pathlib import Path

import pytest
import toy_plans
from graphed.core.execution import Executor, StopReason
from graphed_executors.taskvine_backend import TaskVineExecutor


def test_public_executor_implements_graphed_protocol(tmp_path):
    executor = TaskVineExecutor(work_dir=tmp_path)
    assert isinstance(executor, Executor)
    assert TaskVineExecutor.__module__ == "graphed_executors.taskvine_backend"


def test_empty_plan_does_not_create_a_manager(tmp_path):
    executor = TaskVineExecutor(work_dir=tmp_path)
    result = executor.run(toy_plans.width_plan(0))
    assert result.value == 0
    assert result.n_partitions == 0
    assert result.n_combines == 0
    assert result.stopped == StopReason.EXHAUSTED
    assert executor._manager is None


def test_duplicate_keys_fail_before_submission(tmp_path):
    executor = TaskVineExecutor(work_dir=tmp_path)
    with pytest.raises(ValueError, match="task keys must be unique"):
        executor.run(toy_plans.duplicate_key_plan())
    assert executor._manager is None


def test_lower_empty_has_explicit_error(tmp_path):
    executor = TaskVineExecutor(work_dir=tmp_path)
    with pytest.raises(ValueError, match="cannot lower an empty task set"):
        executor.lower(toy_plans.width_plan(0))


def test_shipped_destinations_must_be_unique(tmp_path):
    left = tmp_path / "left" / "helpers"
    right = tmp_path / "right" / "helpers"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    with pytest.raises(ValueError, match="duplicate worker sandbox destination"):
        TaskVineExecutor(work_dir=tmp_path, ship=[left, right])


def test_missing_shipped_path_fails_at_construction(tmp_path):
    missing = Path(tmp_path, "missing.py")
    with pytest.raises(FileNotFoundError):
        TaskVineExecutor(work_dir=tmp_path, ship=[missing])
