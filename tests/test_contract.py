"""What works without CCTools: the backend and the examples' options import."""

import argparse

import pytest
from graphed_executors.vine_graph_backend import vine_graph_runner

from examples import vine_options


def test_the_backend_imports_without_cctools():
    assert vine_graph_runner.__module__ == "graphed_executors.vine_graph_backend.backend"


def test_example_options():
    parser = argparse.ArgumentParser()
    vine_options.add_arguments(parser, "contract")
    args = parser.parse_args(
        ["--distributed", "--port", "9000:9100", "--workers", "4", "--param", "max-retries=2"]
    )
    assert (args.distributed, args.manager_name, args.port, args.workers) == (
        True,
        "contract",
        [9000, 9100],
        4,
    )
    assert dict(args.param) == {"max-retries": 2.0}
    with pytest.raises(SystemExit):
        parser.parse_args(["--param", "max-retries"])
