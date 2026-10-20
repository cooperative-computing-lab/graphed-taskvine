"""Command-line options shared by the examples for running on vine_graph.

Every example runs its plan through ``graphed_executors.taskvine_backend.TaskVineExecutor``:

- without ``--distributed``, each task runs in this process (vine_graph's local mode), for a quick check;
- with ``--distributed``, a TaskVine manager named ``--manager-name`` listens on ``--port`` and waits for
  ``--workers`` workers, started by hand (``vine_worker -M <name>``) or by a factory
  (``vine_factory -M <name> ...``);
- ``--param name=value`` passes any vine_graph setting (``max-retries``, ``vault-disk-limit``, ...), and
  ``--run-info`` names the directory of the manager's logs.
"""

from __future__ import annotations

import argparse

from graphed_executors.taskvine_backend import TaskVineExecutor


def _port(text: str) -> int | tuple[int, int]:
    """A port, or a range written low:high."""
    low, _, high = text.partition(":")
    return (int(low), int(high)) if high else int(low)


def _setting(text: str) -> tuple[str, float]:
    name, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"expected name=value, not {text!r}")
    return name, float(value)


def add_arguments(parser: argparse.ArgumentParser, manager_name: str) -> None:
    """Add the vine_graph options to an example's parser."""
    group = parser.add_argument_group("vine_graph")
    group.add_argument("--distributed", action="store_true", help="run on TaskVine workers")
    group.add_argument("--manager-name", default=manager_name, help="name workers and factories connect to")
    group.add_argument("--port", type=_port, default=(9100, 9199), help="manager port, or a range low:high")
    group.add_argument("--workers", type=int, default=1, help="workers to wait for before the first task")
    group.add_argument(
        "--param",
        type=_setting,
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="a vine_graph setting, such as max-retries=3 (repeatable)",
    )
    group.add_argument("--run-info", default=None, help="directory of the manager's logs")


def executor(args: argparse.Namespace, **options: object) -> TaskVineExecutor:
    """The TaskVineExecutor the options describe. ``options`` are passed on, such as ``ship``."""
    return TaskVineExecutor(
        manager_name=args.manager_name,
        port=args.port if args.distributed else 0,
        local=not args.distributed,
        wait_for_workers=args.workers if args.distributed else 0,
        params=dict(args.param),
        run_info_path=args.run_info,
        **options,
    )
