"""Command-line interface for Project Bourne."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from . import __version__
from .completion import completion_script, experiment_candidates
from .config import default_database_path
from .lifecycle import run_and_record
from .presentation import format_compare, format_list, format_show
from .references import ExperimentReferenceError, resolve_experiment
from .storage import ExperimentStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bourne",
        description="Record durable provenance for an arbitrary experiment command.",
    )
    parser.add_argument("--version", action="version", version=f"bourne {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    run_parser = subparsers.add_parser("run", help="run and record an arbitrary command")
    run_parser.add_argument("command", nargs=argparse.REMAINDER, help="command and arguments")

    list_parser = subparsers.add_parser("list", help="list recent experiments")
    list_parser.add_argument("--limit", type=int, default=20, help="maximum records to show")
    list_parser.add_argument(
        "--full-id", action="store_true", help="display canonical 26-character ULIDs"
    )

    show_parser = subparsers.add_parser("show", help="show one experiment")
    show_parser.add_argument("experiment_id")

    compare_parser = subparsers.add_parser("compare", help="compare two experiments")
    compare_parser.add_argument("experiment_a")
    compare_parser.add_argument("experiment_b")

    completion_parser = subparsers.add_parser(
        "completion", help="generate shell completion for experiment references"
    )
    completion_parser.add_argument("shell", nargs="?", choices=("bash", "zsh", "fish"))
    completion_parser.add_argument("--candidates", help=argparse.SUPPRESS)
    return parser


def _get(store: ExperimentStore, reference: str):
    try:
        return resolve_experiment(store, reference)
    except ExperimentReferenceError as exc:
        print(f"bourne: {exc}", file=sys.stderr)
        return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    store = ExperimentStore(default_database_path())

    if arguments.subcommand == "run":
        command = list(arguments.command)
        if command and command[0] == "--":
            command.pop(0)
        if not command:
            parser.error("bourne run requires a command")
        experiment = run_and_record(
            command,
            store,
            cwd=Path.cwd(),
            stdout_stream=sys.stdout,
            stderr_stream=sys.stderr,
        )
        print(
            f"Bourne recorded {experiment.id} ({experiment.status}, "
            f"exit {experiment.exit_code})",
            file=sys.stderr,
        )
        return experiment.exit_code

    if arguments.subcommand == "list":
        if arguments.limit < 1:
            parser.error("--limit must be at least 1")
        print(format_list(store.list_recent(arguments.limit), full_id=arguments.full_id))
        return 0

    if arguments.subcommand == "show":
        experiment = _get(store, arguments.experiment_id)
        if experiment is None:
            return 2
        print(format_show(experiment))
        return 0

    if arguments.subcommand == "compare":
        first = _get(store, arguments.experiment_a)
        second = _get(store, arguments.experiment_b)
        if first is None or second is None:
            return 2
        print(format_compare(first, second))
        return 0


    if arguments.subcommand == "completion":
        if arguments.candidates is not None:
            print("\n".join(experiment_candidates(store, arguments.candidates)))
            return 0
        if arguments.shell is None:
            parser.error("bourne completion requires bash, zsh, or fish")
        print(completion_script(arguments.shell), end="")
        return 0


    parser.error(f"unsupported command: {arguments.subcommand}")
    return 2
