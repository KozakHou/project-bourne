"""Command-line interface for Project Bourne."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from . import __version__
from .completion import completion_script, experiment_candidates
from .config import default_database_path
from .discovery import discover_site, find_capabilities
from .inventory_presentation import format_capability_matches, format_inventory
from .inventory_references import InventoryReferenceError, resolve_inventory
from .inventory_storage import InventoryStore
from .lifecycle import run_and_record
from .presentation import format_compare, format_list, format_show, format_trace
from .references import ExperimentReferenceError, resolve_experiment
from .storage import ExperimentStore
from .tracing import ArtifactTraceError, trace_artifact


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bourne",
        description="Record durable provenance for an arbitrary experiment command.",
    )
    parser.add_argument("--version", action="version", version=f"bourne {__version__}")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    run_parser = subparsers.add_parser("run", help="run and record an arbitrary command")
    run_parser.add_argument(
        "--input", action="append", default=[], metavar="PATH", help="declare an input file"
    )
    run_parser.add_argument(
        "--output",
        action="append",
        default=[],
        metavar="PATH",
        help="declare an expected output file",
    )
    run_parser.add_argument(
        "--derived-from",
        metavar="EXPERIMENT",
        help="record that this experiment is intentionally derived from another",
    )
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

    trace_parser = subparsers.add_parser(
        "trace", help="trace a recorded output artifact to its producing experiment"
    )
    trace_parser.add_argument("artifact_path")

    completion_parser = subparsers.add_parser(
        "completion", help="generate shell completion for experiment references"
    )
    completion_parser.add_argument("shell", nargs="?", choices=("bash", "zsh", "fish"))
    completion_parser.add_argument("--candidates", help=argparse.SUPPRESS)

    discover_parser = subparsers.add_parser(
        "discover", help="discover and persist the current compute-site surface"
    )
    discover_parser.add_argument(
        "--json", action="store_true", help="write structured JSON"
    )

    inventory_parser = subparsers.add_parser(
        "inventory", help="inspect a persisted compute-site inventory"
    )
    inventory_parser.add_argument(
        "reference", nargs="?", default="latest", help="snapshot ID, prefix, latest, or @N"
    )
    inventory_parser.add_argument(
        "--find", metavar="NAME", help="find every exact capability-name match"
    )
    inventory_parser.add_argument(
        "--json", action="store_true", help="write structured JSON"
    )
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
    inventory_store = InventoryStore(default_database_path())

    if arguments.subcommand == "run":
        command = list(arguments.command)
        if command and command[0] == "--":
            command.pop(0)
        if not command:
            parser.error("bourne run requires a command")
        parent = None
        if arguments.derived_from is not None:
            parent = _get(store, arguments.derived_from)
            if parent is None:
                return 2
        experiment = run_and_record(
            command,
            store,
            cwd=Path.cwd(),
            stdout_stream=sys.stdout,
            stderr_stream=sys.stderr,
            input_paths=arguments.input,
            output_paths=arguments.output,
            parent_experiment_id=None if parent is None else parent.id,
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
        lineage = store.get_lineage(experiment.id)
        parent = None if lineage is None else store.get(lineage.parent_experiment_id)
        print(format_show(experiment, store.list_artifacts(experiment.id), parent))
        return 0

    if arguments.subcommand == "compare":
        first = _get(store, arguments.experiment_a)
        second = _get(store, arguments.experiment_b)
        if first is None or second is None:
            return 2
        print(format_compare(first, second))
        return 0

    if arguments.subcommand == "trace":
        try:
            traced = trace_artifact(store, arguments.artifact_path, cwd=Path.cwd())
        except ArtifactTraceError as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2
        print(format_trace(traced))
        return 0

    if arguments.subcommand == "completion":
        if arguments.candidates is not None:
            print("\n".join(experiment_candidates(store, arguments.candidates)))
            return 0
        if arguments.shell is None:
            parser.error("bourne completion requires bash, zsh, or fish")
        print(completion_script(arguments.shell), end="")
        return 0

    if arguments.subcommand == "discover":
        snapshot = discover_site(inventory_store, cwd=Path.cwd())
        if arguments.json:
            print(json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True))
        else:
            print(format_inventory(snapshot, discovered=True))
        return 0

    if arguments.subcommand == "inventory":
        if inventory_store.count() == 0:
            print(
                "bourne: No inventory snapshots are recorded. Run 'bourne discover' first.",
                file=sys.stderr,
            )
            return 2
        try:
            snapshot = resolve_inventory(inventory_store, arguments.reference)
        except InventoryReferenceError as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2
        if arguments.find is not None:
            matches = find_capabilities(snapshot, arguments.find)
            if arguments.json:
                print(
                    json.dumps(
                        {
                            "snapshot_id": snapshot.id,
                            "query": {"kind": "exact-capability-name", "name": arguments.find},
                            "matches": [item.to_dict() for item in matches],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
            else:
                print(format_capability_matches(arguments.find, matches))
        elif arguments.json:
            print(json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True))
        else:
            print(format_inventory(snapshot))
        return 0

    parser.error(f"unsupported command: {arguments.subcommand}")
    return 2
