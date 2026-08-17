"""Command-line interface for Project Bourne."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Sequence

from . import __version__
from .backends import BackendError, Submission
from .completion import completion_script, experiment_candidates
from .config import default_database_path
from .discovery import discover_site, find_capabilities
from .execution_service import ExecutionService, PlanningError
from .inventory_presentation import format_capability_matches, format_inventory
from .inventory_references import InventoryReferenceError, resolve_inventory
from .inventory_storage import InventoryStore
from .lifecycle import run_and_record
from .presentation import format_compare, format_list, format_show, format_trace
from .references import ExperimentReferenceError, resolve_experiment
from .storage import ExperimentStore
from .tracing import ArtifactTraceError, trace_artifact
from .worker_result import WorkerResult
from .workload_models import ExecutionConstraints, ResourceRequirements
from .workload_presentation import (
    format_execution,
    format_execution_list,
    format_resolution,
)
from .workload_references import (
    WorkloadReferenceError,
    resolve_execution_attempt,
    resolve_plan,
)
from .workload_storage import ExecutionStore


def _memory_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([KMGT]i?B?|B)?", value, re.IGNORECASE)
    if match is None:
        raise argparse.ArgumentTypeError("memory must be a positive value such as 4G or 512MiB")
    amount = int(match.group(1))
    unit = (match.group(2) or "B").upper().replace("IB", "").replace("B", "")
    return amount * {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}[unit]


def _walltime_seconds(value: str) -> int:
    if value.isdigit() and int(value) > 0:
        return int(value)
    match = re.fullmatch(r"([1-9][0-9]*)([HMS])", value, re.IGNORECASE)
    if match:
        return int(match.group(1)) * {"H": 3600, "M": 60, "S": 1}[match.group(2).upper()]
    match = re.fullmatch(r"([0-9]+):([0-5][0-9]):([0-5][0-9])", value)
    if match:
        seconds = int(match.group(1)) * 3600 + int(match.group(2)) * 60 + int(match.group(3))
        if seconds > 0:
            return seconds
    raise argparse.ArgumentTypeError("walltime must be seconds, 2h/30m, or HH:MM:SS")


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def _nonnegative(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be at least 0")
    return parsed


def _add_planning_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backend", choices=("auto", "direct", "slurm", "pbs"), default="auto")
    parser.add_argument("--target")
    parser.add_argument("--context")
    parser.add_argument("--snapshot", default="latest", help="inventory ID, prefix, latest, or @N")
    parser.add_argument("--cpus", type=_positive)
    parser.add_argument("--gpus", type=_nonnegative)
    parser.add_argument("--nodes", type=_positive)
    parser.add_argument("--mpi-ranks", type=_positive)
    parser.add_argument("--memory", type=_memory_bytes, metavar="SIZE")
    parser.add_argument("--walltime", type=_walltime_seconds, metavar="TIME")
    parser.add_argument("--input", action="append", default=[], metavar="PATH")
    parser.add_argument("--output", action="append", default=[], metavar="PATH")
    parser.add_argument("--derived-from", metavar="EXPERIMENT")
    parser.add_argument("--json", action="store_true", help="write structured JSON")


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

    plan_parser = subparsers.add_parser(
        "plan", help="inspect a workload and persist a safe execution plan"
    )
    _add_planning_options(plan_parser)
    plan_parser.add_argument("command", nargs=argparse.REMAINDER)

    execute_parser = subparsers.add_parser(
        "execute", help="execute an existing plan or plan and execute a command"
    )
    _add_planning_options(execute_parser)
    execute_parser.add_argument("--plan", metavar="PLAN")
    execute_parser.add_argument("command", nargs=argparse.REMAINDER)

    execution_parser = subparsers.add_parser(
        "execution", help="inspect or control Bourne execution attempts"
    )
    execution_subparsers = execution_parser.add_subparsers(
        dest="execution_command", required=True
    )
    execution_list = execution_subparsers.add_parser("list", help="list recent executions")
    execution_list.add_argument("--limit", type=_positive, default=20)
    execution_list.add_argument("--json", action="store_true")
    execution_show = execution_subparsers.add_parser("show", help="show one execution")
    execution_show.add_argument("reference")
    execution_show.add_argument("--json", action="store_true")
    execution_wait = execution_subparsers.add_parser("wait", help="wait for one scheduled execution")
    execution_wait.add_argument("reference")
    execution_wait.add_argument(
        "--poll", type=float, default=15.0,
        help="initial scheduler poll interval in seconds (backs off to 60s)",
    )
    execution_wait.add_argument("--timeout", type=float)
    execution_cancel = execution_subparsers.add_parser("cancel", help="cancel one Bourne-managed job")
    execution_cancel.add_argument("reference")
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
    execution_store = ExecutionStore(default_database_path())

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

    if arguments.subcommand in {"plan", "execute"}:
        service = ExecutionService(execution_store, inventory_store)
        if inventory_store.count() == 0:
            print(
                "bourne: No inventory snapshots are recorded. Run 'bourne discover' first.",
                file=sys.stderr,
            )
            return 2
        try:
            snapshot = resolve_inventory(inventory_store, arguments.snapshot)
        except InventoryReferenceError as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2

        if arguments.subcommand == "execute" and arguments.plan is not None:
            if arguments.command:
                parser.error("--plan cannot be combined with a command")
            try:
                plan = resolve_plan(execution_store, arguments.plan)
            except WorkloadReferenceError as exc:
                print(f"bourne: {exc}", file=sys.stderr)
                return 2
            try:
                plan_snapshot = inventory_store.get(plan.inventory_snapshot_id)
                result = service.execute_plan(plan.id, plan_snapshot)
            except (BackendError, PlanningError, OSError, ValueError) as exc:
                print(f"bourne: {exc}", file=sys.stderr)
                return 2
            return _print_execution_result(result, arguments.json)

        command = list(arguments.command)
        if command and command[0] == "--":
            command.pop(0)
        if not command:
            parser.error(f"bourne {arguments.subcommand} requires a command or --plan")
        parent = None
        if arguments.derived_from is not None:
            parent = _get(store, arguments.derived_from)
            if parent is None:
                return 2
        resources = ResourceRequirements(
            cpus=arguments.cpus, gpus=arguments.gpus, nodes=arguments.nodes,
            mpi_ranks=arguments.mpi_ranks, memory_bytes=arguments.memory,
            walltime_seconds=arguments.walltime,
        )
        constraints = ExecutionConstraints(
            backend=arguments.backend, target=arguments.target,
            context=arguments.context,
        )
        resolution = service.create_plan(
            command, snapshot, cwd=Path.cwd(), inputs=arguments.input,
            outputs=arguments.output, resources=resources,
            constraints=constraints,
            parent_experiment_id=None if parent is None else parent.id,
        )
        if arguments.subcommand == "plan":
            print(
                json.dumps(resolution.to_dict(), ensure_ascii=False, sort_keys=True)
                if arguments.json else format_resolution(resolution)
            )
            return 0 if resolution.selected is not None else 2
        if resolution.selected is None:
            print(
                json.dumps(resolution.to_dict(), ensure_ascii=False, sort_keys=True)
                if arguments.json else format_resolution(resolution),
                file=sys.stderr,
            )
            return 2
        try:
            result = service.execute_plan(resolution.selected.id, snapshot)
        except (BackendError, PlanningError, OSError, ValueError) as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2
        return _print_execution_result(result, arguments.json)

    if arguments.subcommand == "execution":
        service = ExecutionService(execution_store, inventory_store)
        command = arguments.execution_command
        if command == "list":
            executions = execution_store.list_executions(arguments.limit)
            print(
                json.dumps([vars(item) for item in executions], ensure_ascii=False, sort_keys=True)
                if arguments.json else format_execution_list(executions)
            )
            return 0
        try:
            execution = resolve_execution_attempt(execution_store, arguments.reference)
        except WorkloadReferenceError as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2
        if command == "show":
            view = service.get_execution(execution.id)
            print(
                json.dumps(view.to_dict(), ensure_ascii=False, sort_keys=True)
                if arguments.json else format_execution(view)
            )
            return 0
        if execution.backend == "direct":
            print("bourne: direct execution is already synchronous", file=sys.stderr)
            return 2
        try:
            if command == "cancel":
                service.cancel_execution(execution.id)
                print(f"Cancelled execution {execution.id}")
                return 0
            result = service.wait_execution(
                execution.id, poll_seconds=arguments.poll,
                timeout_seconds=arguments.timeout,
            )
        except (BackendError, TimeoutError) as exc:
            print(f"bourne: {exc}", file=sys.stderr)
            return 2
        return _print_execution_result(result, False)

    parser.error(f"unsupported command: {arguments.subcommand}")
    return 2


def _print_execution_result(result: Submission | WorkerResult, structured: bool) -> int:
    if isinstance(result, Submission):
        value = {
            "execution_id": result.execution_id,
            "scheduler_family": result.scheduler_family,
            "job_id": result.job_id,
            "state": "submitted",
        }
        print(
            json.dumps(value, ensure_ascii=False, sort_keys=True)
            if structured
            else (
                f"Execution: {result.execution_id}\n"
                f"Scheduler: {result.scheduler_family}\n"
                f"Job: {result.job_id}\nState: submitted"
            )
        )
        return 0
    value = {
        "execution_id": result.execution_id, "state": result.state,
        "experiment_id": None if result.experiment is None else result.experiment.id,
        "error": result.error,
    }
    print(
        json.dumps(value, ensure_ascii=False, sort_keys=True)
        if structured
        else (
            f"Execution: {result.execution_id}\nState: {result.state}\n"
            f"Experiment: {value['experiment_id'] or 'unavailable'}"
        ),
        file=sys.stderr if not structured and result.experiment is not None else sys.stdout,
    )
    return result.experiment.exit_code if result.experiment is not None else 2
