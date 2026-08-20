# Workload Planning and Scheduler Execution

Project Bourne v0.4.0 provides a framework-independent path from an observed
compute-site inventory to an actual, provenance-bearing scientific experiment.
This document describes the v0.4.0 implementation.

## Durable model

`WorkloadSpec` is an immutable description of a requested scientific command:
working directory, executable and exact argv, explicit inputs and outputs,
resource requirements (CPU, GPU, nodes, MPI ranks, memory, wall time),
capability and launcher requirements, constraints, and evidence. Evidence is
classified as `explicit`, `observed`, `inferred`, `historical`, or `unknown`.
An inference never becomes an observation.

`ExecutionPlan` is a separate immutable decision tied to one workload and one
inventory snapshot. It identifies the backend, access target, optional
execution target/context, requested resources, compatibility state, unresolved
conditions, and decision evidence. A plan is not an execution.

`ExecutionAttempt`, `SchedulerJob`, `AllocationObservation`, lifecycle events,
and the actual experiment are separate records. In particular:

~~~text
requested workload → immutable plan → execution attempt
                                      ├─ scheduler submission/job
                                      ├─ actual allocation observation
                                      └─ actual scientific experiment
~~~

Requested resources remain inside the plan. Allocated resources are observed
at execution time and stored separately.

## Bounded workload inspection

Planning inspects the explicit argv and a fixed allowlist of marker filenames
in the exact working directory. It does not read marker contents, recurse,
inspect `.env`, import user modules, run `--version`, compile, install, activate
an environment, execute Make/CMake, or run the scientific binary. Unknown
software remains a valid workload.

MPI rank count is distinct from CPUs and nodes. An explicit launcher such as
`mpirun -np 64` is preserved exactly. A rank constraint without a launcher
stays unresolved; Bourne does not guess among `mpirun`, `mpiexec`, and `srun`.

## Resolver

The resolver evaluates direct, Slurm, and PBS candidates using one persisted
inventory. Explicit backend, target, and context constraints are applied first.
Known hard incompatibilities are rejected. A fully supported direct candidate
is preferred. One remaining scheduler candidate may be selected with its
unknowns intact; materially ambiguous scheduler targets are not ranked or
guessed.

Visibility is not authorization. Historical success is not current
availability. Cost, queue priority, reservation, account, QoS, and
institutional preference are never invented.

`bourne plan` uses the latest inventory by default, accepts `--snapshot`, and
never silently runs discovery:

~~~bash
bourne discover
bourne plan --backend auto --gpus 1 -- python train.py
~~~

## Backends

`DirectBackend` uses the same experiment runner as `bourne run`: explicit argv,
no implicit shell, live and captured output, POSIX process-group interruption,
actual Git/system/execution-context provenance, artifacts, and lineage.

`SlurmBackend` stages a plan and worker, writes a fixed batch template, submits
with `sbatch --parsable`, queries only the known job ID for the submitting user,
waits, collects, and permits cancellation only through a Bourne execution
record whose submitting identity matches the current identity.

The active Slurm observation uses exact-job `squeue`. If that view no longer
contains the job, Bourne attempts a bounded, read-only `sacct` lookup for the
same job and user. Accounting is optional: unavailable accounting, an
accounting error, or no exact accounting record remains explicit. If the job
is unobservable and no worker result exists, waiting ends as
`collection_failed`; Bourne does not infer scientific completion.

`PBSBackend` provides the equivalent `qsub`, exact-job `qstat`, wait, collect,
and identity-checked `qdel` lifecycle. Scheduler-specific parsing remains in
the backend. A recognized exact-job "unknown job" response is recorded as
unobservable. Without a result bundle it also ends as `collection_failed`;
other status-command failures remain explicit query errors.

All controller scheduler calls use explicit argv, `shell=False`, bounded
stdout/stderr, and timeouts. Bourne never queries all users' jobs, modifies
cluster configuration/reservations/QoS, escalates privileges, bypasses the
scheduler, or SSHes to compute nodes.

On POSIX, scheduler ownership uses the effective UID resolved through the
system password database rather than `USER`, `LOGNAME`, `LNAME`, or `USERNAME`.
The canonical username and effective UID are retained as lifecycle evidence.
If no password entry exists, the numeric effective UID is used. Platforms
without POSIX UID semantics use the standard-library login-name fallback and
record that fallback source explicitly. Scheduler authorization remains the
infrastructure's final enforcement layer.

`execution wait` starts with a 15-second exact-job polling interval and backs
off by 1.5x to 60 seconds. Tests and operators may configure the initial
interval with `--poll`; queued and running jobs have no arbitrary default
wall-clock timeout.

## Compute worker and collection

The controller packages the installed Bourne Python package as a compressed,
stdlib-only zipapp and writes an immutable JSON plan. The fixed scheduler script
selects an available Python 3 interpreter and invokes that worker. Scientific
argv is stored only as a JSON list and is ultimately passed to `Popen` as argv;
it is never flattened into scheduler shell syntax.

Inside the allocation the worker observes an allowlist of Slurm/PBS allocation
variables and the actual hostname, validates the working directory, resolves
the executable, and compares hard resource requests where allocation facts are
available. A known shortfall produces `preflight_failed` and the scientific
program is not launched.

Successful or failed execution produces a bounded, versioned JSON result with
the experiment, artifacts, lineage, allocation, and preflight evidence. The
controller validates IDs, types, relationships, size, nesting, and collection
counts before one transactional SQLite import. Bourne never deserializes pickle
or executable code.

Scheduler `COMPLETED` without a valid result is `collection_failed`; it does not
create or claim a completed experiment.

## CLI

~~~bash
bourne plan [options] -- COMMAND...
bourne execute [options] -- COMMAND...
bourne execute --plan PLAN

bourne execution list
bourne execution show REF
bourne execution wait REF
bourne execution cancel REF
~~~

Plan and execution references support full ULIDs, unique prefixes, `latest`,
and `@N`. JSON is available for planning, execution submission, list, and show.
The structured Python services (`inspect_workload`, `resolve_execution`, and
`ExecutionService`) do not depend on terminal parsing and are intended as the
future SDK/MCP boundary.

## Current limitations

- Scheduler compute allocations must have Python 3.
- The access target and compute allocation must both see the staging directory;
  visibility is unknown until the worker runs.
- The scientific working directory and explicitly declared paths are not
  copied; sites without shared storage need a future explicit staging adapter.
- There is no automatic dependency installation, module loading, launcher
  injection, container orchestration, SSH execution, retry, or policy inference.
- Scheduler state parsing targets common Slurm/PBS interfaces and needs
  additional real-site validation across vendor variants.
- Slurm accounting is optional and may be unavailable or delayed by site
  configuration. Bourne records that uncertainty instead of inventing a
  terminal scheduler outcome.
- Output capture still accumulates stdout and stderr in memory before result
  serialization; large-log spooling remains future work.
- Windows scheduler execution and native Windows process-tree supervision are
  not claimed.
