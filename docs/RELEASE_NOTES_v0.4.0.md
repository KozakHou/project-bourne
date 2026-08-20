# Project Bourne v0.4.0 — Workload Planning and Scheduler Execution

## Overview

Project Bourne v0.4.0 turns discovered compute infrastructure into executable
scientific plans. Bourne can inspect generic workloads, resolve compatible
execution paths, run directly on local systems, and submit and collect
scientific execution through Slurm or PBS while preserving execution-plane
provenance.

The release continues a provenance-first progression:

- v0.1: What ran?
- v0.2: What went in and what came out?
- v0.3: What execution infrastructure is available?
- v0.4: What does this workload need, where can it run, and how do I execute it?

The model is not HPC-only. Direct execution remains first-class on laptops,
workstations, DGX-class personal systems, and lab GPU machines; Slurm and PBS
extend the same model to scheduler-backed sites.

## Highlights

- Immutable, framework-independent `WorkloadSpec` with evidence-backed
  requirements
- Bounded non-executing workload inspection with explicit evidence states
- Immutable `ExecutionPlan` tied to one inventory snapshot
- Conservative direct/Slurm/PBS resolution that refuses material ambiguity
- `DirectBackend`, `SlurmBackend`, and `PBSBackend`
- Portable standard-library-only compute worker
- Requested resources kept separate from observed allocation
- Submission, scheduler job, allocation, experiment, and lifecycle-event records
- Exact-job scheduler observation and identity-checked cancellation
- Trusted POSIX effective identity independent of login-name environment values
- Bounded JSON result validation and transactional, idempotent collection
- Shell-safe scientific argv preserved as an argument vector
- Append-only execution lifecycle evidence
- Transactional schema 4 migration preserving schema 1–3 history
- Zero third-party runtime dependencies

## Workload model

`WorkloadSpec` is an immutable description of the requested command, working
directory, exact argv, declared artifacts, resource requirements, constraints,
and evidence. Evidence remains classified as explicit, observed, inferred,
historical, or unknown; inference is never promoted to observation.

Bounded inspection considers the explicit argv and an allowlist of marker names
in the exact working directory. It does not execute an unknown scientific
binary, import user modules, inspect marker contents, or recursively crawl the
working tree. Planning unknown software therefore does not mean executing it.

## Execution planning

An immutable `ExecutionPlan` records one resolution decision against one
inventory snapshot. The conservative resolver applies explicit constraints,
rejects known hard incompatibilities, reports unresolved conditions, and
refuses to guess between materially ambiguous targets.

A visible target is not proof of authorization. Historical evidence is not
current availability. Unknown compatibility is not compatibility. A plan is a
recorded decision, not an execution.

## Direct execution

`DirectBackend` runs the exact command through Bourne's established experiment
runner. It retains live and captured stdout/stderr, process-group interrupt
handling on POSIX, Git and system provenance, execution-context observations,
declared artifacts, and immediate lineage.

## Scheduler execution

`SlurmBackend` and `PBSBackend` stage an immutable plan and a standard-library
worker, submit through the scheduler, observe only the recorded job, and import
the worker result. The worker resolves and preflights the executable on the
allocated host before launching the scientific process.

Bourne does not install dependencies, load environment modules, transfer
arbitrary scientific files, infer accounts/QoS/reservations, or bypass site
policy. Shared visibility of the staging and working directories remains a site
requirement.

## Execution-plane provenance

Requested resources live in the workload and plan. Allocation facts are
observed on the execution host and stored separately. Execution attempts,
scheduler submissions/jobs, allocation observations, lifecycle events, and the
actual scientific experiment remain distinct durable records.

Scheduler submission is not a scientific experiment. Scheduler `COMPLETED` is
not scientific success. Without a valid worker result, Bourne records
`collection_failed` and does not create a successful experiment.

## Slurm lifecycle

Slurm submission uses `sbatch --parsable`. Active observation uses exact-job
`squeue` scoped to the submitting identity. When the job leaves that view,
Bourne attempts an optional, bounded exact-job `sacct` lookup. Missing or
failed accounting remains explicitly unobservable rather than being reported
as completion.

Waiting begins at a configurable 15-second poll interval, backs off by 1.5x to
60 seconds, and has no arbitrary default scientific wall-clock timeout.
Cancellation accepts a Bourne execution reference and validates its recorded
submitting identity before issuing `scancel` for the exact recorded job ID.

## PBS lifecycle

PBS submission uses `qsub`; observation uses exact-job `qstat`; cancellation
uses the exact recorded ID through `qdel`. Recognized unknown/purged-job
responses become explicit unobservable observations. Other status failures
remain query errors. An absent worker result never establishes scientific
success.

## Security

Scheduler clients are invoked with explicit argv and `shell=False`, bounded
output, and timeouts. Scientific argv is stored as JSON and passed to the
scientific process as argv; it is never flattened into scheduler shell syntax.
The controller never queries all users, cancels an arbitrary caller-supplied
job ID, SSHes to compute nodes, escalates privileges, mutates containers, or
changes scheduler policy.

On POSIX, Bourne derives scheduler ownership from the effective UID and the
system password database. It does not trust `USER`, `LOGNAME`, `LNAME`, or
`USERNAME`. The scheduler remains the final authorization boundary.

Worker results use bounded, versioned JSON. Import validates identities, types,
relationships, size, nesting, collection counts, and consistency with the
immutable plan before a single transactional write. Repeated collection is
idempotent.

## Examples

Direct planning and execution:

~~~bash
bourne discover
bourne plan --backend direct -- python examples/demo.py
bourne execute --backend direct -- python examples/demo.py
bourne execution list
bourne execution show @1
~~~

Scheduler-oriented planning and lifecycle:

~~~bash
bourne plan --backend slurm --cpus 8 --memory 16G --walltime 30m -- ./solver case.yaml
bourne execute --plan @1
bourne execution show @1
bourne execution wait @1
~~~

While the recorded scheduler job is active, cancellation is requested with
`bourne execution cancel @1`. Use `--backend pbs` for a PBS inventory target.

## Compatibility

The release supports Python 3.10 through 3.13 and is continuously tested on
Linux and macOS. The runtime has no third-party dependencies. Existing v0.1.1,
v0.2.0, and v0.3.0 databases migrate transactionally to schema 4 while
preserving their prior experiment, artifact, lineage, execution-context,
inventory, and discovery-evidence records.

The v0.1–v0.3 CLI remains available: `run`, `list`, `show`, `compare`, `trace`,
`discover`, and `inventory`, including declared inputs/outputs and
`derived_from` lineage.

## Current limitations

- Scheduler allocations must provide Python 3 and see the staging directory.
- Scientific working directories and declared files are not copied or archived.
- There is no automatic dependency installation, module loading, launcher
  injection, container orchestration, SSH execution, retry, or policy inference.
- Slurm/PBS parsing needs more validation across real vendor/site variants.
- Slurm accounting is optional and may be absent or delayed.
- stdout and stderr are captured in memory; large-log spooling is future work.
- Native Windows scheduler execution and process-tree supervision are not
  claimed.
- Execution status does not establish scientific correctness; verification is
  a separate future capability.

## What comes next

The v0.4 release stops at trustworthy planning and execution provenance. A
future milestone may add explicit scientific-verification evidence, but v0.4
does not implement verification, autonomous repair, agents, or a graph engine.

See [Workload planning and scheduler execution](WORKLOAD_EXECUTION.md) for the
full model and operational details.
