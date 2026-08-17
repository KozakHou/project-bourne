# Project Bourne v0.4.0 — Workload Planning and Execution

Status: development (`0.4.0.dev0`), not published.

## Highlights

- Immutable, framework-independent `WorkloadSpec`
- Bounded non-executing workload inspection with explicit evidence states
- Immutable `ExecutionPlan` tied to one inventory snapshot
- Conservative direct/Slurm/PBS resolver with ambiguity refusal
- Durable execution, scheduler-job, allocation, event, and experiment links
- Direct execution through existing Bourne provenance semantics
- Slurm and PBS submission/status/wait/cancel/collection boundaries
- Portable stdlib-only zipapp worker for actual allocation-side provenance
- Bounded JSON result validation and transactional import
- Schema 4 migration preserving schema 1–3 history
- Zero third-party runtime dependencies

## Safety and truthfulness

Scientific argv is never shell-interpreted by Bourne. Planning never executes
unknown software. Scheduler operations are restricted to recorded Bourne jobs,
with current-user scoping where supported and identity checks before
cancellation. No compute-node SSH, privilege escalation, environment mutation,
dependency installation, or scheduler-policy mutation is performed.

Submission, scheduler state, allocation, and experiment outcome remain distinct.
A completed scheduler job without a valid worker result is not a completed
scientific experiment.

See [Workload planning and scheduler execution](WORKLOAD_EXECUTION.md) for the
full model and current limitations.
