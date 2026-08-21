# Project Bourne v0.5.0 — Unified Execution Requests, Telemetry and Verification

## Overview

Project Bourne v0.5.0 adds a versioned, machine-readable user-intent boundary
to the v0.4 workload and execution architecture. It also adds low-overhead
summary telemetry and deterministic verification of captured output artifacts.
The runtime remains framework-independent, local-first, and free of third-party
dependencies.

The progression is cumulative:

- v0.1 answered: What ran?
- v0.2 answered: What went in, what came out, and what was derived from what?
- v0.3 answered: What execution infrastructure is available?
- v0.4 answered: What does the workload need, where can it run, and how is it
  executed?
- v0.5 answers: What execution does the user actually want, how can multiple
  frontends express that intent through one stable contract, did the expected
  deterministic outputs appear, and what low-overhead evidence was observed?

## Why `ExecutionRequest`

`ExecutionRequest` is the immutable record of what a user asked Bourne to do.
It is deliberately distinct from Bourne's bounded workload interpretation, a
plan for a particular inventory, an execution attempt, scheduler facts, and
the resulting scientific experiment. This separation preserves user intent
without confusing it with decisions or observations made later in execution.

## `bourne.json`

External request documents use kind `bourne.execution-request` and schema
version 1. The packaged JSON Schema is available through
`bourne request schema`; `bourne request init`, `validate`, and `show` provide
bounded data operations that do not discover, plan, or execute a workload.

The standard-library parser rejects duplicate and unknown fields, enforces
document, collection, string, and nesting limits, and never expands a shell,
imports project code, or executes user code. Relative working directories are
resolved from the request file while their lexical form is retained.

## Unified frontend pipeline

Request files, existing CLI flags, and Python callers all compile through the
same pipeline:

```text
ExecutionRequest -> WorkloadSpec -> ExecutionPlan -> ExecutionAttempt
                                                        -> Experiment
```

`bourne plan --request bourne.json` and
`bourne execute --request bourne.json` use this pipeline. Released flag-based
syntax remains supported and does not maintain a separate execution path.

## Intent fidelity

A requested parent experiment may be a full ULID, a unique prefix, `latest`,
or `@N`. Bourne persists that lexical reference on the request and separately
records the canonical ULID resolved for the compiled workload. An invalid or
ambiguous parent prevents persistence; resolving the parent never overwrites
the user's original request or changes request identity.

## Telemetry

Summary telemetry is enabled by default and derives only from evidence Bourne
already captured: wall duration, UTF-8 stdout/stderr byte counts, complete
declared-artifact byte totals, requested and allocated resources, and scheduler
queue timing when timestamps establish it. It performs no utilization sampling
or profiling. Missing metrics remain unavailable rather than becoming zero.
`"telemetry": {"mode": "off"}` disables the summary.

## Verification

The first deterministic checks are `output_exists`, `output_min_bytes`, and
`output_sha256`. They evaluate captured output `Artifact` records and produce
`passed`, `failed`, or `unknown` evidence plus a separate aggregate result.
Process status is never rewritten: an experiment can complete while
verification fails. These checks establish artifact facts, not general
scientific validity.

## Worker protocol

New staged plans and worker results use protocol version 2. The controller
validates staged request identity, telemetry policy, verification checks,
experiment relationships, and bounded result data before one transactional
import. The compute-side worker captures artifacts and builds telemetry and
verification evidence without requiring Bourne to be installed on the compute
node.

## Direct execution

Direct requests reuse Bourne's live stdout/stderr, process-group interruption,
artifact capture, lineage, experiment persistence, telemetry, and verification
services. A failed command remains a persisted experiment, and execution status
remains separate from verification status.

## Slurm and PBS compatibility

The v0.4 planning and scheduler lifecycle model remains intact. Request-backed
Slurm and PBS executions stage protocol-2 data through the same worker trust
boundary. Submission, scheduler state, allocation observations, experiment
status, telemetry, and verification remain distinct provenance facts.

## Security

Request parsing and planning do not invoke a shell, expand environment
variables, evaluate templates, import project modules, or run the requested
command. Exact argv values are preserved. Secrets and arbitrary environment
variables are not captured. Scheduler result import remains bounded,
relationship-checked, identity-checked, and transactional.

## Migration

SQLite schema 5 adds execution-request records and links, telemetry summaries,
verification runs, and verification checks. Opening schema 1, 2, 3, or 4 data
performs a deterministic transactional migration. Existing history is retained
without inventing requests, telemetry, or verification for earlier records.

## Backward compatibility

Existing run/list/show/compare/trace, discovery/inventory, workload planning,
and direct/Slurm/PBS execution workflows remain supported. The v0.5 worker and
controller safely read released v0.4 staged-plan/result protocol version 1;
request-less v0.4 work does not acquire fictional v0.5 history. The external
request schema remains version 1, while new staged plans and results use
protocol version 2.

## License

Beginning with v0.5.0, Project Bourne is distributed under the Apache License
2.0. Earlier releases through v0.4.0 remain under the MIT License terms under
which they were released.

This licensing transition does not change runtime behavior, SQLite schema 5,
the ExecutionRequest version-1 contract, worker protocol 2, compatibility with
released worker protocol 1, or the zero-runtime-dependency policy.

## Limitations

v0.5.0 does not provide high-frequency CPU/GPU/memory/I/O sampling, profiling,
arbitrary verification programs, automatic artifact discovery, automatic
dependency or module installation, retries, remote copying, SSH orchestration,
container orchestration, general scientific-validity inference, or a web
interface. Captured stdout and stderr remain accumulated in memory before final
persistence.

## What comes next

Future MCP, TypeScript/npm and `npx`, Skill, or natural-language agent
frontends should produce or validate the same version-1 `ExecutionRequest` and
call Bourne's structured services:

```text
Human -> Agent or Skill -> ExecutionRequest -> Bourne core
                                             -> workload -> plan -> backend
```

Those future producers should not duplicate workload inspection, inventory
resolution, scheduler submission, execution supervision, artifact capture,
telemetry, or verification. No MCP server, TypeScript/npm or `npx` package,
Skill, natural-language agent, autonomous loop, or graph engine is implemented
in v0.5.0.
