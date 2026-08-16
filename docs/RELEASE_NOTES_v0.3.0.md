# Project Bourne v0.3.0 — Compute Site Discovery

Project Bourne v0.3.0 introduces safe, read-only discovery of the scientific
execution environment available to the current researcher, from personal
workstations and laptops to scheduler-backed HPC systems.

## Overview

v0.3 extends Bourne from experiment and artifact history to a structured,
durable understanding of the execution environment available to a researcher.
Each discovery is an immutable local snapshot; Bourne observes infrastructure
without operating or modifying it.

## Highlights

- Immutable compute-site inventory snapshots
- Current identity and access-target observations
- User-relevant storage discovery and filesystem/mount observations
- Discovered system, Conda, virtualenv, container-metadata, and loaded-module
  execution contexts
- Generic executable capability discovery, including unknown software as a
  first-class capability
- System and hardware capability observations
- Read-only Slurm partition and PBS queue summaries
- Historical Bourne execution evidence
- Explicit provider status: `complete`, `unavailable`, `partial`, `error`, or
  `timeout`
- Structured JSON output and exact capability search
- Transactional schema 3 migration
- Zero third-party runtime dependencies

## Example

```bash
bourne discover
bourne inventory
bourne inventory --find mpirun
bourne inventory --json
```

## Compute topology

Bourne can represent:

```text
current identity
    ↓
access target
    ↓
scheduler
    ↓
visible execution-target classes
```

Storage, execution contexts, and capabilities are recorded alongside this
topology. Scheduler summaries do not require SSH connections to compute nodes.
An access target and a scheduler-visible compute target remain distinct.

## Generic local compute

The same model works for a personal laptop, desktop workstation, DGX-class
machine, or laboratory GPU system. Scheduler discovery may simply be
unavailable; a scheduler-free machine is a complete and valid discovery target.
HPC is one supported topology, not the definition of the product.

## Safety

- Direct PATH scanning is non-recursive, bounded, and never executes unknown
  binaries.
- Provider subprocesses use explicit argument vectors, bounded execution time,
  and bounded captured output.
- Discovery does not traverse other users' homes, recursively scan storage,
  inspect SSH credentials, or dump arbitrary environment variables.
- Discovery does not submit or cancel scheduler jobs and does not SSH to
  compute nodes.
- Container providers do not start, stop, mutate, attach to, or execute inside
  containers.
- Discovery does not query network registries or perform Internet discovery.

## Truthful evidence

Current observations, historical evidence, unavailable or partial providers,
and unknown authorization are represented distinctly. Historical success does
not establish current availability, and visibility does not prove permission.
Before persistence, evidence references are validated against subjects of the
declared type in the same snapshot; a record cannot claim both current
observation and historical-only evidence.

## Compatibility

The existing experiment and artifact workflow remains available:

```text
bourne run
bourne list
bourne show
bourne compare
bourne trace
bourne completion
--input
--output
--derived-from
```

Existing v0.1.1 and v0.2.0 databases migrate transactionally to schema 3 while
preserving experiment outcomes, artifacts, lineage, and execution context.

## Current limitations

- No workload requirement inference or execution-context resolver
- No scheduler submission or automatic execution planning
- No remote SSH topology discovery or compute-node scanning
- No container-internal capability probing
- No institutional policy discovery; retention, quota, backup, and purge policy
  remain unknown
- Visible scheduler targets are not proven authorized
- No dependency resolution, scientific verification, or profiling/resource
  telemetry
- No MCP or agent interface
- Experiment stdout/stderr still accumulate in memory before final persistence

## Design principle

Bourne understands scientific execution infrastructure generically. Unknown
software remains a first-class workload and capability.

**Every experiment has a history.**
