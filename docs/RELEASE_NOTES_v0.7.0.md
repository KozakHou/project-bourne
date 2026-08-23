# Project Bourne v0.7.0 — Site-Aware Planning

Project Bourne v0.7 adds site-aware, constraint-based scientific execution
while preserving a strict HPC security boundary:

```text
local control / optional AI / local stdio MCP
  → existing OpenSSH configuration
  → one-shot non-AI user-space worker on the access node
  → existing Slurm or PBS scheduler
  → execution-scoped worker inside the allocation
  → exact scientific argv
```

No AI agent, MCP server, AI credential, inbound service, root access,
persistent daemon, or public-internet access is required on the cluster.

## Planning and provenance

- Durable `local` and `remote_ssh` sites contain no secrets.
- Discovery records where each fact was observed; login-node observations are
  not promoted to compute-allocation facts.
- Site policy claims retain sources and conflicts. Only explicit normative
  hard policy can reject a candidate; advisory evidence cannot. Applicability
  scopes prevent queue/partition/node-class/account policy from leaking into
  unrelated shapes, and typed Core/MCP submission retains the evidence kind.
- ResourceShape is first-class and separates nodes, ranks, CPUs, threads,
  accelerators, memory, and wall time. Real scheduler discovery now feeds a
  bounded request-shape generator without converting visibility into access.
- Candidate exploration is deterministic, ephemeral, capped at 64, and reports
  search coverage and truncation. Hard pruning plus fair group enumeration
  prevents an early shape from starving later viable shapes.
- Declarative providers use a versioned typed JSON AST. Trusted-code providers
  require explicit local enablement and must already exist at the remote site.
- Workload variants are separately staged, hashed, linked, and safety-classed;
  the original source is never overwritten. Selection now materializes a
  changed provider-bound JSON input automatically after an explicit semantic
  trust/declaration/approval decision.
- Environments are selected only from existing observations. Activation is
  typed and execution-scoped; v0.7 does not install or build dependencies.

## Remote execution integrity

OpenSSH and SCP use exact argv with `shell=False`; Bourne does not weaken host
trust or accept arbitrary remote command text. A compatible remote worker may
already exist, or Bourne bootstraps an exact-version, digest-verified zipapp to
the user's cache without installing dependencies.

Execution identity is created before submission. Remote state is written
atomically. Once a scheduler accepts a job, the scheduler owns its lifetime and
no keepalive is required. A lost or ambiguous SSH response is never treated as
permission to resubmit: the same execution identity is reconciled against
durable remote state and the exact scheduler job. Missing evidence remains
unknown and never becomes a fabricated successful experiment.

## Interfaces and compatibility

The CLI, Python service, and local stdio MCP adapter expose site inspection,
site discovery, candidate generation, explicit selection, execution, and
reconciliation. MCP exposes no generic shell or remote-filesystem primitive.

ExecutionRequest remains version 1. The remote-worker protocol is v1, the
worker-result protocol is v2, and the staged-plan protocol is v3; released
staged payloads remain backward-readable. SQLite migrates transactionally to
schema 6 while preserving released databases. The Python package remains
compatible with Python 3.10+, keeps zero base runtime dependencies, and
preserves the optional `mcp` extra.

The Bourne control plane is supported and tested on Linux and macOS. Native
Windows is not yet validated or supported.

## Development tooling

`uv.lock` is committed and uv is the canonical contributor, dependency-locking,
test, and build frontend. CI uses locked/frozen semantics and release builds use
`uv build --no-sources`. setuptools remains the build backend. uv is not a
runtime, pip-user, npm-launcher, remote-worker, or HPC-node dependency.

No v0.7 artifact is published during implementation or release-candidate
preparation.
