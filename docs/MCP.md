# Project Bourne MCP integration

Project Bourne v0.6 adds a local agent interface using the official Model
Context Protocol Python SDK. MCP is an optional adapter over Bourne's existing
structured services; it is not a second resolver, scheduler implementation, or
execution engine.

## Install and run

The core remains dependency-free. From a source checkout, install MCP support
explicitly:

```bash
python -m pip install -e ".[mcp]"
bourne mcp
```

After v0.6 is published, use
`python -m pip install "bourneprov[mcp]"`. The latest public release remains
v0.5.0 while this document is on the v0.6 development branch.

The canonical entrypoint uses stdio only. Protocol frames use stdout; human
diagnostics, logs, and direct workload output use stderr. Set
`BOURNE_MCP_LOG_LEVEL` to `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`;
the default is `WARNING`. Request documents and environment dumps are not
logged.

The npm launcher provides the same server after the v0.6 package is published:

```bash
npx -y @project-bourne/mcp
npx -y @project-bourne/mcp --doctor
npx -y @project-bourne/mcp --no-bootstrap
```

It requires Node.js 22+. It first locates Python 3.10+ with the exact compatible
`bourneprov` and MCP extra. If none exists, it may install that exact release
into a private versioned user cache. It never installs into an active project,
virtual environment, Conda environment, or system Python; never uses `sudo`;
and never invokes a shell. Bootstrap output goes to stderr.

Development versions are coupled exactly:

```text
@project-bourne/mcp 0.6.0-dev.0 → bourneprov 0.6.0.dev0
```

## Discovery and Registry identity

The stable official MCP Registry identity is:

```text
io.github.KozakHou/project-bourne
```

The root `server.json` describes the existing local stdio server and points to
the npm package `@project-bourne/mcp`. Its name must equal the package's
`mcpName`; deterministic repository tests enforce that identity and version
coupling. No HTTP or hosted transport is advertised.

The development npm package and Registry entry are not public yet. Final
Registry publication must occur only after the matching final npm version is
available and its `mcpName` has been verified.

## Generic host configuration

Any local MCP-compatible host that accepts a stdio command can use:

```json
{
  "command": "npx",
  "args": ["-y", "@project-bourne/mcp"]
}
```

Alternatively configure `bourne mcp` after installing `bourneprov[mcp]` in a
known environment. Host-specific configuration is intentionally not claimed
unless validated separately.

## Server contract

The stable identity is:

```text
name: project-bourne
title: Project Bourne
transport: stdio
protocol target: 2026-07-28
```

The compact tool surface is:

| Tool | Effect |
|---|---|
| `bourne_request_schema` | Return the packaged ExecutionRequest v1 JSON Schema. |
| `bourne_validate_request` | Validate and normalize without discovery, planning, persistence, or execution. |
| `bourne_discover` | Run bounded discovery and add an immutable inventory snapshot. |
| `bourne_inventory` | Read `latest`, full ID, unique prefix, or `@N`; never rediscover. |
| `bourne_plan` | Persist intent/workload and resolve against one existing inventory; never execute. |
| `bourne_execute_plan` | Execute one existing immutable plan without altering it. |
| `bourne_execution_get` | Read request, plan, lifecycle, scheduler, allocation, experiment, telemetry, and verification state. |
| `bourne_execution_wait` | Wait on one existing Bourne-managed scheduled execution with a bounded caller timeout. |
| `bourne_execution_cancel` | Cancel only the exact job attached to one Bourne execution, subject to existing identity checks. |
| `bourne_trace_artifact` | Trace artifact identity, producer, inputs, and ancestry without guessing. |

Every tool has a structured output envelope:

```json
{"ok": true, "data": {}, "error": null}
```

Product conditions use machine-readable error codes such as `invalid_request`,
`no_inventory`, `ambiguous_inventory`, `incompatible_request`,
`unresolved_plan`, `unknown_plan`, `unknown_execution`,
`execution_not_allowed`, and `scheduler_error`. Python tracebacks are not
returned as normal tool output.

## Two-phase execution and safety

The primary workflow is:

```text
MCP input
  → canonical ExecutionRequest v1 parser
  → existing ExecutionService
  → WorkloadSpec
  → existing resolver
  → immutable ExecutionPlan
  → explicit execute_plan
```

`bourne_plan` never invokes scientific code and never performs discovery.
`bourne_execute_plan` accepts only a persisted plan ID. A different command,
resource request, target, context, or backend requires a new request and plan.
The adapter cannot submit arbitrary scheduler commands or cancel an arbitrary
scheduler job ID.

Tool annotations describe likely effects for host UX. They are not an
authorization or security boundary. Bourne Core remains responsible for
resolution, shell safety, immutable plan checks, scheduler identity and exact
job ownership, result validation, artifact capture, and durable provenance.

Unknown does not become compatible. Multiple equivalent targets do not become
an arbitrary selection. Historical evidence does not become a current
observation, and visible infrastructure does not imply authorization.

## Results and limitations

Direct execution returns an execution and experiment state. Scheduler
submission returns an execution ID, scheduler family, recorded job ID, and
`submitted` state; it does not claim completion. Process status, verification,
telemetry, and scientific validity remain distinct.

The v0.6 server is local stdio on Linux and macOS. It does not provide a hosted
HTTP endpoint, built-in LLM, natural-language parser, MCP Tasks mapping,
dashboard, remote Bourne service, or Windows scientific process-tree claim.
SQLite remains local, and `BOURNE_DB` selects the database as with the CLI.
