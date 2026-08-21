# Execution Requests, Telemetry, and Verification

Project Bourne v0.5 introduces a stable user-intent boundary without replacing
the workload, planning, execution, scheduler, or experiment models delivered
in earlier versions.

```text
CLI flags ─┐
JSON file ─┼─> ExecutionRequest ─> WorkloadSpec ─> ExecutionPlan
Python SDK ┘                                      └─> ExecutionAttempt
                                                        └─> Experiment
```

These identities are deliberately separate:

- `ExecutionRequest` records what the user asked for.
- `WorkloadSpec` records Bourne's bounded understanding of that workload.
- `ExecutionPlan` records how a specific inventory can satisfy it.
- `ExecutionAttempt` records one attempt to carry out the plan.
- scheduler jobs and allocations record infrastructure lifecycle facts.
- `Experiment` records the process that actually executed.
- telemetry and verification record measured and evaluated outcomes without
  changing experiment status.

## JSON contract

The first external request contract is:

```json
{
  "kind": "bourne.execution-request",
  "version": 1,
  "command": ["python", "train.py"],
  "working_directory": ".",
  "artifacts": {
    "inputs": ["config.yaml"],
    "outputs": ["result.h5"]
  },
  "resources": {
    "cpus": 8,
    "gpus": 1,
    "nodes": 1,
    "mpi_ranks": 1,
    "memory": "16GiB",
    "walltime": "2h"
  },
  "execution": {
    "backend": "auto",
    "target": "gpu",
    "context": "current environment"
  },
  "provenance": {
    "parent_experiment": "01M00000000000000000000000"
  },
  "telemetry": {"mode": "summary"},
  "verification": {
    "checks": [
      {"type": "output_exists", "path": "result.h5"}
    ]
  }
}
```

Only `kind`, `version`, and a non-empty `command` are required. Model defaults
are explicit: working directory `.`, execution backend `auto`, telemetry mode
`summary`, and no artifacts, resource constraints, lineage, or verification
checks.

The request schema version is independent of the `bourneprov` package version
and SQLite schema version. Retrieve the packaged JSON Schema with:

```bash
bourne request schema
```

The source file is
`src/bourneprov/schemas/execution-request-v1.schema.json`. No network access or
`jsonschema` dependency is needed at runtime; Bourne uses a matching bounded
standard-library validator.

## CLI

```bash
bourne request init --output bourne.json -- python train.py
bourne request validate bourne.json
bourne request show bourne.json
bourne request schema

bourne plan --request bourne.json
bourne execute --request bourne.json
```

`request init`, `validate`, `show`, and `schema` are data operations. They do
not discover infrastructure, inspect a workload, create a database, plan, or
execute the command.

`--request` is authoritative. It may be combined with control-plane options
such as `--snapshot` and `--json`, but not with a command, artifact/resource
flags, placement flags, `--derived-from`, or `--plan`. Ambiguous combinations
fail instead of silently overriding request fields.

The released flag syntax remains available:

```bash
bourne plan --backend slurm --cpus 8 -- python train.py
bourne execute --backend direct --output result.h5 -- python train.py
```

Those flags are first compiled to the same `ExecutionRequest` service used by
request files. Python callers can use `execution_request_from_cli`,
`parse_execution_request`, `request_to_workload`,
`ExecutionService.plan_request`, and `ExecutionService.execute_request`
without parsing human terminal output.

## Path semantics

For `/project/case/bourne.json`, the request base is `/project/case`.

- Relative `working_directory` values resolve from that base.
- Absolute `working_directory` values remain absolute.
- Declared input/output paths are interpreted from the resolved scientific
  working directory when artifacts are captured.
- The original lexical working-directory value and its canonical
  `Path.resolve(strict=False)` value are preserved separately.
- CLI- and SDK-created requests use the caller's current working directory as
  their base.

The parser does not expand `$HOME` or `${HOME}`, run `$(command)` or backticks,
interpret `;`, `|`, or `&`, strip quotes, invoke a shell, evaluate templates,
import project modules, or run a program. Unicode and these characters remain
literal JSON strings and exact argv values.

## Parent-reference intent

`provenance.parent_experiment` accepts the same full ULID, unique prefix,
`latest`, or `@N` references as the CLI. Bourne preserves that lexical value as
the requested parent reference. Planning resolves it once against the configured
database and records the resulting canonical experiment ULID separately; the
compiled `WorkloadSpec` receives only that canonical ID.

For example, a request containing `"parent_experiment": "latest"` remains
inspectable as `latest` after persistence while also reporting which full ULID
`latest` meant at planning time. Resolution does not change the request identity
or overwrite the original intent. An invalid or ambiguous reference prevents
request, workload, and plan persistence.

## Validation and bounds

Version 1 rejects unknown fields at every semantic object boundary so a typo
such as `"gpu": 4` cannot silently become an unconstrained request. It also
rejects duplicate JSON object keys.

Current limits are:

| Item | Limit |
|---|---:|
| UTF-8 request document | 1 MiB |
| argv values | 4,096 |
| one string | 16,384 characters |
| inputs per request | 2,048 |
| outputs per request | 2,048 |
| verification checks | 2,048 |
| JSON nesting depth | 12 |

Artifact paths are non-empty and unique within each role. Verification paths
must exactly name declared outputs. Malformed requests are rejected before any
request, workload, plan, or execution state is created.

## Request source provenance and immutability

Persisted requests record a source kind of `cli`, `file`, or `sdk`. A file
source may record its canonical local path; arbitrary environment variables and
secrets are not captured. `mcp` and `agent` are future producers, not v0.5
runtime features.

Every accepted planning invocation persists a new ULID request identity. Once
stored, a request is immutable; changing intent creates a new request. Reusing
the same `bourne.json` for another plan or execution therefore creates another
request identity with equivalent semantics rather than mutating history.

## Summary telemetry

Telemetry policy is `summary` by default or `off`. Summary mode samples
nothing and adds no profiler. It derives only facts Bourne already captured:

- experiment wall duration;
- UTF-8 byte counts of captured stdout and stderr;
- total bytes for a declared artifact role only when every relevant artifact
  is present and completely captured;
- requested resources from the immutable plan;
- allocated resources from the compute allocation observation;
- scheduler queue interval when submission and execution-start timestamps
  establish it.

Each summary identifies its evidence sources and coverage. Requested resources
are constraints, allocated resources are infrastructure observations, and
neither is utilization. Bourne makes no CPU, GPU, memory, I/O, whole-node, or
process-tree utilization claim in v0.5. An unavailable metric remains `null`
and is listed as unavailable; it is never represented as zero.

## Deterministic verification

Verification operates only on captured `Artifact` records for declared
outputs. It does not reread arbitrary files after execution, execute a script,
or import a user verifier.

Supported checks are:

- `output_exists`: `present` passes, `missing` fails, and unknown existence is
  unknown.
- `output_min_bytes`: a present, completely captured artifact is compared with
  `min_bytes`; missing fails and incomplete capture is unknown.
- `output_sha256`: a present, completely captured SHA-256 is compared with the
  requested digest; missing or mismatch fails and unavailable/incomplete
  hashing is unknown.

Check states are `passed`, `failed`, or `unknown`. Aggregate states are:

```text
any failed                         -> failed
none failed + at least one unknown -> unknown
all checks passed                  -> passed
no checks                          -> not_requested
```

Verification never rewrites experiment status. A process can be `completed`
with failed artifact verification. Conversely, these file checks do not prove
domain-specific scientific validity.

## Scheduler and protocol compatibility

New staged plans and worker results use protocol version 2 when linked to a
v0.5 request. The staged request identity, telemetry policy, and verification
checks are structurally validated and must match the immutable workload and
plan. The compute worker captures artifacts, builds the low-overhead summary,
and evaluates checks on the execution plane. The controller validates all
relationships and imports the experiment, allocation, telemetry, verification
run, and check rows transactionally.

The v0.5 worker and controller still read released v0.4 staged-plan/result
protocol version 1. A request-less v0.4 plan remains valid; its result does not
invent v0.5 request, telemetry, or verification history.

## Persistence

SQLite schema 5 adds only:

- `execution_requests`;
- `execution_request_workload_links`;
- `telemetry_summaries`;
- `verification_runs`;
- `verification_checks`.

Existing schema-4 facts remain in their original tables. Migration does not
reverse-engineer old plans into fictional requests.

## Future producers

The request model and JSON Schema are the integration boundary for future MCP,
TypeScript/npm, Skills, or natural-language agents. Such a producer should
construct or validate ExecutionRequest v1 and call Bourne's structured
services. It should not duplicate workload inspection, inventory resolution,
scheduler submission, execution supervision, artifact capture, telemetry, or
verification logic. None of those future producer packages is implemented in
v0.5.
