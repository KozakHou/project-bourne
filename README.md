# Project Bourne

Project Bourne is open-source execution and provenance infrastructure for
reproducible scientific and engineering workloads.

It plans and executes computational experiments across local machines, GPUs,
Slurm, and PBS while preserving inputs, outputs, execution context, artifact
lineage, telemetry, verification, and the history needed to reproduce a
result.

It is designed for researchers, students from undergraduate through PhD level,
faculty, research engineers, computational scientists, scientific software
users, and scientific-computing teams across academia, public research, and
industry R&D.

## Quick Start

### Human

The human CLI is public today:

~~~bash
python -m pip install bourneprov

bourne run python examples/demo.py
bourne list
bourne show @1

# Or execute an ExecutionRequest v1 document:
bourne execute --request bourne.json
~~~

### Agent / MCP

The v0.6.0 agent and MCP entrypoints are public:

~~~bash
python -m pip install "bourneprov[mcp]==0.6.0"
npx -y @project-bourne/mcp@0.6.0
~~~

For development from a source checkout instead:

~~~bash
python -m pip install -e ".[mcp]"
bourne mcp
~~~

## Why Bourne

Bourne wraps arbitrary executables without requiring changes to the scientific
program. It is local-first and framework-agnostic: Python, compiled solvers,
Julia, MPI programs, and other commands use the same durable experiment model.

~~~bash
bourne run bash -c "echo hello"
bourne run ./solver case.yaml
bourne run julia simulation.jl
bourne run mpirun -np 64 ./solver
~~~

Program stdout and stderr remain visible during execution and are preserved in
the experiment record.

## Architecture

Bourne Core owns deterministic execution, planning, storage, and provenance.
Humans can use it through the CLI or Python services; agents can use the same
services through the optional MCP adapter:

~~~text
             Project Bourne Core
                    │
       ┌────────────┼────────────┐
       │            │            │
      CLI          SDK          MCP
    humans                     agents
~~~

The agent interface is an optional access path, not Bourne's product identity.
MCP works without the portable Skill, and Bourne contains no embedded LLM.

## Agent and MCP Integration

The canonical local stdio server is `bourne mcp`. The stable official MCP
Registry identity is `io.github.KozakHou/project-bourne`, and the portable
Agent Skill is at [`skills/project-bourne`](skills/project-bourne). The v0.6.0
npm package and matching Registry entry are public.

An MCP-compatible agent can translate an explicit request such as “Run this
simulation using four GPUs and preserve provenance” into ExecutionRequest v1,
ask Bourne to plan it, show the deterministic resolution, and execute the
immutable plan after execution intent is established. Bourne itself does not
interpret unconstrained natural language and does not call another model.

The agent path is deliberately two-phase:

~~~text
agent intent → ExecutionRequest v1 → bourne_plan → inspect → bourne_execute_plan
~~~

Planning never runs the workload or silently discovers infrastructure.
Ambiguous targets and unknown facts remain unresolved. MCP annotations are host
UX hints; Bourne Core still enforces immutable plans, exact argv, scheduler job
ownership, artifact semantics, and provenance. See [MCP integration](docs/MCP.md)
and [Agent guidance](docs/AGENTS.md).

## Execution Requests

An execution can now be described once in a bounded, versioned JSON request:

~~~json
{
  "kind": "bourne.execution-request",
  "version": 1,
  "command": ["python", "train.py", "--case", "case1"],
  "artifacts": {
    "inputs": ["config.yaml"],
    "outputs": ["result.h5"]
  },
  "resources": {"cpus": 8, "gpus": 1, "walltime": "2h"},
  "execution": {"backend": "direct"},
  "verification": {
    "checks": [
      {"type": "output_exists", "path": "result.h5"},
      {"type": "output_min_bytes", "path": "result.h5", "min_bytes": 1024}
    ]
  }
}
~~~

Save it as `bourne.json`, then use the same intent for planning or execution:

~~~bash
bourne request validate bourne.json
bourne request show bourne.json

bourne discover
bourne plan --request bourne.json
bourne execute --request bourne.json
~~~

Create a minimal request without executing or discovering anything:

~~~bash
bourne request init --output bourne.json -- python train.py
bourne request schema > execution-request-v1.schema.json
~~~

Existing flag-based commands remain supported. They compile into the same
`ExecutionRequest → WorkloadSpec → ExecutionPlan` pipeline rather than a
parallel implementation:

~~~bash
bourne execute --backend direct --cpus 2 --output result.txt -- python script.py
~~~

For a request file, a relative `working_directory` is resolved from the
request file's directory. Declared artifacts are then resolved from that
scientific working directory. Bourne preserves both the lexical and resolved
working-directory values and does not expand `$HOME`, evaluate shell syntax,
import project code, or execute anything while parsing or planning.

Parent references follow the same intent-preserving rule. A request may use
`latest`, `@N`, a unique prefix, or a full ULID. Bourne retains that requested
value while separately recording the canonical parent ULID used by the compiled
workload.

Summary telemetry is enabled by default and uses already captured facts: wall
time, UTF-8 stdout/stderr byte counts, known artifact byte totals, requested
resources, observed allocation, and scheduler queue timing when timestamps
establish it. `"telemetry": {"mode": "off"}` disables the summary. Missing
metrics remain unavailable, never zero.

The initial deterministic verification checks are `output_exists`,
`output_min_bytes`, and `output_sha256`. They evaluate only captured declared
output `Artifact` records. Verification is persisted separately from process
status: an experiment may be `completed` while verification is `failed` or
`unknown`. These checks establish artifact facts, not general scientific
validity. See [Execution requests, telemetry, and verification](docs/EXECUTION_REQUESTS.md)
for the exact contract and safety limits.

## Planning and Execution

Project Bourne v0.4.0 adds a durable planning layer over v0.3 inventories:

~~~bash
bourne discover

bourne plan --backend direct -- python examples/demo.py
bourne execute --backend direct -- python examples/demo.py

bourne execution list
bourne execution show @1
~~~

`bourne plan` never runs the scientific command and never performs discovery.
It creates a framework-independent `WorkloadSpec`, compares its explicit and
inferred requirements with an existing inventory, explains every candidate,
and persists an immutable `ExecutionPlan` only when selection is unambiguous.
Use explicit resource and placement constraints when needed:

~~~bash
bourne plan \
  --backend slurm \
  --target gpu \
  --cpus 16 \
  --gpus 4 \
  --nodes 1 \
  --memory 64G \
  --walltime 2h \
  -- ./solver case.yaml
~~~

Execute a selected Slurm plan and then inspect or wait for the resulting
execution attempt:

~~~bash
bourne execute --plan @1
bourne execution show @1
bourne execution wait @1
~~~

While a recorded job is still active, `bourne execution cancel @1` requests
cancellation of that Bourne-managed job. The same planning and lifecycle model
supports `--backend pbs`.

Direct execution reuses Bourne's existing live-output, process-group, artifact,
lineage, and experiment-provenance machinery. Slurm and PBS plans use a
self-contained Bourne worker staged with the plan. The worker performs
preflight and records the actual allocated host and scientific experiment;
the access-side controller imports its bounded JSON result transactionally.
No compute-node SSH or preinstalled `bourneprov` package is required, although
the compute allocation must provide Python 3 and visibility of the staging and
working directories.

Submission is not an experiment, scheduler completion is not scientific
success, and requested resources are not allocated resources. Bourne records
these as separate durable facts. Cancellation accepts a Bourne execution
reference—not an arbitrary scheduler job ID—and checks the submitting identity.
See [Workload planning and scheduler execution](docs/WORKLOAD_EXECUTION.md) for
the exact model, safety boundary, and current limitations.

## Compute-site discovery (v0.3.0)

Bourne can take an immutable, local snapshot of the execution surface visible
to your current identity:

~~~bash
bourne discover
bourne inventory
bourne inventory --find python
bourne inventory --json
~~~

Discovery covers the current identity and access target, allow-listed
user-relevant storage paths, direct execution contexts, generic PATH
executables, optional Conda/virtualenv/container/module contexts, safe system
capabilities, Bourne history, and read-only Slurm/PBS target-class summaries
when available. An unknown executable is recorded generically without being
run. Laptops, desktop and GPU workstations, DGX-class personal machines, shared
laboratory systems, and scheduler-backed HPC sites are all valid compute
sites. A scheduler-free machine is complete in its own right.

Discovery is observational: an executable is not verified workload
compatibility, a visible scheduler partition is not proof of submission
authorization, and a storage role hint is not a retention or backup policy.
Inventories remain local. Providers do not traverse other users' homes, crawl
shared storage, inspect SSH credentials or container secrets, dump arbitrary
environment variables, SSH into compute nodes, submit or cancel scheduler
jobs, or modify environments. See [Compute-site discovery](docs/COMPUTE_SITE_DISCOVERY.md)
for the exact topology, evidence, limits, and security semantics.

## Provenance, Artifacts and Lineage

Project Bourne v0.2 adds explicit input/output fingerprints, a minimal
derived_from relationship, safe execution-context observations, and artifact
tracing. Run the deterministic example from an isolated directory:

~~~bash
cp -R examples/provenance /tmp/bourne-provenance-demo
cd /tmp/bourne-provenance-demo
export BOURNE_DB="$PWD/bourne.sqlite3"

bourne run \
  --input config_A.json \
  --output result_A.csv \
  -- python demo_simulation.py config_A.json result_A.csv

bourne run \
  --derived-from @1 \
  --input config_B.json \
  --input result_A.csv \
  --output result_B.csv \
  -- python demo_simulation.py config_B.json result_B.csv

bourne show @2
bourne show @1
bourne trace result_B.csv
~~~

Inputs are fingerprinted before execution. Outputs are fingerprinted afterward,
including expected outputs that are missing after a failed or interrupted run.
SHA-256 reads are streamed in chunks; Bourne does not copy or upload declared
files.

A path is not artifact identity. Each capture has a stable ULID, while SHA-256
distinguishes content versions. When a historical path could identify several
versions and the current file content cannot disambiguate them, bourne trace
lists candidates and refuses to guess.

See [Artifacts, lineage, and execution context](https://github.com/KozakHou/project-bourne/blob/main/docs/ARTIFACTS_AND_LINEAGE.md)
for exact capture, trace, migration, and security semantics.

## Human-friendly experiment references

Canonical experiment identities remain 26-character ULIDs. Commands that
accept an experiment also understand:

~~~text
01M02GDJEW...   case-insensitive unique ULID prefix
latest          most recent experiment
@1              most recent experiment
@2              second-most-recent experiment
@3              third-most-recent experiment
~~~

For example:

~~~bash
bourne show latest
bourne show 01M02GDJEW
bourne compare @2 @1
bourne run --derived-from @1 -- ./solver case_B.yaml
~~~

Bourne never guesses when a prefix is ambiguous. bourne list displays a
10-character prefix by default; bourne list --full-id displays canonical IDs.

## Shell completion

Completion candidates include canonical experiment IDs, latest, and recent @N
references. Activate completion for the current shell session with:

~~~bash
# Bash
source <(bourne completion bash)

# Zsh
source <(bourne completion zsh)

# Fish
bourne completion fish | source
~~~

Completion for bourne show and bourne compare queries the currently configured
database, including BOURNE_DB.

## What Bourne records

Every experiment records:

- execution status (completed, failed, or interrupted), exact argument vector,
  working directory, UTC timestamps, duration, and exit code;
- live and captured stdout/stderr;
- Git repository root, commit, branch, and dirty state when available;
- operating system, architecture, hostname, CPU, and optional NVIDIA runtime
  metadata;
- requested and resolved executable paths plus strictly allow-listed
  virtualenv/Conda context hints;
- explicitly declared input/output artifact versions and immediate lineage.

Collectors degrade gracefully. Missing Git, NVIDIA tooling, GPUs, environment
hints, or executable resolution does not stop the workload. Arbitrary
environment variables are not persisted, so credentials and tokens are not
captured by default.

Failed and interrupted commands are saved before bourne returns their process
semantics:

~~~bash
bourne run --output expected.csv -- python -c "raise RuntimeError('boom')"
bourne show @1
~~~

On POSIX systems, Bourne uses a dedicated process group so Ctrl+C normally
terminates descendants without targeting unrelated processes.

Execution success is not verification, and deterministic artifact verification
is not general scientific validity. Bourne records these states separately.

## Local storage and migration

The default SQLite path is:

~~~text
~/.local/share/bourne/experiments.sqlite3
~~~

Use a project-specific database with:

~~~bash
export BOURNE_DB=/path/to/experiments.sqlite3
~~~

Opening a v0.1.1, v0.2.0, v0.3.0, or v0.4.0 database with this release
candidate performs deterministic transactional migrations through schema 5.
Existing experiments, artifacts, lineage, inventories, workloads, plans,
executions, scheduler jobs, allocations, events, and experiment links remain
readable. Migration does not invent `ExecutionRequest` history for v0.4
records. Unknown or newer schema versions fail explicitly; Bourne never resets
an existing database. Each new discovery creates a separate immutable
snapshot.

## License

Project Bourne v0.5.0 and later are distributed under the
[Apache License 2.0](LICENSE). Releases through v0.4.0 remain under the MIT
License terms under which they were released. See the
[licensing history](docs/LICENSING.md) for details.

## Release validation

The repository version is `0.6.0`. The base runtime has zero third-party
dependencies; MCP support remains an explicit optional extra.

Run the source-tree tests with:

~~~bash
PYTHONPATH=src python -W error::ResourceWarning -m unittest discover -s tests -v
~~~

stdout and stderr are still accumulated in memory before final persistence.
Disk-spooled experiment logs, automatic artifact discovery, artifact archival,
automatic scientific dependency installation, automatic module loading,
container orchestration, SSH execution, remote copying, utilization sampling,
profiling, arbitrary verification scripts, broad scientific-validity
inference, hosted HTTP MCP, embedded LLMs, and natural-language parsing remain
outside v0.6.0. See docs/VISION.md for the longer-term direction.
