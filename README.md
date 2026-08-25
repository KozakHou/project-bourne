# Project Bourne

Project Bourne is open-source execution and provenance infrastructure for
reproducible scientific and engineering workloads.

It answers: **Exactly how did this scientific result come to exist?**

## Keep AI off the cluster

~~~text
Researcher's workstation
  Linux / macOS
        │
  AI / Agent (optional)
        │ local stdio MCP
        ▼
  Bourne Control Plane
        │
        ├─ freezes immutable ExecutionPlan
        ├─ builds/stages versioned Bourne workers
        └─ uses existing VPN / OpenSSH
        ▼

HPC login / access node
  one-shot Bourne Remote Worker
        │
        ├─ validates the plan
        ├─ verifies staged file digests
        ├─ stages the execution bundle
        └─ submits with sbatch / qsub / bsub
        ▼

Slurm / PBS / IBM LSF
        │
        │ allocates resources
        ▼

Compute allocation
  execution-scoped Bourne Compute Worker
        │
        ├─ reads immutable ExecutionPlan
        ├─ observes actual allocation
        ├─ reproduces selected environment
        ├─ performs compute-side preflight
        ├─ executes exact scientific argv
        └─ writes durable result evidence
        ▼

Scientific workload

Later:

Researcher's workstation
        │
        │ existing SSH
        ▼
Remote Worker: reconcile
        │
        ├─ exact Bourne-owned scheduler job state
        └─ bounded result evidence
        ▼
Local Bourne provenance database
~~~

The Remote Worker and Compute Worker are not agents or persistent services;
both are short-lived, versioned Bourne workers. Bourne does not SSH directly
into compute nodes. Slurm/PBS/LSF places the Compute Worker inside the allocation
and owns job lifetime after accepting the submission. The researcher's
workstation / control plane may disconnect and reconcile the same execution
later.

The HPC path requires no AI, MCP server, AI credential, inbound port, root
access, persistent daemon, or public-internet access on the cluster. It uses
the researcher's existing OpenSSH configuration and scheduler access. Agents
receive typed Bourne operations—not an unrestricted remote shell.

The Bourne control plane is supported and tested on Linux and macOS. Native
Windows is not yet validated or supported.

Bourne remains agent-native, not agent-dependent. The CLI and Python services
work without an agent or MCP.

## Quick Start

### Human

Install Project Bourne v0.8.0 from PyPI:

~~~bash
python -m pip install "bourneprov==0.8.0"

bourne run python examples/demo.py
bourne list
bourne show @1

# Or execute an ExecutionRequest v2 document:
bourne execute --request bourne.json
~~~

Configure a site-aware SSH workflow with the installed CLI:

~~~bash
bourne site add imperial \
  --ssh login.example.edu \
  --scheduler slurm \
  --local-root "$PWD" \
  --remote-root /work/$USER/project

bourne discover --site imperial
bourne plan --site imperial --request bourne.json --provider constraints.json
~~~

The first plan call prints bounded candidates. A human or agent then makes the
preference decision explicitly:

~~~bash
bourne plan --site imperial --request bourne.json \
  --provider constraints.json \
  --trust-provider-classifications \
  --candidate sha256:...

bourne execute --plan <plan-id>
bourne execution wait <execution-id>
~~~

The trust flag is an explicit review decision for semantic classifications in
that declarative provider; the provider cannot grant itself that authority.
Use `--approve-variant-change PARAMETER` or
`--declare-execution-only PARAMETER` for narrower user decisions. If the
selected candidate changes a provider-bound JSON input, Bourne preserves the
original and automatically binds a separately hashed `WorkloadVariant` to the
plan.

Slurm/PBS/LSF owns the job after acceptance. The researcher's workstation /
control plane, VPN, SSH connection, MCP host, and agent may disconnect; Bourne
reconnects later and reconciles the exact execution. An ambiguous connection
failure never triggers blind resubmission.

### Agent / MCP

The v0.8.0 agent and MCP entrypoints remain local stdio:

~~~bash
python -m pip install "bourneprov[mcp]==0.8.0"
bourne mcp

# Or use the public transparent launcher:
npx -y @project-bourne/mcp@0.8.0
~~~

## Development

Project Bourne uses `uv` as its development, dependency-locking, test, and
build frontend. After installing uv, synchronize the committed lockfile and run
the suite with:

~~~bash
uv sync --locked --all-extras --dev
uv run --frozen --no-sync python -W error::ResourceWarning -m unittest discover -s tests -v
uv build --no-sources
~~~

CI uses locked/frozen variants of these commands so an out-of-date `uv.lock`
fails instead of drifting. uv is development tooling only: it is not a
`bourneprov` runtime dependency, is not required for `pip install`, is not
used by the npm launcher, and is never required on HPC login or compute nodes.
See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the complete contributor workflow.

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

## Runtime truth in v0.8

v0.8 keeps planning truth, scheduler truth, runtime truth, experiment truth,
verification, and scientific validity separate. The execution-scoped Compute
Worker records versioned process, allocation, CPU, memory, I/O, GPU, and
environment evidence with explicit `observed`, `partially_observed`,
`unavailable`, `unsupported`, or `unknown` coverage. Missing telemetry does not
fail a valid workload and never becomes a fabricated zero.

IBM LSF joins Slurm and PBS with bounded queue discovery, `bsub`, exact-job
active `bjobs`, recent-finished `bjobs -a`, durable `bhist` reconciliation,
and `bkill`. Existing
Apptainer/Singularity images can be frozen into a selected site-aware plan;
Bourne verifies the existing runtime/image on the compute side and passes the
scientific command as exact argv. It does not build, pull, install, or manage
images. v0.8 does not orchestrate multi-node container launch, choose
MPI-launcher/container ordering, or inject an MPI launcher. See
[runtime evidence and scheduler coverage](docs/RUNTIME_EVIDENCE.md).

## Core architecture

Bourne Core owns deterministic execution, evidence, planning, storage, and
provenance. CLI, SDK, and MCP are adapters over the same services:

~~~text
             Project Bourne Core
                    │
       ┌────────────┼────────────┐
       │            │            │
      CLI          SDK          MCP
    humans                     agents
~~~

The remote worker is one-shot, user-space, non-AI, and non-daemon. It accepts
only versioned operations for discovery, plan validation, staging, scheduler
submission, and reconciliation. Scientific commands remain exact argv in an
immutable plan; no scientific argv is interpolated into remote shell text.
The remote-worker protocol remains v1. v0.8 adds worker-result protocol v3 and
staged-plan protocol v4 while retaining readers for released worker-result
v1/v2 and staged-plan v1/v2/v3 payloads.

## Agent and MCP Integration

The canonical local stdio server is `bourne mcp`. The stable official MCP
Registry identity is `io.github.KozakHou/project-bourne`, and the portable
Agent Skill is at [`skills/project-bourne`](skills/project-bourne). The v0.8.0
npm package and matching Registry metadata use the same release identity.

An MCP-compatible agent can translate an explicit request such as “Run this
simulation using four GPUs and preserve provenance” into ExecutionRequest v2,
ask Bourne to plan it, show the deterministic resolution, and execute the
immutable plan after execution intent is established. Bourne itself does not
interpret unconstrained natural language and does not call another model.

The agent path is deliberately two-phase:

~~~text
agent intent → ExecutionRequest v2 → bourne_plan → inspect → bourne_execute_plan
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
  "version": 2,
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
bourne request schema > execution-request-v2.schema.json
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
supports `--backend pbs` and `--backend lsf`.

Direct execution reuses Bourne's existing live-output, process-group, artifact,
lineage, and experiment-provenance machinery. Slurm, PBS, and LSF plans use a
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
capabilities, Bourne history, and read-only Slurm/PBS/LSF target-class summaries
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

Opening an older Bourne database, including released v0.1.1 through v0.7.0
databases, with v0.8.0 performs deterministic transactional migrations
through schema 7.
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

The release version is `0.8.0`. The base runtime has zero third-party
dependencies; MCP support remains an explicit optional extra.

Run the source-tree tests with:

~~~bash
uv sync --locked --all-extras --dev
uv run --frozen --no-sync python -W error::ResourceWarning -m unittest discover -s tests -v
uv build --no-sources
~~~

Compute-worker stdout and stderr remain live and are bounded to 8 MiB per
captured stream in the result bundle; truncation is explicit runtime evidence.
Ordinary local `bourne run` retains its existing capture behavior. Disk-spooled
logs, automatic artifact discovery/archival, scientific dependency
installation or source builds, generic data synchronization, unrestricted
remote shell, scheduler-free disconnect-safe remote supervision, whole-allocation distributed
telemetry, queue/performance prediction, arbitrary verification scripts,
hosted HTTP MCP, embedded LLMs, and broad scientific-validity inference remain
outside v0.8. See [runtime evidence and scheduler coverage](docs/RUNTIME_EVIDENCE.md),
[the site-aware architecture](docs/SITE_AWARE_PLANNING.md), and [VISION](docs/VISION.md).

Live LSF and live Apptainer validation have not yet been performed. Runtime
sampling is execution-scoped and does not automatically aggregate across a
multi-node allocation. Bourne does not inject MPI launchers or automatically
choose container/MPI ordering. Portable LSF memory/GPU resource syntax remains
site-specific and unresolved.
