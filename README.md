# Project Bourne

> **Every experiment has a history.**

> **Universal experiment provenance and reproducibility for science and engineering.**

Project Bourne records how arbitrary scientific and engineering commands were
executed, which files were explicitly used and produced, and how one experiment
derived from another. It is local-first, framework-agnostic, and requires no
changes to the program being recorded.

The latest public release is Project Bourne v0.2.0. Install it from PyPI; the
current repository is developing v0.3.0 and can briefly be ahead of the
published package:

~~~bash
python -m pip install bourneprov

bourne run python examples/demo.py
bourne list
bourne show @1
~~~

Bourne wraps any executable, not only Python:

~~~bash
bourne run bash -c "echo hello"
bourne run ./solver case.yaml
bourne run julia simulation.jl
bourne run mpirun -np 64 ./solver
~~~

Program stdout and stderr remain visible during execution and are preserved in
the experiment record.

## Compute-site discovery (v0.3 development)

Bourne can take an immutable, local snapshot of the execution surface visible
to your current identity. Install this repository checkout (`python -m pip
install .`) to try the development command set:

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
run. A laptop without a scheduler is a complete and normal compute site.

Discovery is observational: an executable is not verified workload
compatibility, a visible scheduler partition is not proof of submission
authorization, and a storage role hint is not a retention or backup policy.
Inventories remain local and no provider scans other users, compute nodes, or
container contents. See [Compute-site discovery](docs/COMPUTE_SITE_DISCOVERY.md)
for the exact topology, evidence, limits, and security semantics.

## Artifacts and lineage

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

Execution success is not scientific verification. Verification remains a
separate future capability.

## Local storage and migration

The default SQLite path is:

~~~text
~/.local/share/bourne/experiments.sqlite3
~~~

Use a project-specific database with:

~~~bash
export BOURNE_DB=/path/to/experiments.sqlite3
~~~

Opening a v0.1.1 or v0.2.0 database performs deterministic transactional
migrations through schema 3. Existing completed, failed, and interrupted
experiments, artifacts, lineage, and execution-context observations remain
readable. Unknown or newer schema versions fail explicitly; Bourne never
resets an existing database. Each new discovery creates a separate immutable
snapshot.

## Development

The repository development version is 0.3.0.dev0. PyPI publication is a
separate release step. The runtime has zero third-party dependencies.

Run the source-tree tests with:

~~~bash
PYTHONPATH=src python -W error::ResourceWarning -m unittest discover -s tests -v
~~~

stdout and stderr are still accumulated in memory before final persistence.
Disk-spooled experiment logs, automatic artifact discovery, artifact archival,
workload inference, environment resolution/selection, scheduler submission,
remote execution, profiling, scientific verification, MCP, and agents remain
future work. See docs/VISION.md for the longer-term direction.
