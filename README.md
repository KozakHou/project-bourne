# Project Bourne

> **Every experiment has a history.**

> **Universal experiment provenance and reproducibility for science and engineering.**

Project Bourne records how scientific and engineering commands were executed:
the exact argument vector, process output, timing, exit status, Git state, and
the system that ran them. It is local-first, framework-agnostic, and requires no
changes to the program being recorded.

Project Bourne v0.1.1 is distributed through GitHub Releases and is not yet
published on PyPI. Download `bourneprov-0.1.1-py3-none-any.whl` from the
[v0.1.1 release](https://github.com/KozakHou/project-bourne/releases/tag/v0.1.1),
then install that downloaded file:

```bash
python -m pip install ./bourneprov-0.1.1-py3-none-any.whl

bourne run python examples/demo.py
bourne list
bourne show @1
```

The source distribution, `bourneprov-0.1.1.tar.gz`, is provided on the same
release for users who prefer to build locally:

```bash
python -m pip install ./bourneprov-0.1.1.tar.gz
```

Run any executable, then compare the two most recent experiments without
copying their IDs:

```bash
bourne run bash -c "echo hello"
bourne compare @2 @1
```

The program inside an experiment does not need to be Python. The same wrapper
works for commands such as:

```bash
bourne run ./solver case.yaml
bourne run julia simulation.jl
bourne run mpirun -np 64 ./solver
```

Program stdout and stderr remain visible while the command runs and are also
preserved in the experiment record.

## Human-friendly experiment references

Every experiment keeps its canonical 26-character ULID in storage. CLI commands
that accept an experiment also understand convenience references:

```text
01M02GDJEW...   unique ULID prefix
latest          most recent experiment
@1              most recent experiment
@2              second-most-recent experiment
@3              third-most-recent experiment
```

For example:

```bash
bourne show latest
bourne show 01M02GDJEW
bourne compare @2 @1
```

Bourne never guesses when a prefix is ambiguous; it lists the matching
canonical IDs and asks for a longer prefix. `bourne list` displays a concise
10-character prefix by default. Use `bourne list --full-id` when canonical IDs
are needed.

## Shell completion

Completion candidates include canonical experiment IDs, `latest`, and recent
`@N` references. Activate completion for the current shell session with:

```bash
# Bash
source <(bourne completion bash)

# Zsh
source <(bourne completion zsh)

# Fish
bourne completion fish | source
```

Add the appropriate command to your shell startup file to enable it
permanently. Completion for `bourne show` and `bourne compare` queries the
currently configured database, including an exported `BOURNE_DB`.

## What v0.1.1 records

Each experiment has a public, time-sortable ULID and records:

- execution status (`completed`, `failed`, or `interrupted`), command and
  arguments, working directory, UTC timestamps, duration, and exit code;
- captured stdout and stderr;
- Git repository root, commit, branch, and dirty state when available;
- operating system, version, architecture, hostname, CPU, and optional NVIDIA
  GPU, active driver, and driver-supported CUDA metadata.

Collectors degrade gracefully. A missing Git repository, `nvidia-smi`, NVIDIA
GPU, or CUDA installation does not stop the experiment.

Failed and interrupted commands are recorded before `bourne` returns their
process semantics:

```bash
bourne run python -c "raise RuntimeError('boom')"
bourne list
```

On POSIX systems, Bourne runs an experiment in its own process group so Ctrl+C
normally terminates its descendants without targeting unrelated processes.

Process execution success is not a claim of scientific correctness. Scientific
verification is deliberately separate and is not implemented in v0.1.1.

## Local storage

Bourne stores records in a local SQLite database. The default path is:

```text
~/.local/share/bourne/experiments.sqlite3
```

Set `BOURNE_DB` to use a different database, which is useful for isolated
projects and tests:

```bash
BOURNE_DB=/path/to/experiments.sqlite3 bourne list
```

On Windows, the default uses `%LOCALAPPDATA%\Bourne\experiments.sqlite3` when
`LOCALAPPDATA` is set. If `XDG_DATA_HOME` is set, Bourne respects it on other
platforms.

## Development

The v0.1.1 runtime uses only the Python standard library. Run the tests with:

```bash
python -W error::ResourceWarning -m unittest discover -s tests -v
```

The first release intentionally does not include artifact tracking, lineage,
scientific verification, remote execution, schedulers, cloud services,
dashboards, or autonomous agents. See `docs/VISION.md` for the longer-term
direction.
