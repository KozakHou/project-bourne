# Contributing to Project Bourne

Project Bourne uses [uv](https://docs.astral.sh/uv/) as the canonical Python
development, dependency-locking, test, and build frontend. The package itself
continues to use setuptools and has no base runtime dependencies.

## Set up the development environment

Install uv on the contributor workstation, clone the repository, then run:

```bash
uv sync --locked --all-extras --dev
```

This creates the local environment and installs the project, the optional MCP
adapter, and development-only validation tools from `uv.lock`. After changing
Python dependency metadata, regenerate and commit the lockfile with:

```bash
uv lock
uv sync --locked --all-extras --dev
```

Do not hand-edit `uv.lock`.

## Run validation

```bash
uv run --frozen --no-sync python -m unittest discover -s tests -v
uv run --frozen --no-sync python -W error::ResourceWarning -m unittest discover -s tests -v
uv build --no-sources
uv run twine check dist/*
```

Use `uv run --frozen --no-sync bourne ...` for CLI dogfooding from a checkout. CI first performs
`uv sync --locked`; its commands use frozen/no-sync semantics so dependency
metadata and `uv.lock` cannot drift silently.

## Runtime boundary

uv is workstation and CI tooling, not part of Bourne's runtime architecture.
It must not become:

- a `bourneprov` runtime dependency;
- a prerequisite for users installing with pip;
- a prerequisite on HPC login or compute nodes;
- a dependency of the staged Bourne worker; or
- a preinstalled requirement of the npm MCP launcher.

Remote Bourne workers remain dependency-free, exact-version user-space
artifacts. The v0.7 package remains compatible with Python 3.10 and newer.
