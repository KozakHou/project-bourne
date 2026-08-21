# `@project-bourne/mcp`

Plan and execute reproducible scientific and engineering workloads across local
compute, GPUs, Slurm, and PBS with preserved experiment provenance, artifacts,
lineage, telemetry, and verification. This zero-runtime-dependency package
launches the canonical local Project Bourne MCP server:

```bash
npx -y @project-bourne/mcp
```

It requires Node.js 22 or newer. It first looks for Python 3.10+ with the exact
compatible `bourneprov` version and MCP extra. If unavailable, it may install
that exact runtime into a private, versioned user cache. It never installs into
the active project, virtual environment, Conda environment, or system Python.

Use `--no-bootstrap` to require an existing compatible runtime, or `--doctor`
to print compatibility diagnostics without bootstrapping or launching MCP.

The package only locates and launches `python -m bourneprov mcp`. Planning,
execution, schedulers, provenance, telemetry, and verification remain in the
Python Bourne core.

The canonical official MCP Registry identity is
`io.github.KozakHou/project-bourne`. Registry publication follows the matching
final npm release; the release candidate is not published to the Registry.
