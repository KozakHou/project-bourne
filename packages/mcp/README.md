# `@project-bourne/mcp`

This zero-runtime-dependency package launches the canonical Project Bourne MCP
server:

```bash
npx -y @project-bourne/mcp
```

It requires Node.js 24 or newer. It first looks for Python 3.10+ with the exact
compatible `bourneprov` version and MCP extra. If unavailable, it may install
that exact runtime into a private, versioned user cache. It never installs into
the active project, virtual environment, Conda environment, or system Python.

Use `--no-bootstrap` to require an existing compatible runtime, or `--doctor`
to print compatibility diagnostics without bootstrapping or launching MCP.

The package only locates and launches `python -m bourneprov mcp`. Planning,
execution, schedulers, provenance, telemetry, and verification remain in the
Python Bourne core.
