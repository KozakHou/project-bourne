# Project Bourne v0.6.0 — MCP and Agent Interface

Project Bourne v0.6 adds an optional, vendor-neutral agent interface while
preserving the v0.5 deterministic execution and provenance model.

## Highlights

- `bourne mcp` serves ten structured tools over local stdio using the official
  MCP Python SDK and current `2026-07-28` protocol.
- `bourneprov` keeps zero base runtime dependencies; MCP is installed with the
  `bourneprov[mcp]` extra.
- Agents validate ExecutionRequest v1, inspect inventory, plan, and then execute
  an existing immutable plan as separate actions.
- Product errors are machine-readable, uncertainty is preserved, and process
  status, verification, telemetry, and scientific validity remain separate.
- `@project-bourne/mcp` is a zero-runtime-dependency Node.js 22+ launcher with
  exact Python-version coupling, `npx` support, diagnostics, and optional
  isolated bootstrap that does not modify scientific environments.
- Official MCP Registry metadata uses the stable identity
  `io.github.KozakHou/project-bourne`; it is published only after the matching
  final npm package exists.
- `skills/project-bourne/SKILL.md` provides portable, vendor-neutral behavioral
  guidance without granting broad shell permission.
- Python, npm, Registry, and Skill discovery metadata describe reproducible
  scientific execution, provenance, HPC, and verification intent.
- Python and npm distributions remain under Apache-2.0.

## Compatibility

- SQLite schema remains version 5.
- ExecutionRequest remains `bourne.execution-request` version 1.
- Staged-plan and worker-result protocol version 2 remain current; version 1
  remains readable.
- Existing CLI commands and v0.5 request files remain supported.
- The MCP adapter and Node launcher target Linux and macOS. This release does
  not claim complete Windows scientific process-tree semantics.

The final coupled release versions are `bourneprov 0.6.0` and
`@project-bourne/mcp 0.6.0`.

## Limitations

- MCP uses local stdio only; v0.6.0 has no production HTTP or hosted MCP
  transport.
- Bourne contains no embedded LLM or natural-language parser.
- Deterministic verification evidence remains distinct from general scientific
  validity.
- The MCP adapter and Node launcher target Linux and macOS. Windows does not
  have complete scientific process-tree support in this release.
- When no compatible runtime is installed, npm launcher bootstrap requires
  network access to install the exact matching `bourneprov[mcp]` version into
  its private cache.

No v0.6.0 artifact is published during release-candidate preparation.
