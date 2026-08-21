# Project Bourne v0.6.0

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
  exact Python-version coupling, diagnostics, and optional isolated bootstrap.
- Official MCP Registry metadata uses the stable identity
  `io.github.KozakHou/project-bourne`; it is published only after the matching
  final npm package exists.
- `skills/project-bourne/SKILL.md` provides portable, vendor-neutral behavioral
  guidance without granting broad shell permission.

## Compatibility

- SQLite schema remains version 5.
- ExecutionRequest remains `bourne.execution-request` version 1.
- Staged-plan and worker-result protocol version 2 remain current; version 1
  remains readable.
- Existing CLI commands and v0.5 request files remain supported.
- The MCP adapter and Node launcher target Linux and macOS. This release does
  not claim complete Windows scientific process-tree semantics.

The v0.6 development versions are `bourneprov 0.6.0.dev0` and
`@project-bourne/mcp 0.6.0-dev.0`. No v0.6 artifact is published during the
review stage.
