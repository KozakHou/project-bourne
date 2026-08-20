# Project Bourne v0.5.0 — Unified Execution Requests, Telemetry, and Verification

Status: development candidate (`0.5.0.dev0`), not released.

Project Bourne v0.5 adds a versioned, machine-readable user-intent boundary to
the v0.4 workload and execution architecture.

Highlights:

- immutable `ExecutionRequest` identities distinct from workloads, plans,
  attempts, scheduler facts, and experiments;
- JSON request version 1 and packaged JSON Schema, with a bounded stdlib-only
  parser that rejects unknown/duplicate fields and executes no user code;
- `bourne request init`, `validate`, `show`, and `schema`;
- `bourne plan --request bourne.json` and
  `bourne execute --request bourne.json`;
- one common request pipeline for CLI flags, files, and Python callers;
- low-overhead summary telemetry based only on existing Bourne evidence;
- deterministic `output_exists`, `output_min_bytes`, and `output_sha256`
  verification from captured Artifact records;
- worker/staged-plan protocol 2 with safe read compatibility for released
  v0.4 protocol 1;
- transactional SQLite schema-5 persistence and schema 1–4 migration without
  invented historical intent;
- zero runtime dependencies.

This milestone does not add an LLM, MCP server, TypeScript/npm package, Skill,
Rust runtime, high-frequency sampling, arbitrary verification code, retries,
remote execution, containers, or a web interface.
