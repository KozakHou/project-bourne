# Project Bourne v0.8.0 — Runtime Evidence and Scheduler Coverage

v0.8.0 is currently a development release (`0.8.0.dev0`) and is not published.

The release adds first-class IBM LSF discovery and lifecycle support, bounded
execution-scoped runtime evidence, more precise partial/failure semantics, and
optional execution through an already-existing Apptainer/Singularity image.
It preserves the v0.7 control-plane/one-shot-worker architecture and exact argv
boundary.

## Architecture delta from v0.7

- `ExecutionRequest v2` adds explicit `lsf`; released v1 remains readable.
- `SQLite schema 7` persists one versioned runtime-evidence record and
  termination evidence atomically with an execution result.
- `worker-result v3` carries runtime and termination evidence; v1/v2 readers
  remain intact.
- `staged-plan v4` carries current ExecutionRequest v2 payloads and optional
  immutable existing-image execution; v1/v2/v3 are still read without
  reinterpretation.
- `remote-worker v1` is unchanged.

LSF uses bounded queue discovery, stdin `bsub` submission, exact-ID `bjobs`
active/historical reconciliation, and exact-job `bkill`. Resource mapping is
limited to portable Bourne-owned concepts. Site-specific memory/GPU syntax and
generic LSF expression building are intentionally excluded.

Linux process-tree sampling is local to the execution worker and records
coverage explicitly. macOS and unavailable metrics remain truthful rather
than becoming zeros. GPU visibility is not reported as utilization or proven
scheduler allocation.

Apptainer/Singularity support uses existing local images only. There is no
build, pull, install, registry, Docker-daemon, Kubernetes, or shell-string
execution path.

## Intentionally deferred

This development milestone adds no public demo or marketing asset, Docker
requirement, automatic dependency installation/build, cross-cluster placement,
bulk data synchronization, monitoring daemon, distributed telemetry service,
Rust component, or v0.9 work.

See [Runtime evidence and scheduler coverage](RUNTIME_EVIDENCE.md) for evidence
semantics, protocol details, and bounded limitations.
