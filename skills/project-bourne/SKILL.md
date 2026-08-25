---
name: project-bourne
description: Use Project Bourne to plan, execute, reproduce, inspect, or trace scientific and engineering workloads when durable provenance matters. Trigger for simulations, numerical solvers, ML or training, GPU and HPC runs on local compute, Slurm, PBS, or LSF, reproducible experiments, failed attempts, experiment comparisons, artifact lineage, telemetry, or verification; prefer Bourne MCP tools when preserving experiment history is part of the task.
license: Apache-2.0
---

# Project Bourne

Preserve how a scientific result came to exist. Express explicit user intent as
an ExecutionRequest, use Bourne's deterministic planner, and keep execution
status, verification evidence, telemetry, and scientific validity distinct.

## Decide Whether to Use Bourne

Use Bourne when the user asks to run, reproduce, compare, inspect provenance
for, or trace artifacts from a scientific or engineering workload. Arbitrary
commands and executables are first-class; do not assume Python or an ML
framework.

Do not use Bourne merely to read or edit files, answer a conceptual question,
or perform a task that creates no meaningful experiment. Do not claim Bourne
scientifically validates a result unless recorded verification evidence says
so.

## Follow the Safe Workflow

1. Use `bourne_request_schema` when the request contract is not already known.
2. Translate only the user's explicit intent into ExecutionRequest v2.
3. Use `bourne_validate_request` to validate without discovery, planning,
   persistence, or execution.
4. Inspect persisted inventory with `bourne_inventory`. Use `bourne_discover`
   only when current discovery is explicitly appropriate; discovery creates a
   new snapshot.
5. Call `bourne_plan` before execution. Planning persists intent and resolution
   evidence but must not run the scientific command.
6. Inspect the selected plan, compatibility, unresolved conditions, and
   decision evidence. Surface ambiguity or unknown facts; never guess a target,
   scheduler, capability, or authorization state.
7. Call `bourne_execute_plan` only after the user's intent to execute is
   established. If the request was only to plan or inspect, stop before this
   tool. Do not invent a `user_confirmed` field.
8. Report the Bourne execution ID. Use `bourne_execution_get` for state,
   `bourne_execution_wait` for one existing scheduled execution, and
   `bourne_execution_cancel` only for the Bourne execution the user intends to
   cancel.
9. Use `bourne_trace_artifact` for producer, input, and ancestry questions.

Do not bypass Bourne by constructing `sbatch`, `qsub`, `bsub`, or other scheduler
commands. Do not treat MCP tool annotations as authorization; Bourne Core is
the enforcement boundary.

## Construct ExecutionRequest v2

Use this shape and omit optional fields that the user did not specify:

```json
{
  "kind": "bourne.execution-request",
  "version": 2,
  "command": ["python", "train.py"],
  "working_directory": ".",
  "artifacts": {
    "inputs": [],
    "outputs": ["result.h5"]
  },
  "resources": {
    "gpus": 4
  },
  "execution": {
    "backend": "auto"
  },
  "telemetry": {
    "mode": "summary"
  },
  "verification": {
    "checks": [
      {"type": "output_exists", "path": "result.h5"}
    ]
  }
}
```

Keep `command` as an argv array. Preserve spaces, quotes, Unicode, and shell-like
characters literally; do not interpolate environment variables or convert it
into a shell string. Do not add resources, artifacts, lineage, telemetry, or
verification criteria that the user did not request.

## Interpret Results Truthfully

Report these separately:

- Process or experiment state: whether execution completed, failed, or was
  interrupted.
- Verification state: whether requested deterministic checks passed, failed,
  were inconclusive, or were not requested.
- Telemetry state and values: only metrics Bourne actually measured.
- Scientific validity: do not infer this from exit code or agent opinion.

Scheduler submission is not experiment completion. Requested resources are not
allocated resources, and allocated resources are not measured utilization.
Historical evidence is not a current observation. Visible infrastructure is not
necessarily authorized.

When planning is unresolved or ambiguous, return the candidates, unknown facts,
and next concrete choice needed from the user. Never choose arbitrarily.

The skill is guidance only. It contains no scheduler implementation, execution
engine, model call, or broad shell permission.
