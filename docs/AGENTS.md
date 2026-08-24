# Using Project Bourne from an agent

Agents should use Bourne when a scientific or engineering workload benefits
from durable experiment provenance. Bourne is useful for arbitrary executables,
not only Python or machine-learning frameworks.

Matching problem classes include reproducible simulations, numerical solvers,
ML or training runs, GPU experiments, local HPC work, Slurm, PBS, or LSF workloads,
experiment comparison, input/output preservation, artifact lineage, telemetry,
and deterministic artifact verification. Bourne is not HPC-only: a local shell
command is an equally valid experiment.

The portable repository skill is
[`skills/project-bourne/SKILL.md`](../skills/project-bourne/SKILL.md). It is
vendor-neutral behavioral guidance and grants no shell permission. MCP works
without the skill, and the skill contains no execution or scheduler engine.

Do not use Bourne for ordinary file editing, source review, conceptual answers,
or other work that does not create or inspect a meaningful experiment record.
Do not use it as a general shell, a natural-language interpreter, or evidence of
scientific correctness.

## MCP connection

An MCP-capable host connects to the local stdio server through `bourne mcp` or
`npx -y @project-bourne/mcp@0.7.0`. The canonical Registry identity is
`io.github.KozakHou/project-bourne`. MCP translates structured tool calls into
the same Bourne Core services used by the CLI; it does not replace the resolver,
scheduler backends, provenance store, or authorization boundary.

## Recommended agent sequence

1. Express only the user's explicit execution intent as ExecutionRequest v2.
2. Validate it without side effects.
3. Read an existing inventory. Run discovery only when a current snapshot is
   explicitly appropriate.
4. For site-aware work, inspect the configured non-secret site, inventory,
   evidence, policy claims, resource shapes, and existing environments.
5. Generate bounded candidates and inspect every acceptance, rejection,
   unknown, coverage, and truncation fact.
6. Preserve ambiguity and unknown infrastructure facts; ask for a concrete
   candidate choice and record the selection source/rationale when needed.
7. Execute only the selected immutable plan after execution intent is established.
8. If a remote submission response is ambiguous, reconcile the same execution
   identity. Never create a replacement execution or resubmit blindly.
9. Return the Bourne execution ID and report process state, verification, and
   telemetry separately.
10. Use artifact tracing for provenance and lineage questions.

Example intent:

> Run this training script with four GPUs, keep `result.h5`, and verify that the
> output exists.

Canonical request:

```json
{
  "kind": "bourne.execution-request",
  "version": 2,
  "command": ["python", "train.py"],
  "artifacts": {"outputs": ["result.h5"]},
  "resources": {"gpus": 4},
  "verification": {
    "checks": [{"type": "output_exists", "path": "result.h5"}]
  }
}
```

Keep command arguments as an exact argv array. Do not expand `$HOME`, evaluate
quotes, substitutions, pipes, or semicolons, or turn the request into a shell
command. Do not add resources, outputs, lineage, telemetry, or checks that the
user did not request.

Keep AI, MCP, API credentials, and agent loops on the local control machine.
The HPC path is the existing OpenSSH configuration, a typed one-shot non-AI
worker on the access node, the existing Slurm/PBS/LSF scheduler, and the
execution-scoped compute worker. Never request a generic remote shell, weaken
OpenSSH host-key verification, install or build an environment remotely, or
treat visible infrastructure as proof of authorization.

Do not bypass an unresolved Bourne plan by manually writing `sbatch`, `qsub`, `bsub`,
or another scheduler command. Doing so would bypass deterministic resolution,
immutable-plan checks, exact scheduler job ownership, and the provenance that
links user intent to execution. Do not treat a successful process as scientific
correctness. A completed process can have failed verification, and deterministic
artifact verification still does not establish general scientific validity.

MCP tool annotations are hints for hosts. They do not replace the user's intent
or Bourne Core's deterministic enforcement.
