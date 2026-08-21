# Using Project Bourne from an agent

Agents should use Bourne when a scientific or engineering workload benefits
from durable experiment provenance. Bourne is useful for arbitrary executables,
not only Python or machine-learning frameworks.

Matching problem classes include reproducible simulations, numerical solvers,
ML or training runs, GPU experiments, local HPC work, Slurm or PBS workloads,
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

An MCP-capable host connects to the local stdio server through `bourne mcp` or,
after v0.6 publication, `npx -y @project-bourne/mcp`. The canonical Registry
identity is `io.github.KozakHou/project-bourne`. MCP translates structured tool
calls into the same Bourne Core services used by the CLI; it does not replace
the resolver, scheduler backends, provenance store, or authorization boundary.

## Recommended agent sequence

1. Express only the user's explicit execution intent as ExecutionRequest v1.
2. Validate it without side effects.
3. Read an existing inventory. Run discovery only when a current snapshot is
   explicitly appropriate.
4. Plan and inspect all compatibility evidence.
5. Preserve ambiguity and unknown infrastructure facts; ask for a concrete
   choice when needed.
6. Execute only after execution intent is established.
7. Return the Bourne execution ID and report process state, verification, and
   telemetry separately.
8. Use artifact tracing for provenance and lineage questions.

Example intent:

> Run this training script with four GPUs, keep `result.h5`, and verify that the
> output exists.

Canonical request:

```json
{
  "kind": "bourne.execution-request",
  "version": 1,
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

Do not bypass an unresolved Bourne plan by manually writing `sbatch`, `qsub`,
or another scheduler command. Doing so would bypass deterministic resolution,
immutable-plan checks, exact scheduler job ownership, and the provenance that
links user intent to execution. Do not treat a successful process as scientific
correctness. A completed process can have failed verification, and deterministic
artifact verification still does not establish general scientific validity.

MCP tool annotations are hints for hosts. They do not replace the user's intent
or Bourne Core's deterministic enforcement.
