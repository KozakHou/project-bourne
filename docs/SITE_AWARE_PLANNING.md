# Site-Aware Constraint-Based Execution Planning

Project Bourne v0.7 adds a bounded path from a local control plane to an
existing SSH-accessible Slurm/PBS site. It does not turn Bourne into a fleet
manager or remote shell.

## Deployment roles

~~~text
Researcher's computer
  CLI / Python SDK / optional local stdio MCP
  Bourne Control Plane
      ↓ existing OpenSSH access

HPC login/access node
  one-shot Bourne Remote Worker (non-AI, user-space, no daemon)
      ↓ sbatch or qsub

Compute allocation
  execution-scoped Bourne Compute Worker
      ↓ exact positional argv, no implicit scientific shell
  scientific workload
~~~

The control plane owns the local database, site configuration, planning,
candidate exploration, selection provenance, execution history, and later
reconciliation. The remote worker accepts only typed protocol-v1 operations:
`hello`, `discover`, `validate_plan`, `prepare`, `submit`, `reconcile`,
`collect`, and `cancel`. The compute worker remains the scheduler-staged,
stdlib-only worker introduced for local Slurm/PBS execution.

There is no remote `exec`, arbitrary command, scheduler-command, MCP, HTTP, or
daemon operation.

## SSH trust and bootstrap

A `Site` is either `local` or `remote_ssh`. It may retain a name, SSH host or
alias, optional username/port, scheduler hint, project-directory mapping, and
an optional pre-existing worker path. It cannot retain a password, private key,
token, or host-key bypass.

The OpenSSH adapter invokes local `ssh` and `scp` using exact argv with
`shell=False`. It deliberately does not set `StrictHostKeyChecking`, replace
the known-hosts file, select private key material, or implement authentication.
OpenSSH configuration, agent/key access, prompts, VPN, MFA, host trust, and
authorization remain the user's/site's responsibility.

The login-node worker can be:

1. an explicitly configured compatible worker, or
2. an exact-version Bourne zipapp copied into a user-owned cache through the
   existing SSH connection.

The second path verifies the zipapp SHA-256 digest and Bourne version. It uses
no root, remote internet, package manager, compiler, background process, or
third-party scientific dependency. A mismatch is `remote_worker=unavailable`.

## Site-aware observations

`bourne discover --site NAME` executes bounded discovery where the facts are
true. Remote inventory metadata records `remote_ssh_login_access_node` and
explicitly says compute-allocation facts were not observed. A login-node CPU,
GPU, executable, module, or environment fact is never promoted into a
compute-allocation fact. Compute-side preflight remains authoritative for the
allocation.

Discovery still uses allowlists, command time/output bounds, PATH-directory and
entry bounds, and exact user-visible scheduler surfaces. It does not enumerate
other users, crawl shared filesystems, read SSH credentials/secret stores, dump
arbitrary environment variables, or download datasets.

Visibility and authorization are distinct. A visible resource shape with
unknown authorization produces an unresolved candidate, not a compatible one.

## Evidence and policy

Planning evidence uses these semantics:

- `observed_now`: bounded observation in its recorded context;
- `site_declared`: a structured claim from site policy;
- `user_declared`: explicit user input;
- `historical`: a prior fact that is not current availability;
- `inferred`: a stated derivation, not an observation;
- `unknown`: unavailable or unresolved truth.

`SitePolicyClaim` records subject, property, structured value, evidence kind,
interpretation status, source identity, and optional identifier/URL,
retrieval/document time, and content digest. Bourne does not crawl policy
sites. Humans, agents, or future integrations provide claims.

Only a clear `hard_constraint` backed by `site_declared` or `user_declared`
evidence can hard-reject. Advisory language remains advisory. If two credible
hard claims conflict, both remain stored. A shape satisfying every current
interpretation may remain viable; one depending on the permissive
interpretation is rejected from automatic selection. Bourne reports the true
maximum as unknown/conflicted—it does not rewrite it to the conservative
value.

## Resource shapes and bounded solving

`ResourceShape` keeps nodes, CPUs/node, total CPUs, MPI ranks, ranks/node,
threads/rank, GPUs, GPUs/node, memory and memory/node, architecture/node class,
walltime, scheduler queue/partition/class, placement metadata, and evidence.
Two shapes with the same total CPU count can therefore remain distinct.

The declarative resolver intersects:

~~~text
workload/provider constraints
∩ compatible existing environment
∩ hardware/scheduler capability
∩ site policy
∩ authorization evidence
∩ explicit user constraints
~~~

Known hard incompatibilities are rejected. Unknown facts remain unresolved.
Candidate enumeration is deterministic and capped at 64 descriptions. The
result records theoretical/materialized counts, truncation, and coverage; a
truncated set is never described as exhaustive.

Bourne owns feasibility, evidence, and validation. A human or agent owns the
objective and final choice among viable candidates. Selection rationale stays
rationale and never becomes observed evidence. Bourne has no built-in
"balanced" preference and no queue/performance/history model.

Candidate exploration is ephemeral. Bourne persists one bounded
`CandidateSelectionSummary`, then materializes only the selected
`WorkloadVariant`/effective request and immutable `ExecutionPlan`. Rejected
candidates are not requests, executions, or experiments.

## Declarative and trusted providers

The bundled `bourne.constraint-provider` schema version 1 is JSON and uses a
small typed AST. It supports bounded/discrete parameters, semantic classes,
resource/parameter/constants, add/multiply/subtract expressions, equality and
inequality, divisibility, environment/launcher requirements, and safe JSON
path bindings. Core performs full deterministic validation without `eval`,
`exec`, expression strings, template execution, or shell interpolation.

See [`examples/providers/generic-mpi-decomposition.json`](../examples/providers/generic-mpi-decomposition.json).

The public Python `TrustedCodeProvider` protocol is for applications whose
inputs need trusted code. Providers are not sandboxed. Discovery is limited to
installed `bourneprov.constraint_providers.v1` entry points and an explicit
enable list. Project Python files are never auto-imported. Planning providers
must not install/build software, submit work, execute the scientific program,
modify source inputs, or silently use the network.

Declarative JSON can travel as structured data. Required trusted code must
already be explicitly installed and authorized at the remote context. If it is
not, the provider is unavailable; Bourne does not install it or substitute an
agent guess.

## Workload variants and parameter safety

Planning treats original scientific input as immutable. The safe built-in
materializer reads one explicitly declared bounded JSON input and writes each
derived input under a separate Bourne staging identity. It records original
and derived SHA-256 hashes, changed fields, proposer, unchanged semantic
classifications, supporting contract evidence, and change-specific approval.
Multiple variants do not share a derived file.

Parameter rules are:

- `execution_only`: automatic only with machine/provider contract evidence or
  an explicit user declaration;
- `performance_tunable`: automatic only with contract evidence of the relevant
  scientific equivalence;
- `scientific_semantics`: a specific change requires user approval;
- `unknown`: a specific change requires user approval and remains classified
  unknown afterward.

An agent assertion alone does not establish classification or permission.

## Existing environments only

Plans may select a currently observed module/Lmod, virtualenv, Conda, or Spack
environment and record a typed `EnvironmentActivation`. Activation affects
only the compute worker's child environment; it does not mutate the interactive
shell. Virtualenv/Conda/Spack prefixes are absolute typed paths. Module names
are validated and passed to a fixed Bourne-owned shell template; scientific
argv is never part of that template.

The compute worker revalidates activation and executable availability. If the
planned environment cannot be reproduced, the result is `preflight_failed`,
the experiment is `not_started`, and scientific argv is never launched.

There is deliberately no `pip/conda/mamba/spack/apt/yum/dnf/brew install`,
`sudo`, configure/make, CMake/build, or dependency compilation path.

## Submission ownership and ambiguous transport

The execution ULID exists before `sbatch`/`qsub`. Remote staging and an atomic
submission-state document are keyed by it. The remote worker writes
`submitting` before calling the scheduler and `submitted` with the exact job ID
after a parseable acceptance.

After acceptance, Slurm/PBS—not Bourne—owns job lifetime. Bourne starts no
keepalive. The local computer may disconnect. On reconnect, `execution wait`
or the structured reconcile operation reads the exact remote execution state,
queries only the exact Bourne-owned scheduler job, and imports a bounded worker
result through the existing transactional validator.

If transport fails after the scheduler may have accepted:

- a retry first reads the same execution state;
- a known job ID is reconciled and never resubmitted;
- `submitting` without a job ID remains `submission_ambiguous`;
- a new execution from that plan is blocked until the ambiguous execution is
  reconciled;
- missing scheduler/result evidence never becomes completion.

Remote machines without Slurm/PBS may be discovered and used for planning.
v0.7 does not claim disconnect-safe direct remote execution and adds no nohup,
tmux, screen, systemd-user service, daemon, or process supervisor.

## Data boundary and current limits

The remote protocol returns structured metadata, digests, bounded evidence,
and a bounded result bundle. Bourne transfers its own workers and an explicitly
materialized small execution input when needed. It does not automatically
crawl, synchronize, or download project trees, datasets, or bulk artifacts.
Large scientific data may remain on the HPC filesystem.

Current limitations include Python 3 on login/compute nodes, a configured
remote project/staging root for execution, site-dependent module-shell
behavior, common Slurm/PBS command formats rather than every vendor variant,
and no scheduler-free disconnect-safe runtime. Resource shapes and policy
claims are bounded evidence, not proof of future scheduler admission.
