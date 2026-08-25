# Runtime Evidence and Scheduler Coverage

Project Bourne v0.8 adds execution-time truth without turning the Compute
Worker into a monitoring service. The worker is still short-lived,
execution-scoped, user-space, non-AI, and non-daemon.

## Truth layers

Bourne keeps five conclusions separate:

1. **Planning truth** records what the immutable `ExecutionPlan` intended.
2. **Scheduler truth** records what Slurm, PBS, or LSF accepted and reported.
3. **Runtime truth** records what the Compute Worker observed inside the
   allocation.
4. **Experiment truth** records the exact scientific process and its outputs.
5. **Verification** records whether declared checks passed.

None of those facts automatically establishes general scientific validity.
Requested resources, allocated resources, and observed utilization are also
different facts. A visible device is not necessarily proof of scheduler
allocation, and a login-node observation is never promoted to a compute-node
observation.

## Versioned evidence

Runtime evidence schema version 1 contains explicit `process`, `allocation`,
`cpu`, `memory`, `io`, `gpu`, and `environment` groups. Every group declares
one coverage value:

```text
observed
partially_observed
unavailable
unsupported
unknown
```

On Linux, summary mode samples only the scientific root process and descendants
visible through `/proc`, at a bounded 100 ms interval. It records cumulative
CPU time, peak process-tree RSS, and process I/O counters when available. It
does not scan unrelated user or system workloads. These samples cover the
process tree visible to this Compute Worker on this allocation node; they are
not whole-cluster or automatically distributed per-node telemetry.

For Slurm allocations, `SLURM_JOB_CPUS_PER_NODE` is parsed as whole-job CPU
evidence, including compressed forms such as `72(x2),36`. In contrast,
`SLURM_CPUS_ON_NODE` is local-node evidence and is not multiplied by the job's
node count. Raw `SLURM_JOB_NODELIST` is retained as a bounded allocation fact;
Bourne does not expand it or substitute the local hostname as though it were
the complete allocation host list.

On a platform without compatible `/proc` evidence, those groups are explicitly
unavailable or unsupported. Missing NVIDIA tools or GPU evidence never blocks
an otherwise valid workload. `CUDA_VISIBLE_DEVICES` can establish bounded
device visibility, but Bourne labels it partial and makes no utilization claim.
`"telemetry": {"mode": "off"}` disables process sampling and records that
absence without fabricating zero values.

The compute-worker result captures at most 8 MiB per stdout/stderr stream while
continuing to relay output live. Truncation, captured byte counts, and the limit
are recorded. The full worker-result bundle remains bounded to 32 MiB.

## Failure and partial evidence

Termination evidence records the best justified phase and outcome. Supported
outcomes include preflight failure, launch failure, a running process that
failed, signal termination, scheduler cancellation/timeout, out-of-memory or
node failure, missing/partial result-bundle evidence, unavailable telemetry, failed
verification, and completion.

Scheduler classification is evidence-driven and remains separate from
scientific validity. A terminal scheduler state without a valid result bundle
does not create an `Experiment`. Evidence captured before a process failure is
still persisted transactionally where available.

## IBM LSF

LSF is a first-class backend alongside Slurm and PBS:

- discovery uses bounded `bqueues` fields and performs no job query;
- submission sends the Bourne-owned script to `bsub` through stdin and captures
  one exact numeric job ID;
- active observation uses an exact-job `bjobs` query with selectable `jobid`
  and `stat` fields;
- a separately labelled recent-finished `bjobs -a` query is used only after the
  active view no longer establishes the job;
- an exact-job `bhist -l -n 0` query provides durable historical accounting
  after the recent-finished view expires; command time and output remain
  bounded, and unavailable or malformed history remains unknown;
- cancellation uses `bkill` for the exact job attached to the Bourne execution;
- a timed-out or otherwise identity-ambiguous submission is never blindly
  resubmitted.

Raw LSF state remains evidence. `UNKWN` and `ZOMBI` are non-terminal scheduler
uncertainty, while `POST_DONE` and `POST_ERR` are separate post-processing
outcomes rather than scientific workload outcomes.

Portable resource mapping is deliberately narrow. Bourne maps queue, total
CPU/MPI slots, divisible `span[ptile=...]` placement, and wall time. A
single-host request can use `span[hosts=1]`. Generic `-nnodes` is not emitted
because IBM documents it as a CSM-specific option. LSF memory units and GPU
request syntax are site-configurable, so v0.8 leaves those mappings unresolved
instead of inventing a resource expression.

When total ranks are known but node count and per-host capacity are not, the
LSF shape keeps `nodes` and `ranks_per_node` unknown and emits only `#BSUB -n`.
`span[ptile=...]` requires explicit nodes or a provider/per-host fact that
justifies the placement.

Inside an allocation the worker records an allowlist of `LSB_JOBID`,
`LSB_QUEUE`, `LSB_HOSTS`, `LSB_MCPU_HOSTS`, `LSB_DJOB_NUMPROC`,
`LSB_GPU_REQ`, and `CUDA_VISIBLE_DEVICES` when present. Arbitrary environment
variables are not persisted.

## Existing Apptainer/Singularity images

An immutable selected plan may wrap the exact scientific argv with an existing
`apptainer` or `singularity` image:

```bash
bourne plan --site hpc --request bourne.json \
  --candidate sha256:... \
  --container-runtime apptainer \
  --container-image /shared/images/solver.sif \
  --container-bind /project/input:/input:ro
```

The runtime and image are rechecked inside the compute allocation. An optional
`sha256:` image digest is streamed and verified. Bind mounts are bounded,
explicit, and typed. Bourne executes `apptainer exec`/`singularity exec` as
argv, never as a scientific shell string. A missing runtime, image, digest
match, or bind source is `preflight_failed`; scientific argv does not start.

Bourne does not build, pull, install, convert, or register images and does not
manage Docker, a daemon, Kubernetes, or a container registry. v0.8 also does
not automatically orchestrate multi-node container launch or choose whether an
MPI launcher belongs outside or inside the container command. Bourne does not
inject MPI launchers.

## Protocol and storage evolution

v0.8 uses additive evolution:

- ExecutionRequest v2 adds the explicit LSF backend; v1 remains readable;
- SQLite schema 7 adds runtime/termination evidence and admits LSF while
  transactionally migrating released schema 4, 5, and 6 databases (earlier
  released schemas continue through the existing migration chain);
- remote-worker protocol remains v1;
- worker-result protocol v3 adds required runtime/termination evidence while
  v1 and v2 remain readable;
- staged-plan protocol v4 carries current ExecutionRequest v2 payloads and
  optional container execution while v1, v2, and v3 remain readable without
  reinterpretation.

The base Python package remains dependency-free. MCP remains optional. Neither
the Remote Worker nor Compute Worker requires `uv`, a package manager, network
access, root, an inbound port, AI credentials, or a persistent service.

## OpenSSH and current limits

Bourne invokes a configured SSH site alias and lets OpenSSH apply the user's
normal `Host`, `HostName`, `User`, `Port`, `IdentityFile`, and `ProxyJump`
configuration. Bourne does not parse or reimplement `~/.ssh/config`.

Current limitations include common IBM Spectrum LSF 10.x command/output forms
rather than every vendor/site customization, Python 3 inside scheduler
allocations, no whole-allocation multi-node telemetry aggregation, no automatic
cross-cluster placement, no package/environment installation, and no automatic
container image management or multi-node container/MPI ordering. Control-plane
support remains Linux and macOS;
native Windows is not yet supported or validated.
