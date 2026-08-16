# Compute-site discovery

Project Bourne v0.3.0 discovery records what the current researcher's execution
surface looked like at a particular time. It does not choose an environment,
submit a job, or certify that a workload will run.

## Snapshot and topology semantics

Every `bourne discover` creates a new immutable, 26-character ULID snapshot in
the configured Bourne SQLite database. `bourne inventory` reads the latest
snapshot without running discovery; it also accepts a full ID, unique prefix,
`latest`, or snapshot-scoped `@N` reference.

The normalized model distinguishes:

~~~text
current identity
    |
    v
current access target
    |---------------- storage resources
    |---------------- direct execution contexts and capabilities
    |
    +---- scheduler (optional)
              |
              +---- visible aggregate execution-target classes
~~~

The simplest valid site is a laptop, desktop workstation, personal GPU or
DGX-class machine, or shared laboratory workstation with direct contexts and no
scheduler. Institutional HPC and supercomputer-style scheduler environments
use the same model. On an HPC access target, read-only scheduler metadata can
add partition/queue target classes without scanning or connecting to compute
nodes. Bourne does not invent a site name or call a host a login node from its
hostname or from scheduler-client presence. An active Slurm/PBS allocation is
recorded only from direct allow-listed allocation evidence.

Discovered execution contexts are candidate locations such as the current
system environment, Conda environments, virtualenvs, loaded-module state, and
metadata-level containers. They are intentionally distinct from the v0.2
`ExecutionContext`, which records where one experiment actually ran.

## Authorized execution surface

Bourne maps the current researcher's authorized execution surface from evidence
available to the current process, but it does not claim that every visible
resource is authorized. Where authorization is not directly established, the
recorded value remains `unknown`.

Discovery is scoped to the current process identity. The identity provider may
record the current username, POSIX IDs, current supplementary group IDs/names,
and current home path. It never enumerates the system user database or the
members of a group.

Only these filesystem locations may be considered:

- direct entries in the current `PATH` directories;
- current `VIRTUAL_ENV` and bounded project-local `.venv`, `venv`, `env`, or
  direct-child `pyvenv.cfg` candidates;
- standard executable directories for prefixes returned by the bounded Conda
  environment-list command;
- the current working directory and explicit `HOME`, `SCRATCH`, `WORK`,
  `PROJECT`, `PROJECT_DIR`, `TMPDIR`, `TEMP`, and `TMP` path hints.

Storage paths are inspected with metadata/access checks only. Bourne does not
recurse into them, open files to probe permissions, walk `/home/*`, inspect
sibling users, read SSH keys, or crawl shared filesystems. Linux mount metadata
is used only to map these already-known paths to a mount point and filesystem
type. `readable`, `writable`, and `searchable` are observations for the current
process, not ACL analysis or policy guarantees. Roles are hints; retention,
quota, backup, and purge policy remain unknown.

SSH variables are reduced to one `ssh_session` boolean. Source addresses are
not persisted.

## Capabilities and evidence

The primary capability is a generic executable. PATH and environment scanners
enumerate direct executable directory entries and never invoke them. Known
names may receive tags such as `interpreter`, `compiler`, `launcher`,
`scheduler-client`, `container-runtime`, or `shell`; classification is only an
annotation. An unrecognized scientific binary remains first-class.

Every capability has explicit evidence. Filesystem observations are
`observed_now`. Bourne history records completed, failed, and interrupted
observations with last-seen metadata as `historical_only`; historical success
is never promoted to current availability or authorization.

Before a snapshot is persisted, every evidence reference is validated against
a subject of the declared type in that same snapshot. Evidence cannot be both
`observed_now` and `historical_only`.

System observations retain the distinction between NVIDIA driver-supported
CUDA metadata and a separately observed `nvcc` executable.

## Provider behavior and bounds

Provider status is one of `complete`, `unavailable`, `partial`, `error`, or
`timeout`. A provider that completes with no observations is different from an
unavailable provider. Failures are isolated and retained in JSON and SQLite.

Filesystem scanning is non-recursive. The default bounds are 256 PATH entries,
50,000 direct entries per scanned executable directory, and 256 project-local
virtualenv candidates. Hitting a limit produces `partial` status.

Every new subprocess probe uses `shell=False`, a five-second timeout, and a
1 MiB bound for each of stdout and stderr while continuing to drain excess
output to avoid pipe deadlock. The exact read-only command surfaces are:

~~~text
conda env list --json
docker ps --all --no-trunc --format {{json .}}
podman ps --all --format json
sinfo --noheader --format=%P|%a|%l|%D|%c|%m|%G
qstat -Q -f
~~~

The Slurm query returns aggregate partition summaries without node names or job
details. The PBS query returns queue summaries and persists only a small field
allow-list. Bourne does not query all-user jobs—or any jobs in v0.3—submit,
launch, cancel, or mutate scheduler state. Read-only scheduler clients may
contact their locally configured controller; discovery otherwise makes no
network requests and never uses SSH.

Container discovery lists ID, name, image reference, and state. It never
starts, stops, attaches to, executes inside, mounts, removes, builds, or pulls a
container, and never inspects its environment or secrets. Container-internal
capabilities remain explicitly unprobed.

Module discovery reads only `LOADEDMODULES` and `MODULEPATH`. It never runs
module load/unload/purge or an unbounded module inventory.

Discovery never runs package/dependency enumeration, arbitrary `--version` or
`--help` probes, compiler tests, installations, or environment activation.

## Truthfulness and future execution

The following distinctions are permanent:

- observed executable does not mean compatible workload;
- visible scheduler target does not mean the current identity is authorized;
- container existence does not reveal its internal software;
- historical use does not establish present availability or permission;
- storage role hints do not establish policy.
- the current access target is not the same as a scheduler-visible compute
  target class.

The target/context/storage relationships are queryable so a future workload
resolver can consider direct or scheduler-mediated execution without changing
workload semantics. Scheduler execution will need separate control-plane
submission, scheduler job, requested resource, allocated resource, actual
target, and actual scientific experiment records. v0.3 deliberately creates
none of those execution entities and performs no selection or ranking.

## JSON API shape

`bourne discover --json`, `bourne inventory --json`, and
`bourne inventory --find NAME --json` expose structured records rather than
terminal-formatted prose. The inventory top level contains `snapshot`,
`identity`, `current_target`, `storage`, `scheduler`, `execution_targets`,
`execution_contexts`, `capabilities`, `evidence`, and `providers`. The same
domain service and models are reusable by a future Python API, MCP surface, or
native core without parsing CLI output.
