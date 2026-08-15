# Artifacts, lineage, and execution context

Project Bourne v0.2 adds a small generic provenance model around the existing
experiment recorder:

~~~text
declared inputs -> experiment -> declared outputs -> derived experiment
~~~

The model does not interpret file formats or application semantics. An unknown
executable is a first-class workload.

## Artifact capture

Declare files explicitly before the command separator:

~~~bash
bourne run \
  --input config.json \
  --input mesh.dat \
  --output result.csv \
  --output figure.png \
  -- ./solver case.yaml
~~~

Inputs are captured before the process starts. Outputs are captured after it
ends, including when it fails or is interrupted. Every declaration produces a
version record containing:

- its own stable ULID;
- the original and normalized absolute path;
- the input or output role;
- present/missing state;
- SHA-256 content identity, byte size, and modification time when readable;
- capture time and any capture error.

SHA-256 reads are chunked. Bourne references and fingerprints files; it does
not copy them into hidden storage. A path is not identity: repeated writes to
result.csv create distinct records, and changed content has a different hash.

bourne show EXPERIMENT displays declared inputs, outputs, and immediate
ancestry. Expected outputs that do not exist are shown as missing.

## Lineage

Record one intentional parent with any normal experiment reference:

~~~bash
bourne run --derived-from @1 -- ./solver case_B.yaml
~~~

The supported relationship is derived_from. Full ULIDs, unique prefixes,
latest, and @N retain their v0.1.1 semantics. This is provenance metadata, not
a workflow engine.

## Tracing an output

~~~bash
bourne trace result.csv
~~~

When the file exists, Bourne fingerprints its current content and first matches
both normalized path and SHA-256. Content identity can also trace a file that
was moved or copied. When the file is absent, Bourne can use a unique historical
path. If several versions remain possible, it reports every candidate and does
not guess.

Trace output includes the producing experiment, command, resolved executable,
Git/host context, declared inputs, and the derived_from ancestry chain.

## Safe execution context

Bourne records:

- the executable requested by the user;
- its resolved path at launch time when available;
- the executable running the Bourne recorder;
- allow-listed VIRTUAL_ENV, CONDA_PREFIX, and CONDA_DEFAULT_ENV hints;
- a container-presence flag only when a conservative marker file is observed.

Bourne does not dump the environment. Tokens, passwords, credentials, shell
history, and unrelated variables are not captured. Context collection is
observational: it does not modify PATH, install software, inspect other
containers, or choose an environment.

## Database compatibility

Opening a v0.1.1 schema-version-1 database transactionally migrates it to
schema version 2. The migration adds execution context to experiments plus
separate indexed artifacts and experiment_lineage tables. Existing completed,
failed, and interrupted experiments remain schema-version-1 records with no
artifact or lineage relationships.

Unknown or newer database versions fail explicitly. Bourne never resets an
unrecognized database.

## Current limits

- Inputs and outputs require explicit declaration; Bourne does not scan the
  working tree or infer process file access.
- Files are fingerprinted but not archived, so long-term availability remains
  the user's responsibility.
- stdout and stderr still stream live but are accumulated in memory before the
  experiment is committed. Disk-spooled logs and log artifacts remain future
  work.
- Environment discovery/resolution, schedulers, remote execution, telemetry,
  scientific verification, MCP, agents, and application adapters are not part
  of v0.2.
