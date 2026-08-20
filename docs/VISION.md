# Project Bourne

> **Every experiment has a history.**

**Universal experiment provenance and reproducibility for science and engineering.**

---

# The Problem

Scientific results rarely come from source code alone.

A result may depend on:

* a particular commit
* uncommitted modifications
* data
* configuration
* command-line parameters
* environment variables
* dependencies
* compilers
* runtimes
* operating systems
* CPU architecture
* GPU hardware
* drivers
* CUDA state
* numerical solver settings
* random seeds
* preprocessing
* upstream experiments
* postprocessing
* human decisions
* AI-agent decisions

Yet once the final:

* figure
* checkpoint
* table
* simulation field
* dataset
* measurement
* report

has been produced, much of that history is often lost.

Months later, even the original researcher may struggle to answer:

> Exactly how did this result come to exist?

Project Bourne exists to answer that question.

The v0.5 architecture adds a versioned execution-request boundary, truthful
summary telemetry, and deterministic artifact verification over the generic
v0.4 workload and execution-plan layer. User intent, workload understanding,
planning, execution attempts, scheduler facts, actual experiments, telemetry,
and verification remain distinct; see
[Execution requests, telemetry, and verification](EXECUTION_REQUESTS.md).

---

# Vision

Project Bourne is a general-purpose scientific experiment provenance, reproducibility, traceability, and durable record-keeping layer.

Its purpose is to make scientific work:

* reproducible
* traceable
* inspectable
* comparable
* durable
* understandable by humans
* understandable by AI agents
* eventually verifiable through explicit scientific evidence

Bourne should operate across scientific and engineering disciplines rather than belonging to a particular software framework.

---

# The Experiment

The central abstraction in Bourne is the:

# Experiment

An experiment transforms scientific inputs and execution context into outputs.

Conceptually:

```text
source
+
inputs
+
configuration
+
environment
+
hardware
+
execution
+
upstream lineage
        │
        ▼
    EXPERIMENT
        │
        ▼
outputs
+
metrics
+
artefacts
+
scientific result
```

Bourne records this relationship.

---

# Beyond Code History

Git answers:

> How did this code evolve?

Scientific provenance is larger than source code.

The exact same commit may generate different results because:

* data changed
* parameters changed
* dependencies changed
* environment changed
* hardware changed
* compiler changed
* driver changed
* preprocessing changed
* numerical tolerances changed
* solver configuration changed

Bourne extends provenance from:

```text
code history
```

toward:

```text
experimental history
```

A useful mental model is:

> **Git tells you how your code came to exist.
> Bourne tells you how your scientific result came to exist.**

Bourne complements Git.

It does not replace it.

---

# Universal by Default

Bourne should not care which language or framework executes inside an experiment.

This should work:

```bash
bourne run python train.py
```

But so should:

```bash
bourne run ./solver config.yaml
```

and:

```bash
bourne run julia simulation.jl
```

and:

```bash
bourne run matlab -batch "run_experiment"
```

and eventually:

```bash
bourne run comsol batch ...
```

or:

```bash
bourne run mpirun -np 64 ./simulation
```

Python may be used to implement an early Bourne reference implementation.

Python is not the scientific execution model.

---

# Domain-Aware When Available

Generic provenance already has significant value.

Scientific ecosystems contain additional metadata that Bourne may understand through optional collectors or integrations.

Examples include:

## JAX

* JAX version
* jaxlib
* XLA backend
* device topology

## CUDA

* GPU model
* active driver
* CUDA runtime
* compiler
* compute capability

## MATLAB

* MATLAB release
* installed toolboxes

## COMSOL

* COMSOL version
* installed modules
* model identity
* solver configuration

## Slurm

* job ID
* partition
* node allocation
* CPU allocation
* GPU allocation
* scheduler metadata

These capabilities should enrich Bourne.

They must not define Bourne core.

The guiding principle is:

> **Universal by default, domain-aware when available.**

---

# Local First

A researcher should be able to install Bourne and immediately record experiments without:

* creating an account
* configuring an organisation
* connecting cloud infrastructure
* uploading proprietary research
* deploying a server

The initial experience should eventually be:

```bash
pip install bourneprov

bourne run python experiment.py

bourne list

bourne show <experiment-id>
```

Local scientific provenance should remain a first-class capability even if collaborative or hosted services exist later.

---

# Minimal Instrumentation

Scientific software is fragmented.

Researchers use:

* Python
* C
* C++
* Fortran
* Julia
* MATLAB
* R
* CUDA
* MPI
* commercial simulation software
* institutional HPC
* legacy internal applications

Bourne cannot require researchers to rewrite all of these systems around an SDK.

The baseline interaction therefore operates around execution:

```bash
bourne run <existing command>
```

Code-level instrumentation may later provide richer information.

It should be optional.

---

# Execution Happens Somewhere

Many researchers work through:

```text
Laptop
   │
   │ SSH
   ▼
Remote workstation
   │
   │ scheduler
   ▼
Compute node
   │
   ▼
Scientific experiment
```

Scientific provenance belongs to the environment where the experiment actually executes.

For the first version:

```text
ssh remote-machine
bourne run ./experiment
```

Bourne records that remote machine.

Future Bourne versions may understand separate:

* control machines
* SSH connections
* login nodes
* schedulers
* compute nodes
* containers
* execution contexts

Remote orchestration is not required for Bourne to provide immediate value.

---

# Runtime Reality Matters

Installed software state and active runtime state can differ.

For example:

* a driver package may have been upgraded
* an older kernel driver may still be active
* a container may expose a different userspace stack
* environment modules may alter available software
* a process may run inside a virtual or containerised environment

Bourne should strive to preserve the environment actually used by the experiment rather than merely recording what appears to be installed.

Scientific provenance is about runtime reality.

---

# Failed Experiments Matter

Science contains failure.

A program crashes.

A simulation diverges.

A compiler fails.

A solver does not converge.

An MPI process dies.

A training run runs out of memory.

A parameter choice produces an invalid solution.

These failures contain scientific and engineering information.

They should not disappear.

> **A failed experiment is still an experiment.**

The history of failure may later explain why the successful result exists.

---

# Execution Success Is Not Scientific Success

A critical distinction in Bourne is:

```text
The program executed successfully.
```

versus:

```text
The scientific result is valid.
```

These are not equivalent.

A simulation may return:

```text
exit code = 0
```

while violating:

* conservation laws
* boundary conditions
* expected convergence order
* physical constraints
* numerical stability
* reference values

Therefore Bourne should ultimately treat scientific verification as a separate concept from process execution.

---

# Verification

Future Bourne workflows should support explicit evidence that a scientific result is acceptable.

Possible verification methods include:

* unit tests
* integration tests
* numerical residuals
* conservation checks
* physical invariants
* analytical solutions
* reference solutions
* convergence studies
* regression comparisons
* dimensional checks
* uncertainty thresholds
* independent evaluator agents
* human review

Where deterministic scientific evidence exists:

> **Deterministic verification should take precedence over language-model confidence.**

An AI agent saying:

> "This result appears reasonable."

is not sufficient when the result can instead be checked numerically.

---

# Experimental Lineage

Experiments rarely exist in isolation.

They form families.

Example:

```text
Experiment A
     │
     │ mesh 256 → 512
     ▼
Experiment B
     │
     │ solver tolerance changed
     ▼
Experiment C
```

Or:

```text
raw data
   │
   ▼
preprocessing
   │
   ▼
simulation
   │
   ▼
postprocessing
   │
   ▼
Figure 7
```

Possible relationships include:

```text
derived_from
reproduced_from
rerun_of
compared_with
generated_by
uses_artifact
produced_artifact
```

The long-term result is an experimental provenance graph.

---

# Closed-Loop Scientific Execution

Bourne should eventually support more than recording experiments after they happen.

An AI-assisted scientific workflow may operate as a closed loop:

```text
goal
 ↓
plan
 ↓
implement
 ↓
execute
 ↓
verify
 ↓
pass ───────────────────► complete
 │
 fail
 │
 ▼
diagnose
 ↓
modify
 ↓
rerun
 └──────────────────────► verify
```

A failed attempt should not disappear merely because another attempt later succeeds.

Instead, Bourne should preserve the complete path.

Example:

```text
Task
 │
 ├── Experiment 001
 │     └── compilation failed
 │
 ├── Experiment 002
 │     └── executed, conservation check failed
 │
 ├── Experiment 003
 │     └── mesh convergence failed
 │
 └── Experiment 004
       └── verified successfully
```

The final scientific result is therefore connected not only to the final run, but to the history that produced it.

---

# Loop Engineering

A future Bourne agent should be able to operate through iterative scientific improvement:

```text
execute
   ↓
observe
   ↓
evaluate
   ↓
pass? ─────────► finish
   │
   no
   │
   ▼
diagnose
   ↓
repair
   ↓
retry
```

This loop may handle engineering failures such as:

* syntax errors
* compiler errors
* missing files
* shape mismatches
* numerical crashes
* CUDA out-of-memory errors

But it should also handle scientific failures such as:

* poor convergence
* violated conservation
* incorrect boundary behaviour
* excessive residuals
* failed benchmarks

The scientific loop is more than a software-debugging loop.

---

# Graph-Structured Scientific Work

Complex scientific tasks may eventually be represented as execution graphs.

Example:

```text
                 Planner
                    │
                    ▼
                Implementer
                    │
                    ▼
                  Runner
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
    Code Evaluator      Science Evaluator
          │                   │
     ┌────┴────┐         ┌────┴────┐
     │         │         │         │
   fail       pass     fail       pass
     │         │         │         │
     ▼         │         ▼         ▼
  Debugger     │     Diagnoser   Reporter
     │         │         │
     └─────────┴─────────┘
               │
               ▼
              rerun
```

Bourne should not depend on one graph framework.

The reasoning layer may be provided by:

* Codex
* Claude
* DeepSeek
* another agent runtime
* future systems

Bourne's job is to make the resulting scientific process persistent and inspectable.

---

# Tasks, Experiments, and Actions

A future natural-language request may correspond to many experiments.

For example:

```text
Task: verify a FEM implementation

├── Agent Action
│    └── inspect source
│
├── Experiment 001
│    └── failed
│
├── Agent Action
│    └── repair element assembly
│
├── Experiment 002
│    └── execution succeeded
│
├── Verification
│    └── convergence failed
│
├── Agent Action
│    └── repair boundary treatment
│
└── Experiment 003
     └── verified
```

Therefore Bourne should eventually distinguish:

```text
Task
Agent Action
Experiment
Verification
Artifact
Lineage
```

These concepts should be related, not collapsed into one object.

---

# Natural-Language Scientific Execution

A future Bourne interaction may look like:

```text
$ bourne agent

> Implement a 1D finite-element solver for this PDE.
> Verify second-order convergence.
> If verification fails, investigate and fix it.
> Once verified, plot the result.
```

The execution system may then:

```text
understand
→ inspect
→ plan
→ implement
→ execute
→ observe
→ verify
→ diagnose
→ repair
→ rerun
→ verify
→ plot
→ report
```

Every significant execution should remain connected to Bourne provenance.

The user should not merely receive:

```text
result.png
```

They should also be able to determine:

* which experiments produced it
* which code version was used
* what environment executed them
* which failed attempts preceded it
* why modifications were made
* what verification criteria were applied
* which experiment first passed
* which artefacts derived from that verified result

---

# Humans and AI Agents

Bourne must remain valuable without artificial intelligence.

That requirement is fundamental.

AI agents nevertheless make scientific provenance increasingly important.

Agents may:

* edit source code
* choose parameters
* run experiments
* inspect failures
* alter configurations
* generate plots
* compare results
* submit jobs
* launch follow-up experiments

Without persistent provenance, autonomous research becomes difficult to audit.

Conceptually:

```text
Codex ─────┐
Claude ────┤
DeepSeek ──┤
Human ─────┤
Script ────┘
           │
           ▼
         Bourne
           │
           ▼
 provenance + lineage
           │
           ▼
 scientific memory
```

Bourne is therefore:

> **Agent-native, but not agent-dependent.**

---

# The Role of the Agent

Agent systems may decide:

> What should happen next?

Bourne preserves:

> What actually happened?

Long-term, Bourne should also preserve:

> Why the next attempt happened, what evidence was evaluated, and how the final verified result emerged.

The reasoning model may change.

The agent framework may change.

Scientific history should survive both.

---

# Scientific Scope

Bourne should remain applicable across areas such as:

## Physics

* plasma physics
* fusion
* optics
* electromagnetics
* astrophysics
* condensed matter

## Engineering

* CFD
* FEM
* structural mechanics
* chemical engineering
* electrical engineering
* robotics
* control

## Computational Science

* molecular dynamics
* Monte Carlo
* PDE simulation
* optimisation
* uncertainty quantification
* HPC

## Artificial Intelligence

* machine learning
* scientific ML
* neural operators
* benchmarking
* fine-tuning

## Experimental Science

Longer-term provenance concepts may extend toward:

* physical samples
* instruments
* calibration
* acquisition parameters
* protocols
* measurements
* derived datasets

Bourne should not prematurely implement every scientific domain.

Its abstractions should simply avoid excluding them.

---

# Scientific Memory

Experiment tracking is not the final objective.

The objective is persistent:

# Scientific memory

A researcher should eventually be able to ask:

> Which experiment produced this figure?

> What changed between these two runs?

> Which dataset was used?

> Which commit generated this result?

> Was the repository dirty?

> Which GPU and active driver executed this simulation?

> What failed before this successful run?

> Why did the agent modify this file?

> Which verification criterion failed?

> Which experiment first passed convergence?

> Reproduce this experiment.

> Rerun it with only one parameter changed.

> Show everything derived from this result.

Bourne should make these questions answerable.

---

# Initial Product Philosophy

Build the smallest implementation that already provides real scientific value.

Do not begin with:

* dashboards
* enterprise platforms
* cloud accounts
* massive workflow engines
* dozens of framework integrations
* autonomous scientific agents
* elaborate graph runtimes

Begin with one strong promise:

> **Run an experiment through Bourne and preserve enough context to understand later what happened.**

The first proof is:

```bash
bourne run <command>

bourne list

bourne show <id>

bourne compare <id-a> <id-b>
```

Everything else grows from a trustworthy provenance foundation.

---

# Product Evolution

A reasonable evolution is:

```text
v0.1
Provenance core
run / list / show / compare
        │
        ▼
v0.2
Artifacts + lineage
        │
        ▼
v0.3
Compute-site topology + capability discovery
        │
        ▼
v0.4
Workload discovery + execution-context resolution
        │
        ▼
v0.5
Unified execution requests + telemetry + deterministic artifact verification
        │
        ▼
v0.6
Closed-loop execution
        │
        ▼
v0.7+
Graph / agent-assisted scientific execution
```

This sequence is directional rather than contractual.

Do not sacrifice a reliable provenance foundation for premature autonomy.

---

# Identity

**Project:**

```text
Project Bourne
```

**Python distribution:**

```text
bourneprov
```

**Python package:**

```text
bourneprov
```

**CLI:**

```text
bourne
```

**Tagline:**

> **Every experiment has a history.**

**Short description:**

> **Universal experiment provenance and reproducibility for science and engineering.**

---

# Long-Term Ambition

Bourne should become infrastructure researchers stop thinking about.

Experiments happen.

Their provenance simply exists.

Agents iterate.

Their failed and successful attempts remain connected.

Verification is performed.

The evidence remains attached.

Months or years later, a researcher, collaborator, reviewer, automated system, or AI agent should still be able to reconstruct the path from scientific intent to verified result.

The objective is not merely experiment tracking.

The objective is not merely autonomous execution.

The objective is:

> **Reproducible scientific memory.**
