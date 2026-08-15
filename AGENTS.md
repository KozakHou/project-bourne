# Project Bourne — Agent Instructions

## Project Identity

**Project:** Project Bourne
**Python distribution:** `bourneprov`
**Python package:** `bourneprov`
**CLI:** `bourne`

**Tagline:**

> Every experiment has a history.

**Short description:**

> Universal experiment provenance and reproducibility for science and engineering.

Project Bourne is a general-purpose scientific experiment provenance, reproducibility, traceability, and durable record-keeping system.

Its purpose is to preserve how scientific and engineering results came to exist.

The core abstraction is the **experiment**, not the programming language, framework, solver, or AI agent.

---

# Core Product Principle

> **Universal by default, domain-aware when available.**

Bourne core must work without knowing whether an experiment uses:

* Python
* JAX
* PyTorch
* TensorFlow
* Julia
* MATLAB
* R
* C
* C++
* CUDA
* Fortran
* MPI
* COMSOL
* OpenFOAM
* LAMMPS
* GROMACS
* proprietary scientific software
* shell scripts
* arbitrary executables

Framework- and domain-specific knowledge belongs in optional collectors, adapters, integrations, or later verification modules.

---

# Mental Model

Git answers:

> How did this code come to exist?

Bourne answers:

> How did this scientific result come to exist?

A result may depend on:

* source code
* Git state
* data
* configuration
* parameters
* environment
* dependencies
* compiler/runtime
* hardware
* operating system
* execution command
* random state
* upstream experiments
* generated artefacts
* human actions
* agent actions

Bourne exists to preserve that history.

---

# Non-Negotiable Rules

1. Never make Bourne core dependent on a specific ML framework.

2. Arbitrary commands and executables are first-class experiments.

3. Basic provenance capture must not require users to modify their scientific source code.

4. A failed experiment is still an experiment and must be recorded.

5. Provenance correctness takes priority over UI polish.

6. Bourne must remain useful without AI agents.

7. AI agents are consumers and producers of Bourne provenance; Bourne core is not itself an agent harness.

8. Local operation must not require cloud infrastructure.

9. SQLite is the initial local metadata store.

10. Framework-specific metadata belongs outside the universal core.

11. Optional collectors must fail gracefully.

12. Missing Git, GPU, CUDA, NVIDIA tooling, or optional dependencies must not prevent an otherwise valid experiment from running.

13. Prefer automatic capture over manual instrumentation when reliable.

14. Never silently discard provenance when an unavailable or failed field can instead be represented explicitly.

15. Avoid premature abstractions and speculative distributed infrastructure.

16. Every public CLI command must have automated tests.

17. README examples must correspond to behaviour that actually works.

18. Do not claim support for integrations that have not been implemented and validated.

19. Experiment identity must remain independent from any future task, agent session, workflow, or graph identity.

20. Do not equate successful process execution with scientific correctness.

---

# Implementation Language

The initial reference implementation may be written in Python.

Python is an implementation choice, not a product assumption.

The Bourne experiment model, storage semantics, CLI semantics, and provenance model must remain language-agnostic.

A scientific experiment does not need to be written in Python.

Examples must eventually demonstrate multiple execution styles such as:

```bash
bourne run python train.py

bourne run ./solver case.yaml

bourne run julia simulation.jl

bourne run matlab -batch "experiment"

bourne run mpirun -np 64 ./solver
```

Do not encode assumptions that prevent a future native implementation, including a possible Rust core.

---

# Execution Context

Many scientists work through:

```text
Laptop
   │
   │ SSH
   ▼
Remote workstation / HPC
   │
   ▼
Experiment
```

For v0.1:

> Bourne records the environment in which Bourne itself executes.

Example:

```text
Laptop
   │
   │ ssh
   ▼
Remote workstation
   │
   └── bourne run ./solver
```

The recorded execution provenance belongs to the remote workstation.

Do not implement SSH orchestration in v0.1.

However, avoid architecture that permanently assumes:

```text
control machine == execution machine
```

Future Bourne versions may distinguish:

* control machine
* login node
* scheduler
* compute node
* container
* execution environment

---

# v0.1 User Experience

The initial usable workflow must be:

```bash
bourne run python examples/demo.py

bourne list

bourne show <experiment-id>

bourne compare <experiment-a> <experiment-b>
```

A user must not need to import `bourneprov` into their scientific program for basic tracing.

---

# `bourne run`

`bourne run <command>` executes an arbitrary command while recording its provenance.

Capture at minimum, where available:

## Identity

* experiment ID
* execution status

## Execution

* original command
* arguments
* working directory
* start timestamp
* end timestamp
* duration
* exit code

## Process Output

* stdout
* stderr

## Git

* repository root
* commit SHA
* branch
* dirty state

Execution outside a Git repository must work normally.

## System

* operating system
* operating system version where practical
* architecture
* hostname
* CPU information
* GPU information where available
* NVIDIA driver information where available
* CUDA information where available

Systems without NVIDIA hardware must work normally.

---

# Runtime State vs Installed State

Scientific provenance should describe the environment actually used by the experiment.

Installed package state and active runtime state are not always identical.

For example, NVIDIA userspace packages may have been upgraded while an older kernel driver remains loaded until reboot.

Where practical:

> Runtime state should take precedence over merely installed state.

If both are meaningful and detectable, later versions may record them separately.

Do not overengineer platform-specific handling in v0.1, but do not design metadata under the assumption that installed state and active runtime state are always identical.

---

# Failed Experiments

A failed experiment remains part of scientific history.

For example:

```bash
bourne run python -c "raise RuntimeError('boom')"
```

must still generate an experiment record containing:

* experiment ID
* command
* environment provenance
* timestamps
* execution status
* non-zero exit code
* stdout
* stderr

Bourne should persist the record before preserving the underlying process failure semantics.

Failed and interrupted attempts must never be silently discarded merely because a later attempt succeeds.

---

# Execution Status vs Verification Status

These concepts must remain distinct.

## Execution status

Examples:

```text
completed
failed
interrupted
```

describes whether a process executed successfully.

## Scientific verification status

Future examples may include:

```text
unverified
passed
failed
inconclusive
```

Scientific verification asks whether the result satisfies meaningful scientific criteria.

A process may return:

```text
exit code = 0
```

while still producing an invalid scientific result.

Therefore:

> Execution success is not scientific success.

The v0.1 implementation does not need a verification engine.

Do not nevertheless build the data model around the assumption that process success implies scientific correctness.

---

# Storage

Use SQLite for v0.1.

Requirements:

* local
* durable
* simple
* deterministic
* no server required

Do not expose SQLite row IDs as public experiment identities.

Prefer a portable public ID such as ULID or another appropriate sortable identifier.

---

# Architecture Direction

Prefer a conceptually simple structure such as:

```text
bourneprov
├── cli
├── experiment lifecycle
├── execution
├── models
├── storage
├── collectors
│   ├── git
│   ├── system
│   ├── cpu
│   ├── gpu
│   └── cuda
└── presentation
```

This is guidance, not a requirement to create unnecessary modules.

Use the smallest architecture that cleanly satisfies the product requirements.

---

# Collectors

Collectors gather provenance.

They should:

* have narrow responsibilities
* return structured data
* be independently testable
* tolerate unavailable tools
* avoid changing experiment behaviour
* avoid crashing the scientific process

Example:

If:

```bash
nvidia-smi
```

does not exist or returns an error, Bourne must continue.

GPU provenance may be marked unavailable.

The experiment itself must not fail because optional GPU metadata could not be collected.

---

# v0.1 Explicit Non-Goals

Do not implement unless specifically requested:

* web dashboard
* hosted backend
* user accounts
* authentication
* cloud synchronisation
* SSH orchestration
* Slurm integration
* PBS integration
* Kubernetes integration
* COMSOL-specific integration
* MATLAB-specific integration
* JAX-specific integration
* PyTorch-specific integration
* TensorFlow-specific integration
* MLflow integration
* Weights & Biases integration
* remote artefact storage
* workflow DAG engine
* automatic optimisation
* parameter sweeps
* notebook platform
* plugin marketplace
* autonomous scientific agent
* loop engine
* graph engine
* planner agent
* repair agent
* scientific verifier framework
* full MCP ecosystem

Clean extension boundaries are welcome.

Speculative infrastructure is not.

---

# Experimental Lineage — Future Direction

Bourne should eventually support relationships such as:

```text
derived_from
reproduced_from
rerun_of
compared_with
generated_by
uses_artifact
produced_artifact
```

Example:

```text
Experiment A
     │
     │ grid 256 → 512
     ▼
Experiment B
     │
     │ tolerance 1e-6 → 1e-8
     ▼
Experiment C
```

Failed attempts must be able to remain part of this lineage.

Do not implement the complete lineage graph in v0.1 unless a minimal relationship naturally follows from the data model.

---

# Future Closed-Loop Scientific Execution

Long-term, Bourne should be capable of supporting scientific execution loops such as:

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
pass ─────────────► complete
  │
 fail
  │
  ▼
diagnose
  ↓
repair
  ↓
rerun
  └───────────────► verify
```

This is a future architectural requirement.

It is **not a v0.1 implementation requirement**.

Do not implement:

* an LLM loop
* autonomous repair
* a graph runtime
* planning agents
* evaluator agents

during v0.1.

However, avoid assumptions that would make closed-loop execution difficult later.

In particular, do not assume:

* one user request maps to exactly one experiment
* experiments always exist in isolation
* failed attempts are disposable
* only successful runs matter
* one experiment cannot derive from another
* process completion equals scientific verification

---

# Future Task Model

Future Bourne versions may represent a larger scientific task:

```text
Task
├── original goal
├── plan
├── Agent Action 001
├── Experiment 001 — failed execution
├── Agent Action 002
├── Experiment 002 — execution succeeded, verification failed
├── Agent Action 003
├── Experiment 003 — verification failed
└── Experiment 004 — verified
```

Experiment IDs must remain distinct from future:

* task IDs
* session IDs
* agent IDs
* graph-node IDs
* workflow IDs

Do not implement this task model in v0.1.

Simply avoid making it impossible.

---

# Verification Principle

Future scientific verification may include:

* unit tests
* integration tests
* residual thresholds
* conservation laws
* physical invariants
* boundary-condition checks
* analytical solutions
* reference solutions
* mesh convergence
* temporal convergence
* regression checks
* uncertainty criteria
* independent evaluator agents
* human review

Where deterministic scientific checks exist:

> Prefer deterministic evidence over LLM self-evaluation.

An agent saying:

> "The result looks correct."

is not sufficient scientific verification when numerical or physical criteria are available.

Do not implement this framework in v0.1.

---

# Loop Provenance

Future autonomous execution must preserve the unsuccessful path, not just the final successful result.

Example:

```text
Task
├── exp_001 — compilation failed
├── exp_002 — executed, conservation failed
├── exp_003 — convergence criterion failed
└── exp_004 — verified
```

Bourne should eventually be able to explain:

* why each experiment was launched
* what failed
* what evidence triggered another attempt
* what modification was made
* which verification criterion failed
* which attempt first passed
* which artefacts came from the accepted experiment

The entire loop should become scientific provenance.

---

# Future Graph Execution

Future agent systems may structure scientific work as graphs:

```text
                Planner
                   │
                   ▼
               Implementer
                   │
                   ▼
                 Runner
                   │
            ┌──────┴──────┐
            ▼             ▼
     Code Verifier   Science Verifier
            │             │
       ┌────┴────┐   ┌────┴────┐
       │         │   │         │
     fail       pass fail      pass
       │         │   │         │
       ▼         │   ▼         ▼
    Debugger     │ Diagnoser  Reporter
       │         │   │
       └─────────┴───┘
              │
              ▼
             rerun
```

Bourne should not require one particular graph or agent framework.

Codex, Claude, DeepSeek, or future systems may supply reasoning and tool execution.

Bourne's responsibility is to preserve the scientific history created by those systems.

---

# Agent-Native, Not Agent-Dependent

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
 experiment provenance
           │
           ▼
 scientific memory
```

Today:

> Agents decide what to do. Bourne records what happened.

Long-term:

> Agents decide what to do. Bourne records what happened, preserves how one attempt led to the next, and connects the evidence used to verify the result.

Models and agent frameworks may change.

Scientific history should survive them.

---

# Testing Expectations

At minimum test:

1. successful command
2. failing command
3. stdout capture
4. stderr capture
5. arguments
6. execution outside Git
7. clean Git repository
8. dirty Git repository
9. system without NVIDIA tooling
10. SQLite creation
11. experiment persistence
12. `bourne list`
13. `bourne show`
14. `bourne compare`
15. persistence across independent CLI invocations

Tests must not require:

* GPU hardware
* CUDA
* network connectivity
* cloud services

Use small deterministic fixtures.

---

# Platform Expectations

Initial development may take place on:

```text
Linux
ARM64
NVIDIA GPU
```

Do not assume this is the only environment.

Architecture must remain suitable for:

* Linux x86_64
* Linux ARM64
* macOS Apple Silicon
* eventually Windows where practical

Platform-specific logic should remain isolated.

---

# Engineering Style

Prefer:

* typed Python
* explicit models
* narrow interfaces
* small modules
* deterministic behaviour
* clear errors
* minimal dependencies
* standard library where reasonable
* testability
* portability

Avoid:

* giant manager classes
* unnecessary dependency-injection frameworks
* hidden global mutable state
* excessive metaprogramming
* ML-framework assumptions in core
* dashboard-first development
* speculative distributed architecture
* premature agent abstractions

---

# Naming

Do not rename these without explicit instruction:

```text
Project:       Project Bourne
Distribution:  bourneprov
Package:       bourneprov
CLI:           bourne
```

Intended user experience:

```bash
pip install bourneprov

bourne run ./solver case.yaml

bourne list

bourne show <id>
```

---

# Documentation

Primary tagline:

> **Every experiment has a history.**

Short description:

> **Universal experiment provenance and reproducibility for science and engineering.**

The README should explain the core product quickly.

Show working functionality before discussing ambitious future capabilities.

Do not present future agent, graph, verification, or lineage capabilities as already implemented.

---

# Priority Order

When goals conflict, prioritise:

1. provenance correctness
2. reproducibility
3. framework independence
4. data durability
5. predictable execution semantics
6. simplicity
7. portability
8. testability
9. extensibility
10. developer ergonomics
11. visual polish

---

# Before Declaring Work Complete

Always:

1. run relevant automated tests
2. run real CLI smoke tests where practical
3. inspect at least one persisted experiment
4. test a deliberately failing experiment
5. test operation without optional NVIDIA tooling
6. preserve underlying process semantics
7. report known limitations
8. do not claim unsupported functionality

For substantial tasks report:

* functionality implemented
* architecture decisions
* files changed
* tests executed
* exact test results
* smoke-test results
* known limitations
* intentionally deferred work
* recommended next milestone
