from __future__ import annotations

from dataclasses import replace

from bourneprov.models import ExecutionContext, Experiment, GitProvenance, SystemProvenance


def system_provenance() -> SystemProvenance:
    return SystemProvenance(
        operating_system="TestOS",
        os_version="1.0",
        architecture="test-arch",
        hostname="test-host",
        cpu="Test CPU",
        gpu_available=False,
        gpu_error="nvidia-smi executable not found",
    )


def experiment(**changes: object) -> Experiment:
    base = Experiment(
        id="01H00000000000000000000000",
        status="completed",
        command="solver",
        arguments=["case.yaml", "--steps", "4"],
        working_directory="/work",
        started_at="2026-01-01T00:00:00.000000Z",
        ended_at="2026-01-01T00:00:01.000000Z",
        duration_seconds=1.0,
        exit_code=0,
        stdout="answer=42\n",
        stderr="",
        git=GitProvenance(
            available=True,
            repository_root="/work",
            commit_sha="abc123",
            branch="main",
            dirty=False,
        ),
        system=system_provenance(),
        execution_context=ExecutionContext(
            requested_executable="solver",
            resolved_executable="/usr/bin/solver",
            recorder_executable="/usr/bin/bourne-runtime",
        ),
    )
    return replace(base, **changes)
