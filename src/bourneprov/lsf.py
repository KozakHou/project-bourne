"""Bounded IBM LSF job identity, state, and history helpers."""

from __future__ import annotations

import re

from .bounded_subprocess import BoundedCommandResult

SAFE_LSF_JOB = re.compile(r"[0-9]+\Z")

LSF_TERMINAL_STATES = frozenset(
    {
        "completed",
        "failed",
        "post_processing_completed",
        "post_processing_failed",
    }
)


def active_job_arguments(job_id: str, identity: str) -> list[str]:
    _validate_identity(job_id, identity)
    return ["-noheader", "-u", identity, "-o", "jobid stat", job_id]


def recent_job_arguments(job_id: str, identity: str) -> list[str]:
    return ["-a", *active_job_arguments(job_id, identity)]


def historical_job_arguments(job_id: str, identity: str) -> list[str]:
    """Search all bounded LSF event logs for one exact recorded job."""

    _validate_identity(job_id, identity)
    return ["-l", "-n", "0", "-u", identity, job_id]


def parse_bjobs_job(
    stdout: str, *, expected_job_id: str
) -> tuple[str, str] | None:
    if not SAFE_LSF_JOB.fullmatch(expected_job_id):
        return None
    rows = [line.split() for line in stdout.splitlines() if line.strip()]
    if (
        len(rows) != 1
        or len(rows[0]) != 2
        or rows[0][0] != expected_job_id
        or not SAFE_LSF_JOB.fullmatch(rows[0][0])
    ):
        return None
    return rows[0][0], rows[0][1].upper()


def parse_bhist_job(
    stdout: str, *, expected_job_id: str
) -> tuple[str, str] | None:
    """Parse one exact ``bhist -l`` record without accepting unrelated jobs."""

    if not SAFE_LSF_JOB.fullmatch(expected_job_id):
        return None
    headers = list(re.finditer(r"\bJob\s+<([0-9]+)>", stdout))
    if len(headers) != 1 or headers[0].group(1) != expected_job_id:
        return None
    body = stdout[headers[0].end() :]
    statuses = re.findall(r"\bStatus\s+<([A-Za-z_]+)>", body)
    if len(statuses) != 1:
        return None
    return expected_job_id, statuses[0].upper()


def normalize_lsf_state(value: str) -> str:
    return {
        "PEND": "pending",
        "WAIT": "pending",
        "PROV": "pending",
        "RUN": "running",
        "PSUSP": "suspended",
        "USUSP": "suspended",
        "SSUSP": "suspended",
        "DONE": "completed",
        "EXIT": "failed",
        "POST_DONE": "post_processing_completed",
        "POST_ERR": "post_processing_failed",
        "ZOMBI": "scheduler_uncertain",
        "UNKWN": "scheduler_uncertain",
    }.get(value.strip().upper(), "unknown")


def lsf_job_is_unobservable(result: BoundedCommandResult) -> bool:
    diagnostic = f"{result.stderr}\n{result.stdout}".casefold()
    return bool(
        re.search(
            r"job(?:\s+<[^>]+>)?\s+(?:is\s+not\s+found|not\s+found)"
            r"|no\s+(?:unfinished\s+)?job\s+found"
            r"|not\s+found\s+in\s+job\s+list",
            diagnostic,
        )
    )


def _validate_identity(job_id: str, identity: str) -> None:
    if not SAFE_LSF_JOB.fullmatch(job_id):
        raise ValueError("LSF job identity is invalid")
    if (
        not isinstance(identity, str)
        or not identity
        or len(identity) > 256
        or any(character in identity for character in "\0\r\n")
    ):
        raise ValueError("LSF submitting identity is invalid")
