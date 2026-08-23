"""Exact-argv OpenSSH transport for typed, one-shot Bourne operations."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Protocol

from . import __version__
from .bounded_subprocess import BoundedCommandResult, run_bounded_command
from .site_models import Site
from .worker_bundle import build_remote_worker_zipapp
from .worker_result import MAX_RESULT_BUNDLE_BYTES

REMOTE_PROTOCOL = "bourne.remote-worker"
REMOTE_PROTOCOL_VERSION = 1
MAX_REMOTE_REQUEST_BYTES = 1024 * 1024
MAX_REMOTE_RESPONSE_BYTES = MAX_RESULT_BUNDLE_BYTES + 1024 * 1024
REMOTE_TIMEOUT_SECONDS = 30.0

_OPERATION = re.compile(
    r"(?:hello|discover|validate_plan|prepare|submit|reconcile|collect|cancel)\Z"
)
_REMOTE_TOKEN = re.compile(r"[A-Za-z0-9_~./+@:-]+\Z")


class RemoteTransportError(RuntimeError):
    pass


class RemoteProtocolError(RemoteTransportError):
    pass


@dataclass(frozen=True)
class RemoteResponse:
    operation: str
    status: str
    data: dict[str, Any]


class RemoteTransport(Protocol):
    def ensure_worker(self, site: Site) -> str: ...

    def invoke(
        self, site: Site, worker_path: str, operation: str, payload: dict[str, Any]
    ) -> RemoteResponse: ...

    def upload(self, site: Site, local_path: Path, remote_path: str) -> None: ...


CommandRunner = Callable[..., BoundedCommandResult]


class OpenSSHTransport:
    """Use the user's OpenSSH config/agent without handling credentials."""

    def __init__(self, *, runner: CommandRunner = run_bounded_command):
        self.runner = runner

    def ensure_worker(self, site: Site) -> str:
        _require_remote_site(site)
        if site.remote_worker_path is not None:
            try:
                hello = self.invoke(site, site.remote_worker_path, "hello", {})
            except RemoteTransportError:
                hello = None
            if (
                hello is not None
                and hello.status == "ok"
                and hello.data.get("compatible") is True
                and hello.data.get("bourne_version") == __version__
            ):
                return site.remote_worker_path
        with tempfile.TemporaryDirectory(prefix="bourne-remote-worker-") as raw:
            local = build_remote_worker_zipapp(Path(raw) / "remote-worker.pyz")
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            cache = (
                PurePosixPath(site.remote_project_root) / ".bourne" / "workers"
                if site.remote_project_root is not None
                else PurePosixPath(self._bootstrap_default_directory(site))
            )
            remote = str(cache / f"bourne-{__version__}-{digest}.pyz")
            self._bootstrap_directory(site, str(cache))
            self.upload(site, local, remote)
            self._make_remote_executable(site, remote)
        hello = self.invoke(
            site, remote, "hello",
            {"expected_version": __version__, "expected_sha256": digest},
        )
        if (
            hello.status != "ok"
            or hello.data.get("compatible") is not True
            or hello.data.get("bourne_version") != __version__
            or hello.data.get("worker_sha256") != digest
        ):
            raise RemoteTransportError("remote worker version/digest verification failed")
        return remote

    def invoke(
        self, site: Site, worker_path: str, operation: str, payload: dict[str, Any]
    ) -> RemoteResponse:
        _require_remote_site(site)
        if not _OPERATION.fullmatch(operation):
            raise RemoteProtocolError(f"unsupported typed remote operation: {operation}")
        _validate_remote_token(worker_path, "remote worker path")
        envelope = {
            "protocol": REMOTE_PROTOCOL,
            "protocol_version": REMOTE_PROTOCOL_VERSION,
            "operation": operation,
            "payload": payload,
        }
        raw = json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(raw) > MAX_REMOTE_REQUEST_BYTES:
            raise RemoteProtocolError("remote request exceeds the size limit")
        argv = [*self._ssh_prefix(site), worker_path, "_remote", operation]
        result = self._run(argv, input_bytes=raw)
        if result.timed_out:
            raise RemoteTransportError("SSH remote operation timed out")
        if result.truncated:
            raise RemoteTransportError("SSH remote operation output exceeded its bound")
        if result.returncode != 0:
            detail = result.stderr.strip()[:4096] or f"exit {result.returncode}"
            raise RemoteTransportError(f"SSH remote operation failed: {detail}")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RemoteProtocolError("remote worker returned invalid JSON") from exc
        if not isinstance(value, dict) or value.get("protocol") != REMOTE_PROTOCOL:
            raise RemoteProtocolError("remote worker response protocol is invalid")
        if value.get("protocol_version") != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("remote worker protocol version is incompatible")
        if value.get("operation") != operation or not isinstance(value.get("data"), dict):
            raise RemoteProtocolError("remote worker response does not match the request")
        status = value.get("status")
        if status not in {"ok", "unavailable", "failed", "ambiguous", "unknown"}:
            raise RemoteProtocolError("remote worker returned an invalid status")
        return RemoteResponse(operation, status, dict(value["data"]))

    def upload(self, site: Site, local_path: Path, remote_path: str) -> None:
        _require_remote_site(site)
        if not local_path.is_file():
            raise RemoteTransportError(f"upload source is unavailable: {local_path}")
        _validate_remote_token(remote_path, "remote upload path")
        scp = shutil.which("scp")
        if scp is None:
            raise RemoteTransportError("scp executable is unavailable")
        argv = [scp]
        if site.ssh_port is not None:
            argv.extend(["-P", str(site.ssh_port)])
        argv.extend(["--", str(local_path), f"{site.ssh_destination}:{remote_path}"])
        result = self._run(argv)
        if result.returncode != 0 or result.timed_out or result.truncated:
            detail = result.stderr.strip()[:4096] or "upload failed"
            raise RemoteTransportError(f"worker upload failed: {detail}")

    def _bootstrap_directory(self, site: Site, directory: str) -> None:
        _validate_remote_token(directory, "remote cache directory")
        # Fixed Bourne-owned bootstrap program. No user/agent program text is
        # accepted; it only creates this validated user-space directory.
        program = (
            "import pathlib, sys\n"
            "pathlib.Path(sys.argv[1]).mkdir(mode=0o700, parents=True, exist_ok=True)\n"
        )
        # OpenSSH sends a remote command string rather than an argv protocol.
        # Keep that command to safe tokens and send Bourne's fixed program on
        # stdin so quoting cannot change its meaning.
        argv = [*self._ssh_prefix(site), "python3", "-", directory]
        result = self._run(argv, input_bytes=program.encode("utf-8"))
        if result.returncode != 0 or result.timed_out or result.truncated:
            raise RemoteTransportError("could not prepare the remote user-space cache")

    def _bootstrap_default_directory(self, site: Site) -> str:
        program = (
            "import pathlib\n"
            "path = pathlib.Path.home() / '.cache' / 'bourne' / 'workers'\n"
            "path.mkdir(mode=0o700, parents=True, exist_ok=True)\n"
            "print(path)\n"
        )
        result = self._run(
            [*self._ssh_prefix(site), "python3", "-"],
            input_bytes=program.encode("utf-8"),
        )
        value = result.stdout.strip()
        if (
            result.returncode != 0 or result.timed_out or result.truncated
            or not value.startswith("/")
        ):
            raise RemoteTransportError("could not resolve the remote user-space cache")
        _validate_remote_token(value, "remote cache directory")
        return value

    def _make_remote_executable(self, site: Site, worker_path: str) -> None:
        _validate_remote_token(worker_path, "remote worker path")
        result = self._run(
            [*self._ssh_prefix(site), "chmod", "700", "--", worker_path]
        )
        if result.returncode != 0 or result.timed_out or result.truncated:
            raise RemoteTransportError("could not activate the staged remote worker")

    def _ssh_prefix(self, site: Site) -> list[str]:
        ssh = shutil.which("ssh")
        if ssh is None:
            raise RemoteTransportError("ssh executable is unavailable")
        argv = [ssh]
        if site.ssh_port is not None:
            argv.extend(["-p", str(site.ssh_port)])
        # Deliberately no StrictHostKeyChecking/UserKnownHostsFile/identity flags:
        # OpenSSH's configured trust and authentication behavior remains intact.
        argv.extend(["--", site.ssh_destination])
        return argv

    def _run(
        self, argv: list[str], *, input_bytes: bytes | None = None
    ) -> BoundedCommandResult:
        try:
            return self.runner(
                argv, timeout=REMOTE_TIMEOUT_SECONDS,
                max_output_bytes=MAX_REMOTE_RESPONSE_BYTES,
                input_bytes=input_bytes,
                shell=False,
            )
        except OSError as exc:
            raise RemoteTransportError(f"could not run OpenSSH tooling: {exc}") from exc


class RemoteWorkerClient:
    def __init__(self, site: Site, transport: RemoteTransport):
        _require_remote_site(site)
        self.site = site
        self.transport = transport
        self._worker_path: str | None = None

    @property
    def worker_path(self) -> str:
        if self._worker_path is None:
            self._worker_path = self.transport.ensure_worker(self.site)
        return self._worker_path

    def call(self, operation: str, payload: dict[str, Any]) -> RemoteResponse:
        return self.transport.invoke(self.site, self.worker_path, operation, payload)

    def upload(self, local_path: Path, remote_path: str) -> None:
        self.transport.upload(self.site, local_path, remote_path)


def _require_remote_site(site: Site) -> None:
    if site.kind != "remote_ssh":
        raise RemoteTransportError("SSH transport requires a remote_ssh site")
    if site.ssh_host is None or site.ssh_host.startswith("-"):
        raise RemoteTransportError("SSH host/alias is invalid")
    _validate_remote_token(site.ssh_host, "SSH host/alias")
    if site.ssh_username is not None:
        _validate_remote_token(site.ssh_username, "SSH username")


def _validate_remote_token(value: str, label: str) -> None:
    if not _REMOTE_TOKEN.fullmatch(value) or value.startswith("-"):
        raise RemoteTransportError(f"{label} contains unsafe characters")
