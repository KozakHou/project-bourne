"""Reusable artifact-to-experiment provenance tracing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .artifacts import resolve_artifact_path, sha256_file
from .models import Artifact, Experiment
from .storage import ExperimentStore


class ArtifactTraceError(LookupError):
    """Base class for artifact references that cannot be traced reliably."""


class MissingArtifactReference(ArtifactTraceError):
    def __init__(self, path: str):
        self.path = path
        super().__init__(path)

    def __str__(self) -> str:
        return f"No recorded output artifact matches '{self.path}'."


class AmbiguousArtifactReference(ArtifactTraceError):
    def __init__(self, path: str, matches: list[Artifact]):
        self.path = path
        self.matches = matches
        super().__init__(path)

    def __str__(self) -> str:
        candidates = "\n".join(
            f"  artifact {item.id} from experiment {item.experiment_id} "
            f"(sha256 {item.sha256 or 'unavailable'})"
            for item in self.matches
        )
        return (
            f"Artifact reference '{self.path}' is ambiguous.\n\n"
            f"Matches:\n{candidates}\n\n"
            "Provide a file whose current content identifies the intended version."
        )


@dataclass(frozen=True)
class ArtifactTrace:
    artifact: Artifact
    producer: Experiment
    inputs: list[Artifact]
    ancestry: list[Experiment]


def trace_artifact(store: ExperimentStore, path: str, cwd: Path | None = None) -> ArtifactTrace:
    """Trace a current content version, falling back to an unambiguous stored path."""

    working_directory = (cwd or Path.cwd()).resolve()
    resolved = resolve_artifact_path(path, working_directory)
    digest: str | None = None
    try:
        if resolved.is_file():
            digest = sha256_file(resolved)
    except OSError:
        pass

    if digest is not None:
        matches = store.find_output_artifacts(
            resolved_path=str(resolved), sha256=digest
        )
        if not matches:
            # Content identity remains useful if the output has been moved or copied.
            matches = store.find_output_artifacts(sha256=digest)
    else:
        matches = store.find_output_artifacts(resolved_path=str(resolved))

    if not matches:
        raise MissingArtifactReference(path)
    if len(matches) > 1:
        raise AmbiguousArtifactReference(path, matches)

    artifact = matches[0]
    producer = store.get(artifact.experiment_id)
    inputs = store.list_artifacts(producer.id, role="input")
    ancestry: list[Experiment] = []
    seen = {producer.id}
    current = producer.id
    while lineage := store.get_lineage(current):
        if lineage.parent_experiment_id in seen:
            break
        parent = store.get(lineage.parent_experiment_id)
        ancestry.append(parent)
        seen.add(parent.id)
        current = parent.id

    return ArtifactTrace(
        artifact=artifact,
        producer=producer,
        inputs=inputs,
        ancestry=ancestry,
    )
