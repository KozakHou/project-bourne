"""Declarative constraints and explicitly trusted application-provider seam."""

from __future__ import annotations

import importlib.metadata
import itertools
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .planning_models import ParameterClassification, ResourceShape

PROVIDER_SCHEMA_KIND = "bourne.constraint-provider"
PROVIDER_SCHEMA_VERSION = 1
MAX_PROVIDER_BYTES = 1024 * 1024
MAX_PARAMETERS = 128
MAX_CONSTRAINTS = 256
MAX_VALUES_PER_PARAMETER = 64
ENTRY_POINT_GROUP = "bourneprov.constraint_providers.v1"

_CLASSES = {
    "execution_only", "performance_tunable", "scientific_semantics", "unknown"
}
_PREDICATES = {"equal", "not_equal", "less_than", "less_or_equal", "greater_than", "greater_or_equal", "divisible_by"}
_EXPRESSION_KEYS = {"constant", "parameter", "resource", "add", "multiply", "subtract"}


class ProviderContractError(ValueError):
    pass


@dataclass(frozen=True)
class ParameterDefinition:
    name: str
    classification: ParameterClassification
    classification_evidence: dict[str, Any]
    allowed_values: tuple[Any, ...] = ()
    minimum: int | float | None = None
    maximum: int | float | None = None
    default: Any = None
    binding: dict[str, Any] | None = None
    scientific_equivalence: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ParameterDefinition":
        allowed = {
            "name", "classification", "classification_evidence", "allowed_values",
            "minimum", "maximum", "default", "binding", "scientific_equivalence",
        }
        _reject_extra(value, allowed, "parameter")
        name = value.get("name")
        classification = value.get("classification")
        evidence = value.get("classification_evidence")
        values = value.get("allowed_values", [])
        if not isinstance(name, str) or not name or len(name) > 128:
            raise ProviderContractError("parameter name must be a bounded non-empty string")
        if classification not in _CLASSES:
            raise ProviderContractError(f"unsupported parameter classification: {classification}")
        if not isinstance(evidence, dict):
            raise ProviderContractError("parameter classification requires structured evidence")
        if not isinstance(values, list) or len(values) > MAX_VALUES_PER_PARAMETER:
            raise ProviderContractError("parameter allowed values must be a bounded list")
        minimum = value.get("minimum")
        maximum = value.get("maximum")
        for item, label in ((minimum, "minimum"), (maximum, "maximum")):
            if item is not None and (isinstance(item, bool) or not isinstance(item, (int, float))):
                raise ProviderContractError(f"parameter {label} must be numeric")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ProviderContractError("parameter minimum cannot exceed maximum")
        binding = value.get("binding")
        if binding is not None:
            _validate_binding(binding)
        scientific_equivalence = value.get("scientific_equivalence", False)
        if not isinstance(scientific_equivalence, bool):
            raise ProviderContractError("scientific_equivalence must be boolean")
        return cls(
            name=name,
            classification=classification,
            classification_evidence=dict(evidence),
            allowed_values=tuple(values),
            minimum=minimum,
            maximum=maximum,
            default=value.get("default"),
            binding=None if binding is None else dict(binding),
            scientific_equivalence=scientific_equivalence,
        )

    def candidate_values(self) -> tuple[Any, ...]:
        if self.allowed_values:
            return self.allowed_values
        values: list[Any] = []
        for item in (self.default, self.minimum, self.maximum):
            if item is not None and item not in values:
                values.append(item)
        return tuple(values or [None])


@dataclass(frozen=True)
class ConstraintDefinition:
    id: str
    operator: str
    left: dict[str, Any]
    right: dict[str, Any]
    hard: bool
    message: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ConstraintDefinition":
        _reject_extra(value, {"id", "operator", "left", "right", "hard", "message"}, "constraint")
        if not isinstance(value.get("id"), str) or not value["id"]:
            raise ProviderContractError("constraint ID is required")
        if value.get("operator") not in _PREDICATES:
            raise ProviderContractError(f"unsupported constraint operator: {value.get('operator')}")
        left = value.get("left")
        right = value.get("right")
        _validate_expression(left)
        _validate_expression(right)
        if not isinstance(value.get("hard", True), bool):
            raise ProviderContractError("constraint hard flag must be boolean")
        message = value.get("message", value["id"])
        if not isinstance(message, str) or not message:
            raise ProviderContractError("constraint message must be a non-empty string")
        return cls(
            id=value["id"], operator=value["operator"], left=dict(left),
            right=dict(right), hard=value.get("hard", True), message=message,
        )


@dataclass(frozen=True)
class ConstraintEvaluation:
    constraint_id: str
    state: str
    hard: bool
    message: str
    left: Any = None
    right: Any = None


@dataclass(frozen=True)
class DeclarativeConstraintProvider:
    name: str
    provider_version: str
    parameters: tuple[ParameterDefinition, ...]
    constraints: tuple[ConstraintDefinition, ...]
    environment_requirements: tuple[dict[str, Any], ...]
    launcher_requirements: tuple[dict[str, Any], ...]
    source_digest: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeclarativeConstraintProvider":
        allowed = {
            "kind", "schema_version", "name", "provider_version", "parameters",
            "constraints", "environment_requirements", "launcher_requirements",
            "source_digest",
        }
        _reject_extra(value, allowed, "provider")
        if value.get("kind") != PROVIDER_SCHEMA_KIND:
            raise ProviderContractError(f"provider kind must be {PROVIDER_SCHEMA_KIND}")
        if value.get("schema_version") != PROVIDER_SCHEMA_VERSION:
            raise ProviderContractError("unsupported declarative provider schema version")
        name = value.get("name")
        version = value.get("provider_version")
        if not isinstance(name, str) or not name or len(name) > 128:
            raise ProviderContractError("provider name is required")
        if not isinstance(version, str) or not version or len(version) > 64:
            raise ProviderContractError("provider version is required")
        raw_parameters = value.get("parameters", [])
        raw_constraints = value.get("constraints", [])
        if not isinstance(raw_parameters, list) or len(raw_parameters) > MAX_PARAMETERS:
            raise ProviderContractError("provider parameters must be a bounded list")
        if not isinstance(raw_constraints, list) or len(raw_constraints) > MAX_CONSTRAINTS:
            raise ProviderContractError("provider constraints must be a bounded list")
        parameters = tuple(ParameterDefinition.from_dict(item) for item in raw_parameters)
        if len({item.name for item in parameters}) != len(parameters):
            raise ProviderContractError("provider parameter names must be unique")
        constraints = tuple(ConstraintDefinition.from_dict(item) for item in raw_constraints)
        if len({item.id for item in constraints}) != len(constraints):
            raise ProviderContractError("provider constraint IDs must be unique")
        environments = _requirements(value.get("environment_requirements", []), "environment")
        launchers = _requirements(value.get("launcher_requirements", []), "launcher")
        source_digest = value.get("source_digest")
        if source_digest is not None and not (
            isinstance(source_digest, str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", source_digest)
        ):
            raise ProviderContractError(
                "provider source digest must use canonical sha256:<hex> form"
            )
        parameter_names = {item.name for item in parameters}
        for constraint in constraints:
            referenced = _expression_parameters(constraint.left) | _expression_parameters(constraint.right)
            missing = referenced - parameter_names
            if missing:
                raise ProviderContractError(
                    f"constraint references unknown parameters: {', '.join(sorted(missing))}"
                )
        return cls(
            name=name, provider_version=version, parameters=parameters,
            constraints=constraints, environment_requirements=environments,
            launcher_requirements=launchers, source_digest=source_digest,
        )

    @classmethod
    def load(cls, path: Path) -> "DeclarativeConstraintProvider":
        raw = path.read_bytes()
        if len(raw) > MAX_PROVIDER_BYTES:
            raise ProviderContractError("declarative provider exceeds the size limit")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProviderContractError(f"provider is not valid UTF-8 JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise ProviderContractError("provider document must be a JSON object")
        return cls.from_dict(value)

    def parameter_assignments(self, limit: int) -> tuple[list[dict[str, Any]], int]:
        if limit < 1:
            raise ValueError("candidate limit must be positive")
        value_sets = [item.candidate_values() for item in self.parameters]
        theoretical = 1
        for values in value_sets:
            theoretical *= len(values)
        result: list[dict[str, Any]] = []
        for values in itertools.islice(itertools.product(*value_sets), limit):
            result.append({item.name: value for item, value in zip(self.parameters, values)})
        return result, theoretical

    def evaluate(
        self, parameters: Mapping[str, Any], resource_shape: ResourceShape
    ) -> list[ConstraintEvaluation]:
        results: list[ConstraintEvaluation] = []
        for constraint in self.constraints:
            left = _evaluate_expression(constraint.left, parameters, resource_shape)
            right = _evaluate_expression(constraint.right, parameters, resource_shape)
            if left is _UNKNOWN or right is _UNKNOWN:
                state = "unknown"
            else:
                try:
                    state = "satisfied" if _compare(constraint.operator, left, right) else "violated"
                except (TypeError, ValueError, ZeroDivisionError):
                    state = "unknown"
            results.append(
                ConstraintEvaluation(
                    constraint.id, state, constraint.hard, constraint.message,
                    None if left is _UNKNOWN else left,
                    None if right is _UNKNOWN else right,
                )
            )
        return results

    def parameter(self, name: str) -> ParameterDefinition:
        try:
            return next(item for item in self.parameters if item.name == name)
        except StopIteration:
            raise ProviderContractError(f"unknown provider parameter: {name}") from None

    def resource_value_hints(self, limit: int = 64) -> dict[str, tuple[int, ...]]:
        """Derive bounded concrete resource values from hard equality contracts.

        These are candidate-generation hints, not authorization.  Only an equality
        with exactly one direct resource reference and a resource-independent peer
        expression can contribute a value.
        """

        assignments, _ = self.parameter_assignments(limit)
        hints: dict[str, set[int]] = {}
        empty_shape = ResourceShape()
        for constraint in self.constraints:
            if not constraint.hard or constraint.operator != "equal":
                continue
            for resource_expression, value_expression in (
                (constraint.left, constraint.right),
                (constraint.right, constraint.left),
            ):
                resource_name = _direct_resource(resource_expression)
                if resource_name is None or _expression_resources(value_expression):
                    continue
                for assignment in assignments:
                    value = _evaluate_expression(value_expression, assignment, empty_shape)
                    if (
                        value is not _UNKNOWN
                        and isinstance(value, int)
                        and not isinstance(value, bool)
                        and (value > 0 or resource_name in {"gpus", "gpus_per_node"})
                    ):
                        hints.setdefault(resource_name, set()).add(value)
        return {
            name: tuple(sorted(values))
            for name, values in sorted(hints.items())
        }


class TrustedCodeProvider(Protocol):
    """Version-1 trusted extension contract; implementations are not sandboxed."""

    api_version: int
    name: str

    def inspect(self, inputs: Sequence[Path]) -> Mapping[str, Any]: ...

    def extract_constraints(self, inspection: Mapping[str, Any]) -> DeclarativeConstraintProvider: ...

    def validate_variant(self, changes: Mapping[str, Any]) -> Sequence[str]: ...


class TrustedProviderRegistry:
    """Load only installed entry points named by an explicit allow-list."""

    def __init__(self, enabled: Sequence[str] = ()):
        self.enabled = frozenset(enabled)

    def available(self) -> list[str]:
        return sorted(
            entry.name for entry in importlib.metadata.entry_points().select(group=ENTRY_POINT_GROUP)
        )

    def load(self, name: str) -> TrustedCodeProvider:
        if name not in self.enabled:
            raise ProviderContractError(
                f"trusted code provider '{name}' is not explicitly enabled"
            )
        matches = [
            item for item in importlib.metadata.entry_points().select(group=ENTRY_POINT_GROUP)
            if item.name == name
        ]
        if len(matches) != 1:
            raise ProviderContractError(
                f"trusted code provider '{name}' is unavailable or ambiguous"
            )
        provider = matches[0].load()()
        if getattr(provider, "api_version", None) != 1:
            raise ProviderContractError("trusted code provider API version is incompatible")
        return provider


def automatic_change_allowed(
    parameter: ParameterDefinition,
    *,
    explicit_user_declaration: bool = False,
    explicit_change_approval: bool = False,
    trusted_provider_contract: bool = False,
) -> bool:
    """Enforce semantic safety without changing a parameter's classification."""

    evidence_kind = parameter.classification_evidence.get("kind")
    if explicit_change_approval:
        return True
    if parameter.classification == "execution_only":
        return (
            explicit_user_declaration
            or (
                trusted_provider_contract
                and evidence_kind in {
                    "provider_contract", "machine_contract", "user_declared"
                }
            )
        )
    if parameter.classification == "performance_tunable":
        return (
            parameter.scientific_equivalence
            and trusted_provider_contract
            and evidence_kind in {
                "provider_contract", "machine_contract", "user_declared"
            }
        )
    return False


def ensure_remote_provider_available(
    name: str,
    *,
    declarative: bool,
    available_authorized_code_providers: Sequence[str] = (),
) -> None:
    """Declarative contracts may travel; trusted code must already be authorized."""

    if declarative:
        return
    if name not in set(available_authorized_code_providers):
        raise ProviderContractError(
            f"required trusted code provider '{name}' is unavailable in the remote context; "
            "Bourne did not install or guess a replacement"
        )


def provider_schema() -> dict[str, Any]:
    """Return the bundled JSON Schema used for review/editor validation."""

    path = Path(__file__).resolve().parent / "schemas" / "constraint-provider-v1.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


class _Unknown:
    pass


_UNKNOWN = _Unknown()


def _validate_expression(value: Any, depth: int = 0) -> None:
    if depth > 12 or not isinstance(value, dict) or len(value) != 1:
        raise ProviderContractError("constraint expression must be a bounded typed object")
    key, operand = next(iter(value.items()))
    if key not in _EXPRESSION_KEYS:
        raise ProviderContractError(f"unsupported expression operator: {key}")
    if key == "constant":
        if isinstance(operand, (dict, list)):
            raise ProviderContractError("constraint constants must be scalar")
    elif key in {"parameter", "resource"}:
        if not isinstance(operand, str) or not operand:
            raise ProviderContractError(f"{key} reference must be a string")
        if key == "resource":
            ResourceShape().value(operand)
    elif key == "subtract":
        if not isinstance(operand, list) or len(operand) != 2:
            raise ProviderContractError("subtract requires exactly two operands")
        for item in operand:
            _validate_expression(item, depth + 1)
    else:
        if not isinstance(operand, list) or not 2 <= len(operand) <= 16:
            raise ProviderContractError(f"{key} requires two to sixteen operands")
        for item in operand:
            _validate_expression(item, depth + 1)


def _evaluate_expression(
    value: dict[str, Any], parameters: Mapping[str, Any], shape: ResourceShape
) -> Any:
    key, operand = next(iter(value.items()))
    if key == "constant":
        return operand
    if key == "parameter":
        result = parameters.get(operand, _UNKNOWN)
        return _UNKNOWN if result is None else result
    if key == "resource":
        result = shape.value(operand)
        return _UNKNOWN if result is None else result
    values = [_evaluate_expression(item, parameters, shape) for item in operand]
    if any(item is _UNKNOWN for item in values):
        return _UNKNOWN
    if key == "add":
        return sum(values)
    if key == "multiply":
        result: Any = 1
        for item in values:
            result *= item
        return result
    return values[0] - values[1]


def _compare(operator: str, left: Any, right: Any) -> bool:
    if operator == "equal": return left == right
    if operator == "not_equal": return left != right
    if operator == "less_than": return left < right
    if operator == "less_or_equal": return left <= right
    if operator == "greater_than": return left > right
    if operator == "greater_or_equal": return left >= right
    if operator == "divisible_by": return right != 0 and left % right == 0
    raise ProviderContractError(f"unsupported comparison: {operator}")


def _expression_parameters(value: dict[str, Any]) -> set[str]:
    key, operand = next(iter(value.items()))
    if key == "parameter":
        return {operand}
    if key in {"add", "multiply", "subtract"}:
        result: set[str] = set()
        for item in operand:
            result.update(_expression_parameters(item))
        return result
    return set()


def _expression_resources(value: dict[str, Any]) -> set[str]:
    key, operand = next(iter(value.items()))
    if key == "resource":
        return {operand}
    if key in {"add", "multiply", "subtract"}:
        result: set[str] = set()
        for item in operand:
            result.update(_expression_resources(item))
        return result
    return set()


def _direct_resource(value: dict[str, Any]) -> str | None:
    key, operand = next(iter(value.items()))
    return operand if key == "resource" else None


def _validate_binding(value: Any) -> None:
    if not isinstance(value, dict):
        raise ProviderContractError("variant binding must be an object")
    _reject_extra(value, {"kind", "input", "path"}, "variant binding")
    if value.get("kind") != "json_path":
        raise ProviderContractError("only safe json_path bindings are supported")
    if not isinstance(value.get("input"), str) or not value["input"]:
        raise ProviderContractError("variant binding input is required")
    path = value.get("path")
    if (
        not isinstance(path, list) or not path or len(path) > 32
        or not all(isinstance(item, (str, int)) and not isinstance(item, bool) for item in path)
    ):
        raise ProviderContractError("variant binding path must be a bounded key/index list")


def _requirements(value: Any, label: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list) or len(value) > 128:
        raise ProviderContractError(f"{label} requirements must be a bounded list")
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ProviderContractError(f"{label} requirement must be an object")
        _reject_extra(item, {"kind", "name", "version", "required"}, f"{label} requirement")
        if not isinstance(item.get("kind"), str) or not isinstance(item.get("name"), str):
            raise ProviderContractError(f"{label} requirement kind and name are required")
        result.append(dict(item))
    return tuple(result)


def _reject_extra(value: dict[str, Any], allowed: set[str], label: str) -> None:
    if not isinstance(value, dict):
        raise ProviderContractError(f"{label} must be an object")
    extra = set(value) - allowed
    if extra:
        raise ProviderContractError(f"unsupported {label} fields: {', '.join(sorted(extra))}")
