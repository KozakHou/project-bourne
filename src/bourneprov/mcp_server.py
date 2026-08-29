"""Official MCP SDK adapter for Project Bourne's structured agent service."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from functools import partial
from typing import Annotated, Any, Literal

import anyio
from mcp.server import MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field, WithJsonSchema

from . import __version__
from .agent_interface import AgentInterfaceError, BourneAgentService
from .execution_request import execution_request_schema
from .constraint_providers import provider_schema

SERVER_NAME = "project-bourne"
SERVER_TITLE = "Project Bourne"
SERVER_INSTRUCTIONS = (
    "Bourne plans and executes scientific workloads while preserving provenance. "
    "Prefer planning before execution. Preserve unknown infrastructure facts as "
    "unknown, and do not bypass Bourne by constructing scheduler commands."
)
_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _request_input_schema() -> dict[str, Any]:
    """Inline the one local reference so Pydantic can embed the canonical schema."""

    schema = execution_request_schema()
    definition = schema["$defs"]["verificationCheck"]

    def replace(value: Any) -> Any:
        if value == {"$ref": "#/$defs/verificationCheck"}:
            return definition
        if isinstance(value, dict):
            return {
                key: replace(item)
                for key, item in value.items()
                if key not in {"$schema", "$id", "$defs"}
            }
        if isinstance(value, list):
            return [replace(item) for item in value]
        return value

    return replace(schema)


ExecutionRequestDocument = Annotated[
    dict[str, Any], WithJsonSchema(_request_input_schema())
]


def _provider_input_schema() -> dict[str, Any]:
    """Expose a bounded MCP shape while Core performs full recursive validation."""

    def simplify(value: Any) -> Any:
        if isinstance(value, dict):
            if "$ref" in value:
                return {"type": "object"}
            return {
                key: simplify(item)
                for key, item in value.items()
                if key not in {"$schema", "$id", "$defs"}
            }
        if isinstance(value, list):
            return [simplify(item) for item in value]
        return value

    return simplify(provider_schema())


DeclarativeProviderDocument = Annotated[
    dict[str, Any],
    WithJsonSchema(_provider_input_schema()),
    Field(
        description=(
            "Optional bounded declarative constraints used to generate site-aware "
            "candidates; the document cannot execute code or grant itself trust."
        )
    ),
]

BoundedPolicyValue = bool | int | float | Annotated[str, Field(max_length=4096)]
SiteReference = Annotated[
    str,
    Field(description="Exact configured site name or canonical site ID."),
]
InventoryReference = Annotated[
    str,
    Field(
        description=(
            "Existing inventory reference: latest, canonical ID, unique ID prefix, "
            "or @N."
        )
    ),
]
CandidateRequestID = Annotated[
    str,
    Field(
        description=(
            "Request ID returned by bourne_site_candidates in the current live MCP "
            "server session."
        )
    ),
]
CandidateID = Annotated[
    str,
    Field(
        description=(
            "Viable candidate ID returned for request_id by bourne_site_candidates."
        )
    ),
]
SelectionSource = Annotated[
    str,
    Field(
        description=(
            "Provenance label identifying the human, agent, or deterministic rule "
            "that made the selection; this label does not grant authority."
        )
    ),
]
SelectionRationale = Annotated[
    str | None,
    Field(
        description=(
            "Optional explanation stored with the selection; Bourne does not treat "
            "the rationale itself as verification evidence."
        )
    ),
]
VariantApprovals = Annotated[
    list[str] | None,
    Field(
        description=(
            "Provider-bound parameter names whose candidate value changes the user "
            "explicitly approved."
        )
    ),
]
ExecutionOnlyDeclarations = Annotated[
    list[str] | None,
    Field(
        description=(
            "Parameter names the user explicitly declared to affect execution only, "
            "rather than scientific meaning."
        )
    ),
]
ProviderTrustDecision = Annotated[
    bool,
    Field(
        description=(
            "Explicitly trust the declarative provider's semantic classifications; "
            "the provider cannot set this decision for itself."
        )
    ),
]


class PolicyApplicabilityDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope: Annotated[
        Literal[
            "global", "scheduler_class", "queue", "partition", "node_class", "account"
        ],
        Field(
            description=(
                "Site-policy scope. Global applies everywhere; other values restrict "
                "the claim to one scheduler, queue, partition, node class, or account."
            )
        ),
    ] = "global"
    value: Annotated[
        str | None,
        Field(
            min_length=1,
            max_length=256,
            description=(
                "Exact scope value when scope is not global; omit it for global claims."
            ),
        ),
    ] = None


class SitePolicyClaimDocument(BaseModel):
    """Bounded structured policy evidence; never a shell or source-document body."""

    model_config = ConfigDict(extra="forbid")

    subject: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Entity or site capability that the policy claim describes.",
        ),
    ]
    property: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description="Bounded property asserted about the subject.",
        ),
    ]
    value: Annotated[
        BoundedPolicyValue,
        Field(description="Boolean, numeric, or bounded string value being asserted."),
    ]
    evidence_kind: Annotated[
        Literal[
            "observed_now",
            "site_declared",
            "user_declared",
            "historical",
            "inferred",
            "unknown",
        ],
        Field(description="Provenance classification for how the claim was obtained."),
    ]
    interpretation_status: Annotated[
        Literal["hard_constraint", "advisory", "unresolved"],
        Field(
            description=(
                "Whether planning must enforce the claim, may use it as advice, or "
                "must preserve it as unresolved."
            )
        ),
    ]
    source_identity: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Non-secret identity of the person, system, or document that supplied "
                "the claim."
            ),
        ),
    ]
    applicability: PolicyApplicabilityDocument = Field(
        default_factory=PolicyApplicabilityDocument,
        description="Scope that determines which candidate resource shapes use the claim.",
    )
    source_identifier: Annotated[
        str | None,
        Field(
            max_length=2048,
            description="Optional stable identifier for the provenance source.",
        ),
    ] = None
    source_url: Annotated[
        str | None,
        Field(
            max_length=2048,
            description=(
                "Optional provenance URL stored as text only; Bourne does not fetch it."
            ),
        ),
    ] = None
    retrieved_at: Annotated[
        str | None,
        Field(
            max_length=128,
            description="Optional source-retrieval timestamp supplied by the caller.",
        ),
    ] = None
    document_date: Annotated[
        str | None,
        Field(
            max_length=128,
            description="Optional date stated by the provenance source.",
        ),
    ] = None
    content_digest: Annotated[
        str | None,
        Field(
            pattern=r"^sha256:[0-9a-f]{64}$",
            description=(
                "Optional sha256:<hex> digest of source content that remains outside "
                "Bourne."
            ),
        ),
    ] = None


class ContainerMountDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Annotated[
        str,
        Field(
            min_length=1,
            max_length=16384,
            description="Existing host path to bind into the scientific container.",
        ),
    ]
    destination: Annotated[
        str,
        Field(
            min_length=1,
            max_length=16384,
            description="Absolute path where the source is mounted in the container.",
        ),
    ]
    read_only: bool = Field(
        default=True,
        description="Whether the bind mount must be read-only; defaults to true.",
    )


class ContainerExecutionDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: Annotated[
        Literal["apptainer", "singularity"],
        Field(description="Existing container runtime that the selected site will use."),
    ]
    image: Annotated[
        str,
        Field(
            min_length=1,
            max_length=16384,
            description=(
                "Existing image path; Bourne verifies it but never builds or pulls it."
            ),
        ),
    ]
    mounts: list[ContainerMountDocument] = Field(
        default_factory=list,
        max_length=128,
        description="Explicit bind mounts applied when the immutable plan executes.",
    )
    clean_environment: bool = Field(
        default=True,
        description="Request a clean container environment when the runtime supports it.",
    )
    image_digest: Annotated[
        str | None,
        Field(
            pattern=r"^sha256:[0-9a-f]{64}$",
            description="Optional expected sha256:<hex> digest for the existing image.",
        ),
    ] = None


class ProductError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    details: dict[str, Any]


class ToolResult(BaseModel):
    """Stable output envelope used by every Bourne MCP tool."""

    model_config = ConfigDict(extra="forbid")

    ok: bool
    data: dict[str, Any] | None = None
    error: ProductError | None = None


def _annotation(
    *,
    read_only: bool,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
) -> ToolAnnotations:
    return ToolAnnotations(
        read_only_hint=read_only,
        destructive_hint=destructive,
        idempotent_hint=idempotent,
        open_world_hint=open_world,
    )


def _logger() -> logging.Logger:
    requested = os.environ.get("BOURNE_MCP_LOG_LEVEL", "WARNING").upper()
    level = requested if requested in _LOG_LEVELS else "WARNING"
    logger = logging.getLogger("bourneprov.mcp")
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("bourne mcp: %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def create_mcp_server(
    service: BourneAgentService | None = None,
) -> MCPServer[Any]:
    """Create the canonical stdio server without starting a transport."""

    agent = BourneAgentService() if service is None else service
    logger = _logger()
    mutation_lock = anyio.Lock()
    server: MCPServer[Any] = MCPServer(
        name=SERVER_NAME,
        title=SERVER_TITLE,
        description=(
            "Universal experiment provenance and reproducibility for science and "
            "engineering."
        ),
        instructions=SERVER_INSTRUCTIONS,
        version=__version__,
        log_level=logging.getLevelName(logger.level),
    )

    async def call(
        operation: str,
        function: Callable[[], dict[str, Any]],
        *,
        mutation: bool = False,
    ) -> ToolResult:
        try:
            if mutation:
                async with mutation_lock:
                    data = await anyio.to_thread.run_sync(function)
            else:
                data = await anyio.to_thread.run_sync(function)
            return ToolResult(ok=True, data=data)
        except AgentInterfaceError as exc:
            logger.info("%s returned product error %s", operation, exc.code)
            return ToolResult(ok=False, error=ProductError(**exc.to_dict()))
        except Exception as exc:  # The protocol must not expose a Python traceback.
            logger.error("%s failed with %s", operation, type(exc).__name__)
            return ToolResult(
                ok=False,
                error=ProductError(
                    code="internal_error",
                    message="Bourne could not complete this operation.",
                    details={"operation": operation},
                ),
            )

    @server.tool(
        name="bourne_request_schema",
        description="Return Bourne's canonical ExecutionRequest version-2 JSON Schema.",
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_request_schema() -> ToolResult:
        return await call("request_schema", agent.request_schema)

    @server.tool(
        name="bourne_validate_request",
        description=(
            "Validate and normalize an ExecutionRequest v2 without discovery, "
            "planning, persistence, or execution."
        ),
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_validate_request(
        request: ExecutionRequestDocument,
    ) -> ToolResult:
        return await call("validate_request", partial(agent.validate_request, request))

    @server.tool(
        name="bourne_discover",
        description=(
            "Run bounded local compute-site discovery and persist a new immutable "
            "inventory snapshot."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_discover() -> ToolResult:
        return await call("discover", agent.discover, mutation=True)

    @server.tool(
        name="bourne_inventory",
        description=(
            "Read an existing inventory by latest, full ID, unique prefix, or @N; "
            "this never performs discovery."
        ),
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_inventory(reference: InventoryReference = "latest") -> ToolResult:
        return await call("inventory", partial(agent.inventory, reference))

    @server.tool(
        name="bourne_site_list",
        description="List configured non-secret local and SSH site contexts.",
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_site_list() -> ToolResult:
        return await call("site_list", agent.sites)

    @server.tool(
        name="bourne_site_inspect",
        description="Inspect one configured site, its policy claims, and inventory identities.",
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_site_inspect(reference: SiteReference) -> ToolResult:
        return await call("site_inspect", partial(agent.site, reference))

    @server.tool(
        name="bourne_site_discover",
        description=(
            "Discover one configured site and persist a new immutable inventory "
            "snapshot linked to it. `reference` is the site's exact name or canonical "
            "ID. Use this for a named local or SSH site; use `bourne_discover` for "
            "Bourne's current local context and `bourne_site_inspect` to read existing "
            "state. SSH discovery may require existing user authorization, uses only "
            "bounded typed probes, never accepts arbitrary commands, and never "
            "executes a scientific workload. Returns a compact summary and immutable "
            "snapshot ID; use `bourne_inventory` with that ID for the full inventory."
        ),
        annotations=_annotation(read_only=False, open_world=True),
        structured_output=True,
    )
    async def bourne_site_discover(reference: SiteReference) -> ToolResult:
        return await call(
            "site_discover", partial(agent.discover_site, reference), mutation=True
        )

    @server.tool(
        name="bourne_site_policy_claim",
        description=(
            "Append one durable structured policy claim and provenance record to an "
            "existing configured site. `reference` is the exact site name or canonical "
            "ID; `claim` contains the asserted fact, evidence classification, source "
            "identity, and applicability. Use this for reviewed site constraints or "
            "advice before candidate generation; use `bourne_site_discover` to observe "
            "infrastructure. This stores no source document, fetches no URL, runs no "
            "command, and does not modify previous claims."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_site_policy_claim(
        reference: SiteReference,
        claim: Annotated[
            SitePolicyClaimDocument,
            Field(
                description=(
                    "Bounded typed policy fact and provenance metadata to append to "
                    "the configured site."
                )
            ),
        ],
    ) -> ToolResult:
        return await call(
            "site_policy_claim",
            partial(
                agent.site_policy_claim,
                reference,
                claim.model_dump(exclude_none=True),
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_site_candidates",
        description=(
            "Generate at most 64 candidate plans for one configured site from an "
            "ExecutionRequest, an existing inventory, and optional declarative "
            "provider constraints. Use after site discovery and before "
            "`bourne_site_select`; use `bourne_plan` when site-aware candidate "
            "comparison is unnecessary. This does not execute or durably persist a "
            "request or plan, but stores an ephemeral candidate session in this MCP "
            "process; a restart loses that session."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_site_candidates(
        reference: SiteReference,
        request: ExecutionRequestDocument,
        provider: Annotated[
            DeclarativeProviderDocument | None,
            Field(
                description=(
                    "Optional bounded declarative constraints used to generate "
                    "candidates; the document cannot execute code or grant itself "
                    "trust."
                )
            ),
        ] = None,
        inventory_reference: InventoryReference = "latest",
    ) -> ToolResult:
        return await call(
            "site_candidates",
            partial(
                agent.site_candidates, reference, request, provider=provider,
                inventory_reference=inventory_reference,
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_site_select",
        description=(
            "Choose one candidate returned by `bourne_site_candidates`, persist "
            "selection evidence, and create a new immutable execution plan without "
            "executing it. `request_id` and `candidate_id` must come from the same live "
            "candidate session; regenerate candidates after a server restart. This "
            "writes a new plan without editing existing plans. Review the returned "
            "plan before calling `bourne_execute_plan`; selection fails when required "
            "approvals, declarations, or provider trust are missing."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_site_select(
        request_id: CandidateRequestID,
        candidate_id: CandidateID,
        selection_source: SelectionSource,
        rationale: SelectionRationale = None,
        variant_approvals: VariantApprovals = None,
        explicit_user_declarations: ExecutionOnlyDeclarations = None,
        trusted_provider_contract: ProviderTrustDecision = False,
        container: Annotated[
            ContainerExecutionDocument | None,
            Field(
                description=(
                    "Optional existing Apptainer or Singularity image and explicit "
                    "mounts to freeze into the plan; Bourne never builds or pulls it."
                )
            ),
        ] = None,
    ) -> ToolResult:
        return await call(
            "site_select",
            partial(
                agent.site_select, request_id, candidate_id,
                selection_source=selection_source, rationale=rationale,
                variant_approvals=variant_approvals,
                explicit_user_declarations=explicit_user_declarations,
                trusted_provider_contract=trusted_provider_contract,
                container=(
                    None if container is None else container.model_dump()
                ),
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_plan",
        description=(
            "Persist and resolve an ExecutionRequest v2 against an existing inventory. "
            "Planning never executes the workload and preserves ambiguity."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_plan(
        request: ExecutionRequestDocument,
        inventory_reference: InventoryReference = "latest",
    ) -> ToolResult:
        return await call(
            "plan",
            partial(
                agent.plan, request, inventory_reference=inventory_reference
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_execute_plan",
        description=(
            "Execute one immutable persisted Bourne plan without changing its command, "
            "resources, placement, backend, or inventory."
        ),
        annotations=_annotation(
            read_only=False, destructive=True, open_world=True
        ),
        structured_output=True,
    )
    async def bourne_execute_plan(plan_id: str) -> ToolResult:
        return await call(
            "execute_plan", partial(agent.execute_plan, plan_id), mutation=True
        )

    @server.tool(
        name="bourne_execution_get",
        description=(
            "Read a Bourne execution, including request, plan, lifecycle, scheduler, "
            "allocation, experiment, telemetry, and verification state."
        ),
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_execution_get(reference: str) -> ToolResult:
        return await call(
            "execution_get", partial(agent.execution_get, reference)
        )

    @server.tool(
        name="bourne_execution_reconcile",
        description="Reconnect and reconcile one exact Bourne-owned remote execution; never resubmit.",
        annotations=_annotation(read_only=False, open_world=True),
        structured_output=True,
    )
    async def bourne_execution_reconcile(reference: str) -> ToolResult:
        return await call(
            "execution_reconcile",
            partial(agent.execution_reconcile, reference),
            mutation=True,
        )

    @server.tool(
        name="bourne_execution_wait",
        description=(
            "Wait on one existing Bourne-managed scheduled execution with an optional "
            "bounded caller timeout; this creates no execution."
        ),
        annotations=_annotation(read_only=False, open_world=True),
        structured_output=True,
    )
    async def bourne_execution_wait(
        reference: str,
        timeout_seconds: float | None = None,
    ) -> ToolResult:
        return await call(
            "execution_wait",
            partial(
                agent.execution_wait,
                reference,
                timeout_seconds=timeout_seconds,
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_execution_cancel",
        description=(
            "Cancel only the exact scheduler job owned by an existing Bourne execution; "
            "arbitrary scheduler job IDs are not accepted."
        ),
        annotations=_annotation(
            read_only=False, destructive=True, open_world=True
        ),
        structured_output=True,
    )
    async def bourne_execution_cancel(reference: str) -> ToolResult:
        return await call(
            "execution_cancel",
            partial(agent.execution_cancel, reference),
            mutation=True,
        )

    @server.tool(
        name="bourne_trace_artifact",
        description=(
            "Trace a recorded output artifact to its producer, inputs, and experiment "
            "ancestry without guessing across ambiguous matches."
        ),
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_trace_artifact(path: str) -> ToolResult:
        return await call("trace_artifact", partial(agent.trace_artifact, path))

    return server


def run_mcp_server() -> None:
    """Run the canonical local MCP server over stdio."""

    create_mcp_server().run("stdio")
