"""Official MCP SDK adapter for Project Bourne's structured agent service."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from functools import partial
from typing import Annotated, Any

import anyio
from mcp.server import MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, WithJsonSchema

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
    dict[str, Any], WithJsonSchema(_provider_input_schema())
]


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
        description="Return Bourne's canonical ExecutionRequest version-1 JSON Schema.",
        annotations=_annotation(read_only=True, idempotent=True),
        structured_output=True,
    )
    async def bourne_request_schema() -> ToolResult:
        return await call("request_schema", agent.request_schema)

    @server.tool(
        name="bourne_validate_request",
        description=(
            "Validate and normalize an ExecutionRequest v1 without discovery, "
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
    async def bourne_inventory(reference: str = "latest") -> ToolResult:
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
    async def bourne_site_inspect(reference: str) -> ToolResult:
        return await call("site_inspect", partial(agent.site, reference))

    @server.tool(
        name="bourne_site_discover",
        description="Run bounded typed discovery at one configured site; no arbitrary SSH command is accepted.",
        annotations=_annotation(read_only=False, open_world=True),
        structured_output=True,
    )
    async def bourne_site_discover(reference: str) -> ToolResult:
        return await call(
            "site_discover", partial(agent.discover_site, reference), mutation=True
        )

    @server.tool(
        name="bourne_site_candidates",
        description="Generate bounded ephemeral site-aware plan candidates without executing.",
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_site_candidates(
        reference: str,
        request: ExecutionRequestDocument,
        provider: DeclarativeProviderDocument | None = None,
        inventory_reference: str = "latest",
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
        description="Persist a bounded selection summary and materialize one viable immutable plan.",
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_site_select(
        request_id: str,
        candidate_id: str,
        selection_source: str,
        rationale: str | None = None,
    ) -> ToolResult:
        return await call(
            "site_select",
            partial(
                agent.site_select, request_id, candidate_id,
                selection_source=selection_source, rationale=rationale,
            ),
            mutation=True,
        )

    @server.tool(
        name="bourne_plan",
        description=(
            "Persist and resolve an ExecutionRequest v1 against an existing inventory. "
            "Planning never executes the workload and preserves ambiguity."
        ),
        annotations=_annotation(read_only=False, destructive=False),
        structured_output=True,
    )
    async def bourne_plan(
        request: ExecutionRequestDocument,
        inventory_reference: str = "latest",
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
