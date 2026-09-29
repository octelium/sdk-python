"""Stable SDK errors. Native task cancellation is preserved as asyncio.CancelledError."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from octelium.api.main.cordium.v1 import Workspace

    from .models import ExecResult


class CordiumError(Exception):
    """RPC or SDK failure with a stable ``code`` and optional transport ``details``.

    ``code`` is a gRPC status name (e.g. NOT_FOUND) or an SDK code such as
    CLIENT_CLOSED or PROTOCOL_ERROR. The original exception is chained as __cause__.
    """

    def __init__(self, message: str, code: str = "UNKNOWN", *, details: object = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


class ExecError(CordiumError):
    """A checked command exited unsuccessfully; ``result`` contains its captured output."""

    def __init__(self, result: ExecResult) -> None:
        super().__init__(f"Command exited with code {result.exit_code}", "COMMAND_FAILED")
        self.result = result


class WorkspaceFailureError(CordiumError):
    """A run failed; ``workspace`` is retained for diagnosis and explicit deletion."""

    def __init__(self, workspace: Workspace) -> None:
        super().__init__(
            workspace.status.failure.message
            or f"Workspace {workspace.metadata.name} failed to become ready",
            "WORKSPACE_FAILED",
        )
        self.workspace = workspace


def nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError(f"{name} must be a nonempty string without NUL bytes")
    return value


def integer(value: int, name: str, minimum: int = 0, maximum: int = 2**32 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value
