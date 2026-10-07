"""Immutable Python value types shared by synchronous and asynchronous clients."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Literal, TypeVar

from betterproto import Message
from octelium.api.main.cordium.v1 import Workspace
from octelium.api.main.meta.v1 import ObjectReference

from .errors import nonempty

T = TypeVar("T")
M = TypeVar("M", bound=Message)


@dataclass(frozen=True, slots=True, kw_only=True)
class Ref:
    """Identify an object by exactly one of ``name`` or immutable ``uid``.

    Resource methods also accept a plain string, interpreted as a name.
    """

    name: str | None = None
    uid: str | None = None

    def __post_init__(self) -> None:
        if (self.name is None) == (self.uid is None):
            raise ValueError("Specify exactly one of name or uid")
        nonempty(self.name if self.name is not None else self.uid or "", "Reference")


Reference = str | Ref
"""An object name or a Ref identifying an immutable UID."""


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """One page: items, zero-based page, page_size, total_count, and has_more."""

    items: tuple[T, ...]
    page: int = 0
    page_size: int = 0
    total_count: int = 0
    has_more: bool = False


@dataclass(frozen=True, slots=True)
class ExecOutput:
    """An ordered stdout/stderr chunk. Data is binary, not assumed to be UTF-8."""

    stream: Literal["stdout", "stderr"]
    data: bytes


@dataclass(frozen=True, slots=True)
class ExecResult:
    """Command outcome with bounded captures; truncated indicates omitted capture bytes."""

    exit_code: int
    stdout_bytes: bytes = b""
    stderr_bytes: bytes = b""
    truncated: bool = False
    killed: bool = False

    @property
    def stdout(self) -> str:
        """Captured stdout decoded as UTF-8, replacing invalid sequences."""
        return self.stdout_bytes.decode("utf-8", errors="replace")

    @property
    def stderr(self) -> str:
        """Captured stderr decoded as UTF-8, replacing invalid sequences."""
        return self.stderr_bytes.decode("utf-8", errors="replace")

    @property
    def success(self) -> bool:
        """Whether exit_code is zero."""
        return self.exit_code == 0


@dataclass(frozen=True, slots=True)
class TerminalEvent:
    """PTY event: output bytes, resize dimensions, or remote shell closure."""

    type: Literal["output", "resize", "close"]
    data: bytes = b""
    cols: int = 0
    rows: int = 0


@dataclass(frozen=True, slots=True)
class LogEntry:
    """Initialization log entry: raw bytes with their time, output stream and stage.

    stage is ``cloning_repo``, ``pulling_image``, ``building_image``, ``task`` or ``unknown``.
    """

    at: datetime
    stage: Literal["cloning_repo", "pulling_image", "building_image", "task", "unknown"]
    stream: Literal["stdout", "stderr"]
    data: bytes


@dataclass(frozen=True, slots=True)
class WorkspaceEvent:
    """Watched create, update or delete, with deep-copied protobuf snapshots.

    previous is the resource before an update, when the server reported it.
    """

    type: Literal["create", "update", "delete"]
    workspace: Workspace
    previous: Workspace | None = None


def reference(value: Reference) -> ObjectReference:
    if isinstance(value, str):
        return ObjectReference(name=nonempty(value, "Reference"))
    return ObjectReference(name=value.name or "", uid=value.uid or "")


def present(message: M) -> M:
    message._serialized_on_wire = True
    return message


def timeout_value(value: float | None) -> float | None:
    if value is not None and (isinstance(value, bool) or not math.isfinite(value) or value <= 0):
        raise ValueError("timeout must be a positive finite number of seconds or None")
    return value
