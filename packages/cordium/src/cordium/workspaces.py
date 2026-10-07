"""Workspace creation, lifecycle, execution and application endpoints."""

from __future__ import annotations

import copy
import re
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime
from typing import Literal, Unpack, cast

import betterproto
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from octelium.api.main.cordium import v1 as p
from octelium.api.main.meta import v1 as m

from ._engine import Engine
from .errors import CordiumError, WorkspaceFailureError, integer, nonempty, run_failure
from .exec import AsyncExecSession, argv
from .files import AsyncFiles
from .models import (
    ExecResult,
    LogEntry,
    Page,
    Ref,
    Reference,
    WorkspaceEvent,
    present,
    reference,
    timeout_value,
)
from .resources import AsyncSnapshots, _common, _page, _poll
from .spec import WorkspaceOptions, create_workspace_spec, variables
from .streams import AsyncStream
from .terminals import AsyncTerminals

State = p.WorkspaceStatusState
"""Generated lifecycle enum; PREPARING accepts exec, RUNNING has completed setup."""

_STAGES: dict[
    int, Literal["cloning_repo", "pulling_image", "building_image", "task", "unknown"]
] = {
    p.ListenLogResponseType.TYPE_CLONING_REPO: "cloning_repo",
    p.ListenLogResponseType.TYPE_PULLING_IMAGE: "pulling_image",
    p.ListenLogResponseType.TYPE_BUILDING_IMAGE: "building_image",
    p.ListenLogResponseType.TYPE_TASK: "task",
}


class AsyncWorkspaces:
    """Create and find workspace handles. New workspaces are stopped until started."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def create(
        self, *, timeout: float | None = 30, **options: Unpack[WorkspaceOptions]
    ) -> AsyncWorkspace:
        """Create a stopped workspace with a server-assigned name.

        Select template or snapshot with Ref or name. Convenience options override
        the corresponding fields of a deep-copied generated spec. No start is implicit.
        """
        spec = present(create_workspace_spec(**options))
        value = await self._engine.call(
            lambda: self._engine.raw.main.create_workspace(
                p.Workspace(
                    metadata=m.Metadata(display_name=options.get("display_name", "")),
                    spec=spec,
                    status=p.WorkspaceStatus(
                        template_ref=reference(options["template"])
                        if "template" in options
                        else m.ObjectReference(),
                        workspace_snapshot_ref=reference(options["snapshot"])
                        if "snapshot" in options
                        else m.ObjectReference(),
                    ),
                )
            ),
            timeout,
        )
        return AsyncWorkspace(self._engine, value)

    async def run(
        self,
        *,
        timeout: float | None = 300,
        poll_interval: float = 1,
        region: Reference | None = None,
        **options: Unpack[WorkspaceOptions],
    ) -> AsyncWorkspace:
        """Create, start and wait for RUNNING under one total deadline.

        Failures leave the workspace in place for diagnosis; no implicit deletion.
        A CordiumError raised after creation keeps its code and carries the
        workspace's last fetched state in ``error.workspace``. Region selects
        placement for this run. poll_interval is in seconds.
        """
        if timeout_value(poll_interval) is None:
            raise ValueError("poll_interval must be positive")
        workspace: AsyncWorkspace | None = None
        try:
            async with self._engine.operation(timeout):
                workspace = await self.create(timeout=None, **options)
                await workspace.start(region=region, timeout=None)
                return await workspace.wait_until_running(timeout=None, poll_interval=poll_interval)
        except CordiumError as error:
            if workspace is not None and error.workspace is None:
                error.workspace = workspace.proto
            raise

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> AsyncWorkspace:
        """Fetch an existing workspace by name or immutable UID."""
        key = reference(ref)
        value = await self._engine.call(
            lambda: self._engine.raw.main.get_workspace(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )
        return AsyncWorkspace(self._engine, value)

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete a workspace. The server enforces lifecycle restrictions."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_workspace(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        template: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[AsyncWorkspace]:
        """Fetch a zero-based page of the caller's workspaces; filters are exclusive."""
        if space is not None and template is not None:
            raise ValueError("Choose space or template")
        request = p.ListWorkspaceOptions(common=_common(page, page_size, order_by, descending))
        if space is not None:
            request.space_ref = reference(space)
        if template is not None:
            request.template_ref = reference(template)
        result = await self._engine.call(
            lambda: self._engine.raw.main.list_workspace(request), timeout
        )
        return _page(
            [AsyncWorkspace(self._engine, item) for item in result.items], result.list_response_meta
        )

    async def all(
        self,
        *,
        space: Reference | None = None,
        template: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[AsyncWorkspace]:
        """Lazily fetch workspaces. timeout applies to each page request."""
        while True:
            result = await self.list(
                space=space,
                template=template,
                page=page,
                page_size=page_size,
                order_by=order_by,
                descending=descending,
                timeout=timeout,
            )
            for item in result.items:
                yield item
            if not result.has_more:
                return
            if not result.items or result.page != page:
                raise CordiumError("Non-progressing pagination", "PROTOCOL_ERROR")
            page = integer(page + 1, "page")

    async def update(self, workspace: p.Workspace, *, timeout: float | None = 30) -> AsyncWorkspace:
        """Replace a fetched protobuf resource once; no retry of version conflicts."""
        value = copy.deepcopy(workspace)
        result = await self._engine.call(
            lambda: self._engine.raw.main.update_workspace(value), timeout
        )
        return AsyncWorkspace(self._engine, result)

    def watch(
        self,
        ref: Reference | None = None,
        *,
        timeout: float | None = None,
        max_buffer_bytes: int = 8 * 1024 * 1024,
    ) -> AsyncStream[WorkspaceEvent]:
        """Subscribe to future create/update/delete events; no initial snapshot or reconnect.

        Use an async context manager when stopping iteration early. Update events
        carry the previous resource when the server reports it.
        """
        request = p.WatchWorkspaceRequest(
            workspace_ref=reference(ref) if ref is not None else m.ObjectReference()
        )

        async def events() -> AsyncIterator[WorkspaceEvent]:
            stream = self._engine.raw.main.watch_workspace(request)
            try:
                async for message in stream:
                    kind, _ = betterproto.which_one_of(message, "type")
                    if kind == "create":
                        yield WorkspaceEvent("create", message.create.item)
                    elif kind == "update":
                        previous = message.update.old_item
                        yield WorkspaceEvent(
                            "update",
                            message.update.new_item,
                            previous if betterproto.serialized_on_wire(previous) else None,
                        )
                    elif kind == "delete":
                        yield WorkspaceEvent("delete", message.delete.item)
            finally:
                if hasattr(stream, "aclose"):
                    await stream.aclose()

        return AsyncStream(
            self._engine,
            events,
            timeout=timeout,
            size=lambda event: len(bytes(event.workspace)) + len(bytes(event.previous or b"")),
            max_bytes=max_buffer_bytes,
        )


class AsyncWorkspace:
    """Workspace handle with an isolated cache; refresh/lifecycle methods update it.

    Properties never perform network calls. Returned protobuf objects are deep copies.
    Keep the parent client open while using a handle. Closing it never deletes workspaces.
    """

    def __init__(self, engine: Engine, value: p.Workspace) -> None:
        self._engine, self._value = engine, copy.deepcopy(value)

    def _ref(self) -> Ref:
        return Ref(uid=self.uid) if self.uid else Ref(name=self.name)

    @property
    def proto(self) -> p.Workspace:
        """Deep copy of the last resource returned by the Cluster."""
        return copy.deepcopy(self._value)

    @property
    def name(self) -> str:
        """Server-assigned name."""
        return self._value.metadata.name

    @property
    def uid(self) -> str:
        """Immutable Cluster-wide identifier."""
        return self._value.metadata.uid

    @property
    def display_name(self) -> str:
        """Optional human-readable label."""
        return self._value.metadata.display_name

    @property
    def created_at(self) -> datetime:
        """Creation timestamp, represented as a timezone-aware datetime."""
        return self._value.metadata.created_at

    @property
    def is_ephemeral(self) -> bool:
        """Whether storage is discarded when the workspace stops."""
        return self._value.spec.is_ephemeral

    @property
    def space_name(self) -> str:
        """Name of the Space the workspace belongs to."""
        return self._value.status.space_ref.name

    @property
    def template_name(self) -> str:
        """Name of the Template the workspace was created from, or empty."""
        return self._value.status.template_ref.name

    @property
    def region_name(self) -> str:
        """Name of the Region currently hosting the workspace, or empty."""
        return self._value.status.region_ref.name

    @property
    def failure(self) -> p.WorkspaceStatusFailure | None:
        """Deep copy of the current or latest run's failure, if any."""
        return copy.deepcopy(run_failure(self._value))

    @property
    def limit(self) -> p.WorkspaceSpecLimit:
        """Deep copy of the effective compute limits resolved by the Cluster."""
        return copy.deepcopy(self._value.status.limit)

    @property
    def state(self) -> State:
        """Last observed lifecycle state."""
        return self._value.status.state

    @property
    def is_ready(self) -> bool:
        """Whether exec/terminals are available (PREPARING or RUNNING)."""
        return self.state in (State.PREPARING, State.RUNNING)

    @property
    def is_running(self) -> bool:
        """Whether initialization has completed."""
        return self.state == State.RUNNING

    @property
    def is_starting(self) -> bool:
        """Whether provisioning is in progress before PREPARING."""
        return self.state in (
            State.INIT_REQUEST,
            State.INITIALIZING,
            State.PULLING_IMAGE,
            State.BUILDING_IMAGE,
            State.STARTING_RUNTIME,
        )

    @property
    def is_stopping(self) -> bool:
        """Whether a stop is in progress."""
        return self.state in (State.STOPPING_REQUEST, State.STOPPING)

    @property
    def is_stopped(self) -> bool:
        """Whether the workspace is stopped."""
        return self.state == State.STOPPED

    @property
    def hostname(self) -> str:
        """Public hostname, or empty when stopped."""
        return self._value.status.hostname

    @property
    def url(self) -> str:
        """HTTPS URL of the default application, or empty when stopped."""
        return f"https://{self.hostname}" if self.hostname else ""

    @property
    def spec(self) -> p.WorkspaceSpec:
        """Deep copy of the persisted spec."""
        return copy.deepcopy(self._value.spec)

    @property
    def status(self) -> p.WorkspaceStatus:
        """Deep copy of status, including effective limits, run, sharing and failure details."""
        return copy.deepcopy(self._value.status)

    @property
    def applications(self) -> tuple[p.WorkspaceSpecApplication, ...]:
        """Declared named application ports, copied from the cache."""
        return tuple(copy.deepcopy(self._value.spec.applications))

    @property
    def files(self) -> AsyncFiles:
        """Binary-safe file operations implemented through exec."""
        return AsyncFiles(self._engine, self._ref())

    @property
    def terminals(self) -> AsyncTerminals:
        """Persistent PTY shells; detaching does not terminate them."""
        return AsyncTerminals(self._engine, self._ref())

    def app_url(self, name: str) -> str:
        """Application URL using Cordium's underscore routing, or empty when stopped."""
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", name):
            raise ValueError("Invalid application name")
        if not self.hostname:
            return ""
        if any(app.name == name and app.is_default for app in self._value.spec.applications):
            return self.url
        return f"https://{name}_{self.hostname}"

    def port_url(self, port: int) -> str:
        """HTTPS route to a port from 1 through 65535, or empty when stopped."""
        integer(port, "port", 1, 65535)
        return f"https://port_{port}_{self.hostname}" if self.hostname else ""

    async def refresh(self, *, timeout: float | None = 30) -> AsyncWorkspace:
        """Fetch the current resource into this handle and return it."""
        value = await AsyncWorkspaces(self._engine).get(self._ref(), timeout=timeout)
        self._value = value._value
        return self

    async def modify(
        self, mutate: Callable[[p.Workspace], None], *, timeout: float | None = 30
    ) -> AsyncWorkspace:
        """Fetch, mutate a copied resource and update once, refreshing this handle.

        The callback is synchronous; exceptions abort the write. Conflicts are
        surfaced without retrying or repeating callback side effects.
        """
        async with self._engine.operation(timeout):
            value = (await AsyncWorkspaces(self._engine).get(self._ref(), timeout=None)).proto
            mutate(value)
            result = await AsyncWorkspaces(self._engine).update(value, timeout=None)
            self._value = result._value
            return self

    async def start(
        self,
        *,
        vars: Mapping[str, str] | None = None,
        region: Reference | None = None,
        timeout: float | None = 30,
    ) -> AsyncWorkspace:
        """Request an asynchronous start, then refresh; vars/region apply only to this run.

        Starting a workspace that is already starting or running is a no-op.
        """
        config = p.StartWorkspaceRequestConfig(
            vars=variables(vars) if vars is not None else [],
            region_ref=reference(region) if region is not None else m.ObjectReference(),
        )
        async with self._engine.operation(timeout):
            try:
                await self._engine.raw.main.start_workspace(
                    p.StartWorkspaceRequest(workspace_ref=reference(self._ref()), config=config)
                )
            except GRPCError as error:
                if error.status is not Status.ALREADY_EXISTS:
                    raise
            return await self.refresh(timeout=None)

    async def stop(self, *, timeout: float | None = 30) -> AsyncWorkspace:
        """Request an asynchronous stop and refresh. Ephemeral storage is discarded."""
        async with self._engine.operation(timeout):
            await self._engine.raw.main.stop_workspace(
                p.StopWorkspaceRequest(workspace_ref=reference(self._ref()))
            )
            return await self.refresh(timeout=None)

    async def snapshot(self, name: str, *, timeout: float | None = 30) -> p.WorkspaceSnapshot:
        """Request a storage snapshot; use client.snapshots.wait_until_ready before restoring.

        Requires persistent storage. A running workspace snapshot is crash-consistent.
        """
        return await AsyncSnapshots(self._engine).create(name, self._ref(), timeout=timeout)

    async def delete(self, *, timeout: float | None = 30) -> None:
        """Delete this workspace; the server enforces lifecycle restrictions."""
        await AsyncWorkspaces(self._engine).delete(self._ref(), timeout=timeout)

    async def wait_until_running(
        self, *, timeout: float | None = 300, poll_interval: float = 1
    ) -> AsyncWorkspace:
        """Wait for RUNNING, failing promptly on failure or a stopped/stopping run."""
        return await self._wait((State.RUNNING,), timeout, poll_interval)

    async def wait_until_ready(
        self, *, timeout: float | None = 300, poll_interval: float = 1
    ) -> AsyncWorkspace:
        """Wait until PREPARING or RUNNING accepts commands; setup may still be running."""
        return await self._wait((State.PREPARING, State.RUNNING), timeout, poll_interval)

    async def wait_until_stopped(
        self, *, timeout: float | None = 300, poll_interval: float = 1
    ) -> AsyncWorkspace:
        """Wait for STOPPED; raise WorkspaceFailureError if the run that stopped had failed."""
        return await self._wait((State.STOPPED,), timeout, poll_interval)

    async def _wait(
        self, states: tuple[int, ...], timeout: float | None, poll_interval: float
    ) -> AsyncWorkspace:
        def done(item: AsyncWorkspace) -> bool:
            if states != (State.STOPPED,):
                if run_failure(item._value) is not None:
                    raise WorkspaceFailureError(item.proto)
                if item.is_stopped or item.is_stopping:
                    raise CordiumError(
                        "Workspace stopped before becoming ready",
                        "WORKSPACE_STOPPED",
                        details=item.proto,
                    )
            elif item.is_stopped and run_failure(item._value) is not None:
                raise WorkspaceFailureError(item.proto)
            return item.state in states

        return await _poll(
            self._engine, lambda: self.refresh(timeout=None), done, timeout, poll_interval
        )

    async def exec(
        self,
        command: str | Sequence[str],
        *,
        cwd: str = "",
        env: Mapping[str, str] | None = None,
        root: bool = False,
        stdin: str | bytes | None = None,
        check: bool = False,
        timeout: float | None = None,
        max_capture_bytes: int = 1024 * 1024,
    ) -> ExecResult:
        """Run a shell command or safely quoted argv and collect bounded output.

        A nonzero exit is returned in the result; check=True raises ExecError instead. stdin is initial input, not
        an EOF signal: Cordium's protocol cannot half-close stdin. Commands that
        read until EOF need explicit framing (e.g. head -c). Timeout cancels exec.
        """
        session = await self.exec_stream(
            command,
            cwd=cwd,
            env=env,
            root=root,
            stdin=stdin,
            interactive=False,
            check=check,
            timeout=timeout,
            max_capture_bytes=max_capture_bytes,
        )
        async with session:
            return await session.wait()

    async def exec_stream(
        self,
        command: str | Sequence[str],
        *,
        cwd: str = "",
        env: Mapping[str, str] | None = None,
        root: bool = False,
        stdin: str | bytes | None = None,
        interactive: bool = True,
        check: bool = False,
        timeout: float | None = None,
        max_capture_bytes: int = 1024 * 1024,
        max_buffer_bytes: int = 8 * 1024 * 1024,
        kill_grace: float = 10,
    ) -> AsyncExecSession:
        """Start a command; iterate binary stdout/stderr, write input, kill or await its result.

        Use ``async with await workspace.exec_stream(...)`` to close on early exit.
        A full stream queue pauses reading output until it drains; capture truncation
        is reported in ExecResult. A killed command that reports no exit within
        kill_grace seconds ends with exit code -1.
        """
        self._engine.check()
        session = AsyncExecSession(
            self._engine,
            self._ref(),
            command if isinstance(command, str) else argv(*command),
            cwd=cwd,
            env=env,
            root=root,
            stdin=stdin,
            interactive=interactive,
            check=check,
            timeout=timeout,
            max_capture_bytes=max_capture_bytes,
            max_buffer_bytes=max_buffer_bytes,
            kill_grace=kill_grace,
        )
        return await session.ready()

    def logs(
        self, *, timeout: float | None = None, max_buffer_bytes: int = 8 * 1024 * 1024
    ) -> AsyncStream[LogEntry]:
        """Stream initialization logs: repository cloning, image pulls and builds, and tasks."""
        request = p.ListenLogRequest(workspace_ref=reference(self._ref()))

        async def entries() -> AsyncIterator[LogEntry]:
            stream = self._engine.raw.workspace.listen_log(request)
            try:
                async for message in stream:
                    yield LogEntry(
                        message.created_at,
                        _STAGES.get(message.type, "unknown"),
                        "stderr"
                        if message.mode == p.ListenLogResponseMode.MODE_STDERR
                        else "stdout",
                        message.data,
                    )
            finally:
                if hasattr(stream, "aclose"):
                    await stream.aclose()

        return AsyncStream(
            self._engine,
            entries,
            timeout=timeout,
            size=lambda entry: len(entry.data),
            max_bytes=max_buffer_bytes,
        )

    def watch(
        self, *, timeout: float | None = None, max_buffer_bytes: int = 8 * 1024 * 1024
    ) -> AsyncStream[WorkspaceEvent]:
        """Subscribe to this workspace's events; does not implicitly update this handle."""
        return AsyncWorkspaces(self._engine).watch(
            self._ref(), timeout=timeout, max_buffer_bytes=max_buffer_bytes
        )

    async def share_port(
        self,
        application: str,
        *,
        mode: Literal["members", "all"] = "members",
        timeout: float | None = 30,
    ) -> AsyncWorkspace:
        """Share a named application with Space members or all authenticated Cluster users."""
        if mode not in ("members", "all"):
            raise ValueError("Invalid sharing mode")
        async with self._engine.operation(timeout):
            await self._engine.raw.main.share_workspace_port(
                p.ShareWorkspacePortRequest(
                    workspace_ref=reference(self._ref()),
                    application_name=nonempty(application, "Application name"),
                    mode=cast(
                        p.ShareWorkspacePortRequestMode,
                        p.ShareWorkspacePortRequestMode.ALL
                        if mode == "all"
                        else p.ShareWorkspacePortRequestMode.MEMBERS,
                    ),
                )
            )
            return await self.refresh(timeout=None)

    async def unshare_port(self, application: str, *, timeout: float | None = 30) -> AsyncWorkspace:
        """Revoke sharing of a named application and refresh the cache."""
        async with self._engine.operation(timeout):
            await self._engine.raw.main.unshare_workspace_port(
                p.UnshareWorkspacePortRequest(
                    workspace_ref=reference(self._ref()),
                    application_name=nonempty(application, "Application name"),
                )
            )
            return await self.refresh(timeout=None)
