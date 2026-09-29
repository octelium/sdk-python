"""Typed Cordium resources and lazy zero-based pagination."""

from __future__ import annotations

import asyncio
import copy
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Literal, TypeAlias, TypeVar, cast

from betterproto.lib.google.protobuf import ListValue, NullValue, Struct, Value
from octelium.api.main.cordium import v1 as p
from octelium.api.main.meta import v1 as m

from ._engine import Engine
from .errors import CordiumError, integer, nonempty
from .models import Page, Reference, reference, timeout_value
from .spec import (
    Resources,
    SecretRef,
    Task,
    VolumeMount,
    WorkspaceOptions,
    create_workspace_spec,
    environment,
    limits,
)

JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
"""JSON-compatible value; numbers must be finite and exactly representable as protobuf doubles."""
SecretValue: TypeAlias = str | bytes | dict[str, JsonValue]
"""Write-only Secret payload: text, binary, or structured JSON attributes."""
Role: TypeAlias = Literal["owner", "admin", "user"]
"""Space membership role; authorization remains enforced by the Cluster."""
T = TypeVar("T")


def _json(value: JsonValue) -> Value:
    if value is None:
        return Value(null_value=NullValue(0))
    if isinstance(value, bool):
        return Value(bool_value=value)
    if isinstance(value, str):
        return Value(string_value=value)
    if isinstance(value, (float, int)):
        if not math.isfinite(value) or (isinstance(value, int) and abs(value) > 2**53):
            raise ValueError("Structured Secret numbers must be finite and exactly representable")
        return Value(number_value=float(value))
    if isinstance(value, list):
        return Value(list_value=ListValue(values=[_json(v) for v in value]))
    return Value(struct_value=Struct(fields={k: _json(v) for k, v in value.items()}))


def _secret(value: SecretValue) -> p.SecretData:
    if isinstance(value, str):
        return p.SecretData(value=value)
    if isinstance(value, bytes):
        return p.SecretData(value_bytes=value)
    return p.SecretData(attrs=Struct(fields={k: _json(v) for k, v in value.items()}))


def _common(
    page: int, page_size: int, order_by: Literal["name", "created_at"] | None, descending: bool
) -> m.CommonListOptions:
    if order_by not in (None, "name", "created_at"):
        raise ValueError("Invalid sort field")
    return m.CommonListOptions(
        page=integer(page, "page"),
        items_per_page=integer(page_size, "page_size"),
        order_by=m.CommonListOptionsOrderBy(
            type=cast(
                m.CommonListOptionsOrderByType,
                m.CommonListOptionsOrderByType.NAME
                if order_by == "name"
                else m.CommonListOptionsOrderByType.CREATED_AT
                if order_by
                else m.CommonListOptionsOrderByType.TYPE_UNSET,
            ),
            mode=cast(
                m.CommonListOptionsOrderByMode,
                m.CommonListOptionsOrderByMode.DESC
                if descending
                else m.CommonListOptionsOrderByMode.ASC,
            ),
        ),
    )


def _page(items: list[T], info: m.ListResponseMeta) -> Page[T]:
    return Page(tuple(items), info.page, info.items_per_page, info.total_count, info.has_more)


async def _poll(
    engine: Engine,
    get: Callable[[], Awaitable[T]],
    done: Callable[[T], bool],
    timeout: float | None,
    poll_interval: float,
) -> T:
    if timeout_value(poll_interval) is None:
        raise ValueError("poll_interval must be positive")
    async with engine.operation(timeout):
        while True:
            item = await get()
            if done(item):
                return item
            await asyncio.sleep(poll_interval)


class AsyncSpaces:
    """Space API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.Space:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_space(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_space(m.DeleteOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def list(
        self,
        *,
        mode: Literal["owned", "member"] = "owned",
        type: Literal["user", "organization"] | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.Space]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        if mode not in ("owned", "member") or type not in (None, "user", "organization"):
            raise ValueError("Invalid Space relationship or type")
        request = p.ListSpaceOptions(
            common=_common(page, page_size, order_by, descending),
            mode=cast(
                p.ListSpaceOptionsMode,
                p.ListSpaceOptionsMode.MODE_MEMBER
                if mode == "member"
                else p.ListSpaceOptionsMode.MODE_CREATED_BY,
            ),
            type=cast(
                p.SpaceStatusType,
                p.SpaceStatusType.USER
                if type == "user"
                else p.SpaceStatusType.ORGANIZATION
                if type == "organization"
                else p.SpaceStatusType.SPACE_TYPE_UNSET,
            ),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_space(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        mode: Literal["owned", "member"] = "owned",
        type: Literal["user", "organization"] | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.Space]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                mode=mode,
                type=type,
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.Space], None], *, timeout: float | None = 30
    ) -> p.Space:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.Space, *, timeout: float | None = 30) -> p.Space:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(lambda: self._engine.raw.main.update_space(value), timeout)

    async def create(
        self,
        name: str,
        *,
        organization: bool = False,
        display_name: str = "",
        default_resources: Resources | None = None,
        max_resources: Resources | None = None,
        env: Mapping[str, str | SecretRef] | None = None,
        disable_ssh: bool | None = None,
        spec: p.SpaceSpec | None = None,
        timeout: float | None = 30,
    ) -> p.Space:
        """Create a Space. organization=True appends .cordium to a short name.

        Resource limits are millicores/megabytes; env values may reference Space Secrets.
        Convenience fields override their corresponding fields in a copied spec.
        """
        nonempty(name, "Space name")
        if organization and "." not in name:
            name += ".cordium"
        value = copy.deepcopy(spec) if spec is not None else p.SpaceSpec()
        if default_resources is not None:
            value.limit.default_limit = limits(default_resources)
        if max_resources is not None:
            value.limit.max_limit = limits(max_resources)
        if env is not None:
            value.runtime.env_vars = environment(env)
        if disable_ssh is not None:
            value.authorization.disable_ssh = disable_ssh
        return await self._engine.call(
            lambda: self._engine.raw.main.create_space(
                p.Space(metadata=m.Metadata(name=name, display_name=display_name), spec=value)
            ),
            timeout,
        )

    async def leave(self, space: Reference, *, timeout: float | None = 30) -> None:
        """Remove the caller's Space membership; creators cannot leave their own Space."""
        await self._engine.call(
            lambda: self._engine.raw.main.leave_space(
                p.LeaveSpaceRequest(space_ref=reference(space))
            ),
            timeout,
        )


class AsyncTemplates:
    """Template API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.Template:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_template(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_template(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.Template]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListTemplateOptions(
            common=_common(page, page_size, order_by, descending),
            space_ref=reference(space) if space is not None else m.ObjectReference(),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_template(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.Template]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.Template], None], *, timeout: float | None = 30
    ) -> p.Template:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.Template, *, timeout: float | None = 30) -> p.Template:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(
            lambda: self._engine.raw.main.update_template(value), timeout
        )

    async def create(
        self,
        name: str,
        *,
        image: str | p.WorkspaceSpecImage | None = None,
        repository: str | p.WorkspaceSpecRepository | None = None,
        git_provider: str | None = None,
        env: Mapping[str, str | SecretRef] | None = None,
        vars: Mapping[str, str] | None = None,
        resources: Resources | None = None,
        tasks: Sequence[Task] | None = None,
        volumes: Sequence[VolumeMount] | None = None,
        disable_timeout: bool | None = None,
        auto_stop: bool | None = None,
        display_name: str = "",
        spec: p.TemplateSpec | None = None,
        timeout: float | None = 30,
    ) -> p.Template:
        """Create a template in the Space selected by its qualified name.

        image strings are registry references; spec provides lifecycle tasks, features,
        resources, variables and the other generated template fields.
        """
        value = copy.deepcopy(spec) if spec is not None else p.TemplateSpec()
        options: WorkspaceOptions = {}
        if env is not None:
            options["env"] = env
        if vars is not None:
            options["vars"] = vars
        if resources is not None:
            options["resources"] = resources
        if tasks is not None:
            options["tasks"] = tasks
        if volumes is not None:
            options["volumes"] = volumes
        if disable_timeout is not None:
            options["disable_timeout"] = disable_timeout
        if auto_stop is not None:
            options["auto_stop"] = auto_stop
        options["spec"] = p.WorkspaceSpec(runtime=value.runtime, limit=value.limit, vars=value.vars)
        merged = create_workspace_spec(**options)
        value.runtime, value.limit, value.vars = merged.runtime, merged.limit, merged.vars
        if image is not None:
            value.image = (
                p.WorkspaceSpecImage(
                    registry=p.WorkspaceSpecImageRegistry(url=nonempty(image, "Image"))
                )
                if isinstance(image, str)
                else copy.deepcopy(image)
            )
        if repository is not None:
            value.repository = (
                p.WorkspaceSpecRepository(url=repository)
                if isinstance(repository, str)
                else copy.deepcopy(repository)
            )
        if git_provider is not None:
            value.git_provider = git_provider
        return await self._engine.call(
            lambda: self._engine.raw.main.create_template(
                p.Template(
                    metadata=m.Metadata(
                        name=nonempty(name, "Template name"), display_name=display_name
                    ),
                    spec=value,
                )
            ),
            timeout,
        )

    async def build(
        self, template: Reference, *, tags: tuple[str, ...] = (), timeout: float | None = 30
    ) -> p.Template:
        """Start an asynchronous pre-build, optionally tagged for identification."""
        return await self._engine.call(
            lambda: self._engine.raw.main.build_template(
                p.BuildTemplateRequest(template_ref=reference(template), tags=list(tags))
            ),
            timeout,
        )

    async def cancel_build(self, template: Reference, *, timeout: float | None = 30) -> p.Template:
        """Cancel the currently running pre-build."""
        return await self._engine.call(
            lambda: self._engine.raw.main.cancel_build_template(
                p.CancelBuildTemplateRequest(template_ref=reference(template))
            ),
            timeout,
        )

    async def wait_for_build(
        self,
        template: Reference,
        build_id: str,
        *,
        timeout: float | None = 300,
        poll_interval: float = 1,
    ) -> p.Template:
        """Wait for a specific build ID, never mistaking an older successful build for this one."""
        nonempty(build_id, "Build ID")

        def done(item: p.Template) -> bool:
            build = next((b for b in item.status.build_info.builds if b.id == build_id), None)
            if build is None:
                raise CordiumError("Build not in template history", "NOT_FOUND")
            if build.is_canceled or build.state == p.TemplateStatusBuildInfoBuildState.STATE_FAILED:
                raise CordiumError(
                    build.failure.message or "Build failed or was cancelled", "BUILD_FAILED"
                )
            return build.state == p.TemplateStatusBuildInfoBuildState.STATE_READY

        return await _poll(
            self._engine, lambda: self.get(template, timeout=None), done, timeout, poll_interval
        )


class AsyncSnapshots:
    """WorkspaceSnapshot API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.WorkspaceSnapshot:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_workspace_snapshot(
                m.GetOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_workspace_snapshot(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        workspace: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.WorkspaceSnapshot]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListWorkspaceSnapshotOptions(
            common=_common(page, page_size, order_by, descending)
        )
        if space is not None and workspace is not None:
            raise ValueError("space and workspace filters are mutually exclusive")
        if space is not None:
            request.space_ref = reference(space)
        if workspace is not None:
            request.workspace_ref = reference(workspace)
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_workspace_snapshot(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        workspace: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.WorkspaceSnapshot]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
                workspace=workspace,
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

    async def create(
        self, name: str, workspace: Reference, *, timeout: float | None = 30
    ) -> p.WorkspaceSnapshot:
        """Snapshot persistent storage without stopping the source. Running snapshots are crash-consistent."""
        return await self._engine.call(
            lambda: self._engine.raw.main.create_workspace_snapshot(
                p.WorkspaceSnapshot(
                    metadata=m.Metadata(name=nonempty(name, "Snapshot name")),
                    status=p.WorkspaceSnapshotStatus(workspace_ref=reference(workspace)),
                )
            ),
            timeout,
        )

    async def wait_until_ready(
        self, ref: Reference, *, timeout: float | None = 300, poll_interval: float = 1
    ) -> p.WorkspaceSnapshot:
        """Wait until restorable; report terminal CSI/storage failures as SNAPSHOT_FAILED."""

        def done(item: p.WorkspaceSnapshot) -> bool:
            if item.status.state == p.WorkspaceSnapshotStatusState.STATE_FAILED:
                raise CordiumError(
                    item.status.failure.message or "Snapshot failed", "SNAPSHOT_FAILED"
                )
            return item.status.state == p.WorkspaceSnapshotStatusState.STATE_READY

        return await _poll(
            self._engine, lambda: self.get(ref, timeout=None), done, timeout, poll_interval
        )


class AsyncVolumes:
    """Volume API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.Volume:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_volume(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_volume(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.Volume]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListVolumeOptions(
            common=_common(page, page_size, order_by, descending),
            space_ref=reference(space) if space is not None else m.ObjectReference(),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_volume(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.Volume]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.Volume], None], *, timeout: float | None = 30
    ) -> p.Volume:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.Volume, *, timeout: float | None = 30) -> p.Volume:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(lambda: self._engine.raw.main.update_volume(value), timeout)

    async def create(
        self,
        name: str,
        *,
        size: int | None = None,
        shared: bool = False,
        region: Reference | None = None,
        timeout: float | None = 30,
    ) -> p.Volume:
        """Create a volume; size is megabytes. Some backends provision only after the first mount."""
        value = p.Volume(
            metadata=m.Metadata(name=nonempty(name, "Volume name")),
            spec=p.VolumeSpec(
                access_mode=cast(
                    p.VolumeAccessMode,
                    p.VolumeAccessMode.ACCESS_MODE_SHARED
                    if shared
                    else p.VolumeAccessMode.ACCESS_MODE_EXCLUSIVE,
                )
            ),
        )
        if size is not None:
            value.spec.size = p.VolumeSpecSize(megabytes=integer(size, "size", 1))
        if region is not None:
            value.status.region_ref = reference(region)
        return await self._engine.call(lambda: self._engine.raw.main.create_volume(value), timeout)

    async def grow(self, ref: Reference, megabytes: int, *, timeout: float | None = 30) -> p.Volume:
        """Grow capacity in megabytes. Shrinking is rejected; backend expansion support is required."""
        integer(megabytes, "megabytes", 1)
        async with self._engine.operation(timeout):
            item = await self.get(ref, timeout=None)
            if megabytes < max(item.spec.size.megabytes, item.status.capacity.megabytes):
                raise ValueError("Volumes cannot shrink")
            item.spec.size = p.VolumeSpecSize(megabytes=megabytes)
            return await self.update(item, timeout=None)

    async def wait_until_ready(
        self, ref: Reference, *, timeout: float | None = 300, poll_interval: float = 1
    ) -> p.Volume:
        """Wait for READY. Deferred provisioning may require mounting first."""

        def done(item: p.Volume) -> bool:
            if item.status.state == p.VolumeStatusState.STATE_FAILED:
                raise CordiumError(item.status.failure.message or "Volume failed", "VOLUME_FAILED")
            return item.status.state == p.VolumeStatusState.STATE_READY

        return await _poll(
            self._engine, lambda: self.get(ref, timeout=None), done, timeout, poll_interval
        )


class AsyncSecrets:
    """Secret API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.Secret:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_secret(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_secret(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.Secret]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListSecretOptions(
            common=_common(page, page_size, order_by, descending),
            space_ref=reference(space) if space is not None else m.ObjectReference(),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_secret(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.Secret]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
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

    async def create(
        self, name: str, value: SecretValue, *, timeout: float | None = 30
    ) -> p.Secret:
        """Create a write-only Space Secret. Strings, bytes and JSON objects are supported; no update RPC exists."""
        resource = p.Secret(
            metadata=m.Metadata(name=nonempty(name, "Secret name")), data=_secret(value)
        )
        return await self._engine.call(
            lambda: self._engine.raw.main.create_secret(resource), timeout
        )


class AsyncUserSecrets:
    """UserSecret API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.UserSecret:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_user_secret(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_user_secret(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.UserSecret]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListUserSecretOptions(common=_common(page, page_size, order_by, descending))
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_user_secret(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.UserSecret]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.UserSecret], None], *, timeout: float | None = 30
    ) -> p.UserSecret:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.UserSecret, *, timeout: float | None = 30) -> p.UserSecret:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(
            lambda: self._engine.raw.main.update_user_secret(value), timeout
        )

    async def create(
        self, name: str, value: SecretValue, *, timeout: float | None = 30
    ) -> p.UserSecret:
        """Create a write-only personal Secret from text, bytes, or JSON attributes."""
        data = p.UserSecretData().parse(bytes(_secret(value)))
        return await self._engine.call(
            lambda: self._engine.raw.main.create_user_secret(
                p.UserSecret(metadata=m.Metadata(name=nonempty(name, "Secret name")), data=data)
            ),
            timeout,
        )

    async def create_ssh_key(self, name: str, *, timeout: float | None = 30) -> p.UserSecret:
        """Ask the Cluster to generate an SSH key pair. The private key never leaves the Cluster."""
        return await self._engine.call(
            lambda: self._engine.raw.main.create_user_secret(
                p.UserSecret(
                    metadata=m.Metadata(name=nonempty(name, "Secret name")),
                    spec=p.UserSecretSpec(
                        type=cast(p.UserSecretSpecType, p.UserSecretSpecType.SSH_KEY)
                    ),
                )
            ),
            timeout,
        )

    async def set(
        self, ref: Reference, value: SecretValue, *, timeout: float | None = 30
    ) -> p.UserSecret:
        """Replace a personal Secret value, preserving its existing type and metadata."""
        async with self._engine.operation(timeout):
            resource = await self.get(ref, timeout=None)
            resource.data = p.UserSecretData().parse(bytes(_secret(value)))
            return await self.update(resource, timeout=None)


class AsyncGitProviders:
    """GitProvider API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.GitProvider:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_git_provider(
                m.GetOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_git_provider(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.GitProvider]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListGitProviderOptions(
            common=_common(page, page_size, order_by, descending),
            space_ref=reference(space) if space is not None else m.ObjectReference(),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_git_provider(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.GitProvider]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.GitProvider], None], *, timeout: float | None = 30
    ) -> p.GitProvider:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.GitProvider, *, timeout: float | None = 30) -> p.GitProvider:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(
            lambda: self._engine.raw.main.update_git_provider(value), timeout
        )

    async def create(
        self, name: str, spec: p.GitProviderSpec, *, timeout: float | None = 30
    ) -> p.GitProvider:
        """Create a GitProvider using a copied generated spec. OAuth client secrets reference Space Secrets."""
        value = p.GitProvider(
            metadata=m.Metadata(name=nonempty(name, "GitProvider name")), spec=copy.deepcopy(spec)
        )
        return await self._engine.call(
            lambda: self._engine.raw.main.create_git_provider(value), timeout
        )

    async def create_oauth(
        self,
        name: str,
        provider: Literal["github", "gitlab"],
        *,
        client_id: str,
        client_secret: str,
        scopes: tuple[str, ...] = (),
        timeout: float | None = 30,
    ) -> p.GitProvider:
        """Create GitHub/GitLab OAuth integration. client_secret is a Space Secret name, not its value."""
        nonempty(client_id, "Client ID")
        nonempty(client_secret, "Client secret reference")
        if provider == "github":
            spec = p.GitProviderSpec(
                github=p.GitProviderSpecGithub(
                    client_id=client_id,
                    client_secret=p.GitProviderSpecGithubClientSecret(from_secret=client_secret),
                    scopes=list(scopes),
                )
            )
        elif provider == "gitlab":
            spec = p.GitProviderSpec(
                gitlab=p.GitProviderSpecGitlab(
                    client_id=client_id,
                    client_secret=p.GitProviderSpecGitlabClientSecret(from_secret=client_secret),
                    scopes=list(scopes),
                )
            )
        else:
            raise ValueError("provider must be github or gitlab")
        return await self.create(name, spec, timeout=timeout)

    async def create_oauth2(
        self,
        name: str,
        *,
        client_id: str,
        client_secret: str,
        auth_url: str,
        token_url: str,
        scopes: tuple[str, ...],
        timeout: float | None = 30,
    ) -> p.GitProvider:
        """Create a generic/self-hosted OAuth2 provider; client_secret names a Space Secret.

        auth_url/token_url are the provider endpoints. At least one nonempty
        scope is required. The Cluster validates endpoint and provider policy.
        """
        if not scopes:
            raise ValueError("At least one OAuth2 scope is required")
        spec = p.GitProviderSpec(
            oauth2=p.GitProviderSpecOAuth2(
                client_id=nonempty(client_id, "Client ID"),
                client_secret=p.GitProviderSpecOAuth2ClientSecret(
                    from_secret=nonempty(client_secret, "Client secret reference")
                ),
                auth_url=nonempty(auth_url, "Authorization URL"),
                token_url=nonempty(token_url, "Token URL"),
                scopes=[nonempty(scope, "Scope") for scope in scopes],
            )
        )
        return await self.create(name, spec, timeout=timeout)


class AsyncMemberships:
    """Membership API. Names may be qualified with their Space; Ref(uid=...) uses immutable IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, ref: Reference, *, timeout: float | None = 30) -> p.Membership:
        """Fetch by name or immutable UID; the server enforces access permissions."""
        key = reference(ref)
        return await self._engine.call(
            lambda: self._engine.raw.main.get_membership(m.GetOptions(name=key.name, uid=key.uid)),
            timeout,
        )

    async def delete(self, ref: Reference, *, timeout: float | None = 30) -> None:
        """Delete the resource; ownership and lifecycle restrictions are enforced by the server."""
        key = reference(ref)
        await self._engine.call(
            lambda: self._engine.raw.main.delete_membership(
                m.DeleteOptions(name=key.name, uid=key.uid)
            ),
            timeout,
        )

    async def list(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 0,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> Page[p.Membership]:
        """Fetch one zero-based page. page_size=0 uses the server default."""
        request = p.ListMembershipOptions(
            common=_common(page, page_size, order_by, descending),
            space_ref=reference(space) if space is not None else m.ObjectReference(),
        )
        response = await self._engine.call(
            lambda: self._engine.raw.main.list_membership(request), timeout
        )
        return _page(response.items, response.list_response_meta)

    async def all(
        self,
        *,
        space: Reference | None = None,
        page: int = 0,
        page_size: int = 100,
        order_by: Literal["name", "created_at"] | None = None,
        descending: bool = False,
        timeout: float | None = 30,
    ) -> AsyncIterator[p.Membership]:
        """Iterate lazily from page; breaking stops fetching. timeout applies per page."""
        while True:
            result = await self.list(
                space=space,
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

    async def modify(
        self, ref: Reference, mutate: Callable[[p.Membership], None], *, timeout: float | None = 30
    ) -> p.Membership:
        """Fetch, edit with a synchronous callback, and write once under one deadline.

        Preserve metadata/resource version. Callback exceptions abort the write;
        conflicts are not retried. In the blocking client the callback runs on
        its event-loop thread, so it must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def update(self, resource: p.Membership, *, timeout: float | None = 30) -> p.Membership:
        """Replace a complete fetched resource once, preserving its metadata; no automatic retry."""
        value = copy.deepcopy(resource)
        return await self._engine.call(
            lambda: self._engine.raw.main.update_membership(value), timeout
        )

    async def add(
        self,
        space: Reference,
        *,
        email: str | None = None,
        user: Reference | None = None,
        role: Role = "user",
        timeout: float | None = 30,
    ) -> p.Membership:
        """Add an existing Cluster user to an organization Space; select exactly one of email or user."""
        if (email is None) == (user is None):
            raise ValueError("Specify exactly one of email or user")
        roles = {
            "owner": p.CreateMembershipRequestRole.OWNER,
            "admin": p.CreateMembershipRequestRole.ADMIN,
            "user": p.CreateMembershipRequestRole.USER,
        }
        if role not in roles:
            raise ValueError("Invalid membership role")
        value = p.CreateMembershipRequest(
            space_ref=reference(space), role=cast(p.CreateMembershipRequestRole, roles[role])
        )
        if email is not None:
            value.email = nonempty(email, "Email")
        if user is not None:
            value.user_ref = reference(user)
        return await self._engine.call(
            lambda: self._engine.raw.main.create_membership(value), timeout
        )

    async def mine(self, space: Reference, *, timeout: float | None = 30) -> p.Membership:
        """Fetch the calling user's membership in a Space."""
        return await self._engine.call(
            lambda: self._engine.raw.main.get_space_membership(
                p.GetSpaceMembershipRequest(space_ref=reference(space))
            ),
            timeout,
        )

    async def set_role(
        self, ref: Reference, role: Role, *, timeout: float | None = 30
    ) -> p.Membership:
        """Update a membership's role with a single read and write; no conflict retries."""
        roles = {
            "owner": p.MembershipSpecRole.OWNER,
            "admin": p.MembershipSpecRole.ADMIN,
            "user": p.MembershipSpecRole.USER,
        }
        if role not in roles:
            raise ValueError("Invalid membership role")
        async with self._engine.operation(timeout):
            value = await self.get(ref, timeout=None)
            value.spec.role = cast(p.MembershipSpecRole, roles[role])
            return await self.update(value, timeout=None)


class AsyncRegions:
    """Read-only regions capable of hosting Cordium workspaces."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def list(
        self, *, page: int = 0, page_size: int = 0, timeout: float | None = 30
    ) -> Page[p.Region]:
        """Fetch one page of regions; page numbers start at zero."""
        result = await self._engine.call(
            lambda: self._engine.raw.main.list_region(
                p.ListRegionOptions(common=_common(page, page_size, None, False))
            ),
            timeout,
        )
        return _page(result.items, result.list_response_meta)

    async def all(
        self, *, page: int = 0, page_size: int = 100, timeout: float | None = 30
    ) -> AsyncIterator[p.Region]:
        """Iterate region pages lazily; timeout applies per page."""
        while True:
            result = await self.list(page=page, page_size=page_size, timeout=timeout)
            for item in result.items:
                yield item
            if not result.has_more:
                return
            if not result.items or result.page != page:
                raise CordiumError("Non-progressing pagination", "PROTOCOL_ERROR")
            page = integer(page + 1, "page")


class AsyncUserConfig:
    """The caller's environment, tasks, dotfiles and region preferences."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get(self, *, timeout: float | None = 30) -> p.UserConfig:
        """Fetch the caller's complete configuration."""
        return await self._engine.call(
            lambda: self._engine.raw.main.get_user_config(p.GetUserConfigRequest()), timeout
        )

    async def update(self, resource: p.UserConfig, *, timeout: float | None = 30) -> p.UserConfig:
        """Replace a complete fetched configuration. Preserve its metadata and resource version."""
        return await self._engine.call(
            lambda: self._engine.raw.main.update_user_config(copy.deepcopy(resource)), timeout
        )

    async def modify(
        self, mutate: Callable[[p.UserConfig], None], *, timeout: float | None = 30
    ) -> p.UserConfig:
        """Fetch, edit with a synchronous callback and write once, preserving metadata.

        Callback exceptions abort the write; neither callbacks nor conflicts are
        retried. Blocking-client callbacks must not call blocking SDK methods.
        """
        async with self._engine.operation(timeout):
            value = await self.get(timeout=None)
            mutate(value)
            return await self.update(value, timeout=None)

    async def set_preferred_region(
        self, region: str, *, timeout: float | None = 30
    ) -> p.UserConfig:
        """Set the preferred placement region name; an empty string clears the preference."""

        def edit(value: p.UserConfig) -> None:
            value.spec.preferred_region = region

        return await self.modify(edit, timeout=timeout)

    async def set_dotfiles(
        self, url: str, *, branch: str = "", timeout: float | None = 30
    ) -> p.UserConfig:
        """Configure the dotfiles repository and install scripts run during PREPARING.

        Empty branch uses the repository default. Use modify() for authentication
        options of private repositories. This replaces the dotfiles configuration.
        """
        nonempty(url, "Dotfiles repository URL")

        def edit(value: p.UserConfig) -> None:
            value.spec.dotfiles = p.UserConfigSpecDotfiles(url=url, branch=branch)

        return await self.modify(edit, timeout=timeout)


class AsyncManagement:
    """Administrative configuration; requires Cluster administrator privileges."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def get_cluster_config(self, *, timeout: float | None = 30) -> p.ClusterConfig:
        """Read the singleton Cluster configuration."""
        return await self._engine.call(
            lambda: self._engine.raw.management.get_cluster_config(p.GetClusterConfigRequest()),
            timeout,
        )

    async def update_cluster_config(
        self, resource: p.ClusterConfig, *, timeout: float | None = 30
    ) -> p.ClusterConfig:
        """Replace the complete fetched Cluster configuration once, without conflict retries."""
        return await self._engine.call(
            lambda: self._engine.raw.management.update_cluster_config(copy.deepcopy(resource)),
            timeout,
        )

    async def modify_cluster_config(
        self, mutate: Callable[[p.ClusterConfig], None], *, timeout: float | None = 30
    ) -> p.ClusterConfig:
        """Fetch, edit and write the Cluster configuration once under one deadline.

        The synchronous callback receives the full resource. Keep metadata intact;
        exceptions abort the write and conflicts are not retried.
        """
        async with self._engine.operation(timeout):
            value = await self.get_cluster_config(timeout=None)
            mutate(value)
            return await self.update_cluster_config(value, timeout=None)
