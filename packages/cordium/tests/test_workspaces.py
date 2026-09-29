import asyncio

import betterproto
import pytest
from cordium import (
    Application,
    CordiumError,
    Ref,
    Resources,
    SecretRef,
    Task,
    VolumeMount,
    WorkspaceFailureError,
    create_workspace_spec,
    proto,
)
from grpclib.const import Status
from grpclib.exceptions import GRPCError


async def test_create_run_sources_and_cache(client, cluster):
    ws = await client.workspaces.run(
        template=Ref(uid="tpl"),
        image="python:3.13",
        env={"PLAIN": "x", "SECRET": SecretRef("key")},
        resources=Resources(cpu=500, memory=512),
        applications=[Application("api", 3000, default=True)],
        tasks=[Task(name="setup", command="true")],
        volumes=[VolumeMount("data.space", "/data")],
    )
    request = cluster.main.requests[0]
    assert request.status.template_ref.uid == "tpl"
    assert request.spec.runtime.env_vars[1].from_secret == "key"
    assert betterproto.which_one_of(request.spec.runtime.env_vars[0], "type")[0] == "value"
    assert request.spec.limit.cpu.millicores == 500
    assert request.spec.runtime.tasks[0].run == "true"
    assert request.spec.runtime.volume_mounts[0].mount_path == "/data"
    assert ws.is_running and ws.is_ready and not ws.is_starting
    assert ws.url == ws.app_url("api")
    assert ws.app_url("web") == "https://web_abc.cordium.example.test"
    assert ws.port_url(8080) == "https://port_8080_abc.cordium.example.test"
    copied = ws.proto
    copied.metadata.name = "mutated"
    assert ws.name == "abc"
    assert cluster.main.requests[-1].uid == "immutable"
    await ws.stop()
    assert ws.is_stopped
    assert (await ws.wait_until_stopped()).is_stopped
    await ws.delete()
    restored = await client.workspaces.create(snapshot="saved.space")
    assert restored.status.workspace_snapshot_ref.name == "saved.space"


async def test_pagination_is_lazy(client, cluster):
    items = client.workspaces.all(page_size=1)
    assert not cluster.main.pages
    assert (await anext(items)).name == "abc"
    assert cluster.main.pages == [0]
    assert (await anext(items)).name == "abc"
    with pytest.raises(StopAsyncIteration):
        await anext(items)
    assert cluster.main.pages == [0, 1]
    with pytest.raises(ValueError):
        await client.workspaces.list(space="s", template="t")


async def test_lifecycle_failure_and_deadlines(client, cluster):
    ws = await client.workspaces.get("abc")
    with pytest.raises(CordiumError, match="stopped") as stopped:
        await ws.wait_until_running()
    assert stopped.value.code == "WORKSPACE_STOPPED"
    cluster.main.value.status.failure = proto.WorkspaceStatusFailure(message="pull failed")
    with pytest.raises(WorkspaceFailureError) as failed:
        await ws.wait_until_ready()
    assert failed.value.workspace.metadata.name == "abc"
    cluster.main.delay = 1
    with pytest.raises(CordiumError) as timeout:
        await ws.refresh(timeout=0.01)
    assert timeout.value.code == "DEADLINE_EXCEEDED"
    task = asyncio.create_task(ws.refresh())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    cluster.main.delay = 0
    cluster.main.error = GRPCError(Status.PERMISSION_DENIED, "no permission")
    with pytest.raises(CordiumError) as denied:
        await ws.refresh()
    assert denied.value.code == "PERMISSION_DENIED"
    assert isinstance(denied.value.__cause__, GRPCError)


async def test_watch_and_logs_close(client, cluster):
    async with client.workspaces.watch() as events:
        event = await anext(events)
        assert event.create.item.metadata.name == "abc"
    await asyncio.wait_for(cluster.main.watch_closed.wait(), 2)
    ws = await client.workspaces.get("abc")
    async with ws.logs() as entries:
        assert [item.data async for item in entries] == [b"building\xff"]


async def test_client_close_cancels_pending(client, cluster):
    cluster.main.delay = 10
    task = asyncio.create_task(client.workspaces.get("abc"))
    await asyncio.sleep(0.02)
    await client.aclose()
    with pytest.raises(CordiumError) as closed:
        await task
    assert closed.value.code == "CLIENT_CLOSED"
    with pytest.raises(CordiumError):
        await client.workspaces.get("abc")


@pytest.mark.parametrize(
    "options",
    [
        dict(template="a", snapshot="b"),
        dict(snapshot="a", ephemeral=True),
        dict(resources=Resources(cpu=-1)),
        dict(applications=[Application("x", 0)]),
        dict(applications=[Application("x", 1, default=True), Application("y", 2, default=True)]),
        dict(volumes=[VolumeMount("v", "/../data")]),
        dict(tasks=[Task(name="x", command="true"), Task(name="x", command="true")]),
    ],
)
def test_invalid_spec(options):
    with pytest.raises(ValueError):
        create_workspace_spec(**options)


def test_spec_copy_and_zero_presence():
    original = proto.WorkspaceSpec(
        runtime=proto.WorkspaceSpecRuntime(
            env_vars=[proto.WorkspaceSpecRuntimeEnvVar(key="old", value="yes")]
        )
    )
    result = create_workspace_spec(spec=original, env={"EMPTY": ""}, disable_timeout=True)
    result = proto.WorkspaceSpec().parse(bytes(result))
    assert original.runtime.env_vars[0].key == "old"
    assert result.runtime.env_vars[0].key == "EMPTY"
    assert result.runtime.timeout.mode == proto.WorkspaceSpecRuntimeTimeoutMode.DISABLED
    assert betterproto.which_one_of(result.runtime.env_vars[0], "type")[0] == "value"
