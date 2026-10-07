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
    meta,
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
    restored = await client.workspaces.create(template="t", snapshot="saved.space", ephemeral=True)
    assert restored.status.template_ref.name == "t"
    assert restored.spec.is_ephemeral


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
    cluster.main.value.status.run = proto.WorkspaceStatusRun(id="second")
    cluster.main.value.status.state = proto.WorkspaceStatusState.RUNNING
    assert (await ws.wait_until_running()).is_running
    cluster.main.value.status.run.failure = proto.WorkspaceStatusFailure(message="task failed")
    cluster.main.value.status.state = proto.WorkspaceStatusState.PREPARING
    with pytest.raises(WorkspaceFailureError, match="task failed"):
        await ws.wait_until_running()
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
    previous = proto.Workspace(metadata=meta.Metadata(name="before"))
    cluster.main.watch_updates = [
        proto.WatchWorkspaceResponseUpdate(new_item=cluster.main.value, old_item=previous),
        proto.WatchWorkspaceResponseUpdate(new_item=cluster.main.value),
    ]
    async with client.workspaces.watch() as events:
        created, updated, unchanged = [await anext(events) for _ in range(3)]
        assert created.type == "create" and created.workspace.metadata.name == "abc"
        assert updated.type == "update" and updated.previous.metadata.name == "before"
        assert unchanged.previous is None
    await asyncio.wait_for(cluster.main.watch_closed.wait(), 2)
    ws = await client.workspaces.get("abc")
    async with ws.logs() as entries:
        logs = [entry async for entry in entries]
    assert [(entry.stage, entry.stream, entry.data) for entry in logs] == [
        ("building_image", "stderr", b"building\xff")
    ]


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
        dict(resources=Resources(cpu=-1)),
        dict(applications=[Application("x", 0)]),
        dict(applications=[Application("x", 1, default=True), Application("y", 2, default=True)]),
        dict(volumes=[VolumeMount("v", "/../data")]),
        dict(tasks=[Task(name="x", command="true"), Task(name="x", command="true")]),
        dict(env={"EMPTY": ""}),
        dict(tasks=[Task(name="x", command="true", env={"EMPTY": ""})]),
    ],
)
def test_invalid_spec(options):
    with pytest.raises(ValueError):
        create_workspace_spec(**options)


def test_whitespace_environment_values_are_kept():
    spec = create_workspace_spec(
        env={"IFS": " "}, tasks=[Task(name="x", command="true", env={"IFS": " "})]
    )
    assert spec.runtime.env_vars[0].value == spec.runtime.tasks[0].env_vars[0].value == " "


def test_spec_copy_and_zero_presence():
    original = proto.WorkspaceSpec(
        runtime=proto.WorkspaceSpecRuntime(
            env_vars=[proto.WorkspaceSpecRuntimeEnvVar(key="old", value="yes")]
        )
    )
    result = create_workspace_spec(spec=original, env={"BLANK": " "}, disable_timeout=True)
    result = proto.WorkspaceSpec().parse(bytes(result))
    assert original.runtime.env_vars[0].key == "old"
    assert result.runtime.env_vars[0].key == "BLANK"
    assert result.runtime.env_vars[0].value == " "
    assert result.runtime.timeout.mode == proto.WorkspaceSpecRuntimeTimeoutMode.DISABLED
    assert betterproto.which_one_of(result.runtime.env_vars[0], "type")[0] == "value"


async def test_start_is_idempotent_and_failed_runs_fail_stop_waits(client, cluster):
    cluster.main.value.status.space_ref = meta.ObjectReference(name="team")
    cluster.main.value.status.region_ref = meta.ObjectReference(name="eu")
    cluster.main.value.status.limit = proto.WorkspaceSpecLimit(
        cpu=proto.WorkspaceSpecLimitCpu(millicores=500)
    )
    ws = await client.workspaces.get("abc")
    await ws.start()
    assert ws.is_running
    await ws.start()
    assert ws.is_running
    assert ws.space_name == "team" and ws.region_name == "eu" and ws.template_name == ""
    assert ws.limit.cpu.millicores == 500 and not ws.is_ephemeral
    assert ws.failure is None
    await ws.stop()
    cluster.main.value.status.run = proto.WorkspaceStatusRun(
        id="run", failure=proto.WorkspaceStatusFailure(message="task failed")
    )
    with pytest.raises(WorkspaceFailureError, match="task failed"):
        await ws.wait_until_stopped()
    assert ws.is_stopped and ws.failure.message == "task failed"
    cluster.main.value.status.run = proto.WorkspaceStatusRun(id="next")
    assert (await ws.wait_until_stopped()).is_stopped


async def test_run_errors_keep_their_code_and_the_workspace(client, cluster):
    async def initializing(request):
        cluster.main.value.status.state = proto.WorkspaceStatusState.INITIALIZING
        return proto.StartWorkspaceResponse()

    cluster.main.start_workspace = initializing
    with pytest.raises(CordiumError) as deadline:
        await client.workspaces.run(timeout=0.2, poll_interval=0.01)
    assert type(deadline.value) is CordiumError
    assert deadline.value.code == "DEADLINE_EXCEEDED"
    assert deadline.value.workspace.metadata.uid == "immutable"
    cluster.main.value.status.state = proto.WorkspaceStatusState.STOPPED
    cluster.main.value.status.failure = proto.WorkspaceStatusFailure(message="pull failed")
    with pytest.raises(WorkspaceFailureError) as failed:
        await client.workspaces.run()
    assert failed.value.workspace.status.failure.message == "pull failed"
