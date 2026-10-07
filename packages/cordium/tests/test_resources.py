from unittest.mock import AsyncMock

import betterproto
import pytest
from cordium import CordiumError, Ref, Resources, SecretRef
from cordium import meta as m
from cordium import proto as p
from grpclib.const import Status
from grpclib.exceptions import GRPCError


@pytest.mark.parametrize(
    "collection,singular,resource",
    [
        ("spaces", "space", p.Space),
        ("templates", "template", p.Template),
        ("snapshots", "workspace_snapshot", p.WorkspaceSnapshot),
        ("volumes", "volume", p.Volume),
        ("secrets", "secret", p.Secret),
        ("user_secrets", "user_secret", p.UserSecret),
        ("git_providers", "git_provider", p.GitProvider),
        ("memberships", "membership", p.Membership),
    ],
)
async def test_resource_wire_crud_pages(
    client, cluster, monkeypatch, collection, singular, resource
):
    calls = []
    value = resource(metadata=m.Metadata(name="item.space", uid="id"))

    async def get(request):
        calls.append(request)
        return value

    async def delete(request):
        calls.append(request)
        return m.OperationResult()

    async def update(request):
        calls.append(request)
        return request

    async def listing(request):
        calls.append(request)
        return getattr(p, resource.__name__ + "List")(
            items=[value],
            list_response_meta=m.ListResponseMeta(
                page=request.common.page,
                items_per_page=1,
                has_more=request.common.page < 1,
                total_count=2,
            ),
        )

    monkeypatch.setattr(cluster.main, "get_" + singular, get)
    monkeypatch.setattr(cluster.main, "delete_" + singular, delete)
    monkeypatch.setattr(cluster.main, "list_" + singular, listing)
    api = getattr(client, collection)
    assert (await api.get(Ref(uid="id"))).metadata.uid == "id"
    assert calls[-1].uid == "id" and not calls[-1].name
    assert len([item async for item in api.all(page_size=1)]) == 2
    await api.delete("item.space")
    assert calls[-1].name == "item.space"
    if hasattr(api, "update"):
        monkeypatch.setattr(cluster.main, "update_" + singular, update)
        assert (await api.update(value)).metadata.uid == "id"


async def test_create_resource_shapes(client, cluster, monkeypatch):
    received = {}

    def creator(name):
        async def run(request):
            if not betterproto.serialized_on_wire(request.spec) or (
                name in ("secret", "user_secret")
                and not betterproto.serialized_on_wire(request.data)
            ):
                raise GRPCError(Status.INVALID_ARGUMENT, "Resource spec and data must be set")
            received[name] = request
            return request

        return run

    for kind in (
        "space",
        "template",
        "workspace_snapshot",
        "volume",
        "secret",
        "user_secret",
        "git_provider",
    ):
        monkeypatch.setattr(cluster.main, "create_" + kind, creator(kind))
    space = await client.spaces.create(
        "team",
        organization=True,
        default_resources=Resources(cpu=100),
        env={"X": SecretRef("x")},
        disable_ssh=True,
    )
    assert space.metadata.name == "team.cordium"
    assert space.spec.limit.default_limit.cpu.millicores == 100
    assert space.spec.authorization.disable_ssh
    assert (await client.spaces.create("bare")).metadata.name == "bare"
    assert (await client.templates.create("bare.team.cordium")).metadata.name == "bare.team.cordium"
    template = await client.templates.create(
        "python.team.cordium",
        image="python:3.13",
        repository="https://example.test/repo",
        git_provider="git",
    )
    assert template.spec.image.registry.url == "python:3.13"
    assert template.spec.git_provider == "git"
    snapshot = await client.snapshots.create("saved.team.cordium", Ref(uid="ws"))
    assert snapshot.status.workspace_ref.uid == "ws"
    volume = await client.volumes.create(
        "data.team.cordium", size=100, shared=True, region=Ref(uid="region")
    )
    assert volume.spec.size.megabytes == 100
    assert volume.spec.access_mode == p.VolumeAccessMode.ACCESS_MODE_SHARED
    assert volume.status.region_ref.uid == "region"
    secret = await client.secrets.create(
        "config.team.cordium", {"number": 3, "nested": [True, None, {"text": "abc"}]}
    )
    fields = secret.data.attrs.fields
    assert fields["number"].number_value == 3
    assert fields["nested"].list_value.values[2].struct_value.fields["text"].string_value == "abc"
    binary = await client.user_secrets.create("binary", b"\x00\xff")
    assert binary.data.value_bytes == b"\x00\xff"
    key = await client.user_secrets.create_ssh_key("key")
    assert key.spec.type == p.UserSecretSpecType.SSH_KEY
    assert betterproto.serialized_on_wire(key.data)
    assert betterproto.which_one_of(key.data, "type")[0] == ""
    provider = await client.git_providers.create_oauth(
        "git.team.cordium",
        "github",
        client_id="client",
        client_secret="secret-name",
        scopes=("repo",),
    )
    assert provider.spec.github.client_secret.from_secret == "secret-name"


async def test_volume_growth_and_snapshot_readiness(client, cluster, monkeypatch):
    volume = p.Volume(
        metadata=m.Metadata(name="v"),
        spec=p.VolumeSpec(size=p.VolumeSpecSize(megabytes=100)),
        status=p.VolumeStatus(
            capacity=p.VolumeSpecSize(megabytes=120), state=p.VolumeStatusState.STATE_READY
        ),
    )
    monkeypatch.setattr(cluster.main, "get_volume", AsyncMock(return_value=volume))
    update = AsyncMock(side_effect=lambda value: value)
    monkeypatch.setattr(cluster.main, "update_volume", update)
    with pytest.raises(ValueError):
        await client.volumes.grow("v", 110)
    assert not update.called
    assert (await client.volumes.grow("v", 150)).spec.size.megabytes == 150
    assert (
        await client.volumes.wait_until_ready("v")
    ).status.state == p.VolumeStatusState.STATE_READY
    snapshot = p.WorkspaceSnapshot(
        status=p.WorkspaceSnapshotStatus(
            state=p.WorkspaceSnapshotStatusState.STATE_FAILED,
            failure=p.WorkspaceSnapshotStatusFailure(message="storage"),
        )
    )
    monkeypatch.setattr(cluster.main, "get_workspace_snapshot", AsyncMock(return_value=snapshot))
    with pytest.raises(CordiumError, match="storage"):
        await client.snapshots.wait_until_ready("s")
    with pytest.raises(ValueError):
        await client.snapshots.list(space="s", workspace="w")


async def test_membership_oneofs_and_role(client, cluster, monkeypatch):
    member = p.Membership(metadata=m.Metadata(name="m"))
    created = AsyncMock(return_value=member)
    monkeypatch.setattr(cluster.main, "create_membership", created)
    monkeypatch.setattr(cluster.main, "get_membership", AsyncMock(return_value=member))
    update = AsyncMock(side_effect=lambda value: value)
    monkeypatch.setattr(cluster.main, "update_membership", update)
    await client.memberships.add("space", email="a@example.test", role="admin")
    assert created.call_args.args[0].email == "a@example.test"
    await client.memberships.add("space", user=Ref(uid="u"))
    assert created.call_args.args[0].user_ref.uid == "u"
    assert (await client.memberships.set_role("m", "owner")).spec.role == p.MembershipSpecRole.OWNER
    with pytest.raises(ValueError):
        await client.memberships.add("s", email="a", user="u")


async def test_build_wait_uses_exact_id(client, cluster, monkeypatch):
    old = p.TemplateStatusBuildInfoBuild(
        id="old", state=p.TemplateStatusBuildInfoBuildState.STATE_READY
    )
    new = p.TemplateStatusBuildInfoBuild(
        id="new", state=p.TemplateStatusBuildInfoBuildState.STATE_RUNNING
    )
    value = p.Template(
        status=p.TemplateStatus(build_info=p.TemplateStatusBuildInfo(builds=[old, new]))
    )
    monkeypatch.setattr(cluster.main, "get_template", AsyncMock(return_value=value))
    with pytest.raises(CordiumError) as timeout:
        await client.templates.wait_for_build("t", "new", timeout=0.02, poll_interval=0.01)
    assert timeout.value.code == "DEADLINE_EXCEEDED"
    new.state = p.TemplateStatusBuildInfoBuildState.STATE_READY
    assert (await client.templates.wait_for_build("t", "new")).status.build_info.builds[
        -1
    ].id == "new"
    with pytest.raises(CordiumError) as missing:
        await client.templates.wait_for_build("t", "absent")
    assert missing.value.code == "NOT_FOUND"


async def test_nonprogressing_pagination(client, cluster, monkeypatch):
    async def bad(request):
        return p.TemplateList(list_response_meta=m.ListResponseMeta(has_more=True))

    monkeypatch.setattr(cluster.main, "list_template", bad)
    with pytest.raises(CordiumError) as error:
        await anext(client.templates.all())
    assert error.value.code == "PROTOCOL_ERROR"


async def test_modify_once_and_callback_failure(client, cluster, monkeypatch):
    value = p.Template(metadata=m.Metadata(name="t", display_name="before"))
    monkeypatch.setattr(cluster.main, "get_template", AsyncMock(return_value=value))
    update = AsyncMock(side_effect=lambda item: item)
    monkeypatch.setattr(cluster.main, "update_template", update)

    def rename(item):
        item.metadata.display_name = "after"

    assert (await client.templates.modify("t", rename)).metadata.display_name == "after"
    assert update.call_count == 1
    assert value.metadata.display_name == "before"

    def fail(item):
        raise RuntimeError("abort edit")

    with pytest.raises(RuntimeError, match="abort edit"):
        await client.templates.modify("t", fail)
    assert update.call_count == 1
    ws = await client.workspaces.get("abc")
    assert (await ws.modify(rename)).display_name == "after"
    assert ws.display_name == "after"


async def test_template_options_and_regions_config(client, cluster, monkeypatch):
    from cordium import Task

    monkeypatch.setattr(cluster.main, "create_template", AsyncMock(side_effect=lambda item: item))
    template = await client.templates.create(
        "t",
        env={"x": "y"},
        resources=Resources(cpu=300),
        vars={"branch": "main"},
        tasks=[Task(name="prepare", command="true")],
        disable_timeout=True,
    )
    assert template.spec.runtime.tasks[0].run == "true"
    assert template.spec.runtime.env_vars[0].value == "y"
    assert template.spec.limit.cpu.millicores == 300
    assert template.spec.vars[0].value == "main"
    monkeypatch.setattr(
        cluster.main,
        "list_region",
        AsyncMock(return_value=p.RegionList(items=[p.Region(metadata=m.Metadata(name="region"))])),
    )
    assert [item.metadata.name async for item in client.regions.all()] == ["region"]
    monkeypatch.setattr(cluster.main, "get_user_config", AsyncMock(return_value=p.UserConfig()))
    monkeypatch.setattr(
        cluster.main, "update_user_config", AsyncMock(side_effect=lambda item: item)
    )
    config = await client.user_config.get()
    assert isinstance(await client.user_config.update(config), p.UserConfig)


@pytest.mark.parametrize(
    "value", [{"bad": float("nan")}, {"bad": float("inf")}, {"bad": 2**53 + 1}]
)
async def test_secret_rejects_lossy_numbers(client, value):
    with pytest.raises(ValueError):
        await client.secrets.create("s", value)


async def test_preferences_oauth2_and_snapshot_shortcut(client, cluster, monkeypatch):
    monkeypatch.setattr(cluster.main, "get_user_config", AsyncMock(return_value=p.UserConfig()))
    monkeypatch.setattr(
        cluster.main, "update_user_config", AsyncMock(side_effect=lambda item: item)
    )
    assert (await client.user_config.set_preferred_region("eu")).spec.preferred_region == "eu"
    assert (
        await client.user_config.set_dotfiles("https://example.test/dotfiles", branch="main")
    ).spec.dotfiles.branch == "main"
    monkeypatch.setattr(
        cluster.main, "create_git_provider", AsyncMock(side_effect=lambda item: item)
    )
    provider = await client.git_providers.create_oauth2(
        "custom.space",
        client_id="id",
        client_secret="secret",
        auth_url="https://git.test/auth",
        token_url="https://git.test/token",
        scopes=("read",),
    )
    assert provider.spec.oauth2.client_secret.from_secret == "secret"
    assert provider.spec.oauth2.scopes == ["read"]
    monkeypatch.setattr(
        cluster.main, "create_workspace_snapshot", AsyncMock(side_effect=lambda item: item)
    )
    workspace = await client.workspaces.get("abc")
    assert (await workspace.snapshot("saved.space")).status.workspace_ref.uid == "immutable"


async def test_management_wire(tls):
    from conftest import serve
    from cordium import AccessToken, AsyncCordium

    class Management(p.ManagementServiceBase):
        async def get_cluster_config(self, request):
            return p.ClusterConfig(metadata=m.Metadata(name="cluster"))

        async def update_cluster_config(self, request):
            return request

    server, connection = await serve(tls, Management())

    def edit(config):
        config.metadata.display_name = "changed"

    try:
        async with AsyncCordium("example.test", auth=AccessToken("token"), **connection) as client:
            value = await client.management.modify_cluster_config(edit)
            assert value.metadata.name == "cluster" and value.metadata.display_name == "changed"
    finally:
        server.close()
        await server.wait_closed()
