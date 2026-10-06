import importlib.util
import json
import ssl
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import betterproto
import pytest
from betterproto.lib.google.protobuf import Struct, Value
from grpclib.const import Status
from grpclib.events import RecvRequest, listen
from grpclib.exceptions import GRPCError
from grpclib.server import Server
from octelium.api.main.core import v1 as c
from octelium.api.main.meta import v1 as m
from octelium.sdk import AuthConfig, OcteliumClient, OcteliumClientConfig

examples_path = Path(__file__).resolve().parents[1] / "examples"
resource_types = {
    "user": c.User,
    "service": c.Service,
    "policy": c.Policy,
    "credential": c.Credential,
    "group": c.Group,
    "namespace": c.Namespace,
}
list_types = {
    "user": c.UserList,
    "service": c.ServiceList,
    "policy": c.PolicyList,
    "credential": c.CredentialList,
    "group": c.GroupList,
    "namespace": c.NamespaceList,
}
resource_examples = [
    ("users", "user"),
    ("services", "service"),
    ("policies", "policy"),
    ("credentials", "credential"),
    ("groups", "group"),
    ("namespaces", "namespace"),
]


@pytest.fixture(scope="session")
def sdk_examples():
    modules = {}
    for path in sorted(examples_path.glob("*.py")):
        name = f"sdk_example_{path.stem}"
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        modules[path.stem] = module
    yield SimpleNamespace(**modules)
    for path in examples_path.glob("*.py"):
        sys.modules.pop(f"sdk_example_{path.stem}", None)


def resource(kind, name):
    attrs = Struct(fields={"environment": Value(string_value="production")})
    specs = {
        "user": c.UserSpec(
            type=c.UserSpecType.from_string("HUMAN"),
            email="alice@example.com",
            groups=["engineers"],
            attrs=attrs,
            is_disabled=True,
        ),
        "service": c.ServiceSpec(
            mode=c.ServiceSpecMode.from_string("HTTP"),
            is_public=True,
            config=c.ServiceSpecConfig(
                name="default", upstream=c.ServiceSpecConfigUpstream(url="http://old-backend:8080")
            ),
            authorization=c.ServiceSpecAuthorization(policies=["access"]),
            attrs=attrs,
            is_disabled=True,
        ),
        "policy": c.PolicySpec(
            rules=[
                c.PolicySpecRule(
                    name="allow-access",
                    condition=c.Condition(match='ctx.user.metadata.name == "alice"'),
                    effect=c.PolicySpecRuleEffect.from_string("ALLOW"),
                ),
                c.PolicySpecRule(
                    name="blocked",
                    condition=c.Condition(match="false"),
                    effect=c.PolicySpecRuleEffect.from_string("DENY"),
                    priority=50,
                ),
            ],
            attrs=attrs,
            is_disabled=True,
        ),
        "credential": c.CredentialSpec(
            type=c.CredentialSpecType.from_string("AUTH_TOKEN"),
            user="ci-agent",
            expires_at=datetime.now(UTC) + timedelta(hours=24),
            session_type=c.SessionStatusType.from_string("CLIENTLESS"),
            max_authentications=1,
            authorization=c.CredentialSpecAuthorization(policies=["access"]),
            is_disabled=True,
        ),
        "group": c.GroupSpec(
            authorization=c.GroupSpecAuthorization(policies=["access"]), attrs=attrs
        ),
        "namespace": c.NamespaceSpec(
            authorization=c.NamespaceSpecAuthorization(policies=["access"]), attrs=attrs
        ),
    }
    return resource_types[kind](
        metadata=m.Metadata(
            name=name,
            uid=str(uuid4()),
            display_name="Original display name",
            description="Preserve this description",
            labels={"team": "engineering"},
            tags=["production"],
        ),
        spec=specs[kind],
    )


class ExampleCore(c.MainServiceBase):
    def __init__(self):
        self.resources = {kind: {} for kind in resource_types}
        self.calls = []
        self.errors = {}
        self.rotations = {}
        self.config = c.ClusterConfig(
            metadata=m.Metadata(name="cluster", labels={"environment": "production"}),
            spec=c.ClusterConfigSpec(
                ingress=c.ClusterConfigSpecIngress(
                    use_forwarded_for_header=True, xff_num_trusted_hops=2
                ),
                authorization=c.ClusterConfigSpecAuthorization(policies=["access"]),
                session=c.ClusterConfigSpecSession(
                    human=c.ClusterConfigSpecSessionHuman(
                        max_per_user=3, access_token_duration=m.Duration(hours=1)
                    ),
                    workload=c.ClusterConfigSpecSessionWorkload(
                        max_per_user=4, refresh_token_duration=m.Duration(days=7)
                    ),
                ),
            ),
        )

    def record(self, method, request):
        self.calls.append((method, request))
        if method in self.errors:
            raise self.errors[method]

    def create(self, kind, request):
        self.record(f"create_{kind}", request)
        if request.metadata.name in self.resources[kind]:
            raise GRPCError(Status.ALREADY_EXISTS, "Resource exists")
        if not betterproto.serialized_on_wire(request.spec):
            raise GRPCError(Status.INVALID_ARGUMENT, "Missing spec")
        request.metadata.uid = str(uuid4())
        self.resources[kind][request.metadata.name] = request
        return request

    def get(self, kind, request):
        self.record(f"get_{kind}", request)
        if request.name not in self.resources[kind]:
            raise GRPCError(Status.NOT_FOUND, "Resource not found")
        return self.resources[kind][request.name]

    def update(self, kind, request):
        self.record(f"update_{kind}", request)
        if request.metadata.name not in self.resources[kind]:
            raise GRPCError(Status.NOT_FOUND, "Resource not found")
        if not betterproto.serialized_on_wire(request.spec):
            raise GRPCError(Status.INVALID_ARGUMENT, "Missing spec")
        self.resources[kind][request.metadata.name] = request
        return request

    def delete(self, kind, request):
        self.record(f"delete_{kind}", request)
        if request.name not in self.resources[kind]:
            raise GRPCError(Status.NOT_FOUND, "Resource not found")
        del self.resources[kind][request.name]
        return m.OperationResult()

    def listing(self, kind, request):
        self.record(f"list_{kind}", request)
        items = sorted(self.resources[kind].values(), key=lambda item: item.metadata.name)
        if kind == "service" and request.namespace_ref.name:
            items = [
                item
                for item in items
                if item.metadata.name.endswith(f".{request.namespace_ref.name}")
            ]
        if kind == "credential" and request.user_ref.name:
            items = [item for item in items if item.spec.user == request.user_ref.name]
        page, limit = request.common.page, min(request.common.items_per_page or 100, 2)
        start = page * limit
        return list_types[kind](
            items=items[start : start + limit],
            list_response_meta=m.ListResponseMeta(
                page=page,
                items_per_page=limit,
                total_count=len(items),
                has_more=start + limit < len(items),
            ),
        )

    async def create_user(self, request):
        return self.create("user", request)

    async def get_user(self, request):
        return self.get("user", request)

    async def update_user(self, request):
        return self.update("user", request)

    async def delete_user(self, request):
        return self.delete("user", request)

    async def list_user(self, request):
        return self.listing("user", request)

    async def create_service(self, request):
        return self.create("service", request)

    async def get_service(self, request):
        return self.get("service", request)

    async def update_service(self, request):
        return self.update("service", request)

    async def delete_service(self, request):
        return self.delete("service", request)

    async def list_service(self, request):
        return self.listing("service", request)

    async def create_policy(self, request):
        return self.create("policy", request)

    async def get_policy(self, request):
        return self.get("policy", request)

    async def update_policy(self, request):
        return self.update("policy", request)

    async def delete_policy(self, request):
        return self.delete("policy", request)

    async def list_policy(self, request):
        return self.listing("policy", request)

    async def create_credential(self, request):
        return self.create("credential", request)

    async def get_credential(self, request):
        return self.get("credential", request)

    async def update_credential(self, request):
        return self.update("credential", request)

    async def delete_credential(self, request):
        return self.delete("credential", request)

    async def list_credential(self, request):
        return self.listing("credential", request)

    async def create_group(self, request):
        return self.create("group", request)

    async def get_group(self, request):
        return self.get("group", request)

    async def update_group(self, request):
        return self.update("group", request)

    async def delete_group(self, request):
        return self.delete("group", request)

    async def list_group(self, request):
        return self.listing("group", request)

    async def create_namespace(self, request):
        return self.create("namespace", request)

    async def get_namespace(self, request):
        return self.get("namespace", request)

    async def update_namespace(self, request):
        return self.update("namespace", request)

    async def delete_namespace(self, request):
        return self.delete("namespace", request)

    async def list_namespace(self, request):
        return self.listing("namespace", request)

    async def get_cluster_config(self, request):
        self.record("get_cluster_config", request)
        return self.config

    async def update_cluster_config(self, request):
        self.record("update_cluster_config", request)
        self.config = request
        return request

    async def generate_credential_token(self, request):
        self.record("generate_credential_token", request)
        credential = self.resources["credential"][request.credential_ref.name]
        name = credential.metadata.name
        rotation = self.rotations.get(name, 0) + 1
        self.rotations[name] = rotation
        if credential.spec.type == c.CredentialSpecType.AUTH_TOKEN:
            return c.CredentialToken(
                authentication_token=c.CredentialTokenAuthenticationToken(
                    authentication_token=f"auth-secret-{rotation}"
                )
            )
        if credential.spec.type == c.CredentialSpecType.OAUTH2:
            return c.CredentialToken(
                oauth2_credentials=c.CredentialTokenOAuth2Credentials(
                    client_id="worker-id", client_secret=f"oauth-secret-{rotation}"
                )
            )
        return c.CredentialToken(
            access_token=c.CredentialTokenAccessToken(access_token=f"access-secret-{rotation}")
        )


@pytest.fixture
async def example_core_server(sdk_tls):
    core, headers, clients = ExampleCore(), [], []
    server = Server([core])

    async def record(event):
        assert event.peer._transport.get_extra_info("ssl_object").selected_alpn_protocol() == "h2"
        headers.append(dict(event.metadata))

    listen(server, RecvRequest, record)
    await server.start("127.0.0.1", 0, ssl=sdk_tls.server)
    port = server._server.sockets[0].getsockname()[1]

    async def create_client(config=None):
        client = OcteliumClient(
            OcteliumClientConfig(
                domain="example.test",
                auth=AuthConfig(type="access_token", access_token="administrator-access"),
                api_host="127.0.0.1",
                api_port=port,
                tls_server_name="localhost",
                ssl_context_factory=lambda: ssl.create_default_context(cafile=str(sdk_tls.cert)),
            )
        )
        clients.append(client)
        return client

    yield SimpleNamespace(core=core, headers=headers, clients=clients, create_client=create_client)
    for client in clients:
        await client.close()
    server.close()
    await server.wait_closed()


@pytest.fixture
async def example_core_client(example_core_server):
    client = await example_core_server.create_client()
    yield client
    await client.close()


@pytest.mark.parametrize("path", sorted(examples_path.glob("*.py")), ids=lambda path: path.stem)
def test_core_example_help_runs_without_credentials(path):
    result = subprocess.run(
        [sys.executable, str(path), "--help"], check=True, capture_output=True, text=True
    )
    assert "usage:" in result.stdout
    assert "get" in result.stdout and "update" in result.stdout


@pytest.mark.parametrize("module,kind", resource_examples)
async def test_core_lists_follow_every_page(
    sdk_examples, example_core_server, example_core_client, module, kind
):
    store = example_core_server.core.resources[kind]
    for name in ("delta", "alpha", "gamma", "beta", "epsilon"):
        store[name] = resource(kind, name)
    items = [
        item
        async for item in getattr(getattr(sdk_examples, module), f"list_{module}")(
            example_core_client
        )
    ]
    assert [item.metadata.name for item in items] == ["alpha", "beta", "delta", "epsilon", "gamma"]
    calls = example_core_server.core.calls
    assert [request.common.page for method, request in calls if method == f"list_{kind}"] == [
        0,
        1,
        2,
    ]
    assert example_core_server.headers == [{"x-octelium-auth": "administrator-access"}] * 3


@pytest.mark.parametrize("module,kind", resource_examples)
async def test_core_empty_lists_stop_after_first_page(
    sdk_examples, example_core_server, example_core_client, module, kind
):
    items = [
        item
        async for item in getattr(getattr(sdk_examples, module), f"list_{module}")(
            example_core_client
        )
    ]
    assert items == []
    assert len(example_core_server.core.calls) == 1


@pytest.mark.parametrize("module,kind", resource_examples)
async def test_core_updates_preserve_unselected_fields(
    sdk_examples, example_core_server, example_core_client, module, kind
):
    original = resource(kind, "existing")
    example_core_server.core.resources[kind]["existing"] = original
    updates = {
        "user": {"email": "alice@new-domain.example", "disabled": False},
        "service": {"upstream_url": "http://new-backend:8080", "disabled": False, "public": False},
        "policy": {"match": 'ctx.user.metadata.name == "bob"', "disabled": False},
        "credential": {"disabled": False},
        "group": {"display_name": "New display name"},
        "namespace": {"display_name": "New display name"},
    }
    updated = await getattr(getattr(sdk_examples, module), f"update_{kind}")(
        example_core_client, "existing", **updates[kind]
    )
    assert updated.metadata.uid == original.metadata.uid
    assert updated.metadata.description == original.metadata.description
    assert updated.metadata.labels == original.metadata.labels
    assert updated.metadata.tags == original.metadata.tags
    if kind in {"group", "namespace"}:
        assert updated.metadata.display_name == "New display name"
        assert bytes(updated.spec) == bytes(original.spec)
    else:
        assert updated.metadata.display_name == original.metadata.display_name
        assert not updated.spec.is_disabled
    if kind == "user":
        assert updated.spec.email == "alice@new-domain.example"
        assert updated.spec.groups == original.spec.groups
        assert bytes(updated.spec.attrs) == bytes(original.spec.attrs)
    elif kind == "service":
        assert updated.spec.config.upstream.url == "http://new-backend:8080"
        assert not updated.spec.is_public
        assert updated.spec.config.name == original.spec.config.name
        assert bytes(updated.spec.authorization) == bytes(original.spec.authorization)
        assert bytes(updated.spec.attrs) == bytes(original.spec.attrs)
    elif kind == "policy":
        assert updated.spec.rules[0].condition.match == 'ctx.user.metadata.name == "bob"'
        assert bytes(updated.spec.rules[1]) == bytes(original.spec.rules[1])
        assert bytes(updated.spec.attrs) == bytes(original.spec.attrs)
    elif kind == "credential":
        assert updated.spec.user == original.spec.user
        assert updated.spec.expires_at == original.spec.expires_at
        assert updated.spec.max_authentications == original.spec.max_authentications
        assert bytes(updated.spec.authorization) == bytes(original.spec.authorization)
    assert [method for method, request in example_core_server.core.calls] == [
        f"get_{kind}",
        f"update_{kind}",
    ]


@pytest.mark.parametrize("module,kind", resource_examples)
async def test_core_delete_uses_resource_name(
    sdk_examples, example_core_server, example_core_client, module, kind
):
    example_core_server.core.resources[kind]["obsolete"] = resource(kind, "obsolete")
    await getattr(getattr(sdk_examples, module), f"delete_{kind}")(example_core_client, "obsolete")
    assert example_core_server.core.resources[kind] == {}
    method, request = example_core_server.core.calls[0]
    assert method == f"delete_{kind}" and request.name == "obsolete" and request.uid == ""
    with pytest.raises(GRPCError) as exc:
        await getattr(getattr(sdk_examples, module), f"delete_{kind}")(
            example_core_client, "obsolete"
        )
    assert exc.value.status == Status.NOT_FOUND


async def test_user_creation_distinguishes_humans_and_workloads(sdk_examples, example_core_client):
    human = await sdk_examples.users.create_user(
        example_core_client,
        "alice",
        user_type=c.UserSpecType.from_string("HUMAN"),
        email="alice@example.com",
        groups=["engineers"],
        display_name="Alice",
    )
    workload = await sdk_examples.users.create_user(
        example_core_client, "ci-agent", user_type=c.UserSpecType.from_string("WORKLOAD")
    )
    assert human.spec.type == c.UserSpecType.HUMAN and human.spec.email == "alice@example.com"
    assert human.spec.groups == ["engineers"] and human.metadata.display_name == "Alice"
    assert workload.spec.type == c.UserSpecType.WORKLOAD and workload.spec.email == ""


async def test_service_creation_configures_upstream_and_attached_policy(
    sdk_examples, example_core_client
):
    service = await sdk_examples.services.create_service(
        example_core_client,
        "reports.production",
        "http://reports.production.svc.cluster.local:8080",
        policies=["reports-access"],
        public=True,
    )
    assert service.spec.mode == c.ServiceSpecMode.HTTP and service.spec.is_public
    assert not service.spec.is_anonymous
    assert service.spec.config.upstream.url == "http://reports.production.svc.cluster.local:8080"
    assert service.spec.authorization.policies == ["reports-access"]


async def test_policy_creation_and_unknown_rule_error(
    sdk_examples, example_core_client, example_core_server
):
    policy = await sdk_examples.policies.create_policy(
        example_core_client, "reports-access", 'ctx.user.metadata.name == "ci-agent"'
    )
    assert policy.spec.rules[0].name == "allow-access"
    assert policy.spec.rules[0].effect == c.PolicySpecRuleEffect.ALLOW
    assert policy.spec.rules[0].condition.match == 'ctx.user.metadata.name == "ci-agent"'
    with pytest.raises(ValueError, match="no rule named missing"):
        await sdk_examples.policies.update_policy(
            example_core_client, "reports-access", rule_name="missing", match="false"
        )
    assert not any(method == "update_policy" for method, request in example_core_server.core.calls)


@pytest.mark.parametrize("module,kind", [("groups", "group"), ("namespaces", "namespace")])
async def test_group_and_namespace_policy_lists_can_be_cleared(
    sdk_examples, example_core_client, module, kind
):
    example = getattr(sdk_examples, module)
    created = await getattr(example, f"create_{kind}")(example_core_client, "team", ["access"])
    assert created.spec.authorization.policies == ["access"]
    updated = await getattr(example, f"update_{kind}")(example_core_client, "team", policies=[])
    assert updated.spec.authorization.policies == []
    assert betterproto.serialized_on_wire(updated.spec)


async def test_list_filters_by_service_namespace_and_credential_user(
    sdk_examples, example_core_server, example_core_client
):
    core = example_core_server.core
    for name in ("one.production", "two.production", "three.production", "other.staging"):
        core.resources["service"][name] = resource("service", name)
    for name, user in (
        ("one", "worker"),
        ("two", "worker"),
        ("three", "worker"),
        ("other", "other-worker"),
    ):
        credential = resource("credential", name)
        credential.spec.user = user
        core.resources["credential"][name] = credential
    services = [
        item
        async for item in sdk_examples.services.list_services(
            example_core_client, namespace="production"
        )
    ]
    credentials = [
        item
        async for item in sdk_examples.credentials.list_credentials(
            example_core_client, user="worker"
        )
    ]
    assert len(services) == len(credentials) == 3
    assert all(item.metadata.name.endswith(".production") for item in services)
    assert all(item.spec.user == "worker" for item in credentials)


@pytest.mark.parametrize("credential_type", ["AUTH_TOKEN", "OAUTH2", "ACCESS_TOKEN"])
async def test_credential_creation_is_separate_from_generation_and_rotation(
    sdk_examples, example_core_server, example_core_client, credential_type
):
    before = datetime.now(UTC)
    credential = await sdk_examples.credentials.create_credential(
        example_core_client,
        "worker-credential",
        "worker",
        credential_type=c.CredentialSpecType.from_string(credential_type),
        policies=["access"],
    )
    after = datetime.now(UTC)
    assert credential.spec.user == "worker"
    assert credential.spec.session_type == c.SessionStatusType.CLIENTLESS
    assert credential.spec.authorization.policies == ["access"]
    assert before + timedelta(hours=24) <= credential.spec.expires_at <= after + timedelta(hours=24)
    assert credential.spec.max_authentications == (1 if credential_type == "AUTH_TOKEN" else 0)
    assert [method for method, request in example_core_server.core.calls] == ["create_credential"]
    first = await sdk_examples.credentials.generate_credential_token(
        example_core_client, "worker-credential"
    )
    second = await sdk_examples.credentials.generate_credential_token(
        example_core_client, "worker-credential"
    )
    assert bytes(first) != bytes(second)
    variant, value = betterproto.which_one_of(first, "type")
    assert (
        variant
        == {
            "AUTH_TOKEN": "authentication_token",
            "OAUTH2": "oauth2_credentials",
            "ACCESS_TOKEN": "access_token",
        }[credential_type]
    )
    if credential_type == "OAUTH2":
        assert value.client_id == "worker-id" and value.client_secret == "oauth-secret-1"


@pytest.mark.parametrize(
    "options",
    [
        {"expires_hours": 0},
        {"expires_hours": 17521},
        {"max_authentications": -1},
        {"max_authentications": 1000001},
    ],
)
async def test_invalid_credential_limits_rejected_before_api_call(
    sdk_examples, example_core_server, example_core_client, options
):
    with pytest.raises(ValueError):
        await sdk_examples.credentials.create_credential(
            example_core_client,
            "invalid",
            "worker",
            credential_type=c.CredentialSpecType.from_string("AUTH_TOKEN"),
            **options,
        )
    assert example_core_server.core.calls == []


async def test_cluster_config_update_preserves_other_settings(
    sdk_examples, example_core_server, example_core_client
):
    original = example_core_server.core.config
    updated = await sdk_examples.cluster_config.update_session_limits(
        example_core_client, workload_max_sessions=20
    )
    assert updated.spec.session.workload.max_per_user == 20
    assert updated.spec.session.human.max_per_user == 3
    assert bytes(updated.spec.session.human) == bytes(original.spec.session.human)
    assert bytes(updated.spec.session.workload.refresh_token_duration) == bytes(
        original.spec.session.workload.refresh_token_duration
    )
    assert bytes(updated.spec.ingress) == bytes(original.spec.ingress)
    assert bytes(updated.spec.authorization) == bytes(original.spec.authorization)
    assert updated.metadata.labels == original.metadata.labels
    updated = await sdk_examples.cluster_config.update_session_limits(
        example_core_client, human_max_sessions=5
    )
    assert updated.spec.session.human.max_per_user == 5
    assert updated.spec.session.workload.max_per_user == 20


@pytest.mark.parametrize(
    "options", [{}, {"human_max_sessions": 0}, {"workload_max_sessions": 1001}]
)
async def test_invalid_cluster_config_limits_rejected_before_api_call(
    sdk_examples, example_core_server, example_core_client, options
):
    with pytest.raises(ValueError):
        await sdk_examples.cluster_config.update_session_limits(example_core_client, **options)
    assert example_core_server.core.calls == []


@pytest.mark.parametrize("module,kind", resource_examples)
async def test_update_permission_errors_propagate_without_retry(
    sdk_examples, example_core_server, example_core_client, module, kind
):
    example_core_server.core.errors[f"get_{kind}"] = GRPCError(Status.PERMISSION_DENIED, "denied")
    options = {"disabled": True} if kind == "credential" else {}
    with pytest.raises(GRPCError) as exc:
        await getattr(getattr(sdk_examples, module), f"update_{kind}")(
            example_core_client, "existing", **options
        )
    assert exc.value.status == Status.PERMISSION_DENIED
    assert [method for method, request in example_core_server.core.calls] == [f"get_{kind}"]


async def test_documented_workload_commands_use_real_core_api(
    sdk_examples, example_core_server, monkeypatch, capsys
):
    monkeypatch.setattr(OcteliumClient, "create", staticmethod(example_core_server.create_client))
    commands = [
        ("users", ["create", "ci-agent", "--type", "workload", "--display-name", "CI agent"]),
        (
            "policies",
            ["create", "reports-access", "--match", 'ctx.user.metadata.name == "ci-agent"'],
        ),
        (
            "services",
            [
                "create",
                "reports.default",
                "--upstream",
                "http://reports:8080",
                "--policies",
                "reports-access",
                "--public",
            ],
        ),
        (
            "credentials",
            [
                "create",
                "ci-agent-token",
                "--user",
                "ci-agent",
                "--type",
                "auth-token",
                "--expires-hours",
                "24",
            ],
        ),
        ("credentials", ["token", "ci-agent-token"]),
        ("users", ["list"]),
        ("services", ["list", "--namespace", "default"]),
        ("policies", ["get", "reports-access"]),
        ("credentials", ["list", "--user", "ci-agent"]),
        ("services", ["update", "reports.default", "--upstream", "http://reports-v2:8080"]),
        ("users", ["update", "ci-agent", "--disabled"]),
        ("credentials", ["delete", "ci-agent-token"]),
        ("services", ["delete", "reports.default"]),
        ("policies", ["delete", "reports-access"]),
        ("users", ["delete", "ci-agent"]),
    ]
    for module, args in commands:
        monkeypatch.setattr(sys, "argv", [f"{module}.py", *args])
        await getattr(sdk_examples, module).main()
        output = capsys.readouterr().out
        if args[0] == "token":
            assert (
                json.loads(output)["authenticationToken"]["authenticationToken"] == "auth-secret-1"
            )
        else:
            assert "auth-secret-1" not in output
    assert all(store == {} for store in example_core_server.core.resources.values())
    assert all(client._is_closed for client in example_core_server.clients)


@pytest.mark.parametrize(
    "module,commands",
    [
        (
            "users",
            [
                ["create", "alice", "--type", "human", "--email", "alice@example.com"],
                ["get", "alice"],
                ["update", "alice", "--groups", "--no-disabled"],
                ["list"],
                ["delete", "alice"],
            ],
        ),
        (
            "services",
            [
                [
                    "create",
                    "reports.default",
                    "--upstream",
                    "http://reports:8080",
                    "--policies",
                    "access",
                ],
                ["get", "reports.default"],
                ["update", "reports.default", "--policies", "--no-public", "--no-disabled"],
                ["list"],
                ["delete", "reports.default"],
            ],
        ),
        (
            "policies",
            [
                [
                    "create",
                    "access",
                    "--match",
                    'ctx.user.metadata.name == "worker"',
                    "--rule",
                    "worker",
                ],
                ["get", "access"],
                ["update", "access", "--rule", "worker", "--effect", "deny", "--no-disabled"],
                ["list"],
                ["delete", "access"],
            ],
        ),
        (
            "credentials",
            [
                [
                    "create",
                    "worker-oauth",
                    "--user",
                    "worker",
                    "--type",
                    "oauth2",
                    "--max-authentications",
                    "3",
                ],
                ["get", "worker-oauth"],
                ["update", "worker-oauth", "--no-disabled"],
                ["list"],
                ["token", "worker-oauth"],
                ["delete", "worker-oauth"],
            ],
        ),
        (
            "groups",
            [
                ["create", "engineers", "--policies", "access"],
                ["get", "engineers"],
                ["update", "engineers", "--policies", "--display-name", "Engineers"],
                ["list"],
                ["delete", "engineers"],
            ],
        ),
        (
            "namespaces",
            [
                ["create", "production", "--policies", "access"],
                ["get", "production"],
                ["update", "production", "--policies", "--display-name", "Production"],
                ["list"],
                ["delete", "production"],
            ],
        ),
        (
            "cluster_config",
            [
                ["get"],
                ["update", "--human-max-sessions", "5", "--workload-max-sessions", "20"],
                ["get"],
            ],
        ),
    ],
)
async def test_resource_commands_run_with_real_grpc_transport(
    sdk_examples, example_core_server, monkeypatch, capsys, module, commands
):
    monkeypatch.setattr(OcteliumClient, "create", staticmethod(example_core_server.create_client))
    for args in commands:
        monkeypatch.setattr(sys, "argv", [f"{module}.py", *args])
        await getattr(sdk_examples, module).main()
        output = capsys.readouterr().out
        assert output
        if args[0] != "delete":
            assert isinstance(json.loads(output), dict)
    assert all(client._is_closed for client in example_core_server.clients)
    assert all(store == {} for store in example_core_server.core.resources.values())
    if module == "cluster_config":
        assert example_core_server.core.config.spec.session.human.max_per_user == 5
        assert example_core_server.core.config.spec.session.workload.max_per_user == 20


@pytest.mark.parametrize(
    "module,args",
    [
        ("users", ["update", "existing"]),
        ("users", ["create", "worker", "--type", "workload", "--email", "worker@example.com"]),
        ("services", ["update", "existing"]),
        ("policies", ["update", "existing"]),
        ("groups", ["update", "existing"]),
        ("namespaces", ["update", "existing"]),
        ("cluster_config", ["update"]),
        (
            "credentials",
            [
                "create",
                "invalid",
                "--user",
                "worker",
                "--type",
                "auth-token",
                "--expires-hours",
                "0",
            ],
        ),
    ],
)
async def test_invalid_core_commands_fail_before_constructing_client(
    sdk_examples, example_core_server, monkeypatch, module, args
):
    monkeypatch.setattr(OcteliumClient, "create", staticmethod(example_core_server.create_client))
    monkeypatch.setattr(sys, "argv", [f"{module}.py", *args])
    with pytest.raises(SystemExit) as exc:
        await getattr(sdk_examples, module).main()
    assert exc.value.code == 2
    assert example_core_server.clients == []
