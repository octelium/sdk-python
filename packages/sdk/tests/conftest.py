import asyncio
import contextvars
import ssl
import subprocess
from types import SimpleNamespace

import pytest
from grpclib.const import Status
from grpclib.events import RecvRequest, listen
from grpclib.exceptions import GRPCError
from grpclib.server import Server
from octelium.api.main.auth import v1 as a
from octelium.api.main.cordium import v1 as p
from octelium.api.main.meta import v1 as m
from octelium.sdk import AuthConfig, AuthTokenConfig, OcteliumClient, OcteliumClientConfig

incoming = contextvars.ContextVar("sdk_test_metadata")


def session(generation=1, expires_in=1800, refresh_expires_in=3600):
    return a.SessionToken(
        access_token=f"access-{generation}",
        refresh_token=f"refresh-{generation}",
        expires_in=expires_in,
        refresh_token_expires_in=refresh_expires_in,
    )


class ClusterAuth(a.MainServiceBase):
    def __init__(self):
        self.generation = 0
        self.authentication_tokens = []
        self.assertions = []
        self.revoked = set()
        self.refresh_calls = 0
        self.logout_calls = 0
        self.refresh_error = None
        self.logout_error = None
        self.response = None
        self.initial_started = asyncio.Event()
        self.initial_release = asyncio.Event()
        self.initial_release.set()
        self.refresh_started = asyncio.Event()
        self.refresh_release = asyncio.Event()
        self.refresh_release.set()

    async def authenticate_with_authentication_token(self, request):
        if request.authentication_token in self.authentication_tokens:
            raise GRPCError(Status.UNAUTHENTICATED, "credential already consumed")
        self.authentication_tokens.append(request.authentication_token)
        self.generation += 1
        self.initial_started.set()
        await self.initial_release.wait()
        return self.response or session(self.generation)

    async def authenticate_with_assertion(self, request):
        self.assertions.append(
            (request.assertion, list(request.scopes), request.identity_provider_ref.name)
        )
        self.generation += 1
        return self.response or session(self.generation)

    async def authenticate_with_refresh_token(self, request):
        if incoming.get().get("x-octelium-refresh-token") != f"refresh-{self.generation}":
            raise GRPCError(Status.UNAUTHENTICATED, "invalid rotating refresh token")
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        self.generation += 1
        self.refresh_started.set()
        await self.refresh_release.wait()
        return self.response or session(self.generation)

    async def logout(self, request):
        if incoming.get().get("x-octelium-refresh-token") != f"refresh-{self.generation}":
            raise GRPCError(Status.UNAUTHENTICATED, "invalid refresh metadata")
        self.logout_calls += 1
        if self.logout_error is not None:
            raise self.logout_error
        return a.LogoutResponse()


class ClusterService(p.MainServiceBase):
    def __init__(self, auth):
        self.auth = auth

    async def get_workspace(self, request):
        token = incoming.get().get("x-octelium-auth")
        expected = (
            "environment-access" if self.auth.generation == 0 else f"access-{self.auth.generation}"
        )
        if token != expected or token in self.auth.revoked:
            raise GRPCError(Status.UNAUTHENTICATED, "missing access metadata")
        return p.Workspace(metadata=m.Metadata(name=request.name))


@pytest.fixture(autouse=True)
def sdk_environment(monkeypatch):
    for name in ("OCTELIUM_DOMAIN", "OCTELIUM_ACCESS_TOKEN", "OCTELIUM_AUTH_TOKEN"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(scope="session")
def sdk_tls(tmp_path_factory):
    directory = tmp_path_factory.mktemp("sdk-tls")
    cert, key = directory / "cert.pem", directory / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert, key)
    server.set_alpn_protocols(["h2", "http/1.1"])
    return SimpleNamespace(
        server=server, client=ssl.create_default_context(cafile=str(cert)), cert=cert
    )


@pytest.fixture
async def sdk_cluster(sdk_tls):
    auth = ClusterAuth()
    metadata = []
    server = Server([auth, ClusterService(auth)])

    async def record(event):
        assert event.peer._transport.get_extra_info("ssl_object").selected_alpn_protocol() == "h2"
        values = dict(event.metadata)
        incoming.set(values)
        metadata.append((event.method_name, values))

    listen(server, RecvRequest, record)
    await server.start("127.0.0.1", 0, ssl=sdk_tls.server)
    port = server._server.sockets[0].getsockname()[1]
    yield SimpleNamespace(auth=auth, metadata=metadata, port=port)
    server.close()
    await server.wait_closed()


@pytest.fixture
async def sdk_client_factory(sdk_cluster, sdk_tls):
    clients = []

    def create(**kwargs):
        values = dict(
            domain="example.test",
            auth=AuthConfig(type="auth_token", auth_token=AuthTokenConfig(token="credential")),
            api_host="127.0.0.1",
            api_port=sdk_cluster.port,
            ssl_context_factory=lambda: ssl.create_default_context(cafile=str(sdk_tls.cert)),
            tls_server_name="localhost",
        )
        values.update(kwargs)
        client = OcteliumClient(OcteliumClientConfig(**values))
        clients.append(client)
        return client

    yield create
    for client in clients:
        await client.close()


@pytest.fixture
async def sdk_http_server(sdk_tls):
    from aiohttp import web

    runners = []

    async def create(handler, *, tls=False):
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handler)
        runner = web.AppRunner(app, handler_cancellation=True, shutdown_timeout=0.1)
        await runner.setup()
        runners.append(runner)
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=sdk_tls.server if tls else None)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        return f"{'https' if tls else 'http'}://{'localhost' if tls else '127.0.0.1'}:{port}"

    yield create
    for runner in runners:
        await runner.cleanup()
