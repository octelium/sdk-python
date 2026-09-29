import asyncio
import ssl
import subprocess

import httpx
import pytest
from conftest import Main
from cordium import (
    AccessToken,
    AssertionFile,
    AsyncCordium,
    AuthenticationToken,
    CordiumError,
)
from grpclib.client import Channel
from grpclib.server import Server


def make_client(cluster, auth):
    return AsyncCordium("example.test", auth=auth, host="127.0.0.1", port=cluster.port, tls=False)


async def test_auth_single_flight_cancel_refresh(cluster):
    cluster.auth.delay = 0.08
    async with make_client(cluster, AuthenticationToken("one-use")) as client:
        cancelled = asyncio.create_task(client.workspaces.get("abc"))
        requests = [asyncio.create_task(client.workspaces.get("abc")) for _ in range(10)]
        await asyncio.sleep(0.02)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await asyncio.gather(*requests)
        assert len(cluster.auth.requests) == 1
        assert cluster.auth.requests[0].authentication_token == "one-use"
        assert all(
            headers["authorization"] == "Bearer session-token"
            for path, headers in cluster.metadata
            if path.endswith("GetWorkspace")
        )
        client._engine.auth.expires_at = 0
        assert await client.access_token() == "refreshed"
        refresh_headers = [
            headers
            for path, headers in cluster.metadata
            if path.endswith("AuthenticateWithRefreshToken")
        ][0]
        assert refresh_headers == {"x-octelium-refresh-token": "refresh-secret"}
        cluster.auth.fail_refresh = True
        client._engine.auth.expires_at = 0
        with pytest.raises(CordiumError) as invalid:
            await client.access_token()
        assert invalid.value.code == "UNAUTHENTICATED"
        assert len([r for r in cluster.auth.requests if hasattr(r, "authentication_token")]) == 1


async def test_assertion_file_is_reread(cluster, tmp_path):
    path = tmp_path / "assertion"
    path.write_text("first\n")
    async with make_client(cluster, AssertionFile(path, scopes=("scope",))) as client:
        assert await client.access_token() == "assertion-token"
        path.write_text("second")
        client._engine.auth.expires_at = 0
        cluster.auth.fail_refresh = True
        await client.access_token()
        assert cluster.auth.requests[-1].assertion == "second"
        assert cluster.auth.requests[-1].scopes == ["scope"]


async def test_provider_is_dynamic_and_client_ownership(cluster):
    values = iter(["first", "second"])

    async def provider():
        return next(values)

    async with make_client(cluster, AccessToken(provider)) as client:
        assert await client.access_token() == "first"
        assert await client.access_token() == "second"
    channel = Channel("127.0.0.1", cluster.port, ssl=False)
    client = AsyncCordium(channel=channel)
    await client.workspaces.get("abc")
    await client.aclose()
    from octelium.api.main.cordium.v1 import MainServiceStub
    from octelium.api.main.meta.v1 import GetOptions

    assert (
        await MainServiceStub(channel).get_workspace(GetOptions(name="abc"))
    ).metadata.name == "abc"
    channel.close()


async def test_http_auth_scope_redirect_size_and_ownership():
    requests = []

    async def responder(request):
        requests.append(request)
        if request.url.path == "/redirect":
            return httpx.Response(302, headers={"location": "https://evil.test/"})
        return httpx.Response(200, content=b"body")

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(responder), headers={"Host": "evil.test"}
    )
    async with AsyncCordium(
        "example.test",
        auth=AccessToken("secret"),
        http_client=http,
        authorized_http_hosts=("extra.test",),
    ) as client:
        response = await client.request("GET", "https://api_abc.cordium.example.test/ok")
        assert response.content == b"body" and response.is_closed
        assert requests[-1].headers["Authorization"] == "Bearer secret"
        assert requests[-1].headers["Host"] == "api_abc.cordium.example.test"
        assert (await client.request("GET", "https://extra.test/redirect")).status_code == 302
        for url in [
            "https://example.test.evil/",
            "https://evil.test/",
            "https://user:pw@example.test/",
            "https://@example.test/",
            "http://example.test/",
        ]:
            with pytest.raises(ValueError):
                await client.request("GET", url)
        assert len(requests) == 2
        with pytest.raises(CordiumError) as too_large:
            await client.request("GET", "https://example.test", max_response_bytes=3)
        assert too_large.value.code == "RESOURCE_EXHAUSTED"
    assert not http.is_closed
    await http.aclose()


async def test_http_full_body_deadline():
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"first"
            await asyncio.sleep(1)
            yield b"last"

        async def aclose(self):
            self.closed = True

    body = Slow()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body))
    )
    async with (
        http,
        AsyncCordium("example.test", auth=AccessToken("secret"), http_client=http) as client,
    ):
        with pytest.raises(CordiumError) as deadline:
            await client.request("GET", "https://example.test", timeout=0.02)
        assert deadline.value.code == "DEADLINE_EXCEEDED" and body.closed


async def test_verified_tls_and_default_endpoint(tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    await asyncio.to_thread(
        subprocess.run,
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
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)
    server = Server([Main()])
    await server.start("127.0.0.1", 0, ssl=server_context)
    port = server._server.sockets[0].getsockname()[1]
    context = ssl.create_default_context(cafile=str(cert))
    try:
        async with AsyncCordium(
            "example.test", auth=AccessToken("token"), host="localhost", port=port, tls=context
        ) as client:
            assert (await client.workspaces.get("abc")).name == "abc"
        async with AsyncCordium("example.test", auth=AccessToken("token")) as client:
            assert client._engine.host == "octelium-api.example.test"
    finally:
        server.close()
        await server.wait_closed()


def test_environment_and_redaction(monkeypatch):
    for name in (
        "CORDIUM_DOMAIN",
        "OCTELIUM_DOMAIN",
        "OCTELIUM_ACCESS_TOKEN",
        "OCTELIUM_AUTH_TOKEN",
        "OCTELIUM_AUTHENTICATION_TOKEN",
        "OCTELIUM_ASSERTION_FILE",
        "OCTELIUM_ASSERTION",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError):
        AsyncCordium()
    monkeypatch.setenv("OCTELIUM_DOMAIN", "example.test")
    monkeypatch.setenv("OCTELIUM_ACCESS_TOKEN", "redacted-value")
    monkeypatch.setenv("OCTELIUM_AUTH_TOKEN", "other-value")
    client = AsyncCordium()
    assert isinstance(client._engine.credentials, AccessToken)
    assert "redacted-value" not in repr(client._engine.credentials)
    assert "redacted-value" not in repr(AuthenticationToken("redacted-value"))
