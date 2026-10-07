import asyncio
import time

import httpx
import pytest
from cordium import (
    AccessToken,
    AssertionFile,
    AsyncCordium,
    AuthConfig,
    AuthenticationToken,
    CordiumError,
    OAuth2ClientCredentials,
    OcteliumClient,
)
from cordium.auth import octelium_auth
from grpclib.client import Channel
from grpclib.config import Configuration
from octelium.sdk import OcteliumClientConfig


def make_client(cluster, auth):
    return AsyncCordium("example.test", auth=auth, **cluster.connection)


def expire(client):
    client.octelium._session_token_set_at = time.monotonic() - 3590


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
            headers["x-octelium-auth"] == "session-token" and "authorization" not in headers
            for path, headers in cluster.metadata
            if path.endswith("GetWorkspace")
        )
        expire(client)
        assert await client.access_token() == "refreshed"
        refresh_headers = [
            headers
            for path, headers in cluster.metadata
            if path.endswith("AuthenticateWithRefreshToken")
        ][0]
        assert refresh_headers == {"x-octelium-refresh-token": "refresh-secret"}
        cluster.auth.fail_refresh = True
        expire(client)
        with pytest.raises(CordiumError) as invalid:
            await client.access_token()
        assert invalid.value.code == "UNAUTHENTICATED"
        assert len([r for r in cluster.auth.requests if hasattr(r, "authentication_token")]) == 1


async def test_assertion_file_is_reread(cluster, tmp_path):
    path = tmp_path / "assertion"
    path.write_text("first\n")
    async with make_client(cluster, AssertionFile(path, scopes=("scope",))) as client:
        assert await client.access_token() == "assertion-token"
        assert cluster.auth.requests[-1].assertion == "first"
        path.write_text("second")
        cluster.auth.fail_refresh = True
        expire(client)
        await client.access_token()
        assert cluster.auth.requests[-1].assertion == "second"
        assert cluster.auth.requests[-1].scopes == ["scope"]


async def test_provider_is_dynamic_and_client_ownership(cluster, tls):
    values = iter(["first", "second"])

    async def provider():
        return next(values)

    async with make_client(cluster, AccessToken(provider)) as client:
        assert await client.access_token() == "first"
        assert await client.access_token() == "second"
    context = tls.options["ssl_context_factory"]()
    context.set_alpn_protocols(["h2"])
    channel = Channel(
        "127.0.0.1",
        cluster.port,
        ssl=context,
        config=Configuration(ssl_target_name_override="localhost"),
    )
    client = AsyncCordium(channel=channel)
    await client.workspaces.get("abc")
    assert client.octelium is None
    with pytest.raises(CordiumError) as owned:
        await client.access_token()
    assert owned.value.code == "FAILED_PRECONDITION"
    await client.aclose()
    from octelium.api.main.cordium.v1 import MainServiceStub
    from octelium.api.main.meta.v1 import GetOptions

    assert (
        await MainServiceStub(channel).get_workspace(GetOptions(name="abc"))
    ).metadata.name == "abc"
    channel.close()


async def test_supplied_octelium_client_is_shared_and_left_open(cluster, tls):
    octelium = OcteliumClient(
        OcteliumClientConfig(
            domain="example.test",
            auth=AuthConfig(type="access_token", access_token="shared"),
            api_host="127.0.0.1",
            api_port=cluster.port,
            **tls.options,
        )
    )
    for kwargs in (
        {"auth": AccessToken("other")},
        {"host": "127.0.0.1"},
        {"channel": Channel("127.0.0.1", cluster.port)},
    ):
        with pytest.raises(ValueError):
            AsyncCordium(octelium=octelium, **kwargs)
    with pytest.raises(ValueError):
        AsyncCordium("other.test", octelium=octelium)
    client = AsyncCordium(octelium=octelium)
    assert client.domain == "example.test"
    assert client.octelium is octelium
    await client.workspaces.get("abc")
    assert cluster.metadata[-1][1]["x-octelium-auth"] == "shared"
    await client.aclose()
    assert await octelium.get_access_token() == "shared"
    await octelium.close()


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
        response = await client.request(
            "GET",
            "https://api_abc.cordium.example.test/ok",
            headers={"Authorization": "Bearer app-token", "x-octelium-auth": "wrong"},
        )
        assert response.content == b"body" and response.is_closed
        assert requests[-1].headers["x-octelium-auth"] == "secret"
        assert requests[-1].headers["Authorization"] == "Bearer app-token"
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


async def test_tls_verification_options_and_default_endpoint(cluster):
    connection = dict(cluster.connection)
    async with AsyncCordium("example.test", auth=AccessToken("token"), **connection) as client:
        assert (await client.workspaces.get("abc")).name == "abc"
    del connection["ssl_context_factory"]
    async with AsyncCordium("example.test", auth=AccessToken("token"), **connection) as client:
        with pytest.raises(CordiumError) as unverified:
            await client.workspaces.get("abc", timeout=5)
        assert unverified.value.code == "UNAVAILABLE"
    async with AsyncCordium(
        "example.test", auth=AccessToken("token"), insecure_tls=True, **connection
    ) as client:
        assert (await client.workspaces.get("abc")).name == "abc"
    async with AsyncCordium("example.test", auth=AccessToken("token")) as client:
        assert client.octelium._channel._host == "octelium-api.example.test"


async def test_environment_and_redaction(monkeypatch):
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
    async with AsyncCordium() as client:
        assert await client.access_token() == "redacted-value"
    assert "redacted-value" not in repr(octelium_auth(AccessToken("redacted-value")))
    assert "redacted-value" not in repr(AuthenticationToken("redacted-value"))
    assert "redacted-value" not in repr(OAuth2ClientCredentials("client", "redacted-value"))
    oauth = octelium_auth(OAuth2ClientCredentials("client", "secret", scopes=("api",)))
    assert oauth.oauth2_client_credentials.scopes == ("api",)
    explicit = AuthConfig(type="access_token", access_token="explicit")
    assert octelium_auth(explicit) is explicit
