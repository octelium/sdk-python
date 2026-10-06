import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from octelium.sdk import AuthConfig


@pytest.mark.parametrize(
    "url,headers",
    [
        ("http://example.test/", {}),
        ("https://unrelated.test/", {}),
        ("https://example.test.attacker.test/", {}),
        ("https://user:secret@example.test/", {}),
        ("https://@example.test/", {}),
        ("https://example.test:8443/", {}),
        ("https://example.test/", {"Host": "unrelated.test"}),
        ("https://example.test/", {"Host": "example.test:8443"}),
    ],
)
async def test_untrusted_destinations_rejected_before_auth(sdk_client_factory, url, headers):
    client = sdk_client_factory()
    client.get_access_token = AsyncMock(return_value="secret")
    http = client.http_client()
    with pytest.raises(ValueError):
        await http.get(url, headers=headers)
    client.get_access_token.assert_not_awaited()
    assert http._session is None


async def test_plaintext_unrelated_server_never_receives_token(sdk_client_factory, sdk_http_server):
    seen = []

    async def handler(request):
        seen.append(dict(request.headers))
        return web.Response(text="ok")

    url = await sdk_http_server(handler)
    client = sdk_client_factory(auth=AuthConfig(type="access_token", access_token="secret"))
    with pytest.raises(ValueError, match="plain HTTP"):
        await client.http_client().get(url)
    assert seen == []


async def test_authorized_tls_request_and_header_precedence(sdk_client_factory, sdk_http_server):
    seen = []

    async def handler(request):
        seen.append(dict(request.headers))
        return web.json_response({"ok": True})

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(
        auth=AuthConfig(type="access_token", access_token="secret"),
        authorized_http_origins=[origin],
    )
    async with client.http_client() as http:
        headers = {"x-octelium-auth": "stale", "Authorization": "Bearer other"}
        response = await http.get(origin, headers=headers)
        async with response:
            assert await response.json() == {"ok": True}
        assert headers["x-octelium-auth"] == "stale"
        assert seen[0]["x-octelium-auth"] == "secret"
        assert seen[0]["Authorization"] == "Bearer other"
        assert http._session.connector._ssl is client._ssl_context


async def test_redirect_does_not_forward_credentials(sdk_client_factory, sdk_http_server):
    seen = []

    async def destination(request):
        seen.append(dict(request.headers))
        return web.Response(text="leaked")

    other = await sdk_http_server(destination)

    async def redirect(request):
        assert request.headers["x-octelium-auth"] == "secret"
        raise web.HTTPTemporaryRedirect(other)

    origin = await sdk_http_server(redirect)
    client = sdk_client_factory(
        auth=AuthConfig(type="access_token", access_token="secret"),
        allow_insecure_http=True,
        authorized_http_origins=[origin],
    )
    http = client.http_client()
    response = await http.get(origin)
    async with response:
        assert response.status == 307
    assert seen == []
    with pytest.raises(ValueError, match="redirects"):
        await http.get(origin, allow_redirects=True)


async def test_http_deadline_includes_authentication(sdk_client_factory):
    client = sdk_client_factory()
    entered = asyncio.Event()

    async def stalled(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    client._auth_stub.authenticate_with_authentication_token = stalled
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            client.http_client(timeout=aiohttp.ClientTimeout(total=0.02)).get(
                "https://example.test"
            ),
            1,
        )
    assert entered.is_set()
    assert client._auth_task is not None


async def test_http_deadline_keeps_original_budget_for_body(sdk_client_factory, sdk_http_server):
    async def handler(request):
        response = web.StreamResponse()
        await response.prepare(request)
        await asyncio.sleep(0.12)
        await response.write(b"late-body")
        return response

    origin = await sdk_http_server(handler)
    client = sdk_client_factory(
        auth=AuthConfig(type="access_token", access_token="secret"),
        allow_insecure_http=True,
        authorized_http_origins=[origin],
    )

    async def delayed_token():
        await asyncio.sleep(0.08)
        return "secret"

    client.get_access_token = delayed_token
    response = await client.http_client(timeout=aiohttp.ClientTimeout(total=0.15)).get(origin)
    async with response:
        with pytest.raises(TimeoutError):
            await response.read()


async def test_http_session_creation_cannot_reopen_closed_helper(sdk_client_factory):
    client = sdk_client_factory()
    http = client.http_client()
    await http._session_lock.acquire()
    creator = asyncio.create_task(http._get_session())
    await asyncio.sleep(0)
    closer = asyncio.create_task(http.close())
    await asyncio.sleep(0)
    http._session_lock.release()
    with pytest.raises(RuntimeError, match="closed"):
        await creator
    await closer
    assert http._session is None


async def test_parent_close_closes_all_helpers_and_oauth_session(sdk_client_factory):
    client = sdk_client_factory()
    helpers = [client.http_client() for _ in range(3)]
    sessions = [await helper._get_session() for helper in helpers]
    oauth = await client._get_oauth_session()
    await client.close()
    assert all(session.closed for session in sessions)
    assert oauth.closed
    assert client._http_clients == set()
    for helper in helpers:
        with pytest.raises(RuntimeError, match="closed"):
            await helper.get("https://example.test")


async def test_parent_close_cancels_active_http_request(sdk_client_factory, sdk_http_server):
    entered, exited = asyncio.Event(), asyncio.Event()

    async def handler(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            exited.set()

    origin = await sdk_http_server(handler)
    client = sdk_client_factory(
        auth=AuthConfig(type="access_token", access_token="secret"),
        allow_insecure_http=True,
        authorized_http_origins=[origin],
    )
    http = client.http_client()
    task = asyncio.create_task(http.get(origin))
    await asyncio.wait_for(entered.wait(), 2)
    await client.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(exited.wait(), 2)
    assert http._session is None
