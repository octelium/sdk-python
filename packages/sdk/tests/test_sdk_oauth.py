import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from octelium.sdk import AuthConfig, OAuth2ClientCredentialsConfig


def oauth_auth():
    return AuthConfig(
        type="oauth2_client_credentials",
        oauth2_client_credentials=OAuth2ClientCredentialsConfig(
            client_id="client", client_secret="oauth-secret", scopes=["api:core"]
        ),
    )


def response_payload(**kwargs):
    return {"access_token": "oauth-access", "token_type": "Bearer", "expires_in": 3600} | kwargs


async def route_oauth(client, origin):
    session = await client._get_oauth_session()
    client._get_oauth_session = AsyncMock(
        return_value=SimpleNamespace(
            post=lambda url, **kwargs: session.post(origin + "/oauth2/token", **kwargs)
        )
    )
    return session


async def test_oauth_verified_tls_and_long_lifetimes(sdk_client_factory, sdk_http_server):
    seen = []

    async def handler(request):
        seen.append(await request.post())
        return web.json_response(response_payload(expires_in=14 * 24 * 3600))

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    session = await route_oauth(client, origin)
    assert await client.get_access_token() == "oauth-access"
    assert seen[0]["client_secret"] == "oauth-secret"
    assert seen[0]["scope"] == "api:core"
    assert session.connector._ssl is client._ssl_context


@pytest.mark.parametrize("status", [307, 308])
async def test_oauth_redirect_never_replays_secret(sdk_client_factory, sdk_http_server, status):
    seen = []

    async def destination(request):
        seen.append(await request.post())
        return web.json_response(response_payload())

    other = await sdk_http_server(destination, tls=True)

    async def redirect(request):
        return web.Response(status=status, headers={"Location": other})

    origin = await sdk_http_server(redirect, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    await route_oauth(client, origin)
    with pytest.raises(RuntimeError, match=f"status {status}"):
        await client.get_access_token()
    assert seen == []


async def test_oauth_error_does_not_expose_body_or_secret(sdk_client_factory, sdk_http_server):
    async def handler(request):
        return web.Response(status=400, text="oauth-secret" * 10000)

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    await route_oauth(client, origin)
    with pytest.raises(RuntimeError) as error:
        await client.get_access_token()
    assert "oauth-secret" not in str(error.value)
    assert len(str(error.value)) < 100


@pytest.mark.parametrize(
    "payload",
    [
        response_payload(access_token=""),
        response_payload(access_token=123),
        response_payload(token_type="Basic"),
        response_payload(token_type=None),
        response_payload(expires_in=True),
        response_payload(expires_in=1.5),
        response_payload(expires_in="3600"),
        response_payload(expires_in=0),
        response_payload(expires_in=-1),
        [],
    ],
)
async def test_invalid_oauth_response_is_not_cached(sdk_client_factory, sdk_http_server, payload):
    async def handler(request):
        return web.json_response(payload)

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    await route_oauth(client, origin)
    with pytest.raises(RuntimeError):
        await client.get_access_token()
    assert client._oauth2_cache is None


async def test_oauth_body_size_bound_and_chunked_json(sdk_client_factory, sdk_http_server):
    large = True

    async def handler(request):
        if large:
            return web.json_response(response_payload(access_token="x" * 70000))
        response = web.StreamResponse(headers={"Content-Type": "application/json"})
        await response.prepare(request)
        body = json.dumps(response_payload(expires_in=10)).encode()
        await response.write(body[:10])
        await asyncio.sleep(0.01)
        await response.write(body[10:])
        return response

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    await route_oauth(client, origin)
    with pytest.raises(RuntimeError, match="65536"):
        await client.get_access_token()
    large = False
    client._retry_at = 0
    assert await client.get_access_token() == "oauth-access"
    assert await client.get_access_token() == "oauth-access"
    assert client._oauth2_cache.refresh_at < client._oauth2_cache.expires_at


async def test_oauth_transient_failure_uses_valid_cache(sdk_client_factory):
    client = sdk_client_factory(auth=oauth_auth())
    client._fetch_oauth2_token = AsyncMock(side_effect=aiohttp.ClientConnectionError("unavailable"))
    import time

    from octelium.sdk.client import _OAuth2Cache

    now = time.monotonic()
    client._oauth2_cache = _OAuth2Cache("usable", now + 10, now - 1)
    assert await asyncio.gather(*(client.get_access_token() for _ in range(10))) == ["usable"] * 10
    assert client._fetch_oauth2_token.await_count == 1
    client._oauth2_cache.expires_at = now - 1
    with pytest.raises(aiohttp.ClientConnectionError):
        await client.get_access_token()


async def test_oauth_permanent_failure_never_falls_back(sdk_client_factory):
    client = sdk_client_factory(auth=oauth_auth())
    client._fetch_oauth2_token = AsyncMock(side_effect=RuntimeError("invalid response"))
    import time

    from octelium.sdk.client import _OAuth2Cache

    now = time.monotonic()
    client._oauth2_cache = _OAuth2Cache("old", now + 10, now - 1)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="invalid response"):
            await client.get_access_token()
    assert client._fetch_oauth2_token.await_count == 1


async def test_oauth_server_outage_uses_valid_token(sdk_client_factory, sdk_http_server):
    import time

    from octelium.sdk.client import _OAuth2Cache

    async def handler(request):
        return web.Response(status=503, text="temporary failure")

    origin = await sdk_http_server(handler, tls=True)
    client = sdk_client_factory(auth=oauth_auth())
    await route_oauth(client, origin)
    now = time.monotonic()
    client._oauth2_cache = _OAuth2Cache("usable", now + 10, now - 1)
    assert await client.get_access_token() == "usable"
    assert await client.get_access_token() == "usable"
