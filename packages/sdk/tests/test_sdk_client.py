import asyncio
import dataclasses
import inspect
import ssl
import time
from unittest.mock import AsyncMock

import pytest
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from octelium.api.main.auth.v1 import SessionToken
from octelium.api.main.cordium import v1 as p
from octelium.api.main.meta.v1 import GetOptions
from octelium.sdk import (
    AssertionConfig,
    AuthConfig,
    AuthTokenConfig,
    OAuth2ClientCredentialsConfig,
    OcteliumClient,
    OcteliumClientConfig,
    run_sync,
)


async def test_environment_access_token_reaches_real_grpc(
    sdk_client_factory, sdk_cluster, monkeypatch
):
    monkeypatch.setenv("OCTELIUM_ACCESS_TOKEN", "environment-access")
    client = sdk_client_factory(auth=None)
    result = await client.cordium_v1.get_workspace(GetOptions(name="workspace"))
    assert result.metadata.name == "workspace"
    assert sdk_cluster.metadata[-1][1]["x-octelium-auth"] == "environment-access"
    monkeypatch.setenv("OCTELIUM_ACCESS_TOKEN", "different-identity")
    assert await client.get_access_token() == "environment-access"


async def test_explicit_credentials_override_environment(sdk_client_factory, monkeypatch):
    monkeypatch.setenv("OCTELIUM_ACCESS_TOKEN", "environment-access")
    client = sdk_client_factory(
        auth=AuthConfig(type="access_token", access_token="explicit-access")
    )
    assert await client.get_access_token() == "explicit-access"
    managed = sdk_client_factory()
    assert await managed.get_access_token() == "access-1"


async def test_config_is_not_mutated_and_secrets_are_hidden(monkeypatch):
    monkeypatch.setenv("OCTELIUM_DOMAIN", " Example.TEST. ")
    monkeypatch.setenv("OCTELIUM_ACCESS_TOKEN", "secret-access")
    config = OcteliumClientConfig()
    async with await OcteliumClient.create(config) as client:
        assert config.domain == "" and config.auth is None
        assert client._config.domain == "example.test"
        assert await client.get_access_token() == "secret-access"
        assert "secret-access" not in repr(client._config)
    token = AuthTokenConfig(token="secret-auth", scopes=["api:core"])
    oauth = OAuth2ClientCredentialsConfig(client_id="id", client_secret="secret-oauth")
    assert "secret-auth" not in repr(token)
    assert "secret-oauth" not in repr(oauth)
    with pytest.raises(dataclasses.FrozenInstanceError):
        token.token = "changed"
    scopes = ["api:core"]
    frozen = AuthTokenConfig(token="secret", scopes=scopes)
    scopes.append("service:other")
    assert frozen.scopes == ("api:core",)


async def test_cancelled_caller_preserves_committed_rotation(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    assert await client.get_access_token() == "access-1"
    client._session_token_set_at = time.monotonic() - 1800
    sdk_cluster.auth.refresh_release.clear()
    first = asyncio.create_task(client.get_access_token())
    await asyncio.wait_for(sdk_cluster.auth.refresh_started.wait(), 2)
    assert sdk_cluster.auth.generation == 2
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    waiters = [asyncio.create_task(client.get_access_token()) for _ in range(20)]
    sdk_cluster.auth.refresh_release.set()
    assert await asyncio.gather(*waiters) == ["access-2"] * 20
    assert client._session_token.refresh_token == "refresh-2"
    assert sdk_cluster.auth.refresh_calls == 1
    refresh_headers = [
        headers
        for method, headers in sdk_cluster.metadata
        if method.endswith("AuthenticateWithRefreshToken")
    ]
    assert refresh_headers == [{"x-octelium-refresh-token": "refresh-1"}]


async def test_all_cancelled_waiters_do_not_cancel_authentication(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    sdk_cluster.auth.initial_release.clear()
    first = asyncio.create_task(client.get_access_token())
    await asyncio.wait_for(sdk_cluster.auth.initial_started.wait(), 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    worker = client._auth_task
    sdk_cluster.auth.initial_release.set()
    assert await asyncio.wait_for(asyncio.shield(worker), 2) == "access-1"
    assert await client.get_access_token() == "access-1"
    assert sdk_cluster.auth.authentication_tokens == ["credential"]


async def test_failed_one_time_exchange_is_not_replayed(sdk_client_factory):
    client = sdk_client_factory()
    initial = AsyncMock(side_effect=GRPCError(Status.UNAVAILABLE, "response lost"))
    client._auth_stub.authenticate_with_authentication_token = initial
    with pytest.raises(GRPCError):
        await client.get_access_token()
    client._retry_at = 0
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await client.get_access_token()
    assert initial.await_count == 1


async def test_auth_timeout_bounds_worker_and_waiters(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory(authentication_timeout_seconds=0.03)
    sdk_cluster.auth.initial_release.clear()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.get_access_token(), 1)
    assert client._auth_task is None
    client._retry_at = 0
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await client.get_access_token()
    assert sdk_cluster.auth.authentication_tokens == ["credential"]


async def test_close_cancels_authentication_and_clears_cache(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    sdk_cluster.auth.initial_release.clear()
    task = asyncio.create_task(client.get_access_token())
    await asyncio.wait_for(sdk_cluster.auth.initial_started.wait(), 2)
    await asyncio.wait_for(client.close(), 2)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client._session_token is None
    assert client._oauth2_cache is None
    with pytest.raises(RuntimeError, match="closed"):
        await client.get_access_token()
    await client.close()


async def test_late_result_cannot_publish_after_close(sdk_client_factory):
    client = sdk_client_factory()
    started, canceled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def ignore_first_cancel(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            canceled.set()
            await release.wait()
        return SessionToken(
            access_token="late",
            refresh_token="late-refresh",
            expires_in=1800,
            refresh_token_expires_in=3600,
        )

    client._auth_stub.authenticate_with_authentication_token = ignore_first_cancel
    waiter = asyncio.create_task(client.get_access_token())
    await started.wait()
    closing = asyncio.create_task(client.close())
    await canceled.wait()
    release.set()
    await asyncio.wait_for(closing, 2)
    with pytest.raises(RuntimeError, match="closed"):
        await waiter
    assert client._session_token is None


async def test_close_preserves_cancellation_and_shares_cleanup(sdk_client_factory):
    client = sdk_client_factory()
    started, release = asyncio.Event(), asyncio.Event()
    session = await client._get_oauth_session()
    original_close = session.close

    async def delayed_close():
        started.set()
        await release.wait()
        await original_close()

    session.close = delayed_close
    first = asyncio.create_task(client.close())
    await started.wait()
    second = asyncio.create_task(client.close())
    first.cancel()
    await asyncio.sleep(0)
    assert not second.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert session.closed


async def test_close_does_not_logout_or_mask_application_error(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    with pytest.raises(ValueError, match="application failed"):
        async with client:
            assert await client.get_access_token() == "access-1"
            raise ValueError("application failed")
    assert sdk_cluster.auth.logout_calls == 0


async def test_logout_uses_refresh_header_and_is_idempotent(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    await client.get_access_token()
    sdk_cluster.auth.logout_error = GRPCError(Status.UNAUTHENTICATED, "session absent")
    await client.logout()
    await client.logout()
    assert sdk_cluster.auth.logout_calls == 1
    assert client._session_token is None
    assert [headers for method, headers in sdk_cluster.metadata if method.endswith("Logout")] == [
        {"x-octelium-refresh-token": "refresh-1"}
    ]


async def test_reusable_provider_can_reauthenticate(sdk_client_factory, sdk_cluster):
    values = iter(["first", "second"])

    async def provider():
        return next(values)

    client = sdk_client_factory(
        auth=AuthConfig(
            type="auth_token", auth_token=AuthTokenConfig(token=provider, reusable=True)
        )
    )
    assert await client.get_access_token() == "access-1"
    client._session_token_set_at = time.monotonic() - 1800
    sdk_cluster.auth.refresh_error = GRPCError(Status.UNAUTHENTICATED, "revoked")
    assert await client.get_access_token() == "access-2"
    assert sdk_cluster.auth.authentication_tokens == ["first", "second"]


async def test_refresh_rejection_does_not_replay_static_token(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    await client.get_access_token()
    client._session_token_set_at = time.monotonic() - 1800
    sdk_cluster.auth.refresh_error = GRPCError(Status.UNAUTHENTICATED, "revoked")
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await client.get_access_token()
    assert client._session_token is None
    assert sdk_cluster.auth.authentication_tokens == ["credential"]


@pytest.mark.parametrize("status", [Status.UNAVAILABLE, Status.ALREADY_EXISTS])
async def test_transient_refresh_uses_only_valid_token_with_backoff(
    sdk_client_factory, sdk_cluster, status
):
    client = sdk_client_factory()
    await client.get_access_token()
    client._session_token_set_at = time.monotonic() - 1790
    sdk_cluster.auth.refresh_error = GRPCError(status, "temporary")
    assert (
        await asyncio.gather(*(client.get_access_token() for _ in range(25))) == ["access-1"] * 25
    )
    assert sdk_cluster.auth.refresh_calls == 1
    client._session_token_set_at -= 20
    with pytest.raises(GRPCError):
        await client.get_access_token()
    assert sdk_cluster.auth.refresh_calls == 1


async def test_already_exists_never_returns_expired_token(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    await client.get_access_token()
    client._session_token_set_at = time.monotonic() - 1800
    sdk_cluster.auth.refresh_error = GRPCError(Status.ALREADY_EXISTS)
    with pytest.raises(RuntimeError, match="without a usable access token"):
        await client.get_access_token()


@pytest.mark.parametrize(
    "field,value",
    [
        ("access_token", ""),
        ("refresh_token", ""),
        ("expires_in", 0),
        ("expires_in", -1),
        ("refresh_token_expires_in", 0),
        ("refresh_token_expires_in", -1),
        ("refresh_token_expires_in", 1),
    ],
)
async def test_malformed_sessions_are_not_published(sdk_client_factory, sdk_cluster, field, value):
    client = sdk_client_factory()
    response = SessionToken(
        access_token="access",
        refresh_token="refresh",
        expires_in=1800,
        refresh_token_expires_in=3600,
    )
    setattr(response, field, value)
    sdk_cluster.auth.response = response
    with pytest.raises(RuntimeError):
        await client.get_access_token()
    assert client._session_token is None


async def test_short_lifetime_and_expired_refresh(sdk_client_factory, sdk_cluster):
    client = sdk_client_factory()
    sdk_cluster.auth.response = SessionToken(
        access_token="short", refresh_token="refresh", expires_in=10, refresh_token_expires_in=20
    )
    assert await client.get_access_token() == "short"
    assert not client._needs_new_access_token()
    client._session_token_set_at = time.monotonic() - 30
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await client.get_access_token()
    assert sdk_cluster.auth.refresh_calls == 0


async def test_eager_auth_failure_closes_resources(sdk_cluster, sdk_tls, monkeypatch):
    allocated = []
    original_init = OcteliumClient.__init__

    def capture(self, config):
        original_init(self, config)
        allocated.append(self)
        self._auth_stub.authenticate_with_authentication_token = AsyncMock(
            side_effect=GRPCError(Status.UNAUTHENTICATED)
        )

    monkeypatch.setattr(OcteliumClient, "__init__", capture)
    config = OcteliumClientConfig(
        domain="example.test",
        auth=AuthConfig(type="auth_token", auth_token=AuthTokenConfig(token="credential")),
        authenticate_on_creation=True,
        api_host="127.0.0.1",
        api_port=sdk_cluster.port,
        ssl_context_factory=lambda: ssl.create_default_context(cafile=str(sdk_tls.cert)),
        tls_server_name="localhost",
    )
    with pytest.raises(GRPCError):
        await OcteliumClient.create(config)
    assert allocated[0]._is_closed
    assert allocated[0]._close_task.done()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_port": 0},
        {"api_port": 65536},
        {"api_port": True},
        {"authentication_timeout_seconds": 0},
        {"authentication_timeout_seconds": float("nan")},
        {"oauth2_timeout_seconds": float("inf")},
        {"refresh_before_expiry_seconds": -1},
        {"domain": "https://example.test"},
        {"domain": "example.test/path"},
        {"domain": "example.test:443"},
        {"allow_insecure_http": "true"},
        {"authorized_http_origins": ["http://example.test"]},
    ],
)
def test_invalid_config_rejected(kwargs):
    with pytest.raises(ValueError):
        OcteliumClientConfig(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"type": "unknown"},
        {"type": "auth_token"},
        {"type": "access_token", "access_token": ""},
        {"type": "access_token", "access_token": lambda: "secret"},
        {
            "type": "access_token",
            "access_token": "secret",
            "auth_token": AuthTokenConfig(token="credential"),
        },
        {"type": "assertion"},
        {"type": "assertion", "auth_token": AuthTokenConfig(token="credential")},
    ],
)
def test_invalid_auth_combinations_rejected(kwargs):
    with pytest.raises(ValueError):
        AuthConfig(**kwargs)


def test_sync_providers_are_rejected_without_running_them():
    called = False

    def blocking():
        nonlocal called
        called = True
        return "credential"

    with pytest.raises(ValueError, match="async"):
        AuthTokenConfig(token=blocking)
    assert not called


async def test_run_sync_rejects_async_context_without_leaking_coroutine():
    async def work():
        return 42

    coro = work()
    with pytest.raises(RuntimeError, match="async context"):
        run_sync(coro)
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


def test_run_sync_one_shot():
    async def work():
        return 42

    assert run_sync(work()) == 42


async def test_retained_stub_rejects_after_close_without_reconnecting(sdk_client_factory):
    client = sdk_client_factory()
    stub = client.cordium_v1
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await stub.get_workspace(GetOptions(name="workspace"))
    assert client._channel._protocol is None


async def test_provider_can_close_its_client(sdk_client_factory):
    client = None

    async def provider():
        await client.close()
        return "credential"

    client = sdk_client_factory(
        auth=AuthConfig(type="auth_token", auth_token=AuthTokenConfig(token=provider))
    )
    with pytest.raises(RuntimeError, match="closed"):
        await asyncio.wait_for(client.get_access_token(), 1)
    assert client._session_token is None
    assert client._auth_error is None


async def test_cancelled_eager_creation_closes_allocated_client(monkeypatch):
    allocated = []
    started = asyncio.Event()
    original_init = OcteliumClient.__init__

    def capture(self, config):
        original_init(self, config)
        allocated.append(self)

        async def stalled(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        self._auth_stub.authenticate_with_authentication_token = stalled

    monkeypatch.setattr(OcteliumClient, "__init__", capture)
    config = OcteliumClientConfig(
        domain="example.test",
        auth=AuthConfig(type="auth_token", auth_token=AuthTokenConfig(token="credential")),
        authenticate_on_creation=True,
    )
    task = asyncio.create_task(OcteliumClient.create(config))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert allocated[0]._is_closed
    assert allocated[0]._auth_task is None
    assert allocated[0]._close_task.done()


async def test_eager_oauth_failure_closes_http_session(monkeypatch):
    allocated = []
    original_init = OcteliumClient.__init__

    def capture(self, config):
        original_init(self, config)
        allocated.append(self)

        async def failed(*args, **kwargs):
            await self._get_oauth_session()
            raise RuntimeError("bad OAuth response")

        self._fetch_oauth2_token = failed

    monkeypatch.setattr(OcteliumClient, "__init__", capture)
    config = OcteliumClientConfig(
        domain="example.test",
        auth=AuthConfig(
            type="oauth2_client_credentials",
            oauth2_client_credentials=OAuth2ClientCredentialsConfig(
                client_id="client", client_secret="secret"
            ),
        ),
        authenticate_on_creation=True,
    )
    with pytest.raises(RuntimeError, match="bad OAuth response"):
        await OcteliumClient.create(config)
    assert allocated[0]._is_closed
    assert allocated[0]._oauth_session is None


async def test_cleanup_failure_does_not_mask_application_exception():
    client = OcteliumClient(
        OcteliumClientConfig(
            domain="example.test", auth=AuthConfig(type="access_token", access_token="token")
        )
    )
    helper = client.http_client()
    helper.close = AsyncMock(side_effect=RuntimeError("cleanup failed"))
    oauth = await client._get_oauth_session()
    with pytest.raises(ValueError, match="application failed"):
        async with client:
            raise ValueError("application failed")
    assert client._is_closed
    helper.close.assert_awaited_once()
    assert oauth.closed
    assert client._close_task.exception().args == ("cleanup failed",)


def test_client_rejects_cross_loop_reuse():
    async def build():
        return OcteliumClient(
            OcteliumClientConfig(
                domain="example.test", auth=AuthConfig(type="access_token", access_token="token")
            )
        )

    client = asyncio.run(build())
    with pytest.raises(RuntimeError, match="event loop"):
        asyncio.run(client.get_access_token())
    client._channel.close()
    client._auth_channel.close()


async def test_cancelled_close_preserves_cancellation_when_cleanup_fails():
    client = OcteliumClient(
        OcteliumClientConfig(
            domain="example.test", auth=AuthConfig(type="access_token", access_token="token")
        )
    )
    session = await client._get_oauth_session()
    started, release = asyncio.Event(), asyncio.Event()
    original_close = session.close

    async def failing_close():
        started.set()
        await release.wait()
        await original_close()
        raise RuntimeError("cleanup failed")

    session.close = failing_close
    task = asyncio.create_task(client.close())
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.closed
    assert client._close_task.exception().args == ("cleanup failed",)


async def test_tls_factory_rejects_shared_context(sdk_tls):
    config = OcteliumClientConfig(domain="example.test", ssl_context_factory=lambda: sdk_tls.client)
    with pytest.raises(ValueError, match="new SSLContext"):
        OcteliumClient(config)


async def test_assertion_sessions_reauthenticate_with_fresh_assertions(
    sdk_client_factory, sdk_cluster
):
    values = iter(["first", "second"])

    async def provider():
        return next(values)

    client = sdk_client_factory(
        auth=AuthConfig(
            type="assertion",
            assertion=AssertionConfig(token=provider, scopes=["scope"], identity_provider="k8s"),
        )
    )
    assert await client.get_access_token() == "access-1"
    client._session_token_set_at = time.monotonic() - 1800
    sdk_cluster.auth.refresh_error = GRPCError(Status.UNAUTHENTICATED, "revoked")
    assert await client.get_access_token() == "access-2"
    assert sdk_cluster.auth.assertions == [
        ("first", ["scope"], "k8s"),
        ("second", ["scope"], "k8s"),
    ]
    await client.logout()
    assert sdk_cluster.auth.logout_calls == 1
    with pytest.raises(ValueError, match="async"):
        AssertionConfig(token=lambda: "assertion")
    with pytest.raises(ValueError):
        AssertionConfig(token=" ")


async def test_access_token_provider_is_consulted_for_each_call(sdk_client_factory, sdk_cluster):
    values = iter(["environment-access", " "])

    async def provider():
        return next(values)

    client = sdk_client_factory(auth=AuthConfig(type="access_token", access_token=provider))
    result = await client.cordium_v1.get_workspace(GetOptions(name="workspace"))
    assert result.metadata.name == "workspace"
    assert sdk_cluster.metadata[-1][1]["x-octelium-auth"] == "environment-access"
    with pytest.raises(RuntimeError, match="empty"):
        await client.get_access_token()


async def test_rejected_session_token_is_replaced_for_the_next_call(
    sdk_client_factory, sdk_cluster
):
    client = sdk_client_factory()
    assert client.domain == "example.test"
    stub = p.MainServiceStub(client.channel)
    await stub.get_workspace(GetOptions(name="workspace"))
    sdk_cluster.auth.revoked.add("access-1")
    with pytest.raises(GRPCError) as error:
        await stub.get_workspace(GetOptions(name="workspace"))
    assert error.value.status is Status.UNAUTHENTICATED
    result = await stub.get_workspace(GetOptions(name="workspace"))
    assert result.metadata.name == "workspace"
    client._invalidate_access_token("access-1")
    assert await client.get_access_token() == "access-2"
    assert sdk_cluster.auth.refresh_calls == 1
    assert [
        values["x-octelium-auth"]
        for method, values in sdk_cluster.metadata
        if method.endswith("/GetWorkspace")
    ] == ["access-1", "access-1", "access-2"]
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        _ = client.channel
