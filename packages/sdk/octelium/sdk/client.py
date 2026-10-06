from __future__ import annotations

import asyncio
import inspect
import ipaddress
import json
import math
import os
import random
import re
import ssl
import time
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Any, Literal, TypeVar
from urllib.parse import urlsplit

import aiohttp
from grpclib.client import Channel, Stream
from grpclib.config import Configuration
from grpclib.const import Cardinality, Status
from grpclib.events import SendRequest, listen
from grpclib.exceptions import GRPCError, StreamTerminatedError
from grpclib.metadata import Deadline
from multidict import CIMultiDict
from yarl import URL

from octelium.api.main.auth.v1 import (
    AuthenticateWithAuthenticationTokenRequest,
    AuthenticateWithRefreshTokenRequest,
    LogoutRequest,
    SessionToken,
)
from octelium.api.main.auth.v1 import (
    MainServiceStub as AuthStub,
)
from octelium.api.main.cordium.v1 import MainServiceStub as CordiumStub
from octelium.api.main.core.v1 import MainServiceStub as CoreStub
from octelium.api.main.user.v1 import MainServiceStub as UserStub

__all__ = [
    "OcteliumClient",
    "OcteliumClientConfig",
    "AuthConfig",
    "AuthTokenConfig",
    "OAuth2ClientCredentialsConfig",
    "AuthenticatedHTTPClient",
    "run_sync",
]


def _scopes(values: Sequence[str]) -> tuple[str, ...]:
    if (
        not isinstance(values, Sequence)
        or isinstance(values, str)
        or any(not isinstance(value, str) or not value for value in values)
    ):
        raise ValueError("scopes must be a sequence of nonempty strings")
    return tuple(values)


def _positive_number(value: float, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be finite and positive")


def _normalize_host(value: str) -> str:
    value = value.strip().removesuffix(".").lower()
    if not value or any(char in value for char in "/?#@"):
        raise ValueError("invalid hostname")
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        pass
    try:
        value = value.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("invalid hostname") from exc
    if len(value) > 253 or any(
        not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
        for label in value.split(".")
    ):
        raise ValueError("invalid hostname")
    return value


def _origin(value: str, allow_insecure: bool) -> str:
    url = URL(value)
    if (
        url.scheme not in ("https", "http")
        or not url.host
        or (url.raw_user is not None or "@" in urlsplit(value).netloc)
    ):
        raise ValueError("authorized HTTP origins must be absolute URLs without userinfo")
    if url.scheme == "http" and not allow_insecure:
        raise ValueError("plain HTTP requires allow_insecure_http=True")
    if url.path != "/" or url.query_string or url.fragment:
        raise ValueError("authorized HTTP origins must not contain a path, query or fragment")
    return str(url.origin())


@dataclass(frozen=True)
class AuthTokenConfig:
    token: str | Callable[[], Awaitable[str]] = field(repr=False)
    scopes: Sequence[str] = field(default_factory=tuple)
    reusable: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.token, str):
            if not self.token.strip():
                raise ValueError("authentication token must not be empty")
            object.__setattr__(self, "token", self.token.strip())
        elif not (
            inspect.iscoroutinefunction(self.token)
            or (callable(self.token) and inspect.iscoroutinefunction(type(self.token).__call__))
        ):
            raise ValueError("authentication token providers must be async functions")
        if not isinstance(self.reusable, bool):
            raise ValueError("reusable must be a boolean")
        if self.reusable and isinstance(self.token, str):
            raise ValueError("reusable authentication requires an async token provider")
        object.__setattr__(self, "scopes", _scopes(self.scopes))


@dataclass(frozen=True)
class OAuth2ClientCredentialsConfig:
    client_id: str
    client_secret: str = field(repr=False)
    scopes: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.client_id, str) or not self.client_id:
            raise ValueError("oauth2 client_id is required")
        if not isinstance(self.client_secret, str) or not self.client_secret:
            raise ValueError("oauth2 client_secret is required")
        object.__setattr__(self, "scopes", _scopes(self.scopes))


@dataclass(frozen=True)
class AuthConfig:
    type: Literal["auth_token", "oauth2_client_credentials", "access_token"]
    auth_token: AuthTokenConfig | None = None
    oauth2_client_credentials: OAuth2ClientCredentialsConfig | None = None
    access_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        values = (self.auth_token, self.oauth2_client_credentials, self.access_token)
        expected = {"auth_token": 0, "oauth2_client_credentials": 1, "access_token": 2}
        if (
            not isinstance(self.type, str)
            or self.type not in expected
            or any(
                (value is not None) != (index == expected[self.type])
                for index, value in enumerate(values)
            )
        ):
            raise ValueError("auth config must contain exactly the credential matching its type")
        if self.type == "auth_token" and not isinstance(self.auth_token, AuthTokenConfig):
            raise ValueError("auth_token must be an AuthTokenConfig")
        if self.type == "oauth2_client_credentials" and not isinstance(
            self.oauth2_client_credentials, OAuth2ClientCredentialsConfig
        ):
            raise ValueError("oauth2_client_credentials must be an OAuth2ClientCredentialsConfig")
        if self.type == "access_token":
            if not isinstance(self.access_token, str) or not self.access_token.strip():
                raise ValueError("access_token must be a nonempty string")
            object.__setattr__(self, "access_token", self.access_token.strip())


@dataclass(frozen=True)
class OcteliumClientConfig:
    domain: str = ""
    auth: AuthConfig | None = None
    authenticate_on_creation: bool = False
    insecure_tls: bool = False
    refresh_before_expiry_seconds: float = 30.0
    authentication_timeout_seconds: float = 20.0
    oauth2_timeout_seconds: float = 10.0
    max_oauth2_expires_in_seconds: int | None = None
    api_host: str = ""
    api_port: int = 443
    ssl_context_factory: Callable[[], ssl.SSLContext] | None = field(default=None, repr=False)
    tls_server_name: str = ""
    allow_insecure_http: bool = False
    authorized_http_origins: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.domain, str) or not isinstance(self.api_host, str):
            raise ValueError("domain and api_host must be strings")
        if self.domain:
            domain = _normalize_host(self.domain)
            if ":" in domain:
                raise ValueError("Cluster domain must not be an IPv6 address")
            object.__setattr__(self, "domain", domain)
        if self.api_host:
            object.__setattr__(self, "api_host", _normalize_host(self.api_host))
        if not isinstance(self.tls_server_name, str):
            raise ValueError("tls_server_name must be a string")
        if any(
            not isinstance(value, bool)
            for value in (
                self.authenticate_on_creation,
                self.insecure_tls,
                self.allow_insecure_http,
            )
        ):
            raise ValueError("client boolean options must be booleans")
        if self.tls_server_name:
            object.__setattr__(self, "tls_server_name", _normalize_host(self.tls_server_name))
        if self.auth is not None and not isinstance(self.auth, AuthConfig):
            raise ValueError("auth must be an AuthConfig")
        if type(self.api_port) is not int or not 1 <= self.api_port <= 65535:
            raise ValueError("api_port must be between 1 and 65535")
        _positive_number(self.authentication_timeout_seconds, "authentication_timeout_seconds")
        _positive_number(self.oauth2_timeout_seconds, "oauth2_timeout_seconds")
        margin = self.refresh_before_expiry_seconds
        if (
            isinstance(margin, bool)
            or not isinstance(margin, (int, float))
            or not math.isfinite(margin)
            or margin < 0
        ):
            raise ValueError("refresh_before_expiry_seconds must be finite and nonnegative")
        maximum = self.max_oauth2_expires_in_seconds
        if maximum is not None and (type(maximum) is not int or maximum <= 0):
            raise ValueError("max_oauth2_expires_in_seconds must be a positive integer or None")
        if self.ssl_context_factory is not None and (
            not callable(self.ssl_context_factory) or self.insecure_tls
        ):
            raise ValueError(
                "ssl_context_factory must be callable and cannot be combined with insecure_tls"
            )
        if not isinstance(self.authorized_http_origins, Sequence) or isinstance(
            self.authorized_http_origins, str
        ):
            raise ValueError("authorized_http_origins must be a sequence of origins")
        object.__setattr__(
            self,
            "authorized_http_origins",
            tuple(
                _origin(value, self.allow_insecure_http) for value in self.authorized_http_origins
            ),
        )


class _OAuth2HTTPError(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__(f"OAuth2 token fetch failed with HTTP status {status}")
        self.status = status


@dataclass
class _OAuth2Cache:
    access_token: str = field(repr=False)
    expires_at: float
    refresh_at: float


_Request = TypeVar("_Request")
_Response = TypeVar("_Response")


class _ClientChannel(Channel):
    def __init__(self, owner: OcteliumClient, **kwargs: Any) -> None:
        self._owner = owner
        super().__init__(**kwargs)

    def request(
        self,
        name: str,
        cardinality: Cardinality,
        request_type: type[_Request],
        reply_type: type[_Response],
        *,
        timeout: float | None = None,
        deadline: Deadline | None = None,
        metadata: Any = None,
    ) -> Stream[_Request, _Response]:
        self._owner._ensure_open()
        return super().request(
            name,
            cardinality,
            request_type,
            reply_type,
            timeout=timeout,
            deadline=deadline,
            metadata=metadata,
        )


class OcteliumClient:
    def __init__(self, config: OcteliumClientConfig) -> None:
        domain = config.domain or os.environ.get("OCTELIUM_DOMAIN", "")
        if not domain:
            raise ValueError("domain is required via config.domain or OCTELIUM_DOMAIN")
        auth = config.auth
        if auth is None:
            if value := os.environ.get("OCTELIUM_ACCESS_TOKEN"):
                auth = AuthConfig(type="access_token", access_token=value)
            elif value := os.environ.get("OCTELIUM_AUTH_TOKEN"):
                auth = AuthConfig(type="auth_token", auth_token=AuthTokenConfig(token=value))
        self._config = replace(config, domain=domain, auth=auth)
        self._loop = asyncio.get_running_loop()
        self._session_token: SessionToken | None = None
        self._session_token_set_at: float | None = None
        self._oauth2_cache: _OAuth2Cache | None = None
        self._authentication_attempted = False
        self._auth_task: asyncio.Task[str] | None = None
        self._logout_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._auth_error: Exception | None = None
        self._retry_usable = False
        self._retry_at = 0.0
        self._retry_delay = 0.0
        self._is_closed = False
        self._http_clients: set[AuthenticatedHTTPClient] = set()
        self._core_v1: CoreStub | None = None
        self._user_v1: UserStub | None = None
        self._cordium_v1: CordiumStub | None = None
        self._oauth_session: aiohttp.ClientSession | None = None
        self._ssl_context = self._new_ssl_context(self._config)
        self._grpc_ssl_context = self._new_ssl_context(self._config)
        if self._ssl_context is self._grpc_ssl_context:
            raise ValueError("ssl_context_factory must return a new SSLContext for each call")
        self._ssl_context.set_alpn_protocols(["http/1.1"])
        self._grpc_ssl_context.set_alpn_protocols(["h2"])
        host = self._config.api_host or f"octelium-api.{self._config.domain}"
        channel_config = Configuration(
            ssl_target_name_override=self._config.tls_server_name or None
        )
        self._channel = _ClientChannel(
            self,
            host=host,
            port=self._config.api_port,
            ssl=self._grpc_ssl_context,
            config=channel_config,
        )
        try:
            listen(self._channel, SendRequest, self._on_main_send_request)
            self._auth_channel = _ClientChannel(
                self,
                host=host,
                port=self._config.api_port,
                ssl=self._grpc_ssl_context,
                config=channel_config,
            )
            listen(self._auth_channel, SendRequest, self._on_auth_send_request)
            self._auth_stub = AuthStub(self._auth_channel)
        except BaseException:
            self._channel.close()
            if hasattr(self, "_auth_channel"):
                self._auth_channel.close()
            raise

    @staticmethod
    def _new_ssl_context(config: OcteliumClientConfig) -> ssl.SSLContext:
        if config.ssl_context_factory is not None:
            context = config.ssl_context_factory()
            if not isinstance(context, ssl.SSLContext):
                raise ValueError("ssl_context_factory must return an SSLContext")
            return context
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        if config.insecure_tls:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @classmethod
    async def create(cls, config: OcteliumClientConfig | None = None) -> OcteliumClient:
        client = cls(config or OcteliumClientConfig())
        try:
            if client._config.authenticate_on_creation:
                await client.get_access_token()
        except BaseException:
            with suppress(Exception):
                await client.close()
            raise
        return client

    def _ensure_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("OcteliumClient must be used on the event loop that created it")

    def _ensure_open(self) -> None:
        self._ensure_loop()
        if self._is_closed:
            raise RuntimeError("OcteliumClient is closed")

    @staticmethod
    def _set_metadata(event: SendRequest, key: str, value: str) -> None:
        event.metadata[key] = value

    async def _on_main_send_request(self, event: SendRequest) -> None:
        token = await self.get_access_token()
        self._ensure_open()
        self._set_metadata(event, "x-octelium-auth", token)

    async def _on_auth_send_request(self, event: SendRequest) -> None:
        self._ensure_open()
        if self._session_token is not None and self._session_token.refresh_token:
            self._set_metadata(event, "x-octelium-refresh-token", self._session_token.refresh_token)

    def _session_expires_at(self) -> float:
        if self._session_token is None or self._session_token_set_at is None:
            return 0.0
        return self._session_token_set_at + self._session_token.expires_in

    def _needs_new_access_token(self) -> bool:
        token = self._session_token
        if token is None or token.expires_in <= 0:
            return True
        margin = min(self._config.refresh_before_expiry_seconds, token.expires_in / 5)
        return time.monotonic() >= self._session_expires_at() - margin

    def _usable_token(self) -> str | None:
        now = time.monotonic()
        if self._session_token is not None and now < self._session_expires_at():
            return self._session_token.access_token
        if self._oauth2_cache is not None and now < self._oauth2_cache.expires_at:
            return self._oauth2_cache.access_token
        return None

    async def _do_get_access_token(self) -> str:
        self._ensure_open()
        if self._logout_task is not None:
            await asyncio.shield(self._logout_task)
            self._ensure_open()
        auth = self._config.auth
        if auth is None:
            raise RuntimeError(
                "no auth config provided; set config.auth, OCTELIUM_AUTH_TOKEN, or OCTELIUM_ACCESS_TOKEN"
            )
        if auth.type == "access_token":
            assert auth.access_token is not None
            return auth.access_token
        if auth.type == "auth_token" and not self._needs_new_access_token():
            assert self._session_token is not None
            return self._session_token.access_token
        if (
            auth.type == "oauth2_client_credentials"
            and self._oauth2_cache is not None
            and time.monotonic() < self._oauth2_cache.refresh_at
        ):
            return self._oauth2_cache.access_token
        if self._auth_task is None:
            if time.monotonic() < self._retry_at and self._auth_error is not None:
                if self._retry_usable and (token := self._usable_token()):
                    return token
                raise self._auth_error
            self._auth_task = asyncio.create_task(self._run_authentication())
            self._auth_task.add_done_callback(self._consume_task_error)
        return await asyncio.shield(self._auth_task)

    @staticmethod
    def _consume_task_error(task: asyncio.Task[Any]) -> None:
        if not task.cancelled():
            task.exception()

    async def _run_authentication(self) -> str:
        try:
            async with asyncio.timeout(self._config.authentication_timeout_seconds):
                auth = self._config.auth
                assert auth is not None
                if auth.type == "auth_token":
                    await self._set_access_token_response()
                    assert self._session_token is not None
                    token = self._session_token.access_token
                else:
                    assert auth.oauth2_client_credentials is not None
                    token = await self._fetch_oauth2_token(auth.oauth2_client_credentials)
                self._ensure_open()
                self._auth_error = None
                self._retry_at = self._retry_delay = 0.0
                return token
        except Exception as exc:
            if self._is_closed:
                raise
            self._auth_error = exc
            self._retry_delay = min(self._retry_delay * 2, 30.0) if self._retry_delay else 1.0
            self._retry_at = time.monotonic() + random.uniform(
                self._retry_delay / 2, self._retry_delay
            )
            transient = (
                isinstance(exc, (TimeoutError, OSError, aiohttp.ClientError, StreamTerminatedError))
                or (
                    isinstance(exc, _OAuth2HTTPError)
                    and (exc.status == 429 or 500 <= exc.status < 600)
                )
            ) or (
                isinstance(exc, GRPCError)
                and exc.status
                in (
                    Status.UNAVAILABLE,
                    Status.DEADLINE_EXCEEDED,
                    Status.RESOURCE_EXHAUSTED,
                    Status.ALREADY_EXISTS,
                )
            )
            self._retry_usable = transient
            if transient and not self._is_closed and (fallback := self._usable_token()):
                return fallback
            raise
        finally:
            self._auth_task = None

    async def _resolve_authentication_token(self, cfg: AuthTokenConfig) -> str:
        value = cfg.token if isinstance(cfg.token, str) else await cfg.token()
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError("authentication token provider returned an empty token")
        return value.strip()

    async def _authenticate_new_session(self, cfg: AuthTokenConfig) -> SessionToken:
        if self._authentication_attempted and not cfg.reusable:
            raise RuntimeError(
                "Session expired or authentication outcome was ambiguous; authentication token cannot be reused"
            )
        token = await self._resolve_authentication_token(cfg)
        self._ensure_open()
        self._authentication_attempted = True
        return await self._auth_stub.authenticate_with_authentication_token(
            AuthenticateWithAuthenticationTokenRequest(
                authentication_token=token, scopes=list(cfg.scopes)
            ),
            timeout=self._config.authentication_timeout_seconds,
        )

    async def _set_access_token_response(self) -> None:
        auth = self._config.auth
        assert auth is not None and auth.auth_token is not None
        snapshot = self._session_token
        started = time.monotonic()
        if (
            snapshot is not None
            and self._session_token_set_at is not None
            and started >= self._session_token_set_at + snapshot.refresh_token_expires_in
        ):
            self._clear_session()
            snapshot = None
        if snapshot is None:
            resp = await self._authenticate_new_session(auth.auth_token)
        else:
            try:
                resp = await self._auth_stub.authenticate_with_refresh_token(
                    AuthenticateWithRefreshTokenRequest(),
                    timeout=self._config.authentication_timeout_seconds,
                )
            except GRPCError as exc:
                if exc.status == Status.ALREADY_EXISTS:
                    if self._session_token is snapshot and self._usable_token() is not None:
                        raise
                    raise RuntimeError(
                        "refresh was rate limited without a usable access token"
                    ) from exc
                if exc.status != Status.UNAUTHENTICATED:
                    raise
                self._clear_session()
                if not auth.auth_token.reusable:
                    raise RuntimeError(
                        "Session expired and the authentication token cannot be reused"
                    ) from exc
                started = time.monotonic()
                resp = await self._authenticate_new_session(auth.auth_token)
        self._validate_session_token(resp)
        self._ensure_open()
        if time.monotonic() >= started + resp.expires_in:
            raise RuntimeError("authentication response expired during the exchange")
        self._session_token = resp
        self._session_token_set_at = started

    @staticmethod
    def _validate_session_token(token: SessionToken) -> None:
        if (
            not isinstance(token, SessionToken)
            or not token.access_token.strip()
            or not token.refresh_token.strip()
        ):
            raise RuntimeError("authentication response must include access and refresh tokens")
        if (
            type(token.expires_in) is not int
            or token.expires_in <= 0
            or type(token.refresh_token_expires_in) is not int
            or token.refresh_token_expires_in < token.expires_in
        ):
            raise RuntimeError("authentication response has invalid token lifetimes")

    async def _get_oauth_session(self) -> aiohttp.ClientSession:
        self._ensure_open()
        if self._oauth_session is None or self._oauth_session.closed:
            timeout = aiohttp.ClientTimeout(total=self._config.oauth2_timeout_seconds)
            self._oauth_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=aiohttp.TCPConnector(ssl=self._ssl_context),
                cookie_jar=aiohttp.DummyCookieJar(),
            )
        return self._oauth_session

    async def _fetch_oauth2_token(self, auth: OAuth2ClientCredentialsConfig) -> str:
        started = time.monotonic()
        data = {
            "grant_type": "client_credentials",
            "client_id": auth.client_id,
            "client_secret": auth.client_secret,
        }
        if auth.scopes:
            data["scope"] = " ".join(auth.scopes)
        session = await self._get_oauth_session()
        async with session.post(
            f"https://{self._config.domain}/oauth2/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            allow_redirects=False,
        ) as resp:
            if not 200 <= resp.status < 300:
                raise _OAuth2HTTPError(resp.status)
            body = bytearray()
            while chunk := await resp.content.read(min(8192, 65537 - len(body))):
                body.extend(chunk)
                if len(body) > 65536:
                    raise RuntimeError("OAuth2 token response exceeded 65536 bytes")
            if resp.content_type not in ("application/json", "application/problem+json"):
                raise RuntimeError("OAuth2 token endpoint returned an invalid content type")
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeError) as exc:
                raise RuntimeError("OAuth2 token endpoint returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("OAuth2 token endpoint must return a JSON object")
        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise RuntimeError("OAuth2 token response missing access_token")
        token_type = payload.get("token_type")
        if not isinstance(token_type, str) or token_type.lower() != "bearer":
            raise RuntimeError("OAuth2 token response must use the Bearer token type")
        expires_in = payload.get("expires_in")
        if type(expires_in) is not int or expires_in <= 0:
            raise RuntimeError("OAuth2 token response has invalid expires_in")
        maximum = self._config.max_oauth2_expires_in_seconds
        if maximum is not None and expires_in > maximum:
            raise RuntimeError("OAuth2 token response exceeds the configured lifetime maximum")
        try:
            expires_at = started + expires_in
        except OverflowError as exc:
            raise RuntimeError("OAuth2 token response has invalid expires_in") from exc
        if not math.isfinite(expires_at) or time.monotonic() >= expires_at:
            raise RuntimeError("OAuth2 token response expired during the exchange")
        margin = min(self._config.refresh_before_expiry_seconds, expires_in / 5)
        self._ensure_open()
        self._oauth2_cache = _OAuth2Cache(access_token.strip(), expires_at, expires_at - margin)
        return self._oauth2_cache.access_token

    async def get_access_token(self) -> str:
        return await self._do_get_access_token()

    def http_client(
        self, *, timeout: aiohttp.ClientTimeout | None = None
    ) -> AuthenticatedHTTPClient:
        self._ensure_open()
        return AuthenticatedHTTPClient(self, timeout=timeout)

    def _clear_session(self) -> None:
        self._session_token = None
        self._session_token_set_at = None

    async def logout(self) -> None:
        self._ensure_open()
        if self._config.auth is None or self._config.auth.type != "auth_token":
            raise RuntimeError("client does not own a managed Cluster Session")
        if self._logout_task is None:
            self._logout_task = asyncio.create_task(self._logout())
            self._logout_task.add_done_callback(self._consume_task_error)
        await asyncio.shield(self._logout_task)

    async def _logout(self) -> None:
        try:
            async with asyncio.timeout(self._config.authentication_timeout_seconds):
                if self._auth_task is not None:
                    await asyncio.shield(self._auth_task)
                self._ensure_open()
                if self._session_token is not None:
                    try:
                        await self._auth_stub.logout(
                            LogoutRequest(), timeout=self._config.authentication_timeout_seconds
                        )
                    except GRPCError as exc:
                        if exc.status != Status.UNAUTHENTICATED:
                            raise
                    self._clear_session()
                self._auth_error = None
                self._retry_at = self._retry_delay = 0.0
        finally:
            self._logout_task = None

    async def close(self) -> None:
        self._ensure_loop()
        if self._close_task is None:
            self._is_closed = True
            self._close_task = asyncio.create_task(self._close_resources(asyncio.current_task()))
            self._close_task.add_done_callback(self._consume_task_error)
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            with suppress(Exception):
                await asyncio.shield(self._close_task)
            raise

    async def _close_resources(self, initiator: asyncio.Task[Any] | None) -> None:
        tasks = [
            task
            for task in (self._auth_task, self._logout_task)
            if task is not None and task is not initiator
        ]
        for task in tasks:
            task.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            cleanup = [client.close() for client in tuple(self._http_clients)]
            if self._oauth_session is not None:
                cleanup.append(self._oauth_session.close())
            results = await asyncio.gather(*cleanup, return_exceptions=True)
            if self._oauth_session is not None and self._oauth_session.closed:
                self._oauth_session = None
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            self._channel.close()
            self._auth_channel.close()
            self._clear_session()
            self._oauth2_cache = None
            self._auth_error = None
            self._retry_at = self._retry_delay = 0.0
            self._core_v1 = self._user_v1 = self._cordium_v1 = None

    async def __aenter__(self) -> OcteliumClient:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if exc is None:
            await self.close()
        else:
            with suppress(Exception):
                await self.close()

    @property
    def core_v1(self) -> CoreStub:
        self._ensure_open()
        if self._core_v1 is None:
            self._core_v1 = CoreStub(self._channel)
        return self._core_v1

    @property
    def user_v1(self) -> UserStub:
        self._ensure_open()
        if self._user_v1 is None:
            self._user_v1 = UserStub(self._channel)
        return self._user_v1

    @property
    def cordium_v1(self) -> CordiumStub:
        self._ensure_open()
        if self._cordium_v1 is None:
            self._cordium_v1 = CordiumStub(self._channel)
        return self._cordium_v1


class AuthenticatedHTTPClient:
    def __init__(
        self, client: OcteliumClient, *, timeout: aiohttp.ClientTimeout | None = None
    ) -> None:
        client._ensure_open()
        self._client = client
        self._timeout = timeout or aiohttp.ClientTimeout(total=30)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._requests: set[asyncio.Task[aiohttp.ClientResponse]] = set()
        self._client._http_clients.add(self)

    def _ensure_open(self) -> None:
        self._client._ensure_open()
        if self._closed:
            raise RuntimeError("AuthenticatedHTTPClient is closed")

    async def _get_session(self) -> aiohttp.ClientSession:
        self._ensure_open()
        async with self._session_lock:
            self._ensure_open()
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=self._timeout,
                    connector=aiohttp.TCPConnector(ssl=self._client._ssl_context),
                    cookie_jar=aiohttp.DummyCookieJar(),
                )
            return self._session

    def _authorize(self, url: str, headers: CIMultiDict[str]) -> None:
        target = URL(url)
        if (
            not target.host
            or (target.raw_user is not None or "@" in urlsplit(url).netloc)
            or target.scheme not in ("http", "https")
        ):
            raise ValueError("authenticated HTTP requires an absolute HTTP(S) URL without userinfo")
        cfg = self._client._config
        if target.scheme == "http" and not cfg.allow_insecure_http:
            raise ValueError("plain HTTP is disabled")
        hosts = headers.getall("Host", [])
        if any(value != value.strip() or any(char in value for char in "/?#@") for value in hosts):
            raise ValueError("invalid Host header")
        if hosts and (
            len(hosts) != 1 or URL(f"{target.scheme}://{hosts[0]}").origin() != target.origin()
        ):
            raise ValueError("Host header must match the request origin")
        host = (target.raw_host or "").lower().removesuffix(".")
        cluster_host = host == cfg.domain or host.endswith("." + cfg.domain)
        default_port = 443 if target.scheme == "https" else 80
        if (
            not (cluster_host and target.port == default_port)
            and str(target.origin()) not in cfg.authorized_http_origins
        ):
            raise ValueError("HTTP destination is not authorized to receive the access token")

    async def request(self, method: str, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        self._ensure_open()
        headers = CIMultiDict[str](kwargs.pop("headers", None) or {})
        self._authorize(url, headers)
        if kwargs.pop("allow_redirects", False):
            raise ValueError("authenticated HTTP redirects are disabled")
        timeout = kwargs.pop("timeout", self._timeout)
        if not isinstance(timeout, aiohttp.ClientTimeout):
            raise ValueError("timeout must be an aiohttp.ClientTimeout")
        if timeout.total is not None:
            _positive_number(timeout.total, "HTTP timeout total")
        task = asyncio.create_task(self._request(method, url, headers, timeout, kwargs))
        self._requests.add(task)
        try:
            return await task
        finally:
            self._requests.discard(task)

    async def _request(
        self,
        method: str,
        url: str,
        headers: CIMultiDict[str],
        timeout: aiohttp.ClientTimeout,
        kwargs: dict[str, Any],
    ) -> aiohttp.ClientResponse:
        started = asyncio.get_running_loop().time()
        async with asyncio.timeout(timeout.total):
            token = await self._client.get_access_token()
            self._ensure_open()
            headers["x-octelium-auth"] = token
            session = await self._get_session()
            if timeout.total is not None:
                remaining = timeout.total - (asyncio.get_running_loop().time() - started)
                if remaining <= 0:
                    raise TimeoutError("HTTP operation deadline exceeded")
                timeout = aiohttp.ClientTimeout(
                    total=remaining,
                    connect=timeout.connect,
                    sock_read=timeout.sock_read,
                    sock_connect=timeout.sock_connect,
                    ceil_threshold=timeout.ceil_threshold,
                )
            return await session.request(
                method, url, headers=headers, timeout=timeout, allow_redirects=False, **kwargs
            )

    async def get(self, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> aiohttp.ClientResponse:
        return await self.request("DELETE", url, **kwargs)

    async def close(self) -> None:
        self._client._ensure_loop()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close_resources())
            self._close_task.add_done_callback(self._client._consume_task_error)
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            with suppress(Exception):
                await asyncio.shield(self._close_task)
            raise

    async def _close_resources(self) -> None:
        for task in tuple(self._requests):
            task.cancel()
        await asyncio.gather(*self._requests, return_exceptions=True)
        async with self._session_lock:
            if self._session is not None:
                await self._session.close()
                self._session = None
        self._client._http_clients.discard(self)

    async def __aenter__(self) -> AuthenticatedHTTPClient:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if exc is None:
            await self.close()
        else:
            with suppress(Exception):
                await self.close()


_Result = TypeVar("_Result")


def run_sync(coro: Coroutine[Any, Any, _Result]) -> _Result:
    typed_coro = coro
    if not inspect.iscoroutine(coro):
        raise TypeError("run_sync() requires a coroutine")
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(typed_coro)
    coro.close()
    raise RuntimeError("run_sync() cannot be used inside an async context; use await instead")
