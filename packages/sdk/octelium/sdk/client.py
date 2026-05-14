from __future__ import annotations

import asyncio
import inspect
import logging
import os
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Optional, Union

import aiohttp
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.events import SendRequest, listen
from grpclib.exceptions import GRPCError

from octelium.apis.main.authv1 import (
    AuthenticateWithAuthenticationTokenRequest,
    AuthenticateWithRefreshTokenRequest,
    LogoutRequest,
    MainServiceStub as _AuthStub,
    SessionToken,
)
from octelium.apis.main.corev1 import MainServiceStub as _CoreStub
from octelium.apis.main.userv1 import MainServiceStub as _UserStub

__all__ = [
    "OcteliumClient",
    "OcteliumClientConfig",
    "AuthConfig",
    "AuthTokenConfig",
    "OAuth2ClientCredentialsConfig",
    "AuthenticatedHTTPClient",
    "run_sync",
]

log = logging.getLogger(__name__)


@dataclass
class AuthTokenConfig:
    token: Union[str, Callable[[], Union[str, Awaitable[str]]]]
    scopes: list[str] = field(default_factory=list)


@dataclass
class OAuth2ClientCredentialsConfig:
    client_id: str
    client_secret: str
    scopes: list[str] = field(default_factory=list)


@dataclass
class AuthConfig:
    type: Literal["auth_token", "oauth2_client_credentials", "access_token"]
    auth_token: Optional[AuthTokenConfig] = None
    oauth2_client_credentials: Optional[OAuth2ClientCredentialsConfig] = None
    access_token: Optional[str] = None


@dataclass
class OcteliumClientConfig:
    domain: str = ""
    auth: Optional[AuthConfig] = None
    authenticate_on_creation: bool = False
    insecure_tls: bool = False
    refresh_before_expiry_seconds: int = 30
    oauth2_timeout_seconds: float = 10.0
    max_oauth2_expires_in_seconds: int = 24 * 60 * 60
    raise_logout_errors: bool = True
    api_host: str = ""
    api_port: int = 443


@dataclass
class _OAuth2Cache:
    access_token: str
    expires_at: float


class OcteliumClient:
    def __init__(self, config: OcteliumClientConfig) -> None:
        if not config.domain:
            config.domain = os.environ.get("OCTELIUM_DOMAIN", "")
        if not config.domain:
            raise ValueError("domain is required via config.domain or OCTELIUM_DOMAIN")

        self._config = config
        self._session_token: Optional[SessionToken] = None
        self._session_token_set_at: Optional[float] = None
        self._oauth2_cache: Optional[_OAuth2Cache] = None

        self._refresh_lock: Optional[asyncio.Lock] = None
        self._oauth2_lock: Optional[asyncio.Lock] = None
        self._close_lock: Optional[asyncio.Lock] = None
        self._is_closed = False

        self._core_v1: Optional[_CoreStub] = None
        self._user_v1: Optional[_UserStub] = None
        self._oauth_session: Optional[aiohttp.ClientSession] = None

        ssl_ctx = self._new_ssl_context(config)
        host = config.api_host or f"octelium-api.{config.domain}"

        self._channel = Channel(host=host, port=config.api_port, ssl=ssl_ctx)
        if config.auth:
            listen(self._channel, SendRequest, self._on_main_send_request)

        self._auth_channel = Channel(host=host, port=config.api_port, ssl=ssl_ctx)
        listen(self._auth_channel, SendRequest, self._on_auth_send_request)

        self._auth_stub = _AuthStub(self._auth_channel)

    @staticmethod
    def _new_ssl_context(config: OcteliumClientConfig) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        insecure = config.insecure_tls or os.environ.get("OCTELIUM_INSECURE_TLS", "").lower() == "true"
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        return ctx

    @classmethod
    async def create(
        cls,
        config: Optional[OcteliumClientConfig] = None,
    ) -> "OcteliumClient":
        if config is None:
            config = OcteliumClientConfig()

        if config.auth is None:
            auth_token = os.environ.get("OCTELIUM_AUTH_TOKEN")
            if auth_token:
                config.auth = AuthConfig(
                    type="auth_token",
                    auth_token=AuthTokenConfig(token=auth_token),
                )

        client = cls(config)

        if (
            config.authenticate_on_creation
            and config.auth is not None
            and config.auth.type != "access_token"
        ):
            await client._do_get_access_token()

        return client

    def _ensure_open(self) -> None:
        if self._is_closed:
            raise RuntimeError("OcteliumClient is closed")

    @staticmethod
    def _set_metadata(event: SendRequest, key: str, value: str) -> None:
        md = event.metadata

        try:
            md[key] = value
            return
        except Exception as exc:
            log.debug("metadata __setitem__ failed for %r: %s", key, exc)

        try:
            md.add(key, value)
            return
        except Exception as exc:
            log.debug("metadata add() failed for %r: %s", key, exc)

        try:
            md.append((key, value))
            return
        except Exception as exc:
            raise RuntimeError(f"could not set outgoing gRPC metadata {key!r}") from exc

    async def _on_main_send_request(self, event: SendRequest) -> None:
        self._ensure_open()
        token = await self._do_get_access_token()
        self._set_metadata(event, "x-octelium-auth", token)

    async def _on_auth_send_request(self, event: SendRequest) -> None:
        self._ensure_open()

        token = self._session_token
        if token is not None and token.refresh_token:
            self._set_metadata(event, "x-octelium-refresh-token", token.refresh_token)

    def _has_session_token(self) -> bool:
        return self._session_token is not None

    def _session_expires_at(self) -> float:
        if self._session_token is None or self._session_token_set_at is None:
            return 0.0
        return self._session_token_set_at + float(self._session_token.expires_in)

    def _needs_new_access_token(self) -> bool:
        if self._session_token is None:
            return True
        if self._session_token.expires_in <= 0:
            return True

        refresh_margin = max(0, self._config.refresh_before_expiry_seconds)
        return time.monotonic() >= self._session_expires_at() - refresh_margin

    async def _do_get_access_token(self) -> str:
        self._ensure_open()

        access_token = os.environ.get("OCTELIUM_ACCESS_TOKEN")
        if access_token:
            return access_token

        auth = self._config.auth
        if auth is None:
            raise RuntimeError(
                "no auth config provided; set config.auth, "
                "OCTELIUM_AUTH_TOKEN, or OCTELIUM_ACCESS_TOKEN"
            )

        if auth.type == "access_token":
            if not auth.access_token:
                raise RuntimeError("auth.access_token is required")
            return auth.access_token

        if auth.type == "auth_token":
            if auth.auth_token is None:
                raise RuntimeError("auth.auth_token is required")

            if not self._needs_new_access_token():
                assert self._session_token is not None
                return self._session_token.access_token

            if self._refresh_lock is None:
                self._refresh_lock = asyncio.Lock()

            async with self._refresh_lock:
                if not self._needs_new_access_token():
                    assert self._session_token is not None
                    return self._session_token.access_token

                await self._set_access_token_response()

            assert self._session_token is not None
            return self._session_token.access_token

        if auth.type == "oauth2_client_credentials":
            if auth.oauth2_client_credentials is None:
                raise RuntimeError("auth.oauth2_client_credentials is required")
            return await self._resolve_oauth2_token(auth.oauth2_client_credentials)

        raise RuntimeError(f"unknown auth type: {auth.type!r}")

    async def _resolve_authentication_token(self, cfg: AuthTokenConfig) -> str:
        value = cfg.token() if callable(cfg.token) else cfg.token
        if inspect.isawaitable(value):
            value = await value

        if not isinstance(value, str) or not value:
            raise RuntimeError("authentication token provider returned an empty token")

        return value

    async def _set_access_token_response(self) -> None:
        auth = self._config.auth
        if auth is None or auth.auth_token is None:
            raise RuntimeError("auth_token config is required")

        if not self._has_session_token():
            token_str = await self._resolve_authentication_token(auth.auth_token)
            resp = await self._auth_stub.authenticate_with_authentication_token(
                AuthenticateWithAuthenticationTokenRequest(
                    authentication_token=token_str,
                    scopes=auth.auth_token.scopes,
                )
            )
        else:
            try:
                resp = await self._auth_stub.authenticate_with_refresh_token(
                    AuthenticateWithRefreshTokenRequest()
                )
            except GRPCError as exc:
                if exc.status == Status.ALREADY_EXISTS:
                    return
                raise

        if not resp.access_token:
            raise RuntimeError("authentication response did not include an access token")
        if resp.expires_in <= 0:
            raise RuntimeError(f"authentication response has invalid expires_in={resp.expires_in!r}")

        self._session_token = resp
        self._session_token_set_at = time.monotonic()

    async def _get_oauth_session(self) -> aiohttp.ClientSession:
        if self._oauth_session is None or self._oauth_session.closed:
            timeout = aiohttp.ClientTimeout(total=self._config.oauth2_timeout_seconds)
            self._oauth_session = aiohttp.ClientSession(timeout=timeout)
        return self._oauth_session

    async def _resolve_oauth2_token(
        self,
        auth: OAuth2ClientCredentialsConfig,
    ) -> str:
        margin = max(0, self._config.refresh_before_expiry_seconds)
        now = time.monotonic()

        if self._oauth2_cache is not None and now < self._oauth2_cache.expires_at - margin:
            return self._oauth2_cache.access_token

        if self._oauth2_lock is None:
            self._oauth2_lock = asyncio.Lock()

        async with self._oauth2_lock:
            now = time.monotonic()
            if self._oauth2_cache is not None and now < self._oauth2_cache.expires_at - margin:
                return self._oauth2_cache.access_token

            return await self._fetch_oauth2_token(auth)

    async def _fetch_oauth2_token(
        self,
        auth: OAuth2ClientCredentialsConfig,
    ) -> str:
        if not auth.client_id:
            raise RuntimeError("oauth2 client_id is required")
        if not auth.client_secret:
            raise RuntimeError("oauth2 client_secret is required")

        token_url = f"https://{self._config.domain}/oauth2/token"
        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": auth.client_id,
            "client_secret": auth.client_secret,
        }

        if auth.scopes:
            data["scope"] = " ".join(auth.scopes)

        session = await self._get_oauth_session()
        async with session.post(
            token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            if resp.status < 200 or resp.status >= 300:
                body = await resp.text()
                raise RuntimeError(
                    f"OAuth2 token fetch failed: "
                    f"status={resp.status} reason={resp.reason} body={body!r}"
                )

            try:
                payload = await resp.json(content_type=None)
            except Exception as exc:
                raise RuntimeError("OAuth2 token endpoint returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"OAuth2 token endpoint returned {type(payload).__name__}, expected object")

        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError("OAuth2 token response missing access_token")

        token_type = payload.get("token_type")
        if token_type is not None and str(token_type).lower() != "bearer":
            raise RuntimeError(f"unsupported OAuth2 token_type={token_type!r}")

        expires_in_raw = payload.get("expires_in")
        try:
            expires_in = int(expires_in_raw)
        except Exception as exc:
            raise RuntimeError("OAuth2 token response missing or invalid expires_in") from exc

        if expires_in <= 0:
            raise RuntimeError(f"OAuth2 token response has invalid expires_in={expires_in}")
        if expires_in > self._config.max_oauth2_expires_in_seconds:
            raise RuntimeError(
                f"OAuth2 token response expires_in={expires_in} exceeds "
                f"configured maximum={self._config.max_oauth2_expires_in_seconds}"
            )

        self._oauth2_cache = _OAuth2Cache(
            access_token=access_token,
            expires_at=time.monotonic() + expires_in,
        )
        return access_token

    async def get_access_token(self) -> str:
        return await self._do_get_access_token()

    def http_client(
        self,
        *,
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> "AuthenticatedHTTPClient":
        self._ensure_open()
        return AuthenticatedHTTPClient(self, timeout=timeout)

    async def close(self) -> None:
        if self._close_lock is None:
            self._close_lock = asyncio.Lock()

        async with self._close_lock:
            if self._is_closed:
                return

            logout_error: Optional[BaseException] = None

            try:
                if self._has_session_token():
                    await asyncio.wait_for(self._auth_stub.logout(LogoutRequest()), timeout=5.0)
            except BaseException as exc:
                logout_error = exc
                log.warning("Octelium logout failed", exc_info=exc)
            finally:
                self._is_closed = True
                self._channel.close()
                self._auth_channel.close()

                if self._oauth_session is not None:
                    await self._oauth_session.close()
                    self._oauth_session = None

            if logout_error is not None and self._config.raise_logout_errors:
                raise logout_error

    async def __aenter__(self) -> "OcteliumClient":
        self._ensure_open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    @property
    def core_v1(self) -> _CoreStub:
        self._ensure_open()
        if self._core_v1 is None:
            self._core_v1 = _CoreStub(self._channel)
        return self._core_v1

    @property
    def user_v1(self) -> _UserStub:
        self._ensure_open()
        if self._user_v1 is None:
            self._user_v1 = _UserStub(self._channel)
        return self._user_v1


class AuthenticatedHTTPClient:
    def __init__(
        self,
        client: OcteliumClient,
        *,
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> None:
        self._client = client
        self._timeout = timeout or aiohttp.ClientTimeout(total=30)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock: Optional[asyncio.Lock] = None
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("AuthenticatedHTTPClient is closed")

    async def _get_session(self) -> aiohttp.ClientSession:
        self._ensure_open()
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()

        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(timeout=self._timeout)

        return self._session

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
        self._ensure_open()

        token = await self._client.get_access_token()

        headers = dict(kwargs.pop("headers", None) or {})
        headers["Authorization"] = f"Bearer {token}"

        session = await self._get_session()
        return await session.request(method, url, headers=headers, **kwargs)

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
        if self._closed:
            return

        self._closed = True

        if self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> "AuthenticatedHTTPClient":
        self._ensure_open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


def run_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    raise RuntimeError("run_sync() cannot be used inside an async context; use await instead")