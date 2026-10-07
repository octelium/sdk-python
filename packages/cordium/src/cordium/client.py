"""Native async Cordium client with explicit transport ownership."""

from __future__ import annotations

import os
import re
import ssl
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from urllib.parse import urlsplit

import httpx
from grpclib.client import Channel
from octelium.sdk import AuthConfig, OcteliumClient, OcteliumClientConfig

from ._engine import Engine, RawServices
from .auth import Credentials, environment_auth, octelium_auth
from .errors import CordiumError, integer
from .resources import (
    AsyncGitProviders,
    AsyncManagement,
    AsyncMemberships,
    AsyncRegions,
    AsyncSecrets,
    AsyncSnapshots,
    AsyncSpaces,
    AsyncTemplates,
    AsyncUserConfig,
    AsyncUserSecrets,
    AsyncVolumes,
    JsonValue,
)
from .workspaces import AsyncWorkspaces


class AsyncCordium:
    """Native asyncio SDK, bound to its first event loop. Use ``async with``.

    Authentication, session refresh and the gRPC channel are provided by the Octelium
    SDK's OcteliumClient. domain is a bare Cluster domain, e.g. ``example.com``. RPCs
    connect to ``octelium-api.<domain>:443`` unless host/port are supplied. TLS
    verification is enabled; supply ssl_context_factory for custom CAs or mutual TLS
    and tls_server_name to verify a certificate name other than host.

    Omitted domain/auth use CORDIUM_DOMAIN (then OCTELIUM_DOMAIN) and Octelium
    credential environment variables. auth also accepts an octelium.sdk.AuthConfig.
    A supplied octelium client is reused with its credentials and connection, and
    aclose() leaves it open. An injected channel must already implement
    authentication and remains caller-owned. Injected HTTP clients also remain
    caller-owned. No requests or sockets are created by this constructor.
    """

    def __init__(
        self,
        domain: str | None = None,
        *,
        auth: Credentials | AuthConfig | None = None,
        octelium: OcteliumClient | None = None,
        host: str | None = None,
        port: int = 443,
        ssl_context_factory: Callable[[], ssl.SSLContext] | None = None,
        insecure_tls: bool = False,
        tls_server_name: str = "",
        channel: Channel | None = None,
        http_client: httpx.AsyncClient | None = None,
        authorized_http_hosts: Sequence[str] = (),
        allow_insecure_http: bool = False,
    ) -> None:
        if channel is not None and octelium is not None:
            raise ValueError("Supply either an injected channel or an Octelium client")
        if (channel is not None or octelium is not None) and (
            auth is not None
            or host is not None
            or port != 443
            or ssl_context_factory is not None
            or insecure_tls
            or tls_server_name
        ):
            raise ValueError(
                "An injected channel or Octelium client owns authentication and the connection"
            )
        if octelium is not None:
            domain = domain or octelium.domain
        domain = domain or os.getenv("CORDIUM_DOMAIN") or os.getenv("OCTELIUM_DOMAIN")
        if domain is None:
            if channel is None:
                raise ValueError("domain or CORDIUM_DOMAIN/OCTELIUM_DOMAIN is required")
            domain = ""
        domain = domain.lower().rstrip(".")
        if domain and (
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", domain) or ".." in domain
        ):
            raise ValueError("domain must be a bare hostname, without scheme, port or path")
        if octelium is not None and domain != octelium.domain:
            raise ValueError("domain must match the domain of the Octelium client")
        if channel is not None or octelium is not None:
            self._engine = Engine(channel=channel, octelium=octelium)
        else:
            credentials = auth or environment_auth()
            if credentials is None:
                raise ValueError("Provide auth or set an OCTELIUM credential environment variable")
            config = OcteliumClientConfig(
                domain=domain,
                auth=octelium_auth(credentials),
                api_host=host or "",
                api_port=integer(port, "port", 1, 65535),
                ssl_context_factory=ssl_context_factory,
                insecure_tls=insecure_tls,
                tls_server_name=tls_server_name,
            )
            self._engine = Engine(connect=lambda: OcteliumClient(config))
        self._domain = domain
        self._http = http_client
        self._owns_http = http_client is None
        self._http_hosts = frozenset(self._normalize_host(host) for host in authorized_http_hosts)
        self._insecure_http = allow_insecure_http

    @staticmethod
    def _normalize_host(host: str) -> str:
        value = host.lower().rstrip(".")
        if not re.fullmatch(r"[a-z0-9_](?:[a-z0-9_.-]*[a-z0-9_])?", value) or ".." in value:
            raise ValueError("Authorized HTTP hosts must be bare hostnames")
        return value

    @property
    def domain(self) -> str:
        """Normalized Cluster domain used for application HTTP authorization."""
        return self._domain

    @property
    def raw(self) -> RawServices:
        """Authenticated generated async stubs; caller owns deadlines and stream cleanup."""
        return self._engine.raw

    @property
    def octelium(self) -> OcteliumClient | None:
        """The Octelium client that authenticates this client, or None with an injected channel."""
        _ = self._engine.raw
        return self._engine.octelium

    @property
    def workspaces(self) -> AsyncWorkspaces:
        """Workspace creation, listing, lifecycle and event subscriptions."""
        return AsyncWorkspaces(self._engine)

    @property
    def spaces(self) -> AsyncSpaces:
        """Personal and organization Spaces."""
        return AsyncSpaces(self._engine)

    @property
    def templates(self) -> AsyncTemplates:
        """Reusable configurations and asynchronous image builds."""
        return AsyncTemplates(self._engine)

    @property
    def snapshots(self) -> AsyncSnapshots:
        """Workspace storage snapshots and readiness waits."""
        return AsyncSnapshots(self._engine)

    @property
    def volumes(self) -> AsyncVolumes:
        """Persistent attachable volumes and monotonic size growth."""
        return AsyncVolumes(self._engine)

    @property
    def secrets(self) -> AsyncSecrets:
        """Space-scoped write-only Secrets."""
        return AsyncSecrets(self._engine)

    @property
    def user_secrets(self) -> AsyncUserSecrets:
        """Caller-owned Secrets, including Cluster-generated SSH key pairs."""
        return AsyncUserSecrets(self._engine)

    @property
    def git_providers(self) -> AsyncGitProviders:
        """Space OAuth providers and advanced generated configurations."""
        return AsyncGitProviders(self._engine)

    @property
    def memberships(self) -> AsyncMemberships:
        """Space membership invitations and roles."""
        return AsyncMemberships(self._engine)

    @property
    def regions(self) -> AsyncRegions:
        """Available workspace placement regions."""
        return AsyncRegions(self._engine)

    @property
    def user_config(self) -> AsyncUserConfig:
        """The authenticated user's Cordium configuration."""
        return AsyncUserConfig(self._engine)

    @property
    def management(self) -> AsyncManagement:
        """Cluster administration; server-side permissions apply."""
        return AsyncManagement(self._engine)

    async def access_token(self, *, timeout: float | None = 30) -> str:
        """Obtain a current token, refreshing a managed session if necessary. Treat as a secret."""
        return await self._engine.token(timeout)

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        content: str | bytes | None = None,
        json: JsonValue = None,
        timeout: float | None = 30,
        max_response_bytes: int = 16 * 1024 * 1024,
    ) -> httpx.Response:
        """Call a workspace application with current Octelium authentication.

        Only HTTPS Cluster-domain/subdomain destinations and explicitly authorized
        hosts are allowed. Credentials travel in the x-octelium-auth header, leaving
        the application's own Authorization header alone. Redirects are returned
        without following. The response
        body is buffered up to max_response_bytes; non-2xx statuses are returned,
        so call response.raise_for_status() if desired. timeout covers authentication
        and the full body. HTTP transport failures use code UNAVAILABLE.
        """
        target = httpx.URL(url)
        if target.username or target.password or "@" in urlsplit(url).netloc:
            raise ValueError("URL userinfo is forbidden")
        if target.scheme != "https" and not (target.scheme == "http" and self._insecure_http):
            raise ValueError("HTTPS is required")
        host = self._normalize_host(target.host)
        if (
            not (self.domain and (host == self.domain or host.endswith("." + self.domain)))
            and host not in self._http_hosts
        ):
            raise ValueError("HTTP destination is not authorized")
        integer(max_response_bytes, "max_response_bytes", 0, 2**63 - 1)
        async with self._engine.operation(timeout):
            token = await self.access_token(timeout=None)
            if self._http is None:
                self._http = httpx.AsyncClient(trust_env=False)
            request_headers = httpx.Headers(headers)
            request_headers["x-octelium-auth"] = token
            # Reject a different virtual host even when a caller-owned client has defaults.
            request_headers["host"] = target.netloc.decode()
            try:
                async with self._http.stream(
                    method,
                    target,
                    headers=request_headers,
                    content=content,
                    json=json,
                    auth=None,
                    follow_redirects=False,
                    timeout=None,
                ) as response:
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(data) + len(chunk) > max_response_bytes:
                            raise CordiumError(
                                "HTTP response exceeds max_response_bytes", "RESOURCE_EXHAUSTED"
                            )
                        data.extend(chunk)
                    # Preserve response metadata and decoded body without retaining a live stream.
                    response._content = bytes(data)
                    return response
            except httpx.HTTPError as error:
                raise CordiumError(str(error), "UNAVAILABLE") from error

    async def aclose(self) -> None:
        """Cancel SDK operations and close the owned Octelium client; never stop or delete resources."""
        try:
            await self._engine.close()
        finally:
            if self._owns_http and self._http is not None:
                await self._http.aclose()

    async def __aenter__(self) -> AsyncCordium:
        """Bind to this event loop and return the client."""
        self._engine.check()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close owned transports and outstanding SDK operations."""
        await self.aclose()
