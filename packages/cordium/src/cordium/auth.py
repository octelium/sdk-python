"""Credential value objects and shared, cancellation-safe session refresh."""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeAlias

from grpclib.const import Status
from grpclib.exceptions import GRPCError
from octelium.api.main.auth import v1 as a

from .errors import CordiumError, nonempty

TokenProvider: TypeAlias = Callable[[], str | Awaitable[str]]
"""Callable returning a current access token/assertion; async providers may perform network I/O."""


@dataclass(frozen=True, slots=True)
class AccessToken:
    """Externally managed token or provider. The provider is consulted for each request."""

    token: str | TokenProvider = field(repr=False)


@dataclass(frozen=True, slots=True)
class AuthenticationToken:
    """One-time Credential token. The resulting session is refreshed, never recreated by replay."""

    token: str = field(repr=False)
    scopes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Assertion:
    """Renewable assertion provider for workload identity federation; scopes are optional."""

    provider: TokenProvider = field(repr=False)
    scopes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AssertionFile:
    """Assertion file reread on authentication, suitable for rotating Kubernetes projected tokens."""

    path: str | Path
    scopes: tuple[str, ...] = ()


Credentials: TypeAlias = AccessToken | AuthenticationToken | Assertion | AssertionFile
"""Supported Cordium/Octelium credential configurations."""


def environment_auth() -> Credentials | None:
    if value := os.getenv("OCTELIUM_ACCESS_TOKEN"):
        return AccessToken(value)
    if value := os.getenv("OCTELIUM_ASSERTION_FILE"):
        return AssertionFile(value)
    if os.getenv("OCTELIUM_ASSERTION"):
        return Assertion(lambda: os.environ["OCTELIUM_ASSERTION"])
    if value := os.getenv("OCTELIUM_AUTH_TOKEN") or os.getenv("OCTELIUM_AUTHENTICATION_TOKEN"):
        return AuthenticationToken(value)
    return None


async def resolve(value: str | TokenProvider) -> str:
    token = value() if callable(value) else value
    if inspect.isawaitable(token):
        token = await token
    return nonempty(token, "Token")


class AuthManager:
    def __init__(self, auth: Credentials, stub: a.MainServiceStub) -> None:
        self.auth = auth
        self.stub = stub
        self.session: a.SessionToken | None = None
        self.expires_at = 0.0
        self.pending: asyncio.Task[str] | None = None
        self.used = False
        self.closed = False

    async def token(self) -> str:
        if self.closed:
            raise CordiumError("Client is closed", "CLIENT_CLOSED")
        if isinstance(self.auth, AccessToken):
            return await resolve(self.auth.token)
        if self.session is not None and time.monotonic() < self.expires_at:
            return self.session.access_token
        if self.pending is None or self.pending.done():
            self.pending = asyncio.create_task(self.refresh(), name="cordium-auth-refresh")
            self.pending.add_done_callback(
                lambda task: None if task.cancelled() else task.exception()
            )
        return await asyncio.shield(self.pending)

    async def refresh(self) -> str:
        async with asyncio.timeout(30):
            if self.session is not None and self.session.refresh_token:
                try:
                    return self.accept(
                        await self.stub.authenticate_with_refresh_token(
                            a.AuthenticateWithRefreshTokenRequest(),
                            timeout=30,
                            metadata={"x-octelium-refresh-token": self.session.refresh_token},
                        )
                    )
                except GRPCError as error:
                    if error.status != Status.UNAUTHENTICATED or isinstance(
                        self.auth, AuthenticationToken
                    ):
                        raise
                    self.session = None
            if isinstance(self.auth, AuthenticationToken):
                if self.used:
                    raise CordiumError(
                        "Authentication token already attempted; supply new credentials",
                        "UNAUTHENTICATED",
                    )
                self.used = True
                session = await self.stub.authenticate_with_authentication_token(
                    a.AuthenticateWithAuthenticationTokenRequest(
                        authentication_token=nonempty(self.auth.token, "Authentication token"),
                        scopes=list(self.auth.scopes),
                    ),
                    timeout=30,
                )
            elif isinstance(self.auth, (Assertion, AssertionFile)):
                assertion = (
                    await resolve(self.auth.provider)
                    if isinstance(self.auth, Assertion)
                    else await asyncio.to_thread(Path(self.auth.path).read_text, encoding="utf-8")
                )
                session = await self.stub.authenticate_with_assertion(
                    a.AuthenticateWithAssertionRequest(
                        assertion=nonempty(assertion.strip(), "Assertion"),
                        scopes=list(self.auth.scopes),
                    ),
                    timeout=30,
                )
            else:
                raise CordiumError("Unsupported credentials", "INVALID_ARGUMENT")
            return self.accept(session)

    def accept(self, session: a.SessionToken) -> str:
        nonempty(session.access_token, "Server access token")
        if session.expires_in <= 0:
            raise CordiumError("Invalid token lifetime from server", "PROTOCOL_ERROR")
        self.session = session
        self.expires_at = time.monotonic() + session.expires_in - min(30, session.expires_in / 10)
        return session.access_token

    async def close(self) -> None:
        self.closed = True
        if self.pending is not None:
            self.pending.cancel()
            await asyncio.gather(self.pending, return_exceptions=True)
