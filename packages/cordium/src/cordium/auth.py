"""Credential value objects mapped onto the Octelium SDK's authentication."""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeAlias

from octelium.sdk import (
    AssertionConfig,
    AuthConfig,
    AuthTokenConfig,
    OAuth2ClientCredentialsConfig,
)

from .errors import nonempty

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


@dataclass(frozen=True, slots=True)
class OAuth2ClientCredentials:
    """OAuth2 client credentials of an Octelium WORKLOAD User."""

    client_id: str
    client_secret: str = field(repr=False)
    scopes: tuple[str, ...] = ()


Credentials: TypeAlias = (
    AccessToken | AuthenticationToken | Assertion | AssertionFile | OAuth2ClientCredentials
)
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


def _provider(value: TokenProvider) -> Callable[[], Awaitable[str]]:
    async def provide() -> str:
        token = value()
        if inspect.isawaitable(token):
            token = await token
        return token

    return provide


def octelium_auth(auth: Credentials | AuthConfig) -> AuthConfig:
    """Translate Cordium credentials into the Octelium SDK configuration that implements them."""
    if isinstance(auth, AuthConfig):
        return auth
    if isinstance(auth, AccessToken):
        return AuthConfig(
            type="access_token",
            access_token=auth.token if isinstance(auth.token, str) else _provider(auth.token),
        )
    if isinstance(auth, AuthenticationToken):
        return AuthConfig(
            type="auth_token",
            auth_token=AuthTokenConfig(
                token=nonempty(auth.token, "Authentication token"), scopes=auth.scopes
            ),
        )
    if isinstance(auth, Assertion):
        return AuthConfig(
            type="assertion",
            assertion=AssertionConfig(token=_provider(auth.provider), scopes=auth.scopes),
        )
    if isinstance(auth, AssertionFile):
        path = Path(auth.path)

        async def read() -> str:
            return await asyncio.to_thread(path.read_text, encoding="utf-8")

        return AuthConfig(
            type="assertion", assertion=AssertionConfig(token=read, scopes=auth.scopes)
        )
    if isinstance(auth, OAuth2ClientCredentials):
        return AuthConfig(
            type="oauth2_client_credentials",
            oauth2_client_credentials=OAuth2ClientCredentialsConfig(
                client_id=auth.client_id, client_secret=auth.client_secret, scopes=auth.scopes
            ),
        )
    raise ValueError("Unsupported credentials")
