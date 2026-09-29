"""Shared transport ownership, deadline, and error normalization."""

from __future__ import annotations

import asyncio
import ssl
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypeVar

from grpclib.client import Channel
from grpclib.events import SendRequest, listen
from grpclib.exceptions import GRPCError, StreamTerminatedError
from octelium.api.main.auth import v1 as a
from octelium.api.main.cordium import v1 as p

from .auth import AuthManager, Credentials
from .errors import CordiumError
from .models import timeout_value

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RawServices:
    """Generated async service stubs. Raw callers own deadlines, cancellation, and consumption."""

    main: p.MainServiceStub
    workspace: p.WorkspaceServiceStub
    management: p.ManagementServiceStub


class Engine:
    def __init__(
        self,
        host: str,
        port: int,
        tls: ssl.SSLContext | bool,
        auth: Credentials | None,
        channel: Channel | None,
    ) -> None:
        self.host, self.port, self.tls, self.credentials = host, port, tls, auth
        self.channel = channel
        self.owns_channel = channel is None
        self.auth_channel: Channel | None = None
        self.auth: AuthManager | None = None
        self._raw: RawServices | None = None
        self.closed = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self.tasks: Counter[asyncio.Task[object]] = Counter()

    def check(self) -> None:
        if self.closed:
            raise CordiumError("Cordium client is closed", "CLIENT_CLOSED")
        loop = asyncio.get_running_loop()
        if self.loop is not None and self.loop is not loop:
            raise CordiumError(
                "AsyncCordium must be used on the same event loop", "FAILED_PRECONDITION"
            )
        self.loop = loop

    @property
    def raw(self) -> RawServices:
        self.check()
        if self._raw is None:
            if self.channel is None:
                self.channel = Channel(self.host, self.port, ssl=self.tls)
                self.auth_channel = Channel(self.host, self.port, ssl=self.tls)
                if self.credentials is not None:
                    self.auth = AuthManager(self.credentials, a.MainServiceStub(self.auth_channel))

                    async def authenticate(event: SendRequest) -> None:
                        assert self.auth is not None
                        self.check()
                        event.metadata["authorization"] = f"Bearer {await self.auth.token()}"

                    listen(self.channel, SendRequest, authenticate)
            self._raw = RawServices(
                p.MainServiceStub(self.channel),
                p.WorkspaceServiceStub(self.channel),
                p.ManagementServiceStub(self.channel),
            )
        return self._raw

    @asynccontextmanager
    async def operation(self, timeout: float | None) -> AsyncIterator[None]:
        self.check()
        timeout_value(timeout)
        task = asyncio.current_task()
        assert task is not None
        self.tasks[task] += 1
        try:
            async with asyncio.timeout(timeout):
                yield
        except asyncio.CancelledError:
            if self.closed:
                raise CordiumError("Cordium client is closed", "CLIENT_CLOSED") from None
            raise
        except TimeoutError as error:
            raise CordiumError("Operation deadline exceeded", "DEADLINE_EXCEEDED") from error
        except GRPCError as error:
            raise CordiumError(
                error.message or error.status.name, error.status.name, details=error.details
            ) from error
        except (StreamTerminatedError, OSError) as error:
            if self.closed:
                raise CordiumError("Cordium client is closed", "CLIENT_CLOSED") from error
            raise CordiumError(str(error), "UNAVAILABLE") from error
        finally:
            self.tasks[task] -= 1
            if not self.tasks[task]:
                del self.tasks[task]

    async def call(self, factory: Callable[[], Awaitable[T]], timeout: float | None = 30) -> T:
        async with self.operation(timeout):
            return await factory()

    async def token(self, timeout: float | None = 30) -> str:
        async with self.operation(timeout):
            _ = self.raw
            if self.auth is None:
                raise CordiumError(
                    "The injected channel owns its credentials", "FAILED_PRECONDITION"
                )
            return await self.auth.token()

    async def close(self) -> None:
        if self.closed:
            return
        if self.loop is not None:
            self.check()
        self.closed = True
        current = asyncio.current_task()
        tasks = [task for task in self.tasks if task is not current]
        for task in tasks:
            task.cancel()
        try:
            if self.auth is not None:
                await self.auth.close()
        finally:
            if self.owns_channel and self.channel is not None:
                self.channel.close()
            if self.auth_channel is not None:
                self.auth_channel.close()
            await asyncio.gather(*tasks, return_exceptions=True)
