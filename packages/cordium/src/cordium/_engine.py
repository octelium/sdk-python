"""Shared transport ownership, deadline, and error normalization."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypeVar

from grpclib.client import Channel
from grpclib.exceptions import GRPCError, StreamTerminatedError
from octelium.api.main.cordium import v1 as p
from octelium.sdk import AuthenticationError, OcteliumClient

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
        *,
        channel: Channel | None = None,
        octelium: OcteliumClient | None = None,
        connect: Callable[[], OcteliumClient] | None = None,
    ) -> None:
        self.channel = channel
        self.octelium = octelium
        self.connect = connect
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
                if self.octelium is None:
                    assert self.connect is not None
                    self.octelium = self.connect()
                self.channel = self.octelium.channel
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
        except AuthenticationError as error:
            raise CordiumError(str(error), "UNAUTHENTICATED") from error
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
            if self.octelium is None:
                raise CordiumError(
                    "The injected channel owns its credentials", "FAILED_PRECONDITION"
                )
            return await self.octelium.get_access_token()

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
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            if self.connect is not None and self.octelium is not None:
                await self.octelium.close()
