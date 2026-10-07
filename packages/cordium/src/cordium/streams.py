"""Explicitly closable async streams with bounded, backpressured event queues."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from types import TracebackType
from typing import Generic, TypeVar

from ._engine import Engine
from .errors import CordiumError, integer
from .models import timeout_value

T = TypeVar("T")


class AsyncStream(Generic[T]):
    """Single-consumer async iterator with aclose() and async context-manager support.

    Use ``async with stream`` when a loop may stop early: Python's async-for does
    not automatically close an iterator on break. A full queue pauses reading from
    the server until the consumer catches up.
    """

    def __init__(
        self,
        engine: Engine,
        source: Callable[[], AsyncIterator[T]],
        *,
        timeout: float | None = None,
        size: Callable[[T], int] = lambda _: 1,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self._engine, self._source, self._timeout, self._size = (
            engine,
            source,
            timeout_value(timeout),
            size,
        )
        self._max_bytes = integer(max_bytes, "max_bytes", 1)
        self._queue: deque[tuple[T, int]] = deque()
        self._bytes = 0
        self._wake = asyncio.Event()
        self._drained = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._done = False
        self._error: BaseException | None = None
        self._reading = False

    def _start(self) -> None:
        self._engine.check()
        if self._task is None and not self._done:
            self._task = asyncio.create_task(self._pump(), name="cordium-stream")

    async def _pump(self) -> None:
        source: AsyncIterator[T] | None = None
        try:
            async with self._engine.operation(self._timeout):
                source = self._source()
                async for value in source:
                    size = self._size(value)
                    while self._queue and (
                        len(self._queue) >= 1024 or self._bytes + size > self._max_bytes
                    ):
                        self._drained.clear()
                        await self._drained.wait()
                    self._queue.append((value, size))
                    self._bytes += size
                    self._wake.set()
        except BaseException as error:
            self._error = error
            self._queue.clear()
            self._bytes = 0
        finally:
            try:
                if source is not None and hasattr(source, "aclose"):
                    await source.aclose()
            except BaseException as error:
                if self._error is None:
                    self._error = error
            finally:
                self._done = True
                self._wake.set()

    def __aiter__(self) -> AsyncStream[T]:
        """Return this single-consumer stream."""
        self._start()
        return self

    async def __anext__(self) -> T:
        """Wait for the next event; raises StopAsyncIteration when the server finishes."""
        if self._reading:
            raise CordiumError(
                "Concurrent reads from a stream are not supported", "FAILED_PRECONDITION"
            )
        self._start()
        self._reading = True
        try:
            while True:
                if self._error is not None:
                    raise self._error
                if self._queue:
                    value, size = self._queue.popleft()
                    self._bytes -= size
                    self._drained.set()
                    return value
                if self._done:
                    raise StopAsyncIteration
                self._wake.clear()
                await self._wake.wait()
        except asyncio.CancelledError:
            await self.aclose()
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        """Cancel this subscription and release its buffered events; never delete resources."""
        self._done = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._queue.clear()
        self._bytes = 0
        self._wake.set()

    async def __aenter__(self) -> AsyncStream[T]:
        """Start the subscription; __aexit__ cancels it even on early loop exit."""
        self._start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the subscription."""
        await self.aclose()
