"""Persistent PTY shells with explicit detach and remove semantics."""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import TracebackType

import betterproto
from octelium.api.main.cordium import v1 as p

from ._engine import Engine
from .errors import CordiumError, integer, nonempty
from .models import Reference, TerminalEvent, reference
from .streams import AsyncStream


class AsyncTerminals:
    """Create, list and attach to persistent terminals in one workspace."""

    def __init__(self, engine: Engine, ref: Reference) -> None:
        self._engine, self._ref = engine, ref

    async def create(
        self, *, cols: int = 80, rows: int = 24, timeout: float | None = 30
    ) -> AsyncTerminal:
        """Create a PTY shell; its event subscription starts lazily on iteration."""
        request = p.CreateTerminalRequest(
            workspace_ref=reference(self._ref),
            cols=integer(cols, "cols", 1),
            rows=integer(rows, "rows", 1),
        )
        result = await self._engine.call(
            lambda: self._engine.raw.workspace.create_terminal(request), timeout
        )
        return self.attach(result.id)

    async def list(self, *, timeout: float | None = 30) -> tuple[str, ...]:
        """Return IDs of this workspace's open terminals."""
        result = await self._engine.call(
            lambda: self._engine.raw.workspace.list_terminal(
                p.ListTerminalRequest(workspace_ref=reference(self._ref))
            ),
            timeout,
        )
        return tuple(item.id for item in result.items)

    def attach(
        self, id: str, *, timeout: float | None = None, max_buffer_bytes: int = 8 * 1024 * 1024
    ) -> AsyncTerminal:
        """Create a handle to an existing terminal; timeout bounds its event subscription."""
        return AsyncTerminal(self._engine, nonempty(id, "Terminal ID"), timeout, max_buffer_bytes)

    async def remove(self, id: str, *, timeout: float | None = 30) -> None:
        """Terminate the remote shell identified by id."""
        await self._engine.call(
            lambda: self._engine.raw.workspace.remove_terminal(
                p.RemoveTerminalRequest(id=nonempty(id, "Terminal ID"))
            ),
            timeout,
        )


class AsyncTerminal:
    """Persistent terminal. Context exit detaches; remove() terminates the remote shell."""

    def __init__(
        self, engine: Engine, id: str, timeout: float | None, max_buffer_bytes: int
    ) -> None:
        self._engine, self._id, self._closed = engine, id, False
        self._events = AsyncStream(
            engine,
            self._source,
            timeout=timeout,
            size=lambda item: len(item.data),
            max_bytes=max_buffer_bytes,
        )

    @property
    def id(self) -> str:
        """Terminal identifier assigned by the server."""
        return self._id

    @property
    def events(self) -> AsyncStream[TerminalEvent]:
        """Single-consumer stream of binary output, resize and close events."""
        return self._events

    async def _source(self) -> AsyncIterator[TerminalEvent]:
        stream = self._engine.raw.workspace.listen_terminal(p.ListenTerminalRequest(id=self.id))
        try:
            async for message in stream:
                kind, _ = betterproto.which_one_of(message, "type")
                if kind == "stdout":
                    yield TerminalEvent("output", data=message.stdout.data)
                elif kind == "window_size":
                    yield TerminalEvent(
                        "resize", cols=message.window_size.cols, rows=message.window_size.rows
                    )
                elif kind == "close":
                    self._closed = True
                    yield TerminalEvent("close")
                    return
        finally:
            if hasattr(stream, "aclose"):
                await stream.aclose()

    def _check(self) -> None:
        if self._closed:
            raise CordiumError("Terminal handle is detached or closed", "TERMINAL_CLOSED")

    async def write(self, data: str | bytes, *, timeout: float | None = 30) -> None:
        """Write UTF-8 text or bytes to the PTY input."""
        self._check()
        raw = data.encode() if isinstance(data, str) else data
        async with self._engine.operation(timeout):
            for offset in range(0, len(raw), 32768):
                await self._engine.raw.workspace.write_terminal_data(
                    p.WriteTerminalDataRequest(id=self.id, data=raw[offset : offset + 32768])
                )

    async def resize(self, cols: int, rows: int, *, timeout: float | None = 30) -> None:
        """Change window dimensions; both dimensions must be positive integers."""
        self._check()
        request = p.SetTerminalWindowSizeRequest(
            id=self.id, cols=integer(cols, "cols", 1), rows=integer(rows, "rows", 1)
        )
        await self._engine.call(
            lambda: self._engine.raw.workspace.set_terminal_window_size(request), timeout
        )

    async def remove(self, *, timeout: float | None = 30) -> None:
        """Terminate the remote shell, then detach locally even if the request fails."""
        try:
            await self._engine.call(
                lambda: self._engine.raw.workspace.remove_terminal(
                    p.RemoveTerminalRequest(id=self.id)
                ),
                timeout,
            )
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        """Detach this listener, preserving the remote shell for later attachment."""
        self._closed = True
        await self._events.aclose()

    async def __aenter__(self) -> AsyncTerminal:
        """Start listening and return this handle."""
        self._check()
        await self._events.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Detach without terminating the remote shell."""
        await self.aclose()
