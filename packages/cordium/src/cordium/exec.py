"""Binary-safe command execution. Cordium's protocol has no standalone stdin EOF."""

from __future__ import annotations

import asyncio
import shlex
from collections import deque
from collections.abc import Mapping
from types import TracebackType

import betterproto
from grpclib.client import Stream
from grpclib.const import Cardinality
from octelium.api.main.cordium import v1 as p

from ._engine import Engine
from .errors import CordiumError, ExecError, integer, nonempty
from .models import ExecOutput, ExecResult, Reference, reference


class AsyncExecSession:
    """Running command with ordered async output, write(), kill(), wait(), and aclose().

    Use an async context manager to cancel on early iteration exit. wait() drains
    output if iteration has not started. Captures and streaming queues have separate limits.
    """

    def __init__(
        self,
        engine: Engine,
        workspace: Reference,
        command: str,
        *,
        cwd: str = "",
        env: Mapping[str, str] | None = None,
        root: bool = False,
        stdin: str | bytes | None = None,
        interactive: bool = True,
        check: bool = False,
        timeout: float | None = None,
        max_capture_bytes: int = 1024 * 1024,
        max_buffer_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.command = nonempty(command, "Command")
        self._initial = p.ExecRequest(
            request=p.ExecRequestRequest(
                workspace_ref=reference(workspace),
                command=command,
                working_dir=cwd,
                env_vars=[
                    p.ExecRequestRequestEnvVar(key=k, value=v) for k, v in (env or {}).items()
                ],
                run_as_root=root,
                has_stdin=interactive or stdin is not None,
            )
        )
        self._engine, self._timeout, self._check = engine, timeout, check
        self._capture = integer(max_capture_bytes, "max_capture_bytes")
        self._buffer = integer(max_buffer_bytes, "max_buffer_bytes", 1)
        self._stdin, self._interactive = stdin, interactive
        self._stream: Stream[p.ExecRequest, p.ExecResponse] | None = None
        self._ready = asyncio.Event()
        self._wake = asyncio.Event()
        self._writes = asyncio.Lock()
        self._queue: deque[ExecOutput] = deque()
        self._queued = 0
        self._discard = False
        self._iterating = False
        self._reading = False
        self._killed = False
        self._engine.check()
        self._task = asyncio.create_task(self._run(), name="cordium-exec")
        self._task.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    async def _run(self) -> ExecResult:
        out, err = bytearray(), bytearray()
        truncated = False
        sender: asyncio.Task[None] | None = None
        try:
            async with self._engine.operation(self._timeout):
                _ = self._engine.raw
                assert self._engine.channel is not None
                async with self._engine.channel.request(
                    "/octelium.api.main.cordium.v1.WorkspaceService/Exec",
                    Cardinality.STREAM_STREAM,
                    p.ExecRequest,
                    p.ExecResponse,
                ) as stream:
                    self._stream = stream
                    await stream.send_message(self._initial)
                    self._ready.set()
                    if self._stdin is not None:
                        sender = asyncio.create_task(
                            self.write(self._stdin), name="cordium-exec-stdin"
                        )
                        sender.add_done_callback(
                            lambda task: (
                                self._task.cancel()
                                if not task.cancelled() and task.exception() is not None
                                else None
                            )
                        )
                    async for message in stream:
                        kind, value = betterproto.which_one_of(message, "type")
                        if kind == "exit":
                            result = ExecResult(
                                message.exit.code, bytes(out), bytes(err), truncated, self._killed
                            )
                            # The server waits for client cancellation even after reporting exit.
                            await stream.cancel()
                            if self._check and not result.success:
                                raise ExecError(result)
                            return result
                        if kind not in ("stdout", "stderr"):
                            continue
                        data = message.stdout.data if kind == "stdout" else message.stderr.data
                        target = out if kind == "stdout" else err
                        count = min(len(data), max(0, self._capture - len(target)))
                        target.extend(data[:count])
                        truncated |= count < len(data)
                        if not self._discard:
                            if self._queued + len(data) > self._buffer or len(self._queue) >= 4096:
                                raise CordiumError(
                                    "Command output exceeded the stream buffer",
                                    "RESOURCE_EXHAUSTED",
                                )
                            self._queue.append(
                                ExecOutput("stdout" if kind == "stdout" else "stderr", data)
                            )
                            self._queued += len(data)
                            self._wake.set()
                    raise CordiumError(
                        "Command stream ended without an exit status", "PROTOCOL_ERROR"
                    )
        except asyncio.CancelledError:
            if (
                sender is not None
                and sender.done()
                and not sender.cancelled()
                and sender.exception() is not None
            ):
                raise (sender.exception() or RuntimeError("stdin write failed")) from None
            raise
        finally:
            if sender is not None:
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)
            self._ready.set()
            self._wake.set()

    async def ready(self) -> AsyncExecSession:
        """Wait until the initial command request has been sent."""
        try:
            await self._ready.wait()
            if self._task.done():
                await asyncio.shield(self._task)
            return self
        except BaseException:
            await self.aclose()
            raise

    async def write(self, data: str | bytes) -> None:
        """Write stdin with serialized 32-KiB messages. This never sends EOF."""
        if not self._interactive and self._stdin is None:
            raise CordiumError("stdin is disabled", "FAILED_PRECONDITION")
        await self._ready.wait()
        if self._task.done():
            raise CordiumError("Command has finished", "FAILED_PRECONDITION")
        assert self._stream is not None
        payload = data.encode() if isinstance(data, str) else bytes(data)
        async with self._writes:
            async with self._engine.operation(None):
                for offset in range(0, len(payload), 32 * 1024):
                    await self._stream.send_message(
                        p.ExecRequest(
                            write_data=p.ExecRequestWriteData(
                                data=payload[offset : offset + 32 * 1024]
                            )
                        )
                    )

    async def kill(self) -> None:
        """Terminate the remote process group; the server normally reports exit code -1."""
        await self._ready.wait()
        if self._task.done():
            return
        assert self._stream is not None
        async with self._writes:
            async with self._engine.operation(None):
                self._killed = True
                await self._stream.send_message(p.ExecRequest(kill=p.ExecRequestKill()))

    async def wait(self) -> ExecResult:
        """Await completion. Before iteration starts, selects drain-only consumption."""
        if not self._iterating:
            self._discard = True
            self._queue.clear()
            self._queued = 0
        try:
            return await asyncio.shield(self._task)
        except asyncio.CancelledError:
            await self.aclose()
            raise

    def __aiter__(self) -> AsyncExecSession:
        """Consume ordered stdout/stderr chunks once; use a context manager for early exit."""
        if self._discard:
            raise CordiumError("wait() selected drain-only consumption", "FAILED_PRECONDITION")
        self._iterating = True
        return self

    async def __anext__(self) -> ExecOutput:
        """Receive the next output chunk; raise stream failures without hiding them."""
        self.__aiter__()
        if self._reading:
            raise CordiumError("Concurrent output reads are not supported", "FAILED_PRECONDITION")
        self._reading = True
        try:
            while True:
                if self._task.done() and (
                    self._task.cancelled() or self._task.exception() is not None
                ):
                    await self._task
                if self._queue:
                    result = self._queue.popleft()
                    self._queued -= len(result.data)
                    return result
                if self._task.done():
                    raise StopAsyncIteration
                self._wake.clear()
                await self._wake.wait()
        except asyncio.CancelledError:
            await self.aclose()
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        """Cancel execution and release local buffers. Does not delete the workspace."""
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._queue.clear()
        self._queued = 0

    async def __aenter__(self) -> AsyncExecSession:
        """Return the session; cancel it automatically on context exit."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the command stream."""
        await self.aclose()


def shell_quote(value: str) -> str:
    """Quote one literal POSIX shell argument; reject NUL bytes."""
    if "\0" in value:
        raise ValueError("Shell arguments cannot contain NUL bytes")
    return shlex.quote(value)


def argv(*args: str) -> str:
    """Build a remote shell command from literal arguments, without interpreting metacharacters."""
    if not args:
        raise ValueError("At least one argument is required")
    return " ".join(shell_quote(arg) for arg in args)
