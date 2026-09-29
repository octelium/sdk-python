"""Binary-safe POSIX file helpers over framed exec streams."""

from __future__ import annotations

import asyncio
import base64
import os
import tempfile
from collections.abc import AsyncIterator, Callable
from functools import partial
from pathlib import Path
from typing import TypeVar

from ._engine import Engine
from .errors import CordiumError, integer, nonempty
from .exec import AsyncExecSession, shell_quote
from .models import Reference

T = TypeVar("T")


async def _disk(operation: Callable[[], T]) -> T:
    # Wait for an in-flight disk operation before closing/replacing its file.
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _path(path: str) -> str:
    nonempty(path, "Remote path")
    return shell_quote(path if path.startswith("/") else "./" + path)


class AsyncFiles:
    """File operations requiring POSIX sh, cat, head, base64, mkdir and rm remotely.

    Transfers use fixed-length base64 input because exec cannot signal stdin EOF.
    Local downloads are atomic; remote writes may leave a partial file on failure.
    Paths are literal and never expanded by the shell (including ~ and $HOME).
    """

    def __init__(self, engine: Engine, ref: Reference) -> None:
        self._engine, self._ref = engine, ref

    async def _session(
        self,
        command: str,
        *,
        root: bool,
        timeout: float | None,
        capture: int = 65536,
        interactive: bool = False,
    ) -> AsyncExecSession:
        self._engine.check()
        session = AsyncExecSession(
            self._engine,
            self._ref,
            command,
            root=root,
            check=True,
            interactive=interactive,
            timeout=timeout,
            max_capture_bytes=capture,
        )
        return await session.ready()

    async def read_bytes(
        self,
        path: str,
        *,
        max_bytes: int = 16 * 1024 * 1024,
        root: bool = False,
        timeout: float | None = 30,
    ) -> bytes:
        """Read a binary file, raising RESOURCE_EXHAUSTED instead of silently truncating."""
        integer(max_bytes, "max_bytes", 0, 2**31 - 2)
        session = await self._session(
            f"head -c {max_bytes + 1} < {_path(path)}",
            root=root,
            timeout=timeout,
            capture=max_bytes + 1,
        )
        async with session:
            result = await session.wait()
        if result.truncated or len(result.stdout_bytes) > max_bytes:
            raise CordiumError("File exceeds max_bytes", "RESOURCE_EXHAUSTED")
        return result.stdout_bytes

    async def read_text(
        self,
        path: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        max_bytes: int = 16 * 1024 * 1024,
        root: bool = False,
        timeout: float | None = 30,
    ) -> str:
        """Read and decode a bounded file; invalid text raises UnicodeDecodeError by default."""
        return (
            await self.read_bytes(path, max_bytes=max_bytes, root=root, timeout=timeout)
        ).decode(encoding, errors)

    async def _upload(
        self, path: str, source: AsyncIterator[bytes], size: int, root: bool, timeout: float | None
    ) -> None:
        integer(size, "size", 0, 2**63 - 1)
        encoded_size = 4 * ((size + 2) // 3)
        # Chunks are multiples of three bytes so concatenated encodings have no internal padding.
        command = f"head -c {encoded_size} | base64 -d > {_path(path)}"
        async with self._engine.operation(timeout):
            session = await self._session(command, root=root, timeout=None, interactive=True)
            async with session:
                sent = 0
                remainder = b""
                async for chunk in source:
                    sent += len(chunk)
                    if sent > size:
                        raise ValueError("Source grew during upload")
                    data = remainder + chunk
                    count = len(data) - len(data) % 3
                    if count:
                        await session.write(base64.b64encode(data[:count]))
                    remainder = data[count:]
                if sent != size:
                    raise ValueError("Source shrank during upload")
                if remainder:
                    await session.write(base64.b64encode(remainder))
                await session.wait()

    async def write_bytes(
        self, path: str, data: bytes, *, root: bool = False, timeout: float | None = 30
    ) -> None:
        """Create or truncate a remote file with exact bytes; parent directories must exist."""

        async def chunks() -> AsyncIterator[bytes]:
            for offset in range(0, len(data), 24576):
                yield data[offset : offset + 24576]

        await self._upload(path, chunks(), len(data), root, timeout)

    async def write_text(
        self,
        path: str,
        text: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        root: bool = False,
        timeout: float | None = 30,
    ) -> None:
        """Encode text and create or truncate a remote file."""
        await self.write_bytes(path, text.encode(encoding, errors), root=root, timeout=timeout)

    async def upload(
        self,
        local_path: str | Path,
        remote_path: str,
        *,
        root: bool = False,
        timeout: float | None = 300,
    ) -> None:
        """Stream a local file with bounded memory. Mutation of its size aborts the transfer."""
        # Opening is deliberately synchronous: cancelling a threaded open can orphan its fd.
        with Path(local_path).open("rb") as source:
            size = os.fstat(source.fileno()).st_size

            async def chunks() -> AsyncIterator[bytes]:
                while data := await _disk(lambda: source.read(24576)):
                    yield data

            await self._upload(remote_path, chunks(), size, root, timeout)

    async def download(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        root: bool = False,
        timeout: float | None = 300,
    ) -> Path:
        """Stream into a private temporary file and atomically replace the local destination.

        A failed transfer preserves an existing destination. Parent directories must
        exist. The new file has mode 0600 on POSIX; remote permissions are not copied.
        """
        destination = Path(local_path)
        temporary: str | None = None
        try:
            async with self._engine.operation(timeout):
                with tempfile.NamedTemporaryFile(
                    dir=destination.parent, prefix=".cordium-", delete=False
                ) as target:
                    temporary = target.name
                    session = await self._session(
                        f"cat < {_path(remote_path)}", root=root, timeout=None
                    )
                    async with session:
                        async for event in session:
                            if event.stream == "stdout":
                                await _disk(partial(target.write, event.data))
                        await session.wait()
                    await _disk(target.flush)
                os.replace(temporary, destination)
                temporary = None
            return destination
        finally:
            if temporary is not None:
                os.unlink(temporary)

    async def mkdir(
        self, path: str, *, parents: bool = True, root: bool = False, timeout: float | None = 30
    ) -> None:
        """Create a directory, optionally creating its parents."""
        session = await self._session(
            f"mkdir {'-p ' if parents else ''}{_path(path)}", root=root, timeout=timeout
        )
        async with session:
            await session.wait()

    async def remove(
        self,
        path: str,
        *,
        recursive: bool = False,
        missing_ok: bool = False,
        root: bool = False,
        timeout: float | None = 30,
    ) -> None:
        """Remove a literal file/path. Directory recursion must be requested explicitly."""
        session = await self._session(
            f"rm {'-r ' if recursive else ''}{'-f ' if missing_ok else ''}{_path(path)}",
            root=root,
            timeout=timeout,
        )
        async with session:
            await session.wait()
