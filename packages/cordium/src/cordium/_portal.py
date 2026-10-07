"""One owned event-loop thread for the blocking facade."""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import os
import threading
from collections.abc import Awaitable, Callable
from typing import TypeVar, overload

from .errors import CordiumError

T = TypeVar("T")


class Portal:
    def __init__(self) -> None:
        self._pid = os.getpid()
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(target=self._run, name="cordium-event-loop", daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        with asyncio.Runner() as runner:
            self._loop = runner.get_loop()
            self._ready.set()
            self._loop.run_forever()

    def _check(self) -> None:
        if os.getpid() != self._pid:
            raise CordiumError("Create a new Cordium client after fork", "FAILED_PRECONDITION")
        if threading.current_thread() is self._thread:
            raise CordiumError(
                "A blocking client cannot be called from its own callbacks", "FAILED_PRECONDITION"
            )

    @overload
    def invoke(self, operation: Callable[[], Awaitable[T]]) -> T: ...

    @overload
    def invoke(self, operation: Callable[[], T]) -> T: ...

    def invoke(self, operation: Callable[[], T | Awaitable[T]]) -> T:
        self._check()

        async def run() -> T:
            value = operation()
            if inspect.isawaitable(value):
                return await value
            return value

        with self._lock:
            if self._closed:
                raise CordiumError("Cordium client is closed", "CLIENT_CLOSED")
            future = asyncio.run_coroutine_threadsafe(run(), self._loop)
        try:
            return future.result()
        except concurrent.futures.CancelledError as error:
            raise CordiumError("Operation cancelled", "CANCELLED") from error
        except BaseException:
            future.cancel()
            raise

    def release(self, operation: Callable[[], Awaitable[None]]) -> None:
        # The parent has already cancelled operations and shut down the loop.
        # Resource contexts may still unwind afterwards; cleanup is idempotent.
        try:
            self.invoke(operation)
        except CordiumError as error:
            if error.code != "CLIENT_CLOSED":
                raise

    def close(self, cleanup: Callable[[], Awaitable[None]]) -> None:
        self._check()

        async def run_cleanup() -> None:
            await cleanup()
            await asyncio.sleep(0.25)

        with self._lock:
            if self._closed:
                return
            self._closed = True
            future = asyncio.run_coroutine_threadsafe(run_cleanup(), self._loop)
        try:
            future.result()
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
