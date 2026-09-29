"""Local wire-level fixtures; no credentials or live Cluster needed."""

from __future__ import annotations

import asyncio
import copy
import os
import signal
from contextlib import suppress
from types import SimpleNamespace

import betterproto
import pytest
from cordium import AccessToken, AsyncCordium, State
from grpclib.const import Cardinality, Handler, Status
from grpclib.events import RecvRequest, listen
from grpclib.exceptions import GRPCError
from grpclib.server import Server
from octelium.api.main.auth import v1 as a
from octelium.api.main.cordium import v1 as p
from octelium.api.main.meta import v1 as m


class Main(p.MainServiceBase):
    def __init__(self):
        self.value = p.Workspace(
            metadata=m.Metadata(name="abc", uid="immutable"),
            status=p.WorkspaceStatus(state=State.STOPPED),
        )
        self.requests = []
        self.pages = []
        self.delay = 0
        self.error = None
        self.watch_closed = asyncio.Event()

    async def create_workspace(self, request):
        self.requests.append(request)
        self.value.spec = copy.deepcopy(request.spec)
        self.value.status.template_ref = request.status.template_ref
        self.value.status.workspace_snapshot_ref = request.status.workspace_snapshot_ref
        return self.value

    async def get_workspace(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.value

    async def start_workspace(self, request):
        self.requests.append(request)
        self.value.status.state = State.RUNNING
        self.value.status.hostname = "abc.cordium.example.test"
        return p.StartWorkspaceResponse()

    async def stop_workspace(self, request):
        self.requests.append(request)
        self.value.status.state = State.STOPPED
        return p.StopWorkspaceResponse()

    async def delete_workspace(self, request):
        self.requests.append(request)
        return m.OperationResult()

    async def update_workspace(self, request):
        self.requests.append(request)
        self.value = request
        return request

    async def list_workspace(self, request):
        self.requests.append(request)
        page = request.common.page
        self.pages.append(page)
        return p.WorkspaceList(
            items=[self.value],
            list_response_meta=m.ListResponseMeta(
                page=page, items_per_page=1, total_count=2, has_more=page < 1
            ),
        )

    async def watch_workspace(self, request):
        self.requests.append(request)
        try:
            yield p.WatchWorkspaceResponse(create=p.WatchWorkspaceResponseCreate(item=self.value))
            await asyncio.Event().wait()
        finally:
            self.watch_closed.set()

    async def share_workspace_port(self, request):
        self.requests.append(request)
        return p.ShareWorkspacePortResponse()

    async def unshare_workspace_port(self, request):
        self.requests.append(request)
        return p.UnshareWorkspacePortResponse()


class Runtime(p.WorkspaceServiceBase):
    def __init__(self):
        self.requests = []
        self.active = 0
        self.closed = asyncio.Event()
        self.terminal_closed = asyncio.Event()
        self.removed = []

    def __mapping__(self):
        mapping = super().__mapping__()
        mapping["/octelium.api.main.cordium.v1.WorkspaceService/Exec"] = Handler(
            self.execute, Cardinality.STREAM_STREAM, p.ExecRequest, p.ExecResponse
        )
        return mapping

    async def execute(self, stream):
        self.active += 1
        self.closed.clear()
        process = None
        tasks = []
        try:
            first = await stream.recv_message()
            request = first.request
            self.requests.append(first)
            if request.command == "missing-exit":
                return
            if request.command == "overflow":
                await stream.send_message(
                    p.ExecResponse(stdout=p.ExecResponseStdout(data=b"x" * 4096))
                )
                await asyncio.Event().wait()
            process = await asyncio.create_subprocess_shell(
                request.command,
                executable="/bin/sh",
                cwd=request.working_dir or None,
                env=os.environ | {e.key: e.value for e in request.env_vars},
                stdin=asyncio.subprocess.PIPE if request.has_stdin else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )

            async def input_loop():
                async for message in stream:
                    self.requests.append(message)
                    kind, _ = betterproto.which_one_of(message, "type")
                    if kind == "write_data" and process.stdin:
                        process.stdin.write(message.write_data.data)
                        await process.stdin.drain()
                    elif kind == "kill":
                        with suppress(ProcessLookupError):
                            os.killpg(process.pid, signal.SIGKILL)

            async def output_loop(reader, kind):
                while data := await reader.read(8192):
                    value = (
                        p.ExecResponse(stdout=p.ExecResponseStdout(data=data))
                        if kind == "stdout"
                        else p.ExecResponse(stderr=p.ExecResponseStderr(data=data))
                    )
                    await stream.send_message(value)

            tasks = [
                asyncio.create_task(input_loop()),
                asyncio.create_task(output_loop(process.stdout, "stdout")),
                asyncio.create_task(output_loop(process.stderr, "stderr")),
            ]
            code = await process.wait()
            await asyncio.gather(*tasks[1:])
            await stream.send_message(
                p.ExecResponse(exit=p.ExecResponseExit(code=code if code >= 0 else -1))
            )
            # Match the real server: stay open after exit until cancellation.
            await asyncio.Event().wait()
        finally:
            if process is not None:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.active -= 1
            self.closed.set()

    async def create_terminal(self, request):
        self.requests.append(request)
        return p.CreateTerminalResponse(id="abc-term")

    async def list_terminal(self, request):
        return p.ListTerminalResponse(items=[p.Terminal(id="abc-term")])

    async def write_terminal_data(self, request):
        self.requests.append(request)
        return p.WriteTerminalDataResponse()

    async def set_terminal_window_size(self, request):
        self.requests.append(request)
        return p.SetTerminalWindowSizeResponse()

    async def remove_terminal(self, request):
        self.removed.append(request.id)
        return p.RemoveTerminalResponse()

    async def listen_terminal(self, request):
        try:
            yield p.ListenTerminalResponse(stdout=p.ListenTerminalResponseStdout(data=b"hello\x00"))
            yield p.ListenTerminalResponse(
                window_size=p.ListenTerminalResponseWindowSize(cols=100, rows=40)
            )
            await asyncio.Event().wait()
        finally:
            self.terminal_closed.set()

    async def listen_log(self, request):
        yield p.ListenLogResponse(data=b"building\xff")


class Auth(a.MainServiceBase):
    def __init__(self):
        self.requests = []
        self.delay = 0
        self.fail_refresh = False

    async def authenticate_with_authentication_token(self, request):
        self.requests.append(request)
        if self.delay:
            await asyncio.sleep(self.delay)
        return a.SessionToken(
            access_token="session-token", refresh_token="refresh-secret", expires_in=3600
        )

    async def authenticate_with_assertion(self, request):
        self.requests.append(request)
        if self.delay:
            await asyncio.sleep(self.delay)
        return a.SessionToken(
            access_token="assertion-token", refresh_token="refresh-secret", expires_in=3600
        )

    async def authenticate_with_refresh_token(self, request):
        self.requests.append(request)
        if self.fail_refresh:
            raise GRPCError(Status.UNAUTHENTICATED, "expired")
        return a.SessionToken(
            access_token="refreshed", refresh_token="refresh-secret-2", expires_in=3600
        )


@pytest.fixture
async def cluster():
    main, runtime, auth = Main(), Runtime(), Auth()
    metadata = []
    server = Server([main, runtime, auth])

    async def record(event):
        metadata.append((event.method_name, dict(event.metadata)))

    listen(server, RecvRequest, record)
    await server.start("127.0.0.1", 0)
    port = server._server.sockets[0].getsockname()[1]
    yield SimpleNamespace(main=main, runtime=runtime, auth=auth, metadata=metadata, port=port)
    server.close()
    await server.wait_closed()


@pytest.fixture
async def client(cluster):
    async with AsyncCordium(
        "example.test", auth=AccessToken("token"), host="127.0.0.1", port=cluster.port, tls=False
    ) as value:
        yield value
