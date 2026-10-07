import asyncio
import threading

import pytest
from cordium import AccessToken, Cordium, CordiumError, Workspace, meta


async def test_sync_matches_async_and_streams(cluster, tmp_path):
    def run():
        with Cordium(
            "example.test",
            auth=AccessToken("token"),
            **cluster.connection,
        ) as client:
            ws = client.workspaces.run(image="python:3.13")
            assert isinstance(ws, Workspace)
            assert ws.exec(["printf", "%s", "hello"]).stdout == "hello"
            with ws.exec_stream("printf stream") as command:
                assert b"".join(event.data for event in command) == b"stream"
                assert command.wait().success
            with client.workspaces.all(page_size=1) as workspaces:
                assert len(list(workspaces)) == 2
            assert isinstance(client.workspaces.list().items[0], Workspace)
            with ws.watch() as events:
                assert next(events).workspace.metadata.name == "abc"
            with ws.terminals.create() as terminal:
                with terminal.events as events:
                    assert next(events).data == b"hello\x00"
                terminal.remove()
            ws.files.write_text(str(tmp_path / "file"), "hello")
            assert ws.files.read_text(str(tmp_path / "file")) == "hello"
            raw = client.call(
                lambda c: c.raw.main.get_workspace(meta.GetOptions(name="abc"), timeout=1)
            )
            assert raw.metadata.name == "abc"
        assert not client._portal._thread.is_alive()
        client.close()
        with pytest.raises(CordiumError):
            client.workspaces.get("abc")

    await asyncio.to_thread(run)


async def test_sync_close_interrupts_blocking_operation(cluster):
    client = Cordium("example.test", auth=AccessToken("token"), **cluster.connection)
    cluster.main.delay = 20
    request = asyncio.create_task(asyncio.to_thread(client.workspaces.get, "abc"))
    await asyncio.sleep(0.03)
    await asyncio.to_thread(client.close)
    with pytest.raises(CordiumError) as closed:
        await request
    assert closed.value.code == "CLIENT_CLOSED"


def test_invalid_sync_constructor_releases_thread():
    before = {thread.ident for thread in threading.enumerate()}
    with pytest.raises(ValueError):
        Cordium("https://invalid", auth=AccessToken("x"))
    assert {thread.ident for thread in threading.enumerate()} == before


def test_reentrant_sync_callback_rejected():
    with Cordium("example.test", auth=AccessToken("x")) as client:

        async def operation(async_client):
            client.access_token()

        with pytest.raises(CordiumError, match="callbacks"):
            client.call(operation)


async def test_sync_contexts_close_after_parent(cluster):
    def run():
        with Cordium(
            "example.test",
            auth=AccessToken("token"),
            **cluster.connection,
        ) as client:
            workspace = client.workspaces.get("abc")
            with (
                workspace.watch() as events,
                workspace.exec_stream("printf ready; sleep 60") as command,
                workspace.terminals.create() as terminal,
            ):
                assert next(events).workspace.metadata.name == "abc"
                assert next(iter(command)).data == b"ready"
                with terminal.events as output:
                    assert next(output).type == "output"
                    client.close()
            events.close()
            command.close()
            terminal.close()
        assert not client._portal._thread.is_alive()

    await asyncio.to_thread(run)
