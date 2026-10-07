import asyncio

import pytest
from cordium import CordiumError, ExecError, argv


async def test_exec_binary_argv_failure_and_capture(client):
    ws = await client.workspaces.get("abc")
    result = await ws.exec(["printf", "%s", "$(not executed); 'quoted'\n"])
    assert result.stdout == "$(not executed); 'quoted'\n"
    assert argv("A=b", "printf", "x") == "'A=b' printf x"
    with pytest.raises(ExecError) as assignment:
        await ws.exec(["A=b", "printf", "x"], check=True)
    assert assignment.value.result.exit_code == 127
    with pytest.raises(ExecError) as failure:
        await ws.exec("printf fail >&2; exit 7", check=True)
    assert failure.value.result.exit_code == 7
    assert failure.value.result.stderr == "fail"
    assert str(failure.value) == "Command exited with code 7: fail"
    result = await ws.exec("printf abcdef; printf uvwxyz >&2", max_capture_bytes=3)
    assert result.stdout_bytes == b"abc" and result.stderr_bytes == b"uvw"
    assert result.truncated
    result = await ws.exec("exit 3")
    assert not result.success and result.exit_code == 3
    assert (await ws.exec("head -c 4", stdin=b"\x00\xffab")).stdout_bytes == b"\x00\xffab"


async def test_stream_write_kill_cancel(client, cluster):
    ws = await client.workspaces.get("abc")
    async with await ws.exec_stream("head -c 4") as session:
        await session.write(b"test")
        chunks = [event async for event in session]
        assert b"".join(event.data for event in chunks) == b"test"
        assert (await session.wait()).success
    await asyncio.wait_for(cluster.runtime.closed.wait(), 2)
    async with await ws.exec_stream("sleep 60") as session:
        await session.kill()
        result = await session.wait()
        assert result.killed and result.exit_code == -1
    async with await ws.exec_stream("printf ready; sleep 60") as session:
        assert (await anext(session)).data == b"ready"
    await asyncio.wait_for(cluster.runtime.closed.wait(), 2)
    assert cluster.runtime.active == 0


async def test_exec_failures_and_cancellation(client, cluster):
    ws = await client.workspaces.get("abc")
    with pytest.raises(CordiumError) as timeout:
        await ws.exec("sleep 60", timeout=0.03)
    assert timeout.value.code == "DEADLINE_EXCEEDED"
    with pytest.raises(CordiumError) as protocol:
        await ws.exec("missing-exit")
    assert protocol.value.code == "PROTOCOL_ERROR"
    async with await ws.exec_stream("missing-exit") as session:
        with pytest.raises(CordiumError) as ended:
            await session.wait()
        with pytest.raises(CordiumError) as write:
            await session.write(b"input")
        assert ended.value.code == write.value.code == "PROTOCOL_ERROR"
    task = asyncio.create_task(ws.exec("sleep 60"))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(cluster.runtime.closed.wait(), 2)


async def test_slow_consumers_apply_backpressure(client):
    ws = await client.workspaces.get("abc")
    async with await ws.exec_stream("head -c 200000 /dev/zero", max_buffer_bytes=1024) as session:
        received = 0
        async for event in session:
            assert session._queued <= 8192
            received += len(event.data)
            await asyncio.sleep(0.001)
        assert received == 200000
        assert (await session.wait()).success
    async with await ws.exec_stream("overflow", max_buffer_bytes=1024) as stream:
        assert (await anext(stream)).data == b"x" * 4096


async def test_kill_without_exit_ends_after_grace(client, cluster):
    cluster.runtime.exit_after_kill = False
    ws = await client.workspaces.get("abc")
    async with await ws.exec_stream("printf partial; sleep 60", kill_grace=0.05) as session:
        assert (await anext(session)).data == b"partial"
        await session.kill()
        result = await asyncio.wait_for(session.wait(), 2)
        assert result.killed and result.exit_code == -1
        assert result.stdout == "partial"
    await asyncio.wait_for(cluster.runtime.closed.wait(), 2)
    async with await ws.exec_stream("sleep 60", kill_grace=0.05, check=True) as session:
        await session.kill()
        with pytest.raises(ExecError) as killed:
            await asyncio.wait_for(session.wait(), 2)
        assert killed.value.result.killed
    with pytest.raises(ValueError):
        await ws.exec_stream("true", kill_grace=0)


async def test_files_roundtrip_and_quoting(client, tmp_path):
    ws = await client.workspaces.get("abc")
    folder = tmp_path / "sub folder"
    await ws.files.mkdir(str(folder))
    remote = str(folder / "a ' $(false);.bin")
    data = bytes(range(256)) * 1200
    await ws.files.write_bytes(remote, data)
    assert await ws.files.read_bytes(remote) == data
    with pytest.raises(CordiumError) as bounded:
        await ws.files.read_bytes(remote, max_bytes=100)
    assert bounded.value.code == "RESOURCE_EXHAUSTED"
    local = tmp_path / "downloaded"
    assert await ws.files.download(remote, local) == local
    assert local.read_bytes() == data
    assert local.stat().st_mode & 0o777 == 0o600
    await ws.files.upload(local, remote)
    assert await ws.files.read_bytes(remote) == data
    local.write_bytes(b"original")
    with pytest.raises(ExecError):
        await ws.files.download(str(folder / "missing"), local)
    assert local.read_bytes() == b"original"
    assert not list(tmp_path.glob(".cordium-*"))
    await ws.files.write_text(remote, "hello ☃")
    assert await ws.files.read_text(remote) == "hello ☃"
    await ws.files.write_bytes(remote, b"")
    assert await ws.files.read_bytes(remote, max_bytes=0) == b""
    nested = str(tmp_path / "new" / "deep" / "file.txt")
    await ws.files.write_text(nested, "nested")
    downloaded = tmp_path / "local" / "nested" / "file.txt"
    await ws.files.download(nested, downloaded)
    assert downloaded.read_text() == "nested"
    await ws.files.remove(str(folder), recursive=True)
    assert not folder.exists()


async def test_terminal_detach_does_not_remove(client, cluster):
    ws = await client.workspaces.get("abc")
    terminal = await ws.terminals.create(cols=120, rows=40)
    assert terminal.id == "abc-term"
    assert await ws.terminals.list() == ("abc-term",)
    async with terminal:
        assert (await anext(terminal.events)).data == b"hello\x00"
        resized = await anext(terminal.events)
        assert resized.type == "resize" and resized.cols == 100
        await terminal.write(b"echo yes\n")
        await terminal.resize(80, 24)
    await asyncio.wait_for(cluster.runtime.terminal_closed.wait(), 2)
    assert not cluster.runtime.removed
    with pytest.raises(CordiumError):
        await terminal.write("no")
    await terminal.remove()
    assert cluster.runtime.removed == ["abc-term"]
