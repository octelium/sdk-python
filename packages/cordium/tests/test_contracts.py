import asyncio
import inspect

import cordium
import pytest
from cordium import CordiumError, Ref


async def test_watch_deadline_overflow_and_close(client, cluster):
    async with client.workspaces.watch(timeout=0.03) as events:
        await anext(events)
        with pytest.raises(CordiumError) as deadline:
            await anext(events)
        assert deadline.value.code == "DEADLINE_EXCEEDED"
    async with client.workspaces.watch(max_buffer_bytes=1) as events:
        with pytest.raises(CordiumError) as overflow:
            await anext(events)
        assert overflow.value.code == "RESOURCE_EXHAUSTED"
    events = client.workspaces.watch()
    await anext(events)
    pending = asyncio.create_task(anext(events))
    await asyncio.sleep(0.01)
    await client.aclose()
    with pytest.raises(CordiumError) as closed:
        await pending
    assert closed.value.code == "CLIENT_CLOSED"


async def test_wrong_loop_and_early_validation(client, cluster):
    await client.access_token()

    def wrong_loop():
        return asyncio.run(client.access_token())

    with pytest.raises(CordiumError, match="same event loop"):
        await asyncio.to_thread(wrong_loop)
    for timeout in (0, -1, float("nan"), float("inf"), True):
        with pytest.raises(ValueError):
            await client.workspaces.create(timeout=timeout)
    with pytest.raises(ValueError):
        await client.workspaces.run(poll_interval=-1)
    assert not cluster.main.requests


@pytest.mark.parametrize(
    "kwargs", [{}, {"name": ""}, {"uid": ""}, {"name": "x", "uid": "y"}, {"name": "a\0b"}]
)
def test_invalid_references(kwargs):
    with pytest.raises(ValueError):
        Ref(**kwargs)


def test_public_exports_documented_and_sync_parity():
    for name in cordium.__all__:
        value = getattr(cordium, name)
        if not (inspect.isclass(value) or inspect.isfunction(value)) or not getattr(
            value, "__module__", ""
        ).startswith("cordium"):
            continue
        assert inspect.getdoc(value), name
        if inspect.isclass(value):
            for member_name, member in vars(value).items():
                if not member_name.startswith("_") and (
                    inspect.isfunction(member) or isinstance(member, property)
                ):
                    assert inspect.getdoc(member), f"{name}.{member_name}"
        if name.startswith("Async") and name.removeprefix("Async") in cordium.__all__:
            blocking = getattr(cordium, name.removeprefix("Async"))
            for member_name, member in vars(value).items():
                if member_name.startswith("_") or member_name == "raw":
                    continue
                if inspect.isfunction(member):
                    sync_name = "close" if member_name == "aclose" else member_name
                    other = getattr(blocking, sync_name)
                    assert list(inspect.signature(member).parameters) == list(
                        inspect.signature(other).parameters
                    ), (name, member_name)
