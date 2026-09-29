"""Concurrent commands and binary streaming with native asyncio cancellation."""

import asyncio
import sys

from cordium import AsyncCordium


async def main() -> None:
    async with AsyncCordium() as client:
        workspace = await client.workspaces.create(image="python:3.13", ephemeral=True)
        try:
            await workspace.start()
            await workspace.wait_until_running()
            async with asyncio.TaskGroup() as group:
                version = group.create_task(workspace.exec(["python", "--version"]))
                system = group.create_task(workspace.exec(["uname", "-a"]))
            print(version.result().stdout, system.result().stdout)
            async with await workspace.exec_stream(
                ["python", "-u", "-c", "print('streamed output')"], check=True, timeout=30
            ) as command:
                async for event in command:
                    sys.stdout.buffer.write(event.data)
                print("exit:", (await command.wait()).exit_code)
        finally:
            if not (await workspace.refresh()).is_stopped:
                await workspace.stop()
                await workspace.wait_until_stopped()
            await workspace.delete()


if __name__ == "__main__":
    asyncio.run(main())
