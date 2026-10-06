import argparse
import asyncio

from octelium.api.main.core.v1 import ClusterConfig, GetClusterConfigRequest
from octelium.sdk import OcteliumClient


async def get_cluster_config(client: OcteliumClient) -> ClusterConfig:
    return await client.core_v1.get_cluster_config(GetClusterConfigRequest(), timeout=10)


async def update_session_limits(
    client: OcteliumClient,
    *,
    human_max_sessions: int | None = None,
    workload_max_sessions: int | None = None,
) -> ClusterConfig:
    if human_max_sessions is None and workload_max_sessions is None:
        raise ValueError("at least one session limit is required")
    for value in (human_max_sessions, workload_max_sessions):
        if value is not None and not 1 <= value <= 1000:
            raise ValueError("session limits must be between 1 and 1000")
    config = await client.core_v1.get_cluster_config(GetClusterConfigRequest(), timeout=10)
    if human_max_sessions is not None:
        config.spec.session.human.max_per_user = human_max_sessions
    if workload_max_sessions is not None:
        config.spec.session.workload.max_per_user = workload_max_sessions
    return await client.core_v1.update_cluster_config(config, timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Read ClusterConfig and change session limits.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("get", help="Show the current ClusterConfig")
    update = commands.add_parser("update", help="Set maximum concurrent sessions per User")
    update.add_argument("--human-max-sessions", type=int)
    update.add_argument("--workload-max-sessions", type=int)
    args = parser.parse_args()
    if args.command == "update":
        limits = (args.human_max_sessions, args.workload_max_sessions)
        if all(value is None for value in limits):
            parser.error("update requires at least one session limit")
        if any(value is not None and not 1 <= value <= 1000 for value in limits):
            parser.error("session limits must be between 1 and 1000")
    async with await OcteliumClient.create() as client, asyncio.timeout(30):
        if args.command == "get":
            config = await get_cluster_config(client)
        else:
            config = await update_session_limits(
                client,
                human_max_sessions=args.human_max_sessions,
                workload_max_sessions=args.workload_max_sessions,
            )
        print(config.to_json(indent=2))


if __name__ == "__main__":
    asyncio.run(main())
