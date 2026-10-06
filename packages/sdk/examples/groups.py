import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence

from octelium.api.main.core.v1 import Group, GroupSpec, GroupSpecAuthorization, ListGroupOptions
from octelium.api.main.meta.v1 import CommonListOptions, DeleteOptions, GetOptions, Metadata
from octelium.sdk import OcteliumClient


async def list_groups(client: OcteliumClient) -> AsyncIterator[Group]:
    page = 0
    while True:
        result = await client.core_v1.list_group(
            ListGroupOptions(common=CommonListOptions(page=page, items_per_page=100)), timeout=10
        )
        for group in result.items:
            yield group
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_group(client: OcteliumClient, name: str, policies: Sequence[str]) -> Group:
    if not policies:
        raise ValueError("at least one Policy is required by this example")
    return await client.core_v1.create_group(
        Group(
            metadata=Metadata(name=name),
            spec=GroupSpec(authorization=GroupSpecAuthorization(policies=list(policies))),
        ),
        timeout=10,
    )


async def update_group(
    client: OcteliumClient,
    name: str,
    *,
    policies: Sequence[str] | None = None,
    display_name: str | None = None,
) -> Group:
    group = await client.core_v1.get_group(GetOptions(name=name), timeout=10)
    if policies is not None:
        group.spec.authorization.policies = list(policies)
    if display_name is not None:
        group.metadata.display_name = display_name
    return await client.core_v1.update_group(group, timeout=10)


async def delete_group(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_group(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Groups and their attached Policies.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List all Groups, following pagination")
    get = commands.add_parser("get", help="Get a Group by name")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create a Group with existing Policies")
    create.add_argument("name")
    create.add_argument("--policies", nargs="+", required=True)
    update = commands.add_parser("update", help="Replace a Group's Policy list or display name")
    update.add_argument("name")
    update.add_argument("--policies", nargs="*")
    update.add_argument("--display-name")
    delete = commands.add_parser("delete", help="Delete a Group by name")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "update" and args.policies is None and args.display_name is None:
        parser.error("update requires at least one field")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for group in list_groups(client):
                print(group.to_json(indent=2))
        elif args.command == "get":
            group = await client.core_v1.get_group(GetOptions(name=args.name), timeout=10)
            print(group.to_json(indent=2))
        elif args.command == "create":
            group = await create_group(client, args.name, args.policies)
            print(group.to_json(indent=2))
        elif args.command == "update":
            group = await update_group(
                client, args.name, policies=args.policies, display_name=args.display_name
            )
            print(group.to_json(indent=2))
        else:
            await delete_group(client, args.name)
            print(f"Deleted Group {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
