import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence

from octelium.api.main.core.v1 import (
    ListNamespaceOptions,
    Namespace,
    NamespaceSpec,
    NamespaceSpecAuthorization,
)
from octelium.api.main.meta.v1 import CommonListOptions, DeleteOptions, GetOptions, Metadata
from octelium.sdk import OcteliumClient


async def list_namespaces(client: OcteliumClient) -> AsyncIterator[Namespace]:
    page = 0
    while True:
        result = await client.core_v1.list_namespace(
            ListNamespaceOptions(common=CommonListOptions(page=page, items_per_page=100)),
            timeout=10,
        )
        for namespace in result.items:
            yield namespace
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_namespace(client: OcteliumClient, name: str, policies: Sequence[str]) -> Namespace:
    if not policies:
        raise ValueError("at least one Policy is required by this example")
    return await client.core_v1.create_namespace(
        Namespace(
            metadata=Metadata(name=name),
            spec=NamespaceSpec(authorization=NamespaceSpecAuthorization(policies=list(policies))),
        ),
        timeout=10,
    )


async def update_namespace(
    client: OcteliumClient,
    name: str,
    *,
    policies: Sequence[str] | None = None,
    display_name: str | None = None,
) -> Namespace:
    namespace = await client.core_v1.get_namespace(GetOptions(name=name), timeout=10)
    if policies is not None:
        namespace.spec.authorization.policies = list(policies)
    if display_name is not None:
        namespace.metadata.display_name = display_name
    return await client.core_v1.update_namespace(namespace, timeout=10)


async def delete_namespace(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_namespace(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Namespaces and their attached Policies.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List all Namespaces, following pagination")
    get = commands.add_parser("get", help="Get a Namespace by name")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create a Namespace with existing Policies")
    create.add_argument("name")
    create.add_argument("--policies", nargs="+", required=True)
    update = commands.add_parser("update", help="Replace a Namespace's Policy list or display name")
    update.add_argument("name")
    update.add_argument("--policies", nargs="*")
    update.add_argument("--display-name")
    delete = commands.add_parser("delete", help="Delete an empty Namespace by name")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "update" and args.policies is None and args.display_name is None:
        parser.error("update requires at least one field")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for namespace in list_namespaces(client):
                print(namespace.to_json(indent=2))
        elif args.command == "get":
            namespace = await client.core_v1.get_namespace(GetOptions(name=args.name), timeout=10)
            print(namespace.to_json(indent=2))
        elif args.command == "create":
            namespace = await create_namespace(client, args.name, args.policies)
            print(namespace.to_json(indent=2))
        elif args.command == "update":
            namespace = await update_namespace(
                client, args.name, policies=args.policies, display_name=args.display_name
            )
            print(namespace.to_json(indent=2))
        else:
            await delete_namespace(client, args.name)
            print(f"Deleted Namespace {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
