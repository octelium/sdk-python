import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence

from octelium.api.main.core.v1 import ListUserOptions, User, UserSpec, UserSpecType
from octelium.api.main.meta.v1 import CommonListOptions, DeleteOptions, GetOptions, Metadata
from octelium.sdk import OcteliumClient


async def list_users(client: OcteliumClient) -> AsyncIterator[User]:
    page = 0
    while True:
        result = await client.core_v1.list_user(
            ListUserOptions(common=CommonListOptions(page=page, items_per_page=100)), timeout=10
        )
        for user in result.items:
            yield user
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_user(
    client: OcteliumClient,
    name: str,
    *,
    user_type: UserSpecType,
    email: str = "",
    groups: Sequence[str] = (),
    display_name: str = "",
) -> User:
    return await client.core_v1.create_user(
        User(
            metadata=Metadata(name=name, display_name=display_name),
            spec=UserSpec(type=user_type, email=email, groups=list(groups)),
        ),
        timeout=10,
    )


async def update_user(
    client: OcteliumClient,
    name: str,
    *,
    email: str | None = None,
    groups: Sequence[str] | None = None,
    display_name: str | None = None,
    disabled: bool | None = None,
) -> User:
    user = await client.core_v1.get_user(GetOptions(name=name), timeout=10)
    if email is not None:
        user.spec.email = email
    if groups is not None:
        user.spec.groups = list(groups)
    if display_name is not None:
        user.metadata.display_name = display_name
    if disabled is not None:
        user.spec.is_disabled = disabled
    return await client.core_v1.update_user(user, timeout=10)


async def delete_user(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_user(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="List, create, update, and delete Core API Users.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List all Users, following pagination")
    get = commands.add_parser("get", help="Get a User by name")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create a HUMAN or WORKLOAD User")
    create.add_argument("name")
    create.add_argument("--type", choices=("human", "workload"), required=True)
    create.add_argument("--email", default="")
    create.add_argument("--groups", nargs="*", default=[])
    create.add_argument("--display-name", default="")
    update = commands.add_parser("update", help="Change selected fields of an existing User")
    update.add_argument("name")
    update.add_argument("--email")
    update.add_argument("--groups", nargs="*")
    update.add_argument("--display-name")
    update.add_argument("--disabled", action=argparse.BooleanOptionalAction, default=None)
    delete = commands.add_parser("delete", help="Delete a User by name")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "create" and args.type == "workload" and args.email:
        parser.error("--email is only supported for HUMAN Users")
    if args.command == "update" and all(
        value is None for value in (args.email, args.groups, args.display_name, args.disabled)
    ):
        parser.error("update requires at least one field")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for user in list_users(client):
                print(user.to_json(indent=2))
        elif args.command == "get":
            user = await client.core_v1.get_user(GetOptions(name=args.name), timeout=10)
            print(user.to_json(indent=2))
        elif args.command == "create":
            user = await create_user(
                client,
                args.name,
                user_type=UserSpecType.from_string(args.type.upper()),
                email=args.email,
                groups=args.groups,
                display_name=args.display_name,
            )
            print(user.to_json(indent=2))
        elif args.command == "update":
            user = await update_user(
                client,
                args.name,
                email=args.email,
                groups=args.groups,
                display_name=args.display_name,
                disabled=args.disabled,
            )
            print(user.to_json(indent=2))
        else:
            await delete_user(client, args.name)
            print(f"Deleted User {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
