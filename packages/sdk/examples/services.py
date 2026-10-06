import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence

from octelium.api.main.core.v1 import (
    ListServiceOptions,
    Service,
    ServiceSpec,
    ServiceSpecAuthorization,
    ServiceSpecConfig,
    ServiceSpecConfigUpstream,
    ServiceSpecMode,
)
from octelium.api.main.meta.v1 import (
    CommonListOptions,
    DeleteOptions,
    GetOptions,
    Metadata,
    ObjectReference,
)
from octelium.sdk import OcteliumClient


async def list_services(
    client: OcteliumClient, *, namespace: str | None = None
) -> AsyncIterator[Service]:
    page = 0
    while True:
        options = ListServiceOptions(common=CommonListOptions(page=page, items_per_page=100))
        if namespace is not None:
            options.namespace_ref = ObjectReference(name=namespace)
        result = await client.core_v1.list_service(options, timeout=10)
        for service in result.items:
            yield service
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_service(
    client: OcteliumClient,
    name: str,
    upstream_url: str,
    *,
    policies: Sequence[str],
    public: bool = False,
) -> Service:
    return await client.core_v1.create_service(
        Service(
            metadata=Metadata(name=name),
            spec=ServiceSpec(
                mode=ServiceSpecMode.from_string("HTTP"),
                is_public=public,
                config=ServiceSpecConfig(upstream=ServiceSpecConfigUpstream(url=upstream_url)),
                authorization=ServiceSpecAuthorization(policies=list(policies)),
            ),
        ),
        timeout=10,
    )


async def update_service(
    client: OcteliumClient,
    name: str,
    *,
    upstream_url: str | None = None,
    policies: Sequence[str] | None = None,
    public: bool | None = None,
    disabled: bool | None = None,
) -> Service:
    service = await client.core_v1.get_service(GetOptions(name=name), timeout=10)
    if upstream_url is not None:
        service.spec.config.upstream.url = upstream_url
    if policies is not None:
        service.spec.authorization.policies = list(policies)
    if public is not None:
        service.spec.is_public = public
    if disabled is not None:
        service.spec.is_disabled = disabled
    return await client.core_v1.update_service(service, timeout=10)


async def delete_service(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_service(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage HTTP Services through the Core API.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List all Services, optionally in one Namespace")
    listing.add_argument("--namespace")
    get = commands.add_parser("get", help="Get a Service by its full name")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create an HTTP Service")
    create.add_argument("name", help="Service name, such as reports.default")
    create.add_argument("--upstream", required=True)
    create.add_argument("--policies", nargs="+", required=True)
    create.add_argument("--public", action="store_true")
    update = commands.add_parser("update", help="Change an existing Service's configuration")
    update.add_argument("name")
    update.add_argument("--upstream")
    update.add_argument("--policies", nargs="*")
    update.add_argument("--public", action=argparse.BooleanOptionalAction, default=None)
    update.add_argument("--disabled", action=argparse.BooleanOptionalAction, default=None)
    delete = commands.add_parser("delete", help="Delete a Service by name")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "update" and all(
        value is None for value in (args.upstream, args.policies, args.public, args.disabled)
    ):
        parser.error("update requires at least one field")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for service in list_services(client, namespace=args.namespace):
                print(service.to_json(indent=2))
        elif args.command == "get":
            service = await client.core_v1.get_service(GetOptions(name=args.name), timeout=10)
            print(service.to_json(indent=2))
        elif args.command == "create":
            service = await create_service(
                client, args.name, args.upstream, policies=args.policies, public=args.public
            )
            print(service.to_json(indent=2))
        elif args.command == "update":
            service = await update_service(
                client,
                args.name,
                upstream_url=args.upstream,
                policies=args.policies,
                public=args.public,
                disabled=args.disabled,
            )
            print(service.to_json(indent=2))
        else:
            await delete_service(client, args.name)
            print(f"Deleted Service {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
