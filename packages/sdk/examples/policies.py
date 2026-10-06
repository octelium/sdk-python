import argparse
import asyncio
from collections.abc import AsyncIterator

from octelium.api.main.core.v1 import (
    Condition,
    ListPolicyOptions,
    Policy,
    PolicySpec,
    PolicySpecRule,
    PolicySpecRuleEffect,
)
from octelium.api.main.meta.v1 import CommonListOptions, DeleteOptions, GetOptions, Metadata
from octelium.sdk import OcteliumClient


async def list_policies(client: OcteliumClient) -> AsyncIterator[Policy]:
    page = 0
    while True:
        result = await client.core_v1.list_policy(
            ListPolicyOptions(common=CommonListOptions(page=page, items_per_page=100)), timeout=10
        )
        for policy in result.items:
            yield policy
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_policy(
    client: OcteliumClient,
    name: str,
    match: str,
    *,
    rule_name: str = "allow-access",
    effect: PolicySpecRuleEffect | None = None,
) -> Policy:
    return await client.core_v1.create_policy(
        Policy(
            metadata=Metadata(name=name),
            spec=PolicySpec(
                rules=[
                    PolicySpecRule(
                        name=rule_name,
                        condition=Condition(match=match),
                        effect=effect
                        if effect is not None
                        else PolicySpecRuleEffect.from_string("ALLOW"),
                    )
                ]
            ),
        ),
        timeout=10,
    )


async def update_policy(
    client: OcteliumClient,
    name: str,
    *,
    rule_name: str = "allow-access",
    match: str | None = None,
    effect: PolicySpecRuleEffect | None = None,
    disabled: bool | None = None,
) -> Policy:
    policy = await client.core_v1.get_policy(GetOptions(name=name), timeout=10)
    if match is not None or effect is not None:
        rule = next((rule for rule in policy.spec.rules if rule.name == rule_name), None)
        if rule is None:
            raise ValueError(f"Policy {name} has no rule named {rule_name}")
        if match is not None:
            rule.condition = Condition(match=match)
        if effect is not None:
            rule.effect = effect
    if disabled is not None:
        policy.spec.is_disabled = disabled
    return await client.core_v1.update_policy(policy, timeout=10)


async def delete_policy(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_policy(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Core API Policies with CEL rules.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List all Policies, following pagination")
    get = commands.add_parser("get", help="Get a Policy by name")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create a Policy with one named CEL rule")
    create.add_argument("name")
    create.add_argument("--match", required=True)
    create.add_argument("--rule", default="allow-access")
    create.add_argument("--effect", choices=("allow", "deny"), default="allow")
    update = commands.add_parser("update", help="Edit a named rule or enable/disable a Policy")
    update.add_argument("name")
    update.add_argument("--rule", default="allow-access")
    update.add_argument("--match")
    update.add_argument("--effect", choices=("allow", "deny"))
    update.add_argument("--disabled", action=argparse.BooleanOptionalAction, default=None)
    delete = commands.add_parser("delete", help="Delete an unattached Policy")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "update" and all(
        value is None for value in (args.match, args.effect, args.disabled)
    ):
        parser.error("update requires at least one field")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for policy in list_policies(client):
                print(policy.to_json(indent=2))
        elif args.command == "get":
            policy = await client.core_v1.get_policy(GetOptions(name=args.name), timeout=10)
            print(policy.to_json(indent=2))
        elif args.command == "create":
            policy = await create_policy(
                client,
                args.name,
                args.match,
                rule_name=args.rule,
                effect=PolicySpecRuleEffect.from_string(args.effect.upper()),
            )
            print(policy.to_json(indent=2))
        elif args.command == "update":
            policy = await update_policy(
                client,
                args.name,
                rule_name=args.rule,
                match=args.match,
                effect=PolicySpecRuleEffect.from_string(args.effect.upper())
                if args.effect
                else None,
                disabled=args.disabled,
            )
            print(policy.to_json(indent=2))
        else:
            await delete_policy(client, args.name)
            print(f"Deleted Policy {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
