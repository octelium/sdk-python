import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

from octelium.api.main.core.v1 import (
    Credential,
    CredentialSpec,
    CredentialSpecAuthorization,
    CredentialSpecType,
    CredentialToken,
    GenerateCredentialTokenRequest,
    ListCredentialOptions,
    SessionStatusType,
)
from octelium.api.main.meta.v1 import (
    CommonListOptions,
    DeleteOptions,
    GetOptions,
    Metadata,
    ObjectReference,
)
from octelium.sdk import OcteliumClient


async def list_credentials(
    client: OcteliumClient, *, user: str | None = None
) -> AsyncIterator[Credential]:
    page = 0
    while True:
        options = ListCredentialOptions(common=CommonListOptions(page=page, items_per_page=100))
        if user is not None:
            options.user_ref = ObjectReference(name=user)
        result = await client.core_v1.list_credential(options, timeout=10)
        for credential in result.items:
            yield credential
        if not result.list_response_meta.has_more:
            return
        page += 1


async def create_credential(
    client: OcteliumClient,
    name: str,
    user: str,
    *,
    credential_type: CredentialSpecType,
    expires_hours: int = 24,
    max_authentications: int | None = None,
    policies: Sequence[str] = (),
) -> Credential:
    if not 1 <= expires_hours <= 24 * 365 * 2:
        raise ValueError("expires_hours must be between 1 and 17520")
    if max_authentications is None:
        max_authentications = 1 if credential_type == CredentialSpecType.AUTH_TOKEN else 0
    if not 0 <= max_authentications <= 1_000_000:
        raise ValueError("max_authentications must be between 0 and 1000000")
    return await client.core_v1.create_credential(
        Credential(
            metadata=Metadata(name=name),
            spec=CredentialSpec(
                type=credential_type,
                user=user,
                expires_at=datetime.now(UTC) + timedelta(hours=expires_hours),
                max_authentications=max_authentications,
                session_type=SessionStatusType.from_string("CLIENTLESS"),
                authorization=CredentialSpecAuthorization(policies=list(policies)),
            ),
        ),
        timeout=10,
    )


async def generate_credential_token(client: OcteliumClient, name: str) -> CredentialToken:
    return await client.core_v1.generate_credential_token(
        GenerateCredentialTokenRequest(credential_ref=ObjectReference(name=name)), timeout=10
    )


async def update_credential(client: OcteliumClient, name: str, *, disabled: bool) -> Credential:
    credential = await client.core_v1.get_credential(GetOptions(name=name), timeout=10)
    credential.spec.is_disabled = disabled
    return await client.core_v1.update_credential(credential, timeout=10)


async def delete_credential(client: OcteliumClient, name: str) -> None:
    await client.core_v1.delete_credential(DeleteOptions(name=name), timeout=10)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create, issue tokens for, and manage Credentials."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List Credentials, optionally for one User")
    listing.add_argument("--user")
    get = commands.add_parser("get", help="Get a Credential resource without issuing a token")
    get.add_argument("name")
    create = commands.add_parser("create", help="Create a Credential resource for an existing User")
    create.add_argument("name")
    create.add_argument("--user", required=True)
    create.add_argument("--type", choices=("auth-token", "oauth2", "access-token"), required=True)
    create.add_argument("--expires-hours", type=int, default=24)
    create.add_argument("--max-authentications", type=int)
    create.add_argument("--policies", nargs="*", default=[])
    token = commands.add_parser(
        "token", help="Generate or rotate a token; outputs the secret as JSON"
    )
    token.add_argument("name")
    update = commands.add_parser("update", help="Enable or disable an existing Credential")
    update.add_argument("name")
    update.add_argument("--disabled", action=argparse.BooleanOptionalAction, required=True)
    delete = commands.add_parser("delete", help="Delete a Credential by name")
    delete.add_argument("name")
    args = parser.parse_args()
    if args.command == "create":
        if not 1 <= args.expires_hours <= 24 * 365 * 2:
            parser.error("--expires-hours must be between 1 and 17520")
        if args.max_authentications is not None and not 0 <= args.max_authentications <= 1_000_000:
            parser.error("--max-authentications must be between 0 and 1000000")
    async with await OcteliumClient.create() as client, asyncio.timeout(120):
        if args.command == "list":
            async for credential in list_credentials(client, user=args.user):
                print(credential.to_json(indent=2))
        elif args.command == "get":
            credential = await client.core_v1.get_credential(GetOptions(name=args.name), timeout=10)
            print(credential.to_json(indent=2))
        elif args.command == "create":
            credential = await create_credential(
                client,
                args.name,
                args.user,
                credential_type=CredentialSpecType.from_string(args.type.upper().replace("-", "_")),
                expires_hours=args.expires_hours,
                max_authentications=args.max_authentications,
                policies=args.policies,
            )
            print(credential.to_json(indent=2))
        elif args.command == "token":
            result = await generate_credential_token(client, args.name)
            print(result.to_json(indent=2))
        elif args.command == "update":
            credential = await update_credential(client, args.name, disabled=args.disabled)
            print(credential.to_json(indent=2))
        else:
            await delete_credential(client, args.name)
            print(f"Deleted Credential {args.name}")


if __name__ == "__main__":
    asyncio.run(main())
