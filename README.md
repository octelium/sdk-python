# Octelium Python SDK

This is the official Python SDK for Octelium. The SDK provides authenticated access to Octelium gRPC and HTTP APIs using async Python APIs built on top of `grpclib`, `aiohttp`, and `betterproto`.

## Installation

```bash
pip install octelium-sdk
```

Or install the generated protobuf APIs separately:

```bash
pip install octelium-apis
```

# Basic Example

```python
import asyncio

from octelium.sdk import (
    AuthConfig,
    AuthTokenConfig,
    OcteliumClient,
    OcteliumClientConfig,
)

from octelium.api.main.core.v1 import ListNamespaceOptions

async def main() -> None:
    client = await OcteliumClient.create(
        OcteliumClientConfig(
            domain="<DOMAIN>",
            auth=AuthConfig(
                type="auth_token",
                auth_token=AuthTokenConfig(
                    token="<AUTH_TOKEN>"
                ),
            ),
        )
    )

    try:
        resp = await client.core_v1.list_namespace(
            ListNamespaceOptions()
        )       

        for namespace in resp.items:
            print(namespace)

    finally:
        await client.close()


asyncio.run(main())
```


## OAuth2 Client Credentials

```python
from octelium.sdk import (
    AuthConfig,
    OAuth2ClientCredentialsConfig,
    OcteliumClient,
    OcteliumClientConfig,
)

client = await OcteliumClient.create(
    OcteliumClientConfig(
        domain="example.com",
        auth=AuthConfig(
            type="oauth2_client_credentials",
            oauth2_client_credentials=OAuth2ClientCredentialsConfig(
                client_id="client-id",
                client_secret="client-secret",
                scopes=["core"],
            ),
        ),
    )
)
```

## Authenticated HTTP Requests

```python
async with client.http_client() as http:
    resp = await http.get(
        "https://my-api.<DOMAIN>/v1/users"
    )

    data = await resp.json()
    print(data)
```

## Lazy Authentication

Authentication is lazy by default. The SDK only authenticates when the first authenticated request is performed.

To authenticate immediately during client creation:

```python
client = await OcteliumClient.create(
    OcteliumClientConfig(
        authenticate_on_creation=True,
    )
)
```

## License

Apache License 2.0.