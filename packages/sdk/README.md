# Octelium Cluster Python SDK

Requires Python 3.11 or later. Install with `pip install octelium-sdk`; generated APIs are installed automatically.

See the [Core API examples](examples/README.md) for creating, listing, updating, and deleting Users, Services, Policies, Credentials, Groups, and Namespaces, and for reading and updating ClusterConfig.

```python
import asyncio

from octelium.api.main.core.v1 import ListNamespaceOptions
from octelium.sdk import AuthConfig, AuthTokenConfig, OcteliumClient, OcteliumClientConfig


async def main() -> None:
    config = OcteliumClientConfig(
        domain="example.com",
        auth=AuthConfig(
            type="auth_token",
            auth_token=AuthTokenConfig(token="<AUTH_TOKEN>", scopes=["api:core"]),
        ),
    )
    async with await OcteliumClient.create(config) as client:
        namespaces = await client.core_v1.list_namespace(ListNamespaceOptions())
        for namespace in namespaces.items:
            print(namespace)


asyncio.run(main())
```

## Credentials and refresh

Explicit `config.auth` takes precedence over the environment. Without it, the constructor reads `OCTELIUM_ACCESS_TOKEN`, then `OCTELIUM_AUTH_TOKEN`, once. The domain comes from `config.domain` or `OCTELIUM_DOMAIN`. Changing environment variables later does not change an existing client's identity. Configuration objects are frozen, scopes are copied to tuples, and credential fields are omitted from their representations.

Authentication is lazy unless `authenticate_on_creation=True`. Concurrent callers share one client-owned exchange. Canceling a caller cancels its wait while the exchange continues and preserves any returned rotating refresh token. `authentication_timeout_seconds` bounds that exchange; `oauth2_timeout_seconds` also bounds OAuth HTTP requests. Transient proactive refresh failures may use the current token until its actual expiry, with retry backoff.

Static authentication tokens are treated as limited-use credentials. The SDK does not replay an attempted exchange after an ambiguous failure or recreate a session using a consumed credential. A worker timeout or lost response can still lose a remotely committed exchange; caller shielding cannot recover a response that never arrives. Create a client with a new credential when necessary.

Dynamic authentication providers must be async functions. They must honor cancellation and avoid blocking the event loop. Set `reusable=True` only if the provider can obtain a fresh credential to replace an expired or rejected managed session:

```python
async def fresh_credential() -> str:
    return await obtain_a_new_authentication_token()


auth = AuthConfig(
    type="auth_token",
    auth_token=AuthTokenConfig(token=fresh_credential, reusable=True),
)
```

Workload identity federation uses `AuthConfig(type="assertion", assertion=AssertionConfig(token=..., scopes=[...], identity_provider="..."))`. The token is an assertion string or an async provider, such as one that rereads a projected Kubernetes token. Assertions are not limited-use: when a refresh reports an expired session, the SDK authenticates again with a new assertion from the provider. `identity_provider` optionally names the IdentityProvider that verifies it.

`AuthConfig(type="access_token", access_token=...)` accepts a static access token or an async provider for externally managed tokens. A provider is consulted for each call, within `authentication_timeout_seconds`, and must return a nonempty token.

OAuth uses `OAuth2ClientCredentialsConfig(client_id=..., client_secret=..., scopes=["api:core"])` inside `AuthConfig(type="oauth2_client_credentials", oauth2_client_credentials=...)`. Scope examples also include `api:core.MainService/ListUser` and `service:<name>`. OAuth token requests reject redirects, validate their response, and never include raw response bodies in errors. An optional `max_oauth2_expires_in_seconds` imposes an application lifetime limit; the default permits supported long-lived tokens.

Refresh remains demand driven. There is no background maintenance timer for idle sessions. A call rejected with `UNAUTHENTICATED` is not replayed, but the managed session or OAuth token it carried is discarded, so the next call obtains a new one.

`client.channel` is the authenticated grpclib channel underlying `core_v1`, `user_v1` and `cordium_v1`. Pass it to the generated stubs of any other service, such as `WorkspaceServiceStub(client.channel)` from `octelium.api.main.cordium.v1`. `client.domain` is the resolved Cluster domain.

## HTTP destinations and deadlines

The HTTP helper sends `x-octelium-auth` only to HTTPS URLs under the Cluster domain at port 443, or to an exact origin listed in `authorized_http_origins`. URL userinfo, conflicting Host routing, and automatic redirects are rejected. A returned redirect response is left for the application to handle deliberately. `allow_insecure_http=True` is required for plain HTTP and should be limited to local development; an alternate port or external host also requires an explicit authorized origin.

```python
async with client.http_client() as http:
    response = await http.get("https://my-api.example.com/v1/users")
    async with response:
        data = await response.json()
```

Use one HTTP helper for repeated requests to share its pool. Each helper owns its session and the parent tracks and closes all helpers. Close or release responses after reading them. The helper's `aiohttp.ClientTimeout` budget includes the authentication wait; the remaining budget applies to the request and response body. A timeout can cancel the HTTP operation without canceling the shared authentication exchange.

For private PKI, supply `ssl_context_factory=lambda: ssl.create_default_context(cafile=...)`. The factory must return a new context on each call. The SDK creates separate contexts with the same trust configuration: gRPC advertises HTTP/2, while OAuth and service HTTP advertise HTTP/1.1. The factory can also load client certificates for mutual TLS. `api_host`/`api_port` select the gRPC endpoint, while `tls_server_name` independently selects its certificate name. `insecure_tls=True` disables certificate verification consistently and is intended for development.

## Lifecycle and migration

Construct and use the async client on one running event loop. `run_sync()` is a one-shot runner for a complete coroutine program, including resource creation and shutdown; it is not a blocking facade for reusing a client across loops. Use `asyncio.run(main())` for an ordinary entry point.

`close()` releases local resources and clears token caches. It does not revoke the remote session. Use `await client.logout()` explicitly when remote session termination is intended, then close the client. Logout uses refresh-token metadata and treats an already absent session as success. Clients using authentication tokens or assertions own a managed session. Clients using external access tokens or OAuth client credentials do not own a managed session for logout.

Concurrent closes share cleanup. Caller cancellation is propagated after owned cleanup, and an exception raised inside an async context is preserved. Closing the parent cancels owned exchanges and HTTP requests and closes child sessions. A retained service stub still rejects calls through the closed client.

Breaking changes from the previous implementation:

- Python 3.11 is the minimum supported version.
- Configuration is immutable; create a new configuration/client to change identity or transport settings.
- Synchronous authentication callbacks are rejected; use an async provider.
- `close()` performs local cleanup. Replace implicit logout with an explicit `logout()` call; `raise_logout_errors` was removed.
- HTTP uses Octelium headers, requires authorized destinations, and does not follow redirects.
- OAuth response types and token lifetimes are validated strictly.

Licensed under Apache-2.0; the distribution includes `LICENSE`.
