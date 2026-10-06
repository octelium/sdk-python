# Core API examples

These examples show how to use `client.core_v1` to manage Octelium resources. Each file contains small functions with the actual generated request types and API calls, plus commands you can run against your Cluster.

| File | Operations |
| --- | --- |
| [users.py](users.py) | List, get, create, update, and delete HUMAN and WORKLOAD Users |
| [services.py](services.py) | List, get, create, update, and delete HTTP Services; configure upstreams and attach Policies |
| [policies.py](policies.py) | List, get, create, update, and delete Policies with named CEL rules |
| [credentials.py](credentials.py) | List, get, create, enable/disable, and delete Credentials; generate or rotate authentication tokens, OAuth client credentials, and access tokens |
| [cluster_config.py](cluster_config.py) | Read ClusterConfig and update session limits while preserving its other settings |
| [groups.py](groups.py) | List, get, create, update, and delete Groups and their Policy attachments |
| [namespaces.py](namespaces.py) | List, get, create, update, and delete Namespaces and their Policy attachments |

## Setup

Requires Python 3.11 or later. From the repository root:

```bash
python -m pip install ./packages/apis ./packages/sdk
cd packages/sdk/examples
export OCTELIUM_DOMAIN=example.com
export OCTELIUM_ACCESS_TOKEN='<administrator access token>'
```

Use a valid access token for an identity allowed to perform the Core API operations you invoke. If the token has scopes, it needs `api:core` or the corresponding method scopes. Each command creates and closes its own client. The [SDK guide](../README.md) also covers managed authentication sessions.

Replace resource names, email addresses, and upstream URLs with values appropriate for your Cluster. Every script supports `--help`, and every subcommand supports its own `--help`, such as `python users.py create --help`.

## Provision a CI workload and a protected Service

Create a workload identity called `ci-agent`:

```bash
python users.py create ci-agent --type workload --display-name 'CI agent'
```

Create an allow rule matching that identity, then attach its Policy to an HTTP Service in the existing `default` Namespace:

```bash
python policies.py create reports-access --match 'ctx.user.metadata.name == "ci-agent"'
python services.py create reports.default \
  --upstream http://reports.default.svc.cluster.local:8080 \
  --policies reports-access \
  --public
```

Create an authentication Credential for the workload, then generate its token. Creating the resource does not issue a token; `GenerateCredentialToken` is a separate API operation.

```bash
python credentials.py create ci-agent-token --user ci-agent --type auth-token --expires-hours 24
python credentials.py token ci-agent-token
```

The `token` command outputs secret material as JSON. Calling it again rotates the Credential's token and invalidates the previous token. Other commands output resource JSON without generating tokens. The example defaults an authentication Credential to one authentication; the resulting managed session can renew its access token using its refresh token.

Inspect the resources and switch the Service to a new backend:

```bash
python users.py list
python services.py list --namespace default
python policies.py get reports-access
python credentials.py list --user ci-agent
python services.py update reports.default --upstream http://reports-v2.default.svc.cluster.local:8080
```

Disable the workload, then remove the resources when they are no longer needed:

```bash
python users.py update ci-agent --disabled
python credentials.py delete ci-agent-token
python services.py delete reports.default
python policies.py delete reports-access
python users.py delete ci-agent
```

## Create and manage a human User

```bash
python users.py create alice --type human --email alice@example.com --display-name 'Alice'
python users.py get alice
python users.py update alice --email alice@new-domain.example --display-name 'Alice Smith'
python users.py update alice --disabled
python users.py update alice --no-disabled
python users.py delete alice
```

Human Users ordinarily authenticate through an IdentityProvider; creating a User does not create a password. Workload Users do not support an email address.

`users.py update NAME --groups engineers auditors` replaces the User's Group list; those Groups must already exist. `--groups` with no values clears the list.

## Edit an existing Policy rule

For an existing Policy, edit its named rule rather than replacing its whole specification:

```bash
python policies.py list
python policies.py get existing-policy
python policies.py update existing-policy --rule existing-rule \
  --match '"engineers" in ctx.user.spec.groups && ctx.service.metadata.name == "reports.production"'
python policies.py update existing-policy --rule existing-rule --effect deny
python policies.py update existing-policy --disabled
python policies.py delete existing-policy
```

Policies created by this example have a rule named `allow-access` by default. The update preserves other rules, enforcement rules, and attributes. Creating a standalone Policy does not attach it to any resource; attach it to a Service, User, Group, Namespace, Credential, or ClusterConfig as appropriate. Remove its references and child Policies before deleting it.

## Create OAuth and access-token Credentials

For an existing workload User named `existing-worker`:

```bash
python credentials.py create worker-oauth --user existing-worker --type oauth2 --expires-hours 48
python credentials.py token worker-oauth
python credentials.py create worker-access --user existing-worker --type access-token --expires-hours 24
python credentials.py token worker-access
python credentials.py get worker-oauth
python credentials.py update worker-oauth --disabled
python credentials.py update worker-oauth --no-disabled
python credentials.py delete worker-oauth
python credentials.py delete worker-access
```

OAuth and access-token Credentials require a WORKLOAD User. `--policies` optionally attaches existing Policies to the Credential. `--max-authentications` overrides the example's default: one for authentication tokens and zero, meaning unlimited, for OAuth and access-token Credentials. All Credential types in this example use clientless sessions and default to a 24-hour expiry.

## Read and update ClusterConfig

```bash
python cluster_config.py get
python cluster_config.py update --human-max-sessions 5 --workload-max-sessions 20
```

This changes `spec.session.human.maxPerUser` and `spec.session.workload.maxPerUser`. Either flag can be used alone. The example fetches the existing ClusterConfig, changes only the requested fields, then passes the complete resource to `UpdateClusterConfig`, preserving DNS, ingress, authorization, authentication, and other settings. Limits in this example are between 1 and the server's maximum of 1000.

## Manage Groups and Namespaces

For existing Policies named `team-access` and `production-access`:

```bash
python groups.py create engineers --policies team-access
python groups.py list
python groups.py get engineers
python groups.py update engineers --display-name 'Engineering team'
python groups.py update engineers --policies team-access production-access
python groups.py delete engineers
python namespaces.py create production --policies production-access
python namespaces.py list
python namespaces.py get production
python namespaces.py update production --display-name 'Production'
python namespaces.py update production --policies production-access team-access
python namespaces.py delete production
```

Group Policies apply to its members. Namespace Policies apply to its Services. These create examples attach at least one existing Policy. On update, `--policies` replaces the attachment list; passing the flag without values clears it. Remove Users from a Group before deleting it, and delete Services in a Namespace before deleting the Namespace.

All list functions follow pagination until `has_more` is false. All update functions fetch the existing resource before modifying selected fields because the Core API replaces the specification on update. This read/modify/update sequence is not an atomic patch; coordinate concurrent writers. API errors propagate instead of silently retrying creates or ignoring failed deletes.
