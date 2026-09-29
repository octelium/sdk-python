# Cordium Python SDK

Typed synchronous and native `asyncio` clients for [Cordium](https://github.com/octelium/cordium) workspaces. Python 3.11+.

The SDK covers workspaces, execution, files, persistent terminals, logs, events, templates and builds, snapshots, volumes, Spaces, Secrets, Git providers, memberships, regions, and configuration. Generated protobuf messages remain available for the full API.

## Installation

From this checkout:

```bash
python -m pip install ./packages/apis ./packages/cordium
```

The distribution is `cordium-sdk`; the import is `cordium`. It requires the accompanying `octelium-apis>=1.0.3` bindings. Both packages must be released before a registry-only install of this version will work.

```bash
export CORDIUM_DOMAIN=example.com
export OCTELIUM_AUTH_TOKEN='<your authentication token>'
```

The client connects to `octelium-api.<domain>:443` with verified TLS. Construction is lazy; the first request authenticates.

## Start with the blocking client

```python
from cordium import Cordium, Resources

with Cordium() as client:
    workspace = client.workspaces.run(
        image="python:3.13",
        resources=Resources(cpu=1000, memory=1024),
        ephemeral=True,
        timeout=300,
    )
    try:
        result = workspace.exec(["python", "-c", "print('Hello from Cordium')"])
        print(result.stdout)
        workspace.files.write_text("/workspace/hello.txt", "Hello!\n")
    finally:
        workspace.stop().wait_until_stopped()
        workspace.delete()
```

`create()` returns a stopped workspace. `run()` creates, starts, and waits for `RUNNING` under one total deadline. A failed `run()` leaves the workspace for diagnosis. A `WorkspaceFailureError` carries its resource in `.workspace`; other SDK errors expose `.code`, `.details`, and their underlying `__cause__` when available.

**Client context exit closes connections and cancels operations; it does not stop or delete workspaces.** Ephemeral workspaces discard storage on stop but retain their resource object until explicitly deleted.

All examples assume access to an existing Cordium Cluster and permission for the requested operations. No Cluster is provisioned by the SDK.

## Native async

```python
import asyncio
from cordium import AsyncCordium

async def main() -> None:
    async with AsyncCordium() as client:
        workspace = await client.workspaces.run(image="python:3.13")
        try:
            async with asyncio.TaskGroup() as group:
                version = group.create_task(workspace.exec(["python", "--version"]))
                platform = group.create_task(workspace.exec(["uname", "-a"]))
            print(version.result().stdout, platform.result().stdout)
        finally:
            await workspace.stop()
            await workspace.wait_until_stopped()
            await workspace.delete()

asyncio.run(main())
```

Async methods and their blocking counterparts use the same names, arguments, and result types. Async streams use `async for`, `async with`, and `aclose()`; blocking streams use `for`, `with`, and `close()`. `AsyncCordium` belongs to one event loop. `Cordium` owns one event-loop thread and supports calls from multiple threads. Create a new client after `fork`; do not share a client across processes. Use `AsyncCordium` in async applications so the application loop remains responsive.

## Configuration and references

```python
from cordium import Application, Ref, Resources, SecretRef, Task, VolumeMount

workspace = client.workspaces.create(
    template="python.team.cordium",
    display_name="data processing",
    env={"MODE": "development", "API_KEY": SecretRef("api-key")},
    vars={"branch": "main"},
    resources=Resources(cpu=2000, memory=2048, storage=10000),
    applications=[Application("api", 8000, default=True)],
    tasks=[Task(name="setup", command="python -m pip install -r requirements.txt")],
    volumes=[VolumeMount("data.team.cordium", "/data")],
)
workspace.start(region=Ref(uid="region-uid"), vars={"branch": "release"})
workspace.wait_until_running()
```

CPU uses **millicores**; memory and storage use **megabytes**, following the protobuf API. Omitted values inherit Cluster/template/Space policy. `SecretRef` resolves a Space Secret without exposing its value locally. Start-time variables and region selection apply only to that run.

Methods accepting a reference take a name string or `Ref(uid="...")`. Space-scoped resource names may be qualified, such as `python.team.cordium`. Workspace names are assigned by the Cluster. `template` and `snapshot` are exclusive; a snapshot restore cannot be ephemeral. Templates cannot declare workspace applications or ephemeral storage.

Convenience fields replace the corresponding fields of a copied `spec=` message. Lists replace, rather than append. `create_workspace_spec(...)` builds the same validated spec without network I/O. For advanced image sources, repository authentication, networking, capabilities, filesystem policy, devcontainer features, and additional repositories, supply generated messages through `spec=`. Source template/snapshot references are creation options, not persisted spec fields.

Workspace properties (`name`, `uid`, `state`, `hostname`, `url`, `spec`, `status`, `proto`, etc.) read a local cache. `refresh()`, lifecycle methods, and waits refresh it. Protobuf properties return deep copies. `wait_until_ready()` accepts `PREPARING`, when exec is available but setup is still running; `wait_until_running()` waits for setup to finish. Watch events do not silently mutate existing handles.

## Commands and streaming

A command string runs as a shell expression. A sequence of strings is safely quoted as literal argv. `argv(...)` and `shell_quote(...)` are available for composing commands. NUL bytes in shell arguments are rejected. `cwd=`, `env=`, and `root=True` configure execution.

```python
from cordium import ExecError

try:
    result = workspace.exec(["python", "script.py"], timeout=60)
except ExecError as error:
    print(error.result.exit_code, error.result.stderr)

with workspace.exec_stream("python -u server.py", timeout=60) as command:
    for event in command:
        print(event.stream, event.data)  # bytes; chunk boundaries need not be UTF-8 boundaries
    result = command.wait()
```

`exec()` defaults to `check=True`, raising `ExecError` for nonzero exit. `exec_stream()` defaults to `check=False`, allowing inspection of the exit status after streaming. `ExecResult` includes `exit_code`, `stdout_bytes`, `stderr_bytes`, `success`, `truncated`, and `killed`. Its `.stdout`/`.stderr` properties decode UTF-8 with replacement; use bytes for exact data.

Captures are limited to 1 MiB **per output stream** by default. Excess capture sets `truncated`; it does not stop output iteration. Stream queues default to 8 MiB and also have an event-count limit; a stalled consumer gets `RESOURCE_EXHAUSTED` instead of unbounded memory growth. Configure `max_capture_bytes` and `max_buffer_bytes` as needed. Calling `wait()` before output iteration selects drain-only mode. After iteration begins, continue consuming output while waiting for completion.

For interactive input, use `command.write(str_or_bytes)` and `command.kill()`. `stdin=` sends initial data. **The Exec protocol has no independent stdin EOF message.** A command reading until EOF can remain blocked; use explicit framing such as `head -c 4` when sending four bytes. The file helpers handle this framing automatically. Cancelling/closing an exec session cancels the remote command; `kill()` explicitly signals its process group.

Python does not close a custom iterator on `break`. Use a context manager around command, watch, log, and terminal streams whenever iteration can end early. A stream is single-consumer; concurrent reads are rejected. No background reconnect or replay is performed.

## Files

```python
workspace.files.write_bytes("/workspace/data.bin", b"\x00\xff")
text = workspace.files.read_text("/workspace/hello.txt")
workspace.files.upload("./local.zip", "/workspace/input.zip")
workspace.files.download("/workspace/output.zip", "./output.zip")
workspace.files.mkdir("/workspace/results", parents=True)
workspace.files.remove("/workspace/results", recursive=True)
```

`read_bytes`/`read_text` default to a 16 MiB limit and fail rather than silently truncate. Text decoding defaults to strict UTF-8; configure `encoding`/`errors` if needed. `upload` and `download` use bounded chunks; they do not buffer entire files. Downloads atomically replace the destination only on success, using a private file (mode `0600` on POSIX). Existing local files survive failed downloads. Remote writes truncate their destination and may leave partial data on failure. Parent directories must exist unless explicitly created with `mkdir()`.

Paths are quoted literally, including spaces and shell metacharacters; `~` and `$HOME` are not expanded. Helpers require standard POSIX `sh`, `head`, `cat`, `base64`, `mkdir`, and `rm` in the workspace. `root=True` is available on each operation. File upload detects size changes during transfer; callers should avoid editing a source during upload.

## Persistent terminals, logs, and events

```python
terminal = workspace.terminals.create(cols=120, rows=40)
try:
    with terminal:
        terminal.write("echo hello\n")
        with terminal.events as events:
            for event in events:
                if event.type == "output":
                    print(event.data)
                    break
finally:
    terminal.remove()
```

A terminal persists after detachment and may have multiple listeners. `close()`/`aclose()` and context exit detach locally; **only `remove()` terminates its shell**. Use `workspace.terminals.list()` and `.attach(id)` to reconnect. Output subscriptions begin lazily; creating a handle alone is not a guarantee that a listener has reached the server. Events distinguish binary output, resize, and close. `resize(cols, rows)` changes the PTY dimensions.

`workspace.logs()` streams typed initialization-stage records. `client.workspaces.watch()` observes the caller's workspaces; `workspace.watch()` restricts the subscription. They return generated messages, preserving timestamps, oneofs, and both old/new resources for updates. Watch supplies future events, not an initial list or a durable event log.

## Resource collections

| Collection | Operations |
| --- | --- |
| `workspaces` | create/run/get/list/all/update/delete/watch |
| `spaces` | create/get/list/all/update/modify/delete/leave |
| `templates` | create/get/list/all/update/modify/delete/build/cancel_build/wait_for_build |
| `snapshots` | create/get/list/all/delete/wait_until_ready |
| `volumes` | create/get/list/all/update/modify/delete/grow/wait_until_ready |
| `secrets` | create/get/list/all/delete |
| `user_secrets` | create/create_ssh_key/get/list/all/update/modify/set/delete |
| `git_providers` | create/create_oauth/create_oauth2/get/list/all/update/modify/delete |
| `memberships` | add/mine/get/list/all/update/modify/set_role/delete |
| `regions` | list/all |
| `user_config` | get/update/modify/set_preferred_region/set_dotfiles |
| `management` | get_cluster_config/update_cluster_config/modify_cluster_config |

Resource collections return documented generated protobuf messages. List methods return `Page` with `items`, `page`, `page_size`, `total_count`, and `has_more`. Pages start at zero. `page_size=0` uses the server default; `.all()` defaults to 100 and fetches lazily. Its timeout applies per page. Use the supported `space=`, `workspace=`, or `template=` filters; mutually exclusive filters are rejected. Server permissions always apply.

```python
with client.templates.all(space="team.cordium") as templates:
    for template in templates:
        print(template.metadata.name)

client.templates.create(
    "python.team.cordium", image="python:3.13",
    tasks=[Task(name="prepare", command="python -m pip install requests")],
)
started = client.templates.build("python.team.cordium", tags=("release",))
build_id = started.status.build_info.current_running_build_id
client.templates.wait_for_build("python.team.cordium", build_id)
```

Build waits use an explicit build ID so a previous success cannot satisfy a new build's wait. Snapshot creation is asynchronous; wait until ready before restoring via `workspaces.run(snapshot="...")`. Snapshots of running workspaces are crash-consistent; coordinate application writes for application-level consistency. Volume size is megabytes. `grow()` rejects shrinking below requested or observed capacity; expansion support depends on the backend. Some volumes become ready only after first attachment.

Secrets accept text, bytes, or JSON attributes. Structured numbers must be finite; integers outside ±2**53 are rejected. Secret values are write-only. `user_secrets.create_ssh_key()` asks the Cluster to generate a key pair; it does not upload a private key. OAuth Git providers reference a Space Secret **name** for `client_secret`.

For updates, fetch the resource, edit its spec, then pass it to `update()`, preserving metadata/version. `modify(ref, callback)` performs this sequence under one deadline and writes only if the synchronous callback succeeds. It does not retry conflicts or repeat callbacks. In the blocking client callbacks execute on the SDK's loop thread and must not call blocking methods on that same client. `workspace.modify(callback)` also refreshes the handle.

## Authentication, HTTP, errors, and deadlines

Explicit credentials override the environment:

```python
from cordium import AccessToken, Assertion, AssertionFile, AuthenticationToken, Cordium

client = Cordium("example.com", auth=AuthenticationToken("one-time-token"))
# Other options:
# AccessToken("externally-managed-token")
# AccessToken(callable_returning_current_token)  # sync or async callable
# Assertion(callable_returning_assertion, scopes=("scope",))
# AssertionFile("/var/run/secrets/workload/token")
```

Close the client when finished. Authentication tokens are attempted once, then the returned session is refreshed. Assertion files are reread for reauthentication. Concurrent callers share one session refresh; cancellation of one caller does not cancel authentication needed by other callers. Refresh has a 30-second internal deadline. Access-token providers run on each request and may be async; synchronous providers should avoid blocking I/O. Do not log tokens or returned `access_token()` values.

Environment precedence: `OCTELIUM_ACCESS_TOKEN`, `OCTELIUM_ASSERTION_FILE`, `OCTELIUM_ASSERTION`, then `OCTELIUM_AUTH_TOKEN` (or `OCTELIUM_AUTHENTICATION_TOKEN`). Domain precedence is explicit argument, `CORDIUM_DOMAIN`, then `OCTELIUM_DOMAIN`.

```python
response = client.request("GET", workspace.app_url("api") + "/health")
response.raise_for_status()
print(response.json())
```

`request()` attaches the current token only to HTTPS Cluster-domain/subdomain destinations or `authorized_http_hosts`. It supports underscore application routes. Redirects are returned without following. Userinfo and unauthorized destinations are rejected before authentication/network I/O. HTTP non-success statuses are returned for caller handling. Responses are buffered up to `max_response_bytes` (16 MiB default); deadlines include authentication and full-body reads. `allow_insecure_http=True` and `tls=False` explicitly enable insecure local testing. `host=`, `port=`, and an `ssl.SSLContext` support custom deployments.

Timeouts are positive finite **seconds**, with `None` disabling the deadline. Unary operations default to 30 seconds, readiness/run waits to 300 seconds, and exec/event streams to no deadline. Composite operations use one total budget. Local disk completion may delay cancellation until an in-flight write finishes safely. No automatic retries are performed for resource operations or executions; this avoids duplicate side effects. Errors preserve gRPC status names in `CordiumError.code`; SDK codes include `CLIENT_CLOSED`, `PROTOCOL_ERROR`, `WORKSPACE_STOPPED`, `WORKSPACE_FAILED`, `BUILD_FAILED`, `SNAPSHOT_FAILED`, `VOLUME_FAILED`, and `COMMAND_FAILED`. Async caller cancellation remains `asyncio.CancelledError`.

## Generated API escape hatch

```python
from cordium import meta, proto

# Native async client, inside its owning event loop:
resource = await async_client.raw.main.get_workspace(
    meta.GetOptions(name="abc"), timeout=10,
)

# Blocking client: callbacks run on its owned loop.
resource = client.call(
    lambda c: c.raw.main.get_workspace(meta.GetOptions(name="abc"), timeout=10)
)
```

`raw.main`, `raw.workspace`, and `raw.management` expose the generated stubs. Raw calls preserve native `grpclib` errors; callers own their deadlines and streaming cleanup. They share authentication with high-level methods. The async constructor accepts an already-authenticated `grpclib.Channel` or an `httpx.AsyncClient`; injected transports remain caller-owned. Token/HTTP helpers require SDK-managed credentials and cannot extract credentials from an injected gRPC channel.

See the [API reference](docs/api.md), [runnable examples](examples), and [development notes](docs/development.md). Every public method has a docstring available through `help()` and editor tooltips. Both packages carry PEP 561 typing markers.
