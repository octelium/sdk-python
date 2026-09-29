# Development and release

The repository contains independent distributions:

- `packages/apis`: `octelium-apis`, generated betterproto messages and grpclib stubs.
- `packages/sdk`: the existing Octelium SDK.
- `packages/cordium`: `cordium-sdk`, the new typed sync/async Cordium package.

From the repository root with Python 3.11+:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
ruff check .
ruff format --check packages/cordium scripts
mypy
pytest
python -m build packages/apis
python -m build packages/cordium
python -m twine check packages/apis/dist/* packages/cordium/dist/*
```

Tests run local gRPC servers and a temporary POSIX shell runtime, including verified TLS with an ephemeral OpenSSL certificate. They do not access a live Cluster or require credentials. A Linux/macOS environment with `openssl`, `/bin/sh`, `head`, `cat`, and `base64` is needed for those integration tests. The SDK itself supports Python environments with asyncio, grpclib and HTTPX; the workspace file helpers require POSIX tools remotely.

## Generated protobuf bindings

The authoritative source is `pb/apis/protobuf/main/cordiumv1/cordiumv1.proto`; metadata is in `metav1.proto`. To regenerate from a local `pb` checkout, install `protoc` and run:

```bash
python scripts/generate_apis.py ~/pb
```

The script uses the pinned betterproto compiler and replaces only the Cordium and metadata generated modules. Other APIs and package metadata are preserved. Commit generated output alongside source-level SDK changes when preparing a release; no `protoc` binary is needed by SDK users. The SDK pins betterproto to the version used to generate these bindings because its 2.x compiler/runtime is currently a prerelease; upgrading it requires regeneration and compatibility tests.

## Sync facade and reference

The transport and resource logic is native asyncio. The blocking facade runs it on one owned loop thread. It has real, explicitly typed signatures, not dynamic attribute forwarding. When public async methods change, regenerate:

```bash
python scripts/generate_sync.py
python scripts/generate_reference.py
```

The generator copies signatures/docstrings and wraps returned resource handles, pages, and streams. Changes to wrapping rules belong in the generator; do not hand-edit `src/cordium/sync.py`. `help()`, editor completion, and PEP 561 consumers see the same methods for both client styles. The CI workflow verifies that generated facade/reference output is current.

## Transport and ownership

`Engine` owns lazy gRPC channels, operation deadlines, task cancellation, and error normalization. `AuthManager` shares session refresh without tying it to the first caller's cancellation. Exec owns a bidirectional RPC and bounded output queues. Event streams own a pump task so sequential blocking iterator calls do not move an async timeout context between tasks. Client close cancels high-level operations; externally supplied transports remain caller-owned.

No mutating operation is automatically retried. Update/modify operations preserve resource metadata for server conflict checks. Public async cancellation remains `asyncio.CancelledError`. Disk writes complete before their file descriptors are closed during cancellation. Remote resource deletion is always explicit.

## Release order and live validation

Version `cordium-sdk 0.1.0` depends on new snapshot/volume bindings in `octelium-apis 1.0.3`. Build and publish the APIs distribution before the SDK. Build both wheels, install them together into a clean environment outside the checkout, and smoke-test imports and type checking. Do not publish directly from an unreviewed working tree.

Before a public release, validate against a real Cordium Cluster: create/start/stop/delete, authentication policy, template builds, CSI snapshots/volume expansion, application routing, and PTY reconnection. The local suite checks protocol behavior but cannot establish infrastructure/backend compatibility or the Cluster's authorization policy.
