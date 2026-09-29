# Octelium Protobuf APIs

Generated, typed Python messages and asynchronous grpclib stubs for the Octelium and Cordium APIs, using betterproto.

```python
from octelium.api.main.cordium.v1 import Workspace, Volume, WorkspaceSnapshot
from octelium.api.main.meta.v1 import ObjectReference
```

For the ergonomic Cordium API, install the companion `cordium-sdk` distribution and import `cordium`.

Bindings are generated from the Octelium protobuf repository. See the repository's `scripts/generate_apis.py` for updating Cordium and shared metadata. No compiler is required at runtime.

Licensed under Apache-2.0.
