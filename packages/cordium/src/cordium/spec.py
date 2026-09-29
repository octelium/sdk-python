"""Pythonic workspace configuration; generated messages remain an escape hatch."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, TypedDict, Unpack, cast

from octelium.api.main.cordium import v1 as p

from .errors import integer, nonempty
from .models import Reference, reference


@dataclass(frozen=True, slots=True, kw_only=True)
class Resources:
    """Compute allocation: CPU millicores, memory megabytes, storage megabytes.

    Omit a dimension to inherit the server's policy. Values must be positive integers.
    """

    cpu: int | None = None
    memory: int | None = None
    storage: int | None = None


@dataclass(frozen=True, slots=True)
class SecretRef:
    """Source an environment value from the named Secret in the workspace's Space."""

    name: str


@dataclass(frozen=True, slots=True)
class Application:
    """Named portal application: name, TCP port, optional label, and default-host selection."""

    name: str
    port: int
    display_name: str = ""
    default: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class Task:
    """Lifecycle command with a unique name, stage, environment, and failure behavior.

    ``on`` is create/start/stop; background tasks do not hold up readiness.
    ``cwd`` selects the container working directory; ``root`` overrides its user.
    """

    name: str
    command: str
    on: Literal["create", "start", "stop"] = "create"
    env: Mapping[str, str] | None = None
    cwd: str = ""
    background: bool = False
    root: bool = False
    on_failure: Literal["abort", "continue"] | None = None


@dataclass(frozen=True, slots=True)
class VolumeMount:
    """Mount a Space volume at an absolute container path, optionally read-only."""

    volume: Reference
    path: str
    read_only: bool = False


class WorkspaceOptions(TypedDict, total=False):
    """Keyword options accepted by create/run and create_workspace_spec.

    Convenience fields replace corresponding fields of a copied ``spec``;
    unspecified fields are inherited. Arrays replace, rather than append.
    """

    display_name: str
    """Optional human-readable workspace label."""
    template: Reference
    """Template to inherit; mutually exclusive with snapshot."""
    snapshot: Reference
    """Snapshot to restore; cannot be ephemeral."""
    image: str | p.WorkspaceSpecImage
    """Registry image string or a generated image source (Dockerfile/git/devcontainer)."""
    repository: str | p.WorkspaceSpecRepository
    """HTTPS clone URL or repository with clone/authentication options."""
    env: Mapping[str, str | SecretRef]
    """Container environment values or Space Secret references."""
    vars: Mapping[str, str]
    """Cordium substitution variables."""
    resources: Resources
    """CPU, memory and storage allocation."""
    ephemeral: bool
    """Discard storage on stop; does not delete the workspace object."""
    applications: Sequence[Application]
    """Portal applications; at most one may be default."""
    tasks: Sequence[Task]
    """Lifecycle commands."""
    volumes: Sequence[VolumeMount]
    """Persistent volume attachments."""
    disable_timeout: bool
    """Disable inactivity timeout if Cluster policy permits."""
    auto_stop: bool
    """Stop when all foreground lifecycle tasks finish."""
    spec: p.WorkspaceSpec
    """Full generated spec for capabilities, networking, features and future fields."""


def limits(value: Resources) -> p.WorkspaceSpecLimit:
    result = p.WorkspaceSpecLimit()
    if value.cpu is not None:
        result.cpu = p.WorkspaceSpecLimitCpu(millicores=integer(value.cpu, "cpu", 1))
    if value.memory is not None:
        result.memory = p.WorkspaceSpecLimitMemory(megabytes=integer(value.memory, "memory", 1))
    if value.storage is not None:
        result.storage = p.WorkspaceSpecLimitStorage(megabytes=integer(value.storage, "storage", 1))
    return result


def environment(values: Mapping[str, str | SecretRef]) -> list[p.WorkspaceSpecRuntimeEnvVar]:
    return [
        p.WorkspaceSpecRuntimeEnvVar(
            key=nonempty(key, "Environment key"), from_secret=nonempty(value.name, "Secret")
        )
        if isinstance(value, SecretRef)
        else p.WorkspaceSpecRuntimeEnvVar(key=nonempty(key, "Environment key"), value=value)
        for key, value in values.items()
    ]


def variables(values: Mapping[str, str]) -> list[p.WorkspaceSpecVar]:
    return [
        p.WorkspaceSpecVar(name=nonempty(name, "Variable"), value=value)
        for name, value in values.items()
    ]


def create_workspace_spec(**options: Unpack[WorkspaceOptions]) -> p.WorkspaceSpec:
    """Build a validated, independent protobuf spec without making a network request."""
    spec = copy.deepcopy(options.get("spec", p.WorkspaceSpec()))
    if "template" in options and "snapshot" in options:
        raise ValueError("template and snapshot are mutually exclusive")
    if "image" in options:
        image = options["image"]
        spec.image = (
            p.WorkspaceSpecImage(
                registry=p.WorkspaceSpecImageRegistry(url=nonempty(image, "Image"))
            )
            if isinstance(image, str)
            else copy.deepcopy(image)
        )
    if "repository" in options:
        repo = options["repository"]
        spec.repository = (
            p.WorkspaceSpecRepository(url=nonempty(repo, "Repository"))
            if isinstance(repo, str)
            else copy.deepcopy(repo)
        )
    if "env" in options:
        spec.runtime.env_vars = environment(options["env"])
    if "vars" in options:
        spec.vars = variables(options["vars"])
    if "resources" in options:
        spec.limit = limits(options["resources"])
    if "ephemeral" in options:
        spec.is_ephemeral = options["ephemeral"]
    if "snapshot" in options and spec.is_ephemeral:
        raise ValueError("Snapshot restores cannot be ephemeral")
    if "applications" in options:
        names: set[str] = set()
        apps = []
        for app in options["applications"]:
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", app.name) or app.name in names:
                raise ValueError("Application names must be unique lowercase hostname labels")
            names.add(app.name)
            apps.append(
                p.WorkspaceSpecApplication(
                    name=app.name,
                    port=integer(app.port, "port", 1, 65535),
                    display_name=app.display_name,
                    is_default=app.default,
                )
            )
        if sum(app.is_default for app in apps) > 1:
            raise ValueError("Only one application can be default")
        spec.applications = apps
    if "tasks" in options:
        names = set()
        tasks = []
        for task in options["tasks"]:
            nonempty(task.name, "Task name")
            if task.name in names:
                raise ValueError("Task names must be unique")
            names.add(task.name)
            stages = {
                "create": p.WorkspaceSpecRuntimeTaskType.ON_CREATE,
                "start": p.WorkspaceSpecRuntimeTaskType.POST_START,
                "stop": p.WorkspaceSpecRuntimeTaskType.PRE_STOP,
            }
            if task.on not in stages or task.on_failure not in (None, "abort", "continue"):
                raise ValueError("Invalid task stage or failure policy")
            tasks.append(
                p.WorkspaceSpecRuntimeTask(
                    name=task.name,
                    run=nonempty(task.command, "Task command"),
                    type=cast(p.WorkspaceSpecRuntimeTaskType, stages[task.on]),
                    working_dir=task.cwd,
                    env_vars=[
                        p.WorkspaceSpecRuntimeTaskEnvVar(key=k, value=v)
                        for k, v in (task.env or {}).items()
                    ],
                    is_background=task.background,
                    run_as_root=task.root,
                    on_failure=cast(
                        p.WorkspaceSpecRuntimeTaskOnFailure,
                        p.WorkspaceSpecRuntimeTaskOnFailure.ON_FAILURE_ABORT
                        if task.on_failure == "abort"
                        else p.WorkspaceSpecRuntimeTaskOnFailure.ON_FAILURE_CONTINUE
                        if task.on_failure == "continue"
                        else p.WorkspaceSpecRuntimeTaskOnFailure.ON_FAILURE_UNSET,
                    ),
                )
            )
        spec.runtime.tasks = tasks
    if "volumes" in options:
        mounts = []
        for mount in options["volumes"]:
            if (
                not mount.path.startswith("/")
                or mount.path == "/"
                or any(part in (".", "..") for part in mount.path.split("/"))
            ):
                raise ValueError("Mount paths must be absolute, canonical, and not /")
            mounts.append(
                p.WorkspaceSpecRuntimeVolumeMount(
                    volume_ref=reference(mount.volume),
                    mount_path=nonempty(mount.path, "Mount path"),
                    read_only=mount.read_only,
                )
            )
        spec.runtime.volume_mounts = mounts
    if "disable_timeout" in options:
        spec.runtime.timeout = p.WorkspaceSpecRuntimeTimeout(
            mode=cast(
                p.WorkspaceSpecRuntimeTimeoutMode,
                p.WorkspaceSpecRuntimeTimeoutMode.DISABLED
                if options["disable_timeout"]
                else p.WorkspaceSpecRuntimeTimeoutMode.DEFAULT,
            )
        )
    if "auto_stop" in options:
        spec.runtime.auto_stop = options["auto_stop"]
    return spec
