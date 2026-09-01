"""custom backend（ADR-010 BYO 扩展点）：`pkg.module:factory` 引用自带 BackendProtocol 实现。

企业/私有云不改本仓源码即可插入自己的沙箱：pip 装自己的包 + 两个 env。
工厂契约 sync（编排层 to_thread）；自由参数 yaml 原样透传（工厂自校验，本仓不猜形状）。
返回的 backend 若带 `sandbox_id` 属性即接入 run 级 ledger 记录（与 docker/e2b 同构）。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Protocol, runtime_checkable

import yaml
from deepagents.backends.protocol import BackendProtocol
from pydantic import BaseModel, ConfigDict, TypeAdapter

LOGGER = logging.getLogger("kokoro_agent.sandbox.custom")

# 外部 yaml 是不可信字典：TypeAdapter 洗净成 str 键自由域。
_CONFIG_ADAPTER: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])


class CustomBackendContext(BaseModel):
    """工厂唯一入参：本 run 的装配现场。config 是自由域（yaml 原文），strict 由工厂负责。"""

    model_config = ConfigDict(strict=True, frozen=True, arbitrary_types_allowed=True)

    run_id: str
    # 工作区键 {namespace}:{session_id}（文件面约定，与 local/s3 档同一词汇）。
    workspace: str
    # local 根（KOKORO_AGENT_LOCAL_SHELL_ROOT 下的 run 子目录）；未配置则 None。
    workspace_root: str | None
    # ledger 既往记录（HITL resume 现场）；工厂据此重连自己的沙箱而非新建。
    prior_sandbox_id: str | None
    config: dict[str, object]


class CustomBackendFactory(Protocol):
    def __call__(self, context: CustomBackendContext) -> BackendProtocol: ...


@runtime_checkable
class BoundSandbox(Protocol):
    """可选生命周期面：工厂产物带 sandbox_id 即由编排层落 ledger（keep-first）。"""

    @property
    def sandbox_id(self) -> str: ...


class CustomBackendSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    # `pkg.module:attribute` 引用；选 custom backend 时缺失即 fail-loud。
    factory_ref: str | None
    config_path: str | None


def load_custom_factory(ref: str) -> Callable[[CustomBackendContext], object]:
    """importlib 加载 + 可调用校验：坏引用在装配期爆炸，绝不静默降级。
    产物契约（须继承 BackendProtocol）由 connect_custom_sandbox 的 isinstance 收口。
    """
    module_path, _, attribute = ref.partition(":")
    if not module_path or not attribute:
        raise ValueError(f"custom backend ref must be 'pkg.module:attribute', got {ref!r}")
    factory: object = getattr(import_module(module_path), attribute)
    if not callable(factory):
        raise TypeError(f"custom backend factory {ref!r} is not callable")
    return factory


def load_custom_config(path: str | None) -> dict[str, object]:
    if path is None or path == "":
        return {}
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise TypeError(f"custom backend config must be a mapping, got {type(raw).__name__}")
    return _CONFIG_ADAPTER.validate_python(raw)


def connect_custom_sandbox(
    settings: CustomBackendSettings,
    *,
    run_id: str,
    workspace: str,
    workspace_root: str | None,
    prior_sandbox_id: str | None,
) -> BackendProtocol:
    if settings.factory_ref is None:
        raise ValueError("backend custom requires KOKORO_CUSTOM_BACKEND (pkg.module:factory)")
    factory = load_custom_factory(settings.factory_ref)
    context = CustomBackendContext(
        run_id=run_id,
        workspace=workspace,
        workspace_root=workspace_root,
        prior_sandbox_id=prior_sandbox_id,
        config=load_custom_config(settings.config_path),
    )
    backend = factory(context)
    # 契约：产物必须继承 deepagents BackendProtocol（ABC）——BYO 作者本就以它为基类实现。
    if not isinstance(backend, BackendProtocol):
        raise TypeError(
            f"custom backend factory {settings.factory_ref!r} returned "
            f"{type(backend).__name__}; it must subclass deepagents BackendProtocol"
        )
    return backend
