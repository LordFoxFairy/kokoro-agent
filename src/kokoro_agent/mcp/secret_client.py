"""MCP 凭据句柄解析客户端：装配期经 Capability public contract 把 `handle:` 批换明文。

Capability 出口 `POST /capability/runtime/mcp/secrets/resolve {namespace, handles}`
→ `{data:{secrets:{handle:plaintext}}}`；caller=agent 凭据（出站头
`x-kokoro-service: agent` + `x-kokoro-internal-secret: <KOKORO_INTERNAL_SECRET_AGENT>`）。

明文绝不落库/落日志：解析结果只驻内存进 headers（authorization 整值语义同 env:）。Capability 侧
「全有或全无」——任一句柄跨 namespace/不存在返 404；本层把任何非 2xx 与网络/形状异常统一
视作该批失败（`SecretResolveError`），调用方据此把相关 server 占名不可用、不炸 run。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import httpx
from pydantic import BaseModel, ConfigDict, SecretStr, ValidationError

# 与 platform-kit http/route-access.ts / internal-secret-guard.ts 同名头；agent 是已知内部 caller。
_SERVICE_CALLER_HEADER = "x-kokoro-service"
_INTERNAL_SECRET_HEADER = "x-kokoro-internal-secret"
_AGENT_CALLER = "agent"
_RESOLVE_PATH = "/capability/runtime/mcp/secrets/resolve"
_TIMEOUT_S = 10.0


class SecretResolveError(Exception):
    """批解失败（网络 / 非 2xx / 响应形状异常）：绝不回显响应体（可能含错误细节）。"""


class SecretResolver(Protocol):
    async def resolve(self, namespace: str, handles: Sequence[str]) -> Mapping[str, str]: ...


class CapabilitySecretSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    base_url: str
    service_secret: SecretStr


class _ResolveData(BaseModel):
    # Capability 可能追加字段（requestId 在信封顶层）：只读 secrets，其余忽略。
    model_config = ConfigDict(strict=True, extra="ignore")

    secrets: dict[str, str]


class _ResolveResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    data: _ResolveData


class CapabilitySecretResolver:
    """Capability runtime caller 侧的凭据解析客户端（每次 run 至多一批）。"""

    def __init__(self, settings: CapabilitySecretSettings) -> None:
        self._base_url = settings.base_url.rstrip("/")
        self._secret = settings.service_secret

    async def resolve(self, namespace: str, handles: Sequence[str]) -> Mapping[str, str]:
        if not handles:
            return {}
        headers = {
            _SERVICE_CALLER_HEADER: _AGENT_CALLER,
            _INTERNAL_SECRET_HEADER: self._secret.get_secret_value(),
            "content-type": "application/json",
        }
        body = {"namespace": namespace, "handles": list(handles)}
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
                response = await client.post(
                    f"{self._base_url}{_RESOLVE_PATH}", json=body, headers=headers
                )
        except httpx.HTTPError as exc:
            raise SecretResolveError(f"secret resolve request failed: {type(exc).__name__}") from exc
        if response.status_code != 200:
            # 404(跨 namespace/不存在)/503(broker 未配)/其它：统一失败，不回显响应体。
            raise SecretResolveError(f"secret resolve returned HTTP {response.status_code}")
        try:
            parsed = _ResolveResponse.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise SecretResolveError("secret resolve response malformed") from exc
        return dict(parsed.data.secrets)


def capability_secret_resolver_from_env(env: Mapping[str, str]) -> CapabilitySecretResolver | None:
    """两项 env 齐备才装配（KOKORO_CAPABILITY_BASE_URL + KOKORO_INTERNAL_SECRET_AGENT）；
    缺任一 → None（`handle:` 引用无解析出口 → registry 侧占名不可用，不炸 run）。"""
    base_url = env.get("KOKORO_CAPABILITY_BASE_URL")
    secret = env.get("KOKORO_INTERNAL_SECRET_AGENT")
    if not base_url or not secret:
        return None
    return CapabilitySecretResolver(
        CapabilitySecretSettings(base_url=base_url, service_secret=SecretStr(secret))
    )


__all__ = [
    "CapabilitySecretResolver",
    "CapabilitySecretSettings",
    "SecretResolveError",
    "capability_secret_resolver_from_env",
]
