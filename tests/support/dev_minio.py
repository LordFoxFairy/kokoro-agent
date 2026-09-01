"""集成测试用的本机 minio 凭据解析(单一来源,勿在测试里写死密码)。

此前各测试写死 access/secret 为 kokoro/kokoro-secret,与真 dev infra 对不上 → 探测失败后
整组静默 skip:表面绿、S3 路径零真覆盖。改为取真源:显式 env 优先,否则读 gitignored 的
deploy/.env.dev(dev infra 的单一事实来源)。取不到即返回 None,由调用方出声 skip。
"""

from __future__ import annotations

import os
import re
from pathlib import Path

MINIO_URL = os.environ.get("KOKORO_TEST_MINIO_URL", "http://127.0.0.1:9100")

_ENV_DEV = Path(__file__).resolve().parents[2] / "deploy" / ".env.dev"


def _from_env_file(name: str) -> str | None:
    try:
        text = _ENV_DEV.read_text(encoding="utf-8")
    except OSError:
        return None
    match = re.search(rf"^{re.escape(name)}=(.*)$", text, re.MULTILINE)
    return match.group(1).strip() or None if match else None


def _resolve(name: str) -> str | None:
    return os.environ.get(name) or _from_env_file(name)


def minio_creds() -> tuple[str, str] | None:
    """(access_key, secret_key);任一缺失即 None——调用方须出声 skip,不得静默。"""
    access, secret = _resolve("MINIO_ROOT_USER"), _resolve("MINIO_ROOT_PASSWORD")
    if access is None or secret is None:
        return None
    return access, secret


SKIP_REASON = (
    f"minio {MINIO_URL} 凭据缺失或不可达"
    "(设 MINIO_ROOT_USER/MINIO_ROOT_PASSWORD,或备好 deploy/.env.dev)"
)
