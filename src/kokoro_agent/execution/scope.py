"""GA 内部的 Run 定位值对象，不属于 DeepAgents native state。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from kokoro_agent.contract import ExecutionIdentity, RunRequest


def runtime_namespace(identity: ExecutionIdentity) -> str:
    """Derive GA's private isolation key from the trusted execution identity.

    The caller never chooses this value.  Canonical JSON keeps the derivation
    stable across workers while avoiding business identifiers in filesystem or
    Store keys.
    """
    # Namespace follows the stable resource owner, not the acting principal or
    # short-lived assertion. Actor/assertion remain on RunRequest for auth,
    # audit and billing, but rotating either must not fork memory/workspace.
    material = json.dumps(
        {
            "tenant_ref": identity.tenant_ref,
            "subject": identity.subject.model_dump(mode="json"),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"ga:{hashlib.sha256(material).hexdigest()[:40]}"


@dataclass(frozen=True, slots=True)
class RunScope:
    namespace: str
    session_id: str

    @classmethod
    def of(cls, request: RunRequest) -> RunScope:
        return cls(
            namespace=runtime_namespace(request.execution_identity),
            session_id=request.session_id,
        )

    @property
    def scoped_thread_id(self) -> str:
        # Session IDs are globally unique product IDs and are the only
        # checkpoint locator.  Namespace remains a GA Store/workspace scope.
        return self.session_id

__all__ = ["RunScope", "runtime_namespace"]
