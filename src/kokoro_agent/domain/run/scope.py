"""GA 内部的 Run 定位值对象，不属于 DeepAgents native state。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from kokoro_agent.protocol import ExecutionIdentity, RunRequest


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
    checkpoint_run_id: str | None = None

    @classmethod
    def of(cls, request: RunRequest) -> RunScope:
        return cls(
            namespace=runtime_namespace(request.execution_identity),
            session_id=request.session_id,
            checkpoint_run_id=request.run_id
            if request.shared_conversation_context is not None
            else None,
        )

    @property
    def scoped_thread_id(self) -> str:
        # LangGraph's checkpoint tables have no tenant column.  Put the
        # trusted identity-derived namespace in the locator so a caller
        # supplied/reused session id can never select another tenant's state.
        if self.checkpoint_run_id is not None:
            material = json.dumps(
                (self.namespace, self.session_id, self.checkpoint_run_id),
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("ascii")
            # Separate prefix cannot collide with any legacy ga:<digest>:<session>.
            return f"ga-shared-run-v1-{hashlib.sha256(material).hexdigest()}"
        return f"{self.namespace}:{self.session_id}"


__all__ = ["RunScope", "runtime_namespace"]
