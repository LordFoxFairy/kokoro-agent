"""Agent-owned workspace naming rules.

The workspace key is an Agent execution concern. It is not a Storage service
contract and must not be imported from a cross-repository schema package.
"""

from __future__ import annotations

WORKSPACE_KEY_TEMPLATE = "{namespace}:{session_id}"


def workspace_key(namespace: str, session_id: str) -> str:
    """Return the stable key used by the Agent's local/S3 workspace archive."""
    return WORKSPACE_KEY_TEMPLATE.format(namespace=namespace, session_id=session_id)
