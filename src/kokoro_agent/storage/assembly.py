"""Immutable execution-assembly binding errors."""


class AssemblyDigestConflict(RuntimeError):
    def __init__(self) -> None:
        super().__init__("AGENT_ASSEMBLY_DIGEST_CONFLICT")
