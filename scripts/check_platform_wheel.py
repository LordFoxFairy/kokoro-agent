from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _run(command: list[str]) -> None:
    subprocess.run(command, check=True, cwd=ROOT)


def _venv_executable(venv: Path, name: str) -> Path:
    candidates = [venv / "bin" / name, venv / "Scripts" / f"{name}.exe"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit(f"wheel smoke executable is missing: {name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve()
    if not wheel.is_file():
        raise SystemExit(f"wheel is missing: {wheel}")

    with tempfile.TemporaryDirectory(prefix="kokoro-agent-wheel-smoke-") as temporary:
        venv = Path(temporary) / "venv"
        _run(["uv", "venv", "--python", "3.11", str(venv)])
        python = _venv_executable(venv, "python")
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "connectrpc==0.12.1",
                "protobuf-py>=0.3.0,<0.4.0",
                "pydantic>=2",
                "rfc8785>=0.1.4",
            ]
        )
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "--no-deps",
                str(wheel),
            ]
        )
        script = """
from pathlib import Path
from kokoro_agent.execution import platform_request_binding
from kokoro_agent.generated import platform_request_projector
from kokoro_agent.generated.kokoro.common.v1 import common_pb
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_connect
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb

modules = (
    common_pb,
    platform_runtime_connect,
    platform_runtime_pb,
    platform_request_projector,
    platform_request_binding,
)
for module in modules:
    origin = Path(module.__file__).resolve()
    if "site-packages" not in origin.parts:
        raise SystemExit(f"generated module was not imported from installed wheel: {origin}")
assert common_pb.ExecutionIdentity
assert platform_runtime_pb.AuthorizeMcpToolRequest
assert platform_runtime_connect.McpAuthorizationServiceClient
request = platform_runtime_pb.GetMcpConnectorRequest(
    request_id="wheel-smoke",
    connector_id=platform_runtime_pb.McpConnectorId(value="connector-1"),
)
binding = platform_request_binding.project_request_binding(
    tenant_ref="tenant-1", request=request
)
assert binding.operation == "mcp.get_connector"
assert len(binding.sha256) == 64
"""
        _run([str(python), "-I", "-c", script])


if __name__ == "__main__":
    main()
