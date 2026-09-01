"""e2b backend 规格（ADR-009 1b）：生命周期编排（resume 重连/新建落账/keep-first）与
execute 映射。SDK 边界以 fake 注入验证我们的编排语义；SDK 真实行为待 key 真栈复核。
"""

from __future__ import annotations

from typing import Self

import pytest
from e2b import CommandExitException, SandboxException
from pydantic import SecretStr

from support.fakes import request
from kokoro_agent.sandbox.backend import SandboxSettings, make_backend_for_run
from kokoro_agent.sandbox.e2b_backend import E2BSandboxBackend, E2BSettings, connect_e2b_sandbox
from kokoro_agent.storage.ledger import RunLedger


def _e2b_settings(api_key: str | None = "e2b-key") -> E2BSettings:
    return E2BSettings(
        api_key=None if api_key is None else SecretStr(api_key),
        template=None,
        timeout=1800,
    )


def _dispatch_settings() -> SandboxSettings:
    return SandboxSettings.model_validate(
        {
            "local_shell_root": None,
            "local_shell_inherit_env": False,
            "local_shell_timeout": 30,
            "local_shell_max_output_bytes": 100000,
            "workspace": None,
            "workspace_s3_access_key": None,
            "workspace_s3_secret_key": None,
            "e2b": _e2b_settings(),
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": None, "config_path": None},
        }
    )


class FakeCommandResult:
    def __init__(self, stdout: str, stderr: str, exit_code: int) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code


class FakeCommands:
    def __init__(self, owner: FakeSandbox) -> None:
        self._owner = owner

    def run(
        self, cmd: str, *, cwd: str | None = None, timeout: int | None = None
    ) -> FakeCommandResult:
        self._owner.commands_run.append((cmd, cwd))
        if self._owner.fail_next_command is not None:
            failure = self._owner.fail_next_command
            self._owner.fail_next_command = None
            raise failure
        return FakeCommandResult("ok\n", "", 0)


class FakeFiles:
    def write(self, path: str, data: str | bytes) -> object:
        return None

    def read(self, path: str, format: str = "text") -> object:
        raise SandboxException("not found")


class FakeSandbox:
    """镜像 e2b 2.30 调用面：create / 构造+connect / commands.run / sandbox_id。"""

    created: list[FakeSandbox] = []
    connect_should_fail = False

    def __init__(self, sandbox_id: str = "sbx_new", api_key: str | None = None, **_: object) -> None:
        self.sandbox_id = sandbox_id
        self.api_key = api_key
        self.commands_run: list[tuple[str, str | None]] = []
        self.fail_next_command: Exception | None = None
        self.commands = FakeCommands(self)
        self.files = FakeFiles()

    @classmethod
    def create(cls, template: str | None = None, timeout: int | None = None, **opts: object) -> Self:
        instance = cls(sandbox_id=f"sbx_created_{len(cls.created)}", api_key=str(opts.get("api_key")))
        cls.created.append(instance)
        return instance

    @classmethod
    def connect(cls, sandbox_id: str, timeout: int | None = None, **opts: object) -> Self:
        # 镜像 e2b 类形态 connect：按 id 重连，paused 自动 resume。
        if cls.connect_should_fail:
            raise SandboxException("sandbox gone")
        return cls(sandbox_id=sandbox_id, api_key=str(opts.get("api_key")))


@pytest.fixture(autouse=True)
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeSandbox.created = []
    FakeSandbox.connect_should_fail = False
    monkeypatch.setattr("kokoro_agent.sandbox.e2b_backend.Sandbox", FakeSandbox)


class TestLifecycle:
    def test_no_api_key_fail_loud(self) -> None:
        with pytest.raises(ValueError, match="KOKORO_E2B_API_KEY"):
            connect_e2b_sandbox(_e2b_settings(api_key=None), sandbox_id=None)

    def test_fresh_run_creates_sandbox_with_key(self) -> None:
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id=None)
        assert backend.id == "sbx_created_0"
        assert FakeSandbox.created[0].api_key == "e2b-key"

    def test_resume_reconnects_existing_sandbox_not_create(self) -> None:
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id="sbx_prior")
        assert backend.id == "sbx_prior"
        assert FakeSandbox.created == []  # 关键语义：resume 绝不新建（暂停期文件在箱内）

    def test_resume_falls_back_to_create_when_sandbox_gone(self) -> None:
        FakeSandbox.connect_should_fail = True
        backend = connect_e2b_sandbox(_e2b_settings(), sandbox_id="sbx_expired")
        assert backend.id == "sbx_created_0"

    @pytest.mark.asyncio
    async def test_run_scoped_binding_new_sandbox_and_reuse(self, ledger: RunLedger) -> None:
        # 生产路径：run 先被认领（建 run 文档），箱绑定才落账。
        await ledger.try_claim(request("run_1"), "owner")
        first = await make_backend_for_run(
            "e2b", _dispatch_settings(), workspace="ns:s1", run_id="run_1", sandbox_store=ledger
        )
        assert isinstance(first, E2BSandboxBackend)
        assert await ledger.get_sandbox_id("run_1") == first.id
        # HITL resume：重建 backend 走重连，箱不重建、绑定不被覆盖（keep-first）。
        second = await make_backend_for_run(
            "e2b", _dispatch_settings(), workspace="ns:s1", run_id="run_1", sandbox_store=ledger
        )
        assert isinstance(second, E2BSandboxBackend)
        assert second.id == first.id
        assert len(FakeSandbox.created) == 1


class TestExecuteMapping:
    def test_zero_exit_maps_stdout(self) -> None:
        backend = E2BSandboxBackend(FakeSandbox(sandbox_id="sbx_x"))
        result = backend.execute("echo ok")
        assert (result.output, result.exit_code, result.truncated) == ("ok\n", 0, False)
        assert backend.id == "sbx_x"

    def test_nonzero_exit_is_result_not_exception(self) -> None:
        fake = FakeSandbox(sandbox_id="sbx_x")
        fake.fail_next_command = CommandExitException(
            stdout="partial", stderr="boom", exit_code=2, error=""
        )
        result = E2BSandboxBackend(fake).execute("false")
        assert result.exit_code == 2
        assert "boom" in result.output

    def test_download_missing_file_maps_error(self) -> None:
        response = E2BSandboxBackend(FakeSandbox()).download_files(["/ghost.md"])[0]
        assert (response.path, response.content, response.error) == (
            "/ghost.md", None, "file_not_found",
        )
