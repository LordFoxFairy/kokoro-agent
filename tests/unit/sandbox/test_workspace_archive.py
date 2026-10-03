"""workspace S3 归档规格（ADR-009）：type 判别配置 + 写时归档 + execute 全量兜底。

配置与 backend 构造默认纯测；真实归档仅由显式 integration fixture 持有资源。
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from threading import Event
from typing import Literal

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from deepagents.backends.protocol import BackendProtocol
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr, ValidationError

from kokoro_agent.sandbox import load_workspace_config, make_backend
from kokoro_agent.sandbox.archive import (
    ArchivingLocalShellBackend,
    S3Archiver,
    S3Workspace,
)
import kokoro_agent.sandbox.archive as archive_module
import kokoro_agent.sandbox.backend as backend_module
from kokoro_agent.sandbox.backend import SandboxSettings
from kokoro_agent.sandbox.backend import make_backend_for_run
from kokoro_agent.sandbox.docker_backend import DockerShellBackend
from support.fakes import FakeRunRepository, request
from support.dev_minio import MINIO_URL, minio_creds

_TEST_ENDPOINT = "http://127.0.0.1:1"
_TEST_BUCKET = "kokoro-agent-archive-construction"
_TEST_ACCESS = "archive-test-access"
_TEST_SECRET = "archive-test-secret"


def _sandbox_settings(root: str | None, workspace: object = None) -> SandboxSettings:
    return SandboxSettings.model_validate(
        {
            "local_shell_root": root,
            "local_shell_inherit_env": False,
            "local_shell_timeout": 30,
            "local_shell_max_output_bytes": 100000,
            "workspace": workspace,
            "workspace_s3_access_key": SecretStr(_TEST_ACCESS) if workspace else None,
            "workspace_s3_secret_key": SecretStr(_TEST_SECRET) if workspace else None,
            "e2b": {"api_key": None, "template": None, "timeout": 1800},
            "docker": {"image": None, "ttl": 1800},
            "custom": {"factory_ref": None, "config_path": None},
        }
    )


def _attempt_cleanup(
    operation: Callable[[], object], errors: list[BaseException]
) -> None:
    try:
        operation()
    except BaseException as error:
        errors.append(error)


@pytest.fixture
def tracked_s3_clients(
    monkeypatch: pytest.MonkeyPatch, record_property: Callable[[str, object], None]
) -> Iterator[list[S3Client]]:
    original_factory = boto3.client
    clients: list[S3Client] = []

    def create_client(
        service_name: Literal["s3"],
        *,
        endpoint_url: str,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        config: BotoConfig,
    ) -> S3Client:
        client = original_factory(
            service_name,
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config,
        )
        clients.append(client)
        return client

    monkeypatch.setattr(boto3, "client", create_client)
    try:
        yield clients
    finally:
        errors: list[BaseException] = []
        for client in reversed(clients):
            _attempt_cleanup(client.close, errors)
        record_property("archive_clients_created", len(clients))
        record_property("archive_client_close_attempts", len(clients))
        record_property("archive_client_close_failures", len(errors))
        # Pytest keeps a test-body failure alongside any fixture teardown error.
        if errors:
            raise BaseExceptionGroup("Archive test client cleanup failed", errors)


@dataclass(frozen=True)
class _OwnedArchive:
    client: S3Client
    workspace: S3Workspace
    access_key: SecretStr
    secret_key: SecretStr
    created: bool


def _is_missing(error: ClientError, codes: set[str]) -> bool:
    return (
        error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
        and error.response.get("Error", {}).get("Code") in codes
    )


def _require_bucket_absent(client: S3Client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if _is_missing(error, {"404", "NoSuchBucket", "NotFound"}):
            return
        raise
    raise RuntimeError("Archive test bucket must be absent")


def _cleanup_bucket(client: S3Client, bucket: str) -> list[BaseException]:
    errors: list[BaseException] = []

    def remove_current() -> None:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key is None:
                    raise RuntimeError("Archive object listing omitted its key")
                _attempt_cleanup(
                    partial(client.delete_object, Bucket=bucket, Key=key), errors
                )

    def remove_versions() -> None:
        for page in client.get_paginator("list_object_versions").paginate(
            Bucket=bucket
        ):
            for item in [*page.get("Versions", []), *page.get("DeleteMarkers", [])]:
                key, version = item.get("Key"), item.get("VersionId")
                if key is None or version is None:
                    raise RuntimeError("Archive version listing omitted its identity")
                _attempt_cleanup(
                    partial(
                        client.delete_object, Bucket=bucket, Key=key, VersionId=version
                    ),
                    errors,
                )

    _attempt_cleanup(remove_current, errors)
    _attempt_cleanup(remove_versions, errors)
    _attempt_cleanup(lambda: client.delete_bucket(Bucket=bucket), errors)
    _attempt_cleanup(lambda: _require_bucket_absent(client, bucket), errors)
    return errors


@pytest.fixture
def owned_archive(tracked_s3_clients: list[S3Client]) -> Iterator[_OwnedArchive]:
    # The explicit dependency keeps every client alive until bucket teardown ends.
    del tracked_s3_clients
    credentials = minio_creds()
    if not MINIO_URL or credentials is None:
        raise RuntimeError(
            "Archive integration requires MinIO endpoint and credentials"
        )
    access, secret = credentials
    client: S3Client = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        region_name="us-east-1",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=BotoConfig(
            s3={"addressing_style": "path"},
            connect_timeout=1,
            read_timeout=2,
            retries={"max_attempts": 1},
        ),
    )
    bucket = f"kokoro-agent-archive-{uuid.uuid4().hex}"
    created = False
    primary: BaseException | None = None
    try:
        _require_bucket_absent(client, bucket)
        client.create_bucket(Bucket=bucket)
        created = True
        yield _OwnedArchive(
            client=client,
            workspace=S3Workspace(type="s3", endpoint=MINIO_URL, bucket=bucket),
            access_key=SecretStr(access),
            secret_key=SecretStr(secret),
            created=created,
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        if created:
            errors = _cleanup_bucket(client, bucket)
            if errors:
                if primary is not None:
                    errors.insert(0, primary)
                raise BaseExceptionGroup("Archive test bucket cleanup failed", errors)


class TestWorkspaceConfig:
    def test_missing_env_means_local_default(self) -> None:
        assert load_workspace_config(None) is None
        assert load_workspace_config("") is None

    def test_s3_defaults(self, tmp_path: Path) -> None:
        file = tmp_path / "ws.yaml"
        file.write_text("workspace:\n  type: s3\n  endpoint: http://x\n  bucket: b\n")
        config = load_workspace_config(str(file))
        assert isinstance(config, S3Workspace)
        assert (config.region, config.force_path_style) == ("us-east-1", True)

    def test_s3_supports_non_minio_compatible_endpoint(self, tmp_path: Path) -> None:
        file = tmp_path / "ws.yaml"
        file.write_text(
            "workspace:\n"
            "  type: s3\n"
            "  endpoint: https://s3.us-east-1.amazonaws.com\n"
            "  bucket: workspace-prod\n"
            "  region: us-east-1\n"
            "  force_path_style: false\n"
        )
        config = load_workspace_config(str(file))
        assert isinstance(config, S3Workspace)
        assert config.endpoint == "https://s3.us-east-1.amazonaws.com"
        assert config.force_path_style is False

    @pytest.mark.parametrize(
        "content",
        [
            "workspace:\n  type: gcs\n  bucket: x\n",
            "workspace:\n  type: s3\n  endpoint: http://x\n",
            "workspace:\n  type: local\n",
            "workspace:\n  type: local\n  root: /a\n  evil: true\n",
            "storage:\n  type: local\n",
            "workspace:\n  type: local\n  root: /a\nskill_packages:\n  type: local\n  root: /b\n",
            "workspace:\n  type: local\n  root: /a\ndeliveries:\n  type: local\n  root: /b\n",
            "workspace: 42\n",
        ],
    )
    def test_fail_loud_on_bad_shape(self, tmp_path: Path, content: str) -> None:
        file = tmp_path / "ws.yaml"
        file.write_text(content)
        with pytest.raises(ValidationError):
            load_workspace_config(str(file))

    def test_missing_file_fail_loud(self) -> None:
        with pytest.raises(OSError):
            load_workspace_config("/nonexistent/ws.yaml")

    def test_s3_without_credentials_fail_loud(self) -> None:
        with pytest.raises(ValidationError, match="ACCESS_KEY"):
            SandboxSettings.model_validate(
                {
                    "local_shell_root": "/tmp",
                    "local_shell_inherit_env": False,
                    "local_shell_timeout": 30,
                    "local_shell_max_output_bytes": 100000,
                    "workspace": {"type": "s3", "endpoint": "http://x", "bucket": "b"},
                    "workspace_s3_access_key": None,
                    "workspace_s3_secret_key": None,
                    "e2b": {"api_key": None, "template": None, "timeout": 1800},
                    "docker": {"image": None, "ttl": 1800},
                    "custom": {"factory_ref": None, "config_path": None},
                }
            )


async def test_cancelled_archive_operation_remains_owned_until_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Caller cancellation cannot detach the blocking upload or close beneath it."""
    entered = Event()
    release = Event()
    order: list[str] = []

    class BlockingClient:
        close_calls = 0

        def upload_file(self, local: str, bucket: str, key: str) -> None:
            assert Path(local).is_file()
            assert (bucket, key) == ("owned", "tenant:session/proof.txt")
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test did not release archive upload")
            order.append("upload-finished")

        def close(self) -> None:
            self.close_calls += 1
            order.append("client-closed")

    client = BlockingClient()

    def blocking_client(*args: object, **kwargs: object) -> BlockingClient:
        return client

    monkeypatch.setattr(archive_module.boto3, "client", blocking_client)
    backend = make_backend(
        "local_shell",
        _sandbox_settings(
            str(tmp_path),
            S3Workspace(type="s3", endpoint="http://s3.invalid", bucket="owned"),
        ),
        workspace="tenant:session",
    )
    assert isinstance(backend, ArchivingLocalShellBackend)
    write = asyncio.create_task(backend.awrite("/proof.txt", "owned bytes"))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        write.cancel()
        with pytest.raises(asyncio.CancelledError):
            await write

        first_close = asyncio.create_task(backend.aclose_resources())
        await asyncio.sleep(0)
        assert not first_close.done()
        assert client.close_calls == 0
        with pytest.raises(RuntimeError, match="closing"):
            await backend.awrite("/late.txt", "late")

        # Repeated cancellation only cancels waiters; the owned close task remains.
        first_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        second_close = asyncio.create_task(backend.aclose_resources())
        await asyncio.sleep(0)
        second_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second_close

        release.set()
        await backend.aclose_resources()
        await backend.aclose_resources()
        assert order == ["upload-finished", "client-closed"]
        assert client.close_calls == 1
    finally:
        release.set()
        await backend.aclose_resources()


async def test_cancelled_connector_waits_for_thread_and_closes_returned_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A cancelled construction remains its thread's owner until the result is closed."""
    entered = Event()
    release = Event()

    class RecordingClient:
        close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    client = RecordingClient()

    def recording_client(*args: object, **kwargs: object) -> RecordingClient:
        return client

    monkeypatch.setattr(archive_module.boto3, "client", recording_client)
    settings = _sandbox_settings(
        str(tmp_path),
        S3Workspace(type="s3", endpoint="http://s3.invalid", bucket="owned"),
    )

    def blocking_connector(
        context: backend_module.SandboxContext,
    ) -> BackendProtocol | None:
        entered.set()
        if not release.wait(5):
            raise TimeoutError("test did not release connector")
        return make_backend(
            "local_shell", context.settings, workspace=context.workspace
        )

    monkeypatch.setattr(
        "kokoro_agent.sandbox.backend._CONNECTORS",
        {"local_shell": blocking_connector},
    )
    store = FakeRunRepository()
    run = request("cancelled-connector")
    lease = await store.try_claim(run)
    assert lease is not None
    construction = asyncio.create_task(
        make_backend_for_run(
            "local_shell",
            settings,
            workspace="tenant:session",
            run_id=run.run_id,
            lease=lease,
            sandbox_store=store,
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        construction.cancel()
        await asyncio.sleep(0)
        construction.cancel()
        assert not construction.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await construction
        assert client.close_calls == 1
    finally:
        release.set()


@pytest.mark.parametrize(
    ("prior_sandbox_id", "expected_destroyed"),
    [(None, ["docker-created-by-attempt"]), ("docker-prior", [])],
)
async def test_docker_archive_wrapper_failure_closes_client_and_only_new_container(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    prior_sandbox_id: str | None,
    expected_destroyed: list[str],
) -> None:
    close_calls = 0
    destroyed: list[str] = []
    primary = RuntimeError("archive-wrapper-primary")

    class RecordingClient:
        def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    client = RecordingClient()

    def recording_client(*_args: object, **_kwargs: object) -> RecordingClient:
        return client

    container_id = prior_sandbox_id or "docker-created-by-attempt"

    def connect_fixture(*_args: object, **_kwargs: object) -> BackendProtocol:
        return DockerShellBackend(
            root=tmp_path,
            container_id=container_id,
            timeout=30,
            max_output_bytes=100000,
        )

    def fail_wrapper(**_kwargs: object) -> BackendProtocol:
        raise primary

    def record_destroy(sandbox_id: str) -> None:
        destroyed.append(sandbox_id)

    monkeypatch.setattr(archive_module.boto3, "client", recording_client)
    monkeypatch.setattr(backend_module, "connect_docker_sandbox", connect_fixture)
    monkeypatch.setattr(backend_module, "ArchivingDockerShellBackend", fail_wrapper)
    monkeypatch.setattr(
        backend_module,
        "destroy_docker_sandbox",
        record_destroy,
    )
    settings = _sandbox_settings(
        str(tmp_path),
        S3Workspace(type="s3", endpoint="http://s3.invalid", bucket="owned"),
    ).model_copy(
        update={
            "docker": _sandbox_settings(str(tmp_path)).docker.model_copy(
                update={"image": "fixture"}
            )
        }
    )
    store = FakeRunRepository()
    run = request("docker-wrapper-failure-" + (prior_sandbox_id or "new"))
    lease = await store.try_claim(run)
    assert lease is not None
    if prior_sandbox_id is not None:
        store.sandbox_ids[run.run_id] = prior_sandbox_id

    with pytest.raises(RuntimeError) as captured:
        await make_backend_for_run(
            "docker",
            settings,
            workspace="tenant:session",
            run_id=run.run_id,
            lease=lease,
            sandbox_store=store,
        )

    assert captured.value is primary
    assert (close_calls, destroyed) == (1, expected_destroyed)


async def test_docker_archive_wrapper_cleanup_failures_preserve_primary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    close_calls = 0
    destroyed: list[str] = []
    primary = RuntimeError("wrapper-primary-private")

    class FailingCloseClient:
        def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            raise ValueError("client-close-private")

    def failing_client(*_args: object, **_kwargs: object) -> FailingCloseClient:
        return FailingCloseClient()

    def connect_fixture(*_args: object, **_kwargs: object) -> BackendProtocol:
        return DockerShellBackend(
            root=tmp_path,
            container_id="docker-cleanup-failure",
            timeout=30,
            max_output_bytes=100000,
        )

    def fail_wrapper(**_kwargs: object) -> BackendProtocol:
        raise primary

    def fail_destroy(sandbox_id: str) -> None:
        destroyed.append(sandbox_id)
        raise OSError("destroy-private")

    monkeypatch.setattr(
        archive_module.boto3,
        "client",
        failing_client,
    )
    monkeypatch.setattr(backend_module, "connect_docker_sandbox", connect_fixture)
    monkeypatch.setattr(backend_module, "ArchivingDockerShellBackend", fail_wrapper)
    monkeypatch.setattr(backend_module, "destroy_docker_sandbox", fail_destroy)
    settings = _sandbox_settings(
        str(tmp_path),
        S3Workspace(type="s3", endpoint="http://s3.invalid", bucket="owned"),
    ).model_copy(
        update={
            "docker": _sandbox_settings(str(tmp_path)).docker.model_copy(
                update={"image": "fixture"}
            )
        }
    )
    store = FakeRunRepository()
    run = request("docker-wrapper-cleanup-failure")
    lease = await store.try_claim(run)
    assert lease is not None

    with caplog.at_level("ERROR", logger="kokoro_agent.sandbox"):
        with pytest.raises(RuntimeError) as captured:
            await make_backend_for_run(
                "docker",
                settings,
                workspace="tenant:session",
                run_id=run.run_id,
                lease=lease,
                sandbox_store=store,
            )

    assert captured.value is primary
    assert close_calls == 1
    assert destroyed == ["docker-cleanup-failure"]
    assert primary.__notes__ == [
        "docker archive assembly cleanup also failed: ValueError",
        "docker archive assembly cleanup also failed: OSError",
    ]
    assert "docker archive assembly cleanup failed" in caplog.text
    assert "wrapper-primary-private" not in caplog.text
    assert "client-close-private" not in caplog.text
    assert "destroy-private" not in caplog.text


class TestBackendDispatch:
    def test_local_default_plain_backend(self, tmp_path: Path) -> None:
        backend = make_backend(
            "local_shell", _sandbox_settings(str(tmp_path)), workspace="ns:s1"
        )
        assert backend is not None
        assert not isinstance(backend, ArchivingLocalShellBackend)

    def test_s3_workspace_gets_archiving_backend(
        self, tmp_path: Path, tracked_s3_clients: list[S3Client]
    ) -> None:
        workspace = {"type": "s3", "endpoint": _TEST_ENDPOINT, "bucket": _TEST_BUCKET}
        backend = make_backend(
            "local_shell",
            _sandbox_settings(str(tmp_path), workspace),
            workspace="ns:s1",
        )
        assert isinstance(backend, ArchivingLocalShellBackend)


@pytest.mark.integration
class TestArchivingBackend:
    @pytest.fixture(autouse=True)
    def archive_resource(self, owned_archive: _OwnedArchive) -> None:
        self._archive = owned_archive

    def _backend(self, tmp_path: Path, prefix: str) -> ArchivingLocalShellBackend:
        root = tmp_path / prefix
        root.mkdir(parents=True)
        return ArchivingLocalShellBackend(
            root=root,
            archiver=S3Archiver(
                self._archive.workspace,
                access_key=self._archive.access_key,
                secret_key=self._archive.secret_key,
            ),
            prefix=prefix,
            timeout=30,
            max_output_bytes=100000,
            inherit_env=False,
        )

    def _object(self, key: str) -> bytes | None:
        assert self._archive.created
        try:
            response = self._archive.client.get_object(
                Bucket=self._archive.workspace.bucket, Key=key
            )
        except ClientError as error:
            if _is_missing(error, {"404", "NoSuchKey", "NotFound"}):
                return None
            raise
        body = response["Body"]
        try:
            return body.read()
        finally:
            body.close()

    @pytest.mark.asyncio
    async def test_awrite_uploads_incrementally(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.awrite("/plan.md", "# 计划\n本地预览")
        assert self._object(f"{prefix}/plan.md") == "# 计划\n本地预览".encode()

    @pytest.mark.asyncio
    async def test_aedit_reuploads(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.awrite("/plan.md", "draft v1")
        await backend.aedit("/plan.md", "v1", "v2")
        assert self._object(f"{prefix}/plan.md") == b"draft v2"

    @pytest.mark.asyncio
    async def test_aexecute_shell_write_caught_by_full_archive(
        self, tmp_path: Path
    ) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.aexecute("echo kokoro-shell-write > shell.txt")
        assert self._object(f"{prefix}/shell.txt") == b"kokoro-shell-write\n"

    @pytest.mark.asyncio
    async def test_hidden_and_junk_dirs_not_archived(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        backend = self._backend(tmp_path, prefix)
        await backend.aexecute(
            "mkdir -p __pycache__ && echo x > __pycache__/junk.pyc && echo y > .hidden"
        )
        assert self._object(f"{prefix}/__pycache__/junk.pyc") is None
        assert self._object(f"{prefix}/.hidden") is None

    @pytest.mark.asyncio
    async def test_archive_failure_does_not_break_tool(self, tmp_path: Path) -> None:
        prefix = f"ns:s_{uuid.uuid4().hex[:6]}"
        root = tmp_path / prefix
        root.mkdir(parents=True)
        backend = ArchivingLocalShellBackend(
            root=root,
            archiver=S3Archiver(
                S3Workspace(type="s3", endpoint="http://127.0.0.1:1", bucket="dead"),
                access_key=SecretStr(_TEST_ACCESS),
                secret_key=SecretStr(_TEST_SECRET),
            ),
            prefix=prefix,
            timeout=30,
            max_output_bytes=100000,
            inherit_env=False,
        )
        result = await backend.awrite("/plan.md", "still works")
        assert result.error is None
        assert (root / "plan.md").read_text() == "still works"
