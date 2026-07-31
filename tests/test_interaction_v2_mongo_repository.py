from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import replace
from typing import Self, TypeVar, cast

import pytest
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError

from kokoro_agent.interaction.domain.identities import InteractionIdentityFactory
from kokoro_agent.interaction.domain.models import (
    GroupRevisionCandidate,
    InteractionKind,
    OriginCandidate,
    OriginDescriptor,
    OwnerRevisionCandidate,
    OwnerRevisionRef,
    RevisionState,
    RunWriteFence,
)
from kokoro_agent.interaction.adapters.mongo_repository import (
    AGENT_INTERACTION_GROUP_HEADS_COLLECTION,
    AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION,
    AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION,
    AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION,
    AGENT_INTERACTION_OWNER_HEADS_COLLECTION,
    AGENT_INTERACTION_REVISIONS_COLLECTION,
    InteractionRepositoryConflict,
    InteractionFoundationNotReady,
    MongoEvidenceCommit,
    MongoInteractionRepository,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
_MISSING = object()
_U = TypeVar("_U")


def _nested(document: dict[str, object], path: str) -> object:
    current = document
    segments = path.split(".")
    for index, segment in enumerate(segments):
        value = current.get(segment, _MISSING)
        if index == len(segments) - 1:
            return value
        if type(value) is not dict:
            return _MISSING
        current = cast(dict[str, object], value)
    return _MISSING


def _matches(document: dict[str, object], query: Mapping[str, object]) -> bool:
    alternatives = query.get("$or")
    if alternatives is not None:
        typed_alternatives = cast(list[Mapping[str, object]], alternatives)
        if not any(
            _matches(document, alternative) for alternative in typed_alternatives
        ):
            return False
    for key, expected in query.items():
        if key == "$or":
            continue
        actual = _nested(document, key)
        if isinstance(expected, Mapping):
            operators = cast(Mapping[str, object], expected)
            if "$exists" in operators:
                if (actual is not _MISSING) is not bool(operators["$exists"]):
                    return False
            if "$ne" in operators and actual == operators["$ne"]:
                return False
            if "$gt" in operators:
                threshold = operators["$gt"]
                if (
                    not isinstance(actual, int)
                    or not isinstance(threshold, int)
                    or actual <= threshold
                ):
                    return False
            continue
        if actual != expected:
            return False
    return True


class _UpdateResult:
    def __init__(self, modified_count: int) -> None:
        self.modified_count = modified_count


class _Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self._index = 0

    def sort(self, field: str, direction: int) -> Self:
        self._rows.sort(key=lambda row: cast(int, row[field]), reverse=direction < 0)
        return self

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> dict[str, object]:
        if self._index == len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        return row


class _Collection:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.docs: list[dict[str, object]] = []
        self.write_sessions: list[object] = []
        self.unique_indexes: list[tuple[str, ...]] = []
        self.duplicate_once = False
        self.collection_options: dict[str, object] = {}
        self.first_index_options: dict[str, object] = {}

    async def index_information(self) -> dict[str, dict[str, object]]:
        return {
            f"index-{index}": {
                "key": [(field, 1) for field in fields],
                "unique": True,
                **(self.first_index_options if index == 0 else {}),
            }
            for index, fields in enumerate(self.unique_indexes)
        }

    async def options(self) -> dict[str, object]:
        return dict(self.collection_options)

    async def find_one(
        self,
        query: Mapping[str, object],
        projection: Mapping[str, object] | None = None,
        *,
        session: object,
    ) -> dict[str, object] | None:
        for document in self.docs:
            if _matches(document, query):
                return self._project(document, projection)
        return None

    def find(
        self,
        query: Mapping[str, object],
        projection: Mapping[str, object] | None = None,
        *,
        session: object,
    ) -> _Cursor:
        return _Cursor(
            [
                self._project(document, projection)
                for document in self.docs
                if _matches(document, query)
            ]
        )

    async def insert_one(
        self, document: Mapping[str, object], *, session: object
    ) -> None:
        if self.duplicate_once:
            self.duplicate_once = False
            raise DuplicateKeyError("injected duplicate")
        if any(existing.get("_id") == document.get("_id") for existing in self.docs):
            raise DuplicateKeyError("duplicate fake _id")
        self.docs.append(dict(document))
        self.write_sessions.append(session)

    async def insert_many(
        self,
        documents: list[dict[str, object]],
        *,
        ordered: bool,
        session: object,
    ) -> None:
        assert ordered is True
        for document in documents:
            await self.insert_one(document, session=session)

    async def update_one(
        self,
        query: Mapping[str, object],
        update: Mapping[str, Mapping[str, object]],
        *,
        session: object,
    ) -> _UpdateResult:
        for document in self.docs:
            if _matches(document, query):
                document.update(update["$set"])
                self.write_sessions.append(session)
                return _UpdateResult(1)
        return _UpdateResult(0)

    @staticmethod
    def _project(
        document: Mapping[str, object], projection: Mapping[str, object] | None
    ) -> dict[str, object]:
        if projection is None:
            return dict(document)
        included = {key for key, value in projection.items() if value == 1}
        if included:
            return {key: document[key] for key in included if key in document}
        return {
            key: value for key, value in document.items() if projection.get(key) != 0
        }


class _Session:
    def __init__(self, client: _Client) -> None:
        self._client = client

    async def __aenter__(self) -> Self:
        self._client.sessions.append(self)
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    async def with_transaction(
        self,
        callback: Callable[[AsyncClientSession], Awaitable[_U]],
        **options: object,
    ) -> _U:
        assert set(options) == {"read_concern", "write_concern", "read_preference"}
        snapshots = {
            name: deepcopy(collection.docs)
            for name, collection in self._client.database.collections.items()
        }
        try:
            return await callback(cast(AsyncClientSession, self))
        except Exception:
            for name, documents in snapshots.items():
                self._client.database[name].docs = documents
            raise


class _Client:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.sessions: list[_Session] = []

    def start_session(self) -> _Session:
        return _Session(self)


class _Database:
    def __init__(self) -> None:
        self.client = _Client(self)
        self.collections: dict[str, _Collection] = {}

    def __getitem__(self, name: str) -> _Collection:
        return self.collections.setdefault(name, _Collection(self))

    async def command(self, command: Mapping[str, object]) -> dict[str, object]:
        assert command == {"hello": 1}
        return {"setName": "kokoro-rs"}


class _EvidenceCommitter:
    def __init__(self) -> None:
        self.sessions: list[AsyncClientSession] = []
        self.calls = 0
        self.fail_commit = False
        self.verify_ok = True

    async def commit_whole_frame(
        self,
        candidate: GroupRevisionCandidate,
        fence: RunWriteFence,
        *,
        session: AsyncClientSession,
    ) -> MongoEvidenceCommit:
        self.calls += 1
        self.sessions.append(session)
        if self.fail_commit:
            raise InteractionRepositoryConflict("INJECTED_COMMIT_FAILURE")
        return MongoEvidenceCommit(
            evidence_ref=f"aev2_{candidate.group_projection_ref}",
            durable_seq=7 + self.calls,
            event_id=f"event-{self.calls}",
            run_fence_cas_succeeded=True,
        )

    async def verify_existing_whole_frame(
        self,
        candidate: GroupRevisionCandidate,
        fence: RunWriteFence,
        commit: MongoEvidenceCommit,
        *,
        session: AsyncClientSession,
    ) -> bool:
        self.sessions.append(session)
        return self.verify_ok and (
            commit.evidence_ref == f"aev2_{candidate.group_projection_ref}"
            and commit.durable_seq == 8
            and commit.event_id == "event-1"
        )


class _DigestVerifier:
    def __init__(self) -> None:
        self.calls = 0

    def verify_whole_frame(self, candidate: GroupRevisionCandidate) -> None:
        self.calls += 1


def _fence() -> RunWriteFence:
    return RunWriteFence(
        run_id="run-1",
        lease_owner_ref="worker-1",
        producer_instance_ref="producer-1",
        producer_generation=1,
        lease_valid_at_ms=100,
        checkpoint_ref="checkpoint-1",
        checkpoint_sha256=SHA_A,
        checkpoint_generation=1,
    )


async def _repository(
    *, storage_ready: bool = True
) -> tuple[MongoInteractionRepository, _Database, _EvidenceCommitter]:
    database = _Database()
    runs = database["agent_run_ledger"]
    runs.docs.append(
        {
            "_id": "run-1",
            "terminal": False,
            "owner": "worker-1",
            "lease_expires_ms": 200,
            "interaction_v2_fence": {
                "producer_instance_ref": "producer-1",
                "producer_generation": 1,
                "checkpoint_ref": "checkpoint-1",
                "checkpoint_sha256": SHA_A,
                "checkpoint_generation": 1,
            },
        }
    )
    committer = _EvidenceCommitter()
    digest_verifier = _DigestVerifier()
    required_indexes = {
        AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION: [
            (
                "run_id",
                "stable_task_path",
                "origin_tool_call_ref",
                "elicitation_ordinal",
            ),
            ("application_request_ref",),
            ("run_id", "origin_key_digest"),
        ],
        AGENT_INTERACTION_OWNER_HEADS_COLLECTION: [
            ("run_id", "interaction_owner_ref"),
            ("run_id", "origin_key_digest"),
        ],
        AGENT_INTERACTION_REVISIONS_COLLECTION: [
            ("run_id", "interaction_owner_ref", "owner_revision"),
            ("projection_event_ref",),
        ],
        AGENT_INTERACTION_GROUP_HEADS_COLLECTION: [("run_id", "decision_group_ref")],
        AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION: [
            ("run_id", "decision_group_ref", "decision_group_revision"),
            ("group_projection_ref",),
        ],
        AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION: [
            (
                "run_id",
                "decision_group_ref",
                "decision_group_revision",
                "group_member_ordinal",
            ),
            ("run_id", "interaction_owner_ref", "owner_revision"),
        ],
    }
    if storage_ready:
        for name, indexes in required_indexes.items():
            database[name].unique_indexes.extend(indexes)
    repository = MongoInteractionRepository(
        cast(AsyncCollection[dict[str, object]], runs),
        evidence_committer=committer,
        canonical_digest_verifier=digest_verifier,
        clock=lambda: 100,
    )
    if storage_ready:
        await repository.assert_storage_ready()
    return repository, database, committer


def _origin(tool_ref: str, cursor: int = 1) -> OriginCandidate:
    return OriginDescriptor(
        run_id="run-1",
        stable_task_path="root/research",
        origin_tool_call_ref=tool_ref,
        invocation_elicitation_cursor=cursor,
        interaction_kind=InteractionKind.APPROVAL,
        base_descriptor_sha256=SHA_A,
        base_schema_sha256=SHA_B,
    ).to_candidate()


def _member(
    origin: OriginCandidate,
    *,
    ordinal: int,
    revision: int,
    predecessor: OwnerRevisionCandidate | None = None,
) -> OwnerRevisionCandidate:
    canonical = f"member:{origin.interaction_owner_ref}:{revision}".encode()
    return OwnerRevisionCandidate(
        interaction_owner_ref=origin.interaction_owner_ref,
        origin_key_digest=origin.origin_key_digest,
        owner_revision=revision,
        projection_event_ref=InteractionIdentityFactory()
        .projection_event(
            run_id="run-1",
            interaction_owner_ref=origin.interaction_owner_ref,
            owner_revision=revision,
        )
        .value,
        predecessor_projection_event_ref=(
            None if predecessor is None else predecessor.projection_event_ref
        ),
        predecessor_evidence_sha256=(
            None if predecessor is None else predecessor.member_evidence_sha256
        ),
        member_evidence_sha256=hashlib.sha256(canonical).hexdigest(),
        canonical_member_evidence=canonical,
        projection_payload_sha256=SHA_B,
        application_request_ref=origin.application_request_ref,
        interaction_kind=InteractionKind.APPROVAL,
        group_member_ordinal=ordinal,
        required_owner_revision_refs=(),
        state=RevisionState.PENDING,
    )


def _frame(
    origins: tuple[OriginCandidate, ...],
    *,
    revision: int = 1,
    predecessor: GroupRevisionCandidate | None = None,
) -> GroupRevisionCandidate:
    prior_members = None if predecessor is None else predecessor.members
    members = tuple(
        _member(
            origin,
            ordinal=index,
            revision=revision,
            predecessor=None if prior_members is None else prior_members[index - 1],
        )
        for index, origin in enumerate(origins, start=1)
    )
    vector = tuple(
        OwnerRevisionRef(member.interaction_owner_ref, member.owner_revision)
        for member in members
    )
    members = tuple(
        replace(member, required_owner_revision_refs=vector) for member in members
    )
    canonical = f"group:{revision}".encode()
    return GroupRevisionCandidate(
        run_id="run-1",
        decision_group_ref="igrp-1",
        decision_group_revision=revision,
        group_projection_ref=InteractionIdentityFactory()
        .group_projection(
            run_id="run-1",
            decision_group_ref="igrp-1",
            decision_group_revision=revision,
        )
        .value,
        predecessor_group_projection_ref=(
            None if predecessor is None else predecessor.group_projection_ref
        ),
        predecessor_group_evidence_sha256=(
            None if predecessor is None else predecessor.group_evidence_sha256
        ),
        group_evidence_sha256=hashlib.sha256(canonical).hexdigest(),
        canonical_group_evidence=canonical,
        pending_frame_digest=SHA_A,
        member_vector_sha256=SHA_B,
        members=members,
        successor_proof_ref=None if predecessor is None else "proof-1",
        successor_proof_sha256=None if predecessor is None else SHA_A,
    )


def test_mongo_repository_has_no_default_or_second_evidence_committer() -> None:
    signature = inspect.signature(MongoInteractionRepository)
    committer = signature.parameters["evidence_committer"]
    assert committer.default is inspect.Parameter.empty
    digest_verifier = signature.parameters["canonical_digest_verifier"]
    assert digest_verifier.default is inspect.Parameter.empty


def test_mongo_repository_uses_adr_014_bounded_collections() -> None:
    assert {
        AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION,
        AGENT_INTERACTION_OWNER_HEADS_COLLECTION,
        AGENT_INTERACTION_REVISIONS_COLLECTION,
        AGENT_INTERACTION_GROUP_HEADS_COLLECTION,
        AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION,
        AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION,
    } == {
        "agent_interaction_origin_journal",
        "agent_interaction_owner_heads",
        "agent_interaction_revisions",
        "agent_interaction_group_heads",
        "agent_interaction_group_revisions",
        "agent_interaction_group_members",
    }


async def test_prepare_origin_is_keep_first_and_rejects_gap_or_mutation() -> None:
    repository, database, _ = await _repository()
    origin = _origin("tool-1")
    assert await repository.prepare_origin(origin, _fence()) == origin
    assert await repository.prepare_origin(origin, _fence()) == origin
    assert len(database[AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION].docs) == 1

    with pytest.raises(InteractionRepositoryConflict, match="ORIGIN_CONFLICT"):
        await repository.prepare_origin(
            replace(origin, base_schema_sha256=SHA_A), _fence()
        )
    with pytest.raises(InteractionRepositoryConflict, match="ORDINAL_GAP"):
        await repository.prepare_origin(_origin("tool-gap", cursor=2), _fence())


async def test_storage_readiness_fails_closed_without_all_unique_indexes() -> None:
    repository, _, _ = await _repository(storage_ready=False)
    with pytest.raises(InteractionFoundationNotReady, match="UNIQUE_INDEX_MISSING"):
        await repository.assert_storage_ready()
    with pytest.raises(InteractionFoundationNotReady, match="READINESS_NOT_PROVEN"):
        await repository.prepare_origin(_origin("tool-1"), _fence())


async def test_storage_readiness_rejects_collated_sparse_or_partial_unique_indexes() -> (
    None
):
    repository, database, _ = await _repository()
    origins = database[AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION]
    origins.collection_options["collation"] = {"locale": "en", "strength": 2}
    with pytest.raises(InteractionFoundationNotReady, match="COLLATION_UNSAFE"):
        await repository.assert_storage_ready()
    origins.collection_options.clear()

    unsafe_indexes: tuple[dict[str, object], ...] = (
        {"sparse": True},
        {"partialFilterExpression": {"run_id": {"$exists": True}}},
        {"collation": {"locale": "en", "strength": 2}},
    )
    for unsafe in unsafe_indexes:
        origins.first_index_options = unsafe
        with pytest.raises(InteractionFoundationNotReady, match="UNIQUE_INDEX_MISSING"):
            await repository.assert_storage_ready()
    origins.first_index_options = {}


async def test_prepare_origin_retries_duplicate_key_in_a_fresh_transaction() -> None:
    repository, database, _ = await _repository()
    origins = database[AGENT_INTERACTION_ORIGIN_JOURNAL_COLLECTION]
    origins.duplicate_once = True
    candidate = _origin("tool-1")
    assert await repository.prepare_origin(candidate, _fence()) == candidate
    assert len(origins.docs) == 1
    assert len(database.client.sessions) == 2


async def test_prepare_origin_fails_closed_after_terminal_or_lease_loss() -> None:
    repository, database, _ = await _repository()
    database["agent_run_ledger"].docs[0]["terminal"] = True
    with pytest.raises(InteractionRepositoryConflict, match="RUN_FENCE_LOST"):
        await repository.prepare_origin(_origin("tool-1"), _fence())


async def test_initial_whole_frame_commits_all_rows_and_replays_without_recommit() -> (
    None
):
    repository, database, committer = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    for origin in origins:
        await repository.prepare_origin(origin, _fence())
    frame = _frame(origins)

    published = await repository.publish_whole_frame(frame, _fence())
    replayed = await repository.publish_whole_frame(frame, _fence())

    assert published.created is True
    assert replayed == replace(published, created=False)
    assert committer.calls == 1
    assert len(database[AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION].docs) == 1
    assert len(database[AGENT_INTERACTION_REVISIONS_COLLECTION].docs) == 2
    assert len(database[AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION].docs) == 2
    assert len(database[AGENT_INTERACTION_OWNER_HEADS_COLLECTION].docs) == 2
    group_row = database[AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION].docs[0]
    assert group_row["evidence_ref"] == published.evidence_ref
    assert group_row["durable_seq"] == published.durable_seq
    assert all(
        session is committer.sessions[0]
        for name in (
            AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION,
            AGENT_INTERACTION_REVISIONS_COLLECTION,
            AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION,
            AGENT_INTERACTION_OWNER_HEADS_COLLECTION,
            AGENT_INTERACTION_GROUP_HEADS_COLLECTION,
        )
        for session in database[name].write_sessions
    )


async def test_same_projection_ref_with_mutated_bytes_fails_loud() -> None:
    repository, _, _ = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    for origin in origins:
        await repository.prepare_origin(origin, _fence())
    frame = _frame(origins)
    await repository.publish_whole_frame(frame, _fence())
    mutated_bytes = b"mutated-group"
    mutated = replace(
        frame,
        canonical_group_evidence=mutated_bytes,
        group_evidence_sha256=hashlib.sha256(mutated_bytes).hexdigest(),
    )
    with pytest.raises(InteractionRepositoryConflict, match="IDENTITY_CONFLICT"):
        await repository.publish_whole_frame(mutated, _fence())


async def test_replay_revalidates_immutable_evidence_outbox_link() -> None:
    repository, database, _ = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    for origin in origins:
        await repository.prepare_origin(origin, _fence())
    frame = _frame(origins)
    await repository.publish_whole_frame(frame, _fence())
    database[AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION].docs[0]["event_id"] = (
        "tampered-event"
    )
    with pytest.raises(InteractionRepositoryConflict, match="EVIDENCE_LINK_CONFLICT"):
        await repository.publish_whole_frame(frame, _fence())


async def test_whole_frame_rolls_back_every_row_when_evidence_commit_fails() -> None:
    repository, database, committer = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    for origin in origins:
        await repository.prepare_origin(origin, _fence())
    committer.fail_commit = True
    with pytest.raises(InteractionRepositoryConflict, match="COMMIT_FAILURE"):
        await repository.publish_whole_frame(_frame(origins), _fence())
    for name in (
        AGENT_INTERACTION_GROUP_REVISIONS_COLLECTION,
        AGENT_INTERACTION_REVISIONS_COLLECTION,
        AGENT_INTERACTION_GROUP_MEMBERS_COLLECTION,
        AGENT_INTERACTION_OWNER_HEADS_COLLECTION,
        AGENT_INTERACTION_GROUP_HEADS_COLLECTION,
    ):
        assert database[name].docs == []


async def test_successor_advances_same_ordered_owner_set_as_one_frame() -> None:
    repository, database, committer = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    for origin in origins:
        await repository.prepare_origin(origin, _fence())
    first = _frame(origins)
    await repository.publish_whole_frame(first, _fence())
    for row in database[AGENT_INTERACTION_OWNER_HEADS_COLLECTION].docs:
        row["state"] = "applied"
    database[AGENT_INTERACTION_GROUP_HEADS_COLLECTION].docs[0]["state"] = "applied"

    successor = _frame(origins, revision=2, predecessor=first)
    published = await repository.publish_whole_frame(successor, _fence())

    assert published.created is True
    assert committer.calls == 2
    assert [
        row["current_revision"]
        for row in database[AGENT_INTERACTION_OWNER_HEADS_COLLECTION].docs
    ] == [2, 2]
    assert (
        database[AGENT_INTERACTION_GROUP_HEADS_COLLECTION].docs[0]["current_revision"]
        == 2
    )


async def test_successor_rejects_changed_ordered_owner_set() -> None:
    repository, database, _ = await _repository()
    origins = (_origin("tool-1"), _origin("tool-2"))
    third = _origin("tool-3")
    for origin in (*origins, third):
        await repository.prepare_origin(origin, _fence())
    first = _frame(origins)
    await repository.publish_whole_frame(first, _fence())
    for row in database[AGENT_INTERACTION_OWNER_HEADS_COLLECTION].docs:
        row["state"] = "applied"
    database[AGENT_INTERACTION_GROUP_HEADS_COLLECTION].docs[0]["state"] = "applied"

    changed = _frame((origins[0], third), revision=2, predecessor=first)
    with pytest.raises(InteractionRepositoryConflict, match="MEMBER_VECTOR_CONFLICT"):
        await repository.publish_whole_frame(changed, _fence())
