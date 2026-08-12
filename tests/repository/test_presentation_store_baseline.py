from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase

from kokoro_agent.presentation.store_baseline import (
    PRESENTATION_COLLECTIONS,
    PRESENTATION_RETIRED_COLLECTIONS,
    PRESENTATION_STORE_BASELINE_DIGEST,
    PRESENTATION_STORE_BASELINE_REVISION,
    PresentationStoreBaselineError,
    canonical_store_baseline_bytes,
    ensure_presentation_store_baseline,
)


MONGO_URL = os.environ.get(
    "KOKORO_MONGO_URL",
    "mongodb://127.0.0.1:27017/?replicaSet=kokoro-rs&directConnection=true",
)
CreateCollection = Callable[
    ..., Awaitable[AsyncCollection[dict[str, object]]]
]


class _FirstListBarrierDatabase:
    """Force concurrent callers to observe the same empty baseline once."""

    def __init__(
        self,
        database: AsyncDatabase[dict[str, object]],
        barrier: asyncio.Barrier,
    ) -> None:
        self._database = database
        self._barrier = barrier
        self._first_list = True

    async def list_collection_names(
        self,
        session: AsyncClientSession | None = None,
        filter: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        names = await self._database.list_collection_names(
            session=session,
            filter=filter,
            **kwargs,
        )
        if self._first_list:
            self._first_list = False
            await self._barrier.wait()
        return names

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)

    def __getitem__(self, name: str) -> Any:
        return self._database[name]


def test_store_baseline_manifest_freezes_revision_validators_and_indexes() -> None:
    encoded = canonical_store_baseline_bytes()
    manifest = json.loads(encoded)

    assert PRESENTATION_STORE_BASELINE_REVISION == (
        "kokoro.agent.presentation.store-baseline.v1"
    )
    assert PRESENTATION_STORE_BASELINE_DIGEST == (
        "sha256:" + hashlib.sha256(encoded).hexdigest()
    )
    assert PRESENTATION_STORE_BASELINE_DIGEST == (
        "sha256:54dae94191e863a09a32b7bb165ae91dad354b6f52b07f2e2daf10eff0fa1f60"
    )
    assert tuple(row["name"] for row in manifest["collections"]) == (
        PRESENTATION_COLLECTIONS
    )
    assert all(row["validator"] for row in manifest["collections"])
    assert {
        index["name"]
        for row in manifest["collections"]
        for index in row["indexes"]
    } == {
        "run_delivery_seq_unique",
        "run_source_event_ref_unique",
        "admission_original_command_unique",
    }


async def test_store_baseline_rejects_old_mixed_validator_and_index_drift() -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_presentation_baseline_{uuid.uuid4().hex}"]
    try:
        await database.create_collection(PRESENTATION_RETIRED_COLLECTIONS[0])
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_RETIRED_COLLECTION_PRESENT",
        ):
            await ensure_presentation_store_baseline(database)
        await database[PRESENTATION_RETIRED_COLLECTIONS[0]].drop()

        await database.create_collection(PRESENTATION_COLLECTIONS[0])
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_MIXED_BASELINE",
        ):
            await ensure_presentation_store_baseline(database)
        await database[PRESENTATION_COLLECTIONS[0]].drop()

        await ensure_presentation_store_baseline(database)
        await database.command(
            {
                "collMod": PRESENTATION_COLLECTIONS[0],
                "validator": {"$jsonSchema": {"bsonType": "object"}},
            }
        )
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_VALIDATOR_DRIFT",
        ):
            await ensure_presentation_store_baseline(database)

        await client.drop_database(database.name)
        await ensure_presentation_store_baseline(database)
        await database[PRESENTATION_COLLECTIONS[0]].create_index(
            "run_id", name="unexpected_index"
        )
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_INDEX_DRIFT",
        ):
            await ensure_presentation_store_baseline(database)
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_forward_and_rollback_are_exact_reconstructions() -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_reconstruct_{uuid.uuid4().hex}"]
    try:
        await ensure_presentation_store_baseline(database)
        assert set(PRESENTATION_COLLECTIONS) <= set(
            await database.list_collection_names()
        )

        for name in PRESENTATION_COLLECTIONS:
            await database[name].drop()
        for name in PRESENTATION_RETIRED_COLLECTIONS:
            await database.create_collection(name)
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_RETIRED_COLLECTION_PRESENT",
        ):
            await ensure_presentation_store_baseline(database)

        for name in PRESENTATION_RETIRED_COLLECTIONS:
            await database[name].drop()
        await ensure_presentation_store_baseline(database)
        assert set(await database.list_collection_names()).isdisjoint(
            PRESENTATION_RETIRED_COLLECTIONS
        )
        assert set(PRESENTATION_COLLECTIONS) <= set(
            await database.list_collection_names()
        )
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_accepts_non_presentation_collections_in_the_same_database() -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_shared_{uuid.uuid4().hex}"]
    try:
        await database["ledger"].create_index("run_id", name="run_id")
        await ensure_presentation_store_baseline(database)
        assert set(PRESENTATION_COLLECTIONS) <= set(
            await database.list_collection_names()
        )
        assert "ledger" in await database.list_collection_names()
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_rejects_an_exact_empty_partial_baseline() -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_partial_{uuid.uuid4().hex}"]
    manifest = json.loads(canonical_store_baseline_bytes())
    first = manifest["collections"][0]
    try:
        collection = await database.create_collection(
            first["name"],
            validator=first["validator"],
            validationLevel="strict",
            validationAction="error",
        )
        for index in first["indexes"]:
            await collection.create_index(
                [tuple(key) for key in index["keys"]],
                name=index["name"],
                unique=index["unique"],
            )

        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_MIXED_BASELINE",
        ):
            await ensure_presentation_store_baseline(database)
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_concurrent_empty_initializers_converge() -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_concurrent_{uuid.uuid4().hex}"]
    callers = 3
    barrier = asyncio.Barrier(callers)
    wrapped = [
        cast(
            AsyncDatabase[dict[str, object]],
            _FirstListBarrierDatabase(database, barrier),
        )
        for _ in range(callers)
    ]
    try:
        results = await asyncio.gather(
            *(ensure_presentation_store_baseline(item) for item in wrapped),
            return_exceptions=True,
        )

        assert results == [None] * callers
        await ensure_presentation_store_baseline(database)
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_failed_transaction_leaves_no_partial_collections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_atomic_{uuid.uuid4().hex}"]
    original = database.create_collection
    create_collection = cast(CreateCollection, original)
    calls = 0

    async def fail_during_baseline(*args: object, **kwargs: object) -> Any:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("injected baseline creation failure")
        return await create_collection(*args, **kwargs)

    monkeypatch.setattr(database, "create_collection", fail_during_baseline)
    try:
        with pytest.raises(RuntimeError, match="injected baseline creation failure"):
            await ensure_presentation_store_baseline(database)

        assert set(await database.list_collection_names()).isdisjoint(
            PRESENTATION_COLLECTIONS
        )
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_store_baseline_rechecks_retired_collections_after_concurrent_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(MONGO_URL)
    database = client[f"kokoro_pres_retired_race_{uuid.uuid4().hex}"]
    original = database.list_collection_names
    calls = 0

    async def inject_retired_after_initial_snapshot(
        session: AsyncClientSession | None = None,
        filter: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        nonlocal calls
        calls += 1
        names = await original(session=session, filter=filter, **kwargs)
        if calls == 1:
            await database.create_collection(PRESENTATION_RETIRED_COLLECTIONS[0])
        return names

    monkeypatch.setattr(
        database,
        "list_collection_names",
        inject_retired_after_initial_snapshot,
    )
    try:
        with pytest.raises(
            PresentationStoreBaselineError,
            match="PRESENTATION_STORE_RETIRED_COLLECTION_PRESENT",
        ):
            await ensure_presentation_store_baseline(database)
    finally:
        await client.drop_database(database.name)
        await client.close()
