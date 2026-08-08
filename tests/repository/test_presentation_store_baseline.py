from __future__ import annotations

import hashlib
import json
import os
import uuid

import pytest
from pymongo import AsyncMongoClient

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
