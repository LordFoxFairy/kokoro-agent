"""Exact Mongo baseline for Agent-owned Presentation delivery persistence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Final, TypedDict

from pymongo import ReadPreference
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pydantic import TypeAdapter

PRESENTATION_STORE_BASELINE_REVISION: Final = (
    "kokoro.agent.presentation.store-baseline.v1"
)
AGENT_PRESENTATION_DELIVERY_RECORD_COLLECTION: Final = (
    "agent_presentation_delivery_record"
)
AGENT_PRESENTATION_SOURCE_COMMIT_COLLECTION: Final = "agent_presentation_source_commit"
AGENT_PRESENTATION_PLANNER_STATE_COLLECTION: Final = "agent_presentation_planner_state"
AGENT_PRESENTATION_DELIVERY_STATE_COLLECTION: Final = (
    "agent_presentation_delivery_state"
)
AGENT_PRESENTATION_ADMISSION_COMMAND_RECEIPT_COLLECTION: Final = (
    "agent_presentation_admission_command_receipt"
)
PRESENTATION_COLLECTIONS: Final = (
    AGENT_PRESENTATION_DELIVERY_RECORD_COLLECTION,
    AGENT_PRESENTATION_SOURCE_COMMIT_COLLECTION,
    AGENT_PRESENTATION_PLANNER_STATE_COLLECTION,
    AGENT_PRESENTATION_DELIVERY_STATE_COLLECTION,
    AGENT_PRESENTATION_ADMISSION_COMMAND_RECEIPT_COLLECTION,
)
PRESENTATION_RETIRED_COLLECTIONS: Final = (
    "agent_presentation_candidate",
    "agent_presentation_source_batch",
    "agent_presentation_state",
    "agent_presentation_delivery",
    "agent_presentation_admission_command",
)

_INTEGER = ["int", "long"]
_SHA256 = "^sha256:[0-9a-f]{64}$"
_SUBMISSION_REF = "^presentation\\.submission:sha256:[0-9a-f]{64}$"
_RECORD_REF = "^presentation\\.record:sha256:[0-9a-f]{64}$"
_OPTIONS_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])


class _IndexSpec(TypedDict):
    name: str
    keys: tuple[tuple[str, int], ...]
    unique: bool


class _CollectionSpec(TypedDict):
    name: str
    validator: dict[str, object]
    indexes: tuple[_IndexSpec, ...]


def _validator(
    *, required: Sequence[str], properties: Mapping[str, object]
) -> dict[str, object]:
    return {
        "$jsonSchema": {
            "title": PRESENTATION_STORE_BASELINE_REVISION,
            "bsonType": "object",
            "required": list(required),
            "additionalProperties": False,
            "properties": dict(properties),
        }
    }


_COLLECTION_SPECS: Final[tuple[_CollectionSpec, ...]] = (
    {
        "name": AGENT_PRESENTATION_DELIVERY_RECORD_COLLECTION,
        "validator": _validator(
            required=(
                "_id",
                "record_ref",
                "run_id",
                "delivery_seq",
                "envelope_bytes",
                "envelope_digest",
                "submission_ref",
                "submission_digest",
                "recorded_at_ms",
                "producer_instance_ref",
                "producer_generation",
                "source_commit_ref",
                "delivery_record_proto",
            ),
            properties={
                "_id": {"bsonType": "string", "pattern": _RECORD_REF},
                "record_ref": {"bsonType": "string", "pattern": _RECORD_REF},
                "run_id": {"bsonType": "string"},
                "delivery_seq": {"bsonType": _INTEGER, "minimum": 1},
                "envelope_bytes": {"bsonType": "binData"},
                "envelope_digest": {"bsonType": "string", "pattern": _SHA256},
                "submission_ref": {
                    "bsonType": "string",
                    "pattern": _SUBMISSION_REF,
                },
                "submission_digest": {
                    "bsonType": "string",
                    "pattern": _SHA256,
                },
                "recorded_at_ms": {"bsonType": _INTEGER, "minimum": 0},
                "producer_instance_ref": {"bsonType": "string"},
                "producer_generation": {"bsonType": _INTEGER, "minimum": 1},
                "source_commit_ref": {"bsonType": "string"},
                "delivery_record_proto": {"bsonType": "binData"},
            },
        ),
        "indexes": (
            {
                "name": "run_delivery_seq_unique",
                "keys": (("run_id", 1), ("delivery_seq", 1)),
                "unique": True,
            },
        ),
    },
    {
        "name": AGENT_PRESENTATION_SOURCE_COMMIT_COLLECTION,
        "validator": _validator(
            required=(
                "_id",
                "run_id",
                "agent_thread_ref",
                "source_event_ref",
                "source_payload_sha256",
                "submission_count",
                "ordered_envelope_digest",
                "first_delivery_seq",
                "recorded_at_ms",
            ),
            properties={
                "_id": {"bsonType": "string"},
                "run_id": {"bsonType": "string"},
                "agent_thread_ref": {"bsonType": "string"},
                "source_event_ref": {"bsonType": "string"},
                "source_payload_sha256": {
                    "bsonType": "string",
                    "pattern": "^[0-9a-f]{64}$",
                },
                "submission_count": {"bsonType": _INTEGER, "minimum": 0},
                "ordered_envelope_digest": {
                    "bsonType": "string",
                    "pattern": "^[0-9a-f]{64}$",
                },
                "first_delivery_seq": {
                    "bsonType": ["null", "int", "long"],
                    "minimum": 1,
                },
                "recorded_at_ms": {"bsonType": _INTEGER, "minimum": 0},
            },
        ),
        "indexes": (
            {
                "name": "run_source_event_ref_unique",
                "keys": (("run_id", 1), ("source_event_ref", 1)),
                "unique": True,
            },
        ),
    },
    {
        "name": AGENT_PRESENTATION_PLANNER_STATE_COLLECTION,
        "validator": _validator(
            required=("_id", "planner_revision", "state"),
            properties={
                "_id": {"bsonType": "string"},
                "planner_revision": {"bsonType": _INTEGER, "minimum": 1},
                "state": {"bsonType": "object"},
            },
        ),
        "indexes": (),
    },
    {
        "name": AGENT_PRESENTATION_DELIVERY_STATE_COLLECTION,
        "validator": _validator(
            required=(
                "_id",
                "acknowledged_through_delivery_seq",
                "status_revision",
                "updated_at_ms",
            ),
            properties={
                "_id": {"bsonType": "string"},
                "acknowledged_through_delivery_seq": {
                    "bsonType": _INTEGER,
                    "minimum": 0,
                },
                "status_revision": {"bsonType": _INTEGER, "minimum": 1},
                "updated_at_ms": {"bsonType": _INTEGER, "minimum": 0},
                "acknowledged_head_record_digest": {
                    "bsonType": "string",
                    "pattern": _SHA256,
                },
                "last_command_proto": {"bsonType": "binData"},
                "quarantine_proto": {"bsonType": "binData"},
                "terminal_seal_proto": {"bsonType": "binData"},
            },
        ),
        "indexes": (),
    },
    {
        "name": AGENT_PRESENTATION_ADMISSION_COMMAND_RECEIPT_COLLECTION,
        "validator": _validator(
            required=(
                "_id",
                "run_id",
                "kind",
                "original_command_id",
                "original_command_proto",
                "effect_digest",
                "status_proto",
            ),
            properties={
                "_id": {"bsonType": "string"},
                "run_id": {"bsonType": "string"},
                "kind": {"enum": ["ack", "quarantine"]},
                "original_command_id": {"bsonType": "string"},
                "original_command_proto": {"bsonType": "binData"},
                "effect_digest": {"bsonType": "string", "pattern": _SHA256},
                "status_proto": {"bsonType": "binData"},
            },
        ),
        "indexes": (
            {
                "name": "admission_original_command_unique",
                "keys": (("original_command_id", 1),),
                "unique": True,
            },
        ),
    },
)


class PresentationStoreBaselineError(RuntimeError):
    """The database is not exactly the declared Presentation baseline."""


def _manifest_value() -> dict[str, object]:
    return {
        "revision": PRESENTATION_STORE_BASELINE_REVISION,
        "collections": [
            {
                "name": spec["name"],
                "validator": spec["validator"],
                "indexes": [
                    {
                        "name": index["name"],
                        "keys": [list(key) for key in index["keys"]],
                        "unique": index["unique"],
                    }
                    for index in spec["indexes"]
                ],
            }
            for spec in _COLLECTION_SPECS
        ],
    }


def canonical_store_baseline_bytes() -> bytes:
    return json.dumps(
        _manifest_value(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


PRESENTATION_STORE_BASELINE_DIGEST: Final = (
    "sha256:54dae94191e863a09a32b7bb165ae91dad354b6f52b07f2e2daf10eff0fa1f60"
)
_COMPUTED_BASELINE_DIGEST = (
    "sha256:" + hashlib.sha256(canonical_store_baseline_bytes()).hexdigest()
)
if _COMPUTED_BASELINE_DIGEST != PRESENTATION_STORE_BASELINE_DIGEST:
    raise RuntimeError("PRESENTATION_STORE_BASELINE_REBASELINE_REQUIRED")


async def _collection_options(
    database: AsyncDatabase[dict[str, object]], name: str
) -> dict[str, Any]:
    rows = [
        row
        async for row in await database.list_collections(
            filter={"name": name}, nameOnly=False
        )
    ]
    if len(rows) != 1:
        raise PresentationStoreBaselineError("PRESENTATION_STORE_MIXED_BASELINE")
    options = rows[0].get("options")
    if not isinstance(options, dict):
        raise PresentationStoreBaselineError("PRESENTATION_STORE_VALIDATOR_DRIFT")
    return _OPTIONS_ADAPTER.validate_python(options)


async def _create_baseline_transactionally(
    database: AsyncDatabase[dict[str, object]],
) -> None:
    async with database.client.start_session() as session:

        async def create(active: AsyncClientSession) -> None:
            for spec in _COLLECTION_SPECS:
                collection = await database.create_collection(
                    str(spec["name"]),
                    validator=spec["validator"],
                    validationLevel="strict",
                    validationAction="error",
                    check_exists=False,
                    session=active,
                )
                for index in spec["indexes"]:
                    await collection.create_index(
                        list(index["keys"]),
                        name=index["name"],
                        unique=index["unique"],
                        session=active,
                    )

        await session.with_transaction(
            create,
            read_concern=ReadConcern("local"),
            write_concern=WriteConcern("majority"),
            read_preference=ReadPreference.PRIMARY,
        )


async def _verify_baseline(database: AsyncDatabase[dict[str, object]]) -> None:
    for spec in _COLLECTION_SPECS:
        name = str(spec["name"])
        options = await _collection_options(database, name)
        if (
            options.get("validator") != spec["validator"]
            or options.get("validationLevel") != "strict"
            or options.get("validationAction") != "error"
        ):
            raise PresentationStoreBaselineError(
                f"PRESENTATION_STORE_VALIDATOR_DRIFT:{name}"
            )
        actual = await database[name].index_information()
        expected: dict[str, dict[str, object]] = {
            "_id_": {"key": [("_id", 1)], "unique": False},
            **{
                str(index["name"]): {
                    "key": [tuple(key) for key in index["keys"]],
                    "unique": bool(index["unique"]),
                }
                for index in spec["indexes"]
            },
        }
        normalized: dict[str, dict[str, object]] = {}
        for index_name, info in actual.items():
            if set(info).difference({"v", "key", "unique"}):
                raise PresentationStoreBaselineError(
                    f"PRESENTATION_STORE_INDEX_DRIFT:{name}"
                )
            normalized[index_name] = {
                "key": [tuple(key) for key in info.get("key", ())],
                "unique": bool(info.get("unique", False)),
            }
        if normalized != expected:
            raise PresentationStoreBaselineError(
                f"PRESENTATION_STORE_INDEX_DRIFT:{name}"
            )


def _reject_retired_collections(names: set[str]) -> None:
    retired = names.intersection(PRESENTATION_RETIRED_COLLECTIONS)
    if retired:
        raise PresentationStoreBaselineError(
            "PRESENTATION_STORE_RETIRED_COLLECTION_PRESENT:"
            + ",".join(sorted(retired))
        )


async def ensure_presentation_store_baseline(
    database: AsyncDatabase[dict[str, object]],
) -> None:
    """Create an empty exact baseline or verify an existing one exactly."""

    names = set(await database.list_collection_names())
    _reject_retired_collections(names)
    present = names.intersection(PRESENTATION_COLLECTIONS)
    if not present:
        try:
            await _create_baseline_transactionally(database)
        except PyMongoError:
            # A concurrent first-start transaction may have committed the exact
            # baseline. Only that complete verified state is accepted.
            names = set(await database.list_collection_names())
            _reject_retired_collections(names)
            if names.intersection(PRESENTATION_COLLECTIONS) != set(
                PRESENTATION_COLLECTIONS
            ):
                raise
        _reject_retired_collections(set(await database.list_collection_names()))
        await _verify_baseline(database)
        return
    if present != set(PRESENTATION_COLLECTIONS):
        raise PresentationStoreBaselineError("PRESENTATION_STORE_MIXED_BASELINE")
    await _verify_baseline(database)


__all__ = [
    "AGENT_PRESENTATION_ADMISSION_COMMAND_RECEIPT_COLLECTION",
    "AGENT_PRESENTATION_DELIVERY_RECORD_COLLECTION",
    "AGENT_PRESENTATION_DELIVERY_STATE_COLLECTION",
    "AGENT_PRESENTATION_PLANNER_STATE_COLLECTION",
    "AGENT_PRESENTATION_SOURCE_COMMIT_COLLECTION",
    "PRESENTATION_COLLECTIONS",
    "PRESENTATION_RETIRED_COLLECTIONS",
    "PRESENTATION_STORE_BASELINE_DIGEST",
    "PRESENTATION_STORE_BASELINE_REVISION",
    "PresentationStoreBaselineError",
    "canonical_store_baseline_bytes",
    "ensure_presentation_store_baseline",
]
