"""Mongo 后端：跨 pod 共享的 run 状态存储，$setOnInsert/条件更新给原子认领。"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from pydantic import BaseModel, ConfigDict, TypeAdapter

from kokoro_agent.contract import RunRequest


def _now_ms() -> int:
    return int(time.time() * 1000)


class _ToolResultEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    result: str
    is_error: bool


_TOOL_RESULTS_ADAPTER: TypeAdapter[dict[str, _ToolResultEntry]] = TypeAdapter(
    dict[str, _ToolResultEntry]
)


class _SteerEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    message_id: str
    content: str


_STEERS_ADAPTER: TypeAdapter[list[_SteerEntry]] = TypeAdapter(list[_SteerEntry])


class MongoLedger:
    """单 collection、以 run_id 为 _id：upsert 与条件 update 提供跨 pod 原子认领。"""

    def __init__(
        self,
        collection: AsyncCollection[dict[str, object]],
        *,
        ttl_ms: int,
        clock: Callable[[], int] = _now_ms,
    ) -> None:
        self._coll = collection
        self._ttl_ms = ttl_ms
        self._clock = clock

    async def try_claim(self, request: RunRequest, owner: str) -> bool:
        # $setOnInsert + upsert：仅 _id 不存在时写入；并发 upsert 撞 _id 抛 DuplicateKeyError
        # （mongo 文档明载的 upsert 竞态）→ 视为已被他人认领。
        try:
            result = await self._coll.update_one(
                {"_id": request.run_id},
                {
                    "$setOnInsert": {
                        "request_json": request.model_dump_json(),
                        "terminal": False,
                        "lease_expires_ms": self._clock() + self._ttl_ms,
                        "owner": owner,
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.upserted_id is not None

    async def renew(self, run_id: str, owner: str) -> bool:
        # 严格属主续租（fencing）：owner 不符即失败——假死副本苏醒后据此让渡。
        result = await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}, "owner": owner},
            {"$set": {"lease_expires_ms": self._clock() + self._ttl_ms}},
        )
        return result.matched_count == 1

    async def adopt(self, run_id: str, owner: str) -> None:
        # 所有权交接（resume 收养）：置 owner 并把暂停哨兵拉回活跃租约。
        await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}},
            {"$set": {"lease_expires_ms": self._clock() + self._ttl_ms, "owner": owner}},
        )

    async def pause(self, run_id: str) -> None:
        # null 哨兵：HITL 等人可以是小时级，暂停 run 绝不被过期重拾重跑。
        await self._coll.update_one(
            {"_id": run_id, "terminal": {"$ne": True}},
            {"$set": {"lease_expires_ms": None}},
        )

    async def reclaim_expired(self, owner: str) -> list[RunRequest]:
        now = self._clock()
        reclaimed: list[RunRequest] = []
        while True:
            # find_one_and_update 原子认领：多 pod 并发 reclaim 时每个 run 恰被一个赢家拾走；
            # $lte 数值比较按 BSON 类型分桶，天然不命中 null 暂停哨兵。
            doc = await self._coll.find_one_and_update(
                {
                    "terminal": {"$ne": True},
                    "request_json": {"$type": "string"},
                    "lease_expires_ms": {"$lte": now},
                },
                {"$set": {"lease_expires_ms": now + self._ttl_ms, "owner": owner}},
            )
            if doc is None:
                return reclaimed
            raw = doc.get("request_json")
            if isinstance(raw, str):
                reclaimed.append(RunRequest.model_validate_json(raw))

    async def list_paused(self) -> list[str]:
        cursor = self._coll.find(
            {
                "terminal": {"$ne": True},
                "lease_expires_ms": None,
                "request_json": {"$ne": None},
            },
            {"_id": 1},
        ).sort("_id", 1)
        return [str(doc["_id"]) async for doc in cursor]

    async def add_tokens(self, run_id: str, count: int) -> int:
        doc = await self._coll.find_one_and_update(
            {"_id": run_id},
            {"$inc": {"token_total": count}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        total = doc.get("token_total") if doc else None
        if not isinstance(total, int):
            raise TypeError(f"token_total for {run_id!r} is not an int: {total!r}")
        return total

    async def add_usage(self, run_id: str, input_tokens: int, output_tokens: int) -> tuple[int, int]:
        doc = await self._coll.find_one_and_update(
            {"_id": run_id},
            {"$inc": {"usage_input": input_tokens, "usage_output": output_tokens}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        input_total = doc.get("usage_input") if doc else None
        output_total = doc.get("usage_output") if doc else None
        if not isinstance(input_total, int) or not isinstance(output_total, int):
            raise TypeError(f"usage totals for {run_id!r} are not ints")
        return (input_total, output_total)

    async def get_request(self, run_id: str) -> RunRequest | None:
        doc = await self._coll.find_one({"_id": run_id})
        if doc is None:
            return None
        raw = doc.get("request_json")
        if not isinstance(raw, str):
            return None
        return RunRequest.model_validate_json(raw)

    async def purge_terminal(self, max_age_ms: int) -> int:
        result = await self._coll.delete_many(
            {"terminal": True, "terminal_at_ms": {"$lte": self._clock() - max_age_ms}}
        )
        return int(result.deleted_count)

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 条件 update + upsert：已终态则过滤不中、upsert 撞 _id 抛 Duplicate → 已被认领。
        try:
            result = await self._coll.update_one(
                {"_id": run_id, "terminal": {"$ne": True}},
                {"$set": {"terminal": True, "terminal_at_ms": self._clock()}},
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.modified_count == 1 or result.upserted_id is not None

    async def is_terminal(self, run_id: str) -> bool:
        return await self._coll.find_one({"_id": run_id, "terminal": True}) is not None


    async def put_tool_result(
        self, run_id: str, tool_id: str, result: str, is_error: bool
    ) -> None:
        # keep-first：字段已存在（重入/并发）不覆盖首跑结果。
        await self._coll.update_one(
            {"_id": run_id, f"tool_results.{tool_id}": {"$exists": False}},
            {"$set": {f"tool_results.{tool_id}": {"result": result, "is_error": is_error}}},
        )

    async def add_steer(self, run_id: str, message_id: str, content: str) -> None:
        # 仅更新已认领 run 的文档（filter 含 _id 存在语义）；$ne 数组守卫给 keep-first。
        await self._coll.update_one(
            {"_id": run_id, "steers.message_id": {"$ne": message_id}},
            {"$push": {"steers": {"message_id": message_id, "content": content}}},
        )

    async def peek_steers(self, run_id: str) -> list[tuple[str, str]]:
        doc = await self._coll.find_one({"_id": run_id}, {"steers": 1})
        if doc is None:
            return []
        entries = _STEERS_ADAPTER.validate_python(doc.get("steers") or [])
        return [(entry.message_id, entry.content) for entry in entries]

    async def ack_steers(self, run_id: str, message_ids: list[str]) -> None:
        if not message_ids:
            return
        await self._coll.update_one(
            {"_id": run_id},
            {"$pull": {"steers": {"message_id": {"$in": message_ids}}}},
        )

    async def put_sandbox_id(self, run_id: str, sandbox_id: str) -> None:
        # keep-first：resume 竞态下首个绑定生效（与 put_tool_result 同模式）。
        await self._coll.update_one(
            {"_id": run_id, "sandbox_id": {"$exists": False}},
            {"$set": {"sandbox_id": sandbox_id}},
        )

    async def get_sandbox_id(self, run_id: str) -> str | None:
        doc = await self._coll.find_one({"_id": run_id}, {"sandbox_id": 1})
        if doc is None:
            return None
        value = doc.get("sandbox_id")
        return value if isinstance(value, str) else None

    async def get_tool_result(self, run_id: str, tool_id: str) -> tuple[str, bool] | None:
        doc = await self._coll.find_one({"_id": run_id}, {f"tool_results.{tool_id}": 1})
        if doc is None:
            return None
        # mongo 文档是 Any 边界：整块交 Pydantic 洗净（缓存是本仓自写，脏形状即 fail-loud）。
        raw: Any = doc.get("tool_results")
        if raw is None:
            return None
        entries = _TOOL_RESULTS_ADAPTER.validate_python(raw)
        entry = entries.get(tool_id)
        if entry is None:
            return None
        return (entry.result, entry.is_error)


def make_mongo_collection(
    url: str, db: str
) -> tuple[AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]]:
    # 建客户端并取 ledger collection；调用方负责 client 生命周期。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(url)
    return client, client[db]["ledger"]
