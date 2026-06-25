"""Mongo 持久化实现：跨 pod 共享的 run 状态存储（原子去重 / 终态认领）。"""

from __future__ import annotations

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from kokoro_agent.domain.run_request import RunRequest


class MongoRunStateStore:
    """单 collection、以 run_id 为 _id：upsert 与条件 update 提供跨 pod 原子认领。"""

    def __init__(self, collection: AsyncCollection[dict[str, object]]) -> None:
        self._coll = collection

    async def try_register(self, request: RunRequest) -> bool:
        # $setOnInsert + upsert：仅 _id 不存在时写入，upserted_id 非空即本次认领成功。
        # 并发 upsert 同一 _id 时输者可能抛 DuplicateKeyError（mongo 文档明载的 upsert 竞态）→
        # 视为已被他人认领，与 try_mark_terminal 同一道防线。
        try:
            result = await self._coll.update_one(
                {"_id": request.run_id},
                {"$setOnInsert": {"request_json": request.model_dump_json(), "terminal": False}},
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.upserted_id is not None

    async def get_request(self, run_id: str) -> RunRequest | None:
        # 取原 request 供 resume 重建 agent；终态-only 文档无 request_json → None。
        doc = await self._coll.find_one({"_id": run_id})
        if doc is None:
            return None
        raw = doc.get("request_json")
        if not isinstance(raw, str):
            return None
        return RunRequest.model_validate_json(raw)

    async def try_mark_terminal(self, run_id: str) -> bool:
        # 条件 update + upsert：terminal!=True 时置位；已终态则过滤不中、upsert 撞 _id 抛 Duplicate→已被认领。
        try:
            result = await self._coll.update_one(
                {"_id": run_id, "terminal": {"$ne": True}},
                {"$set": {"terminal": True}},
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.modified_count == 1 or result.upserted_id is not None

    async def is_terminal(self, run_id: str) -> bool:
        # 只读查：resume stale 闸。
        return await self._coll.find_one({"_id": run_id, "terminal": True}) is not None


def make_mongo_collection(url: str, db: str) -> tuple[AsyncMongoClient[dict[str, object]], AsyncCollection[dict[str, object]]]:
    # 工厂用：建客户端并取 run_state collection；调用方负责 client 生命周期。
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(url)
    return client, client[db]["run_state"]
