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
from kokoro_agent.contract.storage import RUN_DISPATCHES_COLLECTION

# 不可解析帧死信集合（R1 quarantine 简版；identity 感知的畸形帧留 R5）。
DISPATCH_DLQ_COLLECTION = "dispatch_dlq"


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


class _ControlInboxEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    decision_id: str
    fingerprint: str | None
    status: str
    body: str


_CONTROL_INBOX_ADAPTER: TypeAdapter[list[_ControlInboxEntry]] = TypeAdapter(
    list[_ControlInboxEntry]
)


class ControlInboxRecord(BaseModel):
    """R2 control inbox 待续办条目：persisted 未 applied 的 resume/cancel（重启 scanner 消费）。

    定义在低层 mongo 模块以避免 ledger↔mongo 循环；ledger 面再导出为契约类型。
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    run_id: str
    decision_id: str
    # 记录时的 interrupt 指纹（resume 有值；cancel 无 interrupt 依赖=None）：
    # 重启续办按此校验当前 interrupt 是否仍匹配，不匹配即 stale→superseded。
    fingerprint: str | None
    # 待续办的 control 帧原文（JSON）：重启逐字重放 apply。
    body: str


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
        # dispatch CAS 与死信同库兄弟集合（session 在 message-store 库写 run_dispatches；
        # gate/closure env 令两库同名同实例，单文档 CAS 跨进程可见）。
        self._dispatches = collection.database[RUN_DISPATCHES_COLLECTION]
        self._dlq = collection.database[DISPATCH_DLQ_COLLECTION]

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
                        # run.started 最小 outbox（R1）：claim 即写未发布行，emit 后置 True，
                        # 启动 scanner 补发 unpublished（幂等：session 不投影 run.started）。
                        "run_started_published": False,
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            return False
        return result.upserted_id is not None

    async def claim_dispatch(self, run_id: str, consumer: str) -> bool:
        # dispatch CAS（D5）：pending→claimed 单文档条件转移。赢=授权执行；已 claimed/expired
        # =重复投递/迟到帧丢弃；无记录=兼容放行（迁移/无 intent 期不误杀 run，执行去重仍由
        # try_claim 兜底）。deadline_at>now 是与 session 超时 reconciler 的赛跑闸：deadline 已过
        # 只能被 reconciler 转 expired，agent 不再认领。
        now = self._clock()
        won = await self._dispatches.find_one_and_update(
            {"run_id": run_id, "status": "pending", "deadline_at": {"$gt": now}},
            {"$set": {"status": "claimed", "claimed_by": consumer, "updated_at": now}},
        )
        if won is not None:
            return True
        exists = await self._dispatches.find_one({"run_id": run_id}, {"_id": 1})
        return exists is None

    async def quarantine_dispatch(self, raw_hash: str, source: str, reason: str) -> None:
        # 不可解析帧死信：记录后由调用方 ACK（坏帧无 identity 不重投）。schema 局部定义（R5 重整）。
        await self._dlq.insert_one(
            {"raw_hash": raw_hash, "source": source, "reason": reason, "at": self._clock()}
        )

    async def list_unpublished_started(self) -> list[str]:
        # run.started outbox 补发扫描：claim 落库但发布未确认且非终态的 run。
        cursor = self._coll.find(
            {"terminal": {"$ne": True}, "run_started_published": False},
            {"_id": 1},
        ).sort("_id", 1)
        return [str(doc["_id"]) async for doc in cursor]

    async def mark_started_published(self, run_id: str) -> None:
        await self._coll.update_one(
            {"_id": run_id}, {"$set": {"run_started_published": True}}
        )

    async def record_control_inbox(
        self, run_id: str, decision_id: str, fingerprint: str | None, body: str
    ) -> bool:
        # keep-first：$ne 数组守卫（同 add_steer 模式）——仅当无同 decision_id 条目时 push。
        # modified_count==1=首次落库（persisted）；0=重复 decision_id 或 run 文档缺失→丢弃不重放。
        result = await self._coll.update_one(
            {"_id": run_id, "control_inbox.decision_id": {"$ne": decision_id}},
            {
                "$push": {
                    "control_inbox": {
                        "decision_id": decision_id,
                        "fingerprint": fingerprint,
                        "status": "persisted",
                        "body": body,
                    }
                }
            },
        )
        return result.modified_count == 1

    async def mark_control_applied(self, run_id: str, decision_id: str) -> None:
        # 仅 persisted→applied 前向推进（positional $ 命中 elemMatch 元素）。
        await self._coll.update_one(
            {
                "_id": run_id,
                "control_inbox": {"$elemMatch": {"decision_id": decision_id, "status": "persisted"}},
            },
            {"$set": {"control_inbox.$.status": "applied"}},
        )

    async def mark_control_superseded(self, run_id: str, decision_id: str) -> None:
        await self._coll.update_one(
            {
                "_id": run_id,
                "control_inbox": {"$elemMatch": {"decision_id": decision_id, "status": "persisted"}},
            },
            {"$set": {"control_inbox.$.status": "superseded"}},
        )

    async def list_pending_control_inbox(self) -> list[ControlInboxRecord]:
        # 重启补办扫描：非终态 run 里 persisted 未 applied 的 control 条目。
        cursor = self._coll.find(
            {"terminal": {"$ne": True}, "control_inbox.status": "persisted"},
            {"_id": 1, "control_inbox": 1},
        ).sort("_id", 1)
        records: list[ControlInboxRecord] = []
        async for doc in cursor:
            entries = _CONTROL_INBOX_ADAPTER.validate_python(doc.get("control_inbox") or [])
            for entry in entries:
                if entry.status != "persisted":
                    continue
                records.append(
                    ControlInboxRecord(
                        run_id=str(doc["_id"]),
                        decision_id=entry.decision_id,
                        fingerprint=entry.fingerprint,
                        body=entry.body,
                    )
                )
        return records

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
