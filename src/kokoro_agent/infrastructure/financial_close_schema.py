"""Approved one-time additive expansion, using only the canonical close-table DDL."""

from __future__ import annotations

from typing import Any

import psycopg
from kokoro_agent.distribution_assets import canonical_ddl_path, read_canonical_ddl

_TABLE = "kokoro_agent_model_financial_close"


def financial_close_ddl() -> str:
    canonical = read_canonical_ddl(canonical_ddl_path())
    start, end = "-- BEGIN MODEL FINANCIAL CLOSE\n", "-- END MODEL FINANCIAL CLOSE\n"
    if canonical.count(start) != 1 or canonical.count(end) != 1:
        raise ValueError("canonical financial close DDL block is missing or ambiguous")
    return canonical.split(start, 1)[1].split(end, 1)[0]


async def verify_financial_close_schema(
    conn: psycopg.AsyncConnection[Any], schema: str
) -> None:
    expected = {
        "id": ("uuid", "NO", None, None),
        "tenant_id": ("text", "NO", None, None),
        "run_id": ("text", "NO", None, None),
        "attempt_id": ("uuid", "NO", None, None),
        "event_id": ("uuid", "NO", None, None),
        "revision": ("bigint", "NO", None, None),
        "evidence_digest": ("text", "NO", None, None),
        "actor_ref": ("text", "NO", None, None),
        "receipt_bytes": ("bytea", "NO", None, None),
        "receipt_digest": ("text", "NO", None, None),
        "created_at": ("timestamp with time zone", "NO", "CURRENT_TIMESTAMP(3)", 3),
    }
    checks = {
        "kokoro_agent_model_financial_close_pkey": "PRIMARY KEY (id)",
        "uq_model_financial_close_attempt": "UNIQUE (tenant_id, attempt_id)",
        "ck_model_financial_close_identity": "CHECK (((length(TRIM(BOTH FROM tenant_id)) > 0) AND (length(run_id) > 0) AND (length(actor_ref) > 0) AND (revision > 0) AND (evidence_digest ~ '^[0-9a-f]{64}$'::text)))",
        "ck_model_financial_close_receipt": "CHECK ((((octet_length(receipt_bytes) >= 1) AND (octet_length(receipt_bytes) <= 16384)) AND (receipt_digest ~ '^[0-9a-f]{64}$'::text)))",
    }
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT column_name,data_type,is_nullable,column_default,datetime_precision FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (schema, _TABLE),
        )
        actual = {
            row["column_name"]: (
                row["data_type"],
                row["is_nullable"],
                row["column_default"],
                row["datetime_precision"],
            )
            for row in await cur.fetchall()
        }
        if actual != expected:
            raise ValueError(
                "Agent financial close columns differ from canonical schema"
            )
        await cur.execute(
            "SELECT c.conname,pg_get_constraintdef(c.oid) AS definition FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=%s AND t.relname=%s AND c.contype<>'n'",
            (schema, _TABLE),
        )
        if {
            row["conname"]: row["definition"] for row in await cur.fetchall()
        } != checks:
            raise ValueError(
                "Agent financial close constraints differ from canonical schema"
            )
        await cur.execute(
            "SELECT i.relname,x.indisvalid,x.indisready,x.indisunique,pg_get_expr(x.indpred,x.indrelid) AS predicate,pg_get_indexdef(x.indexrelid,0,true) AS definition FROM pg_index x JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_class i ON i.oid=x.indexrelid WHERE n.nspname=%s AND t.relname=%s",
            (schema, _TABLE),
        )
        rows = await cur.fetchall()
        expected_indexes = {
            "kokoro_agent_model_financial_close_pkey": "(id)",
            "uq_model_financial_close_attempt": "(tenant_id, attempt_id)",
        }
        if len(rows) != 2 or any(
            row["relname"] not in expected_indexes
            or not all(
                row[key] is True for key in ("indisvalid", "indisready", "indisunique")
            )
            or row["predicate"] is not None
            or not row["definition"].endswith(
                " USING btree " + expected_indexes[row["relname"]]
            )
            for row in rows
        ):
            raise ValueError(
                "Agent financial close indexes differ from canonical schema"
            )
