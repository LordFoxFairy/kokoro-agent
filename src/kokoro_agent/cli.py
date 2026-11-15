"""Operator commands for inspecting the Agent and installing its schema."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from collections.abc import Mapping, Sequence

from kokoro_agent.features import FEATURE_CATALOG
from kokoro_agent.inspect import describe_feature, render_catalog
from kokoro_agent.application.schema import (
    apply_database_schema,
    db_apply_schema_main as _schema_db_apply_schema_main,
    expand_model_financial_close,
)

from kokoro_agent.application.model_financial_close import close_unreserved
from kokoro_agent.model.financial_close import ModelFinancialCloseCommand


def main(
    argv: Sequence[str] | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(prog="kokoro-agent")
    subcommands = parser.add_subparsers(dest="command", required=True)
    inspect_parser = subcommands.add_parser(
        "inspect", help="inspect Agent/Feature declarations"
    )
    inspect_parser.add_argument("feature", nargs="?")
    inspect_parser.add_argument("--json", action="store_true", dest="as_json")
    subcommands.add_parser(
        "db:apply-schema",
        help="install the current canonical schema into an empty PostgreSQL namespace",
    )
    subcommands.add_parser(
        "db:expand-model-financial-close",
        help="apply the approved additive close-table expansion; never rebuild existing data",
    )
    close_parser = subcommands.add_parser(
        "model:close-unreserved",
        help="close one exact terminal, never-dispatched original budget command",
    )
    for field in (
        "tenant-id",
        "run-id",
        "attempt-id",
        "evidence-event-id",
        "evidence-revision",
        "evidence-digest",
    ):
        close_parser.add_argument("--" + field, required=True)
    args = parser.parse_args(argv)

    if args.command == "db:expand-model-financial-close":
        asyncio.run(expand_model_financial_close(environment))
        print("verified additive model financial close schema")
        return 0
    if args.command == "model:close-unreserved":
        command = ModelFinancialCloseCommand(
            tenant_id=args.tenant_id,
            run_id=args.run_id,
            attempt_id=args.attempt_id,
            evidence_event_id=args.evidence_event_id,
            evidence_revision=args.evidence_revision,
            evidence_digest=args.evidence_digest,
        )
        receipt = asyncio.run(close_unreserved(environment, command))
        print(json.dumps(asdict(receipt), ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "db:apply-schema":
        if environment is None:
            return db_apply_schema_main()
        asyncio.run(apply_database_schema(environment))
        print("installed canonical Agent schema")
        return 0

    if args.feature is not None:
        description = describe_feature(FEATURE_CATALOG, args.feature)
        print(
            description.model_dump_json(indent=2)
            if args.as_json
            else f"{description.key}: {', '.join(description.agents)} "
            f"(entry: {description.entry_agent})"
        )
        return 0
    print(render_catalog(FEATURE_CATALOG, output="json" if args.as_json else "text"))
    return 0


def db_apply_schema_main() -> int:
    """Console-script entry point for deployments that expose one DB command."""

    return _schema_db_apply_schema_main()


__all__ = ["apply_database_schema", "db_apply_schema_main", "main"]
