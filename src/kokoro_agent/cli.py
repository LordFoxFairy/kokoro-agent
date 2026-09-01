"""Local development commands for inspecting the GA authored surface."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from kokoro_agent.features import FEATURE_CATALOG
from kokoro_agent.inspect import describe_feature, render_catalog


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kokoro-agent")
    subcommands = parser.add_subparsers(dest="command", required=True)
    inspect_parser = subcommands.add_parser("inspect", help="inspect Agent/Feature declarations")
    inspect_parser.add_argument("feature", nargs="?")
    inspect_parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

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


__all__ = ["main"]
