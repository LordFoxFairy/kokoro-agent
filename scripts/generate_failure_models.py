"""Compile the two Agent-owned safe failure profiles from canonical OpenAPI."""

from __future__ import annotations

import argparse
from pathlib import Path

from kokoro_agent.chat_contract_check import generate_failure_models


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generate_failure_models(args.root, check=args.check)


if __name__ == "__main__":
    main()
