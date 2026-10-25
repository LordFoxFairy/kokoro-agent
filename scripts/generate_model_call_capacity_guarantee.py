"""Derive this capacity guarantee's model and immutable source manifest."""

from __future__ import annotations
import argparse
from pathlib import Path
from kokoro_agent.model_call_capacity_guarantee_contract import generate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generate(args.root, check=args.check)


if __name__ == "__main__":
    main()
