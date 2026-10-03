"""Generate strict Agent model-usage wire models from the canonical finite schema."""

from __future__ import annotations

import argparse
from pathlib import Path

from kokoro_agent.model_usage_contract import generate_model_usage_artifact


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generate_model_usage_artifact(args.root, check=args.check)


if __name__ == "__main__":
    main()
