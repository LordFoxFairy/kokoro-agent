"""Source-checkout wrapper for the installed contract checker."""

from kokoro_agent.contract_check import main


if __name__ == "__main__":
    raise SystemExit(main())
