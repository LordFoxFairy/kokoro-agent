---
architectureIndex: 1
rootId: agent.skills
owners:
  - "@LordFoxFairy"
---

# skills — run-scoped Skill consumption

## Responsibilities

Agent consumes the exact Skill grants selected for one run. `HubExecutionAssemblyClient`
resolves and verifies the assembly, streams content-locked archives into a local
content-addressed cache, then constructs an immutable `SkillHub`. This package validates
`SKILL.md`, serves exact `(scope, name, content_hash)` reads, and materializes attachments into
the run backend.

## Non-responsibilities

Catalog CRUD, publishing, review, enablement, Site entitlement, revision selection, artifact
storage, and secret authority belong to Platform Hub. Agent must not read or write Hub Mongo,
Hub object storage, or deployment seed directories.

## Public boundary

- `hub.py`: `SkillHub`, `SkillHubError`, archive/package validation, and content hashing.
- `materialize.py`: one-time package reconciliation into the run backend.
- `package.py`: strict `SKILL.md` frontmatter parsing.
- `supply.py`: backend materialization protocol and `/.skills/` layout.
- `__init__.py`: the supported cross-package imports.

`PackageStore` remains a private compatibility-free implementation detail for the independent
delivery tool. It is not a catalog or Skill authority and must not grow Hub behavior.

## Runtime invariants

- A run resolves one assembly before tool construction; every read is pinned to the granted
  content hash.
- Archive paths, file types, encoding, compression ratio, file count, expanded size,
  description, and content hash are validated before exposure.
- Cached packages are revalidated on every load; callers only receive copies.
- Missing, mismatched, revoked, or unavailable grants fail closed. There is no local fallback.
- Materialization state is a `{name: content_hash}` ledger, so retries are idempotent and stale
  directories are reconciled.

## Callers and dependencies

- `hub/client.py` constructs a run-scoped `SkillHub` from the Platform RPC assembly.
- `agents/assembly/` renders Skill bodies and attaches materialization middleware.
- `tools/skills.py` exposes exact granted bodies and package files.
- `worker/main.py` owns the process-scoped RPC client only; it never seeds Skills.

## Verification

Run `uv run pytest -q tests/test_hub_assembly_client.py tests/test_skill_tools.py` and
`uv run pytest -q tests/test_skill_materialize.py -k 'not ledger_survives_resume'`, followed by
`uv run ruff check .` and `uv run pyright`. The excluded resume test requires the normal Mongo
development infrastructure.
