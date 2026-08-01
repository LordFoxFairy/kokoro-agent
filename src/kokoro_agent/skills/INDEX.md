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
`SKILL.md`, serves exact `(scope, name, content_hash)` reads, and assembles immutable native
DeepAgents skill directories before graph construction.

## Non-responsibilities

Catalog CRUD, publishing, review, enablement, Site entitlement, revision selection, artifact
storage, and secret authority belong to Platform Hub. Agent must not read or write Hub Mongo,
Hub object storage, or deployment seed directories.

## Public boundary

- `hub.py`: `SkillHub`, `SkillHubError`, archive/package validation, and content hashing.
- `materialize.py`: fail-loud native assembly into `/.skills/assemblies/<package-digest>/` and
  explicit `CompositeBackend` routing.
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
- The canonical package digest binds the exact ordered package facts. Every uploaded byte is
  read back and verified before `create_deep_agent`; missing, corrupt, or unwritable packages
  fail before the model can run.
- `skills_metadata` and load errors are native DeepAgents private, untracked state. They may be
  rebuilt after checkpoint recovery and are never a second product-owned Skill ledger.

## Callers and dependencies

- `hub/client.py` constructs a run-scoped `SkillHub` from the Platform RPC assembly.
- `agents/assembly/` materializes packages before graph creation and passes the assembly root
  through native `skills=`. Empty grants pass `None`.
- `worker/main.py` owns the process-scoped RPC client only; it never seeds Skills.

## Verification

Run `uv run pytest -q tests/test_hub_assembly_client.py tests/test_skill_materialize.py
tests/test_build_agent_native.py`, followed by `uv run ruff check .` and `uv run pyright`.
The checkpoint rebuild case uses the normal Mongo development fixture.
