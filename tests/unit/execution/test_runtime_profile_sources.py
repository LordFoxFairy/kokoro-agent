"""Production provenance is explicit, selected, and readable from an installed package."""

from importlib import import_module
from dataclasses import replace
import pytest


def test_production_manifest_has_real_resource_closures() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    assert manifest.sources
    assert len({s.source_id for s in manifest.sources}) == len(manifest.sources)
    for source in manifest.sources:
        descriptor = manifest.descriptor(source.source_id)
        assert descriptor["files"]
    assert manifest.descriptor("native")["version"] == "0.6.6"
    assert manifest.dynamic_edges


def test_unknown_duplicate_and_missing_source_fail_closed() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    with pytest.raises(ValueError, match="unknown"):
        manifest.descriptor("unregistered")
    with pytest.raises(ValueError, match="duplicate"):
        replace(manifest, sources=(*manifest.sources, manifest.sources[0]))
    broken = replace(manifest.sources[0], resource_paths=("missing-source.py",))
    with pytest.raises(ValueError, match="unavailable"):
        replace(manifest, sources=(broken,)).descriptor(broken.source_id)


@pytest.mark.parametrize(
    "package", ["kokoro_agent", "deepagents", "langchain", "langgraph_swarm"]
)
def test_registered_package_import_closure(package: str) -> None:
    import ast
    from importlib.resources import files

    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    root = files(package)
    paths = {
        p
        for source in manifest.sources
        if source.package == package
        for p in source.resource_paths
    }
    missing: set[str] = set()
    for path in paths:
        if not path.endswith(".py"):
            continue
        for node in ast.walk(ast.parse(root.joinpath(path).read_text())):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ""
                if node.level:
                    parts = [package, *path.split("/")[:-1]]
                    prefix = ".".join(
                        parts[: len(parts) - node.level + 1]
                        + ([prefix] if prefix else [])
                    )
                modules.append(prefix)
                modules.extend(prefix + "." + alias.name for alias in node.names)
            for name in modules:
                if not name.startswith(package + "."):
                    continue
                stem = name.removeprefix(package + ".").replace(".", "/")
                target = next(
                    (
                        p
                        for p in (stem + ".py", stem + "/__init__.py")
                        if root.joinpath(p).is_file()
                    ),
                    None,
                )
                if target is not None and target not in paths:
                    missing.add(target)
    assert not missing, f"missing explicit resource closure: {sorted(missing)}"


def test_source_bytes_change_descriptor_without_git_or_runtime_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.execution import runtime_profile

    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    first = manifest.descriptor("memory")
    from importlib.resources import files

    root = files("kokoro_agent")

    class AlteredResource:
        def joinpath(self, path: str):
            self.path = path
            return self

        def read_bytes(self) -> bytes:
            return (
                root.joinpath(self.path).read_bytes() + b"\n# approved source changed\n"
            )

    def altered_files(package: object) -> AlteredResource:
        return AlteredResource()

    monkeypatch.setattr(runtime_profile.resources, "files", altered_files)
    assert manifest.descriptor("memory") != first


def test_unapproved_distribution_version_fails_closed() -> None:
    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    with pytest.raises(ValueError, match="version"):
        replace(manifest, versions=(("deepagents", "0.0.0"),)).descriptor("native")


def test_pinned_runtime_distribution_dependency_closure_is_complete() -> None:
    from importlib import metadata
    from packaging.requirements import Requirement

    module = import_module("kokoro_agent.execution.runtime_profile_sources")
    manifest = module.production_manifest()
    approved = {name.lower().replace("_", "-") for name, _ in manifest.versions}
    pending: list[tuple[str, frozenset[str]]] = [("kokoro-agent", frozenset())]
    visited: set[tuple[str, frozenset[str]]] = set()
    missing: set[str] = set()
    while pending:
        name, extras = pending.pop()
        if (name, extras) in visited:
            continue
        visited.add((name, extras))
        for raw in metadata.requires(name) or ():
            dependency = Requirement(raw)
            if dependency.marker is not None and not any(
                dependency.marker.evaluate({"extra": extra}) for extra in ("", *extras)
            ):
                continue
            key = dependency.name.lower().replace("_", "-")
            if key not in approved:
                missing.add(key)
            pending.append((key, frozenset(dependency.extras)))
    assert not missing, f"unregistered runtime leaf dependencies: {sorted(missing)}"
