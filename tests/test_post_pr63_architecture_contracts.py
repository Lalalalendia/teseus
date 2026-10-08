from __future__ import annotations

import ast
import sys
from dataclasses import fields
from dataclasses import replace
from pathlib import Path

from theseus_contracts import RemoteExecutionResult

from post_pr63_helpers import remote_fixture


ROOT = Path(__file__).resolve().parents[1]


def _source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _tree(relative: str) -> ast.Module:
    return ast.parse(_source(relative), filename=relative)


def _called_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                names.add(node.func.attr)
            elif isinstance(node.func, ast.Name):
                names.add(node.func.id)
    return names


def test_fresh_process_is_the_only_execution_boundary() -> None:
    paths = (
        "theseus_local/execution_backend.py",
        "theseus_local/remote_worker.py",
        "theseus_local/remote_campaign.py",
    )
    for relative in paths:
        tree = _tree(relative)
        source = _source(relative)
        assert "pytest.main" not in source
        assert "sys.modules" not in source
        assert "persistent interpreter" not in source.lower()
        assert "subprocess" not in source
        assert "pytest" not in {name.lower() for name in _called_names(tree)}


def test_remote_result_carries_physical_facts_not_semantic_authority() -> None:
    names = {item.name for item in fields(RemoteExecutionResult)}
    assert {"status", "killed", "survived", "authoritative"}.isdisjoint(names)
    assert {"execution_attempt_id", "evidence_identity", "mutation_identity"} <= names


def test_remote_path_reuses_execution_backend_without_a_second_mutation_runner() -> None:
    tree = _tree("theseus_local/remote_worker.py")
    imported = {
        alias.name if alias.asname is None else alias.asname
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "theseus_local.execution_backend"
        for alias in node.names
    }
    assert "LocalProcessBackend" in imported
    source = _source("theseus_local/remote_campaign.py")
    assert "RemoteWorkerRuntime" in source
    assert "MutationCampaignService" not in source


def test_remote_protocol_path_has_no_pickle_family_serializer() -> None:
    for relative in (
        "theseus_contracts/remote_protocol.py",
        "theseus_local/remote_protocol.py",
        "theseus_local/remote_worker.py",
        "theseus_local/distributed.py",
    ):
        source = _source(relative).lower()
        for forbidden in ("pickle", "cloudpickle", "dill"):
            assert forbidden not in source


def test_ai_and_operations_are_outside_authority_modules() -> None:
    authority_paths = (
        "theseus_local/coordinator.py",
        "theseus_local/distributed.py",
        "theseus_local/remote_worker.py",
        "theseus_local/artifact_store.py",
        "theseus_local/acceptance.py",
    )
    for relative in authority_paths:
        source = _source(relative)
        assert "ai_analysis" not in source
        assert "ai_mutation" not in source

    operations = _tree("theseus_local/operations.py")
    forbidden_calls = {"write_text", "write_bytes", "unlink", "replace", "remove", "rename"}
    assert forbidden_calls.isdisjoint(_called_names(operations))


def test_operations_projection_does_not_create_a_second_state_store() -> None:
    source = _source("theseus_local/operations.py")
    assert "sqlite" not in source.lower()
    assert "scheduler" not in source.lower()


def test_runtime_fresh_process_invariant_is_observable(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    first = replace(
        request,
        execution_attempt_id="fresh-a",
        evidence_identity="fresh-a",
        argv=(sys.executable, "-c", "import os; print(os.getpid())"),
    )
    second = replace(
        request,
        execution_attempt_id="fresh-b",
        evidence_identity="fresh-b",
        argv=(sys.executable, "-c", "import os; print(os.getpid())"),
    )
    first_result = worker.execute(first)
    second_result = worker.execute(second)
    assert first_result.stdout_artifact_id and second_result.stdout_artifact_id
    first_pid = store.get_bytes(first_result.stdout_artifact_id).decode("utf-8").strip()
    second_pid = store.get_bytes(second_result.stdout_artifact_id).decode("utf-8").strip()
    assert first_pid.isdigit() and second_pid.isdigit()
    assert first_pid != second_pid
