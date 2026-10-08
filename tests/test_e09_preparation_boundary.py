from __future__ import annotations

import ast
from pathlib import Path

import test_intelligence_unified_v1.engine as engine_module
from test_intelligence_unified_v1 import workers as legacy_workers
from test_intelligence_unified_v1.preparation_service import PreparedCampaign, prepare_campaign


def test_engine_does_not_import_legacy_worker_runtime() -> None:
    # Prevent the public engine facade from regaining a dependency on legacy worker orchestration.
    source_path = Path(engine_module.__file__).resolve()
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    legacy_imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if (node.level == 1 and node.module == "workers") or (
                node.level == 0 and node.module == "test_intelligence_unified_v1.workers"
            ):
                legacy_imports.append((node.lineno, node.module))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "test_intelligence_unified_v1.workers":
                    legacy_imports.append((node.lineno, alias.name))
    assert legacy_imports == []


def test_legacy_workers_reexport_extracted_preparation_contract() -> None:
    # Keep historical imports compatible while preparation has one implementation owner.
    assert legacy_workers.PreparedCampaign is PreparedCampaign
    assert legacy_workers._prepare_campaign is prepare_campaign
