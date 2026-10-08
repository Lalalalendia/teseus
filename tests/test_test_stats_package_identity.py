from pathlib import Path
import test_intelligence_unified_v1
from test_intelligence_unified_v1 import test_stats


def test_test_stats_package_has_one_canonical_source() -> None:
    # Ensure normal Python and xdist workers resolve the same editable-package files.
    root = Path(__file__).resolve().parents[1]
    assert test_intelligence_unified_v1.__file__ is not None
    assert Path(test_intelligence_unified_v1.__file__).resolve() == (root / "__init__.py").resolve()
    assert Path(test_stats.__file__).resolve() == (root / "test_stats.py").resolve()
    assert not (root / "test_intelligence_unified_v1").exists()
