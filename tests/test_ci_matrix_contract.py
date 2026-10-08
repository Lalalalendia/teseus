import json
from pathlib import Path

from test_intelligence_unified_v1 import cli
from test_intelligence_unified_v1.maintenance import ci_matrix


def test_ci_matrix_exposes_required_and_optional_lanes() -> None:
    # Keep CI lane names, marker expressions, validation and coverage thresholds stable.
    value = ci_matrix()
    lanes = {item["name"]: item for item in value["lanes"]}

    assert value["schema_version"] == 1
    assert set(lanes) == {
        "fast",
        "full",
        "performance",
        "slow",
        "coverage",
        "validation",
        "release",
        "scale",
        "soak",
        "mutation",
    }
    assert lanes["fast"]["required"] is True
    assert lanes["fast"]["markers"] == "not performance and not slow"
    assert lanes["coverage"]["fail_under"] == 78
    assert lanes["coverage"]["optional_dependency"] == "coverage"
    assert lanes["coverage"]["markers"] == "all"
    assert lanes["performance"]["machine_dependent"] is True
    assert lanes["validation"]["machine_dependent"] is True
    assert lanes["validation"]["required"] is False
    assert lanes["release"]["required_environment"] == "THESEUS_RELEASE_WHEEL"
    assert lanes["scale"]["markers"] == "scale"
    assert lanes["soak"]["environment"]["THESEUS_RUN_SOAK"] == "1"
    assert lanes["mutation"]["required"] is True


def test_ci_matrix_cli_is_read_only_and_json_serializable(capsys) -> None:
    # Publish the same contract through the CLI without probing the project or filesystem.
    code = cli.main(["ci-matrix"])
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert value == ci_matrix()


def test_pytest_lane_configuration_is_present() -> None:
    # Keep marker and coverage configuration available for direct package-level CI invocations.
    config_path = Path(__file__).resolve().parents[1] / "pytest.ini"
    text = config_path.read_text(encoding="utf-8")
    coverage_path = config_path.parent / ".coveragerc"
    coverage_text = coverage_path.read_text(encoding="utf-8")

    assert "performance:" in text
    assert "slow:" in text
    assert "fail_under = 78" in coverage_text
