from __future__ import annotations
from types import SimpleNamespace
from theseus_contracts import CampaignBudget
from theseus_planner.planner import CampaignPlanner, _Candidate

def _candidate(mutant_id: str, *, function_key: str = "choose") -> _Candidate:
    # Build one planner candidate without coupling the cost tests to mutation discovery.
    mutant = SimpleNamespace(
        mutant_id=SimpleNamespace(value=mutant_id),
        mutation="condition_to_not",
    )
    return _Candidate(mutant=mutant, priority=100, reasons=("test",), function_key=function_key)

def test_empty_selection_never_creates_a_synthetic_shard() -> None:
    # Preserve an empty execution plan as zero shards rather than one empty ownership record.
    assert CampaignPlanner._shards((), 4) == ()

def test_wall_and_cpu_budgets_are_applied_as_distinct_dimensions() -> None:
    # Let parallel wall time fit while enforcing the independent total CPU ceiling.
    planner = CampaignPlanner()
    candidates = (_candidate("m1"), _candidate("m2"), _candidate("m3"))
    wall_budget = CampaignBudget(max_workers=2, max_seconds=1.0, max_cpu_seconds=10.0)
    selected_wall, excluded_wall = planner._apply_budget(
        wall_budget,
        candidates,
        cost_observations={"m1": 1.0, "m2": 1.0, "m3": 1.0},
    )
    assert [item.mutant.mutant_id.value for item in selected_wall] == ["m1", "m2"]
    assert excluded_wall[0]["reason"] == "budget:max-seconds"
    cpu_budget = CampaignBudget(max_workers=4, max_seconds=10.0, max_cpu_seconds=2.0)
    selected_cpu, excluded_cpu = planner._apply_budget(
        cpu_budget,
        candidates,
        cost_observations={"m1": 1.0, "m2": 1.0, "m3": 1.0},
    )
    assert [item.mutant.mutant_id.value for item in selected_cpu] == ["m1", "m2"]
    assert excluded_cpu[0]["reason"] == "budget:max-cpu-seconds"
