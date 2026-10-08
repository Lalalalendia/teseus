"""Local multi-project control plane used by the browser UI.
The normal public API is intentionally scoped to one campaign database.  A
browser session, however, needs one small coordinator above that boundary: it
must remember the projects the operator registered, resolve a campaign ID to
its database, and route each bounded API operation to the corresponding
public client.  This module provides that adapter without exposing private
SQLite or coordinator objects to the HTTP layer.
"""
from __future__ import annotations
import base64
import hashlib
import json
import os
import shlex
import shutil
import sys
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, RLock, Thread
from uuid import uuid4
from theseus_api import ApiError, ApiRejected, ApiSuccess, LocalOperatorActions
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_local.coordinator import LocalCampaignCoordinator
from theseus_local.project_discovery import discover_project
from theseus_local.project_profile import ProjectProfile
from theseus_local.project_run import MAX_PROJECT_RUN_SOURCE_FILES, ProjectRunEntry, ProjectRunRecord, project_source_files, project_test_cwd
from theseus_local.workspace import cleanup_campaign_workspaces, project_tree_logical_bytes
from theseus_performance import ProjectPerformanceSample, ProjectTuningStore
from .client import CampaignCreateRequest, ClientResponse, PublicApiCampaignClient
from .serialization import JsonValue, to_json_value
LOCAL_UI_SCHEMA_VERSION = 1
DEFAULT_UI_STATE_DIR = Path.home() / ".theseus-ui"
MAX_REGISTERED_PROJECTS = 128
MAX_CAMPAIGNS_PER_PROJECT = MAX_PROJECT_RUN_SOURCE_FILES
MAX_PROJECT_RUNS = 1024
MAX_TEST_COMMAND_ARGUMENTS = 64
PROJECT_RUN_INLINE_SOURCE_LIMIT = 8
PROJECT_RUN_POLL_SECONDS = 0.25
PROJECT_RUN_REGISTRY_CHECKPOINT_SIZE = 32
PROJECT_RUN_STATUS_REFRESH_LIMIT = 8
PROJECT_RUN_DETAIL_ENTRY_LIMIT = 200
PROJECT_RUN_EVENT_LIMIT = 100
PROJECT_RUN_DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
PROJECT_RUN_DISK_OVERHEAD_BYTES = 256 * 1024 * 1024
PROJECT_RUN_DISK_SAFETY_FACTOR = 1.25
_PROJECT_RUN_TERMINAL_STATUSES = frozenset({"complete", "completed", "failed", "cancelled", "canceled", "recovered"})
def _default_project_id(root: Path) -> str:
    """Match the stable project identity used by the direct CLI path."""
    digest = hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()[:20]
    return f"project-{digest}"
def _default_campaign_id() -> str:
    # Create one collision-resistant local campaign identity.
    """Create a collision-resistant local campaign identity."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"campaign-{timestamp}-{uuid4().hex[:8]}"
def _default_project_run_id() -> str:
    # Create one collision-resistant project-level run identity.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"project-run-{timestamp}-{uuid4().hex[:8]}"
def _required_text(value: object, field_name: str) -> str:
    """Normalize one bounded local UI string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    current = value.strip()
    if len(current) > 4096:
        raise ValueError(f"{field_name} is too long")
    return current
def _optional_text(value: object, field_name: str) -> str | None:
    """Normalize one optional local UI string."""
    if value is None or value == "":
        return None
    return _required_text(value, field_name)
def _positive_int(value: object, field_name: str, *, default: int | None = None) -> int | None:
    """Parse one bounded positive integer without accepting booleans."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return parsed
def _positive_float(value: object, field_name: str, *, default: float | None = None) -> float | None:
    """Parse one bounded positive floating-point value."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be greater than zero")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be greater than zero") from exc
    if parsed <= 0.0:
        raise ValueError(f"{field_name} must be greater than zero")
    return parsed
def _safe_command(value: object, root: Path, *, cwd: Path | None = None) -> TestCommandDescriptor:
    # Turn a visible command field into a shell-free argv descriptor bound to one safe project cwd.
    if value is None or value == "":
        argv = (sys.executable, "-m", "pytest", "-q")
    elif isinstance(value, list):
        if any(not isinstance(item, str) or not item.strip() for item in value):
            raise ValueError("test_command must contain non-empty strings")
        argv = tuple(item.strip() for item in value)
    elif isinstance(value, str):
        try:
            # ``posix=False`` keeps Windows quoted executable paths intact;
            # shell execution is never enabled by this parser.
            argv = tuple(shlex.split(value, posix=False))
        except ValueError as exc:
            raise ValueError("test_command is not valid argv text") from exc
    else:
        raise ValueError("test_command must be text or an argv array")
    if argv and argv[0].lower() in {"python", "python3", "py"}:
        argv = (sys.executable, *argv[1:])
    if not argv or len(argv) > MAX_TEST_COMMAND_ARGUMENTS:
        raise ValueError("test_command must contain between 1 and 64 arguments")
    return TestCommandDescriptor(argv=argv, cwd=str((cwd or root).resolve()), shell=False)
def _safe_test_cwd(root: Path, value: object) -> Path:
    # Resolve one optional component working directory without allowing checkout escape.
    if value is None or value == "" or value == ".":
        return root
    raw = Path(_required_text(value, "test_cwd")).expanduser()
    target = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
    if not target.is_relative_to(root) or not target.is_dir():
        raise ValueError("test_cwd must be an existing directory inside the project root")
    return target
def _safe_source(root: Path, value: object) -> str:
    """Resolve one project-relative Python source without allowing escape."""
    raw = Path(_required_text(value, "source_path")).expanduser()
    target = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
    if not target.is_relative_to(root):
        raise ValueError("source_path must remain inside the project root")
    if not target.is_file():
        raise ValueError("source_path does not exist")
    if target.suffix.lower() != ".py":
        raise ValueError("source_path must point to a Python .py file")
    return target.relative_to(root).as_posix()
def _cursor_encode(resource: str, value: str) -> str:
    """Encode one opaque registry cursor."""
    payload = json.dumps(
        {"schema_version": LOCAL_UI_SCHEMA_VERSION, "resource": resource, "value": value},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
def _cursor_decode(cursor: str | None, resource: str) -> str | None:
    """Decode one registry cursor and fail closed on mismatched resources."""
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not cursor:
        raise ValueError("cursor is invalid")
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    if not isinstance(value, Mapping) or value.get("schema_version") != LOCAL_UI_SCHEMA_VERSION or value.get("resource") != resource:
        raise ValueError("cursor does not match the requested resource")
    raw = value.get("value")
    if not isinstance(raw, str) or not raw:
        raise ValueError("cursor is invalid")
    return raw
def _page(items: list[dict[str, JsonValue]], limit: int, cursor: str | None, resource: str) -> dict[str, JsonValue]:
    """Return one bounded keyset page over registry rows."""
    position = _cursor_decode(cursor, resource)
    ordered = sorted(items, key=lambda item: str(item.get("id", "")))
    if position is not None:
        ordered = [item for item in ordered if str(item.get("id", "")) > position]
    visible = ordered[:limit]
    next_cursor = _cursor_encode(resource, str(visible[-1]["id"])) if len(ordered) > limit and visible else None
    return {
        "items": [{key: value for key, value in item.items() if key != "id"} for item in visible],
        "limit": limit,
        "next_cursor": next_cursor,
    }
@dataclass(frozen=True, slots=True)
class RegisteredProject:
    """Private registry row for one operator-selected checkout."""
    project_id: str
    display_name: str
    root_path: Path
    reports_dir: Path
    profile: ProjectProfile | None = None
@dataclass(frozen=True, slots=True)
class RegisteredCampaign:
    """Private registry row binding a campaign to its project database."""
    campaign_id: str
    project_id: str
    database_path: Path
class LocalUiRegistry:
    """Small atomic JSON registry for browser-selected projects and campaigns."""
    def __init__(self, state_dir: str | Path) -> None:
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.path = self.state_dir / "registry.json"
        self._lock = RLock()
        self._projects: dict[str, RegisteredProject] = {}
        self._campaigns: dict[str, RegisteredCampaign] = {}
        self._project_runs: dict[str, ProjectRunRecord] = {}
        self._load()
    def _load(self) -> None:
        # Load persisted projects, profiles, and campaign bindings from private UI state.
        """Load only the bounded, path-bearing private registry file."""
        with self._lock:
            if not self.path.is_file():
                return
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                return
            if not isinstance(raw, Mapping):
                return
            projects = raw.get("projects", [])
            campaigns = raw.get("campaigns", [])
            project_runs = raw.get("project_runs", [])
            if isinstance(projects, list):
                for item in projects[:MAX_REGISTERED_PROJECTS]:
                    if not isinstance(item, Mapping):
                        continue
                    try:
                        project_id = _required_text(item.get("project_id"), "project_id")
                        root = Path(_required_text(item.get("root_path"), "root_path")).expanduser().resolve()
                        reports = Path(_required_text(item.get("reports_dir"), "reports_dir")).expanduser().resolve()
                        display = _required_text(item.get("display_name"), "display_name")
                    except ValueError:
                        continue
                    raw_profile = item.get("profile")
                    try:
                        profile = ProjectProfile.from_dict(raw_profile) if isinstance(raw_profile, Mapping) else None
                    except ValueError:
                        profile = None
                    self._projects[project_id] = RegisteredProject(project_id, display, root, reports, profile)
            if isinstance(campaigns, list):
                for item in campaigns[:MAX_CAMPAIGNS_PER_PROJECT]:
                    if not isinstance(item, Mapping):
                        continue
                    try:
                        campaign_id = _required_text(item.get("campaign_id"), "campaign_id")
                        project_id = _required_text(item.get("project_id"), "project_id")
                        database = Path(_required_text(item.get("database_path"), "database_path")).expanduser().resolve()
                    except ValueError:
                        continue
                    if project_id in self._projects:
                        self._campaigns[campaign_id] = RegisteredCampaign(campaign_id, project_id, database)
            if isinstance(project_runs, list):
                for item in project_runs[:MAX_PROJECT_RUNS]:
                    if not isinstance(item, Mapping):
                        continue
                    try:
                        run = ProjectRunRecord.from_dict(item)
                    except ValueError:
                        continue
                    if run.project_id in self._projects:
                        self._project_runs[run.run_id] = run
    def _save(self) -> None:
        # Persist project profiles beside existing private registry metadata atomically.
        """Atomically persist the registry outside every measured checkout."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": LOCAL_UI_SCHEMA_VERSION,
            "projects": [
                {
                    "project_id": project.project_id,
                    "display_name": project.display_name,
                    "root_path": str(project.root_path),
                    "reports_dir": str(project.reports_dir),
                    "profile": project.profile.to_dict() if project.profile is not None else None,
                }
                for project in sorted(self._projects.values(), key=lambda item: item.project_id)
            ],
            "campaigns": [
                {
                    "campaign_id": campaign.campaign_id,
                    "project_id": campaign.project_id,
                    "database_path": str(campaign.database_path),
                }
                for campaign in sorted(self._campaigns.values(), key=lambda item: item.campaign_id)
            ],
            "project_runs": [
                run.to_dict()
                for run in sorted(self._project_runs.values(), key=lambda item: item.run_id)
            ],
        }
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}-{uuid4().hex}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)
    def register_project(
        self,
        root_path: str | Path,
        display_name: str | None = None,
        *,
        profile: ProjectProfile | None = None,
    ) -> RegisteredProject:
        # Register one checkout and persist a fresh deterministic project profile.
        """Register one existing local checkout and derive its isolated state root."""
        root = Path(root_path).expanduser().resolve()
        if not root.is_dir():
            raise ValueError("project root is not a directory")
        project_id = _default_project_id(root)
        name = (display_name or root.name or "Theseus project").strip()
        if not name:
            raise ValueError("display_name must not be empty")
        reports = self.state_dir / "projects" / project_id / "reports"
        resolved_profile = profile or ProjectProfile.from_discovery(discover_project(root))
        project = RegisteredProject(project_id, name[:256], root, reports.resolve(), resolved_profile)
        with self._lock:
            if project_id not in self._projects and len(self._projects) >= MAX_REGISTERED_PROJECTS:
                raise ValueError("project registry limit reached")
            self._projects[project_id] = project
            self._save()
        return project
    def projects(self) -> tuple[RegisteredProject, ...]:
        """Return registered projects in deterministic identity order."""
        with self._lock:
            return tuple(sorted(self._projects.values(), key=lambda item: item.project_id))
    def project(self, project_id: str) -> RegisteredProject | None:
        """Resolve one project identity without exposing its path."""
        with self._lock:
            return self._projects.get(project_id)
    def project_profile(self, project_id: str) -> ProjectProfile | None:
        # Resolve a persisted profile and backfill legacy registry rows on first use.
        with self._lock:
            project = self._projects.get(project_id)
            if project is None:
                return None
            if project.profile is not None:
                return project.profile
        profile = ProjectProfile.from_discovery(discover_project(project.root_path))
        with self._lock:
            current = self._projects.get(project_id)
            if current is None:
                return None
            updated = RegisteredProject(current.project_id, current.display_name, current.root_path, current.reports_dir, profile)
            self._projects[project_id] = updated
            self._save()
            return profile
    def register_campaign(self, project_id: str, campaign_id: str, database_path: Path, *, persist: bool = True) -> RegisteredCampaign:
        # Retain one campaign binding in memory and optionally checkpoint the private registry.
        """Persist one campaign/database binding after durable campaign creation."""
        campaign = RegisteredCampaign(campaign_id, project_id, database_path.expanduser().resolve())
        with self._lock:
            self._campaigns[campaign_id] = campaign
            if persist:
                self._save()
        return campaign
    def campaigns_snapshot(self, project_id: str | None = None) -> tuple[RegisteredCampaign, ...]:
        # Return only already-known campaign bindings without touching project or report directories.
        with self._lock:
            values = tuple(self._campaigns.values())
        if project_id is not None:
            values = tuple(item for item in values if item.project_id == project_id)
        return tuple(sorted(values, key=lambda item: item.campaign_id))
    def campaigns(self) -> tuple[RegisteredCampaign, ...]:
        # Discover missing campaign databases only when an explicit reconciliation path asks for it.
        """Discover new campaign databases and return all known bindings."""
        with self._lock:
            changed = False
            for project in self._projects.values():
                report_roots = (
                    project.reports_dir,
                    project.root_path.parent / ".theseus-state" / project.project_id / "reports",
                )
                seen_roots: set[Path] = set()
                for reports_root in report_roots:
                    reports_root = reports_root.resolve()
                    if reports_root in seen_roots or not reports_root.is_dir():
                        continue
                    seen_roots.add(reports_root)
                    try:
                        candidates = tuple(reports_root.glob("*/campaign.sqlite3"))
                    except OSError:
                        continue
                    for database in candidates[:MAX_CAMPAIGNS_PER_PROJECT]:
                        campaign_id = database.parent.name
                        if campaign_id and campaign_id not in self._campaigns:
                            self._campaigns[campaign_id] = RegisteredCampaign(campaign_id, project.project_id, database.resolve())
                            changed = True
            if changed:
                self._save()
            return tuple(sorted(self._campaigns.values(), key=lambda item: item.campaign_id))
    def campaign(self, campaign_id: str) -> RegisteredCampaign | None:
        # Resolve the normal hot path from memory and scan report directories only for an unknown legacy campaign.
        with self._lock:
            current = self._campaigns.get(campaign_id)
        if current is not None:
            return current
        self.campaigns()
        with self._lock:
            return self._campaigns.get(campaign_id)
    def register_project_run(self, run: ProjectRunRecord) -> ProjectRunRecord:
        # Persist one project-level run after its file campaigns have been launched.
        if run.project_id not in self._projects:
            raise ValueError("project does not exist")
        with self._lock:
            if run.run_id not in self._project_runs and len(self._project_runs) >= MAX_PROJECT_RUNS:
                raise ValueError("project run registry limit reached")
            self._project_runs[run.run_id] = run
            self._save()
        return run
    def update_project_run(self, run: ProjectRunRecord, *, persist: bool = True) -> ProjectRunRecord:
        # Replace one project-run projection and optionally checkpoint the large private registry.
        with self._lock:
            if run.run_id not in self._project_runs:
                raise ValueError("project run does not exist")
            self._project_runs[run.run_id] = run
            if persist:
                self._save()
        return run
    def project_runs(self, project_id: str | None = None) -> tuple[ProjectRunRecord, ...]:
        # Return persisted project runs in deterministic identity order with optional project filtering.
        with self._lock:
            values = tuple(self._project_runs.values())
        if project_id is not None:
            values = tuple(item for item in values if item.project_id == project_id)
        return tuple(sorted(values, key=lambda item: item.run_id))
    def project_run(self, run_id: str) -> ProjectRunRecord | None:
        # Resolve one project-run identity without exposing private checkout paths.
        with self._lock:
            return self._project_runs.get(run_id)
    def append_project_run_event(self, run_id: str, event: Mapping[str, object]) -> None:
        # Append one bounded private JSONL event without rewriting the project-run registry.
        with self._lock:
            if run_id not in self._project_runs:
                return
            root = self.state_dir / "project-runs" / run_id
            root.mkdir(parents=True, exist_ok=True)
            path = root / "events.jsonl"
            payload = {str(key): value for key, value in event.items()}
            with path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    def project_run_events(self, run_id: str, *, limit: int = PROJECT_RUN_EVENT_LIMIT) -> tuple[dict[str, JsonValue], ...]:
        # Read only the newest bounded project-run events while tolerating an interrupted final JSONL record.
        path = self.state_dir / "project-runs" / run_id / "events.jsonl"
        if not path.is_file():
            return ()
        rows: deque[dict[str, JsonValue]] = deque(maxlen=max(1, min(int(limit), PROJECT_RUN_EVENT_LIMIT)))
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        value = json.loads(line)
                    except (ValueError, json.JSONDecodeError):
                        continue
                    if isinstance(value, dict):
                        rows.append(value)
        except (OSError, UnicodeError):
            return ()
        return tuple(rows)
def _success(value: object) -> ClientResponse:
    """Normalize one stable local response mapping."""
    normalized = to_json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("local UI response must be an object")
    return normalized
def _rejected(code: str, message: str, details: Mapping[str, JsonValue] | None = None) -> ClientResponse:
    # Return one typed local UI rejection with bounded public scalar details.
    return {
        "ok": False,
        "kind": "rejected",
        "error": {"code": code, "message": message, "retriable": False, "details": dict(details or {})},
    }
class LocalBrowserCampaignAuthority:
    """Create and immediately start campaigns selected in the browser."""
    def __init__(
        self,
        registry: LocalUiRegistry,
        *,
        actions_factory: Callable[[CampaignConfiguration], LocalOperatorActions] | None = None,
    ) -> None:
        # Bind campaign creation to the registry and its project-specific performance memory.
        self.registry = registry
        self._actions_factory = actions_factory or LocalOperatorActions.for_configuration
        self.tuning_store = ProjectTuningStore(registry.state_dir / "project-performance.json")
    def _configuration(self, request: CampaignCreateRequest) -> tuple[CampaignConfiguration, RegisteredProject]:
        # Build one campaign from persisted project defaults plus explicit per-run overrides.
        """Build one public campaign contract from the bounded browser request."""
        project_id = _optional_text(request.get("project_id"), "project_id")
        project_root = _optional_text(request.get("project_root"), "project_root")
        project = self.registry.project(project_id) if project_id else None
        if project is None and project_root:
            project = self.registry.register_project(project_root, _optional_text(request.get("display_name"), "display_name"))
        if project is None:
            raise ValueError("select or register a project before creating a campaign")
        profile = self.registry.project_profile(project.project_id)
        if profile is None:
            raise ValueError("project profile is unavailable")
        root = project.root_path
        source_path = _safe_source(root, request.get("source_path"))
        operators = request.get("operators", [])
        if not isinstance(operators, list) or any(not isinstance(item, str) or not item.strip() for item in operators):
            raise ValueError("operators must be an array of non-empty strings")
        campaign_id = _optional_text(request.get("campaign_id"), "campaign_id") or _default_campaign_id()
        test_command = request.get("test_command")
        if test_command is None or test_command == "":
            test_command = list(profile.test_command)
        test_cwd = _safe_test_cwd(root, request.get("test_cwd"))
        reuse_mode = _optional_text(request.get("reuse_mode"), "reuse_mode") or profile.reuse_mode
        config = CampaignConfiguration(
            campaign_id=CampaignId(campaign_id),
            project=ProjectDescriptor(
                project_id=ProjectId(project.project_id),
                display_name=project.display_name,
                root_path=str(root),
                test_command=_safe_command(test_command, root, cwd=test_cwd),
            ),
            scope=MutationScope(
                source_path=source_path,
                function=_optional_text(request.get("function"), "function"),
                scope_kind=_required_text(request.get("scope_kind", "file"), "scope_kind"),
                operators=tuple(dict.fromkeys(item.strip() for item in operators)),
            ),
            budget=CampaignBudget(
                max_mutants=_positive_int(request.get("max_mutants"), "max_mutants", default=profile.max_mutants),
                max_workers=(
                    _positive_int(request.get("max_workers"), "max_workers")
                    or (
                        self.tuning_store.recommend(
                            project.project_id,
                            preferred_workers=profile.preferred_workers,
                            workload_size=_positive_int(request.get("max_mutants"), "max_mutants", default=profile.max_mutants),
                        ).workers
                        if request.get("auto_workers") is True
                        else profile.preferred_workers or 1
                    )
                ),
                max_seconds=_positive_float(request.get("max_seconds"), "max_seconds", default=profile.max_seconds),
                max_test_seconds=_positive_float(request.get("max_test_seconds"), "max_test_seconds", default=profile.max_test_seconds),
            ),
            no_escalation=profile.no_escalation if request.get("no_escalation") is None else bool(request.get("no_escalation")),
            reports_dir=str(project.reports_dir),
            reuse_mode=reuse_mode,
        )
        return config, project
    def __call__(self, request: CampaignCreateRequest) -> object:
        # Commit one campaign through the existing action authority and expose the resolved worker budget.
        """Commit a campaign and launch its detached coordinator exactly once."""
        try:
            configuration, project = self._configuration(request)
            actions = self._actions_factory(configuration)
            create_action_id = _optional_text(request.get("create_action_id"), "create_action_id") or f"ui-create-{uuid4().hex}"
            start_action_id = _optional_text(request.get("start_action_id"), "start_action_id") or f"ui-start-{uuid4().hex}"
            created = actions.create_campaign(create_action_id, configuration)
            created_response = _success(created)
            if created_response.get("ok") is not True:
                return created
            database = LocalCampaignCoordinator.campaign_database_path(configuration)
            self.registry.register_campaign(
                project.project_id,
                configuration.campaign_id.value,
                database,
                persist=request.get("defer_registry_save") is not True,
            )
            started = actions.start_campaign(
                start_action_id,
                configuration.campaign_id.value,
                expected_revision=0,
            )
            started_response = _success(started)
            if started_response.get("ok") is not True:
                return started
            return ApiSuccess(
                {
                    "campaign_id": configuration.campaign_id.value,
                    "project_id": project.project_id,
                    "campaign_revision": started_response.get("value", {}).get("campaign_revision", 0)
                    if isinstance(started_response.get("value"), Mapping)
                    else 0,
                    "status": started_response.get("value", {}).get("status", "running")
                    if isinstance(started_response.get("value"), Mapping)
                    else "running",
                    "create_action_id": create_action_id,
                    "start_action_id": start_action_id,
                    "max_workers": configuration.budget.max_workers,
                }
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            return ApiRejected(
                ApiError("invalid_campaign_launch", "campaign launch request was rejected")
            )
class LocalWorkspaceCampaignClient:
    """CampaignUiClient implementation spanning all projects in one UI registry."""
    def __init__(
        self,
        state_dir: str | Path,
        *,
        project_roots: tuple[str | Path, ...] = (),
        actions_factory: Callable[[CampaignConfiguration], LocalOperatorActions] | None = None,
    ) -> None:
        # Build one browser workspace with shared registry, campaign authority, and tuning memory.
        self.registry = LocalUiRegistry(state_dir)
        for root in project_roots:
            self.registry.register_project(root)
        self.authority = LocalBrowserCampaignAuthority(self.registry, actions_factory=actions_factory)
        self.tuning_store = self.authority.tuning_store
        self._project_run_threads: dict[str, Thread] = {}
        self._project_run_dispatch_lock = RLock()
        self._project_run_execution_lock = Lock()
        self._project_storage_bytes: dict[str, int] = {}
        self._resume_project_run_dispatchers()
    def register_project(self, root_path: str, display_name: str | None = None) -> ClientResponse:
        # Register one browser-selected project and return its persisted A2 profile.
        try:
            discovery = discover_project(root_path)
            project = self.registry.register_project(root_path, display_name, profile=ProjectProfile.from_discovery(discovery))
            profile = project.profile
            if profile is None:
                raise ValueError("project profile is unavailable")
            self._project_storage_bytes.pop(project.project_id, None)
            return {
                "ok": True,
                "kind": "success",
                "value": {
                    "project_id": project.project_id,
                    "display_name": project.display_name,
                    "root_name": project.root_path.name,
                    "discovery": discovery.public_summary(),
                    "profile": profile.public_summary(),
                },
            }
        except (OSError, ValueError):
            return _rejected("project_registration_failed", "project root could not be registered")
    def _campaign_row(self, item: RegisteredCampaign) -> dict[str, JsonValue]:
        # Read one compact campaign projection and degrade to an unavailable row instead of blocking sibling resources.
        try:
            result = PublicApiCampaignClient(item.database_path).list_campaigns(limit=1, project_id=item.project_id)
        except (OSError, RuntimeError, TypeError, ValueError):
            result = {}
        if result.get("ok") is True and isinstance(result.get("value"), Mapping):
            values = result["value"].get("items", [])
            row = next((value for value in values if isinstance(value, dict)), None) if isinstance(values, list) else None
            if row is not None:
                return row
        return {
            "campaign_id": item.campaign_id,
            "project_id": item.project_id,
            "status": "unavailable",
            "revision_number": 0,
            "completed_mutants": 0,
            "total_mutants": 0,
            "mutation_score": None,
            "last_activity": None,
        }
    def _campaign_rows(self, project_id: str | None = None, *, campaign_ids: set[str] | None = None) -> list[dict[str, JsonValue]]:
        # Read only explicitly selected known campaign databases and never rescan report directories from a browser read.
        bindings = self.registry.campaigns_snapshot(project_id)
        if campaign_ids is not None:
            bindings = tuple(item for item in bindings if item.campaign_id in campaign_ids)
        return [self._campaign_row(item) for item in bindings]
    def list_projects(self, *, limit: int, cursor: str | None = None) -> ClientResponse:
        # List projects from in-memory registry metadata only so overview never scans checkout or campaign directories.
        """List registered projects and campaign counts."""
        counts: dict[str, int] = {}
        run_counts: dict[str, int] = {}
        for item in self.registry.campaigns_snapshot():
            counts[item.project_id] = counts.get(item.project_id, 0) + 1
        for run in self.registry.project_runs():
            run_counts[run.project_id] = run_counts.get(run.project_id, 0) + 1
        rows: list[dict[str, JsonValue]] = []
        for project in self.registry.projects():
            profile = project.profile or self.registry.project_profile(project.project_id)
            rows.append(
                {
                    "id": project.project_id,
                    "project_id": project.project_id,
                    "display_name": project.display_name,
                    "root_name": project.root_path.name,
                    "campaign_count": counts.get(project.project_id, 0),
                    "project_run_count": run_counts.get(project.project_id, 0),
                    "profile": profile.public_summary() if profile is not None else None,
                }
            )
        return {"ok": True, "kind": "success", "value": _page(rows, limit, cursor, "projects")}
    def list_campaigns(self, *, limit: int, cursor: str | None = None, project_id: str | None = None) -> ClientResponse:
        # Apply keyset pagination to cheap registry bindings before opening any campaign SQLite database.
        """List campaigns across all registered project databases."""
        position = _cursor_decode(cursor, "campaigns")
        bindings = self.registry.campaigns_snapshot(project_id)
        if position is not None:
            bindings = tuple(item for item in bindings if item.campaign_id > position)
        visible = bindings[:limit]
        next_cursor = _cursor_encode("campaigns", visible[-1].campaign_id) if len(bindings) > limit and visible else None
        rows = [self._campaign_row(item) for item in visible]
        return {
            "ok": True,
            "kind": "success",
            "value": {"items": rows, "limit": limit, "next_cursor": next_cursor},
        }
    @staticmethod
    def _project_run_status(statuses: tuple[str, ...]) -> str:
        # Distinguish a genuinely active run from one that is only waiting in the global dispatcher queue.
        normalized = tuple(item.strip().lower() for item in statuses if item.strip())
        if not normalized:
            return "pending"
        active = {"created", "preparing", "indexing", "materializing", "starting", "running", "cancelling", "canceling"}
        if any(item in active for item in normalized):
            return "running"
        if any(item in {"queued", "pending"} for item in normalized):
            return "queued"
        successful = {"complete", "completed", "recovered"}
        if all(item in successful for item in normalized):
            return "complete"
        cancelled = {"cancelled", "canceled"}
        if all(item in cancelled for item in normalized):
            return "cancelled"
        if any(item in successful for item in normalized):
            return "partial"
        return "failed"
    @staticmethod
    def _project_run_unfinished(run: ProjectRunRecord) -> bool:
        # Treat queued or active child entries as unfinished while rejected and terminal entries no longer hold the dispatcher.
        return any(
            entry.launch_status not in _PROJECT_RUN_TERMINAL_STATUSES and entry.launch_status != "rejected"
            for entry in run.entries
        )
    def _project_run_queue_position(self, run: ProjectRunRecord) -> int | None:
        # Return zero for the active run and a one-based waiting position for queued runs without touching campaign databases.
        statuses = tuple(entry.launch_status for entry in run.entries)
        status = self._project_run_status(statuses)
        thread = self._project_run_threads.get(run.run_id)
        if status == "running" or (thread is not None and thread.is_alive()):
            return 0
        if status != "queued":
            return None
        waiting = 0
        for candidate in self.registry.project_runs():
            if not self._project_run_unfinished(candidate):
                continue
            candidate_status = self._project_run_status(tuple(entry.launch_status for entry in candidate.entries))
            candidate_thread = self._project_run_threads.get(candidate.run_id)
            if candidate_status == "running" or (candidate_thread is not None and candidate_thread.is_alive()):
                continue
            waiting += 1
            if candidate.run_id == run.run_id:
                return waiting
        return waiting or 1
    @staticmethod
    def _project_run_failure_groups(run: ProjectRunRecord) -> list[dict[str, JsonValue]]:
        # Group persisted child failures by stage and code so the operator sees the dominant cause immediately.
        counts: dict[tuple[str, str], int] = {}
        for entry in run.entries:
            if entry.error_code is None:
                continue
            key = (entry.error_stage or "unknown", entry.error_code)
            counts[key] = counts.get(key, 0) + 1
        return [
            {"stage": stage, "code": code, "count": count}
            for (stage, code), count in sorted(counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
        ]
    def _project_run_component(self, run: ProjectRunRecord, entry: ProjectRunEntry) -> str:
        # Resolve the persisted file to the pytest component whose baseline context owns it.
        profile = self.registry.project_profile(run.project_id)
        return project_test_cwd(profile, entry.source_path) if profile is not None else "."
    def _project_run_worker_budget(self, run: ProjectRunRecord) -> int:
        # Resolve the same conservative worker count used to estimate peak workspace storage.
        if run.worker_budget is not None:
            return max(1, int(run.worker_budget))
        profile = self.registry.project_profile(run.project_id)
        if profile is None:
            return 1
        if profile.preferred_workers is not None:
            return max(1, int(profile.preferred_workers))
        try:
            return max(1, int(self.tuning_store.recommend(run.project_id, workload_size=profile.max_mutants).workers))
        except (OSError, RuntimeError, TypeError, ValueError):
            return 1
    def _project_run_storage_budget(self, run: ProjectRunRecord) -> dict[str, int | bool]:
        # Estimate one sequential child peak and preserve a hard free-space reserve on the private state volume.
        project = self.registry.project(run.project_id)
        if project is None:
            raise ValueError("project does not exist")
        logical_bytes = self._project_storage_bytes.get(run.project_id)
        if logical_bytes is None:
            logical_bytes = project_tree_logical_bytes(project.root_path)
            self._project_storage_bytes[run.project_id] = logical_bytes
        workers = self._project_run_worker_budget(run)
        parallel_campaigns = len(run.entries) if len(run.entries) <= PROJECT_RUN_INLINE_SOURCE_LIMIT else 1
        estimated_peak = int(max(1, logical_bytes) * (1 + workers) * max(1, parallel_campaigns) * PROJECT_RUN_DISK_SAFETY_FACTOR) + PROJECT_RUN_DISK_OVERHEAD_BYTES
        storage_root = project.reports_dir.parent
        while not storage_root.exists() and storage_root.parent != storage_root:
            storage_root = storage_root.parent
        free_bytes = int(shutil.disk_usage(storage_root).free)
        required_free = estimated_peak + PROJECT_RUN_DISK_RESERVE_BYTES
        return {
            "project_bytes": int(logical_bytes),
            "workers": workers,
            "parallel_campaigns": max(1, parallel_campaigns),
            "estimated_peak_bytes": estimated_peak,
            "free_bytes": free_bytes,
            "required_free_bytes": required_free,
            "ok": free_bytes >= required_free,
        }
    def _block_project_run_entries(
        self,
        run: ProjectRunRecord,
        *,
        error_code: str,
        error_stage: str,
        component: str | None = None,
    ) -> ProjectRunRecord:
        # Reject only still-queued entries covered by one project-level safety failure.
        entries = list(run.entries)
        changed = False
        failed_at = datetime.now(timezone.utc).isoformat()
        for index, entry in enumerate(entries):
            if entry.launch_status != "queued":
                continue
            if component is not None and self._project_run_component(run, entry) != component:
                continue
            entries[index] = ProjectRunEntry(entry.source_path, entry.campaign_id, "rejected", error_code, 0, 0, error_stage, failed_at)
            changed = True
        if not changed:
            return run
        updated = ProjectRunRecord(run.run_id, run.project_id, tuple(entries), run.created_at, run.completed_at, run.worker_budget)
        return self.registry.update_project_run(updated, persist=True)
    def _cleanup_project_run_entry(self, run: ProjectRunRecord, entry: ProjectRunEntry) -> bool:
        # Reclaim disposable project copies after one child attempt so sequential ProjectRun storage stays bounded.
        project = self.registry.project(run.project_id)
        if project is None:
            return False
        try:
            cleanup_campaign_workspaces(project.reports_dir, entry.campaign_id)
        except (OSError, RuntimeError, ValueError):
            self._log_project_run_event(run.run_id, "workspace_cleanup_failed", entry=entry, status="failed", error_code="project_workspace_cleanup_failed", error_stage="storage")
            return False
        self._log_project_run_event(run.run_id, "workspace_cleaned", entry=entry, status=entry.launch_status)
        return True
    def _apply_project_run_failure_gate(self, run: ProjectRunRecord, entry: ProjectRunEntry) -> ProjectRunRecord:
        # Stop repeating a failed baseline across files that share the same pytest execution context.
        if entry.error_stage != "baseline" and entry.error_code != "campaign_baseline_failed":
            return run
        component = self._project_run_component(run, entry)
        blocked = self._block_project_run_entries(
            run,
            error_code="project_baseline_blocked",
            error_stage="baseline",
            component=component,
        )
        if blocked != run:
            self._log_project_run_event(blocked.run_id, "baseline_gate_blocked", entry=entry, status="failed", error_code=entry.error_code, error_stage="baseline")
        return blocked
    def _log_project_run_event(
        self,
        run_id: str,
        event_type: str,
        *,
        entry: ProjectRunEntry | None = None,
        status: str | None = None,
        error_code: str | None = None,
        error_stage: str | None = None,
    ) -> None:
        # Append one operator-readable transition without copying private exception text into the browser state.
        event: dict[str, object] = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "status": status,
            "error_code": error_code,
            "error_stage": error_stage,
        }
        if entry is not None:
            event["source_path"] = entry.source_path
            event["campaign_id"] = entry.campaign_id
        try:
            self.registry.append_project_run_event(run_id, event)
        except OSError:
            return
    def _project_run_wall_seconds(self, run: ProjectRunRecord) -> float | None:
        # Read reconciled coordinator wall-clock evidence and refuse to learn from missing or inconsistent timing artifacts.
        totals: list[float] = []
        for entry in run.entries:
            if entry.error_code is not None:
                continue
            campaign = self.registry.campaign(entry.campaign_id)
            if campaign is None:
                return None
            path = campaign.database_path.parent / "coordinator.performance.json"
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                total = float(payload.get("total_wall_seconds", 0.0))
                error = float(payload.get("accounting_error_seconds", 0.0))
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                return None
            if payload.get("status") != "completed" or total <= 0.0 or error > 0.001:
                return None
            totals.append(total)
        if not totals:
            return None
        return sum(totals) if len(run.entries) > PROJECT_RUN_INLINE_SOURCE_LIMIT else max(totals)
    def _project_run_summary(
        self,
        run: ProjectRunRecord,
        *,
        include_campaigns: bool = False,
        campaign_rows: Mapping[str, Mapping[str, JsonValue]] | None = None,
    ) -> dict[str, JsonValue]:
        # Aggregate cheap persisted child state and expose queue, failure-group, and current-file diagnostics explicitly.
        rows = campaign_rows or {}
        snapshots: list[tuple[ProjectRunEntry, str, int, int]] = []
        statuses: list[str] = []
        total_mutants = 0
        completed_mutants = 0
        for entry in run.entries:
            row = rows.get(entry.campaign_id, {})
            status = str(row.get("status") or entry.launch_status).strip().lower()
            completed = int(row.get("completed_mutants") if row.get("completed_mutants") is not None else entry.completed_mutants)
            total = int(row.get("total_mutants") if row.get("total_mutants") is not None else entry.total_mutants)
            snapshots.append((entry, status, completed, total))
            statuses.append(status)
            total_mutants += total
            completed_mutants += completed
        active_states = {"created", "preparing", "indexing", "materializing", "starting", "running", "cancelling", "canceling"}
        successful_states = {"complete", "completed", "recovered"}
        cancelled_states = {"cancelled", "canceled"}
        queued_count = sum(1 for _entry, status, _completed, _total in snapshots if status in {"queued", "pending"})
        active_count = sum(1 for _entry, status, _completed, _total in snapshots if status in active_states)
        completed_count = sum(1 for _entry, status, _completed, _total in snapshots if status in successful_states)
        cancelled_count = sum(1 for _entry, status, _completed, _total in snapshots if status in cancelled_states)
        failed_count = sum(
            1
            for entry, status, _completed, _total in snapshots
            if entry.error_code is not None or status in {"failed", "rejected", "error"}
        )
        processed_count = len(run.entries) - queued_count
        active = next((item for item in snapshots if item[1] in active_states), None)
        next_queued = next((item for item in snapshots if item[1] in {"queued", "pending"}), None)
        run_status = self._project_run_status(tuple(statuses))
        thread = self._project_run_threads.get(run.run_id)
        if run_status == "queued" and thread is not None and thread.is_alive():
            run_status = "running"
        value: dict[str, JsonValue] = {
            "run_id": run.run_id,
            "project_id": run.project_id,
            "status": run_status,
            "source_count": len(run.entries),
            "campaign_count": processed_count,
            "processed_count": processed_count,
            "queued_count": queued_count,
            "active_count": active_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "cancelled_count": cancelled_count,
            "launch_failures": failed_count,
            "completed_mutants": completed_mutants,
            "total_mutants": total_mutants,
            "first_campaign_id": next((entry.campaign_id for entry, status, _completed, _total in snapshots if status not in {"queued", "pending"}), None),
            "current_campaign_id": active[0].campaign_id if active is not None else None,
            "current_source_path": active[0].source_path if active is not None else None,
            "next_source_path": next_queued[0].source_path if next_queued is not None else None,
            "queue_position": self._project_run_queue_position(run),
            "worker_budget": run.worker_budget,
            "created_at": run.created_at,
            "completed_at": run.completed_at,
        }
        if include_campaigns:
            priority = sorted(
                snapshots,
                key=lambda item: (
                    0 if item[0].error_code is not None or item[1] in {"failed", "rejected", "error"} else 1 if item[1] in active_states else 2,
                    item[0].source_path,
                ),
            )
            entries: list[dict[str, JsonValue]] = []
            for entry, status, completed, total in priority[:PROJECT_RUN_DETAIL_ENTRY_LIMIT]:
                entries.append(
                    {
                        "source_path": entry.source_path,
                        "campaign_id": entry.campaign_id,
                        "status": status,
                        "completed_mutants": completed,
                        "total_mutants": total,
                        "error_code": entry.error_code,
                        "error_stage": entry.error_stage,
                        "failed_at": entry.failed_at,
                    }
                )
            value["campaigns"] = entries
            value["campaigns_truncated"] = len(run.entries) > len(entries)
            value["failure_groups"] = self._project_run_failure_groups(run)
            value["events"] = list(self.registry.project_run_events(run.run_id))
            try:
                value["storage"] = dict(self._project_run_storage_budget(run))
            except (OSError, RuntimeError, TypeError, ValueError):
                value["storage"] = None
        return value
    def _refresh_project_run_from_rows(
        self,
        run: ProjectRunRecord,
        rows: Mapping[str, Mapping[str, JsonValue]],
    ) -> ProjectRunRecord:
        # Persist terminal child progress observed by a detail read so recovery and performance learning remain idempotent.
        entries = list(run.entries)
        changed = False
        for index, entry in enumerate(entries):
            row = rows.get(entry.campaign_id)
            if row is None:
                continue
            status = str(row.get("status") or entry.launch_status).strip().lower()
            completed = int(row.get("completed_mutants") if row.get("completed_mutants") is not None else entry.completed_mutants)
            total = int(row.get("total_mutants") if row.get("total_mutants") is not None else entry.total_mutants)
            if status == entry.launch_status and completed == entry.completed_mutants and total == entry.total_mutants:
                continue
            entries[index] = ProjectRunEntry(entry.source_path, entry.campaign_id, status, entry.error_code, completed, total, entry.error_stage, entry.failed_at)
            changed = True
        if not changed:
            return run
        updated = ProjectRunRecord(run.run_id, run.project_id, tuple(entries), run.created_at, run.completed_at, run.worker_budget)
        return self.registry.update_project_run(updated, persist=True)
    def _finalize_project_run(self, run: ProjectRunRecord) -> ProjectRunRecord:
        # Mark every terminal project run finished while learning performance only from fully successful evidence.
        if run.completed_at is not None:
            return run
        summary = self._project_run_summary(run)
        status = str(summary["status"])
        if status in {"running", "queued", "pending"}:
            return run
        completed_mutants = int(summary["completed_mutants"] or 0)
        completed_at = datetime.now(timezone.utc).isoformat()
        if status == "complete" and run.worker_budget and completed_mutants > 0:
            wall_seconds = self._project_run_wall_seconds(run)
            if wall_seconds is not None:
                try:
                    self.tuning_store.record(ProjectPerformanceSample(run.run_id, run.project_id, run.worker_budget, completed_mutants, wall_seconds))
                except (OSError, TypeError, ValueError):
                    pass
        updated = ProjectRunRecord(run.run_id, run.project_id, run.entries, run.created_at, completed_at, run.worker_budget)
        try:
            stored = self.registry.update_project_run(updated, persist=True)
        except (OSError, ValueError):
            return run
        self._log_project_run_event(stored.run_id, "run_finished", status=status)
        return stored
    def list_project_runs(self, *, limit: int, cursor: str | None = None, project_id: str | None = None) -> ClientResponse:
        # Page cheap project-run metadata first, then refresh only a bounded number of currently active child campaigns.
        position = _cursor_decode(cursor, "project-runs")
        runs = self.registry.project_runs(project_id)
        if position is not None:
            runs = tuple(run for run in runs if run.run_id > position)
        visible = runs[:limit]
        next_cursor = _cursor_encode("project-runs", visible[-1].run_id) if len(runs) > limit and visible else None
        active_ids: set[str] = set()
        for run in visible:
            active = 0
            for entry in run.entries:
                if entry.launch_status in _PROJECT_RUN_TERMINAL_STATUSES or entry.launch_status in {"queued", "rejected"}:
                    continue
                active_ids.add(entry.campaign_id)
                active += 1
                if active >= PROJECT_RUN_STATUS_REFRESH_LIMIT:
                    break
        authoritative = {str(item.get("campaign_id", "")): item for item in self._campaign_rows(project_id, campaign_ids=active_ids)}
        rows = [self._project_run_summary(run, campaign_rows=authoritative) for run in visible]
        return {
            "ok": True,
            "kind": "success",
            "value": {"items": rows, "limit": limit, "next_cursor": next_cursor},
        }
    def get_project_run(self, run_id: str) -> ClientResponse:
        # Return bounded child membership, retaining the legacy small-run reconciliation path without large-run SQLite scans.
        run = self.registry.project_run(run_id)
        if run is None:
            return _rejected("project_run_not_found", "project run does not exist")
        active_ids = {
            entry.campaign_id
            for entry in run.entries
            if entry.launch_status not in _PROJECT_RUN_TERMINAL_STATUSES and entry.launch_status not in {"queued", "rejected"}
        }
        active_ids = set(sorted(active_ids)[:PROJECT_RUN_STATUS_REFRESH_LIMIT])
        bindings = self.registry.campaigns_snapshot(run.project_id)
        rows = (
            self._campaign_rows(run.project_id)
            if len(bindings) <= PROJECT_RUN_STATUS_REFRESH_LIMIT
            else self._campaign_rows(run.project_id, campaign_ids=active_ids)
        )
        authoritative = {str(item.get("campaign_id", "")): item for item in rows}
        run = self._refresh_project_run_from_rows(run, authoritative)
        run = self._finalize_project_run(run)
        return {"ok": True, "kind": "success", "value": self._project_run_summary(run, include_campaigns=True, campaign_rows=authoritative)}
    def _project_run_campaign_request(self, run: ProjectRunRecord, entry: ProjectRunEntry, index: int) -> CampaignCreateRequest:
        # Rebuild one deterministic child request with the pytest cwd that owns its source component.
        profile = self.registry.project_profile(run.project_id)
        return {
            "project_id": run.project_id,
            "campaign_id": entry.campaign_id,
            "source_path": entry.source_path,
            "scope_kind": "file",
            "function": None,
            "create_action_id": f"ui-create-{run.run_id}-{index:04d}",
            "start_action_id": f"ui-start-{run.run_id}-{index:04d}",
            "max_workers": run.worker_budget,
            "auto_workers": run.worker_budget is None,
            "test_cwd": project_test_cwd(profile, entry.source_path) if profile is not None else ".",
            "defer_registry_save": True,
        }
    def _update_project_run_entry(
        self,
        run: ProjectRunRecord,
        index: int,
        entry: ProjectRunEntry,
        *,
        worker_budget: int | None = None,
        force_persist: bool = False,
    ) -> ProjectRunRecord:
        # Update one child transition and checkpoint large runs periodically instead of rewriting JSON per file.
        entries = list(run.entries)
        entries[index] = entry
        updated = ProjectRunRecord(
            run.run_id,
            run.project_id,
            tuple(entries),
            run.created_at,
            run.completed_at,
            worker_budget or run.worker_budget,
        )
        persist = force_persist or (index + 1) % PROJECT_RUN_REGISTRY_CHECKPOINT_SIZE == 0 or index + 1 == len(entries)
        return self.registry.update_project_run(updated, persist=persist)
    def _launch_project_run_entry(self, run: ProjectRunRecord, index: int) -> tuple[ProjectRunRecord, bool]:
        # Launch one queued file campaign and persist an operator-visible transition for every outcome.
        entry = run.entries[index]
        self._log_project_run_event(run.run_id, "child_starting", entry=entry, status="starting")
        try:
            result = _success(self.authority(self._project_run_campaign_request(run, entry, index + 1)))
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            failed_at = datetime.now(timezone.utc).isoformat()
            failed = ProjectRunEntry(entry.source_path, entry.campaign_id, "rejected", "launch_exception", 0, 0, "launch", failed_at)
            updated = self._update_project_run_entry(run, index, failed, force_persist=True)
            self._log_project_run_event(updated.run_id, "child_failed", entry=failed, status="rejected", error_code="launch_exception", error_stage="launch")
            return updated, False
        value = result.get("value") if isinstance(result.get("value"), Mapping) else {}
        if result.get("ok") is True:
            current_workers = value.get("max_workers")
            worker_budget = current_workers if isinstance(current_workers, int) and not isinstance(current_workers, bool) and current_workers > 0 else None
            launched = ProjectRunEntry(entry.source_path, entry.campaign_id, str(value.get("status", "running")))
            updated = self._update_project_run_entry(run, index, launched, worker_budget=worker_budget)
            self._log_project_run_event(updated.run_id, "child_started", entry=launched, status=launched.launch_status)
            return updated, True
        error = result.get("error") if isinstance(result.get("error"), Mapping) else {}
        code = str(error.get("code", "launch_failed"))
        details = error.get("details") if isinstance(error.get("details"), Mapping) else {}
        stage = str(details.get("stage") or "launch")
        failed_at = datetime.now(timezone.utc).isoformat()
        failed = ProjectRunEntry(entry.source_path, entry.campaign_id, "rejected", code, 0, 0, stage, failed_at)
        updated = self._update_project_run_entry(run, index, failed, force_persist=True)
        self._log_project_run_event(updated.run_id, "child_failed", entry=failed, status="rejected", error_code=code, error_stage=stage)
        return updated, False
    def _launcher_diagnostic(self, campaign_id: str) -> dict[str, JsonValue] | None:
        # Read the private detached-launch diagnostic while exposing only bounded non-path fields.
        campaign = self.registry.campaign(campaign_id)
        if campaign is None:
            return None
        path = campaign.database_path.parent / "campaign-launch.diagnostic.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(payload, Mapping):
            return None
        return {
            "stage": str(payload.get("stage", "launch")),
            "exception_type": str(payload.get("exception_type")) if payload.get("exception_type") else None,
            "recorded_at": str(payload.get("recorded_at")) if payload.get("recorded_at") else None,
            "stderr_log": "campaign-launch.stderr.log",
        }
    def _campaign_attempt_failure(self, campaign_id: str) -> dict[str, JsonValue] | None:
        # Resolve one failed coordinator attempt into a stable stage code plus bounded launcher metadata.
        campaign = self.registry.campaign(campaign_id)
        if campaign is None:
            return None
        launch = self._launcher_diagnostic(campaign_id)
        path = campaign.database_path.parent / "coordinator.performance.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            if launch is None:
                return None
            stage = str(launch.get("stage") or "launch")
            return {"code": "campaign_launch_failed", "stage": stage, "launcher": launch}
        if not payload.get("error"):
            if launch is None:
                return None
            stage = str(launch.get("stage") or "launch")
            return {"code": "campaign_launch_failed", "stage": stage, "launcher": launch}
        phases = payload.get("phases", [])
        phase = next(
            (
                str(item.get("phase", ""))
                for item in reversed(phases)
                if isinstance(item, Mapping) and str(item.get("phase", "")) not in {"cleanup", "unattributed_residual"}
            ),
            "coordinator",
        )
        normalized = "".join(character if character.isalnum() else "_" for character in phase.lower()).strip("_") or "coordinator"
        return {"code": f"campaign_{normalized}_failed", "stage": phase, "launcher": launch}
    def _campaign_attempt_failure_code(self, campaign_id: str) -> str | None:
        # Preserve the compact campaign-detail contract while deriving it from richer project-run diagnostics.
        failure = self._campaign_attempt_failure(campaign_id)
        return str(failure.get("code")) if failure is not None else None
    def _project_run_entry(self, campaign_id: str) -> tuple[ProjectRunRecord, ProjectRunEntry] | None:
        # Resolve one child campaign back to its persisted project-run source identity for detail diagnostics.
        for run in self.registry.project_runs():
            for entry in run.entries:
                if entry.campaign_id == campaign_id:
                    return run, entry
        return None
    def _baseline_diagnostic(self, campaign_id: str) -> dict[str, JsonValue] | None:
        # Expose bounded baseline process facts without returning raw subprocess output or private artifact paths.
        campaign = self.registry.campaign(campaign_id)
        if campaign is None:
            return None
        path = campaign.database_path.parent.parent / "engine" / campaign_id / "baseline.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            return None
        row = next((item for item in rows if isinstance(item, Mapping) and not bool(item.get("passed", False))), None)
        if row is None:
            return None
        excerpts = row.get("diagnostic_excerpts", [])
        clean_excerpts = [str(item)[:500] for item in excerpts[:3]] if isinstance(excerpts, list) else []
        return {
            "status": str(payload.get("status", "baseline_failed")),
            "level": str(row.get("level", "")) or None,
            "exit_code": row.get("exit_code") if isinstance(row.get("exit_code"), int) else None,
            "timed_out": bool(row.get("timed_out", False)),
            "elapsed_seconds": float(row.get("elapsed_seconds", 0.0)) if isinstance(row.get("elapsed_seconds"), (int, float)) else None,
            "diagnostic_excerpts": clean_excerpts,
        }
    def _campaign_state(self, campaign_id: str) -> Mapping[str, JsonValue] | None:
        # Read one known child campaign once so dispatcher polling never rescans all report directories or prior children.
        campaign = self.registry.campaign(campaign_id)
        if campaign is None:
            return None
        row = self._campaign_row(campaign)
        return row if row.get("status") != "unavailable" else None
    def _campaign_terminal(self, campaign_id: str) -> bool:
        # Preserve the small compatibility predicate while delegating to the constant-cost single-campaign read.
        row = self._campaign_state(campaign_id)
        return row is not None and str(row.get("status", "")).strip().lower() in _PROJECT_RUN_TERMINAL_STATUSES
    def _dispatch_project_run(self, run_id: str) -> None:
        # Execute one child at a time, persist each transition, and never hide a failed detached coordinator behind a stale active status.
        try:
            with self._project_run_execution_lock:
                while True:
                    run = self.registry.project_run(run_id)
                    if run is None:
                        return
                    active_index = next(
                        (
                            index
                            for index, entry in enumerate(run.entries)
                            if entry.launch_status not in _PROJECT_RUN_TERMINAL_STATUSES and entry.launch_status not in {"queued", "rejected"}
                        ),
                        None,
                    )
                    if active_index is not None:
                        active = run.entries[active_index]
                        row = self._campaign_state(active.campaign_id)
                        if row is not None:
                            status = str(row.get("status") or active.launch_status).strip().lower()
                            completed = int(row.get("completed_mutants") or 0)
                            total = int(row.get("total_mutants") or 0)
                            if status in _PROJECT_RUN_TERMINAL_STATUSES:
                                failure = self._campaign_attempt_failure(active.campaign_id) if status == "failed" else None
                                code = str(failure.get("code")) if failure is not None else active.error_code
                                stage = str(failure.get("stage")) if failure is not None else active.error_stage
                                failed_at = datetime.now(timezone.utc).isoformat() if status == "failed" and active.failed_at is None else active.failed_at
                                terminal = ProjectRunEntry(active.source_path, active.campaign_id, status, code, completed, total, stage, failed_at)
                                run = self._update_project_run_entry(run, active_index, terminal, force_persist=True)
                                self._log_project_run_event(
                                    run.run_id,
                                    "child_failed" if status == "failed" else "child_terminal",
                                    entry=terminal,
                                    status=status,
                                    error_code=code,
                                    error_stage=stage,
                                )
                                if status == "failed":
                                    run = self._apply_project_run_failure_gate(run, terminal)
                                if not self._cleanup_project_run_entry(run, terminal):
                                    run = self._block_project_run_entries(run, error_code="project_workspace_cleanup_failed", error_stage="storage")
                                    self._finalize_project_run(run)
                                    return
                                continue
                            if status != active.launch_status or completed != active.completed_mutants or total != active.total_mutants:
                                refreshed = ProjectRunEntry(
                                    active.source_path,
                                    active.campaign_id,
                                    status,
                                    active.error_code,
                                    completed,
                                    total,
                                    active.error_stage,
                                    active.failed_at,
                                )
                                run = self._update_project_run_entry(run, active_index, refreshed)
                                active = run.entries[active_index]
                        failure = self._campaign_attempt_failure(active.campaign_id)
                        legacy_failure_code = self._campaign_attempt_failure_code(active.campaign_id) if failure is None else None
                        if failure is not None or legacy_failure_code is not None:
                            code = str(failure.get("code") or "campaign_failed") if failure is not None else str(legacy_failure_code)
                            stage = str(failure.get("stage") or "coordinator") if failure is not None else code.removeprefix("campaign_").removesuffix("_failed") or "coordinator"
                            failed = ProjectRunEntry(
                                active.source_path,
                                active.campaign_id,
                                "failed",
                                code,
                                active.completed_mutants,
                                active.total_mutants,
                                stage,
                                datetime.now(timezone.utc).isoformat(),
                            )
                            run = self._update_project_run_entry(run, active_index, failed, force_persist=True)
                            self._log_project_run_event(run.run_id, "child_failed", entry=failed, status="failed", error_code=code, error_stage=stage)
                            run = self._apply_project_run_failure_gate(run, failed)
                            if not self._cleanup_project_run_entry(run, failed):
                                run = self._block_project_run_entries(run, error_code="project_workspace_cleanup_failed", error_stage="storage")
                                self._finalize_project_run(run)
                                return
                            continue
                        time.sleep(PROJECT_RUN_POLL_SECONDS)
                        continue
                    queued_index = next((index for index, entry in enumerate(run.entries) if entry.launch_status == "queued"), None)
                    if queued_index is None:
                        self._finalize_project_run(run)
                        return
                    try:
                        storage = self._project_run_storage_budget(run)
                    except (OSError, RuntimeError, TypeError, ValueError):
                        storage = {"ok": False}
                    if storage.get("ok") is not True:
                        run = self._block_project_run_entries(run, error_code="project_disk_budget_exceeded", error_stage="storage")
                        self._log_project_run_event(run.run_id, "disk_budget_blocked", status="failed", error_code="project_disk_budget_exceeded", error_stage="storage")
                        self._finalize_project_run(run)
                        return
                    self._launch_project_run_entry(run, queued_index)
        finally:
            with self._project_run_dispatch_lock:
                self._project_run_threads.pop(run_id, None)
            self._resume_project_run_dispatchers()
    def _start_project_run_dispatcher(self, run_id: str) -> None:
        # Keep one daemon dispatcher globally and record which run actually owns execution.
        with self._project_run_dispatch_lock:
            if any(thread.is_alive() for thread in self._project_run_threads.values()):
                return
            thread = Thread(target=self._dispatch_project_run, args=(run_id,), name=f"theseus-project-run-{run_id[-12:]}", daemon=True)
            self._project_run_threads[run_id] = thread
            self._log_project_run_event(run_id, "dispatcher_started", status="running")
            thread.start()
    def _reconcile_project_run_sources(self, run: ProjectRunRecord) -> ProjectRunRecord:
        # Reconcile legacy queued entries only when explicitly requested, never during synchronous UI startup.
        project = self.registry.project(run.project_id)
        profile = self.registry.project_profile(run.project_id)
        if project is None or profile is None:
            return run
        try:
            valid_sources = set(project_source_files(project.root_path, profile))
        except (OSError, ValueError):
            return run
        entries = list(run.entries)
        changed = False
        for index, entry in enumerate(entries):
            if entry.launch_status == "queued" and entry.source_path not in valid_sources:
                entries[index] = ProjectRunEntry(
                    entry.source_path,
                    entry.campaign_id,
                    "rejected",
                    "source_outside_current_profile",
                    entry.completed_mutants,
                    entry.total_mutants,
                    "source_inventory",
                    datetime.now(timezone.utc).isoformat(),
                )
                changed = True
        if not changed:
            return run
        updated = ProjectRunRecord(run.run_id, run.project_id, tuple(entries), run.created_at, run.completed_at, run.worker_budget)
        return self.registry.update_project_run(updated, persist=True)
    def _resume_project_run_dispatchers(self) -> None:
        # Resume only the oldest unfinished persistent run so later runs remain explicitly queued.
        for run in self.registry.project_runs():
            if self._project_run_unfinished(run):
                self._start_project_run_dispatcher(run.run_id)
                return
    def create_project_run(self, request: CampaignCreateRequest) -> ClientResponse:
        # Persist the whole project plan first, then launch small runs inline or large runs through the bounded dispatcher.
        try:
            project_id = _required_text(request.get("project_id"), "project_id")
            project = self.registry.project(project_id)
            profile = self.registry.project_profile(project_id)
            if project is None or profile is None:
                return _rejected("project_not_found", "project does not exist")
            existing = next((item for item in self.registry.project_runs(project_id) if self._project_run_unfinished(item)), None)
            if existing is not None:
                return _rejected(
                    "project_run_already_active",
                    "project already has an unfinished project run",
                    {"run_id": existing.run_id},
                )
            source_paths = project_source_files(project.root_path, profile)
        except ValueError as exc:
            return _rejected("project_run_discovery_failed", str(exc))
        except OSError:
            return _rejected("project_run_discovery_failed", "production source enumeration failed")
        if not source_paths:
            return _rejected("project_run_no_sources", "project profile contains no production Python source files")
        run_id = _default_project_run_id()
        created_at = datetime.now(timezone.utc).isoformat()
        try:
            requested_workers = _positive_int(request.get("max_workers"), "max_workers")
        except ValueError as exc:
            return _rejected("invalid_project_run", str(exc))
        entries = tuple(
            ProjectRunEntry(source_path, f"{run_id}-file-{index:04d}", "queued")
            for index, source_path in enumerate(source_paths, start=1)
        )
        planned = ProjectRunRecord(run_id, project_id, entries, created_at=created_at, worker_budget=requested_workers)
        try:
            storage = self._project_run_storage_budget(planned)
        except (OSError, RuntimeError, TypeError, ValueError):
            return _rejected("project_run_storage_check_failed", "project run storage budget could not be verified")
        if storage.get("ok") is not True:
            return _rejected(
                "project_run_disk_budget_exceeded",
                "project run does not have enough free space for an isolated campaign",
                {
                    "project_bytes": int(storage["project_bytes"]),
                    "estimated_peak_bytes": int(storage["estimated_peak_bytes"]),
                    "free_bytes": int(storage["free_bytes"]),
                    "required_free_bytes": int(storage["required_free_bytes"]),
                },
            )
        try:
            run = self.registry.register_project_run(planned)
        except (OSError, RuntimeError, TypeError, ValueError):
            return _rejected("project_run_registry_failed", "project run could not be persisted")
        self._log_project_run_event(run.run_id, "run_created", status="queued")
        if len(entries) <= PROJECT_RUN_INLINE_SOURCE_LIMIT:
            resolved_workers: int | None = None
            for index in range(len(entries)):
                run, _launched = self._launch_project_run_entry(run, index)
                if resolved_workers is None and run.worker_budget is not None:
                    resolved_workers = run.worker_budget
            if any(
                binding is not None and binding.database_path.is_file()
                for entry in run.entries
                for binding in (self.registry.campaign(entry.campaign_id),)
            ):
                self._start_project_run_dispatcher(run.run_id)
            return {"ok": True, "kind": "success", "value": self._project_run_summary(run, campaign_rows={})}
        self._start_project_run_dispatcher(run.run_id)
        return {"ok": True, "kind": "success", "value": self._project_run_summary(run, campaign_rows={})}
    def cancel_project_run(self, run_id: str) -> ClientResponse:
        # Cancel queued children immediately and forward one idempotent cancellation to the currently active child campaign.
        run = self.registry.project_run(run_id)
        if run is None:
            return _rejected("project_run_not_found", "project run does not exist")
        entries = list(run.entries)
        active_index = next(
            (
                index
                for index, entry in enumerate(entries)
                if entry.launch_status not in _PROJECT_RUN_TERMINAL_STATUSES and entry.launch_status not in {"queued", "rejected"}
            ),
            None,
        )
        changed = False
        for index, entry in enumerate(entries):
            if entry.launch_status != "queued":
                continue
            entries[index] = ProjectRunEntry(
                entry.source_path,
                entry.campaign_id,
                "cancelled",
                entry.error_code,
                entry.completed_mutants,
                entry.total_mutants,
                entry.error_stage,
                entry.failed_at,
            )
            changed = True
        if active_index is not None:
            active = entries[active_index]
            row = self._campaign_state(active.campaign_id)
            revision = row.get("revision_number") if isinstance(row, Mapping) else None
            if isinstance(revision, int) and not isinstance(revision, bool) and revision >= 0:
                result = self.cancel_campaign(f"project-run-cancel-{run_id}", active.campaign_id, expected_revision=revision)
                if result.get("ok") is True:
                    entries[active_index] = ProjectRunEntry(
                        active.source_path,
                        active.campaign_id,
                        "cancelling",
                        active.error_code,
                        active.completed_mutants,
                        active.total_mutants,
                        active.error_stage,
                        active.failed_at,
                    )
                    changed = True
        updated = run
        if changed:
            updated = ProjectRunRecord(run.run_id, run.project_id, tuple(entries), run.created_at, run.completed_at, run.worker_budget)
            updated = self.registry.update_project_run(updated, persist=True)
        self._log_project_run_event(updated.run_id, "run_cancel_requested", status="cancelling" if active_index is not None else "cancelled")
        updated = self._finalize_project_run(updated)
        return {"ok": True, "kind": "success", "value": self._project_run_summary(updated, include_campaigns=True)}
    def create_campaign(self, request: CampaignCreateRequest) -> ClientResponse:
        # Register and start one browser-created campaign.
        """Register and start one browser-created campaign."""
        return _success(self.authority(request))
    def _client(self, campaign_id: str) -> PublicApiCampaignClient | None:
        record = self.registry.campaign(campaign_id)
        return PublicApiCampaignClient(record.database_path) if record is not None else None
    def _missing_campaign(self) -> ClientResponse:
        return _rejected("campaign_not_found", "campaign does not exist")
    def get_campaign(self, campaign_id: str, *, related_limit: int) -> ClientResponse:
        # Enrich one public campaign detail with source, component cwd and bounded failed-baseline diagnostics.
        client = self._client(campaign_id)
        if client is None:
            return self._missing_campaign()
        response = client.get_campaign(campaign_id, related_limit=related_limit)
        if response.get("ok") is not True or not isinstance(response.get("value"), Mapping):
            return response
        value = dict(response["value"])
        campaign_value = value.get("campaign")
        campaign_row = dict(campaign_value) if isinstance(campaign_value, Mapping) else {}
        matched = self._project_run_entry(campaign_id)
        source_path = str(campaign_row.get("source_path") or "")
        test_cwd = "."
        if matched is not None:
            run, entry = matched
            source_path = source_path or entry.source_path
            profile = self.registry.project_profile(run.project_id)
            if profile is not None:
                test_cwd = project_test_cwd(profile, source_path)
        if source_path:
            campaign_row["source_path"] = source_path
            value["campaign"] = campaign_row
        value["attempt_failure_code"] = self._campaign_attempt_failure_code(campaign_id)
        value["execution_context"] = {
            "test_cwd": test_cwd,
            "baseline": self._baseline_diagnostic(campaign_id),
            "launcher": self._launcher_diagnostic(campaign_id),
        }
        return {"ok": True, "kind": str(response.get("kind", "success")), "value": value}
    def get_progress(self, campaign_id: str, *, shard_limit: int, shard_cursor: str | None = None) -> ClientResponse:
        client = self._client(campaign_id)
        return client.get_progress(campaign_id, shard_limit=shard_limit, shard_cursor=shard_cursor) if client else self._missing_campaign()
    def list_plans(self, *, limit: int, cursor: str | None = None, campaign_id: str | None = None) -> ClientResponse:
        client = self._client(campaign_id) if campaign_id else None
        if client and campaign_id:
            return client.list_plans(limit=limit, cursor=cursor, campaign_id=campaign_id)
        rows: list[dict[str, JsonValue]] = []
        for item in self.registry.campaigns():
            result = PublicApiCampaignClient(item.database_path).list_plans(limit=1, campaign_id=item.campaign_id)
            if result.get("ok") is True and isinstance(result.get("value"), Mapping):
                values = result["value"].get("items", [])
                if isinstance(values, list):
                    rows.extend(value for value in values if isinstance(value, dict))
        for item in rows:
            item["id"] = item.get("plan_id", "")
        return {"ok": True, "kind": "success", "value": _page(rows, limit, cursor, "plans")}
    def _campaign_delegate(self, campaign_id: str, method: str, *args: object, **kwargs: object) -> ClientResponse:
        client = self._client(campaign_id)
        if client is None:
            return self._missing_campaign()
        return getattr(client, method)(*args, **kwargs)
    def list_workers(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "list_workers", campaign_id, limit=limit, cursor=cursor)
    def list_shards(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "list_shards", campaign_id, limit=limit, cursor=cursor)
    def list_executions(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "list_executions", campaign_id, limit=limit, cursor=cursor)
    def list_artifacts(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "list_artifacts", campaign_id, limit=limit, cursor=cursor)
    def get_knowledge(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_knowledge", campaign_id, limit=limit, cursor=cursor)
    def stream_events(self, *, limit: int, cursor: str | None = None, campaign_id: str | None = None, event_type: str | None = None) -> ClientResponse:
        if campaign_id is None:
            return _rejected("invalid_request", "campaign_id is required")
        return self._campaign_delegate(campaign_id, "stream_events", limit=limit, cursor=cursor, campaign_id=campaign_id, event_type=event_type)
    def list_statistics(self, entity_type: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        rows: list[dict[str, JsonValue]] = []
        for item in self.registry.campaigns():
            result = PublicApiCampaignClient(item.database_path).list_statistics(entity_type, limit=limit, cursor=None)
            if result.get("ok") is True and isinstance(result.get("value"), Mapping):
                values = result["value"].get("items", [])
                if isinstance(values, list):
                    rows.extend(value for value in values if isinstance(value, dict))
        for item in rows:
            item["id"] = f"{item.get('entity_type', '')}:{item.get('entity_id', '')}"
        return {"ok": True, "kind": "success", "value": _page(rows, limit, cursor, "statistics")}
    def cancel_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "cancel_campaign", action_id, campaign_id, expected_revision=expected_revision)
    def start_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "start_campaign", action_id, campaign_id, expected_revision=expected_revision)
    def retry_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "retry_campaign", action_id, campaign_id, expected_revision=expected_revision)
    def resume_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "resume_campaign", action_id, campaign_id, expected_revision=expected_revision)
    def get_recovery_diagnostics(self, campaign_id: str, *, limit: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_recovery_diagnostics", campaign_id, limit=limit)
    def inspect_quarantine(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "inspect_quarantine", campaign_id, limit=limit, cursor=cursor)
    def get_artifact_registry(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_artifact_registry", campaign_id, limit=limit, cursor=cursor)
    def get_test_statistics(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_test_statistics", campaign_id, limit=limit, cursor=cursor)
    def get_reuse_evidence(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_reuse_evidence", campaign_id, limit=limit, cursor=cursor)
    def get_recovery_action(self, campaign_id: str, action_id: str) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "get_recovery_action", campaign_id, action_id)
    def reconcile_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "reconcile_campaign", action_id, campaign_id, expected_revision=expected_revision)
    def recover_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> ClientResponse:
        return self._campaign_delegate(campaign_id, "recover_campaign", action_id, campaign_id, expected_revision=expected_revision)
__all__ = [
    "DEFAULT_UI_STATE_DIR",
    "LocalBrowserCampaignAuthority",
    "LocalUiRegistry",
    "LocalWorkspaceCampaignClient",
    "RegisteredCampaign",
    "RegisteredProject",
]
