"use strict";
const PAGE_LIMIT = 50;
const EVENT_LIMIT = 40;
const EVENT_HISTORY_LIMIT = 200;
const POLL_DELAY_MS = 1800;
const RECOVERY_ACTION_POLL_DELAY_MS = 1800;
const TERMINAL_ACTION_STATUSES = new Set(["completed", "rejected", "failed", "cancelled"]);
const state = {
  sessionToken: null,
  campaignId: null,
  campaignRevision: null,
  projectRunId: null,
  selectedProjectId: null,
  projects: [],
  activeTab: "campaign",
  eventCursor: null,
  events: [],
  pollTimer: null,
  pollGeneration: 0,
  recoveryPollTimer: null,
  recoveryPollGeneration: 0,
  recoveryActionBusy: false,
  recoveryPages: {
    quarantine: { cursor: null, items: [] },
    artifacts: { cursor: null, items: [] },
    statistics: { cursor: null, items: [] },
    reuse: { cursor: null, items: [] },
  },
};
const byId = (id) => document.getElementById(id);
function setMessage(element, message, kind = "") {
  element.textContent = message;
  if (kind) {
    element.dataset.kind = kind;
  } else {
    delete element.dataset.kind;
  }
}
function clearNode(element) {
  while (element.firstChild) {
    element.removeChild(element.firstChild);
  }
}
const STATUS_LABELS = Object.freeze({
  pending: "Ожидает",
  queued: "В очереди",
  preparing: "Подготовка",
  indexing: "Индексация",
  materializing: "Подготовка тестов",
  starting: "Запускается",
  running: "Выполняется",
  completed: "Завершено",
  complete: "Завершено",
  cancelled: "Остановлено",
  canceled: "Остановлено",
  cancelling: "Останавливается",
  canceling: "Останавливается",
  failed: "Ошибка",
  rejected: "Отклонено",
  accepted: "Принято",
  blocked: "Заблокировано",
  stalled: "Застопорено",
  expired: "Истёк",
  active: "Активен",
  stopped: "Остановлен",
  killed: "Убит",
  survived: "Выжил",
  invalid: "Недействителен",
  invalid_mutant: "Недействителен",
  timeout: "Тайм-аут",
  timed_out: "Тайм-аут",
  error: "Ошибка",
  infrastructure_error: "Ошибка инфраструктуры",
  infrastructure_failed: "Ошибка инфраструктуры",
  verified: "Проверено",
  not_verified: "Не проверено",
  exact: "Точный",
  hint: "Подсказка",
  partial: "Частичный",
  off: "Отключён",
  observed: "Наблюдаемое",
  validated: "Подтверждено",
  unknown: "Неизвестно",
  ineligible: "Не подходит",
  degraded: "Ухудшено",
  unavailable: "Недоступно",
  direct: "Прямое",
  reused: "Переиспользовано",
  campaign_report: "Отчёт кампании",
  canonical_report: "Канонический отчёт",
  execution_evidence: "Данные выполнения",
  execution_result: "Результат выполнения",
  supporting_evidence: "Вспомогательное подтверждение",
});
const ACTION_LABELS = Object.freeze({
  start: "Запуск",
  cancel: "Остановка",
  retry: "Повторный запуск",
  resume: "Продолжение",
  reconcile: "Сверка",
  recover: "Восстановление",
});
const EVENT_LABELS = Object.freeze({
  campaign_created: "Кампания создана",
  campaign_started: "Кампания запущена",
  campaign_completed: "Кампания завершена",
  campaign_cancelled: "Кампания остановлена",
  campaign_failed: "Кампания завершилась с ошибкой",
  worker_started: "Рабочий процесс запущен",
  worker_stopped: "Рабочий процесс остановлен",
  shard_started: "Шард запущен",
  shard_completed: "Шард завершён",
  execution_started: "Выполнение начато",
  execution_completed: "Выполнение завершено",
  recovery_started: "Восстановление начато",
  recovery_completed: "Восстановление завершено",
});
const PROJECT_RUN_EVENT_LABELS = Object.freeze({
  run_created: "Запуск создан",
  dispatcher_started: "Диспетчер начал обработку",
  child_starting: "Запускается файл",
  child_started: "Кампания файла запущена",
  child_failed: "Файл завершился ошибкой",
  child_terminal: "Файл завершён",
  run_finished: "Запуск завершён",
  run_cancel_requested: "Остановка запуска запрошена",
  baseline_gate_blocked: "Компонент заблокирован после сбоя baseline",
  workspace_cleaned: "Временная рабочая копия очищена",
  workspace_cleanup_failed: "Не удалось очистить временную рабочую копию",
  disk_budget_blocked: "Запуск остановлен лимитом свободного места",
});
const FAILURE_STAGE_LABELS = Object.freeze({
  launch: "Запуск процесса",
  spawn: "Создание процесса",
  resume_campaign: "Запуск координатора",
  campaign: "Выполнение кампании",
  campaign_preparation: "Подготовка кампании",
  test_collection: "Сбор тестов",
  project_index: "Индексация проекта",
  baseline: "Baseline",
  source_inventory: "Состав проекта",
  coordinator: "Координатор",
  storage: "Хранилище",
});
function failureStageText(value) {
  const key = value === null || value === undefined ? "" : String(value).toLowerCase();
  return FAILURE_STAGE_LABELS[key] || (key ? humanize(value) : "—");
}
const ERROR_LABELS = Object.freeze({
  request_failed: "Ошибка запроса",
  local_ui_unavailable: "Локальный интерфейс недоступен",
  session_unavailable: "Сессия недоступна",
  stale_revision: "Состояние устарело",
  action_in_progress: "Действие уже выполняется",
  action_not_found: "Действие не найдено",
  project_registration_failed: "Не удалось добавить проект",
  project_run_not_found: "Запуск проекта не найден",
  project_run_no_sources: "В проекте не найдены production Python-файлы",
  project_run_discovery_failed: "Не удалось просканировать проект",
  project_run_registry_failed: "Не удалось сохранить запуск проекта",
  project_run_already_active: "У проекта уже есть незавершённый запуск",
  project_run_storage_check_failed: "Не удалось проверить свободное место",
  project_run_disk_budget_exceeded: "Недостаточно свободного места для безопасного запуска",
  project_disk_budget_exceeded: "Запуск остановлен: недостаточно свободного места",
  project_workspace_cleanup_failed: "Не удалось очистить временные рабочие копии",
  project_baseline_blocked: "Файл пропущен: общий baseline компонента не прошёл",
  campaign_launch_failed: "Сбой detached launcher",
  campaign_test_collection_failed: "Сбой сбора тестов",
  campaign_project_index_failed: "Сбой индексации проекта",
  campaign_baseline_failed: "Baseline не прошёл",
  launch_exception: "Исключение при запуске дочерней кампании",
  invalid_project_run: "Некорректный запуск проекта",
  launch_request_failed: "Не удалось запустить кампанию",
  invalid_campaign_launch: "Некорректные параметры запуска",
  project_registry_unavailable: "Реестр проектов недоступен",
});
const ERROR_MESSAGES = Object.freeze({
  request_failed: "Запрос не выполнен",
  local_ui_unavailable: "Локальный запрос не выполнен",
  session_unavailable: "Не удалось установить локальную сессию",
  stale_revision: "Кампания изменилась; данные обновлены",
  action_in_progress: "Дождитесь завершения текущего действия",
  action_not_found: "Сохранённое действие не найдено",
});
function text(value, fallback = "—") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}
function humanize(value) {
  return text(value).replaceAll("_", " ").replaceAll("-", " ");
}
function byteSize(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes >= 1024 ** 3) return `${(bytes / (1024 ** 3)).toFixed(1)} ГБ`;
  if (bytes >= 1024 ** 2) return `${(bytes / (1024 ** 2)).toFixed(1)} МБ`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} КБ`;
  return `${Math.round(bytes)} Б`;
}
function statusText(value, fallback = "—") {
  const key = value === null || value === undefined ? "" : String(value).toLowerCase();
  return STATUS_LABELS[key] || (key ? humanize(value) : fallback);
}
function actionText(value) {
  return ACTION_LABELS[value] || humanize(value);
}
function eventText(value) {
  return EVENT_LABELS[value] || humanize(value);
}
function countText(value, one, few, many) {
  const count = Number(value);
  const modulo10 = count % 10;
  const modulo100 = count % 100;
  const word = modulo10 === 1 && modulo100 !== 11 ? one : modulo10 >= 2 && modulo10 <= 4 && (modulo100 < 10 || modulo100 >= 20) ? few : many;
  return `${count} ${word}`;
}
function booleanText(value) {
  return value === true ? "Да" : value === false ? "Нет" : "—";
}
function apiError(result) {
  const error = result && result.error ? result.error : {};
  const code = text(error.code, "request_failed");
  const label = ERROR_LABELS[code] || "Ошибка";
  const message = ERROR_MESSAGES[code] || text(error.message, "Запрос не выполнен");
  return `${label}: ${message}`;
}
async function requestJson(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("Accept", "application/json");
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json");
    headers.set("X-Theseus-Session", state.sessionToken || "");
  }
  const response = await fetch(path, {
    method: options.method || "GET",
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    credentials: "same-origin",
    cache: "no-store",
  });
  const result = await response.json();
  return { response, result };
}
async function loadSession() {
  const { result } = await requestJson("/api/session");
  if (!result.ok || !result.value || !result.value.session_token) {
    throw new Error(apiError(result));
  }
  state.sessionToken = result.value.session_token;
}
function pageValue(result) {
  if (!result || !result.ok || !result.value) {
    return null;
  }
  if (Array.isArray(result.value.items)) {
    return result.value;
  }
  if (result.value.records && Array.isArray(result.value.records.items)) {
    return result.value.records;
  }
  return null;
}
function pageItems(result) {
  const page = pageValue(result);
  return page ? page.items : [];
}
const METRIC_TOOLTIPS = Object.freeze({
  "Статус": "Текущая стадия жизненного цикла кампании.",
  "Мутационный балл": "Доля убитых мутантов среди убитых и выживших. Недействительные мутанты и инфраструктурные ошибки в расчёт не входят.",
  "Убито": "Мутанты, для которых выбранные тесты доказали изменение поведения.",
  "Выжило": "Мутанты, которые прошли выбранные тесты и требуют анализа качества тестов или эквивалентности.",
  "Проблемы": "Сумма тайм-аутов и инфраструктурных ошибок текущей кампании.",
  "Рабочий каталог тестов": "Каталог, из которого Theseus запускает pytest. В component-aware проектах он может отличаться от корня репозитория.",
  "Baseline exit code": "Код завершения контрольного запуска тестов до применения мутаций. Ноль означает успешный baseline.",
  "Baseline timeout": "Показывает, был ли контрольный запуск остановлен по лимиту времени.",
  "Reuse": "Сколько результатов было переиспользовано на основании подтверждённого evidence.",
  "Эскалации": "Сколько раз Theseus расширял набор тестов, когда первоначального evidence было недостаточно.",
  "Последняя активность": "Последнее подтверждённое событие кампании в статистическом журнале.",
});
function attachTooltip(element, message) {
  if (!message) {
    return element;
  }
  element.classList.add("has-tooltip");
  element.dataset.tooltip = message;
  element.setAttribute("aria-label", `${element.textContent || "Справка"}: ${message}`);
  element.tabIndex = 0;
  return element;
}
function statusTone(value) {
  const key = String(value || "").toLowerCase();
  if (["completed", "complete", "killed", "verified", "validated", "accepted", "reused", "healthy"].includes(key)) {
    return "success";
  }
  if (["failed", "error", "rejected", "timeout", "timed_out", "infrastructure_error", "infrastructure_failed", "stalled", "expired"].includes(key)) {
    return "danger";
  }
  if (["survived", "invalid", "invalid_mutant", "cancelled", "canceled", "cancelling", "canceling", "blocked", "degraded"].includes(key)) {
    return "warning";
  }
  if (["pending", "queued", "preparing", "indexing", "materializing", "starting", "running", "active"].includes(key)) {
    return "info";
  }
  return "muted";
}
function statusBadge(value, label = null) {
  const badge = document.createElement("span");
  badge.className = "status-badge";
  badge.dataset.tone = statusTone(value);
  badge.textContent = label || statusText(value);
  badge.title = text(value, "unknown");
  return badge;
}
function addCell(row, value, className = "") {
  const cell = document.createElement("td");
  cell.textContent = text(value);
  if (className) {
    cell.className = className;
  }
  row.appendChild(cell);
  return cell;
}
function addStatusCell(row, value) {
  const cell = document.createElement("td");
  cell.appendChild(statusBadge(value));
  row.appendChild(cell);
  return cell;
}
function compactIdentity(value, maxLength = 42) {
  const current = text(value);
  if (current.length <= maxLength) {
    return current;
  }
  const tail = Math.max(12, Math.floor(maxLength * 0.45));
  const head = Math.max(12, maxLength - tail - 1);
  return `${current.slice(0, head)}…${current.slice(-tail)}`;
}
function addIdentityCell(row, value) {
  const cell = addCell(row, compactIdentity(value), "identity-cell");
  cell.title = text(value);
  return cell;
}
function addProgressCell(row, completed, total) {
  const cell = document.createElement("td");
  cell.className = "progress-cell";
  const label = document.createElement("span");
  label.textContent = progressText(completed, total);
  const count = Number(total || 0);
  const done = Number(completed || 0);
  const bar = document.createElement("progress");
  bar.className = "table-progress";
  bar.max = Math.max(1, count);
  bar.value = Math.min(Math.max(0, done), Math.max(1, count));
  cell.append(label, bar);
  row.appendChild(cell);
  return cell;
}
function setCount(id, value) {
  const element = byId(id);
  if (element) {
    element.textContent = String(Number(value || 0));
  }
}
function addMetric(container, label, value, options = {}) {
  const wrapper = document.createElement("dl");
  wrapper.className = `metric${options.className ? ` ${options.className}` : ""}`;
  if (options.tone) {
    wrapper.dataset.tone = options.tone;
  }
  const term = document.createElement("dt");
  term.textContent = label;
  attachTooltip(term, options.tooltip || METRIC_TOOLTIPS[label]);
  const description = document.createElement("dd");
  if (options.badge) {
    description.appendChild(statusBadge(options.statusValue ?? value, text(value)));
  } else {
    description.textContent = text(value);
  }
  wrapper.append(term, description);
  container.appendChild(wrapper);
  return wrapper;
}
function addProgressMetric(container, completed, total) {
  const done = Number(completed || 0);
  const count = Number(total || 0);
  const percent = count > 0 ? Math.max(0, Math.min(100, Math.round((done / count) * 100))) : 0;
  const wrapper = addMetric(container, "Прогресс", progressText(done, count), { className: "metric-progress", tone: "info", tooltip: "Сколько мутантов кампании уже получили конечный результат." });
  const bar = document.createElement("progress");
  bar.className = "progress-bar";
  bar.max = 100;
  bar.value = percent;
  bar.textContent = `${percent}%`;
  wrapper.appendChild(bar);
  return wrapper;
}
function addContextItem(container, label, value, tooltip) {
  const item = document.createElement("div");
  item.className = "context-item";
  const term = document.createElement("span");
  term.className = "context-label";
  term.textContent = label;
  attachTooltip(term, tooltip);
  const content = document.createElement("code");
  content.textContent = text(value);
  content.title = text(value);
  item.append(term, content);
  container.appendChild(item);
}
function addDefinition(container, label, value, tooltip = null) {
  const term = document.createElement("dt");
  term.textContent = label;
  attachTooltip(term, tooltip);
  const description = document.createElement("dd");
  description.textContent = text(value);
  container.append(term, description);
}
function addStatusDefinition(container, label, value, tooltip = null) {
  const term = document.createElement("dt");
  term.textContent = label;
  attachTooltip(term, tooltip);
  const description = document.createElement("dd");
  description.appendChild(statusBadge(value));
  container.append(term, description);
}
function progressText(completed, total) {
  const done = Number(completed || 0);
  const count = Number(total || 0);
  const percent = count > 0 ? Math.round((done / count) * 100) : 0;
  return `${done}/${count} (${percent}%)`;
}
function boundedJoin(values) {
  if (!Array.isArray(values) || values.length === 0) {
    return "—";
  }
  return values.slice(0, 20).map((value) => text(value)).join(", ");
}
function resetRecoveryPages() {
  for (const page of Object.values(state.recoveryPages)) {
    page.cursor = null;
    page.items = [];
  }
}
async function loadOverviewProjects() {
  try {
    const response = await requestJson(`/api/projects?limit=${PAGE_LIMIT}`);
    renderProjects(response.result);
    renderProjectOptions(response.result);
    setMessage(byId("connection-status"), "Подключено к локальному API", "success");
  } catch (_error) {
    setMessage(byId("connection-status"), "Локальный интерфейс недоступен: проекты не загружены", "error");
    setMessage(byId("projects-state"), "Локальный интерфейс недоступен: проекты не загружены", "error");
  }
}
async function loadOverviewCampaigns() {
  try {
    const response = await requestJson(`/api/campaigns?limit=${PAGE_LIMIT}`);
    renderCampaigns(response.result);
  } catch (_error) {
    setMessage(byId("campaigns-state"), "Кампании временно не загружены", "error");
  }
}
async function loadOverviewProjectRuns() {
  try {
    const response = await requestJson(`/api/project-runs?limit=${PAGE_LIMIT}`);
    renderProjectRuns(response.result);
  } catch (_error) {
    setMessage(byId("project-runs-state"), "Запуски временно не загружены", "error");
  }
}
async function loadOverview() {
  stopEventPolling();
  stopRecoveryActionPolling();
  state.campaignId = null;
  state.campaignRevision = null;
  byId("detail-view").hidden = true;
  byId("overview-view").hidden = false;
  setMessage(byId("connection-status"), "Чтение состояния локальных кампаний…");
  setMessage(byId("projects-state"), "Загрузка проектов…");
  setMessage(byId("campaigns-state"), "Загрузка кампаний…");
  setMessage(byId("project-runs-state"), "Загрузка запусков…");
  await loadOverviewProjects();
  void loadOverviewCampaigns();
  void loadOverviewProjectRuns();
}
function renderProjects(result) {
  const list = byId("project-list");
  clearNode(list);
  if (!result.ok) {
    setMessage(byId("projects-state"), apiError(result), "error");
    return;
  }
  const items = pageItems(result);
  if (items.length === 0) {
    setMessage(byId("projects-state"), "Проекты не найдены.");
    return;
  }
  setMessage(byId("projects-state"), countText(items.length, "проект", "проекта", "проектов"));
  for (const project of items) {
    const item = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = `${text(project.display_name, project.project_id)} — ${countText(project.project_run_count || 0, "запуск", "запуска", "запусков")} · ${countText(project.campaign_count, "кампания", "кампании", "кампаний")}`;
    button.addEventListener("click", () => selectProject(project.project_id));
    item.appendChild(button);
    list.appendChild(item);
  }
}
function renderProjectOptions(result) {
  const select = byId("create-project-select");
  if (!select) {
    return;
  }
  const previous = state.selectedProjectId;
  while (select.options.length > 1) {
    select.remove(1);
  }
  state.projects = result && result.ok ? pageItems(result) : [];
  if (!result || !result.ok) {
    select.value = "";
    applyProjectProfile(null);
    return;
  }
  for (const project of state.projects) {
    const option = document.createElement("option");
    option.value = text(project.project_id, "");
    option.textContent = `${text(project.display_name, project.project_id)} (${text(project.root_name, "локальный")})`;
    select.appendChild(option);
  }
  if (previous && Array.from(select.options).some((option) => option.value === previous)) {
    select.value = previous;
  } else if (select.options.length === 2) {
    select.selectedIndex = 1;
    state.selectedProjectId = select.value;
  }
  applyProjectProfile(select.value);
}
function applyProjectProfile(projectId) {
  const project = state.projects.find((item) => item.project_id === projectId);
  const profile = project && project.profile;
  const form = byId("create-campaign-form");
  const runButton = byId("project-run-button");
  if (!form || !profile) {
    setMessage(byId("project-profile-summary"), "Выберите проект, чтобы использовать сохранённый профиль запуска.");
    setMessage(byId("project-run-selection"), "Выберите проект для запуска.");
    if (runButton) {
      runButton.disabled = true;
    }
    return;
  }
  const command = Array.isArray(profile.test_command) ? profile.test_command.join(" ") : "pytest";
  form.elements.namedItem("test_command").placeholder = command;
  form.elements.namedItem("max_mutants").placeholder = String(profile.max_mutants || 100);
  form.elements.namedItem("max_workers").placeholder = String(profile.preferred_workers || 1);
  form.elements.namedItem("max_seconds").placeholder = String(profile.max_seconds || 600);
  form.elements.namedItem("max_test_seconds").placeholder = String(profile.max_test_seconds || 120);
  const sourceRoots = Array.isArray(profile.source_roots) && profile.source_roots.length ? profile.source_roots.join(", ") : "не определены";
  const testRoots = Array.isArray(profile.test_roots) && profile.test_roots.length ? profile.test_roots.join(", ") : "не определены";
  setMessage(byId("project-profile-summary"), `Профиль: Python ${text(profile.python_interpreter, "не определён")} · исходники: ${sourceRoots} · тесты: ${testRoots}`);
  setMessage(byId("project-run-selection"), `Готов к запуску: ${text(project.display_name, project.project_id)} · исходники: ${sourceRoots} · тесты: ${testRoots}`, "success");
  if (runButton) {
    runButton.disabled = false;
  }
}
function selectProject(projectId) {
  state.selectedProjectId = projectId || null;
  const select = byId("create-project-select");
  if (select) {
    select.value = projectId || "";
  }
  applyProjectProfile(projectId);
  return Promise.all([filterCampaigns(projectId), filterProjectRuns(projectId)]);
}
async function filterCampaigns(projectId) {
  state.selectedProjectId = projectId || null;
  setMessage(byId("campaigns-state"), "Загрузка кампаний…");
  try {
    const { result } = await requestJson(`/api/campaigns?limit=${PAGE_LIMIT}&project_id=${encodeURIComponent(projectId)}`);
    renderCampaigns(result);
  } catch (_error) {
    setMessage(byId("campaigns-state"), "Локальный интерфейс недоступен: кампании не загружены", "error");
  }
}
async function filterProjectRuns(projectId) {
  setMessage(byId("project-runs-state"), "Загрузка запусков…");
  try {
    const { result } = await requestJson(`/api/project-runs?limit=${PAGE_LIMIT}&project_id=${encodeURIComponent(projectId)}`);
    renderProjectRuns(result);
  } catch (_error) {
    setMessage(byId("project-runs-state"), "Локальный интерфейс недоступен: запуски не загружены", "error");
  }
}
function projectRunFileSummary(run) {
  const complete = Number(run.completed_count || 0);
  const failed = Number(run.failed_count || 0);
  const active = Number(run.active_count || 0);
  const queued = Number(run.queued_count || 0);
  return `${complete} ✓ · ${failed} × · ${active} ▶ · ${queued} …`;
}
function projectRunQueueText(run) {
  if (run.queue_position === 0) {
    return "сейчас";
  }
  if (Number(run.queue_position || 0) > 0) {
    return `№ ${run.queue_position}`;
  }
  return "—";
}
function renderProjectRuns(result) {
  const body = byId("project-run-table-body");
  clearNode(body);
  if (!result.ok) {
    setMessage(byId("project-runs-state"), apiError(result), "error");
    return;
  }
  const items = pageItems(result);
  if (items.length === 0) {
    setMessage(byId("project-runs-state"), "Запуски проектов не найдены.");
    return;
  }
  setMessage(byId("project-runs-state"), countText(items.length, "запуск", "запуска", "запусков"));
  for (const run of items) {
    const row = document.createElement("tr");
    addIdentityCell(row, run.run_id);
    addStatusCell(row, run.status);
    const filesCell = addCell(row, projectRunFileSummary(run));
    filesCell.title = `Всего файлов: ${Number(run.source_count || 0)}; завершено: ${Number(run.completed_count || 0)}; ошибок: ${Number(run.failed_count || 0)}; активно: ${Number(run.active_count || 0)}; в очереди: ${Number(run.queued_count || 0)}`;
    addCell(row, projectRunQueueText(run), "numeric-cell");
    const failureCell = document.createElement("td");
    if (Number(run.failed_count || 0) > 0) {
      failureCell.appendChild(statusBadge("failed", String(run.failed_count)));
    } else {
      failureCell.textContent = "—";
    }
    row.appendChild(failureCell);
    addProgressCell(row, run.completed_mutants, run.total_mutants);
    const actionCell = document.createElement("td");
    actionCell.className = "project-run-actions";
    const openButton = document.createElement("button");
    openButton.type = "button";
    openButton.textContent = "Открыть";
    openButton.addEventListener("click", () => openProjectRun(run.run_id));
    actionCell.appendChild(openButton);
    if (["queued", "running"].includes(String(run.status || "").toLowerCase())) {
      const cancelButton = document.createElement("button");
      cancelButton.type = "button";
      cancelButton.className = "button-danger";
      cancelButton.textContent = "Остановить";
      cancelButton.addEventListener("click", () => cancelProjectRun(run.run_id, cancelButton));
      actionCell.appendChild(cancelButton);
    }
    row.appendChild(actionCell);
    body.appendChild(row);
  }
}
function renderProjectRunDetail(result) {
  const panel = byId("project-run-detail-panel");
  const summary = byId("project-run-detail-summary");
  const context = byId("project-run-current");
  const failureBody = byId("project-run-failure-groups-body");
  const filesBody = byId("project-run-files-body");
  const events = byId("project-run-event-list");
  clearNode(summary);
  clearNode(context);
  clearNode(failureBody);
  clearNode(filesBody);
  clearNode(events);
  if (!result || !result.ok) {
    setMessage(byId("project-run-detail-state"), apiError(result || {}), "error");
    return;
  }
  const run = result.value || {};
  state.projectRunId = run.run_id || state.projectRunId;
  panel.hidden = false;
  byId("project-run-detail-subtitle").textContent = text(run.run_id);
  setMessage(byId("project-run-detail-state"), "Состояние запуска загружено", "success");
  addMetric(summary, "Статус", statusText(run.status), { badge: true, statusValue: run.status, tooltip: "Фактическое состояние ProjectRun: выполняется, ожидает глобальный dispatcher или завершён." });
  addMetric(summary, "Файлы", `${Number(run.processed_count || 0)}/${Number(run.source_count || 0)}`, { tooltip: "Сколько production-файлов уже вышли из очереди ProjectRun." });
  addMetric(summary, "В очереди", Number(run.queued_count || 0), { tone: Number(run.queued_count || 0) ? "info" : "success", tooltip: "Файлы, для которых дочерняя кампания ещё не запускалась." });
  addMetric(summary, "Активно", Number(run.active_count || 0), { tone: Number(run.active_count || 0) ? "info" : null, tooltip: "Файлы, чья дочерняя кампания сейчас находится в активной стадии." });
  addMetric(summary, "Завершено", Number(run.completed_count || 0), { tone: Number(run.completed_count || 0) ? "success" : null });
  addMetric(summary, "Ошибки", Number(run.failed_count || 0), { tone: Number(run.failed_count || 0) ? "danger" : "success", tooltip: "Файлы с сохранённым terminal failure code." });
  const storage = run.storage && typeof run.storage === "object" ? run.storage : null;
  if (storage) {
    addMetric(summary, "Свободно", byteSize(storage.free_bytes), { tone: storage.ok ? "success" : "danger", tooltip: `Theseus сохраняет резерв ${byteSize(Number(storage.required_free_bytes || 0) - Number(storage.estimated_peak_bytes || 0))} и не запускает новую кампанию ниже него.` });
    addMetric(summary, "Оценка пика", byteSize(storage.estimated_peak_bytes), { tone: "info", tooltip: `Оценка одновременных изолированных рабочих копий: ${Number(storage.parallel_campaigns || 1)} камп. × ${Number(storage.workers || 1)} ворк.` });
  }
  if (run.queue_position === 0) {
    addContextItem(context, "Очередь", "выполняется сейчас", "Этот ProjectRun владеет глобальным последовательным dispatcher.");
  } else if (Number(run.queue_position || 0) > 0) {
    addContextItem(context, "Очередь", `место ${run.queue_position}`, "ProjectRun ожидает завершения более старых запусков.");
  }
  if (run.current_source_path) {
    addContextItem(context, "Текущий файл", run.current_source_path, "Production-файл, дочерняя кампания которого сейчас выполняется.");
  } else if (run.next_source_path) {
    addContextItem(context, "Следующий файл", run.next_source_path, "Первый ещё не запущенный production-файл.");
  }
  const groups = Array.isArray(run.failure_groups) ? run.failure_groups : [];
  setCount("project-run-failure-count", run.failed_count || 0);
  for (const group of groups) {
    const row = document.createElement("tr");
    addCell(row, failureStageText(group.stage), "project-run-file-stage");
    addCell(row, ERROR_LABELS[group.code] || text(group.code), "project-run-file-error");
    addCell(row, Number(group.count || 0), "numeric-cell");
    failureBody.appendChild(row);
  }
  if (groups.length === 0) {
    const row = document.createElement("tr");
    const cell = addCell(row, "Ошибок пока нет");
    cell.colSpan = 3;
    failureBody.appendChild(row);
  }
  const files = Array.isArray(run.campaigns) ? run.campaigns : [];
  setCount("project-run-files-count", run.source_count || files.length);
  for (const item of files) {
    const row = document.createElement("tr");
    addCell(row, item.source_path, "identity-cell");
    addStatusCell(row, item.status);
    addCell(row, failureStageText(item.error_stage), "project-run-file-stage");
    addCell(row, item.error_code ? (ERROR_LABELS[item.error_code] || item.error_code) : "—", item.error_code ? "project-run-file-error" : "");
    addProgressCell(row, item.completed_mutants, item.total_mutants);
    const action = document.createElement("td");
    if (item.campaign_id && !["queued"].includes(String(item.status || "").toLowerCase())) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = "Кампания";
      button.addEventListener("click", () => openCampaign(item.campaign_id));
      action.appendChild(button);
    } else {
      action.textContent = "—";
    }
    row.appendChild(action);
    filesBody.appendChild(row);
  }
  byId("project-run-files-note").textContent = run.campaigns_truncated ? "Показаны приоритетно первые 200 записей; ошибки выводятся раньше обычных файлов." : "";
  const eventRows = Array.isArray(run.events) ? run.events : [];
  setCount("project-run-events-count", eventRows.length);
  for (const item of eventRows.slice().reverse()) {
    const li = document.createElement("li");
    const label = PROJECT_RUN_EVENT_LABELS[item.event_type] || humanize(item.event_type);
    const source = item.source_path ? ` · ${item.source_path}` : "";
    const error = item.error_code ? ` · ${item.error_stage || "unknown"}: ${item.error_code}` : "";
    li.textContent = `${text(item.recorded_at, "—")} · ${label}${source}${error}`;
    events.appendChild(li);
  }
  if (eventRows.length === 0) {
    const li = document.createElement("li");
    li.textContent = "Журнал появится после первого перехода A14; старые переходы до обновления не восстанавливаются задним числом.";
    events.appendChild(li);
  }
  panel.scrollIntoView({ behavior: "smooth", block: "start" });
}
async function openProjectRun(runId) {
  state.projectRunId = runId;
  byId("project-run-detail-panel").hidden = false;
  setMessage(byId("project-run-detail-state"), "Загрузка диагностики запуска…");
  try {
    const { result } = await requestJson(`/api/project-runs/${encodeURIComponent(runId)}`);
    renderProjectRunDetail(result);
  } catch (_error) {
    setMessage(byId("project-run-detail-state"), "Диагностика запуска временно недоступна", "error");
  }
}
async function refreshProjectRunDetail() {
  if (state.projectRunId) {
    await openProjectRun(state.projectRunId);
  }
}
function closeProjectRunDetail() {
  state.projectRunId = null;
  byId("project-run-detail-panel").hidden = true;
}
async function cancelProjectRun(runId, button = null) {
  if (!window.confirm("Остановить этот запуск проекта? Ожидающие файлы будут отменены, активная дочерняя кампания получит запрос на остановку.")) {
    return;
  }
  if (button) {
    button.disabled = true;
  }
  try {
    const { result } = await requestJson(`/api/project-runs/${encodeURIComponent(runId)}/actions/cancel`, { method: "POST", body: {} });
    if (!result.ok) {
      setMessage(byId("project-runs-state"), apiError(result), "error");
      return;
    }
    if (state.projectRunId === runId) {
      renderProjectRunDetail(result);
    }
    await loadOverviewProjectRuns();
  } catch (_error) {
    setMessage(byId("project-runs-state"), "Не удалось остановить запуск проекта", "error");
  } finally {
    if (button) {
      button.disabled = false;
    }
  }
}
function renderCampaigns(result) {
  const body = byId("campaign-table-body");
  clearNode(body);
  if (!result.ok) {
    setMessage(byId("campaigns-state"), apiError(result), "error");
    return;
  }
  const items = pageItems(result);
  if (items.length === 0) {
    setMessage(byId("campaigns-state"), "Кампании не найдены.");
    return;
  }
  setMessage(byId("campaigns-state"), countText(items.length, "кампания", "кампании", "кампаний"));
  for (const campaign of items) {
    const row = document.createElement("tr");
    const nameCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "link-button identity-link";
    button.textContent = compactIdentity(campaign.campaign_id);
    button.title = text(campaign.campaign_id);
    button.addEventListener("click", () => openCampaign(campaign.campaign_id));
    nameCell.appendChild(button);
    row.appendChild(nameCell);
    addStatusCell(row, campaign.status);
    addCell(row, campaign.revision_number, "numeric-cell");
    addProgressCell(row, campaign.completed_mutants, campaign.total_mutants);
    addCell(row, campaign.mutation_score);
    addCell(row, campaign.last_activity);
    body.appendChild(row);
  }
}
async function openCampaign(campaignId) {
  stopEventPolling();
  stopRecoveryActionPolling();
  state.campaignId = campaignId;
  state.campaignRevision = null;
  state.eventCursor = sessionStorage.getItem(`theseus-event-cursor:${campaignId}`);
  state.events = [];
  resetRecoveryPages();
  byId("overview-view").hidden = true;
  byId("detail-view").hidden = false;
  byId("detail-subtitle").textContent = campaignId;
  switchDetailTab("campaign");
  setMessage(byId("detail-state"), "Загрузка деталей кампании…");
  await refreshCampaignDetail();
  startEventPolling();
}
function switchDetailTab(tab) {
  state.activeTab = tab;
  const recovery = tab === "recovery";
  byId("campaign-content").hidden = recovery;
  byId("recovery-content").hidden = !recovery;
  byId("campaign-tab").setAttribute("aria-selected", String(!recovery));
  byId("recovery-tab").setAttribute("aria-selected", String(recovery));
  if (recovery) {
    loadRecoveryScreen();
  }
}
async function refreshCampaignDetail() {
  const campaignId = state.campaignId;
  if (!campaignId) {
    return;
  }
  try {
    const encoded = encodeURIComponent(campaignId);
    const [detailResponse, progressResponse, knowledgeResponse, statisticsResponse] = await Promise.all([
      requestJson(`/api/campaigns/${encoded}?limit=${PAGE_LIMIT}`),
      requestJson(`/api/campaigns/${encoded}/progress?limit=${PAGE_LIMIT}`),
      requestJson(`/api/campaigns/${encoded}/knowledge?limit=${PAGE_LIMIT}`),
      requestJson(`/api/campaigns/${encoded}/statistics?limit=${PAGE_LIMIT}`),
    ]);
    if (!detailResponse.result.ok) {
      setMessage(byId("detail-state"), apiError(detailResponse.result), "error");
      return;
    }
    renderCampaignDetail(detailResponse.result.value, progressResponse.result, knowledgeResponse.result, statisticsResponse.result);
    setMessage(byId("detail-state"), "Состояние кампании загружено", "success");
  } catch (_error) {
    setMessage(byId("detail-state"), "Локальный интерфейс недоступен: детали кампании не загружены", "error");
  }
}
function renderCampaignDetail(detail, progressResult, knowledgeResult, statisticsResult) {
  const campaign = detail.campaign || {};
  const plan = detail.plan || {};
  state.campaignRevision = Number(campaign.revision_number || 0);
  renderSummary(campaign, knowledgeResult, statisticsResult, detail.attempt_failure_code, detail.execution_context || {});
  renderPlan(plan);
  renderProgress(progressResult);
  renderWorkers(detail.workers && detail.workers.items ? detail.workers.items : []);
  renderShards(detail.shards && detail.shards.items ? detail.shards.items : []);
  renderExecutions(detail.executions && detail.executions.items ? detail.executions.items : [], knowledgeResult);
  renderArtifacts(detail.artifacts && detail.artifacts.items ? detail.artifacts.items : []);
}
function countEvidence(counts, names) {
  return names.reduce((total, name) => total + Number(counts[name] || 0), 0);
}
function renderFailureAlert(container, attemptFailureCode, baseline, launcher = null) {
  if (!attemptFailureCode && !launcher && (!baseline || (Number(baseline.exit_code || 0) === 0 && !baseline.timed_out))) {
    return;
  }
  const alert = document.createElement("section");
  alert.className = "diagnostic-alert";
  alert.dataset.tone = "danger";
  const heading = document.createElement("div");
  heading.className = "diagnostic-alert-heading";
  heading.appendChild(statusBadge("failed", "Нужна проверка"));
  const title = document.createElement("strong");
  title.textContent = attemptFailureCode ? (ERROR_LABELS[attemptFailureCode] || humanize(attemptFailureCode)) : "Baseline завершился с ошибкой";
  heading.appendChild(title);
  alert.appendChild(heading);
  const facts = [];
  if (baseline && baseline.exit_code !== null && baseline.exit_code !== undefined) {
    facts.push(`exit code ${baseline.exit_code}`);
  }
  if (baseline && baseline.timed_out) {
    facts.push("превышен лимит времени");
  }
  if (launcher && launcher.stage) {
    facts.push(`launch stage: ${launcher.stage}`);
  }
  if (launcher && launcher.exception_type) {
    facts.push(`exception: ${launcher.exception_type}`);
  }
  if (facts.length) {
    const summary = document.createElement("p");
    summary.textContent = `Baseline: ${facts.join(" · ")}`;
    alert.appendChild(summary);
  }
  if (launcher && launcher.stderr_log) {
    const logHint = document.createElement("p");
    logHint.textContent = `Полный traceback: ${launcher.stderr_log} в каталоге состояния этой кампании.`;
    alert.appendChild(logHint);
  }
  if (baseline && Array.isArray(baseline.diagnostic_excerpts) && baseline.diagnostic_excerpts.length) {
    const disclosure = document.createElement("details");
    disclosure.className = "diagnostic-excerpt";
    const summary = document.createElement("summary");
    summary.textContent = "Показать диагностический фрагмент";
    const pre = document.createElement("pre");
    pre.textContent = baseline.diagnostic_excerpts.join("\n");
    disclosure.append(summary, pre);
    alert.appendChild(disclosure);
  }
  container.appendChild(alert);
}
function renderSummary(campaign, knowledgeResult, statisticsResult, attemptFailureCode = null, executionContext = {}) {
  const container = byId("campaign-summary");
  const secondary = byId("campaign-secondary-summary");
  const context = byId("campaign-context");
  const alerts = byId("campaign-alerts");
  clearNode(container);
  clearNode(secondary);
  clearNode(context);
  clearNode(alerts);
  const counts = knowledgeResult.ok && knowledgeResult.value ? knowledgeResult.value.counts || {} : {};
  const statistics = pageItems(statisticsResult).find((item) => item.entity_id === campaign.campaign_id) || {};
  const killed = countEvidence(counts, ["killed", "kill"]);
  const survived = countEvidence(counts, ["survived", "survive"]);
  const invalid = countEvidence(counts, ["invalid_mutant", "invalid"]);
  const timeout = countEvidence(counts, ["timeout", "timed_out"]) || Number(statistics.timeout_count || 0);
  const infrastructure = countEvidence(counts, ["infrastructure_error", "infrastructure_failed", "error"]) || Number(statistics.infrastructure_failure_count || 0);
  const problems = timeout + infrastructure;
  const score = killed + survived > 0 ? `${Math.round((killed / (killed + survived)) * 100)}%` : "—";
  const baseline = executionContext.baseline || null;
  const launcher = executionContext.launcher || null;
  addContextItem(context, "Файл", campaign.source_path, "Production-файл, для которого создана эта дочерняя mutation campaign.");
  addContextItem(context, "Тесты из", executionContext.test_cwd, METRIC_TOOLTIPS["Рабочий каталог тестов"]);
  addMetric(container, "Статус", statusText(campaign.status), { badge: true, statusValue: campaign.status, tone: statusTone(campaign.status) });
  addProgressMetric(container, campaign.completed_mutants, campaign.total_mutants);
  addMetric(container, "Мутационный балл", score, { tone: "info" });
  addMetric(container, "Убито", killed, { tone: "success" });
  addMetric(container, "Выжило", survived, { tone: survived > 0 ? "warning" : "muted" });
  addMetric(container, "Проблемы", problems, { tone: problems > 0 ? "danger" : "success" });
  addMetric(secondary, "Недействительно", invalid, { tone: invalid > 0 ? "warning" : "muted" });
  addMetric(secondary, "Тайм-ауты", timeout, { tone: timeout > 0 ? "danger" : "muted", tooltip: "Мутанты или test runs, остановленные по лимиту времени." });
  addMetric(secondary, "Инфраструктура", infrastructure, { tone: infrastructure > 0 ? "danger" : "muted", tooltip: "Ошибки среды выполнения, которые нельзя считать killed/survived результатом." });
  addMetric(secondary, "Reuse", Number(statistics.reuse_count || 0));
  addMetric(secondary, "Эскалации", Number(statistics.escalation_count || 0));
  addMetric(secondary, "Последняя активность", statistics.last_event_timestamp, { className: "metric-wide" });
  if (baseline) {
    addMetric(secondary, "Baseline exit code", baseline.exit_code, { tone: Number(baseline.exit_code || 0) === 0 ? "success" : "danger" });
    addMetric(secondary, "Baseline timeout", baseline.timed_out ? "Да" : "Нет", { tone: baseline.timed_out ? "danger" : "success" });
  }
  if (launcher) {
    addMetric(secondary, "Launch stage", launcher.stage, { tone: "danger", tooltip: "Стадия detached launcher, на которой возникло необработанное исключение." });
    addMetric(secondary, "Exception", launcher.exception_type, { tone: "danger", tooltip: "Тип исключения; полный traceback хранится только в приватном launch log." });
  }
  renderFailureAlert(alerts, attemptFailureCode, baseline, launcher);
}
function renderPlan(plan) {
  const container = byId("plan-details");
  clearNode(container);
  addDefinition(container, "Идентификатор плана", plan.plan_id);
  addDefinition(container, "Подготовленный снимок", plan.prepared_snapshot_id);
  addDefinition(container, "Выбранные мутанты", plan.selected_mutants);
  addDefinition(container, "Исключённые мутанты", plan.excluded_mutants);
  addDefinition(container, "Оценка реального времени", plan.estimated_wall_seconds);
  addDefinition(container, "Оценка процессорного времени", plan.estimated_cpu_seconds);
}
function renderProgress(result) {
  const container = byId("progress-details");
  clearNode(container);
  if (!result.ok) {
    addDefinition(container, "Ошибка", apiError(result));
    return;
  }
  const progress = result.value || {};
  addStatusDefinition(container, "Кампания", progress.campaign_status, "Текущая стадия выполнения этой кампании.");
  addDefinition(container, "Мутанты", progressText(progress.completed_mutants, progress.total_mutants), "Завершённые мутанты относительно общего числа в плане.");
  addDefinition(container, "Процессы", progress.active_workers, "Количество активных worker-процессов прямо сейчас.");
  addDefinition(container, "Очередь", progress.pending_outbox, "Сколько событий/работ ожидает отправки или обработки.");
  addDefinition(container, "Карантин", progress.quarantine_count, "Число конфликтных или недостоверных результатов, изолированных от authoritative state.");
  addStatusDefinition(container, "Recovery", progress.recovery_state, "Состояние механизма восстановления durable campaign state.");
}
function renderWorkers(items) {
  const body = byId("workers-body");
  clearNode(body);
  setCount("workers-count", items.length);
  for (const worker of items) {
    const row = document.createElement("tr");
    addIdentityCell(row, worker.worker_id);
    addStatusCell(row, worker.status);
    addIdentityCell(row, worker.current_shard_id);
    addCell(row, worker.completed_mutants, "numeric-cell");
    body.appendChild(row);
  }
}
function renderShards(items) {
  const body = byId("shards-body");
  clearNode(body);
  setCount("shards-count", items.length);
  for (const shard of items) {
    const row = document.createElement("tr");
    addIdentityCell(row, shard.shard_id);
    addStatusCell(row, shard.status);
    addCell(row, shard.attempt, "numeric-cell");
    addProgressCell(row, shard.completed_count, shard.mutant_count);
    body.appendChild(row);
  }
}
function renderExecutions(items, knowledgeResult) {
  const body = byId("executions-body");
  const countsContainer = byId("execution-counts");
  clearNode(body);
  clearNode(countsContainer);
  setCount("executions-count", items.length);
  const knowledgeRecords = knowledgeResult.ok && knowledgeResult.value && knowledgeResult.value.records
    ? knowledgeResult.value.records.items || []
    : [];
  const knowledgeByExecution = new Map(knowledgeRecords.map((item) => [item.execution_id, item]));
  const counts = { killed: 0, survived: 0, invalid_mutant: 0, error: 0, timeout: 0, infrastructure_error: 0 };
  for (const execution of items) {
    const resultCode = execution.semantic_result || execution.status;
    const result = statusText(resultCode);
    const evidence = knowledgeByExecution.get(execution.execution_id) || {};
    if (Object.hasOwn(counts, resultCode)) {
      counts[resultCode] += 1;
    }
    const row = document.createElement("tr");
    addIdentityCell(row, execution.mutant_id);
    addStatusCell(row, resultCode);
    addCell(row, execution.killer_test_id, "identity-cell");
    addCell(row, `${Number(execution.selected_test_count || 0)} выбрано`);
    addStatusCell(row, evidence.source_kind);
    addCell(row, execution.duration_seconds === null ? "—" : `${execution.duration_seconds} с`, "numeric-cell");
    addStatusCell(row, evidence.evidence_quality);
    addStatusCell(row, execution.restore_verified ? "verified" : "not_verified");
    body.appendChild(row);
  }
  addMetric(countsContainer, "Убитые мутанты", counts.killed);
  addMetric(countsContainer, "Выжившие мутанты", counts.survived);
  addMetric(countsContainer, "Недействительные мутанты", counts.invalid_mutant);
  addMetric(countsContainer, "Мутанты с тайм-аутом", counts.timeout);
  addMetric(countsContainer, "Ошибки инфраструктуры", counts.infrastructure_error + counts.error);
}
function renderArtifacts(items) {
  const body = byId("artifacts-body");
  clearNode(body);
  setCount("artifacts-count", items.length);
  for (const artifact of items) {
    const row = document.createElement("tr");
    addCell(row, artifact.logical_key);
    addCell(row, statusText(artifact.logical_role));
    addCell(row, artifact.size_bytes);
    addCell(row, artifact.created_at);
    body.appendChild(row);
  }
}
function renderEvents() {
  const list = byId("event-list");
  clearNode(list);
  setCount("events-count", state.events.length);
  for (const event of state.events) {
    const item = document.createElement("li");
    const heading = document.createElement("strong");
    heading.textContent = `${eventText(event.event_type)} — ${text(event.timestamp)}`;
    const details = document.createElement("p");
    details.textContent = Object.entries(event.details || {}).map(([key, value]) => `${key}: ${value}`).join(", ") || "Нет дополнительных сведений";
    item.append(heading, details);
    list.appendChild(item);
  }
}
function startEventPolling() {
  stopEventPolling();
  const generation = state.pollGeneration;
  const poll = async () => {
    if (!state.campaignId || generation !== state.pollGeneration) {
      return;
    }
    const cursorPart = state.eventCursor ? `&cursor=${encodeURIComponent(state.eventCursor)}` : "";
    try {
      const { result } = await requestJson(`/api/campaigns/${encodeURIComponent(state.campaignId)}/events?limit=${EVENT_LIMIT}${cursorPart}`);
      if (result.ok && result.value) {
        const items = Array.isArray(result.value.items) ? result.value.items : [];
        state.events = [...state.events, ...items].slice(-EVENT_HISTORY_LIMIT);
        if (result.value.next_cursor) {
          state.eventCursor = result.value.next_cursor;
          sessionStorage.setItem(`theseus-event-cursor:${state.campaignId}`, state.eventCursor);
        }
        renderEvents();
        await refreshCampaignDetail();
        setMessage(byId("events-state"), items.length ? countText(items.length, "Новое событие", "Новых события", "Новых событий") : "Новых событий нет.");
      } else {
        setMessage(byId("events-state"), apiError(result), "error");
      }
    } catch (_error) {
      setMessage(byId("events-state"), "Поток недоступен: повторяем с последнего курсора", "error");
    }
    if (state.campaignId && generation === state.pollGeneration) {
      state.pollTimer = window.setTimeout(poll, POLL_DELAY_MS);
    }
  };
  state.pollTimer = window.setTimeout(poll, 0);
}
function stopEventPolling() {
  state.pollGeneration += 1;
  if (state.pollTimer !== null) {
    window.clearTimeout(state.pollTimer);
    state.pollTimer = null;
  }
}
function actionStorageKey(action) {
  return `theseus-action:${state.campaignId}:${action}`;
}
function stableActionId(action) {
  const key = actionStorageKey(action);
  let actionId = sessionStorage.getItem(key);
  if (!actionId) {
    if (typeof crypto.randomUUID === "function") {
      actionId = crypto.randomUUID();
    } else {
      const words = new Uint32Array(4);
      crypto.getRandomValues(words);
      actionId = Array.from(words, (value) => value.toString(16).padStart(8, "0")).join("-");
    }
    sessionStorage.setItem(key, actionId);
  }
  return actionId;
}
function stableLaunchActionId(kind) {
  const key = `theseus-launch:${kind}`;
  let actionId = sessionStorage.getItem(key);
  if (!actionId) {
    actionId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : Array.from(crypto.getRandomValues(new Uint32Array(2)), (value) => value.toString(16)).join("-");
    sessionStorage.setItem(key, actionId);
  }
  return actionId;
}
async function performAction(action, button) {
  if (!state.campaignId || state.campaignRevision === null || button.disabled) {
    return;
  }
  button.disabled = true;
  const actionId = stableActionId(action);
  setMessage(byId("action-result"), `${actionText(action)}: выполняется…`);
  try {
    const { response, result } = await requestJson(`/api/campaigns/${encodeURIComponent(state.campaignId)}/actions/${action}`, {
      method: "POST",
      body: { action_id: actionId, expected_revision: state.campaignRevision },
    });
    if (result.ok) {
      const actionStatus = result.value && result.value.status ? String(result.value.status) : "accepted";
      setMessage(byId("action-result"), `${actionText(action)}: ${statusText(actionStatus)}`, actionStatus === "completed" ? "success" : "stale");
      sessionStorage.removeItem(actionStorageKey(action));
      await refreshCampaignDetail();
    } else {
      setMessage(byId("action-result"), apiError(result), "error");
      const errorCode = result.error ? result.error.code : "request_failed";
      if (errorCode !== "action_in_progress") {
        sessionStorage.removeItem(actionStorageKey(action));
      }
      if (response.status === 409 && errorCode === "stale_revision") {
        await refreshCampaignDetail();
      }
    }
  } catch (_error) {
    setMessage(byId("action-result"), `${actionText(action)}: ошибка запроса; будет повторно использован тот же идентификатор действия`, "error");
  } finally {
    button.disabled = false;
  }
}
function recoveryPageConfig(name) {
  const encoded = encodeURIComponent(state.campaignId || "");
  const configs = {
    quarantine: { path: `/api/campaigns/${encoded}/quarantine`, stateId: "quarantine-state", buttonId: "load-more-quarantine", bodyId: "quarantine-body", render: renderQuarantineRows, identity: (item) => item.quarantine_id || item.conflict_id || `${item.reason_code}:${item.recorded_at}` },
    artifacts: { path: `/api/campaigns/${encoded}/artifact-registry`, stateId: "artifact-registry-state", buttonId: "load-more-artifacts", bodyId: "artifact-registry-body", render: renderArtifactRegistryRows, identity: (item) => item.artifact_id || item.logical_key },
    statistics: { path: `/api/campaigns/${encoded}/test-statistics`, stateId: "test-statistics-state", buttonId: "load-more-statistics", bodyId: "test-statistics-body", render: renderTestStatisticsRows, identity: (item) => item.test_id || item.entity_id },
    reuse: { path: `/api/campaigns/${encoded}/reuse-evidence`, stateId: "reuse-evidence-state", buttonId: "load-more-reuse", bodyId: "reuse-evidence-body", render: renderReuseEvidenceRows, identity: (item) => `${item.mutant_id}:${item.source_event_id || item.source_execution_id || item.reuse_kind}` },
  };
  return configs[name];
}
function appendUnique(existing, incoming, identity) {
  const seen = new Set(existing.map(identity));
  const result = [...existing];
  for (const item of incoming) {
    const key = identity(item);
    if (!seen.has(key)) {
      seen.add(key);
      result.push(item);
    }
  }
  return result.slice(-EVENT_HISTORY_LIMIT);
}
async function loadRecoveryPage(name, append = false) {
  if (!state.campaignId) {
    return;
  }
  const config = recoveryPageConfig(name);
  const pageState = state.recoveryPages[name];
  const cursor = append ? pageState.cursor : null;
  const cursorPart = cursor ? `&cursor=${encodeURIComponent(cursor)}` : "";
  setMessage(byId(config.stateId), append ? "Загрузка следующей страницы…" : "Загрузка…");
  try {
    const { result } = await requestJson(`${config.path}?limit=${PAGE_LIMIT}${cursorPart}`);
    if (!result.ok) {
      setMessage(byId(config.stateId), apiError(result), result.kind === "failed" ? "error" : "stale");
      return;
    }
    const page = pageValue(result) || result.value || {};
    const items = Array.isArray(page.items) ? page.items : [];
    pageState.items = append ? appendUnique(pageState.items, items, config.identity) : items.slice(-EVENT_HISTORY_LIMIT);
    pageState.cursor = page.next_cursor || null;
    config.render(pageState.items);
    const truncated = Boolean(page.truncated || result.value.truncated);
    const button = byId(config.buttonId);
    button.hidden = !(pageState.cursor || truncated);
    button.disabled = truncated && !pageState.cursor;
    button.textContent = pageState.cursor ? "Загрузить следующую страницу" : "Продолжение недоступно";
    setMessage(byId(config.stateId), pageState.items.length ? `${countText(pageState.items.length, "Загружена", "Загружены", "Загружено")} ${truncated ? "(раздел усечён)" : ""}` : "Записей нет.");
  } catch (_error) {
    setMessage(byId(config.stateId), "Локальный интерфейс недоступен: страница не загружена; курсор сохранён", "error");
  }
}
async function loadRecoveryScreen() {
  if (!state.campaignId) {
    return;
  }
  resetRecoveryPages();
  setMessage(byId("recovery-state"), "Загрузка диагностики восстановления…");
  const encoded = encodeURIComponent(state.campaignId);
  try {
    const [diagnostics] = await Promise.all([
      requestJson(`/api/campaigns/${encoded}/recovery-diagnostics?limit=${PAGE_LIMIT}`),
      loadRecoveryPage("quarantine"),
      loadRecoveryPage("artifacts"),
      loadRecoveryPage("statistics"),
      loadRecoveryPage("reuse"),
    ]);
    renderRecoveryDiagnostics(diagnostics.result);
    resumeStoredRecoveryActionPolling();
  } catch (_error) {
    setMessage(byId("recovery-state"), "Локальный интерфейс недоступен: диагностика восстановления не загружена", "error");
  }
}
function renderRecoveryDiagnostics(result) {
  if (!result.ok) {
    setMessage(byId("recovery-state"), apiError(result), result.kind === "failed" ? "error" : "stale");
    renderRecoverySummary({});
    renderRecoveryLeases([]);
    renderRecoveryWorkers([]);
    renderSpool({});
    return;
  }
  const value = result.value || {};
  renderRecoverySummary(value);
  renderRecoveryLeases(value.leases && Array.isArray(value.leases.items) ? value.leases.items : []);
  renderRecoveryWorkers(value.workers && Array.isArray(value.workers.items) ? value.workers.items : []);
  renderSpool(value.spool || {});
  const truncated = Boolean(value.truncated || (value.leases && value.leases.truncated) || (value.workers && value.workers.truncated) || (value.spool && value.spool.truncated));
  setMessage(byId("recovery-state"), truncated ? "Диагностика восстановления загружена; один или несколько разделов усечены." : "Диагностика восстановления загружена.", "success");
}
function renderRecoverySummary(value) {
  const container = byId("recovery-summary");
  clearNode(container);
  const progress = value.progress || value.campaign || {};
  const recovery = value.recovery || value;
  const health = value.health || {};
  addMetric(container, "Статус кампании", statusText(progress.campaign_status || progress.status));
  addMetric(container, "Наблюдаемая версия", progress.campaign_revision ?? progress.revision_number);
  addMetric(container, "Состояние восстановления", statusText(recovery.recovery_state || recovery.state || health.recovery));
  addMetric(container, "Ожидающие доставки", recovery.pending_deliveries ?? (value.spool && value.spool.pending_count));
  addMetric(container, "Ожидающая финализация", booleanText(recovery.pending_finalization));
  addMetric(container, "Последняя активность восстановления", recovery.last_activity || recovery.last_recovery_activity);
  addMetric(container, "Усечено", booleanText(Boolean(value.truncated)));
  const blockers = Array.isArray(value.recovery_blockers) ? value.recovery_blockers : Array.isArray(value.blockers) ? value.blockers : Array.isArray(health.blockers) ? health.blockers : [];
  const list = byId("recovery-blockers");
  clearNode(list);
  if (blockers.length === 0) {
    const item = document.createElement("li");
    item.textContent = "Блокирующих причин восстановления нет.";
    list.appendChild(item);
  } else {
    for (const blocker of blockers.slice(0, PAGE_LIMIT)) {
      const item = document.createElement("li");
      item.textContent = `${text(blocker.code, "blocker")}: ${text(blocker.message, "Восстановление заблокировано")}`;
      list.appendChild(item);
    }
  }
}
function renderRecoveryLeases(items) {
  const body = byId("leases-body");
  clearNode(body);
  for (const lease of items) {
    const row = document.createElement("tr");
    addCell(row, lease.shard_id);
    addCell(row, lease.worker_id);
    addCell(row, lease.attempt);
    addCell(row, statusText(lease.status || lease.lease_status));
    addCell(row, lease.heartbeat_age_seconds);
    addCell(row, statusText(lease.expiration_state));
    addCell(row, statusText(lease.hung_state, booleanText(lease.stalled_or_hung ?? lease.hung)));
    addCell(row, lease.observed_revision ?? lease.revision_number);
    body.appendChild(row);
  }
  setMessage(byId("leases-state"), items.length ? `${countText(items.length, "Загружена", "Загружены", "Загружено")} подтверждённых строк аренды.` : "Данные об арендах отсутствуют.");
}
function renderRecoveryWorkers(items) {
  const body = byId("recovery-workers-body");
  clearNode(body);
  for (const worker of items) {
    const row = document.createElement("tr");
    addCell(row, worker.worker_id || worker.identity);
    addCell(row, statusText(worker.lifecycle_status || worker.status));
    addCell(row, worker.current_shard_id);
    addCell(row, worker.heartbeat_at || worker.last_heartbeat_at);
    addCell(row, booleanText(worker.orphaned));
    addCell(row, statusText(worker.process_evidence_status));
    body.appendChild(row);
  }
  setMessage(byId("recovery-workers-state"), items.length ? `${countText(items.length, "Загружена", "Загружены", "Загружено")} строк рабочего процесса восстановления.` : "Данные о рабочих процессах восстановления отсутствуют.");
}
function renderSpool(spool) {
  const summary = byId("spool-summary");
  clearNode(summary);
  addMetric(summary, "Ожидает", spool.pending_count);
  addMetric(summary, "Подтверждено", spool.acknowledged_count);
  addMetric(summary, "В карантине", spool.quarantined_count);
  addMetric(summary, "Самая старая ожидающая доставка", spool.oldest_pending_at || spool.oldest_pending_timestamp);
  const deliveries = spool.deliveries && Array.isArray(spool.deliveries.items) ? spool.deliveries.items : Array.isArray(spool.items) ? spool.items : [];
  const body = byId("spool-body");
  clearNode(body);
  for (const delivery of deliveries.slice(0, PAGE_LIMIT)) {
    const row = document.createElement("tr");
    addCell(row, delivery.delivery_id || delivery.identity);
    addCell(row, delivery.campaign_id);
    addCell(row, delivery.shard_id);
    addCell(row, delivery.execution_id);
    addCell(row, delivery.attempt);
    addCell(row, statusText(delivery.status));
    addCell(row, delivery.recorded_at || delivery.created_at);
    body.appendChild(row);
  }
  setMessage(byId("spool-state"), spool.truncated ? "Идентификаторы доставок в очереди усечены." : deliveries.length ? `${countText(deliveries.length, "Загружена", "Загружены", "Загружено")} строк доставки.` : "Идентификаторы доставок в очереди отсутствуют.");
}
function renderQuarantineRows(items) {
  const body = byId("quarantine-body");
  clearNode(body);
  for (const item of items) {
    const row = document.createElement("tr");
    addCell(row, humanize(item.reason_code || item.reason));
    addCell(row, item.campaign_id);
    addCell(row, item.shard_id);
    addCell(row, item.execution_id || item.identity_key);
    addCell(row, item.attempt);
    addCell(row, item.recorded_at || item.created_at);
    addCell(row, humanize(item.evidence_summary || item.conflict_type));
    body.appendChild(row);
  }
}
function renderArtifactRegistryRows(items) {
  const body = byId("artifact-registry-body");
  clearNode(body);
  for (const item of items) {
    const row = document.createElement("tr");
    addCell(row, item.logical_key);
    addCell(row, item.artifact_id);
    addCell(row, item.content_sha256);
    addCell(row, item.size_bytes);
    addCell(row, statusText(item.registration_status || item.status));
    addCell(row, statusText(item.finalization_status));
    body.appendChild(row);
  }
}
function renderTestStatisticsRows(items) {
  const body = byId("test-statistics-body");
  clearNode(body);
  for (const item of items) {
    const row = document.createElement("tr");
    addCell(row, item.test_id || item.entity_id);
    addCell(row, item.duration_avg_ms ?? item.duration_ms);
    addCell(row, item.passed_count);
    addCell(row, item.failed_count);
    addCell(row, item.error_count);
    addCell(row, item.timeout_count);
    addCell(row, item.retry_count);
    addCell(row, item.recovery_count);
    addCell(row, item.flaky_transition_count);
    body.appendChild(row);
  }
}
function renderReuseEvidenceRows(items) {
  const body = byId("reuse-evidence-body");
  clearNode(body);
  for (const item of items) {
    const row = document.createElement("tr");
    addCell(row, item.mutant_id);
    addCell(row, statusText(item.reuse_kind || item.kind));
    addCell(row, booleanText(item.authorized));
    addCell(row, booleanText(item.audit_required));
    addCell(row, statusText(item.evidence_quality));
    addCell(row, boundedJoin(item.blockers));
    addCell(row, item.source_execution_id);
    addCell(row, item.source_event_id);
    body.appendChild(row);
  }
}
function renderRecoveryActionReceipt(receipt) {
  const container = byId("recovery-action-receipt");
  clearNode(container);
  addDefinition(container, "Идентификатор действия", receipt.action_id);
  addDefinition(container, "Действие", actionText(receipt.action || receipt.action_type));
  addDefinition(container, "Статус", statusText(receipt.status));
  addDefinition(container, "Версия кампании", receipt.campaign_revision ?? receipt.observed_revision);
  addDefinition(container, "Идентификатор эффекта", receipt.effect_id);
  addDefinition(container, "Код ошибки", receipt.error_code);
}
function recoveryActionStatus(result) {
  if (!result || !result.ok || !result.value) {
    return null;
  }
  return String(result.value.status || "").toLowerCase();
}
function stopRecoveryActionPolling() {
  state.recoveryPollGeneration += 1;
  if (state.recoveryPollTimer !== null) {
    window.clearTimeout(state.recoveryPollTimer);
    state.recoveryPollTimer = null;
  }
}
function pollRecoveryAction(action, actionId) {
  stopRecoveryActionPolling();
  const campaignId = state.campaignId;
  const generation = state.recoveryPollGeneration;
  const poll = async () => {
    if (!campaignId || campaignId !== state.campaignId || generation !== state.recoveryPollGeneration) {
      return;
    }
    try {
      const { result } = await requestJson(`/api/campaigns/${encodeURIComponent(campaignId)}/recovery-actions/${encodeURIComponent(actionId)}?limit=1`);
      if (!result.ok) {
        setMessage(byId("recovery-action-result"), apiError(result), "error");
        if (result.error && result.error.code === "action_not_found") {
          sessionStorage.removeItem(actionStorageKey(action));
          return;
        }
      } else {
        renderRecoveryActionReceipt(result.value || {});
        const status = recoveryActionStatus(result);
        setMessage(byId("recovery-action-result"), `${actionText(action)}: ${statusText(status, "Выполняется")}`, TERMINAL_ACTION_STATUSES.has(status) ? (status === "completed" ? "success" : "error") : "");
        if (TERMINAL_ACTION_STATUSES.has(status)) {
          sessionStorage.removeItem(actionStorageKey(action));
          state.recoveryActionBusy = false;
          await refreshCampaignDetail();
          await loadRecoveryScreen();
          return;
        }
      }
    } catch (_error) {
      setMessage(byId("recovery-action-result"), `${actionText(action)}: статус недоступен; повторяем запрос с тем же идентификатором действия`, "error");
    }
    if (campaignId === state.campaignId && generation === state.recoveryPollGeneration) {
      state.recoveryPollTimer = window.setTimeout(poll, RECOVERY_ACTION_POLL_DELAY_MS);
    }
  };
  state.recoveryPollTimer = window.setTimeout(poll, RECOVERY_ACTION_POLL_DELAY_MS);
}
function resumeStoredRecoveryActionPolling() {
  for (const action of ["reconcile", "recover"]) {
    const actionId = sessionStorage.getItem(actionStorageKey(action));
    if (actionId) {
      pollRecoveryAction(action, actionId);
      return;
    }
  }
}
async function performRecoveryAction(action, button) {
  if (!state.campaignId || state.campaignRevision === null || button.disabled || state.recoveryActionBusy) {
    return;
  }
  const confirmation = action === "reconcile" ? "Сверить кампанию по подтверждённым данным восстановления?" : "Восстановить или продолжить кампанию из сохранённого состояния?";
  if (!window.confirm(confirmation)) {
    setMessage(byId("recovery-action-result"), `${actionText(action)}: отменено оператором`);
    return;
  }
  state.recoveryActionBusy = true;
  button.disabled = true;
  const actionId = stableActionId(action);
  setMessage(byId("recovery-action-result"), `${actionText(action)}: выполняется…`);
  try {
    const { response, result } = await requestJson(`/api/campaigns/${encodeURIComponent(state.campaignId)}/actions/${action}`, {
      method: "POST",
      body: { action_id: actionId, expected_revision: state.campaignRevision },
    });
    if (result.ok) {
      renderRecoveryActionReceipt(result.value || {});
      const status = recoveryActionStatus(result);
      if (TERMINAL_ACTION_STATUSES.has(status)) {
        sessionStorage.removeItem(actionStorageKey(action));
        state.recoveryActionBusy = false;
        setMessage(byId("recovery-action-result"), `${actionText(action)}: ${statusText(status)}`, status === "completed" ? "success" : "error");
        await refreshCampaignDetail();
        await loadRecoveryScreen();
      } else {
        setMessage(byId("recovery-action-result"), `${actionText(action)}: ${statusText(status, "Выполняется")}`);
        pollRecoveryAction(action, actionId);
      }
    } else {
      setMessage(byId("recovery-action-result"), apiError(result), "error");
      const errorCode = result.error ? result.error.code : "request_failed";
      if (errorCode !== "action_in_progress") {
        sessionStorage.removeItem(actionStorageKey(action));
        state.recoveryActionBusy = false;
      } else {
        pollRecoveryAction(action, actionId);
      }
      if (response.status === 409 && errorCode === "stale_revision") {
        state.recoveryActionBusy = false;
        await refreshCampaignDetail();
        await loadRecoveryScreen();
      }
    }
  } catch (_error) {
    // On a transient request failure, the same action ID will be reused.
    state.recoveryActionBusy = false;
    setMessage(byId("recovery-action-result"), `${actionText(action)}: ошибка запроса; будет повторно использован тот же идентификатор действия`, "error");
  } finally {
    button.disabled = false;
  }
}
async function submitRegisterProject(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const submit = form.querySelector("button[type=\"submit\"]");
  if (submit.disabled) {
    return;
  }
  submit.disabled = true;
  const data = new FormData(form);
  setMessage(byId("register-project-result"), "Добавление проекта…");
  try {
    const { result } = await requestJson("/api/projects", {
      method: "POST",
      body: { project_root: data.get("project_root"), display_name: data.get("display_name") },
    });
    if (result.ok) {
      const discovery = result.value && result.value.discovery;
      const sourceRoots = discovery && discovery.source_roots && discovery.source_roots.length ? discovery.source_roots.join(", ") : "не определены";
      const testRoots = discovery && discovery.test_roots && discovery.test_roots.length ? discovery.test_roots.join(", ") : "не определены";
      const interpreter = discovery && discovery.python_interpreter ? discovery.python_interpreter : "не определён";
      setMessage(byId("register-project-result"), `Проект добавлен · Python: ${interpreter} · исходники: ${sourceRoots} · тесты: ${testRoots}`, "success");
      form.reset();
      await loadOverview();
    } else {
      setMessage(byId("register-project-result"), apiError(result), "error");
    }
  } catch (_error) {
    setMessage(byId("register-project-result"), "Не удалось добавить проект: запрос не выполнен", "error");
  } finally {
    submit.disabled = false;
  }
}
function optionalNumber(data, name) {
  const value = data.get(name);
  return value === null || value === "" ? null : Number(value);
}
async function submitProjectRun(event) {
  event.preventDefault();
  const submit = byId("project-run-button");
  if (!state.selectedProjectId || submit.disabled) {
    setMessage(byId("project-run-result"), "Сначала выберите зарегистрированный проект.", "error");
    return;
  }
  submit.disabled = true;
  setMessage(byId("project-run-result"), "Theseus разбивает проект на production-файлы и запускает кампании…");
  try {
    const { result } = await requestJson("/api/project-runs", { method: "POST", body: { project_id: state.selectedProjectId } });
    if (result.ok) {
      const value = result.value || {};
      const status = String(value.status || "queued").toLowerCase();
      const prefix = status === "queued" ? "Запуск поставлен в очередь" : "Запуск проекта создан";
      setMessage(byId("project-run-result"), `${prefix}: ${countText(value.source_count || 0, "файл", "файла", "файлов")}`, "success");
      await loadOverview();
    } else {
      const error = result.error || {};
      const details = error.details || {};
      setMessage(byId("project-run-result"), apiError(result), "error");
      if (error.code === "project_run_already_active" && details.run_id) {
        await openProjectRun(details.run_id);
      }
    }
  } catch (_error) {
    setMessage(byId("project-run-result"), "Не удалось запустить проект: запрос не выполнен", "error");
  } finally {
    applyProjectProfile(state.selectedProjectId);
  }
}
async function submitCreateCampaign(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const submit = form.querySelector('button[type="submit"]');
  if (submit.disabled) {
    return;
  }
  submit.disabled = true;
  const data = new FormData(form);
  const payload = {
    project_id: data.get("project_id"),
    source_path: data.get("source_path"),
    test_command: data.get("test_command"),
    function: data.get("function"),
    scope_kind: data.get("scope_kind"),
    operators: String(data.get("operators") || "").split(",").map((item) => item.trim()).filter(Boolean),
    max_mutants: optionalNumber(data, "max_mutants"),
    max_workers: optionalNumber(data, "max_workers"),
    max_seconds: optionalNumber(data, "max_seconds"),
    max_test_seconds: optionalNumber(data, "max_test_seconds"),
    no_escalation: data.get("no_escalation") === "on" ? true : null,
    reuse_mode: data.get("reuse_mode"),
    create_action_id: stableLaunchActionId("create"),
    start_action_id: stableLaunchActionId("start"),
  };
  setMessage(byId("create-result"), "Создание кампании…");
  try {
    const { result } = await requestJson("/api/campaigns", { method: "POST", body: payload });
    if (result.ok) {
      sessionStorage.removeItem("theseus-launch:create");
      sessionStorage.removeItem("theseus-launch:start");
      setMessage(byId("create-result"), "Кампания создана и запущена", "success");
      if (result.value && result.value.campaign_id) {
        await openCampaign(result.value.campaign_id);
      } else {
        await loadOverview();
      }
    } else {
      setMessage(byId("create-result"), apiError(result), "error");
    }
  } catch (_error) {
    setMessage(byId("create-result"), "Не удалось запустить кампанию: запрос не выполнен", "error");
  } finally {
    submit.disabled = false;
  }
}
function wireEvents() {
  byId("refresh-overview").addEventListener("click", loadOverview);
  byId("back-to-overview").addEventListener("click", loadOverview);
  byId("refresh-detail").addEventListener("click", () => state.activeTab === "recovery" ? loadRecoveryScreen() : refreshCampaignDetail());
  byId("refresh-recovery").addEventListener("click", loadRecoveryScreen);
  byId("campaign-tab").addEventListener("click", () => switchDetailTab("campaign"));
  byId("recovery-tab").addEventListener("click", () => switchDetailTab("recovery"));
  byId("register-project-form").addEventListener("submit", submitRegisterProject);
  byId("create-project-select").addEventListener("change", (event) => selectProject(event.target.value));
  byId("project-run-form").addEventListener("submit", submitProjectRun);
  byId("refresh-project-run-detail").addEventListener("click", refreshProjectRunDetail);
  byId("close-project-run-detail").addEventListener("click", closeProjectRunDetail);
  byId("create-campaign-form").addEventListener("submit", submitCreateCampaign);
  for (const button of document.querySelectorAll("[data-action]")) {
    button.addEventListener("click", () => performAction(button.dataset.action, button));
  }
  for (const button of document.querySelectorAll("[data-recovery-action]")) {
    button.addEventListener("click", () => performRecoveryAction(button.dataset.recoveryAction, button));
  }
  byId("load-more-quarantine").addEventListener("click", () => loadRecoveryPage("quarantine", true));
  byId("load-more-artifacts").addEventListener("click", () => loadRecoveryPage("artifacts", true));
  byId("load-more-statistics").addEventListener("click", () => loadRecoveryPage("statistics", true));
  byId("load-more-reuse").addEventListener("click", () => loadRecoveryPage("reuse", true));
  window.addEventListener("pagehide", () => {
    stopEventPolling();
    stopRecoveryActionPolling();
  });
}
async function initialize() {
  wireEvents();
  try {
    await loadSession();
    await loadOverview();
  } catch (_error) {
    setMessage(byId("connection-status"), "Сессия недоступна: не удалось установить локальную сессию", "error");
  }
}
document.addEventListener("DOMContentLoaded", initialize);
