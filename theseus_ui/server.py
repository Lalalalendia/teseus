"""Loopback-only dependency-free HTTP server for the campaign UI."""
from __future__ import annotations

import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from threading import Thread
from typing import Any, Final
from urllib.parse import parse_qs, urlsplit

from .app import CampaignUiApplication, UiResponse
from .serialization import JsonValue, canonical_json_bytes

LOOPBACK_HOST: Final = "127.0.0.1"
UI_MAX_REQUEST_BODY: Final = 64 * 1024
UI_CSP: Final = (
    "default-src 'self'; "
    "base-uri 'none'; "
    "connect-src 'self'; "
    "font-src 'none'; "
    "form-action 'self'; "
    "frame-ancestors 'none'; "
    "img-src 'self' data:; "
    "manifest-src 'none'; "
    "media-src 'none'; "
    "object-src 'none'; "
    "script-src 'self'; "
    "style-src 'self'; "
    "worker-src 'none'"
)
_STATIC_ASSETS: Final = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/assets/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/assets/styles.css": ("styles.css", "text/css; charset=utf-8"),
}
_STATE_CHANGING_PREFIXES: Final = ("/api/campaigns", "/api/projects", "/api/project-runs")


def asset_bytes(name: str) -> bytes:
    # Load one allowlisted packaged asset independently from the current working directory.
    if name not in {item[0] for item in _STATIC_ASSETS.values()}:
        raise FileNotFoundError(name)
    return files("theseus_ui").joinpath("assets", name).read_bytes()


def _local_host(value: str, port: int) -> bool:
    # Accept only the exact loopback host names for the current local listener.
    host = value.strip().lower()
    return host in {
        f"127.0.0.1:{port}",
        f"localhost:{port}",
        "127.0.0.1" if port == 80 else "",
        "localhost" if port == 80 else "",
    }


def _local_origin(value: str, port: int) -> bool:
    # Accept only same-loopback HTTP origins for state-changing requests.
    try:
        parsed = urlsplit(value)
        effective_port = parsed.port or 80
    except ValueError:
        return False
    if parsed.scheme != "http" or parsed.username or parsed.password:
        return False
    return parsed.hostname in {"127.0.0.1", "localhost"} and effective_port == port


@dataclass(frozen=True, slots=True)
class RunningUiServer:
    """Background server handle used by the CLI and isolated tests."""

    server: ThreadingHTTPServer
    thread: Thread
    session_token: str

    @property
    def port(self) -> int:
        # Return the operating-system-selected loopback port.
        return int(self.server.server_address[1])

    @property
    def url(self) -> str:
        # Return the non-sensitive local browser URL.
        return f"http://{LOOPBACK_HOST}:{self.port}/"

    def close(self) -> None:
        # Stop the listener and join its dedicated serving thread.
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class CampaignUiHttpServer(ThreadingHTTPServer):
    """Threaded loopback server carrying one application and session token."""

    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        port: int,
        application: CampaignUiApplication,
        *,
        session_token: str | None = None,
    ) -> None:
        # Bind exclusively to IPv4 loopback and retain one in-memory session token.
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        self.application = application
        self.session_token = session_token or secrets.token_urlsafe(32)
        super().__init__((LOOPBACK_HOST, port), CampaignUiRequestHandler)


class CampaignUiRequestHandler(BaseHTTPRequestHandler):
    """Exact-route handler with JSON, origin and session fencing."""

    server: CampaignUiHttpServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress request logs so query values and action identifiers are not emitted.
        return None

    def do_GET(self) -> None:
        # Serve one allowlisted asset or bounded read-only API request.
        self._dispatch("GET")

    def do_POST(self) -> None:
        # Serve one session-fenced JSON state-changing request.
        self._dispatch("POST")

    def do_HEAD(self) -> None:
        # Serve only headers for allowlisted static assets.
        parsed = urlsplit(self.path)
        if not self._host_allowed() or parsed.path not in _STATIC_ASSETS:
            self._send_error(404, "not_found", "resource does not exist")
            return
        name, content_type = _STATIC_ASSETS[parsed.path]
        content = asset_bytes(name)
        self._send_bytes(200, content_type, content, head_only=True)

    def _dispatch(self, method: str) -> None:
        # Validate local transport constraints before routing one request.
        parsed = urlsplit(self.path)
        if not self._host_allowed():
            self._send_error(403, "invalid_host", "request host is not allowed")
            return
        origin = self.headers.get("Origin")
        if origin and not _local_origin(origin, int(self.server.server_address[1])):
            self._send_error(403, "invalid_origin", "request origin is not allowed")
            return
        if method == "GET" and parsed.path in _STATIC_ASSETS:
            name, content_type = _STATIC_ASSETS[parsed.path]
            self._send_bytes(200, content_type, asset_bytes(name))
            return
        if method == "GET" and parsed.path == "/api/session":
            self._send_json(
                200,
                {"ok": True, "kind": "success", "value": {"session_token": self.server.session_token}},
                no_store=True,
                sanitize=False,
            )
            return
        if not parsed.path.startswith("/api/"):
            self._send_error(404, "not_found", "resource does not exist")
            return
        if method == "POST" and not self._state_request_allowed():
            return
        payload: Mapping[str, JsonValue] | None = None
        if method == "POST":
            payload = self._read_json_body()
            if payload is None:
                return
        try:
            query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=32)
        except ValueError:
            self._send_error(400, "invalid_query", "query string exceeds the UI limit")
            return
        response = self.server.application.handle(
            method,
            parsed.path,
            query,
            payload,
        )
        self._send_ui_response(response)

    def _host_allowed(self) -> bool:
        # Require the Host header to resolve to the active loopback listener.
        return _local_host(self.headers.get("Host", ""), int(self.server.server_address[1]))

    def _state_request_allowed(self) -> bool:
        # Require JSON, same-origin and the exact in-memory session token for mutations.
        parsed = urlsplit(self.path)
        if not parsed.path.startswith(_STATE_CHANGING_PREFIXES):
            self._send_error(404, "not_found", "resource does not exist")
            return False
        origin = self.headers.get("Origin", "")
        if not origin or not _local_origin(origin, int(self.server.server_address[1])):
            self._send_error(403, "invalid_origin", "request origin is not allowed")
            return False
        supplied = self.headers.get("X-Theseus-Session", "")
        if not secrets.compare_digest(supplied, self.server.session_token):
            self._send_error(403, "invalid_session", "session token is invalid")
            return False
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            self._send_error(415, "json_required", "API requests must use application/json")
            return False
        return True

    def _read_json_body(self) -> Mapping[str, JsonValue] | None:
        # Read one bounded JSON object and reject partial, oversized or malformed bodies.
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            self._send_error(411, "content_length_required", "Content-Length is required")
            return None
        try:
            length = int(raw_length)
        except ValueError:
            self._send_error(400, "invalid_content_length", "Content-Length is invalid")
            return None
        if length < 0 or length > UI_MAX_REQUEST_BODY:
            self._send_error(413, "request_too_large", "request body exceeds the UI limit")
            return None
        content = self.rfile.read(length)
        if len(content) != length:
            self._send_error(400, "incomplete_body", "request body is incomplete")
            return None
        try:
            value = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            self._send_error(400, "invalid_json", "request body is not valid UTF-8 JSON")
            return None
        if not isinstance(value, Mapping):
            self._send_error(400, "invalid_json", "request body must be a JSON object")
            return None
        return value

    def _send_ui_response(self, response: UiResponse) -> None:
        # Serialize one application response through the shared privacy filter.
        self._send_json(response.status, response.body, no_store=True)

    def _send_error(self, status: int, code: str, message: str) -> None:
        # Return one safe typed transport error without stack traces.
        self._send_json(
            status,
            {
                "ok": False,
                "kind": "rejected",
                "error": {
                    "code": code,
                    "message": message,
                    "retriable": False,
                    "details": {},
                },
            },
            no_store=True,
        )

    def _send_json(
        self,
        status: int,
        value: object,
        *,
        no_store: bool,
        sanitize: bool = True,
    ) -> None:
        # Send one bounded JSON response with security headers.
        content = canonical_json_bytes(value, hide_private_paths=sanitize)
        self._send_bytes(
            status,
            "application/json; charset=utf-8",
            content,
            no_store=no_store,
        )

    def _send_bytes(
        self,
        status: int,
        content_type: str,
        content: bytes,
        *,
        head_only: bool = False,
        no_store: bool = False,
    ) -> None:
        # Send one exact response body with CSP and anti-sniffing headers.
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Content-Security-Policy", UI_CSP)
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cache-Control", "no-store" if no_store else "public, max-age=300")
        self.end_headers()
        if not head_only:
            self.wfile.write(content)


def start_ui_server(
    application: CampaignUiApplication,
    *,
    port: int = 0,
    session_token: str | None = None,
) -> RunningUiServer:
    # Start one daemon serving thread on IPv4 loopback only.
    server = CampaignUiHttpServer(port, application, session_token=session_token)
    thread = Thread(target=server.serve_forever, name="theseus-ui", daemon=True)
    thread.start()
    return RunningUiServer(server, thread, server.session_token)
