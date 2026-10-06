"""HTTP 路由与统一错误输出。"""
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .domain import Actor, DomainError, PermissionDenied, ValidationError


RECORD_RE = re.compile(r"^/api/records/(\d+)$")
ACTION_RE = re.compile(r"^/api/records/(\d+)/actions/([a-z_]+)$")
AUDIT_RE = re.compile(r"^/api/records/(\d+)/audit$")
BATCH_RE = re.compile(r"^/api/batches/([A-Za-z0-9._:-]+)$")
BATCH_RESUME_RE = re.compile(r"^/api/batches/([A-Za-z0-9._:-]+)/resume$")
CONFLICT_RESOLVE_RE = re.compile(r"^/api/conflicts/(\d+)/resolve$")


def make_handler(service: Any, static_dir: Path, logistics: Any = None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "subsea-cable-repair/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _actor(self) -> Actor:
            user_id = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            if not user_id or not role:
                raise PermissionDenied("缺少X-User-Id或X-Role")
            return Actor(user_id=user_id, role=role, organization=self.headers.get("X-Org", ""))

        def _body(self) -> Dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValidationError("Content-Length无效") from exc
            if length > 1024 * 1024:
                raise ValidationError("请求体过大")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError("请求体必须是JSON") from exc
            if not isinstance(data, dict):
                raise ValidationError("JSON顶层必须是对象")
            return data

        def _send(self, status: int, payload: Any, content_type: str = "application/json; charset=utf-8") -> None:
            if content_type.startswith("application/json"):
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            else:
                body = payload
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle_error(self, exc: Exception) -> None:
            if isinstance(exc, DomainError):
                self._send(exc.status, {"error": exc.code, "message": str(exc)})
            else:
                self._send(500, {"error": "internal_error", "message": "服务内部错误"})

        def do_GET(self) -> None:
            try:
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._send(200, {"status": "ok", "service": "subsea-cable-repair", "database": service.repository.health()})
                    return
                if parsed.path == "/":
                    page = (static_dir / "index.html").read_bytes()
                    self._send(200, page, "text/html; charset=utf-8")
                    return
                if parsed.path == "/api/records":
                    query = parse_qs(parsed.query)
                    records = service.list_records(self._actor(), state=query.get("state", [None])[0], limit=int(query.get("limit", ["100"])[0]))
                    self._send(200, {"items": records})
                    return
                match = RECORD_RE.match(parsed.path)
                if match:
                    self._send(200, service.get_record(self._actor(), int(match.group(1))))
                    return
                match = AUDIT_RE.match(parsed.path)
                if match:
                    self._send(200, {"items": service.timeline(self._actor(), int(match.group(1)))})
                    return
                if parsed.path == "/api/stats":
                    self._send(200, service.stats(self._actor()))
                    return
                if logistics is not None and parsed.path == "/api/batches":
                    query = parse_qs(parsed.query)
                    self._send(200, {"items": logistics.list_batches(self._actor(), state=query.get("state", [None])[0], limit=int(query.get("limit", ["100"])[0]))})
                    return
                if logistics is not None:
                    match = BATCH_RE.match(parsed.path)
                    if match:
                        self._send(200, logistics.get_batch(self._actor(), match.group(1)))
                        return
                if logistics is not None and parsed.path == "/api/inventory/availability":
                    query = parse_qs(parsed.query)
                    self._send(200, logistics.availability(self._actor(), query.get("warehouse", [""])[0], query.get("cable_type", [""])[0]))
                    return
                if logistics is not None and parsed.path == "/api/inventory/moves":
                    query = parse_qs(parsed.query)
                    self._send(200, {"items": logistics.list_moves(self._actor(), warehouse=query.get("warehouse", [None])[0], cable_type=query.get("cable_type", [None])[0])})
                    return
                if logistics is not None and parsed.path == "/api/outbound":
                    self._send(200, {"items": logistics.list_outbound(self._actor())})
                    return
                if logistics is not None and parsed.path == "/api/settlements":
                    query = parse_qs(parsed.query)
                    self._send(200, {"items": logistics.list_settlements(self._actor(), order_key=query.get("order_key", [None])[0])})
                    return
                if logistics is not None and parsed.path == "/api/reconcile":
                    query = parse_qs(parsed.query)
                    self._send(200, logistics.reconcile(self._actor(), warehouse=query.get("warehouse", [None])[0]))
                    return
                if logistics is not None and parsed.path == "/api/conflicts":
                    query = parse_qs(parsed.query)
                    self._send(200, {"items": logistics.list_conflicts(self._actor(), state=query.get("state", [None])[0])})
                    return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def do_POST(self) -> None:
            try:
                parsed = urlparse(self.path)
                body = self._body()
                if parsed.path == "/api/records":
                    record = service.create(self._actor(), body.get("reference", ""), body.get("data", {}))
                    self._send(201, record)
                    return
                match = ACTION_RE.match(parsed.path)
                if match:
                    version = body.get("expected_version")
                    if not isinstance(version, int):
                        raise ValidationError("expected_version必须是整数")
                    record = service.act(self._actor(), int(match.group(1)), version, match.group(2), body.get("data", {}))
                    self._send(200, record)
                    return
                if logistics is not None and parsed.path == "/api/batches":
                    view, status = logistics.submit_batch(self._actor(), body)
                    self._send(status, view)
                    return
                if logistics is not None:
                    match = BATCH_RESUME_RE.match(parsed.path)
                    if match:
                        self._send(200, logistics.resume_batch(self._actor(), match.group(1)))
                        return
                if logistics is not None and parsed.path == "/api/inventory/receipt":
                    self._send(200, logistics.receipt(self._actor(), body))
                    return
                if logistics is not None and parsed.path == "/api/inventory/in-transit":
                    self._send(200, logistics.update_in_transit(self._actor(), body))
                    return
                if logistics is not None and parsed.path == "/api/outbound":
                    result = logistics.issue_outbound(self._actor(), body)
                    self._send(200 if result.get("duplicated") else 201, result)
                    return
                if logistics is not None and parsed.path == "/api/settlements":
                    result = logistics.record_settlement(self._actor(), body)
                    self._send(200 if result.get("duplicated") else 201, result)
                    return
                if logistics is not None:
                    match = CONFLICT_RESOLVE_RE.match(parsed.path)
                    if match:
                        self._send(200, logistics.resolve_conflict(self._actor(), int(match.group(1)), body))
                        return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

    return Handler


def create_server(host: str, port: int, service: Any, static_dir: Path, logistics: Any = None) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, static_dir, logistics))
