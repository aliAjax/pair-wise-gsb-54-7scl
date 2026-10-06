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
BATCH_REF_RE = re.compile(r"^/api/sync/batches/([^/]+)$")
BATCH_RESUME_RE = re.compile(r"^/api/sync/batches/([^/]+)/resume$")
CONFLICT_ID_RE = re.compile(r"^/api/sync/conflicts/(\d+)/resolve$")
DEMAND_REF_RE = re.compile(r"^/api/sync/demands/([^/]+)$")
DEMAND_PROMOTE_RE = re.compile(r"^/api/sync/demands/([^/]+)/promote$")
ORDER_REF_RE = re.compile(r"^/api/sync/outbound/([^/]+)/(ship|cancel)$")


_NO_MATCH = object()


def make_handler(service: Any, static_dir: Path, sync_service: Any = None):
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
                    self._send(200, {"status": "ok", "service": "subsea-cable-repair",
                                     "database": service.repository.health(),
                                     "sync_database": sync_service.health() if sync_service else False})
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
                if sync_service is not None:
                    response = self._sync_get(parsed)
                    if response is not _NO_MATCH:
                        self._send(200, response)
                        return
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

        def _sync_get(self, parsed) -> Any:
            """返回匹配的响应对象，未匹配返回哨兵。同步子系统同样需要调用身份头。"""
            path = parsed.path
            query = parse_qs(parsed.query)
            self._actor()
            if path == "/api/sync/batches":
                return {"items": sync_service.list_batches(int(query.get("limit", ["100"])[0]))}
            match = BATCH_REF_RE.match(path)
            if match and not path.endswith("/resume"):
                return sync_service.get_batch(match.group(1))
            if path == "/api/sync/conflicts":
                return {"items": sync_service.list_conflicts(query.get("state", [None])[0])}
            if path == "/api/sync/demands":
                return {"items": sync_service.list_demands(
                    query.get("cable", [None])[0], query.get("segment", [None])[0],
                    query.get("status", [None])[0])}
            if path == "/api/sync/availability":
                return sync_service.availability_view(
                    query.get("cable", [None])[0], query.get("segment", [None])[0])
            if path == "/api/sync/stock":
                return {"items": sync_service.list_stock(query.get("warehouse", [None])[0])}
            if path == "/api/sync/moves":
                return {"items": sync_service.list_moves(query.get("warehouse", [None])[0])}
            if path == "/api/sync/vessels":
                return {"items": sync_service.list_vessels()}
            if path == "/api/sync/spares":
                return {"items": sync_service.list_spares()}
            if path == "/api/sync/outbound":
                return {"items": sync_service.list_outbound(query.get("demand_ref", [None])[0])}
            if path == "/api/sync/reconcile":
                return sync_service.reconcile()
            match = DEMAND_REF_RE.match(path)
            if match and not path.endswith("/promote"):
                return sync_service.get_demand(match.group(1))
            return _NO_MATCH

        def _sync_post(self, parsed, body) -> tuple:
            path = parsed.path
            actor = self._actor()
            actor_id = actor.user_id
            if path == "/api/sync/batches":
                items = body.get("items")
                if items is None and isinstance(body.get("payload"), dict):
                    items = body["payload"].get("items")
                result = sync_service.submit_batch(
                    actor_id, body.get("batch_ref", ""), body.get("node", ""),
                    items or [], body.get("checksum"), body.get("payload"))
                return (200 if result.get("idempotent_replay") else 201), result
            match = BATCH_RESUME_RE.match(path)
            if match:
                return 200, sync_service.resume_batch(actor_id, match.group(1))
            match = CONFLICT_ID_RE.match(path)
            if match:
                return 200, sync_service.resolve_conflict(actor_id, int(match.group(1)), body.get("resolution", body))
            match = DEMAND_PROMOTE_RE.match(path)
            if match:
                return 200, sync_service.promote_draft(actor_id, match.group(1))
            match = ORDER_REF_RE.match(path)
            if match:
                order_ref, verb = match.group(1), match.group(2)
                if verb == "ship":
                    return 200, sync_service.ship_outbound(actor_id, order_ref)
                return 200, sync_service.cancel_outbound(actor_id, order_ref)
            if path == "/api/sync/stock-moves":
                result = sync_service.stock_move(
                    actor_id, body.get("move_ref", ""), body.get("warehouse_id", ""),
                    body.get("cable_type", ""), body.get("kind", ""),
                    float(body.get("on_hand_delta", 0) or 0),
                    float(body.get("in_transit_delta", 0) or 0), body.get("reason", ""))
                return 201, result
            if path == "/api/sync/outbound":
                result = sync_service.issue_outbound(
                    actor_id, body.get("order_ref", ""), body.get("demand_ref", ""),
                    body.get("warehouse_id", ""), float(body.get("qty_km", 0)),
                    float(body.get("amount", 0) or 0))
                return 201, result
            if path == "/api/sync/settlements":
                result = sync_service.create_settlement(
                    actor_id, body.get("entry_ref", ""), body.get("order_ref", ""),
                    body.get("cable_type", ""), float(body.get("qty_km", 0)), float(body.get("amount", 0)))
                return 201, result
            return 404, {"error": "not_found", "message": "路径不存在"}

        def do_POST(self) -> None:
            try:
                parsed = urlparse(self.path)
                body = self._body()
                if parsed.path.startswith("/api/sync/") and sync_service is not None:
                    status_code, response = self._sync_post(parsed, body)
                    self._send(status_code, response)
                    return
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
                self._send(404, {"error": "not_found", "message": "路径不存在"})
            except Exception as exc:
                self._handle_error(exc)

    return Handler


def create_server(host: str, port: int, service: Any, static_dir: Path, sync_service: Any = None) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(service, static_dir, sync_service))
