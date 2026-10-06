"""SQLite 表结构与事务访问。"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .domain import Conflict, NotFound, ValidationError


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reference TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);
                CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_events(record_id, id);

                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_key TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    vessel_name TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL,
                    items TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inventory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    warehouse TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    on_hand_km REAL NOT NULL DEFAULT 0,
                    in_transit_km REAL NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL,
                    UNIQUE(warehouse, cable_type)
                );
                CREATE TABLE IF NOT EXISTS inventory_moves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    move_key TEXT NOT NULL UNIQUE,
                    warehouse TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    delta_on_hand REAL NOT NULL DEFAULT 0,
                    delta_in_transit REAL NOT NULL DEFAULT 0,
                    ref_type TEXT NOT NULL DEFAULT '',
                    ref_id TEXT NOT NULL DEFAULT '',
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segment_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cable TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    warehouse TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    ranges TEXT NOT NULL,
                    contributors TEXT NOT NULL,
                    required_km REAL NOT NULL,
                    required_override_km REAL,
                    issued_km REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(cable, segment, warehouse, cable_type)
                );
                CREATE TABLE IF NOT EXISTS segment_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim_key TEXT NOT NULL UNIQUE,
                    batch_key TEXT NOT NULL,
                    vessel_name TEXT NOT NULL,
                    cable TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    start_km REAL NOT NULL,
                    end_km REAL NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbound_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_key TEXT NOT NULL UNIQUE,
                    plan_id INTEGER NOT NULL REFERENCES segment_plans(id),
                    warehouse TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    quantity_km REAL NOT NULL,
                    vessel_name TEXT NOT NULL DEFAULT '',
                    segment TEXT NOT NULL DEFAULT '',
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settlement_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_key TEXT NOT NULL UNIQUE,
                    order_key TEXT NOT NULL REFERENCES outbound_orders(order_key),
                    quantity_km REAL NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conflict_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    batch_key TEXT NOT NULL DEFAULT '',
                    cable TEXT NOT NULL DEFAULT '',
                    segment TEXT NOT NULL DEFAULT '',
                    details TEXT NOT NULL,
                    state TEXT NOT NULL,
                    resolution TEXT,
                    resolved_by TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
                CREATE INDEX IF NOT EXISTS idx_claims_segment ON segment_claims(cable, segment, state);
                CREATE INDEX IF NOT EXISTS idx_plans_wh ON segment_plans(warehouse, cable_type, state);
                CREATE INDEX IF NOT EXISTS idx_moves_wh ON inventory_moves(warehouse, cable_type);
                CREATE INDEX IF NOT EXISTS idx_settle_order ON settlement_entries(order_key);
                """
            )

    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["payload"] = json.loads(item["payload"])
        return item

    def create(self, reference: str, state: str, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO records(reference,state,version,payload,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (reference, state, 1, json.dumps(payload, ensure_ascii=False, sort_keys=True), actor_id, actor_id, now, now),
                )
                record_id = int(cursor.lastrowid)
                connection.execute(
                    "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                    (record_id, "created", actor_id, 1, json.dumps({"state": state}, ensure_ascii=False, sort_keys=True), now),
                )
                row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        except sqlite3.IntegrityError as exc:
            raise Conflict("reference已存在") from exc
        return self._row(row)

    def get(self, record_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        return self._row(row)

    def list_records(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if state:
                rows = connection.execute("SELECT * FROM records WHERE state=? ORDER BY id DESC LIMIT ?", (state, limit)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._row(row) for row in rows]

    def mutate(self, record_id: int, expected_version: int, state: str, payload: Dict[str, Any], actor_id: str, action: str, details: Dict[str, Any]) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            version = int(expected_version) + 1
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (state, version, json.dumps(payload, ensure_ascii=False, sort_keys=True), actor_id, now, record_id),
            )
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (record_id, action, actor_id, version, json.dumps(details, ensure_ascii=False, sort_keys=True), now),
            )
            result = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            connection.commit()
        return self._row(result)

    def add_audit(self, record_id: int, actor_id: str, action: str, details: Dict[str, Any]) -> None:
        with self._connect() as connection:
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                raise NotFound("记录不存在")
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (record_id, action, actor_id, int(row["version"]), json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
            )

    def audit_timeline(self, record_id: int) -> List[Dict[str, Any]]:
        self.get(record_id)
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM audit_events WHERE record_id=? ORDER BY id", (record_id,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def stats(self) -> Dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT state, COUNT(*) AS total FROM records GROUP BY state").fetchall()
        return {str(row["state"]): int(row["total"]) for row in rows}

    def health(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # ---------- 多语句事务 ----------

    @contextmanager
    def transaction(self):
        """单事务执行多步写入：任一步失败整体回滚，批次可从完整记录恢复。"""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    # ---------- 批次 ----------

    @staticmethod
    def _batch_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["items"] = json.loads(item["items"])
        item["result"] = json.loads(item["result"]) if item["result"] else None
        return item

    def insert_batch(self, batch_key: str, source: str, vessel_name: str, items: List[Dict[str, Any]], actor_id: str) -> Optional[Dict[str, Any]]:
        """登记完整批次内容；batch_key重复时返回None，由调用方走幂等回放。"""
        now = _now()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO batches(batch_key,source,vessel_name,state,items,result,error,version,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_key, source, vessel_name, "submitted", json.dumps(items, ensure_ascii=False, sort_keys=True), None, None, 1, actor_id, actor_id, now, now),
                )
                row = connection.execute("SELECT * FROM batches WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        except sqlite3.IntegrityError:
            return None
        return self._batch_row(row)

    def find_batch(self, batch_key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM batches WHERE batch_key=?", (batch_key,)).fetchone()
        return self._batch_row(row) if row else None

    def get_batch(self, batch_key: str) -> Dict[str, Any]:
        batch = self.find_batch(batch_key)
        if batch is None:
            raise NotFound("批次不存在")
        return batch

    def list_batches(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if state:
                rows = connection.execute("SELECT * FROM batches WHERE state=? ORDER BY id DESC LIMIT ?", (state, limit)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._batch_row(row) for row in rows]

    def replace_batch_items(self, batch_key: str, items: List[Dict[str, Any]], actor_id: str) -> Dict[str, Any]:
        """留草稿的批次允许改写后重新提交；改写后旧的待裁决冲突作废。"""
        now = _now()
        with self._connect() as connection:
            connection.execute(
                "UPDATE batches SET items=?,result=NULL,error=NULL,version=version+1,updated_by=?,updated_at=? WHERE batch_key=? AND state='draft'",
                (json.dumps(items, ensure_ascii=False, sort_keys=True), actor_id, now, batch_key),
            )
            connection.execute(
                "UPDATE conflicts SET state='resolved',resolution='superseded_by_rewrite',resolved_by=?,resolved_at=? WHERE batch_key=? AND state='pending'",
                (actor_id, now, batch_key),
            )
            row = connection.execute("SELECT * FROM batches WHERE batch_key=?", (batch_key,)).fetchone()
        if row is None:
            raise NotFound("批次不存在")
        return self._batch_row(row)

    def mark_batch_failed(self, batch_key: str, error: str, actor_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE batches SET state='failed',error=?,version=version+1,updated_by=?,updated_at=? WHERE batch_key=?",
                (error[:500], actor_id, _now(), batch_key),
            )

    def tx_update_batch(self, conn: sqlite3.Connection, batch_key: str, state: str, result: Dict[str, Any], actor_id: str) -> None:
        conn.execute(
            "UPDATE batches SET state=?,result=?,error=NULL,version=version+1,updated_by=?,updated_at=? WHERE batch_key=?",
            (state, json.dumps(result, ensure_ascii=False, sort_keys=True), actor_id, _now(), batch_key),
        )

    # ---------- 库存与台账 ----------

    @staticmethod
    def _inventory_row(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def tx_get_inventory(self, conn: sqlite3.Connection, warehouse: str, cable_type: str) -> Optional[Dict[str, Any]]:
        row = conn.execute("SELECT * FROM inventory WHERE warehouse=? AND cable_type=?", (warehouse, cable_type)).fetchone()
        return self._inventory_row(row) if row else None

    def get_inventory(self, warehouse: str, cable_type: str) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM inventory WHERE warehouse=? AND cable_type=?", (warehouse, cable_type)).fetchone()
        if row is None:
            raise NotFound("仓库%s缺少%s库存主数据" % (warehouse, cable_type))
        return self._inventory_row(row)

    def tx_ensure_inventory(self, conn: sqlite3.Connection, warehouse: str, cable_type: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO inventory(warehouse,cable_type,on_hand_km,in_transit_km,version,updated_at) VALUES(?,?,0,0,1,?)",
            (warehouse, cable_type, _now()),
        )

    def tx_find_move(self, conn: sqlite3.Connection, move_key: str) -> Optional[Dict[str, Any]]:
        row = conn.execute("SELECT * FROM inventory_moves WHERE move_key=?", (move_key,)).fetchone()
        return dict(row) if row else None

    def tx_apply_move(self, conn: sqlite3.Connection, move_key: str, warehouse: str, cable_type: str, kind: str, delta_on_hand: float, delta_in_transit: float, ref_type: str, ref_id: str, actor_id: str) -> bool:
        """写入台账流水；move_key已存在时返回False，重复回传不会重复扣减。"""
        try:
            conn.execute(
                "INSERT INTO inventory_moves(move_key,warehouse,cable_type,kind,delta_on_hand,delta_in_transit,ref_type,ref_id,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (move_key, warehouse, cable_type, kind, delta_on_hand, delta_in_transit, ref_type, ref_id, actor_id, _now()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def tx_adjust_inventory(self, conn: sqlite3.Connection, warehouse: str, cable_type: str, delta_on_hand: float, delta_in_transit: float) -> Dict[str, Any]:
        row = conn.execute("SELECT * FROM inventory WHERE warehouse=? AND cable_type=?", (warehouse, cable_type)).fetchone()
        if row is None:
            raise NotFound("仓库%s缺少%s库存主数据" % (warehouse, cable_type))
        on_hand = round(float(row["on_hand_km"]) + delta_on_hand, 2)
        in_transit = round(float(row["in_transit_km"]) + delta_in_transit, 2)
        if on_hand < 0 or in_transit < 0:
            raise ValidationError("库存数量不能为负")
        conn.execute(
            "UPDATE inventory SET on_hand_km=?,in_transit_km=?,version=version+1,updated_at=? WHERE id=?",
            (on_hand, in_transit, _now(), int(row["id"])),
        )
        return self.tx_get_inventory(conn, warehouse, cable_type)

    def list_inventory_moves(self, warehouse: Optional[str] = None, cable_type: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        sql = "SELECT * FROM inventory_moves"
        params: List[Any] = []
        conditions = []
        if warehouse:
            conditions.append("warehouse=?")
            params.append(warehouse)
        if cable_type:
            conditions.append("cable_type=?")
            params.append(cable_type)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # ---------- 区段计划与占用 ----------

    @staticmethod
    def _plan_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["ranges"] = json.loads(item["ranges"])
        item["contributors"] = json.loads(item["contributors"])
        return item

    def tx_get_plan(self, conn: sqlite3.Connection, cable: str, segment: str, warehouse: str, cable_type: str) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM segment_plans WHERE cable=? AND segment=? AND warehouse=? AND cable_type=?",
            (cable, segment, warehouse, cable_type),
        ).fetchone()
        return self._plan_row(row) if row else None

    def tx_get_plan_by_id(self, conn: sqlite3.Connection, plan_id: int) -> Optional[Dict[str, Any]]:
        row = conn.execute("SELECT * FROM segment_plans WHERE id=?", (plan_id,)).fetchone()
        return self._plan_row(row) if row else None

    def get_plan(self, plan_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM segment_plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFound("区段计划不存在")
        return self._plan_row(row)

    def tx_insert_plan(self, conn: sqlite3.Connection, cable: str, segment: str, warehouse: str, cable_type: str, ranges: List[List[float]], contributors: List[Dict[str, Any]], required_km: float) -> Dict[str, Any]:
        now = _now()
        cursor = conn.execute(
            "INSERT INTO segment_plans(cable,segment,warehouse,cable_type,ranges,contributors,required_km,required_override_km,issued_km,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,NULL,0,?,?,?)",
            (cable, segment, warehouse, cable_type, json.dumps(ranges), json.dumps(contributors, ensure_ascii=False, sort_keys=True), required_km, "open", now, now),
        )
        return self.tx_get_plan_by_id(conn, int(cursor.lastrowid))

    def tx_update_plan(self, conn: sqlite3.Connection, plan_id: int, ranges: List[List[float]], contributors: List[Dict[str, Any]], required_km: float, state: str) -> Dict[str, Any]:
        conn.execute(
            "UPDATE segment_plans SET ranges=?,contributors=?,required_km=?,state=?,updated_at=? WHERE id=?",
            (json.dumps(ranges), json.dumps(contributors, ensure_ascii=False, sort_keys=True), required_km, state, _now(), plan_id),
        )
        return self.tx_get_plan_by_id(conn, plan_id)

    def tx_set_plan_issued(self, conn: sqlite3.Connection, plan_id: int, issued_km: float, state: str) -> Dict[str, Any]:
        conn.execute("UPDATE segment_plans SET issued_km=?,state=?,updated_at=? WHERE id=?", (issued_km, state, _now(), plan_id))
        return self.tx_get_plan_by_id(conn, plan_id)

    def tx_set_plan_override(self, conn: sqlite3.Connection, plan_id: int, override_km: Optional[float]) -> Dict[str, Any]:
        conn.execute("UPDATE segment_plans SET required_override_km=?,updated_at=? WHERE id=?", (override_km, _now(), plan_id))
        return self.tx_get_plan_by_id(conn, plan_id)

    def tx_open_plans(self, conn: sqlite3.Connection, warehouse: str, cable_type: str) -> List[Dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM segment_plans WHERE warehouse=? AND cable_type=? AND state='open' ORDER BY id",
            (warehouse, cable_type),
        ).fetchall()
        return [self._plan_row(row) for row in rows]

    def list_plans(self, state: Optional[str] = None, warehouse: Optional[str] = None, cable_type: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM segment_plans"
        params: List[Any] = []
        conditions = []
        if state:
            conditions.append("state=?")
            params.append(state)
        if warehouse:
            conditions.append("warehouse=?")
            params.append(warehouse)
        if cable_type:
            conditions.append("cable_type=?")
            params.append(cable_type)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._plan_row(row) for row in rows]

    def tx_active_claims(self, conn: sqlite3.Connection, cable: str, segment: str) -> List[Dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM segment_claims WHERE cable=? AND segment=? AND state='active'",
            (cable, segment),
        ).fetchall()
        return [dict(row) for row in rows]

    def tx_insert_claim(self, conn: sqlite3.Connection, claim_key: str, batch_key: str, vessel_name: str, cable: str, segment: str, start_km: float, end_km: float) -> bool:
        try:
            conn.execute(
                "INSERT INTO segment_claims(claim_key,batch_key,vessel_name,cable,segment,start_km,end_km,state,created_at) VALUES(?,?,?,?,?,?,?,'active',?)",
                (claim_key, batch_key, vessel_name, cable, segment, start_km, end_km, _now()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def tx_release_claims(self, conn: sqlite3.Connection, batch_key: str, cable: str, segment: str) -> int:
        cursor = conn.execute(
            "UPDATE segment_claims SET state='released' WHERE batch_key=? AND cable=? AND segment=? AND state='active'",
            (batch_key, cable, segment),
        )
        return int(cursor.rowcount)

    # ---------- 冲突 ----------

    @staticmethod
    def _conflict_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["details"] = json.loads(item["details"])
        return item

    def tx_insert_conflict(self, conn: sqlite3.Connection, conflict_key: str, kind: str, batch_key: str, cable: str, segment: str, details: Dict[str, Any]) -> bool:
        try:
            conn.execute(
                "INSERT INTO conflicts(conflict_key,kind,batch_key,cable,segment,details,state,created_at) VALUES(?,?,?,?,?,?,'pending',?)",
                (conflict_key, kind, batch_key, cable, segment, json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def list_conflicts(self, state: Optional[str] = None, batch_key: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM conflicts"
        params: List[Any] = []
        conditions = []
        if state:
            conditions.append("state=?")
            params.append(state)
        if batch_key:
            conditions.append("batch_key=?")
            params.append(batch_key)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._conflict_row(row) for row in rows]

    def get_conflict(self, conflict_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if row is None:
            raise NotFound("冲突不存在")
        return self._conflict_row(row)

    def tx_get_conflict(self, conn: sqlite3.Connection, conflict_id: int) -> Optional[Dict[str, Any]]:
        row = conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        return self._conflict_row(row) if row else None

    def tx_pending_conflicts_for_batch(self, conn: sqlite3.Connection, batch_key: str) -> int:
        row = conn.execute("SELECT COUNT(*) AS total FROM conflicts WHERE batch_key=? AND state='pending'", (batch_key,)).fetchone()
        return int(row["total"])

    def tx_resolve_conflict(self, conn: sqlite3.Connection, conflict_id: int, resolution: str, actor_id: str) -> None:
        conn.execute(
            "UPDATE conflicts SET state='resolved',resolution=?,resolved_by=?,resolved_at=? WHERE id=?",
            (resolution, actor_id, _now(), conflict_id),
        )

    # ---------- 出库与结算 ----------

    def find_outbound(self, order_key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM outbound_orders WHERE order_key=?", (order_key,)).fetchone()
        return dict(row) if row else None

    def tx_insert_outbound(self, conn: sqlite3.Connection, order_key: str, plan_id: int, warehouse: str, cable_type: str, quantity_km: float, vessel_name: str, segment: str, actor_id: str) -> Dict[str, Any]:
        cursor = conn.execute(
            "INSERT INTO outbound_orders(order_key,plan_id,warehouse,cable_type,quantity_km,vessel_name,segment,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (order_key, plan_id, warehouse, cable_type, quantity_km, vessel_name, segment, actor_id, _now()),
        )
        row = conn.execute("SELECT * FROM outbound_orders WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        return dict(row)

    def list_outbound(self, limit: int = 200) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM outbound_orders ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def find_settlement(self, entry_key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM settlement_entries WHERE entry_key=?", (entry_key,)).fetchone()
        return dict(row) if row else None

    def insert_settlement(self, entry_key: str, order_key: str, quantity_km: float, amount: float, actor_id: str) -> Optional[Dict[str, Any]]:
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO settlement_entries(entry_key,order_key,quantity_km,amount,actor_id,created_at) VALUES(?,?,?,?,?,?)",
                    (entry_key, order_key, quantity_km, amount, actor_id, _now()),
                )
                row = connection.execute("SELECT * FROM settlement_entries WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        except sqlite3.IntegrityError:
            return None
        return dict(row)

    def list_settlements(self, order_key: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            if order_key:
                rows = connection.execute("SELECT * FROM settlement_entries WHERE order_key=? ORDER BY id", (order_key,)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM settlement_entries ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def reconcile_rows(self, warehouse: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = (
            "SELECT o.id, o.order_key, o.plan_id, o.warehouse, o.cable_type, o.quantity_km AS issued_km,"
            " o.vessel_name, o.segment, o.actor_id, o.created_at,"
            " COALESCE(SUM(s.quantity_km),0) AS settled_km, COALESCE(SUM(s.amount),0) AS settled_amount,"
            " COUNT(s.id) AS settlement_entries"
            " FROM outbound_orders o LEFT JOIN settlement_entries s ON s.order_key=o.order_key"
        )
        params: List[Any] = []
        if warehouse:
            sql += " WHERE o.warehouse=?"
            params.append(warehouse)
        sql += " GROUP BY o.id ORDER BY o.id"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
