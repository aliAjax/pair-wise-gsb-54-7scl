"""同步子系统的 SQLite 表结构与事务访问。

所有写入方法都在单一连接上以 ``BEGIN IMMEDIATE`` 开启事务，调用方负责
commit/rollback，从而保证"逐项落盘、失败可续作"。
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from . import errors as exc


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def loads(raw: str) -> Any:
    return json.loads(raw)


ACTIVE_DEMAND_STATES = ("planned", "held", "draft")
OUTSTANDING_DEMAND_STATES = ("planned", "held")


class _GuardedConnection:
    """对 sqlite3.Connection 的薄包装，允许在首条写语句上注入一次故障。

    其余属性代理给真实连接，保证 rollback/close 不受影响。
    """

    def __init__(self, connection: sqlite3.Connection, hook: BaseException) -> None:
        self._conn = connection
        self._hook: Optional[BaseException] = hook

    def execute(self, statement: str, *args, **kwargs):
        if self._hook is not None and str(statement).lstrip()[:6].upper() in {"INSERT", "UPDATE"}:
            hook, self._hook = self._hook, None
            raise hook
        return self._conn.execute(statement, *args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._conn, item)


class SyncStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        # 故障注入钩子：设置后在下一事务首次写语句上触发一次。
        self.write_failure_hook: Optional[BaseException] = None
        self._init_schema()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        connection = sqlite3.connect(self.db_path, timeout=20)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 20000")
        guarded: Any = connection
        if self.write_failure_hook is not None:
            guarded = _GuardedConnection(connection, self.write_failure_hook)
            self.write_failure_hook = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield guarded
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _init_schema(self) -> None:
        with self.transaction() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS sync_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_ref TEXT NOT NULL UNIQUE,
                    node TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    detail TEXT,
                    submitted_count INTEGER NOT NULL DEFAULT 0,
                    applied_count INTEGER NOT NULL DEFAULT 0,
                    duplicate_count INTEGER NOT NULL DEFAULT 0,
                    conflict_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES sync_batches(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    detail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, source, source_ref)
                );
                CREATE INDEX IF NOT EXISTS idx_sync_items_batch ON sync_items(batch_id);
                CREATE INDEX IF NOT EXISTS idx_sync_items_ref ON sync_items(source, source_ref);
                CREATE TABLE IF NOT EXISTS sync_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    group_key TEXT NOT NULL,
                    title TEXT NOT NULL,
                    item_refs TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    resolution TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sync_conflicts_state ON sync_conflicts(state);
                CREATE TABLE IF NOT EXISTS sync_vessel_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vessel_id TEXT NOT NULL,
                    machine_id TEXT NOT NULL,
                    vessel_name TEXT NOT NULL DEFAULT '',
                    spare_onboard_km REAL NOT NULL DEFAULT 0,
                    updated_batch_ref TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    UNIQUE(vessel_id, machine_id)
                );
                CREATE TABLE IF NOT EXISTS sync_spares (
                    spare_id TEXT PRIMARY KEY,
                    cable_type TEXT NOT NULL,
                    km REAL NOT NULL,
                    location TEXT NOT NULL,
                    warehouse_id TEXT NOT NULL DEFAULT '',
                    vessel_id TEXT NOT NULL DEFAULT '',
                    updated_batch_ref TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_stock_moves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    move_ref TEXT NOT NULL UNIQUE,
                    warehouse_id TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    on_hand_delta REAL NOT NULL DEFAULT 0,
                    in_transit_delta REAL NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_warehouse_stock (
                    warehouse_id TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    on_hand_km REAL NOT NULL DEFAULT 0,
                    in_transit_km REAL NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (warehouse_id, cable_type)
                );
                CREATE TABLE IF NOT EXISTS sync_demands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demand_ref TEXT NOT NULL UNIQUE,
                    cable TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    start_km REAL NOT NULL,
                    end_km REAL NOT NULL,
                    union_km REAL NOT NULL,
                    required_km REAL NOT NULL,
                    allocated_km REAL NOT NULL DEFAULT 0,
                    holder_vessel_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    arrived_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_sync_demands_loc ON sync_demands(cable, segment);
                CREATE INDEX IF NOT EXISTS idx_sync_demands_status ON sync_demands(status);
                CREATE TABLE IF NOT EXISTS sync_demand_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demand_id INTEGER NOT NULL REFERENCES sync_demands(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    declared_km REAL NOT NULL,
                    start_km REAL NOT NULL,
                    end_km REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(source, source_ref)
                );
                CREATE TABLE IF NOT EXISTS sync_segment_locks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cable TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    demand_id INTEGER NOT NULL REFERENCES sync_demands(id),
                    vessel_id TEXT NOT NULL,
                    held_at TEXT NOT NULL,
                    UNIQUE(cable, segment)
                );
                CREATE TABLE IF NOT EXISTS sync_outbound_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_ref TEXT NOT NULL UNIQUE,
                    demand_id INTEGER NOT NULL REFERENCES sync_demands(id),
                    warehouse_id TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    qty_km REAL NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'issued',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    shipped_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sync_outbound_demand ON sync_outbound_orders(demand_id);
                CREATE TABLE IF NOT EXISTS sync_settlement_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_ref TEXT NOT NULL UNIQUE,
                    order_ref TEXT NOT NULL,
                    cable_type TEXT NOT NULL,
                    qty_km REAL NOT NULL,
                    amount REAL NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    # ---------- 基础工具 ----------
    @staticmethod
    def _row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        return dict(row) if row is not None else None

    @staticmethod
    def _json_row(row: sqlite3.Row, fields: List[str]) -> Dict[str, Any]:
        item = dict(row)
        for field in fields:
            if item.get(field) is not None:
                item[field] = loads(item[field])
        return item

    # ---------- 批次 ----------
    def insert_batch(self, c: sqlite3.Connection, batch_ref: str, node: str, checksum: str,
                     payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        ts = now()
        cursor = c.execute(
            "INSERT INTO sync_batches(batch_ref,node,checksum,payload,state,submitted_count,created_by,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (batch_ref, node, checksum, dumps(payload), "received", len(payload.get("items", [])), actor_id, ts, ts),
        )
        return self.get_batch(c, int(cursor.lastrowid))

    def get_batch(self, c: sqlite3.Connection, batch_id: int = None, batch_ref: str = None) -> Optional[Dict[str, Any]]:
        if batch_id is not None:
            row = c.execute("SELECT * FROM sync_batches WHERE id=?", (batch_id,)).fetchone()
        else:
            row = c.execute("SELECT * FROM sync_batches WHERE batch_ref=?", (batch_ref,)).fetchone()
        return self._json_row(row, ["payload", "detail"]) if row else None

    def list_batches(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self.transaction() as c:
            rows = c.execute("SELECT * FROM sync_batches ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [self._json_row(row, ["payload", "detail"]) for row in rows]

    def update_batch_totals(self, c: sqlite3.Connection, batch_id: int, state: str,
                            counts: Dict[str, int], detail: Dict[str, Any] = None) -> None:
        c.execute(
            "UPDATE sync_batches SET state=?,applied_count=?,duplicate_count=?,conflict_count=?,"
            "failed_count=?,detail=?,updated_at=? WHERE id=?",
            (state, counts.get("applied", 0), counts.get("duplicate", 0), counts.get("conflict", 0),
             counts.get("failed", 0), dumps(detail) if detail is not None else None, now(), batch_id),
        )

    # ---------- 批次明细 ----------
    def insert_item(self, c: sqlite3.Connection, batch_id: int, source: str, source_ref: str,
                    payload: Dict[str, Any], state: str, detail: Dict[str, Any] = None) -> Dict[str, Any]:
        ts = now()
        cursor = c.execute(
            "INSERT INTO sync_items(batch_id,source,source_ref,payload,state,detail,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (batch_id, source, source_ref, dumps(payload), state,
             dumps(detail) if detail is not None else None, ts, ts),
        )
        return self.get_item(c, int(cursor.lastrowid))

    def get_item(self, c: sqlite3.Connection, item_id: int) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_items WHERE id=?", (item_id,)).fetchone()
        return self._json_row(row, ["payload", "detail"]) if row else None

    def get_item_by_ref(self, c: sqlite3.Connection, source: str, source_ref: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_items WHERE source=? AND source_ref=?", (source, source_ref)).fetchone()
        return self._json_row(row, ["payload", "detail"]) if row else None

    def list_items(self, batch_id: int) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            rows = c.execute("SELECT * FROM sync_items WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
            return [self._json_row(row, ["payload", "detail"]) for row in rows]

    def update_item(self, c: sqlite3.Connection, item_id: int, state: str, detail: Dict[str, Any] = None) -> None:
        c.execute("UPDATE sync_items SET state=?,detail=?,updated_at=? WHERE id=?",
                  (state, dumps(detail) if detail is not None else None, now(), item_id))

    # ---------- 冲突 ----------
    def insert_conflict(self, c: sqlite3.Connection, kind: str, group_key: str, title: str,
                        item_refs: List[str], payload: Dict[str, Any]) -> Dict[str, Any]:
        cursor = c.execute(
            "INSERT INTO sync_conflicts(kind,group_key,title,item_refs,payload,state,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (kind, group_key, title, dumps(item_refs), dumps(payload), "pending", now()),
        )
        return self.get_conflict(c, int(cursor.lastrowid))

    def open_conflict_for(self, c: sqlite3.Connection, group_key: str) -> Optional[Dict[str, Any]]:
        row = c.execute(
            "SELECT * FROM sync_conflicts WHERE group_key=? AND state='pending' ORDER BY id DESC",
            (group_key,),
        ).fetchone()
        return self._json_row(row, ["item_refs", "payload", "resolution"]) if row else None

    def get_conflict(self, c: sqlite3.Connection, conflict_id: int) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_conflicts WHERE id=?", (conflict_id,)).fetchone()
        return self._json_row(row, ["item_refs", "payload", "resolution"]) if row else None

    def list_conflicts(self, state: str = None) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            if state:
                rows = c.execute("SELECT * FROM sync_conflicts WHERE state=? ORDER BY id", (state,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM sync_conflicts ORDER BY id").fetchall()
            return [self._json_row(row, ["item_refs", "payload", "resolution"]) for row in rows]

    def resolve_conflict(self, c: sqlite3.Connection, conflict_id: int, resolution: Dict[str, Any]) -> None:
        c.execute("UPDATE sync_conflicts SET state='resolved',resolution=?,resolved_at=? WHERE id=?",
                  (dumps(resolution), now(), conflict_id))

    # ---------- 船机 ----------
    def upsert_vessel(self, c: sqlite3.Connection, vessel_id: str, machine_id: str,
                      vessel_name: str, spare_onboard_km: float, batch_ref: str) -> None:
        ts = now()
        c.execute(
            "INSERT INTO sync_vessel_states(vessel_id,machine_id,vessel_name,spare_onboard_km,updated_batch_ref,updated_at)"
            " VALUES(?,?,?,?,?,?) ON CONFLICT(vessel_id,machine_id) DO UPDATE SET vessel_name=excluded.vessel_name,"
            " spare_onboard_km=excluded.spare_onboard_km, updated_batch_ref=excluded.updated_batch_ref, updated_at=excluded.updated_at",
            (vessel_id, machine_id, vessel_name, spare_onboard_km, batch_ref, ts),
        )

    def list_vessels(self) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            return [dict(row) for row in c.execute("SELECT * FROM sync_vessel_states ORDER BY vessel_id,machine_id")]

    # ---------- 备缆登记与库存 ----------
    def get_spare(self, c: sqlite3.Connection, spare_id: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_spares WHERE spare_id=?", (spare_id,)).fetchone()
        return self._row(row)

    def upsert_spare(self, c: sqlite3.Connection, spare_id: str, cable_type: str, km: float,
                     location: str, warehouse_id: str, vessel_id: str, batch_ref: str) -> None:
        ts = now()
        c.execute(
            "INSERT INTO sync_spares(spare_id,cable_type,km,location,warehouse_id,vessel_id,updated_batch_ref,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(spare_id) DO UPDATE SET cable_type=excluded.cable_type,km=excluded.km,"
            "location=excluded.location,warehouse_id=excluded.warehouse_id,vessel_id=excluded.vessel_id,"
            "updated_batch_ref=excluded.updated_batch_ref,updated_at=excluded.updated_at",
            (spare_id, cable_type, km, location, warehouse_id, vessel_id, batch_ref, ts),
        )

    def list_spares(self) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            return [dict(row) for row in c.execute("SELECT * FROM sync_spares ORDER BY spare_id")]

    def stock_row(self, c: sqlite3.Connection, warehouse_id: str, cable_type: str) -> Optional[Dict[str, Any]]:
        row = c.execute(
            "SELECT * FROM sync_warehouse_stock WHERE warehouse_id=? AND cable_type=?",
            (warehouse_id, cable_type),
        ).fetchone()
        return self._row(row)

    def list_stock(self, warehouse_id: str = None) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            if warehouse_id:
                rows = c.execute(
                    "SELECT * FROM sync_warehouse_stock WHERE warehouse_id=? ORDER BY cable_type",
                    (warehouse_id,),
                ).fetchall()
            else:
                rows = c.execute("SELECT * FROM sync_warehouse_stock ORDER BY warehouse_id,cable_type").fetchall()
            return [dict(row) for row in rows]

    def apply_stock_move(self, c: sqlite3.Connection, move_ref: str, warehouse_id: str, cable_type: str,
                         kind: str, on_hand_delta: float, in_transit_delta: float,
                         reason: str, actor_id: str) -> Dict[str, Any]:
        """幂等记录一笔库存变动并物化到仓库库存。move_ref 重复时返回既有变动。"""
        existing = c.execute("SELECT * FROM sync_stock_moves WHERE move_ref=?", (move_ref,)).fetchone()
        if existing is not None:
            return dict(existing)
        ts = now()
        c.execute(
            "INSERT INTO sync_stock_moves(move_ref,warehouse_id,cable_type,kind,on_hand_delta,in_transit_delta,"
            "reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (move_ref, warehouse_id, cable_type, kind, on_hand_delta, in_transit_delta, reason, actor_id, ts),
        )
        c.execute(
            "INSERT INTO sync_warehouse_stock(warehouse_id,cable_type,on_hand_km,in_transit_km,version,updated_at)"
            " VALUES(?,?,?,?,1,?) ON CONFLICT(warehouse_id,cable_type) DO UPDATE SET"
            " on_hand_km=on_hand_km+excluded.on_hand_km, in_transit_km=in_transit_km+excluded.in_transit_km,"
            " version=version+1, updated_at=excluded.updated_at",
            (warehouse_id, cable_type, on_hand_delta, in_transit_delta, ts),
        )
        return dict(c.execute("SELECT * FROM sync_stock_moves WHERE move_ref=?", (move_ref,)).fetchone())

    def adjust_stock_direct(self, c: sqlite3.Connection, warehouse_id: str, cable_type: str,
                            on_hand_delta: float, in_transit_delta: float) -> None:
        ts = now()
        c.execute(
            "INSERT INTO sync_warehouse_stock(warehouse_id,cable_type,on_hand_km,in_transit_km,version,updated_at)"
            " VALUES(?,?,?,?,1,?) ON CONFLICT(warehouse_id,cable_type) DO UPDATE SET"
            " on_hand_km=on_hand_km+?, in_transit_km=in_transit_km+?, version=version+1, updated_at=?",
            (warehouse_id, cable_type, on_hand_delta, in_transit_delta, ts, on_hand_delta, in_transit_delta, ts),
        )

    def list_moves(self, warehouse_id: str = None) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            if warehouse_id:
                rows = c.execute(
                    "SELECT * FROM sync_stock_moves WHERE warehouse_id=? ORDER BY id", (warehouse_id,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM sync_stock_moves ORDER BY id").fetchall()
            return [dict(row) for row in rows]

    # ---------- 需求与区段锁 ----------
    def insert_demand(self, c: sqlite3.Connection, demand_ref: str, cable: str, segment: str,
                      cable_type: str, start_km: float, end_km: float, union_km: float, required_km: float,
                      holder_vessel_id: str, status: str, arrived_at: str) -> Dict[str, Any]:
        ts = now()
        cursor = c.execute(
            "INSERT INTO sync_demands(demand_ref,cable,segment,cable_type,start_km,end_km,union_km,required_km,"
            "holder_vessel_id,status,arrived_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (demand_ref, cable, segment, cable_type, start_km, end_km, union_km, required_km,
             holder_vessel_id, status, arrived_at, ts, ts),
        )
        return self.get_demand(c, int(cursor.lastrowid))

    def get_demand(self, c: sqlite3.Connection, demand_id: int) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_demands WHERE id=?", (demand_id,)).fetchone()
        return self._row(row)

    def find_demands(self, c: sqlite3.Connection, cable: str, segment: str,
                     statuses: List[str] = None) -> List[Dict[str, Any]]:
        statuses = statuses or list(ACTIVE_DEMAND_STATES)
        marks = ",".join("?" for _ in statuses)
        rows = c.execute(
            "SELECT * FROM sync_demands WHERE cable=? AND segment=? AND status IN (%s) ORDER BY arrived_at,id" % marks,
            (cable, segment, *statuses),
        ).fetchall()
        return [dict(row) for row in rows]

    def find_demand_by_ref(self, c: sqlite3.Connection, demand_ref: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_demands WHERE demand_ref=?", (demand_ref,)).fetchone()
        return self._row(row)

    def list_demands(self, cable: str = None, segment: str = None, status: str = None) -> List[Dict[str, Any]]:
        clauses, params = [], []
        if cable:
            clauses.append("cable=?")
            params.append(cable)
        if segment:
            clauses.append("segment=?")
            params.append(segment)
        if status:
            clauses.append("status=?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.transaction() as c:
            rows = c.execute("SELECT * FROM sync_demands%s ORDER BY cable,segment,id" % where, params).fetchall()
            return [dict(row) for row in rows]

    def update_demand_geometry(self, c: sqlite3.Connection, demand_id: int, start_km: float,
                               end_km: float, union_km: float, required_km: float, cable_type: str = None) -> None:
        if cable_type is not None:
            c.execute(
                "UPDATE sync_demands SET start_km=?,end_km=?,union_km=?,required_km=?,cable_type=?,updated_at=? WHERE id=?",
                (start_km, end_km, union_km, required_km, cable_type, now(), demand_id),
            )
        else:
            c.execute(
                "UPDATE sync_demands SET start_km=?,end_km=?,union_km=?,required_km=?,updated_at=? WHERE id=?",
                (start_km, end_km, union_km, required_km, now(), demand_id),
            )

    def set_demand_status(self, c: sqlite3.Connection, demand_id: int, status: str,
                          holder_vessel_id: str = None) -> None:
        if holder_vessel_id is not None:
            c.execute("UPDATE sync_demands SET status=?,holder_vessel_id=?,updated_at=? WHERE id=?",
                      (status, holder_vessel_id, now(), demand_id))
        else:
            c.execute("UPDATE sync_demands SET status=?,updated_at=? WHERE id=?", (status, now(), demand_id))

    def set_demand_allocation(self, c: sqlite3.Connection, demand_id: int, allocated_km: float) -> None:
        c.execute("UPDATE sync_demands SET allocated_km=?,updated_at=? WHERE id=?",
                  (round(allocated_km, 3), now(), demand_id))

    def add_demand_link(self, c: sqlite3.Connection, demand_id: int, source: str, source_ref: str,
                        declared_km: float, start_km: float, end_km: float) -> bool:
        existing = c.execute(
            "SELECT id FROM sync_demand_links WHERE source=? AND source_ref=?", (source, source_ref)).fetchone()
        if existing is not None:
            return False
        c.execute(
            "INSERT INTO sync_demand_links(demand_id,source,source_ref,declared_km,start_km,end_km,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (demand_id, source, source_ref, declared_km, start_km, end_km, now()),
        )
        return True

    def list_demand_links(self, c: sqlite3.Connection, demand_id: int) -> List[Dict[str, Any]]:
        rows = c.execute(
            "SELECT * FROM sync_demand_links WHERE demand_id=? ORDER BY id", (demand_id,)).fetchall()
        return [dict(row) for row in rows]

    def get_link_by_ref(self, c: sqlite3.Connection, source: str, source_ref: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_demand_links WHERE source=? AND source_ref=?",
                        (source, source_ref)).fetchone()
        return self._row(row)

    def delete_demand_link(self, c: sqlite3.Connection, link_id: int) -> None:
        c.execute("DELETE FROM sync_demand_links WHERE id=?", (link_id,))

    def acquire_lock(self, c: sqlite3.Connection, cable: str, segment: str,
                     demand_id: int, vessel_id: str) -> bool:
        try:
            c.execute(
                "INSERT INTO sync_segment_locks(cable,segment,demand_id,vessel_id,held_at) VALUES(?,?,?,?,?)",
                (cable, segment, demand_id, vessel_id, now()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def lock_for(self, c: sqlite3.Connection, cable: str, segment: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_segment_locks WHERE cable=? AND segment=?", (cable, segment)).fetchone()
        return self._row(row)

    def release_lock(self, c: sqlite3.Connection, cable: str, segment: str, vessel_id: str = None) -> None:
        if vessel_id is not None:
            c.execute("DELETE FROM sync_segment_locks WHERE cable=? AND segment=? AND vessel_id=?",
                      (cable, segment, vessel_id))
        else:
            c.execute("DELETE FROM sync_segment_locks WHERE cable=? AND segment=?", (cable, segment))

    # ---------- 出库单与结算 ----------
    def create_outbound(self, c: sqlite3.Connection, order_ref: str, demand_id: int, warehouse_id: str,
                        cable_type: str, qty_km: float, amount: float, actor_id: str) -> Dict[str, Any]:
        ts = now()
        c.execute(
            "INSERT INTO sync_outbound_orders(order_ref,demand_id,warehouse_id,cable_type,qty_km,amount,state,"
            "created_by,created_at) VALUES(?,?,?,?,?,?, 'issued',?,?)",
            (order_ref, demand_id, warehouse_id, cable_type, qty_km, amount, actor_id, ts),
        )
        return dict(c.execute("SELECT * FROM sync_outbound_orders WHERE order_ref=?", (order_ref,)).fetchone())

    def get_outbound(self, c: sqlite3.Connection, order_ref: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_outbound_orders WHERE order_ref=?", (order_ref,)).fetchone()
        return self._row(row)

    def mark_outbound_shipped(self, c: sqlite3.Connection, order_ref: str) -> None:
        c.execute("UPDATE sync_outbound_orders SET state='shipped',shipped_at=? WHERE order_ref=?",
                  (now(), order_ref))

    def cancel_outbound(self, c: sqlite3.Connection, order_ref: str) -> None:
        c.execute("UPDATE sync_outbound_orders SET state='cancelled' WHERE order_ref=?", (order_ref,))

    def list_outbound(self, demand_id: int = None) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            if demand_id is not None:
                rows = c.execute("SELECT * FROM sync_outbound_orders WHERE demand_id=? ORDER BY id",
                                 (demand_id,)).fetchall()
            else:
                rows = c.execute("SELECT * FROM sync_outbound_orders ORDER BY id").fetchall()
            return [dict(row) for row in rows]

    def create_settlement(self, c: sqlite3.Connection, entry_ref: str, order_ref: str, cable_type: str,
                          qty_km: float, amount: float, actor_id: str) -> Dict[str, Any]:
        ts = now()
        c.execute(
            "INSERT INTO sync_settlement_entries(entry_ref,order_ref,cable_type,qty_km,amount,created_by,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (entry_ref, order_ref, cable_type, qty_km, amount, actor_id, ts),
        )
        return dict(c.execute("SELECT * FROM sync_settlement_entries WHERE entry_ref=?", (entry_ref,)).fetchone())

    def get_settlement(self, c: sqlite3.Connection, entry_ref: str) -> Optional[Dict[str, Any]]:
        row = c.execute("SELECT * FROM sync_settlement_entries WHERE entry_ref=?", (entry_ref,)).fetchone()
        return self._row(row)

    def settlement_for_order(self, c: sqlite3.Connection, order_ref: str) -> List[Dict[str, Any]]:
        rows = c.execute("SELECT * FROM sync_settlement_entries WHERE order_ref=? ORDER BY id",
                         (order_ref,)).fetchall()
        return [dict(row) for row in rows]

    def list_settlements(self) -> List[Dict[str, Any]]:
        with self.transaction() as c:
            return [dict(row) for row in c.execute("SELECT * FROM sync_settlement_entries ORDER BY id")]

    # ---------- 重算所需的全量视图 ----------
    def demands_for_recompute(self, c: sqlite3.Connection) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in OUTSTANDING_DEMAND_STATES)
        rows = c.execute(
            "SELECT * FROM sync_demands WHERE status IN (%s) ORDER BY arrived_at,id" % marks,
            OUTSTANDING_DEMAND_STATES,
        ).fetchall()
        return [dict(row) for row in rows]

    def health(self) -> bool:
        try:
            with self.transaction() as c:
                c.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False
