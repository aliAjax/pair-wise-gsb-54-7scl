"""续作批次同步引擎：编排四源合并、冲突裁决、区段占用与库存重算。

事务边界：每次 submit/resume/写操作在单个 ``BEGIN IMMEDIATE`` 事务内完成，
SQLite 写锁保证"两艘船同时提交"时全局串行，先到者占用区段。
"""
import hashlib
import json
import threading
from typing import Any, Dict, List, Optional, Tuple

from . import errors as exc
from .store import SyncStore, now


SOURCES = {"shore_workorder", "vessel_task", "spare_register", "warehouse_move", "outbound_order", "settlement"}
NODES = {"shore", "vessel", "warehouse", "settlement"}
ITEM_STATES = {"pending", "applied", "duplicate", "conflict", "failed", "skipped"}
DEMAND_SOURCES = {"shore_workorder", "vessel_task"}
QTY_TOLERANCE_KM = 0.5
EPS = 1e-9


def _text(p: Dict[str, Any], key: str) -> str:
    value = p.get(key)
    if not isinstance(value, str) or not value.strip():
        raise exc.ValidationError("%s不能为空" % key)
    return value.strip()


def _num(p: Dict[str, Any], key: str, minimum: float = None) -> float:
    value = p.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise exc.ValidationError("%s必须是数字" % key)
    value = float(value)
    if minimum is not None and value + EPS < minimum:
        raise exc.ValidationError("%s不能小于%s" % (key, minimum))
    return value


def _optional_num(p: Dict[str, Any], key: str, default: float = None) -> Optional[float]:
    if key not in p or p.get(key) is None:
        return default
    return _num(p, key)


def _overlaps(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return a_start < b_end - EPS and a_end > b_start + EPS


def _union_length(intervals: List[Tuple[float, float]]) -> float:
    ordered = sorted((round(s, 6), round(e, 6)) for s, e in intervals)
    total = 0.0
    cur_start = cur_end = None
    for start, end in ordered:
        if cur_start is None:
            cur_start, cur_end = start, end
        elif start <= cur_end + EPS:
            cur_end = max(cur_end, end)
        else:
            total += cur_end - cur_start
            cur_start, cur_end = start, end
    if cur_start is not None:
        total += cur_end - cur_start
    return round(total, 3)


def _demand_ref(cable: str, segment: str, holder_vessel_id: str, cable_type: str = "") -> str:
    base = "%s|%s|%s|%s" % (cable, segment, holder_vessel_id or "PLANNED", cable_type)
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]
    return "DM-%s" % digest.upper()


class SyncService:
    def __init__(self, store: SyncStore = None) -> None:
        self.store = store or SyncStore("subsea-cable-repair.db")
        # 进程内兜底（SQLite 事务已保证跨进程互斥，这里仅减少无谓竞争）。
        self._lock = threading.RLock()
        # 测试钩子：注入后下一事务的首条写语句抛异常，模拟写入中途失败。
        self._failure_hook: Optional[BaseException] = None

    @property
    def failure_hook(self) -> Optional[BaseException]:
        return self._failure_hook

    @failure_hook.setter
    def failure_hook(self, value: Optional[BaseException]) -> None:
        self._failure_hook = value
        self.store.write_failure_hook = value

    # ------------------------------------------------------------
    # 批次提交 / 恢复
    # ------------------------------------------------------------
    def submit_batch(self, actor_id: str, batch_ref: str, node: str, items: List[Dict[str, Any]],
                     checksum: str = None, payload: Dict[str, Any] = None) -> Dict[str, Any]:
        batch_ref = _text({"batch_ref": batch_ref}, "batch_ref")
        if node not in NODES:
            raise exc.ValidationError("node只能是%s" % "/".join(sorted(NODES)))
        if not isinstance(items, list) or not items:
            raise exc.ValidationError("items至少包含一条")
        self._validate_items(items)
        canonical = {"batch_ref": batch_ref, "node": node, "items": items}
        digest = hashlib.sha256(
            json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        if checksum and checksum != digest:
            raise exc.Conflict("批次checksum不一致，拒绝覆盖")

        with self._lock, self.store.transaction() as c:
            existing = self.store.get_batch(c, batch_ref=batch_ref)
            if existing is not None:
                if existing["checksum"] != digest:
                    raise exc.Conflict("批次%s已存在但内容不同" % batch_ref)
                # 重复回传：原样返回，绝不重复执行副作用。
                existing["idempotent_replay"] = True
                return existing

            batch = self.store.insert_batch(c, batch_ref, node, digest, payload or canonical, actor_id)
            batch_id = batch["id"]
            for item in items:
                source = item["source"]
                source_ref = _text(item, "reference")
                self.store.insert_item(c, batch_id, source, source_ref, item.get("data", {}), "pending")
            self._run_batch(c, batch_id, actor_id)
            return self.store.get_batch(c, batch_id)

    def resume_batch(self, actor_id: str, batch_ref: str) -> Dict[str, Any]:
        """凭batch_ref（或完整批次重新提交）续作：只重试未成功项。"""
        with self._lock, self.store.transaction() as c:
            batch = self.store.get_batch(c, batch_ref=batch_ref)
            if batch is None:
                raise exc.NotFound("批次不存在，请重新提交完整批次")
            self._run_batch(c, batch["id"], actor_id)
            return self.store.get_batch(c, batch["id"])

    def get_batch(self, batch_ref: str) -> Dict[str, Any]:
        with self.store.transaction() as c:
            batch = self.store.get_batch(c, batch_ref=batch_ref)
            if batch is None:
                raise exc.NotFound("批次不存在")
            rows = c.execute("SELECT * FROM sync_items WHERE batch_id=? ORDER BY id", (batch["id"],)).fetchall()
            batch["items"] = [self.store._json_row(row, ["payload", "detail"]) for row in rows]
            return batch

    def list_batches(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.store.list_batches(limit)

    # ------------------------------------------------------------
    # 批处理主循环
    # ------------------------------------------------------------
    def _run_batch(self, c, batch_id: int, actor_id: str) -> None:
        batch = self.store.get_batch(c, batch_id)
        rows = c.execute("SELECT * FROM sync_items WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
        counts = {"applied": 0, "duplicate": 0, "conflict": 0, "failed": 0}
        details: List[Dict[str, Any]] = []
        # 库存/在途或需求一旦在本批变化，末尾统一重算一次。
        needs_recompute = False
        for row in rows:
            item = self.store._json_row(row, ["payload", "detail"])
            if item["state"] in {"applied", "duplicate"}:
                counts[item["state"]] += 1
                continue
            try:
                outcome = self._apply_item(c, item, batch["batch_ref"], actor_id)
                state = outcome.get("state", "applied")
                detail = outcome.get("detail")
                self.store.update_item(c, item["id"], state, detail)
                counts[state] = counts.get(state, 0) + 1
                details.append({"reference": item["source_ref"], "state": state, "detail": detail})
                if state == "applied" and item["source"] in {
                    "warehouse_move", "vessel_task", "shore_workorder", "outbound_order", "spare_register"
                }:
                    needs_recompute = True
            except exc.DomainError as domain_exc:
                self.store.update_item(c, item["id"], "failed", {"message": str(domain_exc), "code": domain_exc.code})
                counts["failed"] += 1
                details.append({"reference": item["source_ref"], "state": "failed", "message": str(domain_exc)})
        if needs_recompute:
            self._recompute(c)
        if counts["failed"]:
            state = "partial"
        elif counts["conflict"]:
            state = "conflict"
        elif counts["applied"] or counts["duplicate"]:
            state = "applied"
        else:
            state = "received"
        self.store.update_batch_totals(c, batch_id, state, counts, {"items": details})

    def _validate_items(self, items: List[Dict[str, Any]]) -> None:
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                raise exc.ValidationError("items每项必须是对象")
            source = item.get("source")
            if source not in SOURCES:
                raise exc.ValidationError("source只能是%s" % "/".join(sorted(SOURCES)))
            reference = item.get("reference")
            if not isinstance(reference, str) or not reference.strip():
                raise exc.ValidationError("reference不能为空")
            key = (source, reference.strip())
            if key in seen:
                raise exc.ValidationError("批次内存在重复项%s/%s" % key)
            seen.add(key)
            data = item.get("data", {})
            if not isinstance(data, dict):
                raise exc.ValidationError("data必须是对象")
            self._validate_item_payload(source, data)

    def _validate_item_payload(self, source: str, p: Dict[str, Any]) -> None:
        if source in DEMAND_SOURCES:
            _text(p, "cable")
            _text(p, "segment")
            _text(p, "cable_type")
            start = _num(p, "start_km", 0)
            end = _num(p, "end_km", 0)
            if end <= start + EPS:
                raise exc.ValidationError("end_km必须大于start_km")
            if "required_km" in p and p["required_km"] is not None:
                _num(p, "required_km", 0)
            if source == "vessel_task":
                _text(p, "vessel_id")
                _text(p, "machine_id")
                _num(p, "spare_onboard_km", 0)
        elif source == "spare_register":
            _text(p, "spare_id")
            _text(p, "cable_type")
            _num(p, "km", 0)
            location = _text(p, "location")
            if location not in {"warehouse", "vessel"}:
                raise exc.ValidationError("location只能是warehouse/vessel")
            if location == "warehouse":
                _text(p, "warehouse_id")
            else:
                _text(p, "vessel_id")
        elif source == "warehouse_move":
            _text(p, "warehouse_id")
            _text(p, "cable_type")
            kind = _text(p, "kind")
            if kind not in {"receive", "inbound", "transfer_in", "transfer_out", "adjust", "in_transit", "arrive"}:
                raise exc.ValidationError("不支持的库存变动类型")
        elif source == "outbound_order":
            _text(p, "demand_ref")
            _text(p, "warehouse_id")
            _num(p, "qty_km", 0.001)
            _optional_num(p, "amount", 0)
        elif source == "settlement":
            _text(p, "order_ref")
            _text(p, "cable_type")
            _num(p, "qty_km", 0.001)
            _num(p, "amount", 0)

    # ------------------------------------------------------------
    # 单项路由
    # ------------------------------------------------------------
    def _apply_item(self, c, item: Dict[str, Any], batch_ref: str, actor_id: str) -> Dict[str, Any]:
        source = item["source"]
        data = item["payload"]
        reference = item["source_ref"]
        prior = self.store.get_item_by_ref(c, source, reference)
        # 同一(source, reference)此前已在别的批次成功，本次重复回传不重复扣减。
        if prior is not None and prior["id"] != item["id"] and prior["state"] in {"applied", "duplicate"}:
            return {"state": "duplicate", "detail": {"first_batch_item_id": prior["id"]}}

        if source in DEMAND_SOURCES:
            return self._ingest_demand(c, item, batch_ref)
        if source == "spare_register":
            return self._ingest_spare(c, data, reference, batch_ref)
        if source == "warehouse_move":
            return self._ingest_move(c, data, reference, actor_id)
        if source == "outbound_order":
            return self._ingest_outbound(c, data, reference, actor_id)
        if source == "settlement":
            return self._ingest_settlement(c, data, reference, actor_id)
        raise exc.ValidationError("未知来源%s" % source)

    def _ingest_spare(self, c, p: Dict[str, Any], reference: str, batch_ref: str) -> Dict[str, Any]:
        warehouse_id = p.get("warehouse_id", "") if p["location"] == "warehouse" else ""
        vessel_id = p.get("vessel_id", "") if p["location"] == "vessel" else ""
        previous = self.store.get_spare(c, reference)
        self.store.upsert_spare(c, reference, p["cable_type"], float(p["km"]), p["location"],
                                warehouse_id, vessel_id, batch_ref)
        # 备缆登记本身体现仓库在库快照：同一备缆从在途到库时产生在库增量。
        if previous is None and p["location"] == "warehouse":
            self.store.adjust_stock_direct(c, warehouse_id, p["cable_type"], float(p["km"]), 0.0)
        return {"state": "applied", "detail": {"spare_id": reference}}

    def _ingest_move(self, c, p: Dict[str, Any], reference: str, actor_id: str) -> Dict[str, Any]:
        on_hand_delta = float(p.get("on_hand_delta", 0.0) or 0.0)
        in_transit_delta = float(p.get("in_transit_delta", 0.0) or 0.0)
        if abs(on_hand_delta) < EPS and abs(in_transit_delta) < EPS:
            raise exc.ValidationError("库存变动量不能全为0")
        existing = c.execute("SELECT * FROM sync_stock_moves WHERE move_ref=?", (reference,)).fetchone()
        if existing is not None:
            # 同一变动单重传：不重复加减库存。
            return {"state": "duplicate", "detail": {"move_ref": reference}}
        move = self.store.apply_stock_move(
            c, reference, p["warehouse_id"], p["cable_type"], p["kind"],
            on_hand_delta, in_transit_delta, p.get("reason", ""), actor_id,
        )
        return {"state": "applied", "detail": {"move_ref": move["move_ref"]}}

    # ------------------------------------------------------------
    # 需求合并（核心）
    # ------------------------------------------------------------
    def _ingest_demand(self, c, item: Dict[str, Any], batch_ref: str) -> Dict[str, Any]:
        p = item["payload"]
        source = item["source"]
        reference = item["source_ref"]
        cable, segment = p["cable"], p["segment"]
        start, end = float(p["start_km"]), float(p["end_km"])
        cable_type = p["cable_type"]
        declared = float(p.get("required_km") if p.get("required_km") is not None else round(end - start, 3))
        vessel_id = p.get("vessel_id", "") if source == "vessel_task" else ""

        # 已被合并过的同一来源单据 → 重复回传。
        known_link = self.store.get_link_by_ref(c, source, reference)
        if known_link is not None:
            return {"state": "duplicate", "detail": {"demand_id": known_link["demand_id"]}}

        if source == "vessel_task":
            self.store.upsert_vessel(c, vessel_id, p["machine_id"], p.get("vessel_name", vessel_id),
                                     float(p["spare_onboard_km"]), batch_ref)

        candidate = self._match_demand(c, cable, segment, cable_type, start, end, vessel_id, source)
        if candidate is None:
            demand, acquired = self._open_demand(c, cable, segment, cable_type, start, end, declared,
                                                 vessel_id, source, reference, item)
            return {
                "state": "applied",
                "detail": {
                    "demand_ref": demand["demand_ref"], "demand_id": demand["id"],
                    "merged": False, "holder_vessel_id": demand["holder_vessel_id"],
                    "status": demand["status"], "lock_acquired": acquired,
                },
            }

        demand = candidate
        # 型号不一致 → 属性冲突，不并入。
        if demand["cable_type"] != cable_type:
            self._record_conflict(
                c, kind="attribute", demand=demand, item=item,
                title="%s/%s 备缆型号不一致" % (cable, segment),
                payload={"cable_type_existing": demand["cable_type"], "cable_type_incoming": cable_type},
            )
            return {"state": "conflict", "detail": {"reason": "cable_type_mismatch", "demand_id": demand["id"]}}

        # 同区段同型号但各源"显式申报用量"差异超阈 → 数量冲突，留待裁决。
        # 里程并集自然扩大不算分歧，因此这里只比较各来源显式申报的用量。
        declared_values = [float(link["declared_km"])
                           for link in self.store.list_demand_links(c, demand["id"])] + [declared]
        if max(declared_values) - min(declared_values) > QTY_TOLERANCE_KM:
            self._record_conflict(
                c, kind="quantity", demand=demand, item=item,
                title="%s/%s 申报备缆数量存在分歧" % (cable, segment),
                payload={"declared_km_incoming": declared,
                         "declared_km_max_existing": max(declared_values[:-1])},
            )
            return {"state": "conflict", "detail": {"reason": "quantity_disagree", "demand_id": demand["id"]}}

        # 合并：里程并集，数量取并集长度与申报最大值。
        self.store.add_demand_link(c, demand["id"], source, reference, declared, start, end)
        links = self.store.list_demand_links(c, demand["id"])
        intervals = [(float(link["start_km"]), float(link["end_km"])) for link in links]
        union_km = _union_length(intervals)
        required_km = round(max(union_km, max(float(link["declared_km"]) for link in links)), 3)
        new_start = min(s for s, _ in intervals)
        new_end = max(e for _, e in intervals)
        self.store.update_demand_geometry(c, demand["id"], new_start, new_end, union_km, required_km)
        lock_acquired = False
        # 岸端计划需求被到场船舶认领时升级为占用；若区段已被他船占用则转草稿。
        if source == "vessel_task" and demand["status"] == "planned":
            lock_acquired = self.store.acquire_lock(c, cable, segment, demand["id"], vessel_id)
            new_status = "held" if lock_acquired else "draft"
            self.store.set_demand_status(c, demand["id"], new_status, vessel_id)
            demand = self.store.get_demand(c, demand["id"])
        return {
            "state": "applied",
            "detail": {
                "demand_ref": demand["demand_ref"], "demand_id": demand["id"],
                "merged": True, "union_km": union_km, "required_km": required_km,
                "status": demand["status"], "lock_acquired": lock_acquired,
            },
        }

    def _match_demand(self, c, cable: str, segment: str, cable_type: str,
                      start: float, end: float, vessel_id: str, source: str):
        """找到应并入的活跃需求。

        - 岸端工单：并入同区段同型号里程相交的任一活跃需求（无论占用方是谁）；
          没有则开planned。
        - 船端任务：只并入自己持有/未占用的需求；他船持锁则另开草稿。
        """
        demands = self.store.find_demands(c, cable, segment)
        for demand in demands:
            if not _overlaps(start, end, float(demand["start_km"]), float(demand["end_km"])):
                continue
            held_by_other = (
                source == "vessel_task"
                and demand["holder_vessel_id"] not in {"", vessel_id}
            )
            # 船端任务遇到他船持锁的需求：型号不同直接另开草稿，
            # 不把型号差异升级为属性冲突（现场并非在裁决同一需求）。
            if held_by_other and demand["cable_type"] != cable_type:
                continue
            if held_by_other:
                continue
            return demand
        return None

    def _open_demand(self, c, cable: str, segment: str, cable_type: str, start: float, end: float,
                     declared: float, vessel_id: str, source: str, reference: str, item: Dict[str, Any]):
        status = "held" if vessel_id else "planned"
        demand_ref = _demand_ref(cable, segment, vessel_id, cable_type)
        # 极端情况下ref碰撞（不同船各自草稿）由holder_vessel_id区分。
        arrived = item["payload"].get("occurred_at") if source == "vessel_task" else None
        demand = self.store.insert_demand(
            c, demand_ref, cable, segment, cable_type, start, end,
            round(end - start, 3), max(round(end - start, 3), declared), vessel_id, status,
            arrived or now(),
        )
        self.store.add_demand_link(c, demand["id"], source, reference, declared, start, end)
        acquired = False
        if vessel_id:
            acquired = self.store.acquire_lock(c, cable, segment, demand["id"], vessel_id)
            if not acquired:
                # 后到者：区段已被他船占用 → 草稿保留，待占用释放后可promote。
                self.store.set_demand_status(c, demand["id"], "draft", vessel_id)
                demand = self.store.get_demand(c, demand["id"])
        return demand, acquired

    def _record_conflict(self, c, kind: str, demand: Dict[str, Any], item: Dict[str, Any],
                         title: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        group_key = "%s|%s|%s" % (kind, demand["cable"], demand["segment"])
        open_conflict = self.store.open_conflict_for(c, group_key)
        if open_conflict is not None:
            refs = set(open_conflict["item_refs"])
            refs.add("%s/%s" % (item["source"], item["source_ref"]))
            refs.update("%s/%s" % (link["source"], link["source_ref"])
                        for link in self.store.list_demand_links(c, demand["id"]))
            merged_payload = dict(open_conflict["payload"])
            merged_payload.update(payload)
            c.execute("UPDATE sync_conflicts SET item_refs=?,payload=?,title=? WHERE id=?",
                      (json.dumps(sorted(refs), ensure_ascii=False),
                       json.dumps(merged_payload, ensure_ascii=False, sort_keys=True),
                       title, open_conflict["id"]))
            return self.store.get_conflict(c, open_conflict["id"])
        refs = {"%s/%s" % (item["source"], item["source_ref"])}
        refs.update("%s/%s" % (link["source"], link["source_ref"])
                    for link in self.store.list_demand_links(c, demand["id"]))
        payload = dict(payload)
        payload.update({"cable": demand["cable"], "segment": demand["segment"],
                        "existing_demand_ref": demand["demand_ref"]})
        return self.store.insert_conflict(c, kind, group_key, title, sorted(refs), payload)

    # ------------------------------------------------------------
    # 冲突裁决
    # ------------------------------------------------------------
    def list_conflicts(self, state: str = None) -> List[Dict[str, Any]]:
        return self.store.list_conflicts(state)

    def resolve_conflict(self, actor_id: str, conflict_id: int, resolution: Dict[str, Any]) -> Dict[str, Any]:
        decision = _text(resolution, "decision")
        if decision not in {"keep_existing", "accept_incoming"}:
            raise exc.ValidationError("decision只能是keep_existing/accept_incoming")
        with self._lock, self.store.transaction() as c:
            conflict = self.store.get_conflict(c, conflict_id)
            if conflict is None:
                raise exc.NotFound("冲突不存在")
            if conflict["state"] != "pending":
                raise exc.Conflict("冲突已裁决")
            demand_ref = conflict["payload"].get("existing_demand_ref")
            demand = self.store.find_demand_by_ref(c, demand_ref) if demand_ref else None
            if demand is not None and decision == "accept_incoming":
                if conflict["kind"] == "attribute":
                    incoming_type = conflict["payload"]["cable_type_incoming"]
                    self.store.update_demand_geometry(
                        c, demand["id"], float(demand["start_km"]), float(demand["end_km"]),
                        float(demand["union_km"]), float(demand["required_km"]), cable_type=incoming_type,
                    )
                elif conflict["kind"] == "quantity":
                    required_km = float(conflict["payload"]["declared_km_incoming"])
                    self.store.update_demand_geometry(
                        c, demand["id"], float(demand["start_km"]), float(demand["end_km"]),
                        max(float(demand["union_km"]), required_km),
                        max(float(demand["union_km"]), required_km),
                    )
            # 关联的conflict项转为applied。
            refs = conflict["item_refs"]
            for ref in refs:
                source, source_ref = ref.split("/", 1)
                row_item = self.store.get_item_by_ref(c, source, source_ref)
                if row_item is not None and row_item["state"] == "conflict":
                    self.store.update_item(c, row_item["id"], "applied",
                                           {"resolved_conflict_id": conflict_id, "decision": decision})
                    if decision == "accept_incoming" and demand is not None:
                        p = row_item["payload"]
                        s, e = float(p["start_km"]), float(p["end_km"])
                        declared = float(p.get("required_km")
                                         if p.get("required_km") is not None else round(e - s, 3))
                        if self.store.add_demand_link(c, demand["id"], source, source_ref, declared, s, e):
                            links = self.store.list_demand_links(c, demand["id"])
                            intervals = [(float(l["start_km"]), float(l["end_km"])) for l in links]
                            union_km = _union_length(intervals)
                            required_km = round(max(union_km, max(float(l["declared_km"]) for l in links)), 3)
                            self.store.update_demand_geometry(
                                c, demand["id"], min(x for x, _ in intervals), max(x for _, x in intervals),
                                union_km, required_km,
                                cable_type=conflict["payload"].get("cable_type_incoming")
                                if conflict["kind"] == "attribute" else None,
                            )
            self.store.resolve_conflict(c, conflict_id, {"decision": decision, "actor_id": actor_id,
                                                         "note": resolution.get("note", "")})
            self._recompute(c)
            return self.store.get_conflict(c, conflict_id)

    # ------------------------------------------------------------
    # 库存联动重算
    # ------------------------------------------------------------
    def stock_move(self, actor_id: str, move_ref: str, warehouse_id: str, cable_type: str, kind: str,
                   on_hand_delta: float = 0.0, in_transit_delta: float = 0.0, reason: str = "") -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            move = self.store.apply_stock_move(c, move_ref, warehouse_id, cable_type, kind,
                                               float(on_hand_delta), float(in_transit_delta), reason, actor_id)
            self._recompute(c)
        return move

    def _recompute(self, c) -> None:
        """库存/在途一变，所有未出库（planned/held）需求按到场先后重算可用量。"""
        # 先清零再分配，保证库存缩水时会收回原有分配。
        demands = self.store.demands_for_recompute(c)
        for demand in demands:
            if abs(float(demand["allocated_km"])) > EPS:
                self.store.set_demand_allocation(c, demand["id"], 0.0)

        stock_rows = c.execute("SELECT * FROM sync_warehouse_stock").fetchall()
        # 每个(仓库,型号)的可分配量 = 在库 + 在途（在途可视作可承诺量，现场看到同一数字）。
        available: Dict[Tuple[str, str], float] = {}
        for row in stock_rows:
            available[(row["warehouse_id"], row["cable_type"])] = (
                float(row["on_hand_km"]) + float(row["in_transit_km"])
            )

        for demand in demands:
            if demand["status"] == "draft":
                continue
            required = float(demand["required_km"])
            remaining = required
            for (warehouse_id, cable_type), pool in list(available.items()):
                if cable_type != demand["cable_type"] or pool <= EPS:
                    continue
                take = min(pool, remaining)
                available[(warehouse_id, cable_type)] = round(pool - take, 3)
                remaining = round(remaining - take, 3)
                if remaining <= EPS:
                    break
            allocated = round(required - remaining, 3)
            self.store.set_demand_allocation(c, demand["id"], allocated)

    # ------------------------------------------------------------
    # 出库单
    # ------------------------------------------------------------
    def issue_outbound(self, actor_id: str, order_ref: str, demand_ref: str, warehouse_id: str,
                       qty_km: float, amount: float = 0.0) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            result = self._issue_outbound(c, order_ref, demand_ref, warehouse_id, float(qty_km),
                                          float(amount), actor_id)
            return self.store.get_outbound(c, order_ref)

    def _ingest_outbound(self, c, p: Dict[str, Any], reference: str, actor_id: str) -> Dict[str, Any]:
        return self._issue_outbound(c, reference, p["demand_ref"], p["warehouse_id"],
                                    float(p["qty_km"]), float(p.get("amount", 0.0) or 0.0), actor_id)

    def _issue_outbound(self, c, order_ref: str, demand_ref: str, warehouse_id: str,
                        qty_km: float, amount: float, actor_id: str) -> Dict[str, Any]:
        existing = self.store.get_outbound(c, order_ref)
        if existing is not None:
            return {"state": "duplicate", "detail": {"order_ref": order_ref}}
        demand = self.store.find_demand_by_ref(c, demand_ref)
        if demand is None:
            raise exc.NotFound("需求%s不存在" % demand_ref)
        if demand["status"] not in {"planned", "held"}:
            raise exc.Conflict("需求当前状态%s不允许出库" % demand["status"])
        stock = self.store.stock_row(c, warehouse_id, demand["cable_type"])
        usable = (float(stock["on_hand_km"]) + float(stock["in_transit_km"])) if stock else 0.0
        if usable + EPS < qty_km:
            raise exc.Conflict("可用备缆不足：可用%s，申请%s" % (round(usable, 3), qty_km))
        order = self.store.create_outbound(c, order_ref, demand["id"], warehouse_id,
                                           demand["cable_type"], qty_km, amount, actor_id)
        # 冻结出库量：在途部分先转入库再扣，保证可用池净减qty、在库不为负。
        stock = self.store.stock_row(c, warehouse_id, demand["cable_type"])
        on_hand_now = float(stock["on_hand_km"]) if stock else 0.0
        in_transit_now = float(stock["in_transit_km"]) if stock else 0.0
        from_transit = round(min(in_transit_now, max(0.0, qty_km - on_hand_now)), 3)
        self.store.apply_stock_move(
            c, "MOVE-OUT-%s" % order_ref, warehouse_id, demand["cable_type"], "transfer_out",
            round(-qty_km + from_transit, 3), -from_transit,
            "出库单%s冻结（含在途转库%s）" % (order_ref, from_transit), actor_id,
        )
        self._recompute(c)
        return {"state": "applied", "detail": {"order_ref": order["order_ref"], "demand_ref": demand_ref}}

    def ship_outbound(self, actor_id: str, order_ref: str) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            order = self.store.get_outbound(c, order_ref)
            if order is None:
                raise exc.NotFound("出库单不存在")
            if order["state"] == "shipped":
                return order
            if order["state"] != "issued":
                raise exc.Conflict("出库单状态%s不允许发运" % order["state"])
            self.store.mark_outbound_shipped(c, order_ref)
            demand = self.store.get_demand(c, order["demand_id"])
            # 累计发运达到申报量即需求完结并释放区段，其他船可抢占。
            if demand is not None and demand["status"] in {"held", "planned"}:
                shipped_rows = c.execute(
                    "SELECT COALESCE(SUM(qty_km),0) AS total FROM sync_outbound_orders"
                    " WHERE demand_id=? AND state='shipped'", (demand["id"],)).fetchone()
                if float(shipped_rows["total"]) + EPS >= float(demand["required_km"]):
                    self.store.set_demand_status(c, demand["id"], "fulfilled")
                    self.store.release_lock(c, demand["cable"], demand["segment"], demand["holder_vessel_id"])
            self._recompute(c)
            return self.store.get_outbound(c, order_ref)

    def cancel_outbound(self, actor_id: str, order_ref: str) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            order = self.store.get_outbound(c, order_ref)
            if order is None:
                raise exc.NotFound("出库单不存在")
            if order["state"] == "cancelled":
                return order
            if order["state"] == "shipped":
                raise exc.Conflict("已发运出库单不可取消")
            self.store.cancel_outbound(c, order_ref)
            # 回补冻结量。
            self.store.apply_stock_move(c, "MOVE-BACK-%s" % order_ref, order["warehouse_id"],
                                        order["cable_type"], "transfer_in", float(order["qty_km"]), 0.0,
                                        "出库单%s取消回补" % order_ref, actor_id)
            self._recompute(c)
            return self.store.get_outbound(c, order_ref)

    # ------------------------------------------------------------
    # 结算流水与对账
    # ------------------------------------------------------------
    def _ingest_settlement(self, c, p: Dict[str, Any], reference: str, actor_id: str) -> Dict[str, Any]:
        existing = self.store.get_settlement(c, reference)
        if existing is not None:
            return {"state": "duplicate", "detail": {"entry_ref": reference}}
        order = self.store.get_outbound(c, p["order_ref"])
        if order is None:
            raise exc.ValidationError("结算流水对应的出库单%s不存在" % p["order_ref"])
        entry = self.store.create_settlement(c, reference, p["order_ref"], p["cable_type"],
                                             float(p["qty_km"]), float(p["amount"]), actor_id)
        return {"state": "applied", "detail": {"entry_ref": entry["entry_ref"]}}

    def create_settlement(self, actor_id: str, entry_ref: str, order_ref: str, cable_type: str,
                          qty_km: float, amount: float) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            existing = self.store.get_settlement(c, entry_ref)
            if existing is not None:
                return existing
            order = self.store.get_outbound(c, order_ref)
            if order is None:
                raise exc.NotFound("出库单不存在")
            entry = self.store.create_settlement(c, entry_ref, order_ref, cable_type,
                                                 float(qty_km), float(amount), actor_id)
            return entry

    def reconcile(self) -> Dict[str, Any]:
        with self.store.transaction() as c:
            orders = [dict(row) for row in c.execute("SELECT * FROM sync_outbound_orders ORDER BY id")]
            settlements = [dict(row) for row in c.execute("SELECT * FROM sync_settlement_entries ORDER BY id")]
        by_order: Dict[str, List[Dict[str, Any]]] = {}
        for entry in settlements:
            by_order.setdefault(entry["order_ref"], []).append(entry)

        matched, issues = [], []
        for order in orders:
            if order["state"] == "cancelled":
                continue
            entries = by_order.pop(order["order_ref"], [])
            if not entries:
                issues.append({"type": "missing_settlement", "order_ref": order["order_ref"],
                               "qty_km": order["qty_km"], "amount": order["amount"]})
                continue
            entry = entries[0]
            if abs(float(entry["qty_km"]) - float(order["qty_km"])) > EPS:
                issues.append({"type": "qty_mismatch", "order_ref": order["order_ref"],
                               "outbound_qty_km": order["qty_km"], "settlement_qty_km": entry["qty_km"]})
            elif abs(float(entry["amount"]) - float(order["amount"])) > EPS:
                issues.append({"type": "amount_mismatch", "order_ref": order["order_ref"],
                               "outbound_amount": order["amount"], "settlement_amount": entry["amount"]})
            else:
                matched.append(order["order_ref"])
        for order_ref, entries in by_order.items():
            for entry in entries:
                issues.append({"type": "settlement_only", "order_ref": order_ref,
                               "entry_ref": entry["entry_ref"], "amount": entry["amount"]})
        return {
            "matched_count": len(matched),
            "issue_count": len(issues),
            "balanced": not issues,
            "matched": matched,
            "issues": issues,
        }

    # ------------------------------------------------------------
    # 草稿 promote / 查询
    # ------------------------------------------------------------
    def promote_draft(self, actor_id: str, demand_ref: str) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            demand = self.store.find_demand_by_ref(c, demand_ref)
            if demand is None:
                raise exc.NotFound("需求不存在")
            if demand["status"] != "draft":
                raise exc.Conflict("仅草稿需求可尝试占用")
            if not demand["holder_vessel_id"]:
                raise exc.ValidationError("草稿需求缺少占用船舶")
            acquired = self.store.acquire_lock(c, demand["cable"], demand["segment"],
                                               demand["id"], demand["holder_vessel_id"])
            if not acquired:
                raise exc.Conflict("区段仍被占用，草稿保留")
            self.store.set_demand_status(c, demand["id"], "held", demand["holder_vessel_id"])
            self._recompute(c)
            return self.store.get_demand(c, demand["id"])

    def release_segment(self, actor_id: str, cable: str, segment: str) -> Dict[str, Any]:
        with self._lock, self.store.transaction() as c:
            self.store.release_lock(c, cable, segment)
            self._recompute(c)
        return {"cable": cable, "segment": segment, "released": True}

    def get_demand(self, demand_ref: str, with_links: bool = True) -> Dict[str, Any]:
        with self.store.transaction() as c:
            demand = self.store.find_demand_by_ref(c, demand_ref)
            if demand is None:
                raise exc.NotFound("需求不存在")
            if with_links:
                demand["links"] = self.store.list_demand_links(c, demand["id"])
            lock = self.store.lock_for(c, demand["cable"], demand["segment"])
            demand["lock"] = lock
            return demand

    def list_demands(self, cable: str = None, segment: str = None, status: str = None) -> List[Dict[str, Any]]:
        return self.store.list_demands(cable, segment, status)

    def availability_view(self, cable: str = None, segment: str = None) -> Dict[str, Any]:
        """现场与岸端共用：同一批在库/在途/已占/可用量，以及同一缺口。"""
        with self.store.transaction() as c:
            stock_rows = c.execute("SELECT * FROM sync_warehouse_stock").fetchall()
            clauses, params = [], []
            if cable:
                clauses.append("cable=?")
                params.append(cable)
            if segment:
                clauses.append("segment=?")
                params.append(segment)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            demand_rows = c.execute(
                "SELECT * FROM sync_demands%s ORDER BY cable,segment,arrived_at,id" % where, params).fetchall()
            locks = c.execute("SELECT * FROM sync_segment_locks").fetchall()
            stock_rows = [dict(row) for row in stock_rows]
            demand_rows = [dict(row) for row in demand_rows]
            locks = [dict(row) for row in locks]

        by_type: Dict[str, Dict[str, float]] = {}
        for row in stock_rows:
            bucket = by_type.setdefault(row["cable_type"], {"on_hand_km": 0.0, "in_transit_km": 0.0})
            bucket["on_hand_km"] += float(row["on_hand_km"])
            bucket["in_transit_km"] += float(row["in_transit_km"])
        for bucket in by_type.values():
            bucket["on_hand_km"] = round(bucket["on_hand_km"], 3)
            bucket["in_transit_km"] = round(bucket["in_transit_km"], 3)

        lock_index = {(row["cable"], row["segment"]): dict(row) for row in locks}
        demand_items = []
        gap_by_type: Dict[str, float] = {}
        for row in demand_rows:
            demand = dict(row)
            required, allocated = float(demand["required_km"]), float(demand["allocated_km"])
            gap = round(max(0.0, required - allocated), 3)
            if demand["status"] != "draft":
                gap_by_type[demand["cable_type"]] = round(
                    gap_by_type.get(demand["cable_type"], 0.0) + gap, 3)
            demand["gap_km"] = gap
            demand["segment_lock"] = lock_index.get((demand["cable"], demand["segment"]))
            demand_items.append(demand)

        for cable_type, bucket in by_type.items():
            bucket["allocated_km"] = round(
                sum(float(d["allocated_km"]) for d in demand_items
                    if d["cable_type"] == cable_type and d["status"] != "draft"), 3)
            bucket["available_km"] = round(bucket["on_hand_km"] + bucket["in_transit_km"], 3)
            bucket["gap_km"] = gap_by_type.get(cable_type, 0.0)

        return {
            "stock_by_type": by_type,
            "demands": demand_items,
            "segment_locks": [dict(row) for row in locks],
        }

    # ------------------------------------------------------------
    # 船机 / 备缆 / 库存只读
    # ------------------------------------------------------------
    def list_vessels(self) -> List[Dict[str, Any]]:
        return self.store.list_vessels()

    def list_spares(self) -> List[Dict[str, Any]]:
        return self.store.list_spares()

    def list_stock(self, warehouse_id: str = None) -> List[Dict[str, Any]]:
        return self.store.list_stock(warehouse_id)

    def list_moves(self, warehouse_id: str = None) -> List[Dict[str, Any]]:
        return self.store.list_moves(warehouse_id)

    def list_outbound(self, demand_ref: str = None) -> List[Dict[str, Any]]:
        if demand_ref is None:
            return self.store.list_outbound()
        with self.store.transaction() as c:
            demand = self.store.find_demand_by_ref(c, demand_ref)
            if demand is None:
                raise exc.NotFound("需求不存在")
            return [dict(row) for row in c.execute(
                "SELECT * FROM sync_outbound_orders WHERE demand_id=? ORDER BY id", (demand["id"],))]

    def health(self) -> bool:
        return self.store.health()
