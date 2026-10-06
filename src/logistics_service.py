"""岸端工单、船端离线任务、备缆仓库与出库单的用例编排。

核心约定：
- 批次先完整落库再合并，写入失败可从完整批次恢复；
- 台账流水、出库单、结算流水全部带幂等键，重复回传不洗牌扣减；
- 可用量与缺口由同一份台账实时算出，岸端与现场看到同一数字。
"""
from typing import Any, Dict, List, Optional, Tuple

from .domain import Actor, Conflict, NotFound, PermissionDenied, ValidationError, number, text
from .logistics import (
    CONFLICT_PENDING,
    EXTRA_KNOWN_ROLES,
    PLAN_CLOSED,
    PLAN_ISSUED,
    PLAN_OPEN,
    RESOLVE_ROLES,
    RESUME_ROLES,
    SETTLE_ROLES,
    SUBMIT_ROLES,
    WAREHOUSE_ROLES,
    LogisticsRules,
)
from .repository import Repository
from .rules import DomainRules


class LogisticsService:
    def __init__(self, repository: Repository, rules: LogisticsRules, domain_rules: DomainRules = None) -> None:
        self.repository = repository
        self.rules = rules
        self.domain_rules = domain_rules or DomainRules()

    # ---------- 身份与权限 ----------

    def _actor(self, actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        if not (self.domain_rules.known_role(actor.role) or actor.role in EXTRA_KNOWN_ROLES):
            raise PermissionDenied("角色无权访问该服务")
        return actor

    @staticmethod
    def _require(actor: Actor, roles: set, message: str) -> None:
        if actor.role != "admin" and actor.role not in roles:
            raise PermissionDenied(message)

    # ---------- 批次提交与恢复 ----------

    def submit_batch(self, actor: Actor, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """提交可续作批次：先完整落库再合并，重复回传返回已存结果。"""
        actor = self._actor(actor)
        data = self.rules.validate_batch_payload(payload)
        self._require(actor, SUBMIT_ROLES[data["source"]], "角色无权提交%s批次" % ("岸端" if data["source"] == "shore" else "船端"))
        batch = self.repository.find_batch(data["batch_key"])
        created = False
        if batch is None:
            batch = self.repository.insert_batch(data["batch_key"], data["source"], data["vessel_name"], data["items"], actor.user_id)
            if batch is None:  # 并发同键：以先落库者为准
                batch = self.repository.get_batch(data["batch_key"])
            else:
                created = True
        if batch["state"] == "draft" and batch["items"] != data["items"]:
            batch = self.repository.replace_batch_items(data["batch_key"], data["items"], actor.user_id)
        elif batch["state"] in ("merged", "conflicted"):
            return self._batch_view(batch, deduplicated=True), 200
        self._run_merge(batch, actor.user_id)
        return self._batch_view(self.repository.get_batch(data["batch_key"]), deduplicated=not created), (201 if created else 200)

    def resume_batch(self, actor: Actor, batch_key: str) -> Dict[str, Any]:
        """从完整批次恢复：failed/submitted/draft/conflicted 都可重新合并。"""
        actor = self._actor(actor)
        self._require(actor, RESUME_ROLES, "角色无权恢复批次")
        batch = self.repository.get_batch(text({"batch_key": batch_key}, "batch_key"))
        if batch["state"] == "merged":
            return self._batch_view(batch, deduplicated=True)
        self._run_merge(batch, actor.user_id)
        return self._batch_view(self.repository.get_batch(batch["batch_key"]), deduplicated=False)

    def get_batch(self, actor: Actor, batch_key: str) -> Dict[str, Any]:
        self._actor(actor)
        return self._batch_view(self.repository.get_batch(batch_key), deduplicated=False)

    def list_batches(self, actor: Actor, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_batches(state=state, limit=limit)

    def _run_merge(self, batch: Dict[str, Any], actor_id: str) -> None:
        try:
            with self.repository.transaction() as conn:
                self._merge_batch(conn, batch, actor_id)
        except Exception as exc:  # 写入失败：完整批次已留库，标记后可恢复
            message = str(exc) or exc.__class__.__name__
            self.repository.mark_batch_failed(batch["batch_key"], message, actor_id)

    def _merge_batch(self, conn: Any, batch: Dict[str, Any], actor_id: str) -> None:
        """按区段与里程合并：同区段里程取并集只算一次；冲突留待裁决。"""
        rules = self.rules
        batch_key = batch["batch_key"]
        source = batch["source"]
        vessel_name = batch["vessel_name"]
        groups: Dict[tuple, List[Dict[str, Any]]] = {}
        for item in batch["items"]:
            key = (item["cable"], item["segment"], item["warehouse"], item["cable_type"])
            groups.setdefault(key, []).append(item)

        plans_out: List[Dict[str, Any]] = []
        conflicts_out: List[Dict[str, Any]] = []
        held = 0
        for (cable, segment, warehouse, cable_type), group_items in sorted(groups.items()):
            ranges = rules.union_ranges([(i["start_km"], i["end_km"]) for i in group_items])
            planned = round(sum(i["spare_planned_km"] for i in group_items), 2)
            if self.repository.tx_get_inventory(conn, warehouse, cable_type) is None:
                raise NotFound("仓库%s缺少%s库存主数据" % (warehouse, cable_type))

            if source == "vessel":
                blocking = [
                    claim for claim in self.repository.tx_active_claims(conn, cable, segment)
                    if claim["vessel_name"] != vessel_name and claim["batch_key"] != batch_key
                    and rules.ranges_overlap((claim["start_km"], claim["end_km"]), ranges)
                ]
                if blocking:
                    # 先到者占用，后到者留草稿并登记冲突
                    held += 1
                    holder = blocking[0]
                    details = {
                        "holder_batch_key": holder["batch_key"],
                        "holder_vessel": holder["vessel_name"],
                        "warehouse": warehouse,
                        "cable_type": cable_type,
                        "ranges": ranges,
                        "message": "区段%s/%s已由%s占用" % (cable, segment, holder["vessel_name"]),
                    }
                    conflict_key = "occ:%s:%s:%s" % (batch_key, cable, segment)
                    self.repository.tx_insert_conflict(conn, conflict_key, "segment_occupied", batch_key, cable, segment, details)
                    conflicts_out.append({"conflict_key": conflict_key, "kind": "segment_occupied", "cable": cable, "segment": segment, "details": details})
                    continue
                for index, (start, end) in enumerate(ranges):
                    claim_key = "%s:%s:%s:%d" % (batch_key, cable, segment, index)
                    self.repository.tx_insert_claim(conn, claim_key, batch_key, vessel_name, cable, segment, start, end)

            contributor = {"batch_key": batch_key, "source": source, "vessel_name": vessel_name, "ranges": ranges, "spare_planned_km": planned}
            plan = self.repository.tx_get_plan(conn, cable, segment, warehouse, cable_type)
            if plan is None or plan["state"] == PLAN_CLOSED:
                contributors = [contributor]
            else:
                contributors = [c for c in plan["contributors"] if c["batch_key"] != batch_key]
                contributors.append(contributor)
            merged_ranges = rules.union_ranges([r for c in contributors for r in c["ranges"]])
            required = rules.required_km(merged_ranges)
            if plan is None:
                plan = self.repository.tx_insert_plan(conn, cable, segment, warehouse, cable_type, merged_ranges, contributors, required)
            elif plan["state"] == PLAN_CLOSED:
                plan = self.repository.tx_update_plan(conn, plan["id"], merged_ranges, contributors, required, PLAN_OPEN)
            else:
                issued = float(plan["issued_km"])
                override = plan.get("required_override_km")
                effective = float(override) if override is not None else required
                state = PLAN_OPEN if effective - issued > 0.005 else PLAN_ISSUED
                plan = self.repository.tx_update_plan(conn, plan["id"], merged_ranges, contributors, required, state)

            mismatch = rules.detect_quantity_mismatch(contributors)
            if mismatch:
                details = dict(mismatch)
                details.update({"warehouse": warehouse, "cable_type": cable_type, "message": "岸端与船端申报备缆密度偏差超过25%"})
                conflict_key = "qm:%s:%s:%s:%s:%s" % (batch_key, cable, segment, warehouse, cable_type)
                self.repository.tx_insert_conflict(conn, conflict_key, "quantity_mismatch", batch_key, cable, segment, details)
                conflicts_out.append({"conflict_key": conflict_key, "kind": "quantity_mismatch", "cable": cable, "segment": segment, "details": details})
            plans_out.append(self._plan_view(plan))

        total = len(groups)
        pending = self.repository.tx_pending_conflicts_for_batch(conn, batch_key)
        if held == total:
            state = "draft"  # 后到者留草稿
        elif held or pending:
            state = "conflicted"
        else:
            state = "merged"
        availability = {}
        for key in {(p["warehouse"], p["cable_type"]) for p in plans_out}:
            availability["%s|%s" % key] = self._availability_view(conn, key[0], key[1])
        result = {"plans": plans_out, "conflicts": conflicts_out, "held_groups": held, "availability": availability}
        self.repository.tx_update_batch(conn, batch_key, state, result, actor_id)

    # ---------- 库存：入库、在途、可用量 ----------

    def receipt(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        """备缆入库；from_in_transit表示在途到库。在途量一变，未出库任务就重算。"""
        actor = self._actor(actor)
        self._require(actor, WAREHOUSE_ROLES, "角色无权登记入库")
        data = self.rules.validate_receipt(payload)
        with self.repository.transaction() as conn:
            self.repository.tx_ensure_inventory(conn, data["warehouse"], data["cable_type"])
            delta_transit = -data["quantity_km"] if data["from_in_transit"] else 0.0
            applied = self.repository.tx_apply_move(
                conn, data["move_key"], data["warehouse"], data["cable_type"], "receipt",
                data["quantity_km"], delta_transit, "manual", data["move_key"], actor.user_id,
            )
            if applied:
                self.repository.tx_adjust_inventory(conn, data["warehouse"], data["cable_type"], data["quantity_km"], delta_transit)
            view = self._availability_view(conn, data["warehouse"], data["cable_type"])
        return {"inventory": view, "duplicated": not applied, "recalculated": view["open_tasks"]}

    def update_in_transit(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        """在途量变更（乐观并发+幂等），随后重算所有未出库任务。"""
        actor = self._actor(actor)
        self._require(actor, WAREHOUSE_ROLES, "角色无权调整在途量")
        data = self.rules.validate_in_transit(payload)
        with self.repository.transaction() as conn:
            applied = False
            if self.repository.tx_find_move(conn, data["move_key"]) is None:
                inventory = self.repository.tx_get_inventory(conn, data["warehouse"], data["cable_type"])
                if inventory is None:
                    raise NotFound("仓库%s缺少%s库存主数据" % (data["warehouse"], data["cable_type"]))
                if int(inventory["version"]) != data["expected_version"]:
                    raise Conflict("版本冲突，请刷新后重试")
                delta = round(data["in_transit_km"] - float(inventory["in_transit_km"]), 2)
                applied = self.repository.tx_apply_move(
                    conn, data["move_key"], data["warehouse"], data["cable_type"], "in_transit",
                    0.0, delta, "manual", data["move_key"], actor.user_id,
                )
                if applied:
                    self.repository.tx_adjust_inventory(conn, data["warehouse"], data["cable_type"], 0.0, delta)
            view = self._availability_view(conn, data["warehouse"], data["cable_type"])
        return {"inventory": view, "duplicated": not applied, "recalculated": view["open_tasks"]}

    def availability(self, actor: Actor, warehouse: str, cable_type: str) -> Dict[str, Any]:
        """现场与岸端共用：同一可用量和缺口。"""
        self._actor(actor)
        warehouse = text({"warehouse": warehouse}, "warehouse")
        cable_type = text({"cable_type": cable_type}, "cable_type")
        with self.repository.transaction() as conn:
            return self._availability_view(conn, warehouse, cable_type)

    def list_moves(self, actor: Actor, warehouse: Optional[str] = None, cable_type: Optional[str] = None) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_inventory_moves(warehouse=warehouse, cable_type=cable_type)

    def _availability_view(self, conn: Any, warehouse: str, cable_type: str) -> Dict[str, Any]:
        inventory = self.repository.tx_get_inventory(conn, warehouse, cable_type)
        if inventory is None:
            raise NotFound("仓库%s缺少%s库存主数据" % (warehouse, cable_type))
        plans = self.repository.tx_open_plans(conn, warehouse, cable_type)
        view = {"warehouse": warehouse, "cable_type": cable_type, "inventory_version": int(inventory["version"])}
        view.update(self.rules.availability(inventory, plans))
        remaining = float(inventory["on_hand_km"])
        tasks = []
        for plan in plans:
            need = self.rules.plan_remaining(plan)
            covered = remaining + 0.005 >= need
            tasks.append({
                "plan_id": plan["id"],
                "cable": plan["cable"],
                "segment": plan["segment"],
                "required_km": need,
                "covered": covered,
                "shortfall_km": 0.0 if covered else round(need - remaining, 2),
            })
            remaining = round(remaining - need, 2)
        view["open_tasks"] = tasks
        return view

    # ---------- 出库与结算 ----------

    def issue_outbound(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        """出库：扣在库、核销未出库任务余量；order_key幂等，重复回传不重复扣减。"""
        actor = self._actor(actor)
        self._require(actor, WAREHOUSE_ROLES, "角色无权办理出库")
        data = self.rules.validate_outbound(payload)
        existing = self.repository.find_outbound(data["order_key"])
        if existing is not None:
            return {"order": existing, "duplicated": True}
        with self.repository.transaction() as conn:
            plan = self.repository.tx_get_plan_by_id(conn, data["plan_id"])
            if plan is None:
                raise NotFound("区段计划不存在")
            if plan["state"] != PLAN_OPEN:
                raise Conflict("该批任务已出库或关闭")
            remaining = self.rules.plan_remaining(plan)
            if data["quantity_km"] > remaining + 0.005:
                raise ValidationError("出库数量超过区段未出库需求%.2f公里" % remaining)
            inventory = self.repository.tx_get_inventory(conn, plan["warehouse"], plan["cable_type"])
            if inventory is None:
                raise NotFound("仓库%s缺少%s库存主数据" % (plan["warehouse"], plan["cable_type"]))
            if float(inventory["on_hand_km"]) + 0.005 < data["quantity_km"]:
                raise ValidationError("在库备缆不足：缺口%.2f公里" % round(data["quantity_km"] - float(inventory["on_hand_km"]), 2))
            self.repository.tx_apply_move(
                conn, "outbound:%s" % data["order_key"], plan["warehouse"], plan["cable_type"], "issue",
                -data["quantity_km"], 0.0, "outbound", data["order_key"], actor.user_id,
            )
            self.repository.tx_adjust_inventory(conn, plan["warehouse"], plan["cable_type"], -data["quantity_km"], 0.0)
            order = self.repository.tx_insert_outbound(
                conn, data["order_key"], plan["id"], plan["warehouse"], plan["cable_type"],
                data["quantity_km"], data["vessel_name"], plan["segment"], actor.user_id,
            )
            issued = round(float(plan["issued_km"]) + data["quantity_km"], 2)
            state = PLAN_ISSUED if self.rules.effective_required(plan) - issued <= 0.005 else PLAN_OPEN
            self.repository.tx_set_plan_issued(conn, plan["id"], issued, state)
            view = self._availability_view(conn, plan["warehouse"], plan["cable_type"])
        return {"order": order, "duplicated": False, "availability": view}

    def list_outbound(self, actor: Actor) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_outbound()

    def record_settlement(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        """登记结算流水；entry_key幂等。"""
        actor = self._actor(actor)
        self._require(actor, SETTLE_ROLES, "角色无权登记结算")
        data = self.rules.validate_settlement(payload)
        if self.repository.find_outbound(data["order_key"]) is None:
            raise NotFound("出库单不存在")
        entry = self.repository.insert_settlement(data["entry_key"], data["order_key"], data["quantity_km"], data["amount"], actor.user_id)
        if entry is None:
            return {"entry": self.repository.find_settlement(data["entry_key"]), "duplicated": True}
        return {"entry": entry, "duplicated": False}

    def list_settlements(self, actor: Actor, order_key: Optional[str] = None) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_settlements(order_key=order_key)

    def reconcile(self, actor: Actor, warehouse: Optional[str] = None) -> Dict[str, Any]:
        """出库单与结算流水对账。"""
        self._actor(actor)
        rows = []
        summary = {"matched": 0, "partial": 0, "unsettled": 0, "over_settled": 0}
        for row in self.repository.reconcile_rows(warehouse=warehouse):
            issued = round(float(row["issued_km"]), 2)
            settled = round(float(row["settled_km"]), 2)
            status = self.rules.reconcile_status(issued, settled)
            summary[status] += 1
            rows.append({
                "order_key": row["order_key"],
                "plan_id": row["plan_id"],
                "warehouse": row["warehouse"],
                "cable_type": row["cable_type"],
                "segment": row["segment"],
                "vessel_name": row["vessel_name"],
                "issued_km": issued,
                "settled_km": settled,
                "settled_amount": round(float(row["settled_amount"]), 2),
                "settlement_entries": int(row["settlement_entries"]),
                "status": status,
            })
        return {"rows": rows, "summary": summary}

    # ---------- 冲突裁决 ----------

    def list_conflicts(self, actor: Actor, state: Optional[str] = None) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_conflicts(state=state)

    def resolve_conflict(self, actor: Actor, conflict_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._require(actor, RESOLVE_ROLES, "角色无权裁决冲突")
        resolution = text({"resolution": (payload or {}).get("resolution", "")}, "resolution")
        with self.repository.transaction() as conn:
            conflict = self.repository.tx_get_conflict(conn, conflict_id)
            if conflict is None:
                raise NotFound("冲突不存在")
            if conflict["state"] != CONFLICT_PENDING:
                raise Conflict("冲突已裁决")
            details = conflict["details"]
            if conflict["kind"] == "segment_occupied":
                if resolution not in ("release_holder", "keep_holder"):
                    raise ValidationError("resolution只能是release_holder/keep_holder")
                if resolution == "release_holder":
                    holder = details["holder_batch_key"]
                    self.repository.tx_release_claims(conn, holder, conflict["cable"], conflict["segment"])
                    plan = self.repository.tx_get_plan(conn, conflict["cable"], conflict["segment"], details["warehouse"], details["cable_type"])
                    if plan is not None and plan["state"] != PLAN_CLOSED:
                        contributors = [c for c in plan["contributors"] if c["batch_key"] != holder]
                        if contributors:
                            ranges = self.rules.union_ranges([r for c in contributors for r in c["ranges"]])
                            required = self.rules.required_km(ranges)
                            state = PLAN_OPEN if required - float(plan["issued_km"]) > 0.005 else plan["state"]
                            self.repository.tx_update_plan(conn, plan["id"], ranges, contributors, required, state)
                        else:
                            self.repository.tx_update_plan(conn, plan["id"], [], [], 0.0, PLAN_CLOSED)
            elif conflict["kind"] == "quantity_mismatch":
                if resolution not in ("use_shore", "use_vessel", "keep_merged"):
                    raise ValidationError("resolution只能是use_shore/use_vessel/keep_merged")
                if payload and payload.get("override_required_km") is not None:
                    override = number(payload, "override_required_km", 0)
                    plan = self.repository.tx_get_plan(conn, conflict["cable"], conflict["segment"], details["warehouse"], details["cable_type"])
                    if plan is None:
                        raise NotFound("区段计划不存在")
                    self.repository.tx_set_plan_override(conn, plan["id"], override)
            else:
                raise ValidationError("未知冲突类型")
            self.repository.tx_resolve_conflict(conn, conflict_id, resolution, actor.user_id)
        return self.repository.get_conflict(conflict_id)

    # ---------- 视图 ----------

    @staticmethod
    def _batch_view(batch: Dict[str, Any], deduplicated: bool) -> Dict[str, Any]:
        view = dict(batch)
        view["deduplicated"] = deduplicated
        return view

    @staticmethod
    def _plan_view(plan: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "plan_id": plan["id"],
            "cable": plan["cable"],
            "segment": plan["segment"],
            "warehouse": plan["warehouse"],
            "cable_type": plan["cable_type"],
            "ranges": plan["ranges"],
            "required_km": plan["required_km"],
            "required_override_km": plan["required_override_km"],
            "issued_km": plan["issued_km"],
            "state": plan["state"],
        }
