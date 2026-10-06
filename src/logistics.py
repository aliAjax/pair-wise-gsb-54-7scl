"""岸端工单、船端离线任务、备缆仓库与出库单的批次规则。

本模块只放纯计算与校验：批次负载、里程并集、区段占用、数量口径冲突、
可用量与对账状态。库存扣减全部通过带幂等键的台账流水完成，重复回传不会重复扣减。
"""
import re
from typing import Any, Dict, List, Optional, Tuple

from .domain import ValidationError, boolean, choice, integer, number, optional_text, text, text_list


SPARE_MARGIN = 1.05          # 备缆余量系数，与抢修工单规则一致
MISMATCH_TOLERANCE = 0.25    # 岸端与船端申报密度（公里备缆/公里里程）偏差阈值
EPSILON = 0.005              # 公里数比较精度
MAX_BATCH_ITEMS = 50
KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,80}$")

BATCH_SOURCES = ("shore", "vessel")
PLAN_OPEN = "open"
PLAN_ISSUED = "issued"
PLAN_CLOSED = "closed"
CONFLICT_PENDING = "pending"
CONFLICT_RESOLVED = "resolved"

SUBMIT_ROLES = {"shore": {"noc_operator", "repair_manager"}, "vessel": {"vessel_master"}}
RESUME_ROLES = {"noc_operator", "repair_manager", "vessel_master"}
WAREHOUSE_ROLES = {"warehouse_keeper"}
SETTLE_ROLES = {"settlement_clerk"}
RESOLVE_ROLES = {"repair_manager"}
EXTRA_KNOWN_ROLES = {"warehouse_keeper", "settlement_clerk"}


def _key(data: Dict[str, Any], field: str) -> str:
    value = text(data, field)
    if not KEY_RE.match(value):
        raise ValidationError("%s只能包含字母数字._:-且长度1-80" % field)
    return value


class LogisticsRules:
    """批次合并与库存计算的纯规则，不接触数据库。"""

    # ---------- 输入校验 ----------

    def validate_batch_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        batch_key = _key(p, "batch_key")
        source = choice(p, "source", list(BATCH_SOURCES))
        vessel_name = optional_text(p, "vessel_name")
        if source == "vessel" and not vessel_name:
            raise ValidationError("船端批次必须填写vessel_name")
        items = p.get("items")
        if not isinstance(items, list) or not 1 <= len(items) <= MAX_BATCH_ITEMS:
            raise ValidationError("items必须是1-%s项的列表" % MAX_BATCH_ITEMS)
        normalized = [self.validate_item(item) for item in items]
        keys = [item["item_key"] for item in normalized]
        if len(set(keys)) != len(keys):
            raise ValidationError("item_key不能重复")
        return {"batch_key": batch_key, "source": source, "vessel_name": vessel_name, "items": normalized}

    def validate_item(self, item: Any) -> Dict[str, Any]:
        if not isinstance(item, dict):
            raise ValidationError("items必须是对象列表")
        i = dict(item)
        out = {
            "item_key": _key(i, "item_key"),
            "cable": text(i, "cable"),
            "segment": text(i, "segment"),
            "warehouse": text(i, "warehouse"),
            "cable_type": text(i, "cable_type"),
            "start_km": number(i, "start_km", 0),
            "end_km": number(i, "end_km", 0),
            "spare_planned_km": number(i, "spare_planned_km", 0),
            "machinery": text_list(i, "machinery"),
            "note": optional_text(i, "note"),
        }
        if out["end_km"] <= out["start_km"]:
            raise ValidationError("结束里程必须大于开始里程")
        return out

    def validate_receipt(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        return {
            "move_key": _key(p, "move_key"),
            "warehouse": text(p, "warehouse"),
            "cable_type": text(p, "cable_type"),
            "quantity_km": number(p, "quantity_km", EPSILON),
            "from_in_transit": boolean(p, "from_in_transit"),
        }

    def validate_in_transit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        return {
            "move_key": _key(p, "move_key"),
            "warehouse": text(p, "warehouse"),
            "cable_type": text(p, "cable_type"),
            "in_transit_km": number(p, "in_transit_km", 0),
            "expected_version": integer(p, "expected_version", 1),
        }

    def validate_outbound(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        return {
            "order_key": _key(p, "order_key"),
            "plan_id": integer(p, "plan_id", 1),
            "quantity_km": number(p, "quantity_km", EPSILON),
            "vessel_name": optional_text(p, "vessel_name"),
        }

    def validate_settlement(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload or {})
        return {
            "entry_key": _key(p, "entry_key"),
            "order_key": _key(p, "order_key"),
            "quantity_km": number(p, "quantity_km", EPSILON),
            "amount": number(p, "amount", 0),
        }

    # ---------- 里程并集与需求 ----------

    @staticmethod
    def union_ranges(ranges: List[Tuple[float, float]]) -> List[List[float]]:
        """里程区间并集：同一区段无论报多少次，只按覆盖里程算一次。"""
        merged: List[List[float]] = []
        for start, end in sorted((float(s), float(e)) for s, e in ranges):
            if merged and start <= merged[-1][1] + EPSILON:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        return merged

    @staticmethod
    def ranges_overlap(a: Tuple[float, float], b: Any) -> bool:
        """a为单一区间(start,end)，b为单一区间或区间列表。"""
        candidates = b if b and isinstance(b[0], (list, tuple)) else [b]
        return any(float(a[0]) < float(e) - EPSILON and float(a[1]) > float(s) + EPSILON for s, e in candidates)

    def required_km(self, ranges: List[List[float]]) -> float:
        return round(sum(end - start for start, end in ranges) * SPARE_MARGIN, 2)

    @staticmethod
    def effective_required(plan: Dict[str, Any]) -> float:
        override = plan.get("required_override_km")
        return float(override) if override is not None else float(plan["required_km"])

    @staticmethod
    def plan_remaining(plan: Dict[str, Any]) -> float:
        """计划未出库余量：在途量变化后需要重算的就是这部分。"""
        return round(max(0.0, LogisticsRules.effective_required(plan) - float(plan.get("issued_km", 0))), 2)

    # ---------- 冲突与对账 ----------

    @staticmethod
    def detect_quantity_mismatch(contributors: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """岸端与船端对同一区段的申报密度（备缆公里/里程公里）偏差超阈值时产生口径冲突。

        按密度而非总量比较：船端只覆盖区段子段时总量天然更小，不算冲突。
        """
        def density(source: str) -> Optional[Tuple[float, float]]:
            selected = [c for c in contributors if c.get("source") == source]
            planned = sum(float(c.get("spare_planned_km", 0)) for c in selected)
            length = sum(e - s for c in selected for s, e in LogisticsRules.union_ranges([tuple(r) for r in c.get("ranges", [])]))
            if planned <= 0 or length <= 0:
                return None
            return round(planned, 2), round(planned / length, 3)

        shore = density("shore")
        vessel = density("vessel")
        if shore is None or vessel is None:
            return None
        low = min(shore[1], vessel[1])
        if (max(shore[1], vessel[1]) - low) / low > MISMATCH_TOLERANCE:
            return {"shore_planned_km": shore[0], "vessel_planned_km": vessel[0], "shore_density": shore[1], "vessel_density": vessel[1]}
        return None

    @staticmethod
    def reconcile_status(issued_km: float, settled_km: float) -> str:
        if settled_km <= EPSILON:
            return "unsettled"
        if settled_km < issued_km - EPSILON:
            return "partial"
        if settled_km > issued_km + EPSILON:
            return "over_settled"
        return "matched"

    # ---------- 可用量 ----------

    def availability(self, inventory: Dict[str, Any], open_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """现场与岸端共用同一份台账：可用量与缺口由同一函数算出。"""
        on_hand = round(float(inventory["on_hand_km"]), 2)
        in_transit = round(float(inventory["in_transit_km"]), 2)
        reserved = round(sum(self.plan_remaining(plan) for plan in open_plans), 2)
        return {
            "on_hand_km": on_hand,
            "in_transit_km": in_transit,
            "reserved_km": reserved,
            "available_km": round(on_hand - reserved, 2),
            "projected_available_km": round(on_hand + in_transit - reserved, 2),
            "gap_km": round(max(0.0, reserved - on_hand), 2),
            "projected_gap_km": round(max(0.0, reserved - on_hand - in_transit), 2),
        }
