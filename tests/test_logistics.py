import tempfile
import unittest
from pathlib import Path

from app import build_services
from src.domain import Actor, Conflict, PermissionDenied, ValidationError


SHORE = Actor("noc-1", "noc_operator")
MANAGER = Actor("rm-1", "repair_manager")
VESSEL_A = Actor("vm-a", "vessel_master")
VESSEL_B = Actor("vm-b", "vessel_master")
KEEPER = Actor("wh-1", "warehouse_keeper")
CLERK = Actor("fin-1", "settlement_clerk")


def shore_batch(key, segment="S3", start=120.0, end=135.0, planned=16.0, warehouse="WH-East"):
    return {"batch_key": key, "source": "shore", "items": [
        {"item_key": "i1", "cable": "SEA-1", "segment": segment, "start_km": start, "end_km": end,
         "warehouse": warehouse, "cable_type": "LW-24", "spare_planned_km": planned}]}


def vessel_batch(key, vessel, segment="S3", start=120.0, end=135.0, planned=16.0, warehouse="WH-East", machinery=None):
    return {"batch_key": key, "source": "vessel", "vessel_name": vessel, "items": [
        {"item_key": "i1", "cable": "SEA-1", "segment": segment, "start_km": start, "end_km": end,
         "warehouse": warehouse, "cable_type": "LW-24", "spare_planned_km": planned,
         "machinery": machinery if machinery is not None else ["grapple", "rov"]}]}


class LogisticsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service, self.logistics = build_services(str(Path(self.temp.name) / "test.db"))
        self.logistics.receipt(KEEPER, {"move_key": "rcpt-init", "warehouse": "WH-East", "cable_type": "LW-24", "quantity_km": 40.0})

    def tearDown(self):
        self.temp.cleanup()

    def availability(self):
        return self.logistics.availability(SHORE, "WH-East", "LW-24")

    def test_merge_dedupes_same_segment(self):
        # 岸端工单与船端离线任务覆盖同一里程：回连合并后只算一次
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        view, _ = self.logistics.submit_batch(VESSEL_A, vessel_batch("ves-1", "CS-1"))
        self.assertEqual(view["state"], "merged")
        avail = self.availability()
        self.assertEqual(avail["reserved_km"], 15.75)  # 15km*1.05，而非两套记录相加的31.5
        self.assertEqual(avail["available_km"], 24.25)
        self.assertEqual(avail["gap_km"], 0.0)

    def test_duplicate_submission_does_not_double_count(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        again, status = self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        self.assertEqual(status, 200)
        self.assertTrue(again["deduplicated"])
        self.assertEqual(self.availability()["reserved_km"], 15.75)

    def test_first_vessel_occupies_second_stays_draft(self):
        first, _ = self.logistics.submit_batch(VESSEL_A, vessel_batch("ves-a", "CS-A"))
        self.assertEqual(first["state"], "merged")
        held, _ = self.logistics.submit_batch(VESSEL_B, vessel_batch("ves-b", "CS-B", start=125.0, end=130.0))
        self.assertEqual(held["state"], "draft")  # 后到者留草稿
        conflicts = self.logistics.list_conflicts(MANAGER, state="pending")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["kind"], "segment_occupied")
        # 裁决释放先到者，后到者恢复合并
        self.logistics.resolve_conflict(MANAGER, conflicts[0]["id"], {"resolution": "release_holder"})
        merged = self.logistics.resume_batch(VESSEL_B, "ves-b")
        self.assertEqual(merged["state"], "merged")
        avail = self.availability()
        self.assertEqual(avail["reserved_km"], 5.25)  # 5km*1.05，占用释放后仍是只有一份

    def test_held_batch_can_be_rewritten_and_resubmitted(self):
        self.logistics.submit_batch(VESSEL_A, vessel_batch("ves-a", "CS-A"))
        self.logistics.submit_batch(VESSEL_B, vessel_batch("ves-b", "CS-B"))
        rewritten = vessel_batch("ves-b", "CS-B", segment="S9", start=300.0, end=310.0)
        view, _ = self.logistics.submit_batch(VESSEL_B, rewritten)
        self.assertEqual(view["state"], "merged")

    def test_in_transit_change_recalculates_open_tasks(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        self.logistics.submit_batch(SHORE, shore_batch("shore-2", segment="S4", start=200.0, end=230.0, planned=32.0))
        avail = self.availability()
        self.assertEqual(avail["reserved_km"], 47.25)  # 15.75 + 31.5
        self.assertEqual(avail["gap_km"], 7.25)
        version = avail["inventory_version"]
        result = self.logistics.update_in_transit(KEEPER, {"move_key": "tr-1", "warehouse": "WH-East", "cable_type": "LW-24", "in_transit_km": 10.0, "expected_version": version})
        self.assertFalse(result["duplicated"])
        self.assertEqual(result["inventory"]["projected_gap_km"], 0.0)  # 在途计入后缺口闭合
        self.assertEqual(len(result["recalculated"]), 2)  # 两个未出库任务都被重算
        # 重复回传同一在途变更不重复生效
        again = self.logistics.update_in_transit(KEEPER, {"move_key": "tr-1", "warehouse": "WH-East", "cable_type": "LW-24", "in_transit_km": 10.0, "expected_version": version})
        self.assertTrue(again["duplicated"])
        self.assertEqual(again["inventory"]["in_transit_km"], 10.0)
        # 在途到库：缺口真正消除
        self.logistics.receipt(KEEPER, {"move_key": "rcpt-2", "warehouse": "WH-East", "cable_type": "LW-24", "quantity_km": 10.0, "from_in_transit": True})
        final = self.availability()
        self.assertEqual(final["gap_km"], 0.0)
        self.assertEqual(final["in_transit_km"], 0.0)
        self.assertTrue(all(task["covered"] for task in final["open_tasks"]))

    def test_failed_batch_recovers_from_stored_payload(self):
        # 未注册的仓库导致合并写入失败：完整批次留库，修复主数据后恢复
        view, _ = self.logistics.submit_batch(SHORE, shore_batch("shore-x", warehouse="WH-Nowhere"))
        self.assertEqual(view["state"], "failed")
        self.assertIn("WH-Nowhere", view["error"])
        self.logistics.receipt(KEEPER, {"move_key": "rcpt-x", "warehouse": "WH-Nowhere", "cable_type": "LW-24", "quantity_km": 50.0})
        recovered = self.logistics.resume_batch(SHORE, "shore-x")
        self.assertEqual(recovered["state"], "merged")
        avail = self.logistics.availability(SHORE, "WH-Nowhere", "LW-24")
        self.assertEqual(avail["reserved_km"], 15.75)
        # 恢复后重复回传仍不重复扣减
        replay, _ = self.logistics.submit_batch(SHORE, shore_batch("shore-x", warehouse="WH-Nowhere"))
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(self.logistics.availability(SHORE, "WH-Nowhere", "LW-24")["reserved_km"], 15.75)

    def test_outbound_and_settlement_reconcile(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        plan_id = self.availability()["open_tasks"][0]["plan_id"]
        issued = self.logistics.issue_outbound(KEEPER, {"order_key": "out-1", "plan_id": plan_id, "quantity_km": 15.75, "vessel_name": "CS-1"})
        self.assertFalse(issued["duplicated"])
        # 重复出库单不重复扣减
        dup = self.logistics.issue_outbound(KEEPER, {"order_key": "out-1", "plan_id": plan_id, "quantity_km": 15.75})
        self.assertTrue(dup["duplicated"])
        avail = self.availability()
        self.assertEqual(avail["on_hand_km"], 24.25)
        self.assertEqual(avail["reserved_km"], 0.0)
        # 结算流水与出库单对账
        self.logistics.record_settlement(CLERK, {"entry_key": "st-1", "order_key": "out-1", "quantity_km": 10.0, "amount": 5000})
        row = self.logistics.reconcile(CLERK)["rows"][0]
        self.assertEqual(row["status"], "partial")
        self.logistics.record_settlement(CLERK, {"entry_key": "st-2", "order_key": "out-1", "quantity_km": 5.75, "amount": 2875})
        report = self.logistics.reconcile(CLERK)
        self.assertEqual(report["rows"][0]["status"], "matched")
        self.assertEqual(report["summary"]["matched"], 1)
        # 重复结算流水不重复入账
        dup_entry = self.logistics.record_settlement(CLERK, {"entry_key": "st-2", "order_key": "out-1", "quantity_km": 5.75, "amount": 2875})
        self.assertTrue(dup_entry["duplicated"])
        self.assertEqual(self.logistics.reconcile(CLERK)["rows"][0]["status"], "matched")

    def test_reconcile_flags_unsettled_and_over_settled(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        self.logistics.submit_batch(SHORE, shore_batch("shore-2", segment="S4", start=200.0, end=210.0))
        plans = {t["segment"]: t["plan_id"] for t in self.availability()["open_tasks"]}
        self.logistics.issue_outbound(KEEPER, {"order_key": "out-1", "plan_id": plans["S3"], "quantity_km": 15.75})
        self.logistics.issue_outbound(KEEPER, {"order_key": "out-2", "plan_id": plans["S4"], "quantity_km": 10.5})
        self.logistics.record_settlement(CLERK, {"entry_key": "st-1", "order_key": "out-2", "quantity_km": 12.0, "amount": 6000})
        report = self.logistics.reconcile(CLERK)
        status = {row["order_key"]: row["status"] for row in report["rows"]}
        self.assertEqual(status["out-1"], "unsettled")
        self.assertEqual(status["out-2"], "over_settled")

    def test_outbound_blocked_when_stock_insufficient(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1", segment="S5", start=0.0, end=40.0))  # 需求42 > 在库40
        avail = self.availability()
        self.assertEqual(avail["gap_km"], 2.0)  # 现场提前看到缺口，而不是到船才发现
        plan_id = avail["open_tasks"][0]["plan_id"]
        with self.assertRaises(ValidationError):
            self.logistics.issue_outbound(KEEPER, {"order_key": "out-x", "plan_id": plan_id, "quantity_km": 42.0})

    def test_quantity_mismatch_goes_to_adjudication(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1", planned=20.0))
        view, _ = self.logistics.submit_batch(VESSEL_A, vessel_batch("ves-1", "CS-1", planned=10.0))
        self.assertEqual(view["state"], "conflicted")
        conflicts = self.logistics.list_conflicts(MANAGER, state="pending")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["kind"], "quantity_mismatch")
        resolved = self.logistics.resolve_conflict(MANAGER, conflicts[0]["id"], {"resolution": "use_shore", "override_required_km": 20.0})
        self.assertEqual(resolved["state"], "resolved")
        self.assertEqual(self.availability()["reserved_km"], 20.0)

    def test_field_and_shore_see_same_availability(self):
        self.logistics.submit_batch(SHORE, shore_batch("shore-1"))
        shore_view = self.logistics.availability(SHORE, "WH-East", "LW-24")
        vessel_view = self.logistics.availability(VESSEL_A, "WH-East", "LW-24")
        self.assertEqual(shore_view, vessel_view)

    def test_permissions(self):
        with self.assertRaises(PermissionDenied):
            self.logistics.submit_batch(VESSEL_A, shore_batch("shore-9"))  # 船端角色不能提岸端工单
        with self.assertRaises(PermissionDenied):
            self.logistics.submit_batch(KEEPER, vessel_batch("ves-9", "CS-9"))  # 库管不能提船端任务
        with self.assertRaises(PermissionDenied):
            self.logistics.receipt(SHORE, {"move_key": "rcpt-9", "warehouse": "WH-East", "cable_type": "LW-24", "quantity_km": 1.0})
        with self.assertRaises(PermissionDenied):
            self.logistics.record_settlement(KEEPER, {"entry_key": "st-9", "order_key": "out-9", "quantity_km": 1.0, "amount": 1})
        with self.assertRaises(PermissionDenied):
            self.logistics.submit_batch(Actor("outsider", "outsider"), shore_batch("shore-10"))

    def test_stale_in_transit_version_rejected(self):
        self.logistics.update_in_transit(KEEPER, {"move_key": "tr-1", "warehouse": "WH-East", "cable_type": "LW-24", "in_transit_km": 5.0, "expected_version": 2})
        with self.assertRaises(Conflict):
            self.logistics.update_in_transit(KEEPER, {"move_key": "tr-2", "warehouse": "WH-East", "cable_type": "LW-24", "in_transit_km": 8.0, "expected_version": 2})


if __name__ == "__main__":
    unittest.main()
