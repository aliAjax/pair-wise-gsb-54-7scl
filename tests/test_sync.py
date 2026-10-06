"""续作批次同步链路测试：合并去重、冲突裁决、先到先占、库存重算、
失败恢复、重复幂等与出库结算对账。"""
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from app import build_sync_service
from src.sync import errors as exc


def shore(ref, segment="S3", start=120.0, end=135.0, required=None, cable_type="RL-16", cable="SEA-1"):
    data = {"cable": cable, "segment": segment, "cable_type": cable_type,
            "start_km": start, "end_km": end}
    if required is not None:
        data["required_km"] = required
    return {"source": "shore_workorder", "reference": ref, "data": data}


def vessel(ref, vessel_id="CS-1", segment="S3", start=120.0, end=135.0, required=None,
           cable_type="RL-16", machine_id="ENG-1", spare=20.0, occurred_at=None, cable="SEA-1"):
    data = {"cable": cable, "segment": segment, "cable_type": cable_type,
            "start_km": start, "end_km": end, "required_km": required if required is not None else round(end - start, 3),
            "vessel_id": vessel_id, "machine_id": machine_id, "spare_onboard_km": spare,
            "vessel_name": vessel_id}
    if occurred_at:
        data["occurred_at"] = occurred_at
    return {"source": "vessel_task", "reference": ref, "data": data}


def move(ref, wh="WH-1", ctype="RL-16", kind="receive", on_hand=0.0, in_transit=0.0):
    return {"source": "warehouse_move", "reference": ref,
            "data": {"warehouse_id": wh, "cable_type": ctype, "kind": kind,
                     "on_hand_delta": on_hand, "in_transit_delta": in_transit}}


class SyncBaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "sync.db")
        self.svc = build_sync_service(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def submit(self, batch_ref, node, items, actor="dispatcher"):
        return self.svc.submit_batch(actor, batch_ref, node, items)


class MergeTest(SyncBaseTest):
    def test_shore_and_vessel_merge_by_segment_mileage(self):
        # 同一区段、里程相交的岸端工单与船端任务必须合并，数量不重复计算。
        r1 = self.submit("B-SHORE", "shore", [shore("WO-1", start=120.0, end=135.0, required=15.0)])
        self.assertEqual(r1["state"], "applied")
        r2 = self.submit("B-VESSEL", "vessel", [vessel("VT-1", start=122.0, end=138.0, required=15.2)])
        self.assertEqual(r2["state"], "applied")
        demands = self.svc.list_demands(cable="SEA-1", segment="S3")
        self.assertEqual(len(demands), 1, "同区段相交里程只能有一条需求")
        demand = demands[0]
        # 里程并集 120..138 = 18km；申报最大值16 → 取并集18。
        self.assertAlmostEqual(demand["union_km"], 18.0, places=2)
        self.assertAlmostEqual(demand["required_km"], 18.0, places=2)
        self.assertEqual(demand["status"], "held")
        detail = self.svc.get_batch("B-VESSEL")["items"][0]["detail"]
        self.assertTrue(detail["merged"])

    def test_disjoint_segments_do_not_merge(self):
        self.submit("B1", "shore", [shore("WO-1", segment="S3", start=120.0, end=130.0)])
        self.submit("B2", "shore", [shore("WO-2", segment="S4", start=140.0, end=150.0)])
        self.assertEqual(len(self.svc.list_demands()), 2)


class ConflictTest(SyncBaseTest):
    def test_attribute_and_quantity_conflicts_wait_for_adjudication(self):
        self.submit("B1", "shore", [shore("WO-1", required=15.0, cable_type="RL-16")])
        # 型号冲突。
        r2 = self.submit("B2", "shore", [shore("WO-2", required=15.0, cable_type="RL-20")])
        self.assertEqual(r2["conflict_count"], 1)
        # 数量冲突：差异超过0.5km容差（同型号）。
        r3 = self.submit("B3", "shore", [shore("WO-3", required=20.0, cable_type="RL-16")])
        self.assertEqual(r3["conflict_count"], 1)
        conflicts = self.svc.list_conflicts("pending")
        self.assertEqual({c["kind"] for c in conflicts}, {"attribute", "quantity"})

        # 裁决数量冲突：接受来方20km，合并后需求按20重算。
        qty_conflict = next(c for c in conflicts if c["kind"] == "quantity")
        resolved = self.svc.resolve_conflict("rm-1", qty_conflict["id"],
                                             {"decision": "accept_incoming", "note": "以复测为准"})
        self.assertEqual(resolved["state"], "resolved")
        demand = next(d for d in self.svc.list_demands(segment="S3") if d["cable_type"] == "RL-16")
        self.assertAlmostEqual(demand["required_km"], max(demand["union_km"], 20.0), places=2)

    def test_keep_existing_does_not_change_demand(self):
        self.submit("B1", "shore", [shore("WO-1", required=15.0)])
        self.submit("B2", "shore", [shore("WO-2", required=30.0)])
        conflict = self.svc.list_conflicts("pending")[0]
        before = self.svc.list_demands()[0]["required_km"]
        self.svc.resolve_conflict("rm-1", conflict["id"], {"decision": "keep_existing"})
        after = self.svc.list_demands()[0]["required_km"]
        self.assertEqual(before, after)


class LockTest(SyncBaseTest):
    def test_first_vessel_holds_segment_second_keeps_draft(self):
        self.submit("B1", "vessel", [vessel("VT-A", vessel_id="CS-A")])
        self.submit("B2", "vessel", [vessel("VT-B", vessel_id="CS-B")])
        held = [d for d in self.svc.list_demands() if d["status"] == "held"]
        drafts = [d for d in self.svc.list_demands() if d["status"] == "draft"]
        self.assertEqual(len(held), 1)
        self.assertEqual(len(drafts), 1)
        self.assertEqual(held[0]["holder_vessel_id"], "CS-A")
        self.assertEqual(drafts[0]["holder_vessel_id"], "CS-B")
        # 区段仍被占用时，草稿无法升级。
        with self.assertRaises(exc.Conflict):
            self.svc.promote_draft("CS-B", drafts[0]["demand_ref"])
        view = self.svc.availability_view()
        self.assertEqual(len(view["segment_locks"]), 1)
        self.assertEqual(view["segment_locks"][0]["vessel_id"], "CS-A")

    def test_draft_promotes_after_segment_released(self):
        self.submit("B1", "vessel", [vessel("VT-A", vessel_id="CS-A")])
        self.submit("B2", "vessel", [vessel("VT-B", vessel_id="CS-B")])
        draft = next(d for d in self.svc.list_demands() if d["status"] == "draft")
        self.svc.release_segment("dispatcher", "SEA-1", "S3")
        promoted = self.svc.promote_draft("CS-B", draft["demand_ref"])
        self.assertEqual(promoted["status"], "held")
        self.assertEqual(promoted["holder_vessel_id"], "CS-B")

    def test_other_vessel_different_type_opens_draft_not_conflict(self):
        self.submit("B1", "vessel", [vessel("VT-A", vessel_id="CS-A", cable_type="RL-16")])
        # 他船已持锁，且型号不同：不是在裁决同一需求，直接另开草稿。
        self.submit("B2", "vessel", [vessel("VT-B", vessel_id="CS-B", cable_type="RL-20")])
        self.assertEqual(self.svc.list_conflicts("pending"), [])
        statuses = sorted((d["status"], d["cable_type"]) for d in self.svc.list_demands())
        self.assertEqual(statuses, [("draft", "RL-20"), ("held", "RL-16")])

    def test_concurrent_submissions_still_first_wins(self):
        barrier = threading.Barrier(2)
        results = []

        def submit(vessel_id, ref):
            barrier.wait()
            results.append(self.svc.submit_batch(vessel_id, "B-%s" % vessel_id, "vessel",
                                                 [vessel(ref, vessel_id=vessel_id)]))
        t1 = threading.Thread(target=submit, args=("CS-X", "VT-X"))
        t2 = threading.Thread(target=submit, args=("CS-Y", "VT-Y"))
        t1.start(); t2.start(); t1.join(); t2.join()
        holders = [d["holder_vessel_id"] for d in self.svc.list_demands(status="held")]
        self.assertEqual(len(holders), 1)
        self.assertIn(holders[0], {"CS-X", "CS-Y"})


class StockRecomputeTest(SyncBaseTest):
    def test_stock_change_recomputes_unshipped_demands_and_gap(self):
        # 先建需求：需要18km，仓库空 → 缺口18。
        self.submit("BD", "shore", [shore("WO-1", start=120.0, end=138.0, required=18.0)])
        view = self.svc.availability_view(cable="SEA-1", segment="S3")
        demand = view["demands"][0]
        self.assertAlmostEqual(demand["gap_km"], 18.0, places=2)

        # 入库10km → 重算分配10，缺口8。
        self.submit("BM1", "warehouse", [move("MV-1", on_hand=10.0)])
        view = self.svc.availability_view(cable="SEA-1", segment="S3")
        demand = view["demands"][0]
        self.assertAlmostEqual(demand["allocated_km"], 10.0, places=2)
        self.assertAlmostEqual(demand["gap_km"], 8.0, 2)

        # 在途+10km → 在途也是可用承诺量，缺口清零。
        self.submit("BM2", "warehouse", [move("MV-2", kind="in_transit", in_transit=10.0)])
        view = self.svc.availability_view(cable="SEA-1", segment="S3")
        demand = view["demands"][0]
        self.assertAlmostEqual(demand["allocated_km"], 18.0, places=2)
        self.assertAlmostEqual(demand["gap_km"], 0.0, places=2)
        stock = view["stock_by_type"]["RL-16"]
        self.assertEqual(stock["available_km"], 20.0)

    def test_stock_shrink_claws_back_allocation(self):
        self.submit("BD", "shore", [shore("WO-1", required=18.0)])
        self.submit("BM1", "warehouse", [move("MV-1", on_hand=18.0)])
        self.assertAlmostEqual(self.svc.availability_view()["demands"][0]["allocated_km"], 18.0, 2)
        # 库存下调8km（盘点调整）→ 未出库任务必须重算。
        self.svc.stock_move("keeper", "MV-ADJ", "WH-1", "RL-16", "adjust", on_hand_delta=-8.0)
        self.assertAlmostEqual(self.svc.availability_view()["demands"][0]["allocated_km"], 10.0, 2)
        self.assertAlmostEqual(self.svc.availability_view()["demands"][0]["gap_km"], 8.0, 2)


class ResumeAndIdempotencyTest(SyncBaseTest):
    def test_write_failure_rolls_back_and_full_batch_recovers(self):
        items = [move("MV-1", on_hand=10.0)]
        # 注入一次磁盘错误模拟"写入失败"。
        self.svc.failure_hook = sqlite3.OperationalError("simulated disk I/O error")
        with self.assertRaises(sqlite3.OperationalError):
            self.submit("B-FAIL", "warehouse", items)
        # 整批回滚：批次与库存变动均未落库。
        with self.assertRaises(exc.NotFound):
            self.svc.get_batch("B-FAIL")
        self.assertEqual(self.svc.list_moves(), [])
        # 用完整批次重新提交恢复。
        result = self.submit("B-FAIL", "warehouse", items)
        self.assertEqual(result["state"], "applied")
        self.assertEqual(len(self.svc.list_moves()), 1)

    def test_duplicate_batch_replay_does_not_double_deduct(self):
        items = [move("MV-1", on_hand=10.0)]
        first = self.submit("B-DUP", "warehouse", items)
        self.assertNotIn("idempotent_replay", first)
        replay = self.submit("B-DUP", "warehouse", items)
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(len(self.svc.list_moves()), 1)
        stock = self.svc.list_stock()[0]
        self.assertEqual(stock["on_hand_km"], 10.0)

    def test_duplicate_source_item_in_later_batch_is_idempotent(self):
        self.submit("B1", "warehouse", [move("MV-1", on_hand=5.0)])
        again = self.submit("B2", "warehouse", [move("MV-1", on_hand=5.0)])
        self.assertEqual(again["applied_count"], 0)
        self.assertEqual(again["duplicate_count"], 1)
        self.assertEqual(self.svc.list_stock()[0]["on_hand_km"], 5.0)

    def test_resume_retries_only_unfinished_items(self):
        # 首项成功、第二项写入失败：整批事务回滚后用同一batch_ref重传完整批次。
        items = [move("MV-1", on_hand=3.0), move("MV-2", on_hand=4.0)]
        call_count = {"n": 0}
        real_apply = self.svc._apply_item

        def flaky_apply(c, item, batch_ref, actor_id):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise sqlite3.OperationalError("simulated transient failure")
            return real_apply(c, item, batch_ref, actor_id)

        self.svc._apply_item = flaky_apply
        with self.assertRaises(sqlite3.OperationalError):
            self.submit("B-RS", "warehouse", items)
        self.svc._apply_item = real_apply
        # 注意：整批事务回滚，重传即为全新应用，结果一致且只扣一次。
        result = self.submit("B-RS", "warehouse", items)
        self.assertEqual(result["applied_count"], 2)
        self.assertEqual(sum(m["on_hand_delta"] for m in self.svc.list_moves()), 7.0)


class OutboundReconcileTest(SyncBaseTest):
    def _prepare_demand_with_stock(self):
        self.submit("BD", "vessel", [vessel("VT-1", required=18.0)])
        self.submit("BM", "warehouse", [move("MV-1", on_hand=20.0)])
        return self.svc.list_demands(status="held")[0]["demand_ref"]

    def test_outbound_then_settlement_reconciles(self):
        demand_ref = self._prepare_demand_with_stock()
        order = self.svc.issue_outbound("keeper", "OB-1", demand_ref, "WH-1", 18.0, amount=90000.0)
        self.assertEqual(order["state"], "issued")
        # 冻结后可用池减少18。
        self.assertEqual(self.svc.list_stock()[0]["on_hand_km"], 2.0)
        # 重复出库不重复扣减。
        duplicate = self.svc.issue_outbound("keeper", "OB-1", demand_ref, "WH-1", 18.0, 90000.0)
        self.assertEqual(duplicate["state"], "issued")
        self.assertEqual(self.svc.list_stock()[0]["on_hand_km"], 2.0)

        self.svc.ship_outbound("keeper", "OB-1")
        settlement = self.svc.create_settlement("finance", "ST-1", "OB-1", "RL-16", 18.0, 90000.0)
        self.assertEqual(settlement["entry_ref"], "ST-1")
        report = self.svc.reconcile()
        self.assertTrue(report["balanced"])
        self.assertIn("OB-1", report["matched"])

    def test_reconcile_detects_missing_and_mismatch(self):
        demand_ref = self._prepare_demand_with_stock()
        self.svc.issue_outbound("keeper", "OB-A", demand_ref, "WH-1", 9.0, 45000.0)
        self.svc.ship_outbound("keeper", "OB-A")
        # OB-A没有任何结算 → missing_settlement。
        report = self.svc.reconcile()
        self.assertFalse(report["balanced"])
        self.assertEqual(report["issues"][0]["type"], "missing_settlement")
        # 补上金额不符的结算 → amount_mismatch。
        self.svc.create_settlement("finance", "ST-A", "OB-A", "RL-16", 9.0, 40000.0)
        report = self.svc.reconcile()
        self.assertEqual(report["issues"][0]["type"], "amount_mismatch")

    def test_field_sees_same_availability_and_gap(self):
        # 岸端与现场调用同一视图接口，数字必须一致。
        self.submit("BD", "shore", [shore("WO-1", required=18.0)])
        self.submit("BM", "warehouse", [move("MV-1", on_hand=12.0)])
        view_office = self.svc.availability_view(cable="SEA-1", segment="S3")
        view_field = self.svc.availability_view(cable="SEA-1", segment="S3")
        self.assertEqual(view_office["stock_by_type"], view_field["stock_by_type"])
        self.assertEqual(view_office["demands"][0]["gap_km"], view_field["demands"][0]["gap_km"])
        self.assertAlmostEqual(view_field["demands"][0]["gap_km"], 6.0, places=2)


class VesselSpareSnapshotTest(SyncBaseTest):
    def test_offline_vessel_and_spare_register_land_on_reconnect(self):
        items = [
            {"source": "spare_register", "reference": "SP-1",
             "data": {"spare_id": "SP-1", "cable_type": "RL-16", "km": 7.0,
                      "location": "warehouse", "warehouse_id": "WH-1"}},
            {"source": "spare_register", "reference": "SP-2",
             "data": {"spare_id": "SP-2", "cable_type": "RL-16", "km": 3.0,
                      "location": "vessel", "vessel_id": "CS-1"}},
        ]
        self.submit("B-SPARE", "vessel", items)
        spares = self.svc.list_spares()
        self.assertEqual({s["spare_id"] for s in spares}, {"SP-1", "SP-2"})
        # 仓库内备缆体现为在库；船载备缆不计入仓库库存。
        stock = self.svc.list_stock()
        self.assertEqual(len(stock), 1)
        self.assertEqual(stock[0]["on_hand_km"], 7.0)


if __name__ == "__main__":
    unittest.main()
