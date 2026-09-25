from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from traffic_dispatch import acceptance as traffic_acceptance
from traffic_dispatch.api import JsonApplication
from traffic_dispatch.clock import FrozenClock
from traffic_dispatch.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from traffic_dispatch.planning import AllocationRequest, RiskPoint, allocate_capacity, latest_streak
from traffic_dispatch.service import TrafficDispatchService
from traffic_dispatch.risk import DemandBucket, inventory_coverage, mark_to_risk, traffic_gap


ROOT = Path(__file__).resolve().parents[1]


class PlanningTests(unittest.TestCase):
    def test_latest_down_streak_uses_first_close_as_base(self) -> None:
        streak = latest_streak([
            RiskPoint("2026-09-18", Decimal("108")),
            RiskPoint("2026-09-19", Decimal("105")),
            RiskPoint("2026-09-20", Decimal("102")),
            RiskPoint("2026-09-21", Decimal("98")),
        ])
        self.assertEqual(streak.direction, "down")
        self.assertEqual(streak.sessions, 4)
        self.assertEqual(streak.start_date, "2026-09-18")
        self.assertEqual(streak.end_close, Decimal("98"))

    def test_allocation_is_stable_and_does_not_exceed_capacity(self) -> None:
        rows = allocate_capacity(Decimal("100"), [
            AllocationRequest("later", Decimal("80"), 20, "2026-09-24T09:00:00Z"),
            AllocationRequest("first", Decimal("70"), 10, "2026-09-24T10:00:00Z"),
        ])
        self.assertEqual(rows[0]["dispatch_id"], "first")
        self.assertEqual(rows[0]["allocated_units"], "70.000")
        self.assertEqual(rows[1]["allocated_units"], "30.000")

    def test_inventory_coverage_and_traffic_gap(self) -> None:
        coverage = inventory_coverage(
            [{"center_id": "command-center", "response_resource_kind": "tow-truck", "available_units": "250"}],
            [DemandBucket("command-center", "tow-truck", Decimal("100"), Decimal("20"))],
        )
        self.assertEqual(coverage[0]["coverage_days"], "2.30")
        self.assertTrue(coverage[0]["below_three_days"])
        gap = traffic_gap(
            opening_inventory=Decimal("100"),
            confirmed_inbound=Decimal("30"),
            forecast_demand=Decimal("120"),
            protected_reserve=Decimal("40"),
        )
        self.assertEqual(gap["traffic_gap"], "30.000")

    def test_mark_to_risk_groups_deterministically(self) -> None:
        result = mark_to_risk(
            [{"position_id": "p1", "risk_index": "COLLISION", "quantity_units": "100", "baseline_value": "105"}],
            {"COLLISION": Decimal("98")},
        )
        self.assertEqual(result["unrealized_pnl_cny"], "-700.00")


class TrafficDispatchServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrafficDispatchService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"center_id": "center-east", "name": "北部事故快处中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
        self.service.create_facility("plan", {"center_id": "command-center-b", "name": "沿海终端", "kind": "command-center", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
        self.service.create_route("plan", {"corridor_id": "corridor-east-1", "origin_center_id": "center-east", "destination_center_id": "command-center-b", "response_resource_kind": "patrol-unit", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})

    def tearDown(self) -> None:
        self.connection.close()

    def risk_record(self, day: int, close: str) -> dict[str, object]:
        return self.service.record_risk_record("plan", {"risk_index": "COLLISION", "duty_date": f"2026-09-{day}", "index_value": close, "source_revision": f"r-{day}", "observed_at": f"2026-09-{day}T21:00:00Z"})

    def test_risk_record_revisions_preserve_history(self) -> None:
        first = self.risk_record(23, "98")
        second = self.service.record_risk_record("plan", {"risk_index": "COLLISION", "duty_date": "2026-09-23", "index_value": "97.8", "source_revision": "r-23-corrected", "observed_at": "2026-09-23T22:00:00Z"})
        self.assertNotEqual(first["risk_record_id"], second["risk_record_id"])
        rows = self.connection.execute("SELECT * FROM risk_index_risk_records ORDER BY risk_record_id").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["supersedes_risk_record_id"], rows[0]["risk_record_id"])

    def test_dispatch_request_replay_and_payload_conflict(self) -> None:
        payload = {"dispatch_id": "nom-1", "corridor_id": "corridor-east-1", "incident_id": "medical-center", "duty_date": "2026-09-25", "requested_units": "80000", "priority": 10, "idempotency_key": "key-1"}
        first = self.service.submit_dispatch("dispatch", payload)
        self.assertEqual(first, self.service.submit_dispatch("dispatch", payload))
        changed = dict(payload, requested_units="81000")
        with self.assertRaises(Conflict):
            self.service.submit_dispatch("dispatch", changed)

    def test_outage_reduces_allocation_and_deployment_consumes_inventory(self) -> None:
        self.service.announce_restriction("risk", "corridor-east-1", "2026-09-25T00:00:00Z", "2026-09-25T23:59:59Z", "50", "检修")
        for number, requested, priority in ((1, "40000", 10), (2, "30000", 20)):
            self.service.submit_dispatch("dispatch", {"dispatch_id": f"nom-{number}", "corridor_id": "corridor-east-1", "incident_id": f"incident-{number}", "duty_date": "2026-09-25", "requested_units": requested, "priority": priority, "idempotency_key": f"key-{number}"})
        allocation = self.service.allocate("dispatch", "corridor-east-1", "2026-09-25")
        self.assertEqual(allocation["available_units"], "50000.000")
        self.assertEqual(allocation["allocations"][1]["allocated_units"], "10000.000")
        self.service.add_inventory_lot("dispatch", {"response_resource_lot_id": "lot-1", "center_id": "center-east", "response_resource_kind": "patrol-unit", "grade": "COLLISION", "quantity_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
        deployment = self.service.dispatch_deployment("dispatch", "deployment-1", "nom-1", "lot-1", 2)
        self.assertEqual(deployment["deployed_units"], "40000.000")
        self.assertEqual(self.service.inventory_lot("lot-1")["available_units"], "20000.000")

    def test_scenario_is_approved_and_replayed_by_input(self) -> None:
        self.risk_record(23, "98")
        self.service.add_inventory_lot("dispatch", {"response_resource_lot_id": "lot-1", "center_id": "center-east", "response_resource_kind": "patrol-unit", "grade": "COLLISION", "quantity_units": "60000", "unit_cost_cny": "91", "received_at": "2026-09-24T06:00:00Z"})
        self.service.create_scenario("plan", {"scenario_id": "restart", "name": "道路通行恢复", "risk_index_drop_percent": "9", "route_capacity_changes": {"corridor-east-1": "20"}, "demand_changes": {"center-east:patrol-unit": "-5"}})
        with self.assertRaises(Forbidden):
            self.service.approve_scenario("plan", "restart", 1)
        self.service.approve_scenario("risk", "restart", 1)
        first = self.service.run_scenario("plan", "restart", "2026-09-23")
        second = self.service.run_scenario("plan", "restart", "2026-09-23")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["run_id"], second["run_id"])

    def test_audit_chain_detects_tampering(self) -> None:
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE traffic_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])

    def test_api_exposes_browser_free_boundary(self) -> None:
        app = JsonApplication(self.service)
        self.assertEqual(app.handle("GET", "/health").status, 200)
        response = app.handle("GET", "/risk_records/summary/COLLISION", {"X-Actor-Id": "plan"})
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "not_found")


class RecoveryFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 23, 0, tzinfo=timezone.utc))
        self.service = TrafficDispatchService(self.connection, self.clock)
        for user_id, role in (
            ("plan", "planner"),
            ("dispatch", "dispatcher"),
            ("risk", "risk"),
            ("command", "commander"),
            ("audit", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"center_id": "center-east", "name": "北部事故快处中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
        self.service.create_facility("plan", {"center_id": "command-center-b", "name": "沿海终端", "kind": "command-center", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
        self.service.create_route("plan", {"corridor_id": "corridor-east-1", "origin_center_id": "center-east", "destination_center_id": "command-center-b", "response_resource_kind": "patrol-unit", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})
        self.restriction = self.service.announce_restriction("risk", "corridor-east-1", "2026-09-24T20:00:00Z", None, "25", "夜间重大事故封控")

    def tearDown(self) -> None:
        self.connection.close()

    def plan_payload(self, plan_id: str = "rec-1", restriction_id: int | None = None, first_percent: str = "60") -> dict[str, object]:
        return {
            "plan_id": plan_id,
            "corridor_id": "corridor-east-1",
            "restriction_id": self.restriction["restriction_id"] if restriction_id is None else restriction_id,
            "incident_id": "incident-night-1",
            "stages": [
                {
                    "stage_id": f"{plan_id}-s1",
                    "sequence": 1,
                    "zone_label": "封控区A",
                    "lane_codes": ["L1", "L2"],
                    "restored_capacity_percent": first_percent,
                    "items": [
                        {"item_id": f"{plan_id}-s1-casualty", "item_kind": "casualty_transport", "responsible_unit": "市急救中心", "required": True},
                        {"item_id": f"{plan_id}-s1-debris", "item_kind": "debris_cleanup", "responsible_unit": "路政养护一队", "required": True},
                    ],
                },
                {
                    "stage_id": f"{plan_id}-s2",
                    "sequence": 2,
                    "zone_label": "封控区B",
                    "lane_codes": ["L3"],
                    "restored_capacity_percent": "100",
                    "items": [
                        {"item_id": f"{plan_id}-s2-evidence", "item_kind": "evidence_collection", "responsible_unit": "事故处理中队", "required": True},
                        {"item_id": f"{plan_id}-s2-facility", "item_kind": "facility_inspection", "responsible_unit": "设施巡检班", "required": False},
                    ],
                },
            ],
        }

    def create_active_plan(self) -> None:
        self.service.create_recovery_plan("command", self.plan_payload())
        self.service.activate_recovery_plan("command", "rec-1", 1)

    def confirm_stage_one(self) -> None:
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-casualty", "receipt-casualty-1", "伤员已全部转运")
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-1", "散落物清理完毕")

    def complete_plan(self) -> None:
        self.confirm_stage_one()
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s2-evidence", "receipt-evidence-1", "证据采集完成")
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s2")

    def test_required_items_gate_stage_release(self) -> None:
        self.create_active_plan()
        with self.assertRaises(InvalidState):
            self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-casualty", "receipt-casualty-1", "伤员已全部转运")
        with self.assertRaises(InvalidState):
            self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-1", "散落物清理完毕")
        result = self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.assertEqual(result["plan_state"], "active")
        row = self.connection.execute("SELECT * FROM corridor_restrictions WHERE restriction_id=?", (self.restriction["restriction_id"],)).fetchone()
        self.assertEqual(row["capacity_percent"], "60")
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-25")["available_units"], "60000.000")

    def test_stages_release_in_sequence(self) -> None:
        self.create_active_plan()
        self.confirm_stage_one()
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s2-evidence", "receipt-evidence-1", "证据采集完成")
        with self.assertRaises(InvalidState):
            self.service.release_recovery_stage("command", "rec-1", "rec-1-s2")

    def test_duplicate_receipt_is_recorded_not_reapplied(self) -> None:
        self.create_active_plan()
        first = self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-1", "散落物清理完毕")
        self.assertFalse(first["duplicate"])
        second = self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-1", "重复上报")
        self.assertTrue(second["duplicate"])
        third = self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-2", "另一张回执")
        self.assertTrue(third["duplicate"])
        plan = self.service.recovery_plan("rec-1")
        receipts = [row for row in plan["receipts"] if row["item_id"] == "rec-1-s1-debris"]
        self.assertEqual(len(receipts), 3)
        self.assertEqual([row["duplicate"] for row in receipts], [0, 1, 1])
        item = plan["stages"][0]["items"][1]
        self.assertEqual(item["state"], "confirmed")
        self.assertEqual(item["revision"], 2)

    def test_receipt_key_reused_on_other_item_conflicts(self) -> None:
        self.create_active_plan()
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-casualty", "receipt-shared", "伤员已全部转运")
        with self.assertRaises(Conflict):
            self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-shared", "冒用回执编号")

    def test_withdrawal_blocks_release_and_preserves_history(self) -> None:
        self.create_active_plan()
        self.confirm_stage_one()
        withdrawn = self.service.withdraw_recovery_item("command", "rec-1", "rec-1-s1-debris", "复查发现散落物未清净")
        self.assertEqual(withdrawn["state"], "pending")
        with self.assertRaises(InvalidState):
            self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        with self.assertRaises(InvalidState):
            self.service.withdraw_recovery_item("command", "rec-1", "rec-1-s1-debris", "重复撤回")
        self.service.confirm_recovery_item("command", "rec-1", "rec-1-s1-debris", "receipt-debris-2", "二次清理完毕")
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(len(plan["withdrawals"]), 1)
        self.assertEqual(plan["withdrawals"][0]["reason"], "复查发现散落物未清净")
        self.assertEqual(len([row for row in plan["receipts"] if row["item_id"] == "rec-1-s1-debris"]), 2)

    def test_emergency_release_requires_commander_and_validity(self) -> None:
        self.create_active_plan()
        with self.assertRaises(Forbidden):
            self.service.create_emergency_release("risk", "rec-1", "rec-1-s1", "越权放行", "2026-09-25T08:00:00Z")
        with self.assertRaises(ValidationFailed):
            self.service.create_emergency_release("command", "rec-1", "rec-1-s1", "过期放行", "2026-09-24T08:00:00Z")
        release = self.service.create_emergency_release("command", "rec-1", "rec-1-s1", "救护车需优先离场", "2026-09-25T01:00:00Z")
        self.clock.advance(hours=3)
        with self.assertRaises(InvalidState):
            self.service.release_recovery_stage("command", "rec-1", "rec-1-s1", release["release_id"])
        fresh = self.service.create_emergency_release("command", "rec-1", "rec-1-s1", "再次紧急放行", "2026-09-25T08:00:00Z")
        result = self.service.release_recovery_stage("command", "rec-1", "rec-1-s1", fresh["release_id"])
        self.assertEqual(result["state"], "released")
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(plan["stages"][0]["emergency_release_id"], fresh["release_id"])
        self.assertEqual(len(plan["emergency_releases"]), 2)

    def test_completion_closes_restriction_and_restores_capacity(self) -> None:
        self.create_active_plan()
        self.complete_plan()
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(plan["state"], "completed")
        self.assertIsNotNone(plan["completed_at"])
        row = self.connection.execute("SELECT * FROM corridor_restrictions WHERE restriction_id=?", (self.restriction["restriction_id"],)).fetchone()
        self.assertEqual(row["state"], "closed")
        self.assertIsNotNone(row["ends_at"])
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-26")["available_units"], "100000.000")
        self.service.submit_dispatch("dispatch", {"dispatch_id": "nom-after", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-26", "requested_units": "90000", "priority": 10, "idempotency_key": "key-after"})
        allocation = self.service.allocate("dispatch", "corridor-east-1", "2026-09-26")
        self.assertEqual(allocation["available_units"], "100000.000")
        self.assertEqual(allocation["allocations"][0]["allocated_units"], "90000.000")

    def test_hazard_after_completion_reopens_and_preserves_history(self) -> None:
        self.create_active_plan()
        self.complete_plan()
        hazard = self.service.report_recovery_hazard("command", "rec-1", {"description": "恢复通行后发现护栏螺栓缺失", "capacity_percent": "40", "stage_id": "rec-1-s2"})
        self.assertEqual(hazard["plan_state"], "reopened")
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-26")["available_units"], "40000.000")
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(plan["state"], "reopened")
        self.assertTrue(all(stage["state"] == "pending" for stage in plan["stages"]))
        self.assertEqual(len(plan["hazards"]), 1)
        self.assertEqual(plan["hazards"][0]["post_completion"], 1)
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s2")
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(plan["state"], "completed")
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-26")["available_units"], "100000.000")
        self.assertEqual(len(plan["hazards"]), 1)
        self.assertEqual(len(plan["receipts"]), 3)

    def test_hazard_on_active_plan_only_tightens_capacity(self) -> None:
        self.create_active_plan()
        self.confirm_stage_one()
        self.service.release_recovery_stage("command", "rec-1", "rec-1-s1")
        self.service.report_recovery_hazard("command", "rec-1", {"description": "已放行车道发现油污", "capacity_percent": "30"})
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-25")["available_units"], "30000.000")
        self.service.report_recovery_hazard("command", "rec-1", {"description": "误报，通行条件未变化", "capacity_percent": "80"})
        self.assertEqual(self.service.corridor_capacity("corridor-east-1", "2026-09-25")["available_units"], "30000.000")
        plan = self.service.recovery_plan("rec-1")
        self.assertEqual(plan["state"], "active")
        self.assertEqual(len(plan["hazards"]), 2)

    def test_plan_requires_open_restriction_and_single_active_plan(self) -> None:
        self.create_active_plan()
        with self.assertRaises(Conflict):
            self.service.create_recovery_plan("command", dict(self.plan_payload(), plan_id="rec-2"))
        self.complete_plan()
        payload = dict(self.plan_payload(), plan_id="rec-3")
        with self.assertRaises(InvalidState):
            self.service.create_recovery_plan("command", payload)

    def test_activation_checks_revision_and_percent_progression(self) -> None:
        self.service.create_recovery_plan("command", self.plan_payload())
        with self.assertRaises(InvalidState):
            self.service.activate_recovery_plan("command", "rec-1", 99)
        second = self.service.announce_restriction("risk", "corridor-east-1", "2026-09-24T21:00:00Z", None, "50", "第二处封控")
        self.service.create_recovery_plan("command", self.plan_payload(plan_id="rec-2", restriction_id=second["restriction_id"], first_percent="50"))
        with self.assertRaises(ValidationFailed):
            self.service.activate_recovery_plan("command", "rec-2", 1)

    def test_recovery_permissions_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_recovery_plan("plan", self.plan_payload())
        self.create_active_plan()
        with self.assertRaises(Forbidden):
            self.service.confirm_recovery_item("dispatch", "rec-1", "rec-1-s1-casualty", "receipt-x", "越权确认")
        with self.assertRaises(Forbidden):
            self.service.release_recovery_stage("risk", "rec-1", "rec-1-s1")
        with self.assertRaises(Forbidden):
            self.service.report_recovery_hazard("audit", "rec-1", {"description": "越权登记", "capacity_percent": "50"})

    def test_recovery_api_flow(self) -> None:
        app = JsonApplication(self.service)
        headers = {"X-Actor-Id": "command"}
        payload = self.plan_payload()
        response = app.handle("POST", "/recovery_plans", headers, json.dumps(payload).encode("utf-8"))
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["state"], "draft")
        response = app.handle("POST", "/recovery_plans/rec-1/activate", headers, json.dumps({"expected_revision": 1}).encode("utf-8"))
        self.assertEqual(response.status, 200)
        response = app.handle("POST", "/recovery_plans/rec-1/receipts", headers, json.dumps({"item_id": "rec-1-s1-casualty", "receipt_key": "receipt-casualty-1", "note": "伤员已全部转运"}).encode("utf-8"))
        self.assertEqual(response.status, 201)
        response = app.handle("POST", "/recovery_plans/rec-1/receipts", headers, json.dumps({"item_id": "rec-1-s1-debris", "receipt_key": "receipt-debris-1", "note": "散落物清理完毕"}).encode("utf-8"))
        self.assertEqual(response.status, 201)
        response = app.handle("POST", "/recovery_plans/rec-1/stages/rec-1-s1/release", headers, b"{}")
        self.assertEqual(response.status, 200)
        response = app.handle("GET", "/road_corridors/corridor-east-1/capacity?duty_date=2026-09-25", headers)
        self.assertEqual(response.body["available_units"], "60000.000")
        response = app.handle("POST", "/recovery_plans/rec-1/hazards", headers, json.dumps({"description": "演练隐患", "capacity_percent": "50"}).encode("utf-8"))
        self.assertEqual(response.status, 201)
        response = app.handle("GET", "/recovery_plans/rec-1", headers)
        self.assertEqual(response.status, 200)
        self.assertEqual(len(response.body["stages"]), 2)
        self.assertEqual(len(response.body["hazards"]), 1)
        response = app.handle("GET", "/recovery_plans/missing", headers)
        self.assertEqual(response.status, 404)


class TrafficAcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = traffic_acceptance.run(ROOT)
        self.assertEqual(result["status"], "ok")
        recovery = result["recovery"]
        self.assertEqual(recovery["state"], "completed")
        self.assertEqual(recovery["duplicate_receipts"], 1)
        self.assertEqual(recovery["hazards"], 1)
        self.assertEqual(
            recovery["capacity_progression"],
            {"partial": "60000.000", "restored": "100000.000", "hazard": "40000.000", "final": "100000.000"},
        )
        self.assertTrue(result["audit"]["valid"])


if __name__ == "__main__":
    unittest.main()
