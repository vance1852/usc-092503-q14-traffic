from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone

from traffic_dispatch.api import JsonApplication
from traffic_dispatch.clock import FrozenClock
from traffic_dispatch.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from traffic_dispatch.service import TrafficDispatchService
from traffic_dispatch.storage import initialize


PLAN_PAYLOAD = {
    "plan_id": "rec-1",
    "corridor_id": "corridor-east-1",
    "restriction_id": 1,
    "incident_id": "incident-night-1",
    "name": "夜间重大事故分阶段恢复",
    "zones": [
        {
            "zone_id": "zone-a",
            "name": "事故点封控区",
            "sequence": 1,
            "lanes": [
                {"lane_id": "lane-a1", "name": "内侧车道"},
                {"lane_id": "lane-a2", "name": "外侧车道"},
            ],
        },
        {
            "zone_id": "zone-b",
            "name": "缓冲区",
            "sequence": 2,
            "lanes": [{"lane_id": "lane-b1", "name": "应急车道"}],
        },
    ],
    "items": [
        {"item_id": "item-casualty", "kind": "casualty-transfer", "responsible_unit": "医疗急救中心"},
        {"item_id": "item-evidence", "kind": "evidence-collection", "responsible_unit": "事故处理民警队"},
        {"item_id": "item-debris", "kind": "debris-cleanup", "responsible_unit": "道路施救单位"},
        {"item_id": "item-facility", "kind": "facility-inspection", "responsible_unit": "路政设施单位", "zone_id": "zone-b"},
    ],
}


class RecoveryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 25, 21, 0, tzinfo=timezone.utc))
        self.service = TrafficDispatchService(self.connection, self.clock)
        for user_id, role in (
            ("plan", "planner"),
            ("dispatch", "dispatcher"),
            ("risk", "risk"),
            ("audit", "auditor"),
            ("cmd", "commander"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"center_id": "center-east", "name": "北部事故快处中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
        self.service.create_facility("plan", {"center_id": "command-center-b", "name": "沿海终端", "kind": "command-center", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
        self.service.create_route("plan", {"corridor_id": "corridor-east-1", "origin_center_id": "center-east", "destination_center_id": "command-center-b", "response_resource_kind": "patrol-unit", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})
        self.service.announce_restriction("risk", "corridor-east-1", "2026-09-25T20:00:00Z", None, "0", "夜间重大事故全封闭")

    def tearDown(self) -> None:
        self.connection.close()

    def create_plan(self) -> dict[str, object]:
        return self.service.create_recovery_plan("cmd", PLAN_PAYLOAD)

    def confirm_all_items(self) -> None:
        for item_id in ("item-casualty", "item-evidence", "item-debris", "item-facility"):
            self.service.confirm_check_item("dispatch", "rec-1", item_id, {"idempotency_key": f"cf-{item_id}", "note": "现场完成"})


class RecoveryPlanCreationTests(RecoveryTestCase):
    def test_create_plan_links_zones_lanes_items_and_units(self) -> None:
        view = self.create_plan()
        self.assertEqual(view["state"], "active")
        self.assertEqual(view["base_capacity_percent"], "0")
        self.assertEqual(view["current_capacity_percent"], "0")
        self.assertEqual([zone["zone_id"] for zone in view["zones"]], ["zone-a", "zone-b"])
        self.assertEqual(len(view["zones"][0]["lanes"]), 2)
        items = {item["item_id"]: item for item in view["items"]}
        self.assertEqual(items["item-casualty"]["responsible_unit"], "医疗急救中心")
        self.assertTrue(items["item-debris"]["required"])
        self.assertEqual(items["item-facility"]["zone_id"], "zone-b")

    def test_create_plan_validates_payload(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_recovery_plan("plan", PLAN_PAYLOAD)
        bad_kind = dict(PLAN_PAYLOAD, items=[{"item_id": "x", "kind": "tow-away", "responsible_unit": "单位"}])
        with self.assertRaises(ValidationFailed):
            self.service.create_recovery_plan("cmd", bad_kind)
        bad_zone = dict(PLAN_PAYLOAD, items=[{"item_id": "x", "kind": "debris-cleanup", "responsible_unit": "单位", "zone_id": "zone-x"}])
        with self.assertRaises(ValidationFailed):
            self.service.create_recovery_plan("cmd", bad_zone)
        with self.assertRaises(NotFound):
            self.service.create_recovery_plan("cmd", dict(PLAN_PAYLOAD, restriction_id=99))

    def test_only_one_active_plan_per_restriction(self) -> None:
        self.create_plan()
        with self.assertRaises(Conflict):
            self.service.create_recovery_plan("cmd", dict(PLAN_PAYLOAD, plan_id="rec-2"))

    def test_closed_restriction_rejects_new_plan(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        self.service.open_zone("cmd", "rec-1", "zone-a")
        self.service.open_zone("cmd", "rec-1", "zone-b")
        self.assertEqual(self.service.recovery_plan("rec-1")["state"], "completed")
        with self.assertRaises(InvalidState):
            self.service.create_recovery_plan("cmd", dict(PLAN_PAYLOAD, plan_id="rec-3"))


class RecoveryGateTests(RecoveryTestCase):
    def test_expansion_blocked_until_required_items_complete(self) -> None:
        self.create_plan()
        with self.assertRaises(InvalidState) as caught:
            self.service.open_zone("cmd", "rec-1", "zone-a")
        self.assertIn("item-casualty", str(caught.exception))
        self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", {"idempotency_key": "cf-1"})
        with self.assertRaises(InvalidState):
            self.service.open_zone("cmd", "rec-1", "zone-a")
        self.confirm_all_items()
        opened = self.service.open_zone("cmd", "rec-1", "zone-a")
        self.assertEqual(opened["zone_state"], "open")

    def test_optional_item_does_not_block_expansion(self) -> None:
        payload = dict(PLAN_PAYLOAD)
        payload["items"] = [dict(item) for item in PLAN_PAYLOAD["items"]]
        payload["items"][0] = dict(payload["items"][0], required=False)
        self.service.create_recovery_plan("cmd", payload)
        for item_id in ("item-evidence", "item-debris", "item-facility"):
            self.service.confirm_check_item("dispatch", "rec-1", item_id, {"idempotency_key": f"cf-{item_id}"})
        opened = self.service.open_zone("cmd", "rec-1", "zone-a")
        self.assertEqual(opened["zone_state"], "open")

    def test_zone_scoped_item_only_blocks_its_zone(self) -> None:
        self.create_plan()
        for item_id in ("item-casualty", "item-evidence", "item-debris"):
            self.service.confirm_check_item("dispatch", "rec-1", item_id, {"idempotency_key": f"cf-{item_id}"})
        opened = self.service.open_zone("cmd", "rec-1", "zone-a")
        self.assertEqual(opened["zone_state"], "open")
        with self.assertRaises(InvalidState) as caught:
            self.service.open_zone("cmd", "rec-1", "zone-b")
        self.assertIn("item-facility", str(caught.exception))

    def test_phases_must_open_in_sequence(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        with self.assertRaises(InvalidState):
            self.service.open_zone("cmd", "rec-1", "zone-b")
        self.service.open_zone("cmd", "rec-1", "zone-a")
        self.service.open_zone("cmd", "rec-1", "zone-b")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["state"], "completed")
        self.assertEqual(view["restriction_state"], "closed")

    def test_open_zone_requires_commander_permission(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        with self.assertRaises(Forbidden):
            self.service.open_zone("dispatch", "rec-1", "zone-a")


class RecoveryReceiptTests(RecoveryTestCase):
    def test_confirm_replay_and_payload_conflict(self) -> None:
        self.create_plan()
        payload = {"idempotency_key": "cf-1", "note": "伤员已转运"}
        first = self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", payload)
        second = self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", payload)
        self.assertEqual(first, second)
        self.assertFalse(first["duplicate"])
        with self.assertRaises(Conflict):
            self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", {"idempotency_key": "cf-1", "note": "篡改"})
        count = self.connection.execute("SELECT count(*) FROM recovery_receipts").fetchone()[0]
        self.assertEqual(count, 1)

    def test_duplicate_receipt_keeps_history(self) -> None:
        self.create_plan()
        self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", {"idempotency_key": "cf-1"})
        again = self.service.confirm_check_item("dispatch", "rec-1", "item-casualty", {"idempotency_key": "cf-2", "note": "重复上报"})
        self.assertTrue(again["duplicate"])
        history = self.service.recovery_receipt_history("rec-1")
        self.assertEqual(len(history["receipts"]), 2)
        self.assertEqual([row["action"] for row in history["receipts"]], ["confirm", "confirm"])
        self.assertTrue(history["receipts"][1]["duplicate"])

    def test_withdraw_keeps_history_and_blocks_expansion(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        with self.assertRaises(ValidationFailed):
            self.service.withdraw_check_item("dispatch", "rec-1", "item-debris", {"idempotency_key": "wd-0"})
        withdrawn = self.service.withdraw_check_item(
            "dispatch", "rec-1", "item-debris", {"idempotency_key": "wd-1", "note": "散落物复核发现遗漏"}
        )
        self.assertEqual(withdrawn["state"], "pending")
        with self.assertRaises(InvalidState):
            self.service.open_zone("cmd", "rec-1", "zone-a")
        with self.assertRaises(InvalidState):
            self.service.withdraw_check_item("dispatch", "rec-1", "item-debris", {"idempotency_key": "wd-2", "note": "重复撤回"})
        self.service.confirm_check_item("dispatch", "rec-1", "item-debris", {"idempotency_key": "cf-debris-2", "note": "复清完成"})
        self.service.open_zone("cmd", "rec-1", "zone-a")
        history = self.service.recovery_receipt_history("rec-1")
        actions = [(row["item_id"], row["action"]) for row in history["receipts"]]
        self.assertIn(("item-debris", "withdraw"), actions)
        self.assertEqual(actions.count(("item-debris", "confirm")), 2)


class RecoveryCapacityTests(RecoveryTestCase):
    def test_capacity_syncs_to_corridor_and_dispatch_uses_it(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        opened = self.service.open_zone("cmd", "rec-1", "zone-a")
        self.assertEqual(opened["capacity_percent"], "66.667")
        restriction = self.connection.execute("SELECT * FROM corridor_restrictions WHERE restriction_id=1").fetchone()
        self.assertEqual(restriction["capacity_percent"], "66.667")
        self.service.submit_dispatch("dispatch", {"dispatch_id": "nom-night", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-26", "requested_units": "80000", "priority": 10, "idempotency_key": "key-night"})
        allocation = self.service.allocate("dispatch", "corridor-east-1", "2026-09-26")
        self.assertEqual(allocation["available_units"], "66667.000")
        self.assertEqual(allocation["allocations"][0]["unfilled_units"], "13333.000")
        self.service.open_zone("cmd", "rec-1", "zone-b")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["state"], "completed")
        self.assertEqual(view["current_capacity_percent"], "100")
        self.service.submit_dispatch("dispatch", {"dispatch_id": "nom-next", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-27", "requested_units": "100000", "priority": 10, "idempotency_key": "key-next"})
        restored = self.service.allocate("dispatch", "corridor-east-1", "2026-09-27")
        self.assertEqual(restored["available_units"], "100000.000")
        syncs = self.connection.execute("SELECT * FROM recovery_capacity_syncs ORDER BY sync_id").fetchall()
        self.assertEqual([row["trigger"] for row in syncs], ["zone-opened", "zone-opened"])


class EmergencyReleaseTests(RecoveryTestCase):
    def test_emergency_release_requires_commander_and_records_reason(self) -> None:
        self.create_plan()
        with self.assertRaises(Forbidden):
            self.service.emergency_release("dispatch", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": "救护车通行", "expires_at": "2026-09-25T23:00:00Z"})
        with self.assertRaises(ValidationFailed):
            self.service.emergency_release("cmd", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": " ", "expires_at": "2026-09-25T23:00:00Z"})
        with self.assertRaises(ValidationFailed):
            self.service.emergency_release("cmd", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": "救护车通行", "expires_at": "2026-09-25T20:00:00Z"})
        release = self.service.emergency_release("cmd", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": "救护车转运绿色通道", "expires_at": "2026-09-25T23:00:00Z"})
        self.assertEqual(release["state"], "active")
        self.assertEqual(release["capacity_percent"], "66.667")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["emergency_releases"][0]["reason"], "救护车转运绿色通道")
        self.assertEqual(view["zones"][0]["lanes"][0]["opened_via"], "emergency")

    def test_expired_release_closes_lanes_and_restores_restriction(self) -> None:
        self.create_plan()
        self.service.emergency_release("cmd", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": "消防车通行", "expires_at": "2026-09-25T22:00:00Z"})
        self.clock.advance(hours=2)
        self.service.submit_dispatch("dispatch", {"dispatch_id": "nom-exp", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-26", "requested_units": "50000", "priority": 10, "idempotency_key": "key-exp"})
        allocation = self.service.allocate("dispatch", "corridor-east-1", "2026-09-26")
        self.assertEqual(allocation["available_units"], "0.000")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["emergency_releases"][0]["state"], "expired")
        self.assertEqual(view["zones"][0]["state"], "closed")
        self.assertEqual(view["current_capacity_percent"], "0.000")

    def test_emergency_lanes_become_permanent_once_items_confirmed(self) -> None:
        self.create_plan()
        self.service.emergency_release("cmd", "rec-1", {"release_id": "er-1", "scope_type": "zone", "scope_id": "zone-a", "reason": "救护车通行", "expires_at": "2026-09-25T22:30:00Z"})
        self.confirm_all_items()
        self.service.open_zone("cmd", "rec-1", "zone-a")
        self.service.open_zone("cmd", "rec-1", "zone-b")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["state"], "completed")
        self.assertEqual(view["emergency_releases"][0]["state"], "closed")
        self.clock.advance(hours=2)
        settled = self.service.recovery_plan("rec-1")
        self.assertEqual(settled["state"], "completed")
        self.assertEqual(settled["current_capacity_percent"], "100")


class HazardTests(RecoveryTestCase):
    def complete_plan(self) -> None:
        self.create_plan()
        self.confirm_all_items()
        self.service.open_zone("cmd", "rec-1", "zone-a")
        self.service.open_zone("cmd", "rec-1", "zone-b")

    def test_hazard_after_recovery_suspends_and_resyncs(self) -> None:
        self.complete_plan()
        self.assertEqual(self.service.recovery_plan("rec-1")["state"], "completed")
        hazard = self.service.report_hazard("dispatch", "rec-1", {"hazard_id": "hz-1", "zone_id": "zone-b", "description": "护栏螺栓缺失存在二次事故风险"})
        self.assertEqual(hazard["plan_state"], "suspended")
        self.assertEqual(hazard["capacity_percent"], "66.667")
        view = self.service.recovery_plan("rec-1")
        self.assertEqual(view["restriction_state"], "active")
        self.assertEqual(view["zones"][1]["state"], "closed")
        with self.assertRaises(InvalidState):
            self.service.open_zone("cmd", "rec-1", "zone-b")
        resolved = self.service.resolve_hazard("cmd", "rec-1", "hz-1")
        self.assertEqual(resolved["plan_state"], "active")
        self.service.open_zone("cmd", "rec-1", "zone-b")
        final = self.service.recovery_plan("rec-1")
        self.assertEqual(final["state"], "completed")
        self.assertEqual(final["hazards"][0]["state"], "resolved")
        self.assertIsNotNone(final["hazards"][0]["resolved_at"])

    def test_hazard_history_is_append_only(self) -> None:
        self.complete_plan()
        self.service.report_hazard("cmd", "rec-1", {"hazard_id": "hz-1", "lane_id": "lane-a1", "description": "路面油污"})
        self.service.resolve_hazard("cmd", "rec-1", "hz-1")
        self.service.report_hazard("cmd", "rec-1", {"hazard_id": "hz-2", "description": "标志牌倾倒"})
        view = self.service.recovery_plan("rec-1")
        self.assertEqual([hazard["hazard_id"] for hazard in view["hazards"]], ["hz-1", "hz-2"])
        self.assertEqual(view["state"], "suspended")
        with self.assertRaises(InvalidState):
            self.service.resolve_hazard("cmd", "rec-1", "hz-1")

    def test_receipts_rejected_after_completion(self) -> None:
        self.complete_plan()
        with self.assertRaises(InvalidState):
            self.service.confirm_check_item("dispatch", "rec-1", "item-debris", {"idempotency_key": "cf-late"})
        with self.assertRaises(InvalidState):
            self.service.withdraw_check_item("dispatch", "rec-1", "item-debris", {"idempotency_key": "wd-late", "note": "太迟"})


class RecoveryMigrationTests(unittest.TestCase):
    def test_legacy_user_table_is_migrated_for_commander_role(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
                CREATE TABLE traffic_users (
                    user_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor')),
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                    created_at TEXT NOT NULL
                );
                INSERT INTO traffic_users(user_id,display_name,role,created_at)
                VALUES('legacy','旧用户','planner','2026-09-01T00:00:00Z');
                """
            )
            initialize(connection)
            initialize(connection)
            service = TrafficDispatchService(connection, FrozenClock(datetime(2026, 9, 25, 21, 0, tzinfo=timezone.utc)))
            created = service.create_user("cmd-new", "指挥员", "commander")
            self.assertEqual(created["role"], "commander")
            legacy = connection.execute("SELECT * FROM traffic_users WHERE user_id='legacy'").fetchone()
            self.assertEqual(legacy["role"], "planner")
        finally:
            connection.close()


class RecoveryApiTests(RecoveryTestCase):
    def test_recovery_routes(self) -> None:
        app = JsonApplication(self.service)
        headers = {"X-Actor-Id": "cmd"}
        created = app.handle("POST", "/recovery_plans", headers, json.dumps(PLAN_PAYLOAD).encode())
        self.assertEqual(created.status, 201)
        confirm = app.handle(
            "POST",
            "/recovery_plans/rec-1/items/item-casualty/confirm",
            {"X-Actor-Id": "dispatch"},
            json.dumps({"idempotency_key": "api-cf-1"}).encode(),
        )
        self.assertEqual(confirm.status, 201)
        blocked = app.handle("POST", "/recovery_plans/rec-1/zones/zone-a/open", headers, b"{}")
        self.assertEqual(blocked.status, 409)
        self.assertEqual(blocked.body["error"]["code"], "invalid_state")
        release = app.handle(
            "POST",
            "/recovery_plans/rec-1/emergency_releases",
            headers,
            json.dumps({"release_id": "er-api", "scope_type": "lane", "scope_id": "lane-a1", "reason": "救护车通行", "expires_at": "2026-09-25T23:30:00Z"}).encode(),
        )
        self.assertEqual(release.status, 201)
        hazard = app.handle(
            "POST",
            "/recovery_plans/rec-1/hazards",
            {"X-Actor-Id": "dispatch"},
            json.dumps({"hazard_id": "hz-api", "description": "散落物反弹"}).encode(),
        )
        self.assertEqual(hazard.status, 201)
        resolved = app.handle("POST", "/recovery_plans/rec-1/hazards/hz-api/resolve", headers, b"{}")
        self.assertEqual(resolved.status, 200)
        view = app.handle("GET", "/recovery_plans/rec-1", headers)
        self.assertEqual(view.status, 200)
        self.assertEqual(view.body["plan_id"], "rec-1")
        receipts = app.handle("GET", "/recovery_plans/rec-1/receipts", headers)
        self.assertEqual(len(receipts.body["receipts"]), 1)
        self.assertTrue(self.service.audit_chain("audit")["valid"])


if __name__ == "__main__":
    unittest.main()
