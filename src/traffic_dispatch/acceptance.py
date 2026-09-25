"""贯通风险指数、道路走廊、应急资源库存、调度申请和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import InvalidState
from .service import TrafficDispatchService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
    service = TrafficDispatchService(connection, clock)
    for user_id, role in (
        ("plan", "planner"),
        ("dispatch", "dispatcher"),
        ("risk", "risk"),
        ("audit", "auditor"),
        ("cmd", "commander"),
    ):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_risk_record("plan", {"risk_index": "COLLISION", "duty_date": f"2026-09-{index}", "index_value": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"center_id": "center-east", "name": "北部事故快处中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
    service.create_facility("plan", {"center_id": "command-center-b", "name": "沿海终端", "kind": "command-center", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
    service.create_route("plan", {"corridor_id": "corridor-east-1", "origin_center_id": "center-east", "destination_center_id": "command-center-b", "response_resource_kind": "patrol-unit", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})
    service.add_inventory_lot("dispatch", {"response_resource_lot_id": "lot-001", "center_id": "center-east", "response_resource_kind": "patrol-unit", "grade": "COLLISION", "quantity_units": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-001", "corridor_id": "corridor-east-1", "incident_id": "medical-center-east", "duty_date": "2026-09-25", "requested_units": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "corridor-east-1", "2026-09-25")
    deployment = service.dispatch_deployment("dispatch", "deployment-001", "nom-001", "lot-001", 2)
    service.create_scenario("plan", {"scenario_id": "road-section-restart", "name": "主干路恢复通行与事故需求回落", "risk_index_drop_percent": "9", "route_capacity_changes": {"corridor-east-1": "20"}, "demand_changes": {"center-east:patrol-unit": "-5"}})
    service.approve_scenario("risk", "road-section-restart", 1)
    scenario = service.run_scenario("plan", "road-section-restart", "2026-09-23")
    recovery = _night_accident_recovery(service, clock)
    result = {"status": "ok", "index": service.risk_summary("COLLISION"), "plan_id": allocation["plan_id"], "deployment": deployment, "scenario_run_id": scenario["run_id"], "recovery": recovery, "audit": service.audit_chain("audit"), "workspace": workspace.name}
    connection.close()
    return result


def _night_accident_recovery(service: TrafficDispatchService, clock: FrozenClock) -> dict[str, object]:
    """夜间重大事故处置后的分阶段恢复：回执、紧急放行、隐患与容量同步。"""
    service.announce_restriction("risk", "corridor-east-1", "2026-09-25T20:00:00Z", None, "0", "夜间重大事故全封闭")
    clock.current = datetime(2026, 9, 25, 21, 0, tzinfo=timezone.utc)
    service.create_recovery_plan("cmd", {
        "plan_id": "rec-night-1",
        "corridor_id": "corridor-east-1",
        "restriction_id": 1,
        "incident_id": "incident-night-1",
        "name": "夜间重大事故分阶段恢复",
        "zones": [
            {"zone_id": "zone-a", "name": "事故点封控区", "sequence": 1, "lanes": [
                {"lane_id": "lane-a1", "name": "内侧车道"},
                {"lane_id": "lane-a2", "name": "外侧车道"},
            ]},
            {"zone_id": "zone-b", "name": "缓冲区", "sequence": 2, "lanes": [
                {"lane_id": "lane-b1", "name": "应急车道"},
            ]},
        ],
        "items": [
            {"item_id": "item-casualty", "kind": "casualty-transfer", "responsible_unit": "医疗急救中心"},
            {"item_id": "item-evidence", "kind": "evidence-collection", "responsible_unit": "事故处理民警队"},
            {"item_id": "item-debris", "kind": "debris-cleanup", "responsible_unit": "道路施救单位"},
            {"item_id": "item-facility", "kind": "facility-inspection", "responsible_unit": "路政设施单位", "zone_id": "zone-b"},
        ],
    })
    blocked = None
    try:
        service.open_zone("cmd", "rec-night-1", "zone-a")
    except InvalidState as exc:
        blocked = str(exc)
    release = service.emergency_release("cmd", "rec-night-1", {"release_id": "er-ambulance", "scope_type": "zone", "scope_id": "zone-a", "reason": "救护车转运绿色通道", "expires_at": "2026-09-25T23:00:00Z"})
    for item_id in ("item-casualty", "item-evidence", "item-debris", "item-facility"):
        service.confirm_check_item("dispatch", "rec-night-1", item_id, {"idempotency_key": f"cf-{item_id}", "note": "负责单位现场确认"})
    duplicate = service.confirm_check_item("dispatch", "rec-night-1", "item-debris", {"idempotency_key": "cf-debris-dup", "note": "施救单位重复上报"})
    service.withdraw_check_item("dispatch", "rec-night-1", "item-debris", {"idempotency_key": "wd-debris", "note": "复核发现遗漏散落物"})
    service.confirm_check_item("dispatch", "rec-night-1", "item-debris", {"idempotency_key": "cf-debris-2", "note": "二次清理完成"})
    service.open_zone("cmd", "rec-night-1", "zone-a")
    service.open_zone("cmd", "rec-night-1", "zone-b")
    completed = service.recovery_plan("rec-night-1")
    hazard = service.report_hazard("dispatch", "rec-night-1", {"hazard_id": "hz-guardrail", "zone_id": "zone-b", "description": "护栏螺栓缺失存在二次事故风险"})
    service.resolve_hazard("cmd", "rec-night-1", "hz-guardrail")
    service.open_zone("cmd", "rec-night-1", "zone-b")
    restored = service.recovery_plan("rec-night-1")
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-recovery", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-26", "requested_units": "100000", "priority": 10, "idempotency_key": "nom-key-recovery"})
    next_day = service.allocate("dispatch", "corridor-east-1", "2026-09-26")
    return {
        "blocked_before_clearance": blocked,
        "emergency_release": {"state": release["state"], "capacity_percent": release["capacity_percent"]},
        "duplicate_receipt": duplicate["duplicate"],
        "receipts": len(service.recovery_receipt_history("rec-night-1")["receipts"]),
        "completed_capacity_percent": completed["current_capacity_percent"],
        "hazard": {"plan_state": hazard["plan_state"], "capacity_percent": hazard["capacity_percent"]},
        "final_state": restored["state"],
        "final_restriction_state": restored["restriction_state"],
        "restored_available_units": next_day["available_units"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行事故快处中心调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
