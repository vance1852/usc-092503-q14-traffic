"""贯通风险指数、道路走廊、应急资源库存、调度申请和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import TrafficDispatchService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = TrafficDispatchService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("command", "commander"), ("audit", "auditor")):
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
    restriction = service.announce_restriction("risk", "corridor-east-1", "2026-09-24T20:00:00Z", None, "25", "夜间重大事故封控")
    service.create_recovery_plan("command", {
        "plan_id": "rec-001",
        "corridor_id": "corridor-east-1",
        "restriction_id": restriction["restriction_id"],
        "incident_id": "incident-night-1",
        "stages": [
            {
                "stage_id": "rec-001-s1",
                "sequence": 1,
                "zone_label": "封控区A",
                "lane_codes": ["L1", "L2"],
                "restored_capacity_percent": "60",
                "items": [
                    {"item_id": "rec-001-s1-casualty", "item_kind": "casualty_transport", "responsible_unit": "市急救中心", "required": True},
                    {"item_id": "rec-001-s1-debris", "item_kind": "debris_cleanup", "responsible_unit": "路政养护一队", "required": True},
                ],
            },
            {
                "stage_id": "rec-001-s2",
                "sequence": 2,
                "zone_label": "封控区B",
                "lane_codes": ["L3"],
                "restored_capacity_percent": "100",
                "items": [
                    {"item_id": "rec-001-s2-evidence", "item_kind": "evidence_collection", "responsible_unit": "事故处理中队", "required": True},
                    {"item_id": "rec-001-s2-facility", "item_kind": "facility_inspection", "responsible_unit": "设施巡检班", "required": True},
                ],
            },
        ],
    })
    service.activate_recovery_plan("command", "rec-001", 1)
    service.confirm_recovery_item("command", "rec-001", "rec-001-s1-casualty", "receipt-casualty-1", "伤员已全部转运")
    service.confirm_recovery_item("command", "rec-001", "rec-001-s1-debris", "receipt-debris-1", "散落物清理完毕")
    service.confirm_recovery_item("command", "rec-001", "rec-001-s1-debris", "receipt-debris-1", "重复上报清理完毕")
    service.release_recovery_stage("command", "rec-001", "rec-001-s1")
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-002", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-26", "requested_units": "50000", "priority": 10, "idempotency_key": "nom-key-002"})
    partial_allocation = service.allocate("dispatch", "corridor-east-1", "2026-09-26")
    service.confirm_recovery_item("command", "rec-001", "rec-001-s2-evidence", "receipt-evidence-1", "现场证据采集完成")
    emergency = service.create_emergency_release("command", "rec-001", "rec-001-s2", "救护车辆需优先离场", "2026-09-25T08:00:00Z")
    service.release_recovery_stage("command", "rec-001", "rec-001-s2", emergency["release_id"])
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-003", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-27", "requested_units": "50000", "priority": 10, "idempotency_key": "nom-key-003"})
    restored_allocation = service.allocate("dispatch", "corridor-east-1", "2026-09-27")
    service.report_recovery_hazard("command", "rec-001", {"description": "恢复通行后发现护栏螺栓缺失", "capacity_percent": "40", "stage_id": "rec-001-s2"})
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-004", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-28", "requested_units": "40000", "priority": 10, "idempotency_key": "nom-key-004"})
    hazard_allocation = service.allocate("dispatch", "corridor-east-1", "2026-09-28")
    service.release_recovery_stage("command", "rec-001", "rec-001-s1")
    followup = service.create_emergency_release("command", "rec-001", "rec-001-s2", "设施复检完成前临时放行", "2026-09-26T08:00:00Z")
    service.release_recovery_stage("command", "rec-001", "rec-001-s2", followup["release_id"])
    service.submit_dispatch("dispatch", {"dispatch_id": "nom-005", "corridor_id": "corridor-east-1", "incident_id": "incident-night-1", "duty_date": "2026-09-29", "requested_units": "50000", "priority": 10, "idempotency_key": "nom-key-005"})
    final_allocation = service.allocate("dispatch", "corridor-east-1", "2026-09-29")
    recovery = service.recovery_plan("rec-001")
    result = {
        "status": "ok",
        "index": service.risk_summary("COLLISION"),
        "plan_id": allocation["plan_id"],
        "deployment": deployment,
        "scenario_run_id": scenario["run_id"],
        "recovery": {
            "plan_id": recovery["plan_id"],
            "state": recovery["state"],
            "stages": [{"stage_id": stage["stage_id"], "state": stage["state"]} for stage in recovery["stages"]],
            "duplicate_receipts": sum(1 for receipt in recovery["receipts"] if receipt["duplicate"]),
            "emergency_releases": len(recovery["emergency_releases"]),
            "hazards": len(recovery["hazards"]),
            "capacity_progression": {
                "partial": partial_allocation["available_units"],
                "restored": restored_allocation["available_units"],
                "hazard": hazard_allocation["available_units"],
                "final": final_allocation["available_units"],
            },
        },
        "audit": service.audit_chain("audit"),
        "workspace": workspace.name,
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行事故快处中心调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
