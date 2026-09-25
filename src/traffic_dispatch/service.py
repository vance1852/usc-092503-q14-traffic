"""风险指数、应急资源库存、道路走廊和调度申请的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    EmergencyReleaseInput,
    HazardReportInput,
    RecoveryPlanInput,
    RiskIndexRecord,
    ResponseCenter,
    ResponseResourceLot,
    DispatchRequest,
    RoadCorridor,
    ResponseScenario,
)
from .planning import (
    AllocationRequest,
    RiskPoint,
    allocate_capacity,
    canonical_json,
    decimal_text,
    delivered_after_loss,
    digest,
    effective_capacity,
    latest_streak,
    moving_average,
    quantize_volume,
    scenario_projection,
    weighted_inventory_cost,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"risk_record.write", "catalog.write", "scenario.write", "scenario.run"},
    "dispatcher": {
        "dispatch_request.write",
        "allocation.run",
        "deployment.write",
        "inventory.write",
        "recovery.receipt",
        "recovery.hazard.write",
    },
    "risk": {"outage.write", "scenario.approve", "report.read"},
    "auditor": {"report.read", "audit.read"},
    "commander": {
        "recovery.write",
        "recovery.receipt",
        "recovery.advance",
        "recovery.release.emergency",
        "recovery.hazard.write",
    },
}


class TrafficDispatchService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM traffic_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM traffic_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO traffic_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO traffic_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    def record_risk_record(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "risk_record.write")
        risk_record = RiskIndexRecord.from_dict(raw)
        previous = self.connection.execute(
            "SELECT risk_record_id,source_revision FROM risk_index_risk_records WHERE risk_index=? AND duty_date=? "
            "ORDER BY risk_record_id DESC LIMIT 1",
            (risk_record.risk_index, risk_record.duty_date),
        ).fetchone()
        if previous is not None and previous["source_revision"] == risk_record.source_revision:
            raise Conflict("同一来源修订已登记")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO risk_index_risk_records(risk_index,duty_date,index_value,source_revision,observed_at,"
                    "supersedes_risk_record_id,recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        risk_record.risk_index,
                        risk_record.duty_date,
                        decimal_text(risk_record.index_value),
                        risk_record.source_revision,
                        risk_record.observed_at,
                        None if previous is None else previous["risk_record_id"],
                        actor_id,
                        self._now(),
                    ),
                )
                risk_record_id = int(cursor.lastrowid)
                self._audit(
                    "risk_record",
                    str(risk_record_id),
                    "risk_record.recorded",
                    actor_id,
                    {"risk_index": risk_record.risk_index, "duty_date": risk_record.duty_date},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("风险指数版本冲突") from exc
        return {"risk_record_id": risk_record_id, "risk_index": risk_record.risk_index, "duty_date": risk_record.duty_date}

    def risk_summary(self, risk_index: str, sessions: int = 20) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT q.duty_date,q.index_value FROM risk_index_risk_records q "
            "JOIN (SELECT duty_date,max(risk_record_id) risk_record_id FROM risk_index_risk_records "
            "WHERE risk_index=? GROUP BY duty_date) latest ON latest.risk_record_id=q.risk_record_id "
            "ORDER BY q.duty_date DESC LIMIT ?",
            (risk_index.upper(), sessions),
        ).fetchall()
        points = [RiskPoint(row["duty_date"], Decimal(row["index_value"])) for row in rows]
        if not points:
            raise NotFound("没有基准风险指数")
        streak = latest_streak(points)
        average = moving_average(points, min(5, len(points)))
        latest = max(points, key=lambda item: item.duty_date)
        return {
            "risk_index": risk_index.upper(),
            "latest": {"duty_date": latest.duty_date, "index_value": decimal_text(latest.close)},
            "latest_streak": None if streak is None else streak.as_dict(),
            "moving_average": None if average is None else decimal_text(average),
            "evidence_items": len(points),
        }

    def create_facility(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        facility = ResponseCenter.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO response_centers(center_id,name,kind,timezone,capacity_units,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        facility.center_id,
                        facility.name,
                        facility.kind,
                        facility.timezone,
                        decimal_text(facility.capacity_units),
                        self._now(),
                    ),
                )
                self._audit("facility", facility.center_id, "facility.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("设施编号已经存在") from exc
        return dict(raw)

    def create_route(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        route = RoadCorridor.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO road_corridors(corridor_id,origin_center_id,destination_center_id,response_resource_kind,hourly_capacity,"
                    "delay_basis_points,response_minutes,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        route.corridor_id,
                        route.origin_center_id,
                        route.destination_center_id,
                        route.response_resource_kind,
                        decimal_text(route.hourly_capacity),
                        route.delay_basis_points,
                        route.response_minutes,
                        self._now(),
                    ),
                )
                self._audit("route", route.corridor_id, "route.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("道路走廊编号冲突或设施不存在") from exc
        return self.route(route.corridor_id)

    def route(self, corridor_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM road_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
        if row is None:
            raise NotFound("道路走廊不存在")
        return dict(row)

    def announce_restriction(
        self,
        actor_id: str,
        corridor_id: str,
        starts_at: str,
        ends_at: str | None,
        capacity_percent: object,
        reason: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "outage.write")
        self.route(corridor_id)
        try:
            start = parse_utc(starts_at, "starts_at")
            end = None if ends_at is None else parse_utc(ends_at, "ends_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        if end is not None and end <= start:
            raise ValidationFailed("ends_at 必须晚于 starts_at")
        percentage = Decimal(str(capacity_percent))
        if percentage < 0 or percentage > 100:
            raise ValidationFailed("capacity_percent 必须在 0 到 100 之间")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO corridor_restrictions(corridor_id,starts_at,ends_at,capacity_percent,reason,created_by,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (corridor_id, utc_text(start), None if end is None else utc_text(end), decimal_text(percentage), reason, actor_id, self._now()),
            )
            restriction_id = int(cursor.lastrowid)
            self._audit("route", corridor_id, "outage.announced", actor_id, {"restriction_id": restriction_id})
        return {"restriction_id": restriction_id, "corridor_id": corridor_id, "state": "announced"}

    def add_inventory_lot(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "inventory.write")
        lot = ResponseResourceLot.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO response_resource_lots(response_resource_lot_id,center_id,response_resource_kind,grade,quantity_units,available_units,"
                    "unit_cost_cny,received_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        lot.response_resource_lot_id,
                        lot.center_id,
                        lot.response_resource_kind,
                        lot.grade,
                        decimal_text(lot.quantity_units),
                        decimal_text(lot.quantity_units),
                        decimal_text(lot.unit_cost_cny),
                        lot.received_at,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("inventory_lot", lot.response_resource_lot_id, "inventory.received", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("应急资源批次冲突或设施不存在") from exc
        return self.inventory_lot(lot.response_resource_lot_id)

    def inventory_lot(self, response_resource_lot_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM response_resource_lots WHERE response_resource_lot_id=?", (response_resource_lot_id,)).fetchone()
        if row is None:
            raise NotFound("应急资源批次不存在")
        return dict(row)

    def inventory_summary(self, center_id: str, response_resource_kind: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM response_resource_lots WHERE center_id=? AND response_resource_kind=? ORDER BY received_at,response_resource_lot_id",
            (center_id, response_resource_kind),
        ).fetchall()
        return {"center_id": center_id, "response_resource_kind": response_resource_kind, **weighted_inventory_cost(rows)}

    def submit_dispatch(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dispatch_request.write")
        dispatch_request = DispatchRequest.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM traffic_idempotency WHERE scope='dispatch_request' AND idempotency_key=?",
            (dispatch_request.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同调度申请内容")
            return json.loads(stored["response_json"])
        route = self.route(dispatch_request.corridor_id)
        if route["state"] != "active":
            raise InvalidState("道路走廊当前不可调度申请")
        response = {
            "dispatch_id": dispatch_request.dispatch_id,
            "corridor_id": dispatch_request.corridor_id,
            "state": "submitted",
            "revision": 1,
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dispatch_requests(dispatch_id,corridor_id,incident_id,duty_date,requested_units,"
                    "priority,idempotency_key,submitted_by,submitted_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        dispatch_request.dispatch_id,
                        dispatch_request.corridor_id,
                        dispatch_request.incident_id,
                        dispatch_request.duty_date,
                        decimal_text(dispatch_request.requested_units),
                        dispatch_request.priority,
                        dispatch_request.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO traffic_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('dispatch_request',?,?,?,?)",
                    (dispatch_request.idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("dispatch_request", dispatch_request.dispatch_id, "dispatch_request.submitted", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("调度申请编号或幂等键冲突") from exc
        return response

    def _capacity_for_date(self, route: sqlite3.Row, duty_date: str) -> Decimal:
        start = duty_date + "T00:00:00Z"
        end = duty_date + "T23:59:59Z"
        rows = self.connection.execute(
            "SELECT capacity_percent FROM corridor_restrictions WHERE corridor_id=? AND state IN ('announced','active') "
            "AND starts_at<=? AND (ends_at IS NULL OR ends_at>=?) ORDER BY restriction_id",
            (route["corridor_id"], end, start),
        ).fetchall()
        percentages = [Decimal(row["capacity_percent"]) for row in rows]
        return effective_capacity(Decimal(route["hourly_capacity"]), percentages)

    def allocate(self, actor_id: str, corridor_id: str, duty_date: str) -> dict[str, Any]:
        self._require(actor_id, "allocation.run")
        route = self.connection.execute("SELECT * FROM road_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
        if route is None:
            raise NotFound("道路走廊不存在")
        self._settle_corridor_releases(actor_id, corridor_id)
        dispatch_requests = self.connection.execute(
            "SELECT * FROM dispatch_requests WHERE corridor_id=? AND duty_date=? AND state='submitted' "
            "ORDER BY priority,submitted_at,dispatch_id",
            (corridor_id, duty_date),
        ).fetchall()
        if not dispatch_requests:
            raise InvalidState("没有待分配调度申请")
        requests = [
            AllocationRequest(
                row["dispatch_id"],
                Decimal(row["requested_units"]),
                int(row["priority"]),
                row["submitted_at"],
            )
            for row in dispatch_requests
        ]
        available = self._capacity_for_date(route, duty_date)
        input_value = [dict(row) for row in dispatch_requests]
        input_sha256 = digest({"route": dict(route), "dispatch_requests": input_value, "capacity": str(available)})
        result_rows = allocate_capacity(available, requests)
        result = {
            "corridor_id": corridor_id,
            "duty_date": duty_date,
            "available_units": decimal_text(available),
            "allocations": result_rows,
        }
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO dispatch_plans(corridor_id,duty_date,input_sha256,available_units,result_json,"
                "created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (corridor_id, duty_date, input_sha256, decimal_text(available), canonical_json(result), actor_id, self._now()),
            )
            for item in result_rows:
                state = "allocated" if Decimal(item["allocated_units"]) > 0 else "cancelled"
                self.connection.execute(
                    "UPDATE dispatch_requests SET allocated_units=?,state=?,revision=revision+1 "
                    "WHERE dispatch_id=? AND state='submitted'",
                    (item["allocated_units"], state, item["dispatch_id"]),
                )
            plan_id = int(cursor.lastrowid)
            self._audit("route", corridor_id, "allocation.completed", actor_id, {"plan_id": plan_id})
        return {"plan_id": plan_id, **result}

    def dispatch_deployment(
        self,
        actor_id: str,
        deployment_id: str,
        dispatch_id: str,
        response_resource_lot_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        self._require(actor_id, "deployment.write")
        dispatch_request = self.connection.execute(
            "SELECT n.*,r.delay_basis_points,r.response_minutes,r.origin_center_id FROM dispatch_requests n "
            "JOIN road_corridors r ON r.corridor_id=n.corridor_id WHERE n.dispatch_id=?",
            (dispatch_id,),
        ).fetchone()
        if dispatch_request is None:
            raise NotFound("调度申请不存在")
        if dispatch_request["state"] != "allocated" or dispatch_request["revision"] != expected_revision:
            raise InvalidState("调度申请不是当前可资源到场版本")
        lot = self.connection.execute("SELECT * FROM response_resource_lots WHERE response_resource_lot_id=?", (response_resource_lot_id,)).fetchone()
        if lot is None:
            raise NotFound("应急资源批次不存在")
        allocated = Decimal(dispatch_request["allocated_units"])
        available = Decimal(lot["available_units"])
        if lot["center_id"] != dispatch_request["origin_center_id"] or lot["response_resource_kind"] != self.route(dispatch_request["corridor_id"])["response_resource_kind"]:
            raise Conflict("应急资源批次与道路走廊起点或电源类型不匹配")
        if available < allocated:
            raise Conflict("应急资源库存不足以完成分配")
        expected_delivery = delivered_after_loss(allocated, int(dispatch_request["delay_basis_points"]))
        departed_at = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE response_resource_lots SET available_units=?,revision=revision+1 WHERE response_resource_lot_id=? AND revision=?",
                (decimal_text(quantize_volume(available - allocated)), response_resource_lot_id, lot["revision"]),
            )
            self.connection.execute(
                "UPDATE dispatch_requests SET state='in_transit',revision=revision+1 WHERE dispatch_id=? AND revision=?",
                (dispatch_id, expected_revision),
            )
            self.connection.execute(
                "INSERT INTO deployments(deployment_id,dispatch_id,inventory_response_resource_lot_id,deployed_units,"
                "expected_arrived_units,departed_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    deployment_id,
                    dispatch_id,
                    response_resource_lot_id,
                    decimal_text(allocated),
                    decimal_text(expected_delivery),
                    departed_at,
                    actor_id,
                    departed_at,
                ),
            )
            self._audit("deployment", deployment_id, "deployment.dispatched", actor_id, {"dispatch_id": dispatch_id})
        return {
            "deployment_id": deployment_id,
            "state": "in_transit",
            "deployed_units": decimal_text(allocated),
            "expected_arrived_units": decimal_text(expected_delivery),
            "expected_arrival": utc_text(parse_utc(departed_at) + timedelta(hours=int(dispatch_request["response_minutes"]))),
        }

    def create_scenario(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "scenario.write")
        scenario = ResponseScenario.from_dict(raw)
        definition = canonical_json(raw)
        content_sha256 = hashlib.sha256(definition.encode("utf-8")).hexdigest()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO response_scenarios(scenario_id,name,definition_json,content_sha256,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (scenario.scenario_id, scenario.name, definition, content_sha256, actor_id, self._now()),
                )
                self._audit("scenario", scenario.scenario_id, "scenario.created", actor_id, {"sha256": content_sha256})
        except sqlite3.IntegrityError as exc:
            raise Conflict("情景编号或内容已经存在") from exc
        return {"scenario_id": scenario.scenario_id, "state": "draft", "sha256": content_sha256}

    def approve_scenario(self, actor_id: str, scenario_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "scenario.approve")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE response_scenarios SET state='approved',revision=revision+1 "
                "WHERE scenario_id=? AND state='draft' AND revision=?",
                (scenario_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("情景不是当前草稿版本")
            self._audit("scenario", scenario_id, "scenario.approved", actor_id, {})
        return {"scenario_id": scenario_id, "state": "approved", "revision": expected_revision + 1}

    def run_scenario(self, actor_id: str, scenario_id: str, as_of_date: str) -> dict[str, Any]:
        self._require(actor_id, "scenario.run")
        row = self.connection.execute(
            "SELECT * FROM response_scenarios WHERE scenario_id=?", (scenario_id,)
        ).fetchone()
        if row is None:
            raise NotFound("情景不存在")
        if row["state"] != "approved":
            raise InvalidState("只有已批准情景可以运行")
        scenario = ResponseScenario.from_dict(json.loads(row["definition_json"]))
        index_row = self.connection.execute(
            "SELECT index_value FROM risk_index_risk_records WHERE duty_date<=? ORDER BY duty_date DESC,risk_record_id DESC LIMIT 1",
            (as_of_date,),
        ).fetchone()
        if index_row is None:
            raise InvalidState("截止日期没有可用风险指数")
        road_corridors = self.connection.execute("SELECT * FROM road_corridors WHERE state='active' ORDER BY corridor_id").fetchall()
        inventory = self.connection.execute(
            "SELECT center_id,response_resource_kind,sum(CAST(available_units AS REAL)) available_units "
            "FROM response_resource_lots GROUP BY center_id,response_resource_kind ORDER BY center_id,response_resource_kind"
        ).fetchall()
        input_value = {
            "scenario_sha256": row["content_sha256"],
            "as_of_date": as_of_date,
            "index": index_row["index_value"],
            "road_corridors": [dict(item) for item in road_corridors],
            "inventory": [dict(item) for item in inventory],
        }
        input_sha256 = digest(input_value)
        existing = self.connection.execute(
            "SELECT run_id,result_json FROM response_scenario_runs WHERE scenario_id=? AND as_of_date=? AND input_sha256=?",
            (scenario_id, as_of_date, input_sha256),
        ).fetchone()
        if existing is not None:
            return {"run_id": existing["run_id"], **json.loads(existing["result_json"]), "replayed": True}
        result = scenario_projection(
            current_index=Decimal(index_row["index_value"]),
            risk_index_drop_percent=scenario.risk_index_drop_percent,
            road_corridors=road_corridors,
            inventory=inventory,
            route_capacity_changes=scenario.route_capacity_changes,
            demand_changes=scenario.demand_changes,
        )
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO response_scenario_runs(scenario_id,as_of_date,input_sha256,result_json,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (scenario_id, as_of_date, input_sha256, canonical_json(result), actor_id, self._now()),
            )
            run_id = int(cursor.lastrowid)
            self._audit("scenario", scenario_id, "scenario.executed", actor_id, {"run_id": run_id})
        return {"run_id": run_id, **result, "replayed": False}

    def create_recovery_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan_input = RecoveryPlanInput.from_dict(raw)
        self.route(plan_input.corridor_id)
        restriction = self.connection.execute(
            "SELECT * FROM corridor_restrictions WHERE restriction_id=?", (plan_input.restriction_id,)
        ).fetchone()
        if restriction is None:
            raise NotFound("道路限制不存在")
        if restriction["corridor_id"] != plan_input.corridor_id:
            raise ValidationFailed("道路限制不属于该道路走廊")
        if restriction["state"] not in ("announced", "active"):
            raise InvalidState("道路限制已关闭或取消，无法建立恢复方案")
        existing = self.connection.execute(
            "SELECT plan_id FROM recovery_plans WHERE restriction_id=? AND state!='completed'",
            (plan_input.restriction_id,),
        ).fetchone()
        if existing is not None:
            raise Conflict("该道路限制已存在进行中的恢复方案")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO recovery_plans(plan_id,corridor_id,restriction_id,incident_id,name,"
                    "base_capacity_percent,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        plan_input.plan_id,
                        plan_input.corridor_id,
                        plan_input.restriction_id,
                        plan_input.incident_id,
                        plan_input.name,
                        restriction["capacity_percent"],
                        actor_id,
                        self._now(),
                    ),
                )
                for zone in plan_input.zones:
                    self.connection.execute(
                        "INSERT INTO recovery_zones(zone_id,plan_id,name,sequence) VALUES(?,?,?,?)",
                        (zone.zone_id, plan_input.plan_id, zone.name, zone.sequence),
                    )
                    for lane in zone.lanes:
                        self.connection.execute(
                            "INSERT INTO recovery_lanes(lane_id,zone_id,name) VALUES(?,?,?)",
                            (lane.lane_id, zone.zone_id, lane.name),
                        )
                for item in plan_input.items:
                    self.connection.execute(
                        "INSERT INTO recovery_check_items(item_id,plan_id,zone_id,kind,responsible_unit,required) "
                        "VALUES(?,?,?,?,?,?)",
                        (
                            item.item_id,
                            plan_input.plan_id,
                            item.zone_id,
                            item.kind,
                            item.responsible_unit,
                            1 if item.required else 0,
                        ),
                    )
                self._audit(
                    "recovery_plan",
                    plan_input.plan_id,
                    "recovery.plan.created",
                    actor_id,
                    {
                        "corridor_id": plan_input.corridor_id,
                        "restriction_id": plan_input.restriction_id,
                        "incident_id": plan_input.incident_id,
                        "zones": len(plan_input.zones),
                        "items": len(plan_input.items),
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("恢复方案、封控区、车道或检查事项编号冲突") from exc
        return self._recovery_plan_view(plan_input.plan_id)

    def _plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM recovery_plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if row is None:
            raise NotFound("恢复方案不存在")
        return row

    def _recovery_plan_view(self, plan_id: str) -> dict[str, Any]:
        plan = self._plan_row(plan_id)
        restriction = self.connection.execute(
            "SELECT state,capacity_percent,ends_at FROM corridor_restrictions WHERE restriction_id=?",
            (plan["restriction_id"],),
        ).fetchone()
        zones: list[dict[str, Any]] = []
        zone_rows = self.connection.execute(
            "SELECT * FROM recovery_zones WHERE plan_id=? ORDER BY sequence", (plan_id,)
        ).fetchall()
        for zone in zone_rows:
            lanes = self.connection.execute(
                "SELECT * FROM recovery_lanes WHERE zone_id=? ORDER BY lane_id", (zone["zone_id"],)
            ).fetchall()
            zones.append({
                "zone_id": zone["zone_id"],
                "name": zone["name"],
                "sequence": zone["sequence"],
                "state": zone["state"],
                "lanes": [
                    {
                        "lane_id": lane["lane_id"],
                        "name": lane["name"],
                        "state": lane["state"],
                        "opened_via": lane["opened_via"],
                        "opened_at": lane["opened_at"],
                    }
                    for lane in lanes
                ],
            })
        items = self.connection.execute(
            "SELECT i.*,(SELECT count(*) FROM recovery_receipts r WHERE r.item_id=i.item_id) receipt_count "
            "FROM recovery_check_items i WHERE i.plan_id=? ORDER BY i.item_id",
            (plan_id,),
        ).fetchall()
        releases = self.connection.execute(
            "SELECT * FROM recovery_emergency_releases WHERE plan_id=? ORDER BY created_at,release_id",
            (plan_id,),
        ).fetchall()
        hazards = self.connection.execute(
            "SELECT * FROM recovery_hazards WHERE plan_id=? ORDER BY created_at,hazard_id", (plan_id,)
        ).fetchall()
        last_sync = self.connection.execute(
            "SELECT * FROM recovery_capacity_syncs WHERE plan_id=? ORDER BY sync_id DESC LIMIT 1", (plan_id,)
        ).fetchone()
        return {
            "plan_id": plan["plan_id"],
            "corridor_id": plan["corridor_id"],
            "restriction_id": plan["restriction_id"],
            "incident_id": plan["incident_id"],
            "name": plan["name"],
            "state": plan["state"],
            "revision": plan["revision"],
            "base_capacity_percent": plan["base_capacity_percent"],
            "current_capacity_percent": restriction["capacity_percent"],
            "restriction_state": restriction["state"],
            "zones": zones,
            "items": [
                {
                    "item_id": item["item_id"],
                    "zone_id": item["zone_id"],
                    "kind": item["kind"],
                    "responsible_unit": item["responsible_unit"],
                    "required": bool(item["required"]),
                    "state": item["state"],
                    "receipts": item["receipt_count"],
                }
                for item in items
            ],
            "emergency_releases": [
                {
                    "release_id": release["release_id"],
                    "scope_type": release["scope_type"],
                    "scope_id": release["scope_id"],
                    "reason": release["reason"],
                    "expires_at": release["expires_at"],
                    "state": release["state"],
                    "created_by": release["created_by"],
                }
                for release in releases
            ],
            "hazards": [
                {
                    "hazard_id": hazard["hazard_id"],
                    "zone_id": hazard["zone_id"],
                    "lane_id": hazard["lane_id"],
                    "description": hazard["description"],
                    "state": hazard["state"],
                    "created_by": hazard["created_by"],
                    "created_at": hazard["created_at"],
                    "resolved_by": hazard["resolved_by"],
                    "resolved_at": hazard["resolved_at"],
                }
                for hazard in hazards
            ],
            "last_capacity_sync": None if last_sync is None else {
                "capacity_percent": last_sync["capacity_percent"],
                "open_lanes": last_sync["open_lanes"],
                "total_lanes": last_sync["total_lanes"],
                "trigger": last_sync["trigger"],
                "created_at": last_sync["created_at"],
            },
        }

    def recovery_plan(self, plan_id: str) -> dict[str, Any]:
        plan = self._plan_row(plan_id)
        with transaction(self.connection, immediate=True):
            self._settle_emergency_releases(plan_id, plan["created_by"])
        return self._recovery_plan_view(plan_id)

    def recovery_receipt_history(self, plan_id: str) -> dict[str, Any]:
        self._plan_row(plan_id)
        rows = self.connection.execute(
            "SELECT r.*,i.kind,i.responsible_unit FROM recovery_receipts r "
            "JOIN recovery_check_items i ON i.item_id=r.item_id WHERE i.plan_id=? ORDER BY r.receipt_id",
            (plan_id,),
        ).fetchall()
        return {
            "plan_id": plan_id,
            "receipts": [
                {
                    "receipt_id": row["receipt_id"],
                    "item_id": row["item_id"],
                    "kind": row["kind"],
                    "responsible_unit": row["responsible_unit"],
                    "action": row["action"],
                    "duplicate": bool(row["duplicate"]),
                    "note": row["note"],
                    "actor_id": row["actor_id"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }

    def _refresh_zone_state(self, zone_id: str) -> None:
        stats = self.connection.execute(
            "SELECT count(*) total,sum(CASE WHEN state='open' THEN 1 ELSE 0 END) open "
            "FROM recovery_lanes WHERE zone_id=?",
            (zone_id,),
        ).fetchone()
        total = int(stats["total"])
        open_lanes = int(stats["open"] or 0)
        state = "open" if open_lanes == total else ("partial" if open_lanes > 0 else "closed")
        self.connection.execute(
            "UPDATE recovery_zones SET state=?,revision=revision+1 WHERE zone_id=? AND state!=?",
            (state, zone_id, state),
        )

    def _settle_emergency_releases(self, plan_id: str, actor_id: str) -> int:
        now = parse_utc(self._now())
        rows = self.connection.execute(
            "SELECT * FROM recovery_emergency_releases WHERE plan_id=? AND state='active' ORDER BY release_id",
            (plan_id,),
        ).fetchall()
        expired = [row for row in rows if parse_utc(row["expires_at"]) <= now]
        if not expired:
            return 0
        affected_zones: set[str] = set()
        for release in expired:
            if release["scope_type"] == "zone":
                zone_id = release["scope_id"]
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='closed',opened_via=NULL,opened_at=NULL,revision=revision+1 "
                    "WHERE zone_id=? AND opened_via='emergency'",
                    (zone_id,),
                )
                affected_zones.add(zone_id)
            else:
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='closed',opened_via=NULL,opened_at=NULL,revision=revision+1 "
                    "WHERE lane_id=? AND opened_via='emergency'",
                    (release["scope_id"],),
                )
                lane_zone = self.connection.execute(
                    "SELECT zone_id FROM recovery_lanes WHERE lane_id=?", (release["scope_id"],)
                ).fetchone()
                if lane_zone is not None:
                    affected_zones.add(lane_zone["zone_id"])
            self.connection.execute(
                "UPDATE recovery_emergency_releases SET state='expired',revision=revision+1 WHERE release_id=?",
                (release["release_id"],),
            )
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery.release.expired",
                actor_id,
                {"release_id": release["release_id"], "scope_type": release["scope_type"], "scope_id": release["scope_id"]},
            )
        for zone_id in sorted(affected_zones):
            self._refresh_zone_state(zone_id)
        self._sync_capacity(plan_id, actor_id, "release-expired")
        return len(expired)

    def _settle_corridor_releases(self, actor_id: str, corridor_id: str) -> None:
        rows = self.connection.execute(
            "SELECT DISTINCT plan_id FROM recovery_emergency_releases WHERE state='active' AND plan_id IN "
            "(SELECT plan_id FROM recovery_plans WHERE corridor_id=?)",
            (corridor_id,),
        ).fetchall()
        if not rows:
            return
        with transaction(self.connection, immediate=True):
            for row in rows:
                self._settle_emergency_releases(row["plan_id"], actor_id)

    def _sync_capacity(self, plan_id: str, actor_id: str, trigger: str) -> str:
        plan = self._plan_row(plan_id)
        stats = self.connection.execute(
            "SELECT count(*) total,sum(CASE WHEN l.state='open' THEN 1 ELSE 0 END) open "
            "FROM recovery_lanes l JOIN recovery_zones z ON z.zone_id=l.zone_id WHERE z.plan_id=?",
            (plan_id,),
        ).fetchone()
        total = int(stats["total"])
        open_lanes = int(stats["open"] or 0)
        base = Decimal(plan["base_capacity_percent"])
        fraction = Decimal(open_lanes) / Decimal(total)
        percent = (base + (Decimal("100") - base) * fraction).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        incomplete = self.connection.execute(
            "SELECT count(*) FROM recovery_check_items WHERE plan_id=? AND required=1 AND state!='completed'",
            (plan_id,),
        ).fetchone()[0]
        open_hazards = self.connection.execute(
            "SELECT count(*) FROM recovery_hazards WHERE plan_id=? AND state='open'", (plan_id,)
        ).fetchone()[0]
        restriction = self.connection.execute(
            "SELECT * FROM corridor_restrictions WHERE restriction_id=?", (plan["restriction_id"],)
        ).fetchone()
        complete = open_lanes == total and incomplete == 0 and open_hazards == 0
        now = self._now()
        if complete:
            if plan["state"] == "completed":
                return decimal_text(percent)
            self.connection.execute(
                "UPDATE recovery_lanes SET opened_via='phase',revision=revision+1 "
                "WHERE opened_via='emergency' AND zone_id IN (SELECT zone_id FROM recovery_zones WHERE plan_id=?)",
                (plan_id,),
            )
            self.connection.execute(
                "UPDATE recovery_emergency_releases SET state='closed',revision=revision+1 "
                "WHERE plan_id=? AND state='active'",
                (plan_id,),
            )
            self.connection.execute(
                "UPDATE recovery_plans SET state='completed',revision=revision+1 WHERE plan_id=?",
                (plan_id,),
            )
            self.connection.execute(
                "UPDATE corridor_restrictions SET state='closed',ends_at=?,capacity_percent='100',revision=revision+1 "
                "WHERE restriction_id=?",
                (now, plan["restriction_id"]),
            )
            self._audit("recovery_plan", plan_id, "recovery.plan.completed", actor_id, {"trigger": trigger})
        else:
            if plan["state"] == "completed":
                self.connection.execute(
                    "UPDATE recovery_plans SET state='suspended',revision=revision+1 WHERE plan_id=?",
                    (plan_id,),
                )
            state_sql = restriction["state"]
            ends_at = restriction["ends_at"]
            if state_sql == "closed":
                state_sql = "active"
                ends_at = None
            if (
                Decimal(restriction["capacity_percent"]) == percent
                and state_sql == restriction["state"]
            ):
                return decimal_text(percent)
            self.connection.execute(
                "UPDATE corridor_restrictions SET capacity_percent=?,state=?,ends_at=?,revision=revision+1 "
                "WHERE restriction_id=?",
                (decimal_text(percent), state_sql, ends_at, plan["restriction_id"]),
            )
        self.connection.execute(
            "INSERT INTO recovery_capacity_syncs(plan_id,corridor_id,restriction_id,capacity_percent,"
            "open_lanes,total_lanes,trigger,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                plan_id,
                plan["corridor_id"],
                plan["restriction_id"],
                "100" if complete else decimal_text(percent),
                open_lanes,
                total,
                trigger,
                actor_id,
                now,
            ),
        )
        self._audit(
            "recovery_plan",
            plan_id,
            "recovery.capacity.synced",
            actor_id,
            {
                "trigger": trigger,
                "capacity_percent": "100" if complete else decimal_text(percent),
                "open_lanes": open_lanes,
                "total_lanes": total,
            },
        )
        return "100" if complete else decimal_text(percent)

    def _incomplete_required_items(self, plan_id: str, zone_id: str | None = None) -> list[sqlite3.Row]:
        if zone_id is None:
            return self.connection.execute(
                "SELECT * FROM recovery_check_items WHERE plan_id=? AND required=1 AND state!='completed' "
                "ORDER BY item_id",
                (plan_id,),
            ).fetchall()
        return self.connection.execute(
            "SELECT * FROM recovery_check_items WHERE plan_id=? AND required=1 AND state!='completed' "
            "AND (zone_id IS NULL OR zone_id=?) ORDER BY item_id",
            (plan_id, zone_id),
        ).fetchall()

    def confirm_check_item(self, actor_id: str, plan_id: str, item_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.receipt")
        key = raw.get("idempotency_key")
        if not isinstance(key, str) or not key.strip():
            raise ValidationFailed("idempotency_key 不能为空")
        key = key.strip()
        note = str(raw.get("note", "")).strip()
        request_digest = digest({"action": "confirm", "item_id": item_id, "plan_id": plan_id, "note": note})
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM traffic_idempotency WHERE scope='recovery_receipt' AND idempotency_key=?",
            (key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同回执内容")
            return json.loads(stored["response_json"])
        self._plan_row(plan_id)
        item = self.connection.execute(
            "SELECT * FROM recovery_check_items WHERE item_id=? AND plan_id=?", (item_id, plan_id)
        ).fetchone()
        if item is None:
            raise NotFound("检查事项不存在")
        try:
            with transaction(self.connection, immediate=True):
                self._settle_emergency_releases(plan_id, actor_id)
                plan = self._plan_row(plan_id)
                if plan["state"] == "completed":
                    raise InvalidState("恢复方案已完结，恢复后问题请通过隐患上报处理")
                duplicate = item["state"] == "completed"
                cursor = self.connection.execute(
                    "INSERT INTO recovery_receipts(item_id,action,duplicate,note,idempotency_key,actor_id,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (item_id, "confirm", 1 if duplicate else 0, note, key, actor_id, self._now()),
                )
                receipt_id = int(cursor.lastrowid)
                if not duplicate:
                    self.connection.execute(
                        "UPDATE recovery_check_items SET state='completed',revision=revision+1 WHERE item_id=?",
                        (item_id,),
                    )
                response = {
                    "receipt_id": receipt_id,
                    "plan_id": plan_id,
                    "item_id": item_id,
                    "state": "completed",
                    "duplicate": duplicate,
                }
                self.connection.execute(
                    "INSERT INTO traffic_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('recovery_receipt',?,?,?,?)",
                    (key, request_digest, canonical_json(response), self._now()),
                )
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery.receipt.duplicated" if duplicate else "recovery.item.confirmed",
                    actor_id,
                    {
                        "item_id": item_id,
                        "kind": item["kind"],
                        "responsible_unit": item["responsible_unit"],
                        "receipt_id": receipt_id,
                    },
                )
                if not duplicate:
                    self._sync_capacity(plan_id, actor_id, "item-confirmed")
        except sqlite3.IntegrityError as exc:
            raise Conflict("回执幂等键冲突") from exc
        return response

    def withdraw_check_item(self, actor_id: str, plan_id: str, item_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.receipt")
        key = raw.get("idempotency_key")
        if not isinstance(key, str) or not key.strip():
            raise ValidationFailed("idempotency_key 不能为空")
        key = key.strip()
        note = str(raw.get("note", "")).strip()
        if not note:
            raise ValidationFailed("撤回必须填写原因")
        request_digest = digest({"action": "withdraw", "item_id": item_id, "plan_id": plan_id, "note": note})
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM traffic_idempotency WHERE scope='recovery_receipt' AND idempotency_key=?",
            (key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同回执内容")
            return json.loads(stored["response_json"])
        self._plan_row(plan_id)
        item = self.connection.execute(
            "SELECT * FROM recovery_check_items WHERE item_id=? AND plan_id=?", (item_id, plan_id)
        ).fetchone()
        if item is None:
            raise NotFound("检查事项不存在")
        try:
            with transaction(self.connection, immediate=True):
                self._settle_emergency_releases(plan_id, actor_id)
                plan = self._plan_row(plan_id)
                if plan["state"] == "completed":
                    raise InvalidState("恢复方案已完结，恢复后问题请通过隐患上报处理")
                if item["state"] != "completed":
                    raise InvalidState("检查事项未处于已完成状态，无法撤回")
                cursor = self.connection.execute(
                    "INSERT INTO recovery_receipts(item_id,action,duplicate,note,idempotency_key,actor_id,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (item_id, "withdraw", 0, note, key, actor_id, self._now()),
                )
                receipt_id = int(cursor.lastrowid)
                self.connection.execute(
                    "UPDATE recovery_check_items SET state='pending',revision=revision+1 WHERE item_id=?",
                    (item_id,),
                )
                response = {
                    "receipt_id": receipt_id,
                    "plan_id": plan_id,
                    "item_id": item_id,
                    "state": "pending",
                    "duplicate": False,
                }
                self.connection.execute(
                    "INSERT INTO traffic_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('recovery_receipt',?,?,?,?)",
                    (key, request_digest, canonical_json(response), self._now()),
                )
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery.item.withdrawn",
                    actor_id,
                    {"item_id": item_id, "kind": item["kind"], "receipt_id": receipt_id, "note": note},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("回执幂等键冲突") from exc
        return response

    def open_zone(self, actor_id: str, plan_id: str, zone_id: str) -> dict[str, Any]:
        self._require(actor_id, "recovery.advance")
        self._plan_row(plan_id)
        zone = self.connection.execute(
            "SELECT * FROM recovery_zones WHERE zone_id=? AND plan_id=?", (zone_id, plan_id)
        ).fetchone()
        if zone is None:
            raise NotFound("封控区不存在")
        with transaction(self.connection, immediate=True):
            self._settle_emergency_releases(plan_id, actor_id)
            plan = self._plan_row(plan_id)
            if plan["state"] == "completed":
                raise InvalidState("恢复方案已完结")
            if plan["state"] == "suspended":
                raise InvalidState("存在未处置隐患，恢复方案已挂起")
            pending_lanes = self.connection.execute(
                "SELECT count(*) FROM recovery_lanes WHERE zone_id=? AND (state!='open' OR opened_via!='phase')",
                (zone_id,),
            ).fetchone()[0]
            if pending_lanes == 0:
                raise InvalidState("封控区已开放")
            blocking = self.connection.execute(
                "SELECT zone_id FROM recovery_zones WHERE plan_id=? AND sequence<? AND state!='open'",
                (plan_id, zone["sequence"]),
            ).fetchone()
            if blocking is not None:
                raise InvalidState("必须按阶段顺序恢复，前一阶段封控区尚未开放")
            incomplete = self._incomplete_required_items(plan_id, zone_id)
            if incomplete:
                missing = ",".join(row["item_id"] for row in incomplete)
                raise InvalidState(f"必需检查事项未完成，不得扩大通行范围: {missing}")
            now = self._now()
            self.connection.execute(
                "UPDATE recovery_lanes SET state='open',opened_via='phase',opened_at=?,revision=revision+1 "
                "WHERE zone_id=? AND state!='open'",
                (now, zone_id),
            )
            self.connection.execute(
                "UPDATE recovery_lanes SET opened_via='phase',revision=revision+1 "
                "WHERE zone_id=? AND opened_via='emergency'",
                (zone_id,),
            )
            self._refresh_zone_state(zone_id)
            capacity = self._sync_capacity(plan_id, actor_id, "zone-opened")
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery.zone.opened",
                actor_id,
                {"zone_id": zone_id, "sequence": zone["sequence"]},
            )
        return {"plan_id": plan_id, "zone_id": zone_id, "zone_state": "open", "capacity_percent": capacity}

    def emergency_release(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.release.emergency")
        release_input = EmergencyReleaseInput.from_dict(raw)
        self._plan_row(plan_id)
        expires = parse_utc(release_input.expires_at, "expires_at")
        if expires <= parse_utc(self._now()):
            raise ValidationFailed("expires_at 必须晚于当前时间")
        with transaction(self.connection, immediate=True):
            self._settle_emergency_releases(plan_id, actor_id)
            plan = self._plan_row(plan_id)
            if plan["state"] == "completed":
                raise InvalidState("恢复方案已完结")
            if release_input.scope_type == "zone":
                scope = self.connection.execute(
                    "SELECT z.zone_id id FROM recovery_zones z WHERE z.zone_id=? AND z.plan_id=?",
                    (release_input.scope_id, plan_id),
                ).fetchone()
            else:
                scope = self.connection.execute(
                    "SELECT l.lane_id id FROM recovery_lanes l JOIN recovery_zones z ON z.zone_id=l.zone_id "
                    "WHERE l.lane_id=? AND z.plan_id=?",
                    (release_input.scope_id, plan_id),
                ).fetchone()
            if scope is None:
                raise NotFound("放行范围不在恢复方案内")
            now = self._now()
            try:
                self.connection.execute(
                    "INSERT INTO recovery_emergency_releases(release_id,plan_id,scope_type,scope_id,reason,"
                    "expires_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        release_input.release_id,
                        plan_id,
                        release_input.scope_type,
                        release_input.scope_id,
                        release_input.reason,
                        utc_text(expires),
                        actor_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("紧急放行编号冲突") from exc
            if release_input.scope_type == "zone":
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='open',opened_via='emergency',opened_at=?,revision=revision+1 "
                    "WHERE zone_id=? AND state!='open'",
                    (now, release_input.scope_id),
                )
                self._refresh_zone_state(release_input.scope_id)
            else:
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='open',opened_via='emergency',opened_at=?,revision=revision+1 "
                    "WHERE lane_id=? AND state!='open'",
                    (now, release_input.scope_id),
                )
                lane_zone = self.connection.execute(
                    "SELECT zone_id FROM recovery_lanes WHERE lane_id=?", (release_input.scope_id,)
                ).fetchone()
                self._refresh_zone_state(lane_zone["zone_id"])
            capacity = self._sync_capacity(plan_id, actor_id, "emergency-release")
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery.release.emergency",
                actor_id,
                {
                    "release_id": release_input.release_id,
                    "scope_type": release_input.scope_type,
                    "scope_id": release_input.scope_id,
                    "reason": release_input.reason,
                    "expires_at": utc_text(expires),
                },
            )
        return {
            "release_id": release_input.release_id,
            "plan_id": plan_id,
            "state": "active",
            "expires_at": utc_text(expires),
            "capacity_percent": capacity,
        }

    def report_hazard(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.hazard.write")
        hazard_input = HazardReportInput.from_dict(raw)
        self._plan_row(plan_id)
        with transaction(self.connection, immediate=True):
            self._settle_emergency_releases(plan_id, actor_id)
            plan = self._plan_row(plan_id)
            if hazard_input.zone_id is not None:
                zone = self.connection.execute(
                    "SELECT zone_id FROM recovery_zones WHERE zone_id=? AND plan_id=?",
                    (hazard_input.zone_id, plan_id),
                ).fetchone()
                if zone is None:
                    raise NotFound("隐患封控区不在恢复方案内")
            if hazard_input.lane_id is not None:
                lane = self.connection.execute(
                    "SELECT l.zone_id FROM recovery_lanes l JOIN recovery_zones z ON z.zone_id=l.zone_id "
                    "WHERE l.lane_id=? AND z.plan_id=?",
                    (hazard_input.lane_id, plan_id),
                ).fetchone()
                if lane is None:
                    raise NotFound("隐患车道不在恢复方案内")
                if hazard_input.zone_id is not None and lane["zone_id"] != hazard_input.zone_id:
                    raise ValidationFailed("隐患车道不属于指定封控区")
            now = self._now()
            try:
                self.connection.execute(
                    "INSERT INTO recovery_hazards(hazard_id,plan_id,zone_id,lane_id,description,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        hazard_input.hazard_id,
                        plan_id,
                        hazard_input.zone_id,
                        hazard_input.lane_id,
                        hazard_input.description,
                        actor_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise Conflict("隐患编号冲突") from exc
            affected_zones: set[str] = set()
            if hazard_input.lane_id is not None:
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='closed',opened_via=NULL,opened_at=NULL,revision=revision+1 "
                    "WHERE lane_id=?",
                    (hazard_input.lane_id,),
                )
                lane_zone = self.connection.execute(
                    "SELECT zone_id FROM recovery_lanes WHERE lane_id=?", (hazard_input.lane_id,)
                ).fetchone()
                affected_zones.add(lane_zone["zone_id"])
            elif hazard_input.zone_id is not None:
                self.connection.execute(
                    "UPDATE recovery_lanes SET state='closed',opened_via=NULL,opened_at=NULL,revision=revision+1 "
                    "WHERE zone_id=?",
                    (hazard_input.zone_id,),
                )
                affected_zones.add(hazard_input.zone_id)
            for zone_id in sorted(affected_zones):
                self._refresh_zone_state(zone_id)
            if plan["state"] != "suspended":
                self.connection.execute(
                    "UPDATE recovery_plans SET state='suspended',revision=revision+1 WHERE plan_id=?",
                    (plan_id,),
                )
            capacity = self._sync_capacity(plan_id, actor_id, "hazard")
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery.hazard.reported",
                actor_id,
                {
                    "hazard_id": hazard_input.hazard_id,
                    "zone_id": hazard_input.zone_id,
                    "lane_id": hazard_input.lane_id,
                    "description": hazard_input.description,
                },
            )
        return {
            "hazard_id": hazard_input.hazard_id,
            "plan_id": plan_id,
            "state": "open",
            "plan_state": "suspended",
            "capacity_percent": capacity,
        }

    def resolve_hazard(self, actor_id: str, plan_id: str, hazard_id: str) -> dict[str, Any]:
        self._require(actor_id, "recovery.hazard.write")
        self._plan_row(plan_id)
        hazard = self.connection.execute(
            "SELECT * FROM recovery_hazards WHERE hazard_id=? AND plan_id=?", (hazard_id, plan_id)
        ).fetchone()
        if hazard is None:
            raise NotFound("隐患不存在")
        with transaction(self.connection, immediate=True):
            self._settle_emergency_releases(plan_id, actor_id)
            if hazard["state"] != "open":
                raise InvalidState("隐患已处置")
            now = self._now()
            self.connection.execute(
                "UPDATE recovery_hazards SET state='resolved',resolved_by=?,resolved_at=?,revision=revision+1 "
                "WHERE hazard_id=?",
                (actor_id, now, hazard_id),
            )
            remaining = self.connection.execute(
                "SELECT count(*) FROM recovery_hazards WHERE plan_id=? AND state='open'", (plan_id,)
            ).fetchone()[0]
            plan_state = self._plan_row(plan_id)["state"]
            if remaining == 0 and plan_state == "suspended":
                self.connection.execute(
                    "UPDATE recovery_plans SET state='active',revision=revision+1 WHERE plan_id=?",
                    (plan_id,),
                )
                plan_state = "active"
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery.hazard.resolved",
                actor_id,
                {"hazard_id": hazard_id},
            )
        return {"hazard_id": hazard_id, "plan_id": plan_id, "state": "resolved", "plan_state": plan_state}

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM traffic_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
