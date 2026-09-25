"""风险指数、应急资源库存、道路走廊和调度申请的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    DispatchRequest,
    RecoveryPlanInput,
    ResponseCenter,
    ResponseResourceLot,
    RiskIndexRecord,
    RoadCorridor,
    ResponseScenario,
    date_text,
    decimal_value,
    identifier,
    required_text,
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
    "dispatcher": {"dispatch_request.write", "allocation.run", "deployment.write", "inventory.write"},
    "risk": {"outage.write", "scenario.approve", "report.read"},
    "commander": {"recovery.write", "recovery.override", "report.read"},
    "auditor": {"report.read", "audit.read"},
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

    def _recovery_plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM recovery_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFound("恢复方案不存在")
        return row

    def _restriction_row(self, restriction_id: int) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM corridor_restrictions WHERE restriction_id=?", (restriction_id,)
        ).fetchone()
        if row is None:
            raise NotFound("道路限制不存在")
        return row

    def _recovery_stage_row(self, plan_id: str, stage_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM recovery_stages WHERE plan_id=? AND stage_id=?", (plan_id, stage_id)
        ).fetchone()
        if row is None:
            raise NotFound("恢复阶段不存在")
        return row

    def _recovery_item_row(self, plan_id: str, item_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT i.* FROM recovery_check_items i JOIN recovery_stages s ON s.stage_id=i.stage_id "
            "WHERE s.plan_id=? AND i.item_id=?",
            (plan_id, item_id),
        ).fetchone()
        if row is None:
            raise NotFound("检查事项不存在")
        return row

    def create_recovery_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan = RecoveryPlanInput.from_dict(raw)
        self.route(plan.corridor_id)
        restriction = self._restriction_row(plan.restriction_id)
        if restriction["corridor_id"] != plan.corridor_id:
            raise ValidationFailed("道路限制不属于该道路走廊")
        if restriction["state"] not in ("announced", "active"):
            raise InvalidState("道路限制已关闭，无法建立恢复方案")
        existing = self.connection.execute(
            "SELECT plan_id FROM recovery_plans WHERE restriction_id=? AND state IN ('draft','active','reopened')",
            (plan.restriction_id,),
        ).fetchone()
        if existing is not None:
            raise Conflict("该道路限制已有进行中的恢复方案")
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO recovery_plans(plan_id,corridor_id,restriction_id,incident_id,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (plan.plan_id, plan.corridor_id, plan.restriction_id, plan.incident_id, actor_id, now),
                )
                for stage in plan.stages:
                    self.connection.execute(
                        "INSERT INTO recovery_stages(stage_id,plan_id,sequence,zone_label,lane_codes_json,"
                        "restored_capacity_percent,created_at) VALUES(?,?,?,?,?,?,?)",
                        (
                            stage.stage_id,
                            plan.plan_id,
                            stage.sequence,
                            stage.zone_label,
                            json.dumps(list(stage.lane_codes), ensure_ascii=False),
                            decimal_text(stage.restored_capacity_percent),
                            now,
                        ),
                    )
                    for item in stage.items:
                        self.connection.execute(
                            "INSERT INTO recovery_check_items(item_id,stage_id,item_kind,responsible_unit,required,created_at) "
                            "VALUES(?,?,?,?,?,?)",
                            (item.item_id, stage.stage_id, item.item_kind, item.responsible_unit, 1 if item.required else 0, now),
                        )
                self._audit(
                    "recovery_plan",
                    plan.plan_id,
                    "recovery_plan.created",
                    actor_id,
                    {"corridor_id": plan.corridor_id, "restriction_id": plan.restriction_id, "stages": len(plan.stages)},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("恢复方案、阶段或检查事项编号冲突") from exc
        return self.recovery_plan(plan.plan_id)

    def activate_recovery_plan(self, actor_id: str, plan_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] != "draft":
            raise InvalidState("恢复方案不是草稿状态")
        restriction = self._restriction_row(plan["restriction_id"])
        if restriction["state"] not in ("announced", "active"):
            raise InvalidState("道路限制已关闭，无法启动恢复方案")
        first = self.connection.execute(
            "SELECT restored_capacity_percent FROM recovery_stages WHERE plan_id=? ORDER BY sequence LIMIT 1",
            (plan_id,),
        ).fetchone()
        if Decimal(first["restored_capacity_percent"]) <= Decimal(restriction["capacity_percent"]):
            raise ValidationFailed("首个阶段恢复比例必须高于当前限制通行比例")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE recovery_plans SET state='active',activated_at=?,revision=revision+1 "
                "WHERE plan_id=? AND state='draft' AND revision=?",
                (self._now(), plan_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("恢复方案不是当前草稿版本")
            self._audit("recovery_plan", plan_id, "recovery_plan.activated", actor_id, {})
        return {"plan_id": plan_id, "state": "active", "revision": expected_revision + 1}

    def confirm_recovery_item(
        self, actor_id: str, plan_id: str, item_id: str, receipt_key: str, note: str
    ) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] not in ("active", "reopened"):
            raise InvalidState("恢复方案未在进行中，不能登记回执")
        item = self._recovery_item_row(plan_id, item_id)
        key = identifier(receipt_key, "receipt_key")
        note_text = required_text(note, "note")
        same_key = self.connection.execute(
            "SELECT r.item_id FROM recovery_receipts r JOIN recovery_check_items i ON i.item_id=r.item_id "
            "JOIN recovery_stages s ON s.stage_id=i.stage_id WHERE r.receipt_key=? AND s.plan_id=?",
            (key, plan_id),
        ).fetchall()
        if any(row["item_id"] != item_id for row in same_key):
            raise Conflict("回执编号已用于其他检查事项")
        duplicate = bool(same_key) or item["state"] == "confirmed"
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO recovery_receipts(item_id,receipt_key,note,duplicate,confirmed_by,confirmed_at) "
                "VALUES(?,?,?,?,?,?)",
                (item_id, key, note_text, 1 if duplicate else 0, actor_id, self._now()),
            )
            receipt_id = int(cursor.lastrowid)
            if duplicate:
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery_item.receipt_duplicated",
                    actor_id,
                    {"item_id": item_id, "receipt_id": receipt_id, "receipt_key": key},
                )
            else:
                self.connection.execute(
                    "UPDATE recovery_check_items SET state='confirmed',revision=revision+1 "
                    "WHERE item_id=? AND state='pending'",
                    (item_id,),
                )
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery_item.confirmed",
                    actor_id,
                    {"item_id": item_id, "receipt_id": receipt_id},
                )
        return {"item_id": item_id, "state": "confirmed", "receipt_id": receipt_id, "duplicate": duplicate}

    def withdraw_recovery_item(self, actor_id: str, plan_id: str, item_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] not in ("active", "reopened"):
            raise InvalidState("恢复方案未在进行中，不能撤回事项")
        item = self._recovery_item_row(plan_id, item_id)
        if item["state"] != "confirmed":
            raise InvalidState("检查事项尚未确认，无法撤回")
        reason_text = required_text(reason, "reason")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE recovery_check_items SET state='pending',revision=revision+1 "
                "WHERE item_id=? AND state='confirmed'",
                (item_id,),
            )
            cursor = self.connection.execute(
                "INSERT INTO recovery_withdrawals(item_id,reason,withdrawn_by,withdrawn_at) VALUES(?,?,?,?)",
                (item_id, reason_text, actor_id, self._now()),
            )
            withdrawal_id = int(cursor.lastrowid)
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery_item.withdrawn",
                actor_id,
                {"item_id": item_id, "withdrawal_id": withdrawal_id},
            )
        return {"item_id": item_id, "state": "pending", "withdrawal_id": withdrawal_id}

    def create_emergency_release(
        self, actor_id: str, plan_id: str, stage_id: str, reason: str, expires_at: str
    ) -> dict[str, Any]:
        self._require(actor_id, "recovery.override")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] not in ("active", "reopened"):
            raise InvalidState("恢复方案未在进行中，不能紧急放行")
        stage = self._recovery_stage_row(plan_id, stage_id)
        if stage["state"] != "pending":
            raise InvalidState("阶段已解除封控，无需紧急放行")
        reason_text = required_text(reason, "reason")
        try:
            expiry = parse_utc(required_text(expires_at, "expires_at", 40), "expires_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        if expiry <= self.clock.now():
            raise ValidationFailed("expires_at 必须晚于当前时间")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO recovery_emergency_releases(stage_id,reason,expires_at,created_by,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_id, reason_text, utc_text(expiry), actor_id, self._now()),
            )
            release_id = int(cursor.lastrowid)
            self._audit(
                "recovery_plan",
                plan_id,
                "emergency_release.created",
                actor_id,
                {"release_id": release_id, "stage_id": stage_id, "reason": reason_text, "expires_at": utc_text(expiry)},
            )
        return {"release_id": release_id, "stage_id": stage_id, "expires_at": utc_text(expiry)}

    def release_recovery_stage(
        self, actor_id: str, plan_id: str, stage_id: str, emergency_release_id: int | None = None
    ) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        if emergency_release_id is not None and (
            isinstance(emergency_release_id, bool) or not isinstance(emergency_release_id, int)
        ):
            raise ValidationFailed("emergency_release_id 必须是正整数")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] not in ("active", "reopened"):
            raise InvalidState("恢复方案未在进行中，不能解除封控")
        stage = self._recovery_stage_row(plan_id, stage_id)
        if stage["state"] != "pending":
            raise InvalidState("阶段已解除封控")
        blocker = self.connection.execute(
            "SELECT stage_id FROM recovery_stages WHERE plan_id=? AND sequence<? AND state!='released' "
            "ORDER BY sequence LIMIT 1",
            (plan_id, stage["sequence"]),
        ).fetchone()
        if blocker is not None:
            raise InvalidState("存在未解除的前置阶段，不能扩大通行范围")
        pending_required = self.connection.execute(
            "SELECT i.item_id FROM recovery_check_items i JOIN recovery_stages s ON s.stage_id=i.stage_id "
            "WHERE s.plan_id=? AND s.sequence<=? AND i.required=1 AND i.state!='confirmed' ORDER BY i.item_id",
            (plan_id, stage["sequence"]),
        ).fetchall()
        emergency = None
        if emergency_release_id is not None:
            emergency = self.connection.execute(
                "SELECT * FROM recovery_emergency_releases WHERE release_id=? AND stage_id=?",
                (emergency_release_id, stage_id),
            ).fetchone()
            if emergency is None:
                raise NotFound("紧急放行记录不存在")
            if parse_utc(emergency["expires_at"]) <= self.clock.now():
                raise InvalidState("紧急放行已过有效期")
        if pending_required and emergency is None:
            missing = [row["item_id"] for row in pending_required]
            raise InvalidState(f"必需检查事项未完成，不能扩大通行范围: {','.join(missing)}")
        now = self._now()
        remaining = self.connection.execute(
            "SELECT COUNT(*) AS total FROM recovery_stages WHERE plan_id=? AND sequence>?",
            (plan_id, stage["sequence"]),
        ).fetchone()
        is_final = remaining["total"] == 0
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE recovery_stages SET state='released',released_at=?,released_by=?,emergency_release_id=?,"
                "revision=revision+1 WHERE stage_id=? AND state='pending'",
                (now, actor_id, None if emergency is None else emergency["release_id"], stage_id),
            )
            if is_final:
                self.connection.execute(
                    "UPDATE corridor_restrictions SET state='closed',ends_at=?,capacity_percent=?,revision=revision+1 "
                    "WHERE restriction_id=?",
                    (now, stage["restored_capacity_percent"], plan["restriction_id"]),
                )
                self.connection.execute(
                    "UPDATE recovery_plans SET state='completed',completed_at=?,revision=revision+1 WHERE plan_id=?",
                    (now, plan_id),
                )
            else:
                self.connection.execute(
                    "UPDATE corridor_restrictions SET capacity_percent=?,revision=revision+1 WHERE restriction_id=?",
                    (stage["restored_capacity_percent"], plan["restriction_id"]),
                )
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery_stage.released",
                actor_id,
                {
                    "stage_id": stage_id,
                    "zone_label": stage["zone_label"],
                    "restored_capacity_percent": stage["restored_capacity_percent"],
                    "emergency_release_id": None if emergency is None else emergency["release_id"],
                },
            )
            if is_final:
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery_plan.completed",
                    actor_id,
                    {"restriction_id": plan["restriction_id"]},
                )
        return {
            "stage_id": stage_id,
            "state": "released",
            "restored_capacity_percent": stage["restored_capacity_percent"],
            "plan_state": "completed" if is_final else plan["state"],
        }

    def report_recovery_hazard(self, actor_id: str, plan_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "recovery.write")
        plan = self._recovery_plan_row(plan_id)
        if plan["state"] not in ("active", "reopened", "completed"):
            raise InvalidState("恢复方案当前状态不能登记隐患")
        description = required_text(raw.get("description"), "description")
        percent = decimal_value(
            raw.get("capacity_percent"), "capacity_percent", minimum=Decimal("0"), maximum=Decimal("100")
        )
        stage_ref = raw.get("stage_id")
        if stage_ref is not None:
            stage_ref = identifier(stage_ref, "stage_id")
            self._recovery_stage_row(plan_id, stage_ref)
        post_completion = plan["state"] == "completed"
        if post_completion and percent >= Decimal("100"):
            raise ValidationFailed("恢复后隐患的安全通行比例必须低于 100")
        restriction = self._restriction_row(plan["restriction_id"])
        tighten = not post_completion and percent < Decimal(restriction["capacity_percent"])
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO recovery_hazards(plan_id,stage_id,description,capacity_percent,post_completion,"
                "reported_by,reported_at) VALUES(?,?,?,?,?,?,?)",
                (plan_id, stage_ref, description, decimal_text(percent), 1 if post_completion else 0, actor_id, now),
            )
            hazard_id = int(cursor.lastrowid)
            self._audit(
                "recovery_plan",
                plan_id,
                "recovery_hazard.reported",
                actor_id,
                {"hazard_id": hazard_id, "stage_id": stage_ref, "post_completion": post_completion},
            )
            if post_completion:
                self.connection.execute(
                    "UPDATE recovery_stages SET state='pending',released_at=NULL,released_by=NULL,"
                    "emergency_release_id=NULL,revision=revision+1 WHERE plan_id=? AND state='released'",
                    (plan_id,),
                )
                self.connection.execute(
                    "UPDATE corridor_restrictions SET state='announced',ends_at=NULL,capacity_percent=?,"
                    "revision=revision+1 WHERE restriction_id=?",
                    (decimal_text(percent), plan["restriction_id"]),
                )
                self.connection.execute(
                    "UPDATE recovery_plans SET state='reopened',completed_at=NULL,revision=revision+1 WHERE plan_id=?",
                    (plan_id,),
                )
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery_plan.reopened",
                    actor_id,
                    {"hazard_id": hazard_id, "capacity_percent": decimal_text(percent)},
                )
            elif tighten:
                self.connection.execute(
                    "UPDATE corridor_restrictions SET capacity_percent=?,revision=revision+1 WHERE restriction_id=?",
                    (decimal_text(percent), plan["restriction_id"]),
                )
                self._audit(
                    "recovery_plan",
                    plan_id,
                    "recovery_capacity.tightened",
                    actor_id,
                    {"hazard_id": hazard_id, "capacity_percent": decimal_text(percent)},
                )
        return {
            "hazard_id": hazard_id,
            "plan_id": plan_id,
            "plan_state": "reopened" if post_completion else plan["state"],
            "post_completion": post_completion,
        }

    def recovery_plan(self, plan_id: str) -> dict[str, Any]:
        plan = self._recovery_plan_row(plan_id)
        stages = self.connection.execute(
            "SELECT * FROM recovery_stages WHERE plan_id=? ORDER BY sequence", (plan_id,)
        ).fetchall()
        stage_rows = []
        for stage in stages:
            items = self.connection.execute(
                "SELECT * FROM recovery_check_items WHERE stage_id=? ORDER BY item_id", (stage["stage_id"],)
            ).fetchall()
            stage_rows.append(
                {
                    "stage_id": stage["stage_id"],
                    "sequence": stage["sequence"],
                    "zone_label": stage["zone_label"],
                    "lane_codes": json.loads(stage["lane_codes_json"]),
                    "restored_capacity_percent": stage["restored_capacity_percent"],
                    "state": stage["state"],
                    "released_at": stage["released_at"],
                    "released_by": stage["released_by"],
                    "emergency_release_id": stage["emergency_release_id"],
                    "items": [
                        {
                            "item_id": item["item_id"],
                            "item_kind": item["item_kind"],
                            "responsible_unit": item["responsible_unit"],
                            "required": bool(item["required"]),
                            "state": item["state"],
                            "revision": item["revision"],
                        }
                        for item in items
                    ],
                }
            )
        receipts = [
            dict(row)
            for row in self.connection.execute(
                "SELECT r.* FROM recovery_receipts r JOIN recovery_check_items i ON i.item_id=r.item_id "
                "JOIN recovery_stages s ON s.stage_id=i.stage_id WHERE s.plan_id=? ORDER BY r.receipt_id",
                (plan_id,),
            ).fetchall()
        ]
        withdrawals = [
            dict(row)
            for row in self.connection.execute(
                "SELECT w.* FROM recovery_withdrawals w JOIN recovery_check_items i ON i.item_id=w.item_id "
                "JOIN recovery_stages s ON s.stage_id=i.stage_id WHERE s.plan_id=? ORDER BY w.withdrawal_id",
                (plan_id,),
            ).fetchall()
        ]
        emergency_releases = [
            dict(row)
            for row in self.connection.execute(
                "SELECT e.* FROM recovery_emergency_releases e JOIN recovery_stages s ON s.stage_id=e.stage_id "
                "WHERE s.plan_id=? ORDER BY e.release_id",
                (plan_id,),
            ).fetchall()
        ]
        hazards = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM recovery_hazards WHERE plan_id=? ORDER BY hazard_id", (plan_id,)
            ).fetchall()
        ]
        return {
            "plan_id": plan["plan_id"],
            "corridor_id": plan["corridor_id"],
            "restriction_id": plan["restriction_id"],
            "incident_id": plan["incident_id"],
            "state": plan["state"],
            "revision": plan["revision"],
            "created_by": plan["created_by"],
            "created_at": plan["created_at"],
            "activated_at": plan["activated_at"],
            "completed_at": plan["completed_at"],
            "stages": stage_rows,
            "receipts": receipts,
            "withdrawals": withdrawals,
            "emergency_releases": emergency_releases,
            "hazards": hazards,
        }

    def corridor_capacity(self, corridor_id: str, duty_date: str) -> dict[str, Any]:
        route = self.connection.execute(
            "SELECT * FROM road_corridors WHERE corridor_id=?", (corridor_id,)
        ).fetchone()
        if route is None:
            raise NotFound("道路走廊不存在")
        date_text(duty_date, "duty_date")
        available = self._capacity_for_date(route, duty_date)
        return {
            "corridor_id": corridor_id,
            "duty_date": duty_date,
            "hourly_capacity": route["hourly_capacity"],
            "available_units": decimal_text(available),
        }

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
