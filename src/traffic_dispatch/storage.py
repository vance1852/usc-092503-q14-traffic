"""事故快处服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS traffic_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor','commander')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_index_risk_records (
    risk_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    risk_index TEXT NOT NULL,
    duty_date TEXT NOT NULL,
    index_value TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    supersedes_risk_record_id INTEGER REFERENCES risk_index_risk_records(risk_record_id),
    recorded_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    recorded_at TEXT NOT NULL,
    UNIQUE(risk_index, duty_date, source_revision)
);

CREATE INDEX IF NOT EXISTS idx_risk_records_series
ON risk_index_risk_records(risk_index, duty_date, risk_record_id);

CREATE TABLE IF NOT EXISTS response_centers (
    center_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    timezone TEXT NOT NULL,
    capacity_units TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS road_corridors (
    corridor_id TEXT PRIMARY KEY,
    origin_center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    destination_center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    response_resource_kind TEXT NOT NULL,
    hourly_capacity TEXT NOT NULL,
    delay_basis_points INTEGER NOT NULL,
    response_minutes INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','retired')),
    created_at TEXT NOT NULL,
    CHECK(origin_center_id <> destination_center_id)
);

CREATE TABLE IF NOT EXISTS corridor_restrictions (
    restriction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    capacity_percent TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'announced' CHECK(state IN ('announced','active','closed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outages_route_time
ON corridor_restrictions(corridor_id, starts_at, ends_at);

CREATE TABLE IF NOT EXISTS response_resource_lots (
    response_resource_lot_id TEXT PRIMARY KEY,
    center_id TEXT NOT NULL REFERENCES response_centers(center_id),
    response_resource_kind TEXT NOT NULL,
    grade TEXT NOT NULL,
    quantity_units TEXT NOT NULL,
    available_units TEXT NOT NULL,
    unit_cost_cny TEXT NOT NULL,
    received_at TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_available
ON response_resource_lots(center_id, response_resource_kind, received_at);

CREATE TABLE IF NOT EXISTS response_resource_adjustments (
    adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    response_resource_lot_id TEXT NOT NULL REFERENCES response_resource_lots(response_resource_lot_id),
    delta_units TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_requests (
    dispatch_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    incident_id TEXT NOT NULL,
    duty_date TEXT NOT NULL,
    requested_units TEXT NOT NULL,
    allocated_units TEXT NOT NULL DEFAULT '0',
    arrived_units TEXT NOT NULL DEFAULT '0',
    priority INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'submitted'
        CHECK(state IN ('submitted','allocated','in_transit','delivered','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    submitted_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    submitted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dispatch_requests_schedule
ON dispatch_requests(corridor_id, duty_date, priority, submitted_at);

CREATE TABLE IF NOT EXISTS dispatch_plans (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    duty_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    available_units TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(corridor_id, duty_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_id TEXT PRIMARY KEY,
    dispatch_id TEXT NOT NULL UNIQUE REFERENCES dispatch_requests(dispatch_id),
    inventory_response_resource_lot_id TEXT NOT NULL REFERENCES response_resource_lots(response_resource_lot_id),
    deployed_units TEXT NOT NULL,
    expected_arrived_units TEXT NOT NULL,
    departed_at TEXT NOT NULL,
    arrived_at TEXT,
    state TEXT NOT NULL DEFAULT 'in_transit' CHECK(state IN ('in_transit','delivered','disputed')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS response_scenarios (
    scenario_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','approved','retired')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS response_scenario_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id TEXT NOT NULL REFERENCES response_scenarios(scenario_id),
    as_of_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(scenario_id, as_of_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS traffic_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS recovery_plans (
    plan_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    restriction_id INTEGER NOT NULL REFERENCES corridor_restrictions(restriction_id),
    incident_id TEXT NOT NULL,
    name TEXT NOT NULL,
    base_capacity_percent TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','completed')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recovery_plans_restriction
ON recovery_plans(restriction_id, state);

CREATE TABLE IF NOT EXISTS recovery_zones (
    zone_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES recovery_plans(plan_id),
    name TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'closed' CHECK(state IN ('closed','partial','open')),
    revision INTEGER NOT NULL DEFAULT 1,
    UNIQUE(plan_id, sequence)
);

CREATE TABLE IF NOT EXISTS recovery_lanes (
    lane_id TEXT PRIMARY KEY,
    zone_id TEXT NOT NULL REFERENCES recovery_zones(zone_id),
    name TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'closed' CHECK(state IN ('closed','open')),
    opened_via TEXT CHECK(opened_via IN ('phase','emergency')),
    opened_at TEXT,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_recovery_lanes_zone
ON recovery_lanes(zone_id, state);

CREATE TABLE IF NOT EXISTS recovery_check_items (
    item_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES recovery_plans(plan_id),
    zone_id TEXT REFERENCES recovery_zones(zone_id),
    kind TEXT NOT NULL
        CHECK(kind IN ('casualty-transfer','evidence-collection','debris-cleanup','facility-inspection')),
    responsible_unit TEXT NOT NULL,
    required INTEGER NOT NULL DEFAULT 1 CHECK(required IN (0,1)),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','completed')),
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_recovery_items_plan
ON recovery_check_items(plan_id, state);

CREATE TABLE IF NOT EXISTS recovery_receipts (
    receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL REFERENCES recovery_check_items(item_id),
    action TEXT NOT NULL CHECK(action IN ('confirm','withdraw')),
    duplicate INTEGER NOT NULL DEFAULT 0 CHECK(duplicate IN (0,1)),
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recovery_receipts_item
ON recovery_receipts(item_id, receipt_id);

CREATE TABLE IF NOT EXISTS recovery_emergency_releases (
    release_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES recovery_plans(plan_id),
    scope_type TEXT NOT NULL CHECK(scope_type IN ('zone','lane')),
    scope_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','expired','closed')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recovery_releases_plan
ON recovery_emergency_releases(plan_id, state, expires_at);

CREATE TABLE IF NOT EXISTS recovery_hazards (
    hazard_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES recovery_plans(plan_id),
    zone_id TEXT REFERENCES recovery_zones(zone_id),
    lane_id TEXT REFERENCES recovery_lanes(lane_id),
    description TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','resolved')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL,
    resolved_by TEXT REFERENCES traffic_users(user_id),
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS recovery_capacity_syncs (
    sync_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES recovery_plans(plan_id),
    corridor_id TEXT NOT NULL REFERENCES road_corridors(corridor_id),
    restriction_id INTEGER NOT NULL REFERENCES corridor_restrictions(restriction_id),
    capacity_percent TEXT NOT NULL,
    open_lanes INTEGER NOT NULL,
    total_lanes INTEGER NOT NULL,
    trigger TEXT NOT NULL,
    actor_id TEXT NOT NULL REFERENCES traffic_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS traffic_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traffic_audit_entity
ON traffic_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
    _migrate_user_roles(connection)


def _migrate_user_roles(connection: sqlite3.Connection) -> None:
    """为旧库补充 commander 角色：重建 traffic_users 的角色检查约束。"""
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='traffic_users'"
    ).fetchone()
    if row is None or "'commander'" in row[0]:
        return
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        with transaction(connection, immediate=True):
            connection.execute(
                "CREATE TABLE traffic_users_migrated ("
                "user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, "
                "role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor','commander')), "
                "active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)), created_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO traffic_users_migrated(user_id,display_name,role,active,created_at) "
                "SELECT user_id,display_name,role,active,created_at FROM traffic_users"
            )
            connection.execute("DROP TABLE traffic_users")
            connection.execute("ALTER TABLE traffic_users_migrated RENAME TO traffic_users")
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
