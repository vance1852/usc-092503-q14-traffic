# 道路交通事故快处与处罚协同服务

本项目是一套可离线运行的 Python 后台，用于事故受理、道路风险研判、警力与拖车调度、结构化证据复核、责任认定、处罚执行和审计追溯。系统把同一事故从报警到结案的关键状态保存在 SQLite 中，角色权限覆盖接警员、调度员、事故处理民警、复核人员和审计人员。

## 目录

- `src/traffic_dispatch/`：事故风险指数、快处中心、道路走廊、应急资源、调度申请、响应情景和分阶段道路恢复；
- `src/evidence_review/`：采集设备、证据规范、结构化记录导入、一致性分析、复核租约和采信决定；
- `src/penalty_ops/`：事故案件、违法记录、风险告警、处置工单、处罚流转和审计；
- `fixtures/`：离线验收使用的证据规范与结构化事故记录；
- `tests/`：领域规则、事务边界、权限、HTTP API 和 CLI 验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅依赖 Python 标准库与 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m traffic_dispatch.acceptance --workspace .
PYTHONPATH=src python3 -m evidence_review.acceptance --workspace .
PYTHONPATH=src python3 -m penalty_ops.acceptance
```

验收会建立临时 SQLite 数据库，登记事故风险记录、快处中心、道路走廊和应急资源，完成调度与证据复核，并输出 JSON 结果。命令不会访问公网，也不需要额外数据库、队列或常驻服务。

## HTTP API

```bash
PYTHONPATH=src python3 -m traffic_dispatch.api --database traffic.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m evidence_review.api --database evidence.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m penalty_ops.api --database penalties.sqlite3 --host 127.0.0.1 --port 8082
```

三个服务均提供 `GET /health`，其余接口使用 JSON。SQLite 文件保存业务状态、幂等结果和审计记录，进程重启后可继续查询。

## 分阶段道路恢复

夜间重大事故处置结束后，道路不会因拖车离场就立即恢复。`traffic_dispatch` 服务支持在已登记的道路限制上建立恢复方案（`POST /recovery_plans`），把封控区、车道、检查事项（伤员转运、证据采集、散落物清理、设施检查）和负责单位关联成有序阶段：

- 负责单位回执确认事项（`POST /recovery_plans/{id}/receipts`），重复回执只记录不重复生效，确认也可撤回（`POST /recovery_plans/{id}/withdrawals`），全部留痕；
- 任何必需事项未完成或前置阶段未解除时，不得解除封控扩大通行范围（`POST /recovery_plans/{id}/stages/{stage_id}/release`）；
- 紧急放行（`POST /recovery_plans/{id}/emergency_releases`）必须由具备 `recovery.override` 权限的指挥员记录理由和有效期，过期即失效；
- 恢复后发现隐患（`POST /recovery_plans/{id}/hazards`）会保留历史并重新收紧或重开封控；
- 每个阶段解除都会同步道路走廊容量，后续调度分配自动使用新的可用范围（`GET /road_corridors/{id}/capacity?duty_date=...`）。
