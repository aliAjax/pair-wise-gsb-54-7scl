# 跨海光缆故障与抢修协调

纯Python标准库实现的跨海光缆故障与抢修协调原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、海况、船机许可、备缆窗口和接续质量和冲突检查。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/service.py`：用例编排、权限检查、乐观并发和审计。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `src/sync/`：岸端工单、船端离线任务、备缆仓库、出库单的**可续作批次同步**子系统（合并去重、冲突裁决、先到先占、库存重算、出库结算对账）。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算、失败场景与同步批次测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8330
```

默认端口为`8330`，默认数据库位于项目目录。服务启动时自动建表。

## 主要接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情。
- `GET /api/records/{id}/audit`：审计时间线。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 四源可续作批次同步

岸端工单、船端离线任务、备缆仓库、出库单各自一套账的问题，由`src/sync/`解决：

- **可续作批次**：`POST /api/sync/batches`整体落盘（`batch_ref`+内容checksum唯一）。写中途失败整批回滚，可凭**完整批次**重新提交恢复；`POST /api/sync/batches/{batch_ref}/resume`只续作未成功项；同一`batch_ref`重复回传原样返回且不重复扣减。
- **断网记录、回连合并**：`vessel_task`携带船机(`vessel_id/machine_id/spare_onboard_km`)与备缆；按`光缆+区段+里程并集`合并，`required_km`取并集长度与各源申报最大值，同区段不重复计数。型号不一致或申报用量差超过0.5km进入冲突：`GET /api/sync/conflicts?state=pending`、`POST /api/sync/conflicts/{id}/resolve`（`keep_existing/accept_incoming`）。
- **先到先占**：同区段由唯一锁保护，先到船`held`占用，后到船保留`draft`草稿；占用释放（需求发运完结或人工释放）后`POST /api/sync/demands/{ref}/promote`可升级。
- **库存联动重算**：在库/在途任一变化（`POST /api/sync/stock-moves`，支持`receive/in_transit/arrive/adjust`等）立即重算所有未出库(`planned/held`)需求的分配量与缺口；在途计入可承诺量。
- **出库与结算**：`POST /api/sync/outbound`（幂等`order_ref`，冻结在库/在途）、`/ship`、`/cancel`（回补）；`POST /api/sync/settlements`（幂等`entry_ref`）。`GET /api/sync/reconcile`逐条比对出库单与结算流水，报告`missing_settlement/settlement_only/qty_mismatch/amount_mismatch`。
- **同一视图**：`GET /api/sync/availability?cable=&segment=`返回按型号汇总的在库、在途、已分配、可用量，以及每条需求的`allocated_km/gap_km`与区段锁；岸端与现场看到同一组数字。

批次`node`取值：`shore/vessel/warehouse/settlement`；明细`source`取值：
`shore_workorder`、`vessel_task`、`spare_register`、`warehouse_move`、`outbound_order`、`settlement`。
其余只读接口：`GET /api/sync/batches/{ref}`、`/api/sync/demands`、`/api/sync/stock`、
`/api/sync/moves`、`/api/sync/vessels`、`/api/sync/spares`、`/api/sync/outbound`。

批次提交示例：

```json
POST /api/sync/batches
{"batch_ref":"B-20261006-01","node":"vessel","items":[
  {"source":"vessel_task","reference":"VT-7","data":{
    "cable":"SEA-1","segment":"S3","cable_type":"RL-16",
    "start_km":120.0,"end_km":138.0,"required_km":18.0,
    "vessel_id":"CS-1","machine_id":"ENG-1","spare_onboard_km":20.0}}]}
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝和版本冲突。
