# 跨海光缆故障与抢修协调

纯Python标准库实现的跨海光缆故障与抢修协调原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：抢修工单状态转换、海况、船机许可、备缆窗口和接续质量和冲突检查。
- `src/logistics.py`：批次合并与库存纯规则：里程并集、区段占用、申报密度冲突、可用量与对账状态。
- `src/logistics_service.py`：物流用例编排：可续作批次、失败恢复、幂等回传、出库与结算对账。
- `src/repository.py`：SQLite建表、事务和查询（含批次、库存台账、区段计划、占用、出库单、结算流水、冲突表）。
- `src/service.py`：抢修工单用例编排、权限检查、乐观并发和审计。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算、失败场景与物流批次测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8330
```

默认端口为`8330`，默认数据库位于项目目录。服务启动时自动建表。

## 抢修工单接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情。
- `GET /api/records/{id}/audit`：审计时间线。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 抢修物流接口（岸端工单/船端离线任务/备缆仓库/出库单）

核心约定：批次先完整落库再合并，写入失败可从完整批次恢复；台账流水、出库单、
结算流水全部以客户端幂等键去重，重复回传不重复扣减；可用量与缺口由同一份台账
实时算出，岸端与现场看到同一数字。

- `POST /api/batches`：提交可续作批次。请求体`{"batch_key":"...","source":"shore|vessel","vessel_name":"...","items":[{"item_key":"i1","cable":"SEA-1","segment":"S3","start_km":120,"end_km":135,"warehouse":"WH-East","cable_type":"LW-24","spare_planned_km":16,"machinery":["grapple"]}]}`。
  同一`batch_key`重复回传返回已存结果；留草稿（`draft`）的批次可改写后重新提交。
- `GET /api/batches`、`GET /api/batches/{batch_key}`：批次列表与详情。
- `POST /api/batches/{batch_key}/resume`：从完整批次恢复（`failed`/`submitted`/`draft`/`conflicted`均可重新合并）。
- `POST /api/inventory/receipt`：备缆入库，`from_in_transit=true`表示在途到库；`move_key`幂等。
- `POST /api/inventory/in-transit`：在途量变更（`expected_version`乐观并发）；在途量一变，未出库任务随响应重算返回。
- `GET /api/inventory/availability?warehouse=..&cable_type=..`：现场可用量与缺口（在库/在途/已预留/可用/缺口/未出库任务覆盖情况）。
- `GET /api/inventory/moves`：库存台账流水。
- `POST /api/outbound`：出库单，`order_key`幂等；扣在库并核销区段未出库余量，在库不足时拒绝并给出缺口。
- `GET /api/outbound`：出库单列表。
- `POST /api/settlements`：结算流水，`entry_key`幂等。
- `GET /api/settlements`、`GET /api/reconcile`：结算查询与出库单对账（`matched/partial/unsettled/over_settled`）。
- `GET /api/conflicts`、`POST /api/conflicts/{id}/resolve`：待裁决冲突与裁决（`release_holder/keep_holder`，数量口径`use_shore/use_vessel/keep_merged`，可带`override_required_km`）。

合并规则：同一（光缆,区段,仓库,缆型）的里程取并集，备缆需求=并集里程×1.05只算一次；
两船同区段里程重叠时先到者占用、后到者留草稿并登记冲突；岸端与船端申报密度
（备缆公里/里程公里）偏差超过25%时登记口径冲突留待裁决。

角色：岸端批次`noc_operator`/`repair_manager`，船端批次`vessel_master`，
库存与出库`warehouse_keeper`，结算`settlement_clerk`，裁决`repair_manager`，`admin`全能。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝、版本冲突，以及物流侧的合并去重、
幂等回传、先到者占用、在途重算、失败恢复、出库对账。
