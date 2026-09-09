# 大 QMT helper 重构前的接口测试基准

本轮没有重构 helper。此清单限定于 helper 当前 28 条业务路由及 GET `/health`，不是声明 BulletTrade 所有数据供应商、所有聚宽 API 已实现。

## 测试如何分层

1. **传输合同**：`test_big_qmt_interface_contract.py` 对每条业务路由测试 GET/POST、认证失败零分派、参数和 request_id 传递、实际 action 分派、返回封装。另验健康、未知路由和错误码/broker_called 保留。只替换业务函数，不启动 HTTP 监听或 QMT。返回的动态 `ts` 验类型，不固定某次墙钟数值。
2. **原生参数/业务语义**：复用 `test_big_qmt_gateway_strategy_sample.py`、`test_big_qmt_adjustment_contract.py` 和 `../server/test_big_qmt_adapter.py`。这些包含字段、单位、日期、复权枚举、事件结构、账户隔离、订单身份、submit_unknown 和精确撤单确认。传输层的 mock 通过不能替代这一层。
3. **真实数据离线回放**：`../server/test_big_qmt_intraday_boundary.py` 和已有 history/paused/adjustment 系列。用原始价格、完整事件、日历独立算期望值，不用被测函数给自己生成答案。新样本 `../fixtures/big_qmt_intraday_boundary_20260909.json` 保留两证券共82行（含竞价），不含账号、地址、口令。
4. **仿真实机合同**：待本地结果汇报、仿真外部数据服务升级后采集。记录源提交、helper build、请求时刻、完整参数、原始输入/公开输出、首读/重读结果与差异。真实查询可能下载窗口缓存，不能称作零写入；不得清空缓存制造“冷缓存”，用未预取的新窗口验证。下单、撤单、批量撤单、规则撤单、显式下载/订阅不进入自动实机清单。

## 当前路由覆盖

下表全部有传输层用例。细节列说明后续重构必须保留的原生/适配层合同；并不表示每一项已采集到仿真样本。

| helper 路由 | action / 核对细节 | 原生或适配层现有测试 |
|---|---|---|
| GET `/health` | build、ready、事件 schema；不进行业务分派 | helper health / adjustment health / adapter health |
| `/data/history` | 日期秒数、none/pre/post、字段、索引、下载边界、订阅参数、价格量额单位 | helper history / history standardization / intraday boundary |
| `/data/snapshot` | 原始全 tick 快照 | helper current_tick / adapter data payload |
| `/data/current_tick` | 单证券 tick，不变成历史 bar | helper current_tick / adapter current_tick |
| `/data/live_current` | 观察时刻、源时刻、age；不能捏造新鲜度 | helper live observation / adapter live_current |
| `/data/trade_days` | 日期格式、条数、原生与日线回退 | helper trade_days / adapter trade_days |
| `/data/security_info` | 证券映射、类型、上市日、价格精度 | helper non_tick_data_apis / adapter data payload |
| `/data/ensure_cache` | 证券、周期、首尾时间；有下载副作用，仅 mock | helper non_tick_data_apis |
| `/data/all_securities` | 股票/ETF 类型筛选与表结构 | helper non_tick_data_apis / adapter data payload |
| `/data/index_stocks` | 成分股列表、权重来源与回退来源 | helper index_stocks / adapter data payload |
| `/data/split_dividend` | 七项原始字段、schema、事件范围、旧字段兼容 | adjustment_contract / helper split_dividend |
| `/account` | account | adapter account_positions_orders_trades |
| `/positions`、`/api/holding` | positions；单位、成本价回退 | helper cost / adapter positions |
| `/orders`、`/api/order/status` | orders；身份、状态、账户过滤 | helper orders/trades / adapter list_orders |
| `/trades` | trades；成交时间、身份与量价 | helper orders/trades / adapter list_trades |
| `/order_status` | 指定 order_id，只返回匹配订单；未找到报错 | interface contract / adapter get_order_status |
| `/place_order`、`/api/order/buy`、`/api/order/sell` | place_order；别名方向、客户身份、broker_called、unknown 不等于成功 | helper passorder / adapter submit_unknown 和确认系列 |
| `/cancel_order` | cancel_order；只取消精确目标 | adapter cancel_request_confirms_exact_order_terminal_status |
| `/debug/trade_detail`、`/debug/qmt_trade_detail` | debug_trade_detail；限定查询组合 | helper debug_scans_trade_detail_combinations |
| `/api/money/total`、`/api/money/available` | money_total / money_available；从账户结果提取正确字段 | interface contract / adapter account |
| `/api/order/cancel_all` | 默认 DANGEROUS_OPERATION_DISABLED；不在测试中放开 | interface contract |
| `/api/order/cancel_order` | rule_cancel；必须保持与精确 cancel_order 的分派区别 | interface contract（业务函数 mock，尚无新增真实样本） |

`RemoteQmtProvider.get_bars(include_now=False/True)` 当前明确未实现，已有两条异常合同测试。本轮不把它伪装成 get_price，也不补实现。

## 本次边界回归

- 股票/ETF × 1m/5m/60m × none/pre/post × start/count × 09:31、10:10、11:30、13:01、15:00；每例首读与重读逐格比较。
- 整分/含秒/未来截止/今天日期/默认现在，只返回已完成的一分钟；get_price 的多分钟末组仍可包含不足整周期的已完成基础分钟。
- 盘前 count 回到上一交易日，午休/收盘/周末不生成虚构 bar；未来起点空表不下载行情。
- 扩大窗口内的额外行裁剪，真正窗口外响应失败，真实缺行仍报错，日线请求不扩大。
- 82行现场 QMT 输入独立合并竞价、聚合成 1m/5m/60m；与聚宽数值差异不在本测试内消除。

## 以后如何重构

先冻结输入、输出、来源与每个接口的副作用，再只迁移一个责任点并跑以上三层离线测试，最后仿真复验同一合同。传输合同负责发现漏路由/错参数；原生与数值合同负责发现错单位、错时段和身份丢失。不能只比较两版接口都返回 HTTP 200。

2026-09-09已补升级后的仿真证据：旧版两证券同参空表，新版首读/重读均为7行；52个公开请求、958行/5748格与独立原始事实完全一致，24个聚宽参考时间轴一致。新增 `test_big_qmt_sim_intraday_replay.py` 的106项测试及 `big_qmt_sim_intraday_20260909.json` 保存这些输入、输出与只读接口摘要。聚宽原始数值差异仍单列，不能把时间轴通过称为数值全兼容。

账户只读接口需传入既有外部服务账户参数；helper默认账户未设置的负例与补参后的正例均保留。不存在订单的ORDER_NOT_FOUND属于预期失败。以后若迁移规则撤单等业务实现，仍须先补其原生调用/过滤规则的 mock 用例；本轮传输 mock 不能作为删除交易安全逻辑的依据。

## 本地执行

在 `bullet-trade/` 使用项目 Python：

```sh
python -m pytest tests/helpers/test_big_qmt*.py tests/server/test_big_qmt*.py tests/unit/test_qmt_adjustment*.py tests/unit/test_remote_qmt_history_contract.py tests/unit/test_big_qmt_history_contract_probe.py -q --no-cov
```

只读取纳管样本，helper 导入会沿既有逻辑写本地测试日志；不使用生产配置、不运行网络或真实 QMT 交易。
