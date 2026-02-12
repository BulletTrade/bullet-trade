# 更新日志

本文档记录所有重要的变更。格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [0.6.6] - 2026-02-12

### 新增
- **交易日历守卫诊断日志**：`TradingCalendarGuard` 新增非交易日原因与异常诊断日志（含 `reason/query/sample_days` 等上下文），并加入 300 秒日志节流，减少重复刷屏

### 修复
- **attribute_history 时间边界对齐**：对齐聚宽语义，`1d` 日线默认排除当天，`1m` 分钟线默认包含当前分钟，避免历史窗口错位
- **交易日判定鲁棒性**：`TradingCalendarGuard` 对 `get_trade_days` 的空 DataFrame、`None`、空列表、不可迭代与异常返回做了显式处理，交易日确认更稳定
- **远程券商下单参数兼容**：`RemoteQmtBroker.buy/sell` 的 `remark` 参数改为可选默认 `None`，减少调用侧参数不匹配

### 增强
- **future guard 语义优化**：`get_price/get_trade_days` 统一使用 `_should_avoid_future()` 判定，仅在非 live 场景启用 `avoid_future_data`，避免实盘被回测式未来数据校验误拦截

### 测试
- 新增 `test_attribute_history_alignment.py`，覆盖日线/分钟线 `attribute_history` 结束时间对齐
- 新增 `test_data_api_live_boundaries.py`，覆盖 live/backtest 下 `get_trade_days/get_price` 的 `avoid_future_data` 边界行为

## [0.6.5] - 2026-02-09

### 新增
- **券商全量委托查询**：`get_orders` 新增 `from_broker` 参数。默认行为保持不变；当 `from_broker=True` 时，可直接返回券商侧全量订单（包含人工下单或外部系统下单），方便策略对账与接管（#25）
- **远程 helper 同步支持**：`helpers/bullet_trade_jq_remote_helper.py` 的 `get_orders` 同步支持 `from_broker` 并透传到远程服务，聚宽远程运行场景可直接查看券商全量委托（#25）

### 修复
- **远程历史数据字段过滤失效**：`QmtDataAdapter.get_history` 已透传 `fields` 到 `provider.get_price`，远程 `data.history` 现在可按需返回字段，避免字段不一致并减少无关数据返回（#24）
- **非交易日启动稳定性**：实盘引擎新增交易日日历重试等待机制，周末/节假日/盘前启动时不再因日历尚未就绪而误判流程

### 文档
- **开发模式生效说明**：补充“为什么 Jupyter 改了源码能生效但 CLI 不生效”的排查指引，明确 `pip install -e "bullet-trade[dev]"` 与重启进程/Kernel 的必要性（#25）

### 测试
- 新增 `get_orders(from_broker=True)` 的实盘引擎覆盖用例，验证“默认行为不变 + 可选券商全量”两种路径（#25）
- 新增远程 helper 的 `from_broker` 参数透传测试，确保请求链路完整（#25）
- 补充 `get_history(fields=...)` 参数透传回归测试（#24）

## [0.6.4] - 2026-01-28

### 新增
- **订单备注/策略名**：下单支持 `remark`，新增 `STRATEGY_NAME`，订单与事件携带策略名与备注，便于多策略区分
- **订单字段完善**：`Order` 增加已成交数量 `filled`、买卖方向 `is_buy`，并区分委托价与成交均价；`Order.extra` 补充 `order_remark/strategy_name/order_price`

### 修复
- **远程 QMT get_price datetime 支持**：`start_date/end_date/pre_factor_ref_date` 支持 `datetime`，修复 JSON 序列化异常（#23）
- **分钟级频率时间截断**：`1m/5m` 等分钟级频率保留时分秒，避免被误处理为日线
- **复权参考日期透传**：`pre_factor_ref_date` 透传到远程 `data.history` 处理链路

### 增强
- **远程握手协议提示**：服务端握手返回协议版本，聚宽 helper 每次连接检测版本差异并提示升级

### 文档
- **聚宽 helper 升级提示**：请务必升级到最新版帮助文件，参见 `https://github.com/BulletTrade/bullet-trade/blob/main/helpers/bullet_trade_jq_remote_helper.py`

### 测试
- **订单与助手用例**：新增订单备注/订单查询相关测试与 helper 告警输出测试

## [0.6.3] - 2026-01-25

### 修复
- **周/月任务重启重复**：实盘重启后沿用策略起始日计算周/月序号，避免 `force=True` 因重启重复触发
- **旧运行态兼容**：缺失起始日字段时自动补写，不影响历史 runtime 加载

### 测试
- **起始日恢复用例**：新增策略起始日持久化/恢复与旧运行态补写测试

## [0.6.2] - 2026-01-25

### 修复
- **实盘交易日判定**：交易日列表为 list/可迭代时也校验是否包含当天，避免节假日/盘前误判导致任务异常触发
- **周/月度任务重复**：交易日历缺当天时不再回退为单日历，避免 `force=True` 导致周/月任务连续触发

### 增强
- **实盘缓存强制关闭**：覆盖所有数据源入口，确保实盘模式禁用磁盘缓存，降低延迟与 IO 风险
- **交易日确认节奏**：`CALENDAR_RETRY_MINUTES` 默认改为 1 分钟，盘前更快确认交易日

### 测试
- **交易日守卫用例**：补充 list 返回交易日判定与缺当天日历的调度测试

## [0.6.1] - 2026-01-18

### 新增
- **研究分析模块**：新增因子研究与评估模块，支持因子回测/评价流程
- **数据校验工具**：新增价格精度对比策略与配置，用于核对数据源价格精度一致性
- **当前价限制探测**：新增 current data limit probe 策略与环境配置，辅助验证涨跌停/价格限制基线
- **订单与成交查询**：券商类新增订单/成交查询接口，便于实盘状态核对

### 修复
- **JQData 前复权价格异常**：引入 `force_no_engine` 选项并适配数据源，支持绕过引擎复权问题（#19）
- **MiniQMT 指数成分获取**：修复/增强 MiniQMTProvider 并补充相关测试（#13）

### 增强
- **行情取价稳定性**：价格获取逻辑强化（含未来数据限制与空数据处理），提升回测稳定性与兼容性
- **涨跌停规则兜底**：新增证券涨跌停规则配置与回退逻辑，异常行情处理更稳
- **限制探测日志**：current data limit probe 的参考与日志更清晰，便于排查
- **远程实盘助手可观测性**：`bullet_trade_jq_remote_helper` 日志与调试能力增强

### 测试
- **远程订单状态**：完善远程服务订单状态断言与 stub 警告输出测试
- **行情校验与限制**：补充价格精度对比、limit probe、MiniQMT 指数成分相关测试

## [0.6.0] - 2026-01-01 新年快乐~

### 新增
- **交易日历驱动调度**：`run_weekly/run_monthly` 支持按“当周/当月第 N 个交易日”（含倒数）触发，新增 `reference_security/force` 控制参考标的与起算方式；引入全局交易日历构建/缓存（同步与异步调度共用），`generate_daily_schedule`/异步调度器按日预生成任务表，回测/实盘按真实交易日节奏执行

### 增强
- **数据源直连与 SDK 回退**：`get_data_provider("name")` 可直接拿到指定数据源实例（不修改默认数据源），按名称缓存实例与认证状态；当 provider 未实现方法时，同源 SDK 自动兜底（JQData→`jqdatasdk`、Tushare→`pro_api`、MiniQMT→`xtquant.xtdata`），并补充示例策略、使用指引与测试用例
- **配置与文档**：`.env` 示例新增 `MINIQMT_MARKET` 以明确交易日市场代码，清理未使用的 `CACHE_TTL_DAYS/BACKTEST_OUTPUT_DIR/DEBUG` 等字段；`config.md` 与 env loader 及对应单测同步更新，避免混淆配置

### 修复
- **MiniQMT 停牌判断准确性**：停牌逻辑改为按状态码白名单判定（1/17/20 等停牌、区分休市/集合竞价/波动性中断），避免午休、集合竞价被误判为停牌

---

## [0.5.8] - 2025-12-29




## [0.5.8] - 2025-12-29

### 修复
- **环境变量刷新失效（Fixes #7）**：支持 `.env`/环境变量更新后重载数据源与日志配置，避免配置更新不生效

### 新增
- **数据接口扩展**：新增 `get_bars/get_ticks/get_current_tick`，支持 `dt/df` 参数与回测兜底；`get_security_info/get_all_securities` 支持按日期查询历史口径
- **订单查询能力**：`QmtBroker` 新增 `get_open_orders`，仅返回未完成订单并保留原始状态字段

### 增强
- **交易合规手数规则统一**：下单数量按最小手数+步进取整，买入向下取整、卖出不足最小手数可按可卖余量收尾；不足最小手数直接拒单并返回规则明细
- **普通股票/创业板规则**：未命中前缀/市场规则时走默认规则，最小 100 股、步进 100
- **科创板规则**：`68` 前缀最小 200 股、步进 1；**北交所**：BJ/BSE 市场最小 100 股、步进 1；**可转债**：`110/113/118/123/127/128` 前缀最小 10 张、步进 10
- **市价单合规保护价**：服务端统一计算保护价并裁剪至价格笼子/涨跌停；主板/创业板按 2%/98% + 十档，科创板按 2%/98%，北交所按 5% 或 ±0.1 约束；ETF/B 股最小价差 0.001，A 股按价格分 0.01/0.001
- **Tushare 复权与数据对齐**：事件复权补齐（分红送转）、交易日对齐与最新交易日兜底，因子缺失自动回退事件复权
- **QMT 稳定性与可观测性**：请求超时控制、会话日志完善，QMT 同步调用独立线程池避免阻塞
- **序列化与回测稳定**：QMT dataframe payload 支持 Timestamp/NaT 序列化；回测日报文件动态定位并给出缺失提示

### 测试
- 新增订单手数规则、open orders、Tushare 复权、QMT payload 序列化、数据源一致性等单元/端到端测试

---

## [0.5.7] - 2025-12-26

### 新增
- **JoinQuant 远程实盘交易支持**：新增 `04.joinquant_remote_live_trade.ipynb` Notebook，支持通过 JoinQuant 平台进行远程实盘交易
  - 包含远程服务器连接配置（host、port、token）
  - 提供账户查询、持仓查询、限价单下单与撤单等功能
  - 附带完整的使用文档和辅助脚本引用

### 增强
- **订单处理功能优化**：
  - `QmtBrokerAdapter` 增强订单预处理，新增市场状态检查、价格限制检查和数量调整
  - `RemoteOrder` 类新增 `actual_amount` 和 `actual_price` 字段，记录实际成交信息
  - `RemoteBrokerClient` 订单方法优化，服务端统一处理 100 股取整、停牌检查和价格验证
  - 完善订单方法的文档说明，明确服务端处理逻辑和预期行为

- **TushareProvider 数据源增强**：
  - 新增 Tushare 与 JoinQuant 代码后缀映射字典
  - 实现 `_to_ts_code` 和 `_to_jq_code` 代码转换方法
  - 数据获取方法支持两种代码格式，提升兼容性
  - 优化分红和基金分配数据的解析与验证逻辑

- **持仓数据处理重构**：
  - `RemoteBrokerClient` 持仓数据解析优化，引入独立变量处理 amount、available 和 frozen 值
  - 增强可用/冻结数量的判断逻辑，支持多个可能的服务端响应字段
  - 简化 `RemotePosition` 实例创建逻辑，提升代码可读性和可维护性

### 测试
- 新增 Tushare 价格获取的单元测试，确保功能与数据完整性

---

## [0.5.6] - 2025-12-22

### 修复
- **远程交易助手价格获取问题**：修复 `bullet_trade_jq_remote_helper` 获取价格返回为空的问题（Fixes #4）
  - 添加调试信息，便于排查价格获取失败的原因

### 增强
- **MiniQMTProvider 错误处理优化**：
  - 为数据获取过程添加详细日志记录，捕获参数和错误信息
  - 改进 `_fetch_local_data` 方法，处理缺失 'time' 列等异常情况
  - 完善方法文档字符串，明确参数和返回类型说明

- **QmtDataAdapter 日志增强**：
  - 为 `get_history` 和 `get_snapshot` 方法添加详细日志记录
  - 记录详细的错误信息和请求参数，便于问题排查
  - 改进文档说明，提升代码可读性

---

## [0.5.5] - 2025-12-21

### 修复
- **TushareProvider 类拼写错误修复**：修复 `TushareProvider` 类中的拼写错误（Fixes #5）

### 文档
- **Tushare 配置文档增强**：
  - 增强 Tushare 自定义 URL 配置的文档说明
  - 更新 `docs/config.md` 和 `docs/data/DATA_PROVIDER_TUSHARE.md` 中的相关配置说明

---

## [0.5.4] - 2025-12-21

### 修复
- **碎股卖出问题修复**：修复碎股无法卖出的问题（Fixes #6）
  - 优化 `bullet_trade/core/engine.py` 中的订单处理逻辑，支持碎股（不足100股的股票）的正常卖出

### 致谢
- 感谢 [Sheng Li](https://github.com/mrlouisleel) 贡献的碎股卖出问题修复（PR #6）

---

## [0.5.3] - 2025-12-21

### 新增
- **Tushare 自定义 API URL 支持**：添加对自定义 Tushare API URL 的支持（Fixes #5）
  - 更新环境变量加载器，支持 `TUSHARE_API_URL` 配置
  - 更新示例配置文件 `env.backtest.example`，添加相关配置说明
  - 允许用户自定义 Tushare API 服务地址，提升灵活性

### 增强
- **回测引擎价格获取逻辑重构**：
  - 重构 `BacktestEngine` 中的价格获取逻辑，支持长格式数据框（包含 'code' 和 'close' 列）
  - 增强错误日志记录，当价格数据缺失或列不匹配时提供更详细的错误信息
  - 优化 `JQDataProvider` 的日志级别，将价格引擎回退检测从 info 调整为 debug

### 致谢
- 感谢 [Vanilla_Yukirin](https://github.com/Vanilla_Yukirin) 贡献的 Tushare 自定义 API URL 支持功能（PR #5）

---

## [0.5.2] - 2025-12-10

### 新增
- **交易成本管理功能**：
  - 新增 `set_commission` 函数，支持设置交易佣金
  - 新增 `set_universe` 函数，支持管理资产池
  - `OrderCost` 类新增 `commission_type` 属性，提供更灵活的费率管理

- **滑点类型扩展**：
  - 新增 `PriceRelatedSlippage`（价格相关滑点）类型
  - 新增 `StepRelatedSlippage`（阶梯相关滑点）类型
  - 保留现有的 `FixedSlippage`（固定滑点）类型

### 增强
- **交易引擎优化**：
  - 优化 `BacktestEngine` 和 `LiveEngine` 中的交易设置处理逻辑
  - 增强交易成本计算的准确性和灵活性
  - 改进滑点处理机制，支持多种滑点模型

- **文档和测试**：
  - 更新 API 文档，添加新功能的使用示例
  - 新增滑点行为测试（`tests/core/test_slippage.py`）
  - 增强订单成本测试（`tests/unit/test_order_costs.py`），确保功能正确性

---

## [未发布]

### 计划中
- 更多功能改进...

---

## 版本说明

- **新增**：新功能
- **增强**：现有功能的改进
- **修复**：Bug 修复
- **变更**：破坏性变更或重要行为变更
- **废弃**：即将移除的功能
- **移除**：已移除的功能
- **安全**：安全相关的修复

[0.5.8]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.7...v0.5.8
[0.5.7]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.1...v0.5.2
