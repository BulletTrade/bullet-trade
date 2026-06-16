# 数据源接入验收框架

新增或重做数据源时，必须按本框架输出验收结果。目标不是让所有数据源都和 JQData 完全一致，而是把每个函数的实际能力、对比结果、兼容处理和残余风险写清楚。

## 验收原则

- 每个 provider 公共函数都要进入验收清单：要么通过对比，要么标明不可比、未实现、受权限限制或数据源限制。
- 行情价格、复权和动态真实价格必须优先和 JQData/MiniQMT 等已验证基准做数值对账。
- 列表、实时快照、证券信息等天然不完全一致的接口，不能硬要求全量相等，应验证 schema、关键字段、样例标的、时间视角和失败模式。
- 不支持的接口也要测试：必须稳定抛 `NotImplementedError`、返回空表，或在文档中说明降级行为，不能静默返回假数据。
- Beta 数据源可以允许部分失败，但必须说明失败函数、失败原因、已尝试的兼容方案和后续需要谁补测。

## 结果状态

| 状态 | 含义 | 发布要求 |
| --- | --- | --- |
| PASS | 与基准对齐，或按该接口定义完成验收 | 可标记支持 |
| PARTIAL | 只通过 schema、样例或部分场景 | 文档必须写清边界 |
| LIMIT | 数据源天然受限，例如免费历史深度不足 | 可发布，但不能宣称完整支持 |
| UNSUPPORTED | 明确不支持，稳定抛错或显式空返回 | 能发布，但能力矩阵标为未实现 |
| BLOCKED | 缺账号、缺客户端、权限不足，未完成真实验收 | Beta 可发布，正式支持前必须补测 |
| FAIL | 与基准冲突且没有可接受解释 | 不应宣称支持，需修复或降级 |

## 对比方式

| 接口类型 | 对比方式 | 典型断言 |
| --- | --- | --- |
| 原始日线/分钟线 | 与 JQData/MiniQMT 数值对账 | OHLC 在价格容忍度内，volume/money 单位一致 |
| 前复权/后复权 | 跨分红窗口对账 | 不能只测最近几天，必须覆盖除权除息事件 |
| 动态前复权 | `raw * factor / factor_ref` 对账 | 多个 `pre_factor_ref_date`，至少一个跨分红参考日 |
| factor | 与动态复权结果互相验证 | factor 能单独返回，并能支撑外层动态缓存 |
| 交易日 | 与基准日历集合对比 | 日期集合、count 语义、边界日期一致或说明差异 |
| 证券列表 | schema 和样例标的校验 | 必备列、代码格式、样例股票/ETF/指数存在 |
| 指数成分/权重 | 样例指数集合对比 | 成分数量、关键成分、历史视角；无法全等时说明来源差异 |
| 实时快照 | 字段和基本区间校验 | last_price、涨跌停、停牌字段存在；不与历史分钟混用 |
| 除权除息 | 事件字段和复权闭环验证 | 事件能构造 factor，且复权价格能对账 |
| 财务/行业/概念等扩展接口 | 权限或 schema 验证 | 没有权限时标 BLOCKED，不要伪造兼容 |

## 必测函数清单

| 层级 | 函数 | 最低验收要求 |
| --- | --- | --- |
| 核心 | `auth` | 缺依赖、缺账号、连接失败要有清晰错误；真实模式不能自动返回 stub |
| 核心 | `get_price` | 日线、分钟线、单证券、多证券、`panel=True/False`、`count/end_date`、`start/end`、字段映射、单位 |
| 核心 | `get_price(fq="none")` | 未复权原始价对账 |
| 核心 | `get_price(fq="pre")` | 跨分红窗口前复权对账 |
| 核心 | `fields=["factor"]` | 能单独返回 factor，或明确 UNSUPPORTED |
| 核心 | `pre_factor_ref_date` | 动态前复权对账，或明确 UNSUPPORTED |
| 核心 | `get_trade_days` / `get_trade_day` | 交易日集合、`count` 和边界日期 |
| 核心 | `get_all_securities` / `get_security_info` | schema、代码格式、样例标的、历史视角说明 |
| 核心 | `get_split_dividend` | 事件字段、单位、与复权闭环一致 |
| 常用 | `get_bars` | 与 `get_price` 等价窗口、字段和返回形状 |
| 常用 | `get_current_tick` / `get_live_current` | 实时字段、涨跌停、停牌状态、失败模式 |
| 指数 | `get_index_stocks` / `get_index_weights` | 样例指数成分/权重；无法支持时明确空返回或抛错 |
| 扩展 | `get_extras`、`get_fundamentals`、行业/概念/融资融券/期货等 | 有实现就按基准验收；无实现必须稳定 `NotImplementedError` |
| API 兼容 | `history`、`attribute_history`、`get_current_data` | 通过 provider 后还要测聚宽风格外层 API 的 shape 和字段 |

## 报告模板

每个数据源接入 PR 或发布说明应包含以下表格：

| 函数 | 场景 | 基准 | 样例/窗口 | 状态 | 结果摘要 | 兼容尝试 | 剩余风险 | 测试入口 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `get_price` | 日线未复权 | JQData | `000001.XSHE` 等 | PASS | OHLC 最大偏差 | 字段/单位映射 | 账号权限 | `tests/...` |
| `get_all_securities` | 股票/ETF/指数列表 | JQData | 样例标的 | PARTIAL | schema 通过 | 代码格式转换 | 全量不保证一致 | `tests/...` |

报告必须记录：

- 运行日期、数据源版本、Python 版本、平台、账号或 SDK 前置条件。
- 基准数据源和基准可见截止日。
- 数值容忍度和为什么采用该容忍度。
- 不可比函数的原因：数据源定义不同、免费接口限制、权限不足、实时数据不可回放等。
- 失败或限制是否影响默认 provider、回测、实盘或只影响显式启用的数据源。

## easy_tdx 打样结果

以下是 2026-06-16 本机 online 验收样例，作为后续 provider 报告格式参考。

| 函数 | 场景 | 基准 | 状态 | 结果摘要 | 兼容尝试/说明 |
| --- | --- | --- | --- | --- | --- |
| `auth` | 自动选择通达信服务器 | online smoke | PASS | 能连接并返回 quote/K 线 | 连接前创建 `~/.easy_tdx`，避免上游保存配置失败 |
| `get_price` | 日线未复权长窗口 | JQData | PASS | `000001.XSHE`、`600519.XSHG`、`510050.XSHG` 在 2024-01-01 到 2026-06-12 的 OHLC 基本对齐 | 修正旧日期短窗口 count 估算，避免只拉最新几根 |
| `get_price` | 1m 未复权近期窗口 | JQData | PASS | 2026-06-12 14:31 到 15:00 价格字段最大偏差约 `5e-7` | 仅代表近期分钟线 |
| `get_price` | 1m 旧窗口 | JQData | LIMIT | 2015-12 和 2025-12 样例返回 0 行；JQData 2015-12 有 5520 行 | 免费 online 分钟线历史深度限制，不宣称长期分钟回测 |
| `get_price(fq="pre")` | 日线前复权长窗口 | JQData | PASS | 构造 factor 后，三个样例最大价格偏差约 `0.091` 元以内 | 不直接信 SDK 自带前复权；用 TDX 除权除息事件构造 factor |
| `fields=["factor"]` | 日线/近期分钟 factor | 复权闭环 | PASS | 能返回构造 factor，支撑外层动态前复权缓存 | `fenhong` 按每股现金，`songzhuangu/peigu` 按每 10 股折算 |
| `pre_factor_ref_date` | 动态前复权日线 | JQData | PASS | `600519.XSHG`、`pre_factor_ref_date=2025-06-27` 最大偏差约 `0.094` 元 | 用 `raw * factor / factor_ref` 锚定 |
| `get_bars` | 聚宽兼容 K 线窗口 | provider unit | PASS | 已覆盖包装 `get_price` 的 shape | 真实对账可复用 `get_price` 核心结果 |
| `get_trade_days` | 交易日历 | online smoke | PASS | 通过上证指数日线推导，支持日期范围和 count 语义 | 不宣称与 JQData 日历源逐日全量强一致 |
| `get_all_securities` | 股票/ETF/指数列表 | schema/smoke | PASS | 返回股票、基金、指数样例和聚宽格式代码 | 免费 online 列表不保证与 JQData 全量一致 |
| `get_security_info` | 单标的信息 | heuristic/unit | PASS | 能区分深市股票、上证指数、基金等常见类型 | 名称和历史起止日期不作为强一致 |
| `get_index_stocks` | 指数成分 | online board 接口 | LIMIT | 接口不可稳定时返回空列表 | 不把空列表解释为指数无成分 |
| `get_split_dividend` | 除权除息事件 | 复权闭环/JQData | PASS | 事件字段已用于构造 factor 并通过复权对账 | 直接事件列表仍需更多公司行为样例 |
| `get_current_tick` / `get_live_current` | 实时 quote | online smoke | PASS | 能返回最新价、涨跌停、停牌状态字段 | 实时值不和历史回放做强一致 |
| 扩展接口 | 财务、行业、概念、融资融券等 | 无 | UNSUPPORTED | 未作为 easy_tdx online Beta 能力 | 保持 `NotImplementedError` 或不暴露支持 |

## RQData 打样状态

RQData 当前完成 mock 合同测试，但缺真实账号，因此真实验收报告必须标为 BLOCKED：

| 函数 | 状态 | 当前结论 |
| --- | --- | --- |
| `get_price`、`factor`、`pre_factor_ref_date` | PARTIAL | mock 已覆盖 raw + factor + 动态锚定路径 |
| `get_trade_days`、`get_all_securities`、指数成分/权重 | PARTIAL | mock 已覆盖 shape 和字段，真实账号待验收 |
| 实时和扩展能力 | BLOCKED | 需要 RQData license/API 权限后补真实对账 |

RQData 拿到账号后，应按 easy_tdx 同样格式补真实报告，不能只因为 SDK 文档支持就标 PASS。
