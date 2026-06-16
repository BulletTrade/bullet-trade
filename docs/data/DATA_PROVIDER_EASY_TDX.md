# easy_tdx 通达信数据源 Beta

`EasyTdxProvider` 位于 `bullet_trade/data/providers/easy_tdx.py`，通过 `DEFAULT_DATA_PROVIDER=easy_tdx`、`DEFAULT_DATA_PROVIDER=tdx` 或 `set_data_provider("easy_tdx")` 启用。

## 安装与平台

easy_tdx online 模式通过 SDK 直连通达信行情服务器，不需要安装 Windows 通达信客户端。本机 macOS/Linux 也可以测试在线行情。

```bash
pip install "bullet-trade[tdx]"
```

注意：`easy-tdx` 上游包要求 Python 3.10+。BulletTrade 主包仍支持 Python 3.8+，因此 `tdx` extra 对 `easy-tdx` 使用了 Python 版本 marker；Python 3.8/3.9 用户需要单独升级环境后再使用该 provider。

`.env` 示例：

```env
DEFAULT_DATA_PROVIDER=easy_tdx

# 留空时由 easy_tdx 自动选择可用服务器
EASY_TDX_HOST=
EASY_TDX_PORT=7709
EASY_TDX_TIMEOUT=10

# 仅单元测试或演示使用；真实回测不要打开
EASY_TDX_USE_STUB=false
```

## 关键安全边界

Beta 版本保留 `use_stub=True`，但只用于测试和 demo。真实模式下：

- 缺少 `easy_tdx` 依赖会抛出清晰安装错误。
- 连接行情服务器失败会抛出错误，不会自动返回假行情。
- `factor` 和 `pre_factor_ref_date` 通过通达信除权除息事件构造，仍属于 Beta 能力；新增标的类型或异常公司行为需要测试补充样例。

这样做的目的是避免用户在真实回测或实盘中误用自动生成的假行情。

## 支持能力

当前优先支持：

- `get_price()`：日线、1m、5m、15m、30m、60m K 线，字段映射为 `open/high/low/close/volume/money`。
- `get_current_tick()` / `get_live_current()`：通过 quote 快照读取当前价、涨跌停和停牌状态；如果上游字段缺失，外层 `current_data` 仍会按已有规则兜底。
- `get_trade_days()`：通过上证指数日线推导交易日。
- `get_all_securities()`：依赖 easy_tdx 的 quotes-list 能力，实际完整性以 online 服务器返回为准。
- `get_split_dividend()`：尝试使用 TdxClient 读取除权除息记录；不可用时返回空列表。

行情单位：

- K 线 `volume` 按 easy_tdx 返回值保持为股。
- quote 快照 `vol` 按通达信手数换算为股。
- `money` 保持元。
- 代码输入支持聚宽格式，例如 `000001.XSHE`、`600519.XSHG`。

## 免费数据使用约束

通达信 online 行情属于免费行情能力，必须按能力边界使用：

| 能力 | 当前结论 |
| --- | --- |
| 股票/ETF/指数日线 | 通过 online K 线接口探测，实际历史深度以服务器返回为准 |
| 分钟线 | 通常只覆盖近期数据；越老的 1m/5m/15m 数据越可能为空 |
| 实时 quote | 支持轮询快照，不等同于交易所逐笔或完整 Level2 |
| 历史 tick/逐笔 | Beta 阶段未作为回测核心能力接入 |
| 复权 | 未复权 K 线使用 online 原始数据；前复权优先用除权除息事件构造 `factor` 后计算，不再直接依赖 SDK 自带前复权 |
| 动态真实价格 | 支持日线 `pre_factor_ref_date` 动态前复权；分钟线动态前复权受限于免费分钟线历史深度 |
| 请求频率 | 免费服务器可能限流或断开；批量任务应控制 sleep 和重试 |
| Windows 客户端 | online 模式不需要；只有离线文件或安装目录验证才需要 Windows |

2026-06-16 在本机 `easy-tdx==1.14.4`、自动选择行情服务器、`000001.XSHE`、`count=50000` 的探测结果：

| 频率 | 返回行数 | 最早时间 | 最新时间 |
| --- | ---: | --- | --- |
| 日线 | 8390 | 1991-04-03 | 2026-06-16 |
| 1m | 23280 | 2026-01-19 09:31 | 2026-06-16 15:00 |
| 5m | 23856 | 2024-05-29 09:35 | 2026-06-16 15:00 |
| 15m | 7952 | 2024-05-29 09:45 | 2026-06-16 15:00 |
| 30m | 3976 | 2024-05-29 10:00 | 2026-06-16 15:00 |
| 60m | 1988 | 2024-05-29 10:30 | 2026-06-16 15:00 |

同次探测中，证券列表返回股票约 5040 条、基金约 1609 条、指数约 100 条；quote 可返回最新价和涨跌停。上述结果只代表本次自动选择服务器的样例，不应写死为协议保证。注意：`count=8000` 只会返回最新 8000 条，不代表历史上限；做历史深度探测应使用更大的 `count` 或分页偏移。

同日进一步指定旧分钟窗口探测，`000001.XSHE`、`600519.XSHG`、`510050.XSHG` 的 easy_tdx online 1m K 线结果如下：

| 窗口 | easy_tdx online 1m | JQData 1m |
| --- | ---: | ---: |
| 2015-12-01 到 2015-12-31 | 0 行 | 5520 行 |
| 2025-12-01 到 2025-12-31 | 0 行 | 未作为本次 online 边界基准 |

结论是：当前接入的 easy_tdx online 免费分钟线不能覆盖 2015 年、2025 年这类旧分钟回测窗口；如果用户的通达信客户端或离线文件有更老分钟数据，那属于后续“本地文件/客户端数据源”能力，不应混同为 online provider 已验证能力。

## 与 JQData 对账

2026-06-16 使用 JQData 当前可见的最后交易日窗口（截至 2026-06-12）做样例对账：

| 场景 | 窗口 | 结果 |
| --- | --- | --- |
| 日线未复权 | 2026-06-10 到 2026-06-12 | `open/high/low/close` 最大偏差约 `4e-7`，`volume` 最大偏差 7 股，`money` 最大偏差约 115 元 |
| 1m 未复权 | 2026-06-12 14:31 到 15:00 | `open/high/low/close` 最大偏差约 `5e-7`，`volume` 最大偏差 28 股，`money` 最大偏差 8 元 |
| 日线前复权 | 2026-06-10 到 2026-06-12 | 构造 factor 后价格字段最大偏差约 0.003 元；JQData 会调整前复权成交量，easy_tdx 不调整成交量，因此前复权成交量不作为一致性判断 |
| 日线未复权长窗口 | 2024-01-01 到 2026-06-12 | 三个样例的 `open/high/low/close` 均能基本对齐，说明基础日 K 线口径可用 |
| 日线前复权长窗口 | 2024-01-01 到 2026-06-12 | 使用 TDX 除权除息事件构造 factor 后，`000001.XSHE`、`600519.XSHG`、`510050.XSHG` 价格字段最大偏差分别约 0.011、0.091、0.0006 |
| 动态前复权 | `pre_factor_ref_date=2025-06-27` | `600519.XSHG` 日线动态前复权最大价格偏差约 0.094 元 |

JQData 当前只可见到 2026-06-12，因此不能用 2026-06-15/2026-06-16 与 easy_tdx 对账；否则会把 JQData 的不可见日期填充值误判为数据源差异。

## 能力探测脚本

仓库提供本机探测脚本：

```bash
python scripts/probe_easy_tdx_capabilities.py --count 50000 --json-output reports/easy_tdx_probe.json
```

也可以指定时间窗口复测旧分钟数据：

```bash
python scripts/probe_easy_tdx_capabilities.py \
  --symbols 000001.XSHE,600519.XSHG,510050.XSHG \
  --frequencies 1m \
  --start-date "2015-12-01 09:30:00" \
  --end-date "2015-12-31 15:00:00" \
  --count 50000 \
  --json-output reports/easy_tdx_probe_2015_12.json
```

报告会记录：

- 探测日期、Python 版本、easy-tdx 版本。
- 样例股票、ETF、指数的日线和分钟线返回行数、时间范围、字段。
- quote 快照字段、最新价、涨跌停。
- 证券列表接口的返回行数和样例。

发布前应把该报告中的实际结果同步到本页“免费数据使用约束”。如果探测失败，发布说明必须保留失败原因，不能把 provider 描述为已完成真实数据验收。
