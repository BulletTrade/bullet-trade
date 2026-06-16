# 配置总览

这页分两层：

- 前半部分是**最小可跑配置**，新机器先按这里配通。
- 后半部分是**当前代码仍然生效的完整配置索引**。有默认值的不一定要写进 `.env`，但文档里必须能查到。

没有列入这里的旧变量，通常表示当前代码没有读取，或者只是某个示例脚本里的 Python 常量，不再作为通用 `.env` 配置入口。

## 1. 本地 QMT 最小配置

适用场景：策略和 QMT 在同一台 Windows 机器上运行。

```env
DEFAULT_DATA_PROVIDER=qmt
DEFAULT_BROKER=qmt
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
```

运行：

```bash
bullet-trade live strategies/demo_strategy.py --broker qmt
```

补充：

- 股票账户默认就是 `stock`，所以 `QMT_ACCOUNT_TYPE` 不用写。
- 只有期货账户才需要写 `QMT_ACCOUNT_TYPE=future`。

## 2. 远程 server 最小配置

适用场景：Windows 机器只负责连 QMT，对外提供远程行情和交易服务。

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

运行：

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

补充：

- 当前版本不支持 `--data-path`。
- `QMT_DATA_PATH` 必须写在 `.env`。

## 3. 远程 qmt-remote 客户端最小配置

适用场景：策略跑在另一台机器上，通过 TCP 连远程 server。

```env
DEFAULT_DATA_PROVIDER=qmt-remote
DEFAULT_BROKER=qmt-remote
QMT_SERVER_HOST=10.0.0.8
QMT_SERVER_PORT=58620
QMT_SERVER_TOKEN=secret
```

运行：

```bash
bullet-trade live strategies/demo_strategy.py --broker qmt-remote
```

单账户时，不用写 `QMT_SERVER_ACCOUNT_KEY`。

如果策略外层还接了其他调度网关，要分清两层端口：

- `QMT_SERVER_PORT`：bullet-trade server 端口，默认常用 `58620`。
- 上层网关端口：由上层系统自己的配置维护，不要填到 `QMT_SERVER_PORT`。

## 4. 重点配置

这些不是都必须配置，但排查实盘时最常用。

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `MARKET_BUY_PRICE_PERCENT` | `0.015` | 普通 `bullet-trade` 市价买入保护价偏移，参考价乘以 `1 + pct`，默认买高 1.5%。 |
| `MARKET_SELL_PRICE_PERCENT` | `-0.015` | 普通 `bullet-trade` 市价卖出保护价偏移，参考价乘以 `1 + pct`，默认卖低 1.5%。 |
| `TRADE_MAX_WAIT_TIME` | `16` | 实盘下单/撤单同步等待秒数；设为 `0` 或小于等于 `0` 时走异步立即返回。 |
| `QMT_SERVER_RPC_TIMEOUT` | `60` | `qmt-remote` 客户端等待远程 server 响应的网络超时；应大于 `TRADE_MAX_WAIT_TIME` 或单次 `wait_timeout` 加安全余量。 |
| `QMT_PLACE_ORDER_TIMEOUT_MARGIN` | `30` | 远程下单时在 `wait_timeout` 之外追加的请求超时安全余量，避免服务端刚返回 open/timed_out 时客户端先超时。 |
| `MIN_BUY_ORDER_VALUE` | `0` | 最小买入订单金额；`0` 表示不限制，只拦截买入，不拦截卖出、降仓、清仓。 |
| `RISK_CHECK_ENABLED` | `false` | 本地 LiveEngine 风控开关；开启后才使用本地风控参数。 |
| `QMT_SERVER_ORDER_RISK_ENABLED` | `false` | 远程 QMT server 风控开关；开启后 server 端才使用风控参数。 |

## 5. 多账户参数

### server 端

如果一个 server 上挂多个账户，才需要 `--accounts` 或 `QMT_SERVER_ACCOUNTS`。

股票账户：

```bash
--accounts main=123456
```

期货账户：

```bash
--accounts hedge=654321:future
```

也可以写成环境变量：

```env
QMT_SERVER_ACCOUNTS=main=123456,hedge=654321:future
```

### 客户端

多账户时，客户端才需要指定：

```env
QMT_SERVER_ACCOUNT_KEY=main
QMT_SERVER_SUB_ACCOUNT=demo@main
```

## 6. 通用配置

| 变量 | 默认/示例 | 作用 |
| --- | --- | --- |
| `BT_ENV_FILE` / `BULLET_TRADE_ENV_FILE` / `ENV_FILE` | `./.env.live` | 显式指定要加载的 `.env` 文件；优先于自动向上查找 `.env`。 |
| `DEFAULT_DATA_PROVIDER` | `jqdata` | 默认行情源：`jqdata`、`tushare`、`qmt`、`qmt-remote`、`rqdata`、`easy_tdx`。RQData/easy_tdx 仍为 Beta，需要显式启用。 |
| `DEFAULT_BROKER` | `simulator` | 默认券商/交易通道：`simulator`、`qmt`、`qmt-remote`。 |
| `LOG_DIR` | `./logs` | 日志目录。 |
| `LOG_LEVEL` | `INFO` | 控制台日志级别。 |
| `LOG_FILE_LEVEL` | 跟随 `LOG_LEVEL` | 文件日志级别。 |
| `RUNTIME_DIR` | `./runtime` | 运行态目录，保存 live 状态、`g` 自动保存等文件。 |
| `BULLET_TRADE_HOME` | 用户 home | 研究环境、JupyterLab 设置、live lock 等默认目录的根路径覆盖。 |
| `MESSAGE_KEY` / `WECHAT_MESSAGE_KEY` | 空 | 企业微信机器人 key，配置后 `send_msg` 会尝试推送。 |
| `BT_LIVE_ORDER_DEBUG` | `false` | 打印 live/QMT 下单调试日志。 |
| `LOG_FORCE_COLOR` | 空 | 强制彩色日志。 |
| `NO_COLOR` | 空 | 禁用彩色日志。 |

## 7. 数据源与缓存

| 变量 | 默认/示例 | 作用 |
| --- | --- | --- |
| `DATA_CACHE_DIR` | 空 | 行情缓存根目录；有值时按 provider 创建子目录，留空禁用磁盘缓存。 |
| `JQDATA_USERNAME` / `JQDATA_PASSWORD` | 空 | 聚宽数据账号密码。 |
| `JQDATA_USER` / `JQDATA_PWD` | 空 | 聚宽账号密码旧别名，仍兼容。 |
| `JQDATA_SERVER` / `JQDATA_PORT` | 空 / `0` | 聚宽自定义服务地址和端口。 |
| `JQDATA_CACHE_EXPIRE_DAYS` | `1` | 动态区间缓存过期天数。 |
| `JQDATA_CACHE_FORMAT` | `parquet` | DataFrame 缓存格式；失败时内部会回退。 |
| `JQDATA_CACHE_VERSION` | `2` | 缓存 schema 版本；改值可强制旧缓存失效。 |
| `TUSHARE_TOKEN` | 空 | Tushare token。 |
| `TUSHARE_CUSTOM_URL` | 空 | Tushare 自定义接入地址。 |
| `RQDATA_LICENSE` | 空 | RQData license；配置后优先使用 license 初始化 `rqdatac`。 |
| `RQDATA_USERNAME` / `RQDATA_PASSWORD` | 空 | RQData 用户名和密码；未配置 license 时使用。 |
| `RQDATA_USER` / `RQDATA_PWD` | 空 | RQData 账号密码旧别名，仍兼容。 |
| `EASY_TDX_HOST` / `EASY_TDX_PORT` | 自动选主机 / `7709` | easy_tdx 通达信 online 行情服务器地址和端口；留空 host 时使用 SDK 自动选主机。 |
| `EASY_TDX_TIMEOUT` | `10` | easy_tdx 连接超时秒数。 |
| `EASY_TDX_USE_STUB` | `false` | 仅用于测试/demo 的显式 stub 开关；真实模式连接失败不会返回假行情。 |
| `QMT_DATA_PATH` | 空 | MiniQMT/xtquant 数据目录；本地 QMT 行情和本地 QMT 交易都依赖它。 |
| `MINIQMT_AUTO_DOWNLOAD` | `true` | MiniQMT 行情是否自动下载；设为 `false` 时只读本地缓存。 |
| `MINIQMT_MARKET` | `SH` | MiniQMT 行情市场代码。 |
| `QMT_SERVER_HOST` / `QMT_SERVER_PORT` / `QMT_SERVER_TOKEN` | `127.0.0.1` / `58620` / 空 | `qmt-remote` 行情和交易客户端连接远程 bullet-trade server。 |
| `QMT_SERVER_TLS_CERT` | 空 | `qmt-remote` 客户端 TLS 证书路径。 |

## 8. 券商与本地 QMT

| 变量 | 默认/示例 | 作用 |
| --- | --- | --- |
| `SIMULATOR_INITIAL_CASH` | `1000000` | `simulator` 券商初始资金。 |
| `QMT_ACCOUNT_ID` | 空 | 本地 QMT 账户号；server 端没有 `QMT_SERVER_ACCOUNTS` 时也会作为 default 账户。 |
| `QMT_ACCOUNT_TYPE` | `stock` | QMT 账户类型，例如 `stock` / `future`。 |
| `QMT_DATA_PATH` | 空 | QMT 数据目录。 |
| `QMT_SESSION_ID` | 空 | 固定 QMT session id；不填时由运行时生成或按默认逻辑处理。 |
| `QMT_AUTO_SUBSCRIBE` | `true` | QMT 连接后是否自动订阅账户。 |

## 9. 实盘运行参数

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `ORDER_MAX_VOLUME` | `1000000` | 单笔委托最大股数，超过会拆单。 |
| `TRADE_MAX_WAIT_TIME` | `16` | 下单/撤单同步等待秒数；小于等于 `0` 时异步立即返回。 |
| `QMT_SERVER_RPC_TIMEOUT` | `60` | 远程 QMT 客户端默认 RPC 超时，只保护网络响应，不代表等待成交时间。 |
| `QMT_PLACE_ORDER_TIMEOUT_MARGIN` | `30` | 远程下单请求超时相对订单等待窗口的默认余量。 |
| `EVENT_TIME_OUT` | `60` | 策略事件超时秒数。 |
| `STRATEGY_NAME` | 空 | 策略名称，用于订单备注和日志标识；未设置时通常用策略文件名。 |
| `SCHEDULER_MARKET_PERIODS` | 空 | 覆盖交易时段，例如 `09:30-11:30,13:00-15:00`。 |
| `ACCOUNT_SYNC_ENABLED` / `ACCOUNT_SYNC_INTERVAL` | `true` / `60` | 账户后台同步开关和间隔秒数。 |
| `ORDER_SYNC_ENABLED` / `ORDER_SYNC_INTERVAL` | `true` / `10` | 订单后台轮询开关和间隔秒数。 |
| `G_AUTOSAVE_ENABLED` / `G_AUTOSAVE_INTERVAL` | `true` / `60` | `g` 状态自动保存开关和间隔秒数。 |
| `TICK_SUBSCRIPTION_LIMIT` | `100` | Tick 订阅标的数量上限。 |
| `TICK_SYNC_ENABLED` / `TICK_SYNC_INTERVAL` | `true` / `2` | Tick 轮询同步开关和间隔秒数。 |
| `RISK_CHECK_ENABLED` / `RISK_CHECK_INTERVAL` | `false` / `300` | 本地 LiveEngine 风控后台任务开关和间隔秒数。 |
| `CALENDAR_SKIP_WEEKEND` | `true` | 非交易日检测时是否直接跳过周末。 |
| `CALENDAR_RETRY_MINUTES` | `1` | 非交易日或交易日历暂不可用时的重试间隔分钟数。 |
| `BROKER_HEARTBEAT_INTERVAL` | `30` | 券商心跳检测间隔秒数。 |
| `PORTFOLIO_REFRESH_THROTTLE_MS` | `200` | 读取实时资金/持仓前的最小刷新间隔毫秒数。 |
| `MARKET_BUY_PRICE_PERCENT` | `0.015` | 普通市价买入保护价偏移比例；例如 `0.015` 表示参考价上浮 1.5%。 |
| `MARKET_SELL_PRICE_PERCENT` | `-0.015` | 普通市价卖出保护价偏移比例；例如 `-0.015` 表示参考价下浮 1.5%。 |

## 10. 风控参数

风控参数只在对应风控开关打开后拦截订单：

- 本地 LiveEngine：`RISK_CHECK_ENABLED=true`
- 远程 QMT server：`QMT_SERVER_ORDER_RISK_ENABLED=true`

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `MAX_ORDER_VALUE` | `100000` | 单笔订单金额上限。 |
| `MAX_DAILY_TRADE_VALUE` | `500000` | 单日累计交易金额上限。 |
| `MAX_DAILY_TRADES` | `100` | 单日最大交易次数。 |
| `MAX_DAILY_CANCELS` | `100` | 单日最大撤单次数。 |
| `MIN_CANCEL_INTERVAL_SECONDS` | `0.0` | 两次撤单之间的最小间隔秒数。 |
| `MAX_CANCEL_PER_ORDER` | `3` | 单笔订单最大撤单尝试次数。 |
| `MIN_BUY_ORDER_VALUE` | `0.0` | 最小买入订单金额；`0` 表示不限制，只检查买入。 |
| `MAX_STOCK_COUNT` | `20` | 最大持仓标的数，仅买入检查。 |
| `MAX_POSITION_RATIO` | `20.0` | 单标下单金额占总资产比例上限，单位是百分比。 |
| `STOP_LOSS_RATIO` | `5.0` | 止损阈值，供风控辅助判断使用。 |

## 11. 远程 QMT server

这些配置作用在 `bullet-trade server` 进程。

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `QMT_SERVER_TYPE` | `qmt` | server adapter 类型。 |
| `QMT_SERVER_LISTEN` | `0.0.0.0` | server 监听地址。 |
| `QMT_SERVER_PORT` | `58620` | server 监听端口。 |
| `QMT_SERVER_TOKEN` | 自动生成临时 token | 访问 token；生产环境必须显式配置。 |
| `QMT_SERVER_ENABLE_DATA` | `true` | 是否启用远程行情接口。 |
| `QMT_SERVER_ENABLE_BROKER` | `true` | 是否启用远程交易接口。 |
| `QMT_SERVER_TLS_CERT` / `QMT_SERVER_TLS_KEY` | 空 | server 端 TLS 证书和私钥；两者都有值时启用 TLS。 |
| `QMT_SERVER_ALLOWLIST` | 空 | 允许访问的 IP 或 CIDR 列表，逗号分隔。 |
| `QMT_SERVER_MAX_CONNECTIONS` | `64` | 最大连接数。 |
| `QMT_SERVER_MAX_SUBSCRIPTIONS` | `200` | 最大订阅数。 |
| `QMT_SERVER_ALLOW_FULL_MARKET` | `false` | 是否允许全市场订阅。 |
| `QMT_SERVER_LOG_FILE` | 空 | server 访问日志文件路径。 |
| `QMT_SERVER_LOG_ACCOUNT` | `false` | 是否打印账户快照。 |
| `QMT_SERVER_ACCESS_LOG` | `true` | 是否启用访问日志。 |
| `QMT_SERVER_ORDER_RISK_ENABLED` | `false` | 是否启用 server 端订单/撤单风控。 |
| `QMT_SERVER_IDEMPOTENCY_TTL_SECONDS` | `300` | 下单幂等缓存窗口秒数，避免重试导致重复下单。 |
| `QMT_SERVER_ACCOUNTS` | 空 | 多账户映射，例如 `main=123456,hedge=654321:future`。 |
| `QMT_SERVER_SUB_ACCOUNTS` | 空 | 子账户映射，例如 `demo@main:limit=50000`。 |

## 12. qmt-remote 客户端

这些配置作用在远程客户端，也就是 `DEFAULT_DATA_PROVIDER=qmt-remote` 或 `DEFAULT_BROKER=qmt-remote` 的一侧。

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `QMT_SERVER_HOST` | `127.0.0.1` | 远程 bullet-trade server 地址。 |
| `QMT_SERVER_PORT` | `58620` | 远程 bullet-trade server 端口。 |
| `QMT_SERVER_TOKEN` | 空 | 远程访问 token；客户端必填。 |
| `QMT_SERVER_TLS_CERT` | 空 | 客户端 TLS 证书路径。 |
| `QMT_SERVER_ACCOUNT_KEY` | 空 | 多账户时指定 server 账户 key。 |
| `QMT_SERVER_SUB_ACCOUNT` | 空 | 子账户 ID，用于远程账户路由。 |

## 13. 下单规则配置文件

下单的最小手数、步进、证券分类、涨跌停兜底规则在这个文件里：

```text
bullet_trade/config/security_overrides.json
```

常用字段：

- `lot_rules`：最小手数与步进规则。
- `security_defaults`：按证券分类设置 T+0、滑点、价格精度。
- `by_code`：按单个证券覆盖分类、T+0、滑点等。
- `limit_rules`：数据源没有返回涨跌停时，用前收或最新价按比例兜底。

## 14. 一个判断原则

如果只是先跑通：

- 本地 QMT：只配 `DEFAULT_DATA_PROVIDER`、`DEFAULT_BROKER`、`QMT_DATA_PATH`、`QMT_ACCOUNT_ID`。
- 远程 server：只配 `QMT_DATA_PATH`、`QMT_ACCOUNT_ID`、`QMT_SERVER_TOKEN`。
- 远程客户端：只配 `DEFAULT_DATA_PROVIDER`、`DEFAULT_BROKER`、`QMT_SERVER_HOST`、`QMT_SERVER_PORT`、`QMT_SERVER_TOKEN`。

其他配置先不写进 `.env` 也可以，但需要查的时候，上面的索引应该都能找到。
