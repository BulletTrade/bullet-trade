# 实盘引擎

这页只讲最常见的两种实盘方式：

- 本地 QMT
- 远程 qmt-remote

## 1. 本地 QMT 最小配置

这是 MiniQMT/xtquant 直连模式，要求本机能访问 `userdata_mini`。大 QMT 不支持直接套用这组 `QMT_DATA_PATH` 配置；大 QMT 需要先启动 helper，再通过远程 `qmt-remote` 使用，见 [大 QMT 服务向导](big-qmt-server.md)。

`.env` 最少只写这几个：

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

说明：

- 股票账户默认就是 `stock`，所以 `QMT_ACCOUNT_TYPE` 不用写
- 只有期货账户才需要写 `QMT_ACCOUNT_TYPE=future`

## 2. 远程 qmt-remote 最小配置

如果 QMT 在另一台 Windows 机器上，客户端 `.env` 只要：

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

## 3. 远程 server 怎么启动

下面是 MiniQMT 后端的 server 启动方式。大 QMT 后端请使用 [大 QMT 服务向导](big-qmt-server.md) 里的 `--server-type big_qmt` 和 `BIG_QMT_GATEWAY_*` 配置。

Windows 服务端 `.env`：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

启动：

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

更完整的 MiniQMT server 说明看 [QMT server](qmt-server.md)。大 QMT 后端看 [大 QMT 服务向导](big-qmt-server.md)。

## 4. 多账户时再看这两个参数

### server 端

多账户时才用 `--accounts`。  
股票账户示例：

```bash
--accounts main=123456
```

期货账户示例：

```bash
--accounts hedge=654321:future
```

### 客户端

多账户时，客户端才需要：

```env
QMT_SERVER_ACCOUNT_KEY=main
```

## 5. 模拟盘/实盘切换检查

模拟盘和实盘应尽量使用同一份策略代码，只通过 `.env` 切换账户、网关地址和风控参数。切换前先确认这几件事：

| 检查项 | 模拟盘 | 实盘 |
|--------|--------|------|
| QMT 账号 | 仿真或测试资金号 | 真实资金号 |
| bullet-trade server | 默认 `58620`，提供行情和交易能力 | 默认 `58620`，提供行情和交易能力 |
| 上层调度网关 | 如使用上层网关，策略通常连接网关自己的端口 | 同样连接上层网关端口，再由网关连接 bullet-trade server |
| 下单等待 | 可用 `TRADE_MAX_WAIT_TIME=0` 压测异步链路 | 建议保留同步等待或按单设置 `wait_timeout` |
| 风控 | 先放宽，确认链路能跑通 | 再启用 `RISK_CHECK_ENABLED`、`MIN_BUY_ORDER_VALUE` 等 |

不要把 `QMT_SERVER_PORT` 和上层调度网关端口混用：`QMT_SERVER_PORT` 是 bullet-trade server 的端口；上层网关自己的端口应写在上层系统配置里。

## 6. 下单等待（同步/异步）

实盘下单时，引擎默认会**同步等待最多 16 秒**再返回结果。可以通过两种方式调整：

| 方式 | 说明 |
|------|------|
| `.env` 设置 `TRADE_MAX_WAIT_TIME` | 全局生效，默认 `16`；设 `0` 为纯异步 |
| 函数参数 `wait_timeout=10` | 单次下单覆盖，优先级高于环境变量 |

`TRADE_MAX_WAIT_TIME` / `wait_timeout` 是订单终态等待窗口，不是网络请求超时。远程 `qmt-remote` 默认使用 `QMT_SERVER_RPC_TIMEOUT=60`，并在下单时保证请求超时大于 `wait_timeout + QMT_PLACE_ORDER_TIMEOUT_MARGIN`，避免订单已提交但客户端先报 RPC 超时。长连接 `RemoteQmtBroker` 如果显式配置了默认等待窗口，会把同一个等待值传给 server 并用于 RPC timeout 预算；如果没有配置，则保持旧行为，由 server 端使用自己的默认等待设置。

server session 的外层请求超时默认也是 60 秒；当 `broker.place_order` 显式传入更长 `wait_timeout` 时，会自动扩展到 `wait_timeout + 30s`，避免 server 外层先于订单等待窗口超时。

策略中批量异步下单示例：

```python
order_target('000001.XSHE', 0, wait_timeout=0)   # 立即返回
order('600519.XSHG', 100, wait_timeout=10)         # 等 10 秒
```

### 远程下单兼容性与升级边界

本次远程 QMT 下单语义保持向后兼容：

- `buy` / `sell` / `order` 正常提交后仍返回订单号字符串，旧策略不需要改成读取新对象。
- `broker.place_order` 响应只新增可选字段，例如 `timed_out`、`async_tracking`、`last_snapshot`、`sub_account_id`，旧客户端可以忽略。
- `broker.orders` / `broker.trades` 仍返回原有 list/dict 结构；新字段只用于排查和上层系统认领迟到订单。
- 新 helper 连接旧 server 时，缺少这些新增字段也能运行；旧 helper 连接新 server 时，未知字段不会影响旧字段读取。
- 若看到 `status=open/submitted` 且 `timed_out=true`，含义是“委托已提交但等待终态超时”，不是下单失败；后续应通过订单/成交查询确认最终状态。
- 若客户端网络超时且没有拿到 `order_id`，只能视为 `submit_unknown`，需要后续查订单/成交，不应直接重复下单。

开源用户升级时建议保留默认值：

```env
QMT_SERVER_RPC_TIMEOUT=60
QMT_PLACE_ORDER_TIMEOUT_MARGIN=30
```

聚宽短连接 helper 不读取 `.env`，需要在初始化时传参；不传时也使用相同默认值：

```python
bt.configure(
    host="127.0.0.1",
    token="secret",
    rpc_timeout=60,
    place_order_timeout_margin=30,
)
```

如果明确希望只按 `wait_timeout` 本身设置请求超时，可以把 `place_order_timeout_margin=0`；该显式 0 值会被保留，不会被默认 30 秒覆盖。

如果显式把 `TRADE_MAX_WAIT_TIME` 调大，也应同步确认 RPC timeout 至少大于 `wait_timeout + margin`。

## 7. 常见问题

### 为什么文档里不再写一大堆 `.env`

因为绝大多数参数都有默认值。  
第一步应该先跑通最小链路，不要一开始就把日志、风控、后台任务、通知全部写进去。

正式实盘如果账号没有免五，建议在开启风控后按需配置 `MIN_BUY_ORDER_VALUE`，例如 `MIN_BUY_ORDER_VALUE=1000`。默认值为 `0`，不限制买入小单；该规则只拦截买入，不影响卖出或清仓。

### `:stock` 要不要写

单账户股票场景不用写。  
默认就是 `stock`。

### `--data-path` 为什么不能写

因为当前版本没有这个 CLI 参数。  
MiniQMT 数据目录要写在 `.env` 的 `QMT_DATA_PATH`。大 QMT 不使用这个配置入口，见 [大 QMT 服务向导](big-qmt-server.md)。

### 运行态目录和日志目录要不要先配

先不用。  
除非你有明确的目录要求，否则先用默认值即可。
