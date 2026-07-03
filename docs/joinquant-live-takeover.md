# 聚宽模拟盘完全接管设计说明

> 用户接入流程见 [聚宽策略修改方案 2：接管聚宽函数](joinquant-live-takeover-usage.md)。当前文档用于说明兼容层实现边界、账户代理、下单映射和验收清单。

## 目标

让强依赖聚宽平台环境的策略继续在聚宽模拟盘里运行，但账户状态和真实下单由 BulletTrade 完全接管。

目标用户只需要在策略头部增加一小段初始化代码，后面的选股、择时、仓位判断和下单代码尽量不改。

推荐写法：

```python
import bullet_trade_jq_remote_helper as bt


BT_REMOTE_HOST = "your.server.ip"
BT_REMOTE_PORT = 58620
BT_REMOTE_TOKEN = "secret"
BT_ACCOUNT_KEY = "main"
BT_SUB_ACCOUNT_ID = None


def _install_bt(context):
    bt.install_jq_compat(
        globals(),
        context=context,
        host=BT_REMOTE_HOST,
        port=BT_REMOTE_PORT,
        token=BT_REMOTE_TOKEN,
        account_key=BT_ACCOUNT_KEY,
        sub_account_id=BT_SUB_ACCOUNT_ID,
        mirror_jq_orders=False,
        default_wait_timeout=16,
    )


def initialize(context):
    # 原策略原来的 initialize 逻辑继续写在下面
    set_benchmark("000300.XSHG")
    run_daily(market_open, time="09:35")


def process_initialize(context):
    _install_bt(context)
```

如果不想封装 `_install_bt(context)`，也可以直接写在 `process_initialize(context)` 里：

```python
import bullet_trade_jq_remote_helper as bt


def initialize(context):
    # 原策略原来的 initialize 逻辑继续写在下面
    set_benchmark("000300.XSHG")
    run_daily(market_open, time="09:35")


def process_initialize(context):
    bt.install_jq_compat(
        globals(),
        context=context,
        host="your.server.ip",
        port=58620,
        token="secret",
        account_key="main",
        sub_account_id=None,
        mirror_jq_orders=False,
        default_wait_timeout=16,
    )
```

参数说明：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `globals()` | 是 | 当前策略文件的全局命名空间。helper 会在这里替换 `order`、`order_value`、`order_percent`、`order_target`、`order_target_value`、`order_target_percent` 等函数。 |
| `context` | 是 | 聚宽传入的策略上下文。用于判断 `context.run_params.type`，并在模拟盘环境下接管 `context.portfolio` 和 `context.subportfolios`。 |
| `host` | 是 | `bullet-trade server` 地址，可以是公网 IP、内网 IP 或域名。 |
| `port` | 否 | `bullet-trade server` 端口，默认建议 `58620`。 |
| `token` | 是 | server 端配置的访问 token。 |
| `account_key` | 否 | 多账户配置时的账户 key；单账户可以传 `None` 或不传。 |
| `sub_account_id` | 否 | BulletTrade 虚拟子账户 ID；不用虚拟账户时传 `None`。 |
| `mirror_jq_orders` | 否 | 默认 `False`。是否在远程下单成功后，额外调用聚宽原始下单函数做页面展示。真实交易建议保持 `False`。 |
| `default_wait_timeout` | 否 | 默认 `16` 秒。聚宽兼容接管层的下单默认同步等待时间；单次下单传 `wait_timeout=0` 可异步立即返回。 |

安装后，原策略里的这些代码仍按原样写：

```python
cash = context.portfolio.available_cash
total_value = context.portfolio.total_value

if context.portfolio.positions["510300.XSHG"].closeable_amount == 0:
    order_target_value("510300.XSHG", total_value * 0.2)
```

在聚宽模拟盘环境中，上面读取到的是 BulletTrade 远程账户状态，下单也发到 `bullet-trade server`。

## 基本原则

### 1. 回测不接管

当 `context.run_params.type` 是下面任意值时，helper 不替换账户对象，也不替换下单函数：

- `simple_backtest`
- `full_backtest`

回测仍完全由聚宽自己的回测系统处理，避免把历史回测信号误发到真实账户。

### 2. 模拟盘完全接管

当 `context.run_params.type == "sim_trade"` 时，helper 接管两类入口：

- 账户读取：`context.portfolio`、`context.subportfolios[0]`、`positions`、`available_cash`、`total_value` 等
- 交易函数：`order`、`order_value`、`order_percent`、`order_target`、`order_target_value`、`order_target_percent`、`cancel_order`、`get_open_orders`、`get_orders`、`get_trades`

策略中的仓位计算、现金判断和下单金额都基于 BulletTrade 远程账户，避免“用聚宽虚拟盘现金计算，真实账户执行”的错配。

### 3. 默认不在聚宽下单

默认 `mirror_jq_orders=False`。

也就是说，聚宽模拟盘只负责运行策略代码、取聚宽平台数据、打印日志；真实下单只走 BulletTrade。

如果用户明确想让聚宽页面里也显示一份虚拟订单，可以设置：

```python
bt.install_jq_compat(..., mirror_jq_orders=True)
```

此时 helper 会在远程下单成功后，尽力调用一次聚宽原始下单函数做页面展示。这个调用只用于展示，不作为真实交易结果；如果聚宽虚拟盘资金或持仓不匹配导致失败，只记录 warning，不影响远程真实订单。

### 4. 默认同步等待 16 秒

聚宽兼容接管层的下单默认是同步等待，默认等待窗口为 `16` 秒：

```python
bt.install_jq_compat(..., default_wait_timeout=16)
```

这和 BulletTrade 实盘侧 `TRADE_MAX_WAIT_TIME=16` 的默认语义保持一致。策略中如果没有显式传 `wait_timeout`，wrapper 会把 `16` 秒传给远程 server；如果某一次下单希望异步立即返回，可以显式写：

```python
order_target_value("510300.XSHG", 100000, wait_timeout=0)
```

低层 `bt.order(...)` / `bt.order_target_value(...)` 直接调用接口仍保留旧 helper 的兼容行为；`install_jq_compat` 包出来的聚宽同名函数采用同步 16 秒作为默认值。

## 账户状态接管

模拟盘接管后，helper 会从 `bullet-trade server` 拉取真实账户信息，并构造聚宽风格的只读对象。

### Portfolio 字段

需要优先兼容这些常用字段：

- `available_cash`
- `total_value`
- `positions_value`
- `positions`
- `subportfolios`

`context.portfolio.positions` 应表现为一个 dict：

```python
position = context.portfolio.positions["510300.XSHG"]
```

不存在的持仓建议返回空持仓对象，而不是直接抛 `KeyError`，以兼容常见写法：

```python
if context.portfolio.positions[stock].closeable_amount > 0:
    order_target_value(stock, 0)
```

### Position 字段

远程持仓对象至少兼容：

- `security`
- `total_amount`
- `closeable_amount`
- `value`
- `price`
- `avg_cost`
- `hold_cost`

字段映射建议：

| 聚宽字段 | BulletTrade 来源 |
| --- | --- |
| `security` | `RemotePosition.security` |
| `total_amount` | `RemotePosition.amount` |
| `closeable_amount` | `RemotePosition.available` |
| `value` | `RemotePosition.market_value` |
| `price` | `market_value / amount`，无持仓时为 `0` |
| `avg_cost` / `hold_cost` | `RemotePosition.avg_cost` |

### 刷新策略

账户对象应使用远程快照代理，而不是一次性复制。

建议实现为短 TTL 的 lazy snapshot：

- 首次访问 `context.portfolio` 字段时拉取远程账户和持仓
- 同一轮策略回调内复用快照
- 下单、撤单后主动刷新快照
- 远程账户不可用时 fail closed：不回退到聚宽虚拟盘账本，不发送远程订单

## 下单函数接管

兼容聚宽官方常用交易函数签名：

```python
order(security, amount, style=None, side="long", pindex=0, close_today=False)
order_value(security, value, style=None, side="long", pindex=0, close_today=False)
order_percent(security, percent, style=None, side="long", pindex=0, close_today=False)
order_target(security, amount, style=None, side="long", pindex=0, close_today=False)
order_target_value(security, value, style=None, side="long", pindex=0, close_today=False)
order_target_percent(security, percent, style=None, side="long", pindex=0, close_today=False)
```

同时保留现有 helper 扩展参数：

```python
price=None
wait_timeout=None
market=None
remark=None
order_remark=None
idempotency_key=None
```

`wait_timeout=None` 表示使用 `install_jq_compat(default_wait_timeout=16)` 的默认等待窗口；显式传 `wait_timeout=0` 表示异步立即返回。

### style 映射

| 调用方式 | 远程 payload |
| --- | --- |
| `order("000001.XSHE", 100)` | 市价单 |
| `order("688001.XSHG", 100, MarketOrderStyle(10))` | 市价保护单，`protect_price=10` |
| `order("000001.XSHE", 100, LimitOrderStyle(10))` | 限价单，`price=10` |
| `bt.order(..., price=10, market=True)` | 保留老 helper 语义：市价保护价 |
| `bt.order(..., price=10)` | 保留老 helper 语义：限价单 |

第一版只支持股票/ETF 的 `side="long"`、`pindex=0`、`close_today=False`。

这些情况应明确报不支持，不静默忽略：

- `side="short"`
- `pindex != 0`
- `close_today=True`
- `StopMarketOrderStyle`
- `StopLimitOrderStyle`

## 按比例或权重下单

很多策略并不会直接写死金额，而是先按资产比例算目标市值：

```python
weight = 0.2
target_value = context.portfolio.total_value * weight
order_target_value("510300.XSHG", target_value)
```

这种写法可以继续使用。关键是 `context.portfolio.total_value` 已经被远程账户接管，所以按比例计算出来的目标金额来自真实账户，而不是聚宽虚拟盘。

如果策略直接调用聚宽的 `order_percent` / `order_target_percent`，兼容层会用远程 `total_value` 换算下单金额，再走 BulletTrade 远程下单。

## 用户策略可直接使用的内容

安装兼容层后，策略中下面这些聚宽常见函数可以继续直接调用，不需要改成 `bt.xxx`：

```python
order("000001.XSHE", 100)
order_value("000001.XSHE", 10000)
order_percent("000001.XSHE", 0.1)
order_target("000001.XSHE", 0)
order_target_value("000001.XSHE", 10000)
order_target_percent("000001.XSHE", 0.2)
cancel_order(order_id)
get_open_orders()
get_orders()
get_trades()
```

这些函数在回测环境中保持聚宽原始行为；在聚宽模拟盘环境中走 BulletTrade 远程账户和远程下单。

策略里下面这些账户变量也可以继续按聚宽写法读取，但模拟盘环境下数据来自 BulletTrade 真实账户：

```python
context.portfolio.available_cash
context.portfolio.total_value
context.portfolio.positions_value
context.portfolio.positions
context.subportfolios[0].available_cash
context.subportfolios[0].total_value
context.subportfolios[0].positions_value
context.subportfolios[0].long_positions
```

持仓读取示例：

```python
pos = context.portfolio.positions["510300.XSHG"]

amount = pos.total_amount
closeable = pos.closeable_amount
value = pos.value
avg_cost = pos.avg_cost
```

按现金或总资产比例下单也可以继续沿用原写法：

```python
cash = context.portfolio.available_cash
order_value("510300.XSHG", cash * 0.5)

weight = 0.2
target_value = context.portfolio.total_value * weight
order_target_value("159915.XSHE", target_value)

order_target_percent("510300.XSHG", 0.3)
```

如果策略自己封装了按百分比下单函数，例如：

```python
def order_target_percent(security, percent):
    value = context.portfolio.total_value * percent
    return order_target_value(security, value)
```

这种封装不需要额外适配，因为最终读取的是远程 `total_value`，调用的是已接管的 `order_target_value`。

## 返回值

为了尽量少改用户策略，兼容层函数返回值应尽量接近聚宽：

- 成功创建委托：返回一个订单对象
- 创建失败或无需下单：返回 `None`

订单对象至少提供：

- `order_id`
- `security`
- `amount`
- `price`
- `is_buy`
- `status`

现有低层 helper 可以继续返回远程订单号字符串；`install_jq_compat` 包装出来的聚宽兼容函数应返回订单对象，避免破坏原策略里对 `Order` 的判断。

## 启动顺序

推荐用户把 `_install_bt(context)` 放在：

- `process_initialize`

原因：

- `initialize` 更适合放原策略初始化逻辑，例如 `set_benchmark`、`run_daily`
- 聚宽模拟盘进程会重启并恢复状态，网络连接和 helper 内部状态不能依赖持久化
- `process_initialize` 更适合在每次进程启动或重启后重新安装 wrapper 和恢复远程连接

`install_jq_compat` 必须是幂等的。重复调用不应重复包裹函数，也不应重复覆盖已经安装的代理对象。

## 失败策略

实盘接管必须 fail closed：

- 无法连接 `bullet-trade server`：抛错或阻断远程下单
- 远程账户不可读取：不使用聚宽虚拟盘账户兜底
- 远程下单失败：返回 `None` 或抛出可读错误，并写日志
- 聚宽镜像下单失败：仅当 `mirror_jq_orders=True` 时记录 warning，不影响远程真实订单

## 实现清单

1. 在 `helpers/bullet_trade_jq_remote_helper.py` 增加 `install_jq_compat(...)`。
2. 保存聚宽原始交易函数，支持卸载或重复安装检测。
3. 增加聚宽风格 `MarketOrderStyle` / `LimitOrderStyle` 识别和 style 解析。
4. 增加远程 `Portfolio` / `SubPortfolio` / `Position` 代理对象。
5. 在 `sim_trade` 下覆盖 `context.portfolio` 和 `context.subportfolios`。
6. 包装 `order/order_value/order_percent/order_target/order_target_value/order_target_percent/cancel_order/get_orders/get_open_orders/get_trades`。
7. 聚宽兼容包装层默认 `default_wait_timeout=16`，单次调用允许 `wait_timeout=0` 异步覆盖。
8. 增加测试：回测环境不远程、模拟盘接管、style 映射、按比例下单、默认同步等待 16 秒、远程不可用 fail closed、`mirror_jq_orders` best effort。

## 和当前方案的关系

当前文档描述的是下一代推荐接入方式。

两种聚宽侧策略修改方案都保留：

- [策略修改方案 1：显式调用 helper](joinquant-helper-explicit.md)
- [策略修改方案 2：接管聚宽函数](joinquant-live-takeover-usage.md)

现有显式 helper 方式仍可使用：

```python
import bullet_trade_jq_remote_helper as bt

bt.configure(...)
bt.order_target_value("510300.XSHG", 100000)
```

它适合 notebook、研究环境、手工联调和已经改好的老策略。新方案面向“存量聚宽策略尽量不改代码”的模拟盘接管场景。
