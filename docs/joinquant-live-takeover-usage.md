# 聚宽策略修改方案 2：接管聚宽函数

这份文档给已经在聚宽模拟盘运行策略、希望尽量少改策略代码的用户使用。

目标是：**回测不使用 BulletTrade；聚宽模拟盘运行时，账户资金、持仓读取和下单函数由 BulletTrade 接管**。策略主体逻辑尽量不用改。

这个方案只表示“怎么改策略代码”，不是单独的网络部署方案。只要策略运行在聚宽侧，聚宽都需要能访问 `bullet-trade server` 的入口地址和端口；如果 `bullet-trade server` 跑在 QMT 那台 Windows 机器上，通常需要公网 IP、域名、端口映射，或 FRP / VPN 等可达通道。

如果你希望显式写 `bt.order_target_value(...)`，看 [策略修改方案 1：显式调用 helper](joinquant-helper-explicit.md)。两种策略修改方案的取舍见 [聚宽策略修改方案对比](joinquant-integration-options.md)。

## 1. 上传 helper

把下面文件上传到聚宽研究根目录：

- `bullet_trade_jq_remote_helper.py`

文件来源：

- GitHub：`helpers/bullet_trade_jq_remote_helper.py`

上传后，聚宽策略里可以直接：

```python
import bullet_trade_jq_remote_helper as bt
```

## 2. 策略顶部增加接入代码

推荐把下面代码放到策略文件顶部，并把服务器参数改成自己的。

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
    # 原来的 initialize 逻辑继续写在下面
    set_benchmark("000300.XSHG")


def process_initialize(context):
    _install_bt(context)
```

如果你的策略已经有 `process_initialize(context)`，不要新建第二个同名函数；把 `_install_bt(context)` 加到已有函数最前面即可。

`initialize(context)` 继续放原策略初始化逻辑，例如 `set_benchmark`、`run_daily` 等。`process_initialize(context)` 用来在聚宽模拟盘进程启动或重启后安装接管层；`install_jq_compat` 是幂等的，重复执行不会重复包裹函数。

## 3. 参数说明

| 参数 | 说明 |
| --- | --- |
| `globals()` | 当前策略文件的全局命名空间。helper 会替换其中的 `order`、`order_value`、`order_percent`、`order_target`、`order_target_value`、`order_target_percent` 等函数。 |
| `context` | 聚宽传入的策略上下文。helper 用它判断当前是回测还是模拟盘，并在模拟盘接管 `context.portfolio`。 |
| `host` | `bullet-trade server` 地址，可以是公网 IP、内网 IP 或域名。 |
| `port` | `bullet-trade server` 端口，默认建议 `58620`。 |
| `token` | server 端配置的访问 token。 |
| `account_key` | 多账户配置时的账户 key；单账户可以传 `None` 或不传。 |
| `sub_account_id` | BulletTrade 虚拟子账户 ID；不用虚拟账户时传 `None`。 |
| `mirror_jq_orders` | 默认 `False`。是否在远程下单成功后，额外调用聚宽原始下单函数做页面展示。真实交易建议保持 `False`。 |
| `default_wait_timeout` | 默认 `16` 秒。下单默认同步等待时间；单次下单传 `wait_timeout=0` 可异步立即返回。 |

## 4. 完整策略示例

下面是一个最小但完整的聚宽模拟盘策略示例。  
重点看两点：

- 只在 `process_initialize(context)` 里安装一次接管层。
- 策略里继续写聚宽原来的 `context.portfolio`、`order_target_value(...)`。

```python
from jqdata import *
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
    set_benchmark("000300.XSHG")
    run_daily(rebalance, time="09:35")


def process_initialize(context):
    _install_bt(context)


def rebalance(context):
    target = "510300.XSHG"

    cash = context.portfolio.available_cash
    total_value = context.portfolio.total_value
    position = context.portfolio.positions[target]

    log.info(
        "remote account cash=%.2f total=%.2f %s amount=%s closeable=%s",
        cash,
        total_value,
        target,
        position.total_amount,
        position.closeable_amount,
    )

    # 模拟盘接管后，这里会走 BulletTrade 远程账户和远程 QMT 下单。
    # 第一次联调建议先保持注释，只查账户和持仓；确认通了以后再打开。
    # order_target_value(target, total_value * 0.2)
```

如果原策略已经有 `process_initialize(context)`，不要复制第二个同名函数，改成这样：

```python
def process_initialize(context):
    _install_bt(context)
    # 原来 process_initialize 里的逻辑继续放在这里
```

## 5. 会不会执行真实下单

helper 会根据聚宽运行环境自动判断：

| 聚宽运行方式 | 行为 |
| --- | --- |
| 回测 `simple_backtest` / `full_backtest` | 不接管，不连接 BulletTrade，不远程下单。 |
| 模拟盘 `sim_trade` | 接管账户状态和下单函数，真实下单发到 `bullet-trade server`。 |

默认 `mirror_jq_orders=False`，所以模拟盘里不会再调用聚宽原始下单函数。聚宽页面主要用于运行策略、取平台数据和看日志；真实资金、真实持仓、真实订单以 BulletTrade / QMT 为准。

## 6. 原策略哪些代码不用改

安装后，策略里这些函数可以继续直接写：

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

回测时它们仍走聚宽原始函数；模拟盘时它们走 BulletTrade 远程账户和远程下单。

聚宽常见下单样式也可以继续用：

```python
order("688001.XSHG", 100, MarketOrderStyle(10))
order("000001.XSHE", 100, LimitOrderStyle(10.0))
order_target_value("510300.XSHG", 100000)
```

第一版只支持股票/ETF 的多头交易：

- 支持 `side="long"`、`pindex=0`、`close_today=False`
- 暂不支持 `side="short"`、`pindex!=0`、`close_today=True`
- 暂不支持停止单 `StopMarketOrderStyle` / `StopLimitOrderStyle`

## 7. 账户和持仓读取

模拟盘接管后，下面这些聚宽写法会读取 BulletTrade 远程真实账户：

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

不存在的持仓会返回空持仓对象，常见判断可以继续写：

```python
if context.portfolio.positions["510300.XSHG"].closeable_amount == 0:
    order_target_value("510300.XSHG", 100000)
```

按现金或资产比例下单也可以继续用：

```python
cash = context.portfolio.available_cash
order_value("510300.XSHG", cash * 0.5)

target_value = context.portfolio.total_value * 0.2
order_target_value("159915.XSHE", target_value)

order_target_percent("510300.XSHG", 0.3)
```

## 8. 重要提示

- 真实账户是唯一权威账本。开启接管后，策略中的现金、持仓和下单都以 BulletTrade 远程账户为准。
- 默认不会在聚宽虚拟盘里下单，所以聚宽页面的持仓和收益曲线不代表真实账户。
- 如果远程 server 连不上，helper 不会回退到聚宽虚拟盘资金做真实下单；应先修复连接。
- 下单默认同步等待 16 秒。如果不想等待，可以单次传 `wait_timeout=0`。
- 第一次使用请先小金额测试，确认 server、QMT、账户和 token 都配置正确。

## 9. 聚宽研究里测试连接

在聚宽研究环境里可以先运行下面代码，只测试连接、账户读取和兼容层安装；默认不下单。

```python
import bullet_trade_jq_remote_helper as bt


BT_REMOTE_HOST = "your.server.ip"
BT_REMOTE_PORT = 58620
BT_REMOTE_TOKEN = "secret"
BT_ACCOUNT_KEY = "main"
BT_SUB_ACCOUNT_ID = None


class _RunParams:
    type = "sim_trade"


class _Context:
    run_params = _RunParams()


context = _Context()

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

print("远程可用资金:", context.portfolio.available_cash)
print("远程总资产:", context.portfolio.total_value)
print("远程持仓数量:", len(context.portfolio.positions))

for code, pos in context.portfolio.positions.items():
    print(code, pos.total_amount, pos.closeable_amount, pos.value, pos.avg_cost)

# 谨慎：取消注释后会真实远程下单
# order("000001.XSHE", 100, wait_timeout=0)
```
