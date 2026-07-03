# 聚宽策略修改方案 1：显式调用 helper

这份文档适合需要在聚宽策略或聚宽研究里直接调用 `bt.xxx` 的用户。

策略修改方案 1 的核心是：聚宽继续运行策略，真实下单点显式改成 `bullet_trade_jq_remote_helper` 的函数。

这个方案只表示“怎么改策略代码”，不是单独的网络部署方案。只要策略运行在聚宽侧，聚宽都需要能访问 `bullet-trade server` 的入口地址和端口；如果 `bullet-trade server` 跑在 QMT 那台 Windows 机器上，通常需要公网 IP、域名、端口映射，或 FRP / VPN 等可达通道。

## 1. 上传 helper

把下面文件上传到聚宽研究根目录：

- `bullet_trade_jq_remote_helper.py`

文件来源：

- GitHub：`helpers/bullet_trade_jq_remote_helper.py`

上传后，聚宽策略里可以直接：

```python
import bullet_trade_jq_remote_helper as bt
```

## 2. 策略里配置远程 server

推荐把服务器参数放在策略文件开头，把 `bt.configure(...)` 放在 `process_initialize(context)`。

```python
import bullet_trade_jq_remote_helper as bt


BT_REMOTE_HOST = "your.server.ip"
BT_REMOTE_PORT = 58620
BT_REMOTE_TOKEN = "secret"
BT_ACCOUNT_KEY = "main"
BT_SUB_ACCOUNT_ID = None


def process_initialize(context):
    bt.configure(
        host=BT_REMOTE_HOST,
        port=BT_REMOTE_PORT,
        token=BT_REMOTE_TOKEN,
        account_key=BT_ACCOUNT_KEY,
        sub_account_id=BT_SUB_ACCOUNT_ID,
    )


def initialize(context):
    # 原来的 initialize 逻辑继续写在这里
    set_benchmark("000300.XSHG")
```

`process_initialize(context)` 用来在聚宽模拟盘进程启动或重启后恢复 helper 配置。`initialize(context)` 继续放原策略初始化逻辑，例如 `set_benchmark`、`run_daily`。

## 3. 参数说明

| 参数 | 说明 |
| --- | --- |
| `host` | `bullet-trade server` 地址，可以是公网 IP、内网 IP 或域名。 |
| `port` | `bullet-trade server` 端口，默认建议 `58620`。 |
| `token` | server 端配置的访问 token。 |
| `account_key` | 多账户配置时的账户 key；单账户可以传 `None` 或不传。 |
| `sub_account_id` | BulletTrade 虚拟子账户 ID；不用虚拟账户时传 `None`。 |

## 4. 下单函数怎么改

把聚宽原始下单函数显式改成 `bt.xxx`：

| 原聚宽写法 | 策略修改方案 1 写法 |
| --- | --- |
| `order(...)` | `bt.order(...)` |
| `order_value(...)` | `bt.order_value(...)` |
| `order_percent(...)` | `bt.order_percent(...)` |
| `order_target(...)` | `bt.order_target(...)` |
| `order_target_value(...)` | `bt.order_target_value(...)` |
| `order_target_percent(...)` | `bt.order_target_percent(...)` |
| `cancel_order(order_id)` | `bt.cancel_order(order_id)` |

示例：

```python
# 原来
order_target_value("510300.XSHG", 100000)

# 改成
bt.order_target_value("510300.XSHG", 100000)
```

下单默认是异步立即返回；如果希望同步等待，可以传 `wait_timeout`：

```python
bt.order_target_value("510300.XSHG", 100000, wait_timeout=16)
```

## 5. 账户和持仓读取

方案 A 不接管 `context.portfolio`。如果策略里的仓位判断要按真实账户计算，应显式读取远程账户：

```python
acct = bt.get_account()
positions = bt.get_positions()

print("远程可用资金:", acct.available_cash)
print("远程总资产:", acct.total_value)
print("远程持仓数量:", len(positions))
```

如果原策略继续使用下面这些字段，它们仍然来自聚宽虚拟盘：

```python
context.portfolio.available_cash
context.portfolio.total_value
context.portfolio.positions
```

所以当策略依赖现金、持仓、市值比例来决定真实下单金额时，要么把这些判断也改成 `bt.get_account()` / `bt.get_positions()`，要么改用 [策略修改方案 2：接管聚宽函数](joinquant-live-takeover-usage.md)。

## 6. 聚宽研究里测试连接

在聚宽研究环境里可以先运行下面代码，只测试连接、账户读取和持仓读取；默认不下单。

```python
import bullet_trade_jq_remote_helper as bt


bt.configure(
    host="your.server.ip",
    port=58620,
    token="secret",
    account_key="main",
    sub_account_id=None,
)

acct = bt.get_account()
positions = bt.get_positions()

print("远程可用资金:", acct.available_cash)
print("远程总资产:", acct.total_value)
print("远程持仓数量:", len(positions))

for pos in positions:
    print(pos.security, pos.amount, pos.available, pos.market_value, pos.avg_cost)

# 谨慎：取消注释后会真实远程下单
# bt.order("000001.XSHE", 100, wait_timeout=0)
```

## 7. 优缺点

优点：

- 真实下单点很明确。
- 不覆盖聚宽原始函数，适合研究、调试和小范围改造。
- 对已经按 `bt.xxx` 改好的老策略完全兼容。

缺点：

- 需要逐个修改下单函数。
- 账户和持仓判断也要人工确认口径，否则可能出现“聚宽虚拟盘资金判断、远程真实账户下单”的错配。
- 存量策略下单点很多时，改造成本高于方案 B。
