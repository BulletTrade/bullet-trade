# 交易支撑：聚宽模拟盘远程实盘

这页只保留最小流程：聚宽策略如何连到远程 `bullet-trade server` 做真实下单。

聚宽侧改策略有两种方式：

- [聚宽接入方案 A：显式调用 helper](joinquant-helper-explicit.md)：下单处写 `bt.order(...)`、`bt.order_target_value(...)`。
- [聚宽接入方案 B：接管聚宽函数](joinquant-live-takeover-usage.md)：在 `process_initialize` 安装兼容层，原来的 `order(...)`、`context.portfolio` 尽量不改。

两种方式的优缺点见 [聚宽策略接入方案对比](joinquant-integration-options.md)。

## 1. 先启动远程 server

下面是 MiniQMT 后端的启动方式。若 Windows 侧使用大 QMT，请先按 [大 QMT 服务向导](big-qmt-server.md) 在大 QMT 里运行 helper，再启动 `bullet-trade server --server-type big_qmt`；聚宽侧 helper 仍然连接 `58620`。

Windows 机器上的 `.env` 最少只要：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

启动命令：

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

如果是单账户，到这里就够了。

## 2. 上传 helper 到聚宽

上传这个文件到聚宽根目录：

- `helpers/bullet_trade_jq_remote_helper.py`

## 3. 在策略里最小配置（显式 helper 调用）

```python
import bullet_trade_jq_remote_helper as bt

def initialize(context):
    set_benchmark('000300.XSHG')

def process_initialize(context):
    bt.configure(
        host="your.server.ip",
        port=58620,
        token="secret",
    )

def handle_data(context, data):
    bt.order('000001.XSHE', 100)
```

单账户默认不用写 `account_key`。  
只有多账户时才写，例如：

```python
bt.configure(
    host="your.server.ip",
    port=58620,
    token="secret",
    account_key="main",
)
```

## 常见问题

### `account_key` 必须写吗

不是。  
单账户场景不用写；只有多账户才需要。

### server 端为什么不能写 `--data-path`

因为当前版本没有这个参数。  
MiniQMT 数据目录要放到 `.env` 里的 `QMT_DATA_PATH`。大 QMT 不使用这个配置入口。

### `:stock` 必须写吗

也不是。  
股票账户默认就是 `stock`，所以多账户示例写成：

```bash
--accounts main=123456
```

就可以。  
只有期货账户才需要写成：

```bash
--accounts hedge=654321:future
```

更多 MiniQMT server 说明见 [QMT server](qmt-server.md)。大 QMT 后端见 [大 QMT 服务向导](big-qmt-server.md)。
