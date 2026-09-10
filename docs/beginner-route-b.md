# 把你的聚宽策略接入大 QMT

策略继续在聚宽运行，选股和取数基本不变，账户查询和下单交给你的 QMT。按下面五步操作即可。

**先用 QMT 仿真账号练习。聚宽虽然叫“模拟盘”，接到 QMT 实盘账号后，下单就会使用真实资金。**

## 1. 在 Windows 上准备 BulletTrade

准备一台能登录大 QMT 的 Windows 电脑，并安装 Python。还没安装的，先看[Python 安装步骤](python-setup.md)。

打开 Windows 命令提示符（cmd），执行：

```bat
python -m pip install -U --pre bullet-trade
```

这条命令安装最新发布版本，包含 Beta 版，不需要填写版本号。

## 2. 启动大 QMT 和 BulletTrade 服务

在大 QMT 中：

1. 新建 Python 策略，将[网关文件](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/big_qmt_gateway_strategy_sample.py)的完整内容复制进去。
2. 找到顶部的 `GATEWAY_PASSWORD`，改成自己设置的一段密码并保存。下载文件导入时保持 GBK 编码。
3. 新建“策略交易”运行项，选择该策略和仿真账号。主图代码可填 `000300`，周期选日线，**不要勾选“启动本地 Python”**，然后运行。

<img src="assets/big-qmt-4-new-server.png" alt="在大 QMT 中选择网关策略和资金账号并创建运行项" width="760">

日志出现 `listen success listen=127.0.0.1:9000`，说明网关启动。找不到界面可对照[完整截图](big-qmt-server.md)。

在 Windows 新建一个文件夹，在里面创建 `.env.bigqmt` 文件，用记事本写入以下内容。注意文件名不要变成 `.env.bigqmt.txt`：

```env
QMT_ACCOUNT_ID=你的QMT资金账号
QMT_SERVER_TOKEN=自己设置的一段较长随机令牌
BIG_QMT_GATEWAY_PASSWORD=刚才在大QMT填写的密码
QMT_SERVER_PORT=58620
```

前三项换成自己的值。`token` 是自己设置的连接令牌，不是券商交易密码。

在这个文件夹的地址栏输入 `cmd`，按回车，执行：

```bat
bullet-trade --env-file .env.bigqmt server --server-type big_qmt --listen 0.0.0.0
```

保持这个窗口和大 QMT 运行，不要关闭或让电脑休眠。

## 3. 在聚宽填写连接信息，查一下账户

你需要知道三个值：**服务器地址、入口端口、token**。如果有人帮你部署，向对方索取即可。

- 默认端口是 `58620`。要改服务端口，修改上面的 `QMT_SERVER_PORT` 并重启该服务。
- 如果公网端口 `15862` 映射到 Windows 的 `58620`，聚宽填 `15862`。
- 地址只填域名或 IP，不加 `http://`；不要填 `127.0.0.1`。大 QMT 内部的 `9000` 不给聚宽使用。

服务启动不会自动打通网络。以下按已准备好可达入口操作；跨公网的安全连接由部署时配置，详见[服务向导](big-qmt-server.md)，不要直接暴露未加密的交易服务。

下载[聚宽连接文件](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/bullet_trade_jq_remote_helper.py)，保持文件名 `bullet_trade_jq_remote_helper.py`，上传到**聚宽研究根目录**。不用把这个文件的内容粘进自己的策略。

在聚宽研究中新建 Python Notebook，复制下面代码，填好三个值后运行：

```python
import bullet_trade_jq_remote_helper as bt

bt.configure(
    host="你的服务器地址",
    port=58620,  # 有端口映射时填外部入口端口
    token="你的QMT_SERVER_TOKEN",
)
print("可用资金:", bt.get_account().available_cash)
print("持仓数量:", len(bt.get_positions()))
```

这段代码不会下单。**看到资金和持仓数量，并与 QMT 一致，才继续下一步。** 如果导入失败，检查文件名和上传位置；如果连接失败，检查地址、端口、token 及 Windows 服务。

## 4. 选择一种方式修改原策略

先复制一份原策略，保留原版。下面两种方式选一种，不要一起复制。

### 方案 1：逐个改下单接口

适合下单点少、希望明确控制每个交易点的策略。直接的 `bt.order()` 不会判断回测，所以要用下面的函数区分：回测走聚宽，模拟盘走 QMT。

在原策略的 `from jqdata import *` 等导入后添加：

```python
import bullet_trade_jq_remote_helper as bt


def _use_bt(context):
    """根据聚宽 context 返回是否访问 QMT；未知运行环境抛错。"""
    mode = context.run_params.type
    if mode == "sim_trade":
        return True
    if mode in ("simple_backtest", "full_backtest"):
        return False
    raise RuntimeError("无法识别运行环境: %s" % mode)


def process_initialize(context):
    """输入聚宽 context，仅在模拟盘配置远程连接；无返回值。"""
    if _use_bt(context):
        bt.configure(
            host="你的服务器地址",
            port=58620,
            token="你的QMT_SERVER_TOKEN",
        )


def my_order_target_value(context, code, value):
    """按运行环境提交目标金额委托，返回远程订单号或聚宽订单对象。"""
    if _use_bt(context):
        return bt.order_target_value(code, value)
    return order_target_value(code, value)
```

然后把原来的下单语句：

```python
order_target_value(code, value)
```

改成：

```python
my_order_target_value(context, code, value)
```

其他下单、撤单也要按同样方式修改。策略用资金和持仓判断仓位时，模拟盘分支要读 `bt.get_account()`、`bt.get_positions()`，不能继续拿聚宽虚拟账户的数据决定 QMT 下单金额。具体字段和其他接口见[方案 1 参考](joinquant-helper-explicit.md)。如果这些地方很多，可以考虑方案 2。

### 方案 2：接管聚宽原函数，少改策略

常见股票、ETF 多头策略可以保留 `order()`、`order_target_value()` 和 `context.portfolio` 写法。接管层会自动识别回测和模拟盘。

在原策略的 `from jqdata import *` 等导入后添加：

```python
import bullet_trade_jq_remote_helper as bt


def process_initialize(context):
    """根据聚宽 context 安装接管并输出状态；回测不接管，无返回值。"""
    state = bt.install_jq_compat(
        globals(),
        context=context,
        host="你的服务器地址",
        port=58620,
        token="你的QMT_SERVER_TOKEN",
    )
    log.info("接管状态: %s" % state)
```

填好连接信息后，常见的原下单语句不用加 `bt.`。回测仍走聚宽；模拟盘接管成功时，日志中会有 `'enabled': True`。

**此方案已有离线测试，真实聚宽到 QMT 的完整验证仍待完成，先在仿真账号验证。** 特殊交易类型、跨文件下单等是否支持，见[方案 2 兼容范围](joinquant-live-takeover-usage.md)。另外直接写出的 `bt.order()` 不受接管层的回测判断保护，不要混用。

### 两种方案都注意这一点

如果原策略**已经有 `process_initialize()`**，把所选方案的函数内容合并进去，不要再定义第二个同名函数。原来的 `initialize()`、`run_daily()`、选股逻辑保留，不用删除。

## 5. 在聚宽启动模拟盘，看 QMT 是否收到订单

确认连接的是 **QMT 仿真账号**，启动修改后的聚宽策略，等待它原定的交易时间。不要同时运行多个相同策略副本。

看三个地方：

1. 聚宽日志：策略是否正常运行、产生下单动作。
2. QMT 委托列表：证券、方向、数量是否正确。
3. 如有成交，QMT 成交列表、资金和持仓变化是否一致。

返回订单号不等于成交；超时后先查 QMT 委托，不要直接重复下单。方案 2 默认不往聚宽虚拟账户镜像下单，聚宽页面的持仓和收益曲线不代表 QMT 实际结果。

本文默认使用整个 QMT 账户，已有持仓也会被策略读到。先完成仿真验证，再决定是否连接实盘。
