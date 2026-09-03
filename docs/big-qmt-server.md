# 大 QMT：两种接入方式

这是当前推荐的 QMT 接入方式。只需要先在大 QMT 中运行一个网关策略，再根据策略运行位置选择下面两条路线之一：

- **聚宽运行策略**：聚宽策略通过互联网连接家里或办公室的 BulletTrade 服务。
- **本地运行策略**：策略通过 `bullet-trade live` 在本地运行，并连接同一台机器上的 BulletTrade 服务。

网页版入口：[https://bullettrade.cn/docs/big-qmt-server.html](https://bullettrade.cn/docs/big-qmt-server.html)

!!! warning "先用仿真账号"
    配置完成只说明链路可用，不代表已经成交。第一次接入请使用仿真账号，依次核对行情、资金、持仓、委托、成交与最终持仓。

## 先看懂两个端口

大 QMT 内部网关和对外服务不是同一个端口：

| 端口 | 用途 | 是否对公网开放 |
|---|---|---|
| `9000` | 大 QMT 策略内部接口 | **不开放**，只监听 `127.0.0.1` |
| `58620` | BulletTrade 的 `qmt-remote` 服务 | 聚宽路线需要；本地路线不需要 |

无论选择哪条路线，策略都连接 `58620`，不要直接连接 `9000`。

## 第一步：在大 QMT 中启动网关策略

### 1. 登录大 QMT

登录需要提供交易能力的账号，并保持大 QMT 运行。

<img src="assets/big-qmt-1-login.png" alt="登录大 QMT" width="640">

### 2. 新建 Python 策略

在大 QMT 左侧策略区新建 Python 策略，名称可填写 `BT_BIG_QMT_GATEWAY`。

<img src="assets/big-qmt-2-new-strategy.png" alt="新建 Python 策略" width="640">

将下面这个文件的完整内容复制到大 QMT 策略编辑器：

```text
helpers/big_qmt_gateway_strategy_sample.py
```

源文件：[GitHub 查看 helper](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/big_qmt_gateway_strategy_sample.py)

### 3. 只修改一个参数

第一次配置只需要修改内部密码：

```python
GATEWAY_PASSWORD = "请换成自己的内部密码"
```

- `GATEWAY_PASSWORD` 要与下一步 `.env.bigqmt` 中的 `BIG_QMT_GATEWAY_PASSWORD` 完全一致。
- helper 中普通下单和按明确订单号撤单已经默认开启，不需要用户配置或执行“开交易”。
- `LISTEN_HOST=127.0.0.1`、`LISTEN_PORT=9000` 和 `ACCOUNT_TYPE=stock` 都有合适的默认值，不需要重复修改。
- 账号由 BulletTrade 服务统一传给网关。只有直接调试 `9000` 时，才需要填写 helper 顶部的 `ACCOUNT_ID`。

<img src="assets/big-qmt-3-modify-strategy-and-save.png" alt="修改网关策略参数并保存" width="900">

!!! note "保持 GBK 编码"
    该文件运行在大 QMT 内置 Python 环境，复制和保存时不要将其自动转换成 UTF-8。

### 4. 创建并运行“策略交易”

在大 QMT 的模型交易区新建一个“策略交易”运行项：

1. 选择刚才保存的 `BT_BIG_QMT_GATEWAY` 策略。
2. 选择要提供服务的资金账号。
3. 主图代码可选 `000300`，周期选日线即可。
4. 建议勾选“终端启动后自动运行”。
5. **不要勾选“启动本地 Python”**。

<img src="assets/big-qmt-4-new-server.png" alt="新建大 QMT 网关运行项" width="760">

从“策略交易”运行项启动它，确保大 QMT 调用了 `init(ContextInfo)`。

<img src="assets/big-qmt-5-run-server.png" alt="启动大 QMT 网关策略" width="900">

看到下面几类日志，说明 `9000` 网关已经启动：

```text
[BT_BIG_QMT] init starting account=...
[BT_BIG_QMT] listen success listen=127.0.0.1:9000
[BT_BIG_QMT] entering tornado ioloop; gateway should keep running
```

## 第二步：启动 BulletTrade 服务

在运行大 QMT 的同一台 Windows 机器上安装 BulletTrade：

```powershell
pip install bullet-trade
```

将仓库中的 `env.bigqmt.example` 复制为 `.env.bigqmt`，只填写三个值：

```env
QMT_SERVER_TOKEN=请生成一个新的客户端令牌
QMT_ACCOUNT_ID=你的QMT资金账号
BIG_QMT_GATEWAY_PASSWORD=与helper顶部完全一致
```

这里有两层凭证，作用不同：

- `BIG_QMT_GATEWAY_PASSWORD`：只保护本机 `9000` 内部接口。
- `QMT_SERVER_TOKEN`：客户端连接 `58620` 时使用；即使只在本机使用也不能留空。

本地路线只监听本机：

```powershell
bullet-trade --env-file .env.bigqmt server --server-type big_qmt --listen 127.0.0.1
```

聚宽路线需要允许远程连接：

```powershell
bullet-trade --env-file .env.bigqmt server --server-type big_qmt --listen 0.0.0.0
```

端口、网关地址、超时和数据/交易模块都有默认值，不需要写进首次配置。聚宽路线只开放 `58620`，不要开放 `9000`。

!!! danger "不要把 58620 作为裸 TCP 直接暴露到公网"
    跨互联网访问时应使用 VPN、加密隧道，或正确配置 TLS 与 IP 白名单。`QMT_SERVER_TOKEN` 只负责身份校验，不能代替传输加密。

### 从 0.10.0 Beta 1 升级

- 升级运行 BulletTrade server 的 Python 包或源码并重启 server。
- 聚宽运行策略需要重新上传本版 `helpers/bullet_trade_jq_remote_helper.py`，以获得 `data.history` 的 180 秒默认等待窗口。
- 大 QMT 中运行的 `helpers/big_qmt_gateway_strategy_sample.py` 本批没有变化，不需要仅为本次升级重新复制或重启网关策略。
- 本地运行策略直接使用升级后的 `qmt-remote` 客户端，不需要额外增加超时配置。

<a id="joinquant-route"></a>

## 路线一：策略在聚宽运行

```mermaid
flowchart LR
    A[聚宽策略] --> B[聚宽 helper]
    B -->|互联网 58620| C[BulletTrade server]
    C -->|本机 9000| D[大 QMT 网关策略]
    D --> E[QMT 账号与交易通道]
```

适合希望继续在聚宽编辑、定时和运行策略，只把账户查询与下单交给自己的大 QMT 的用户。

### 1. 上传聚宽 helper

将下面文件上传到聚宽研究根目录：

```text
helpers/bullet_trade_jq_remote_helper.py
```

源文件：[GitHub 查看聚宽 helper](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/bullet_trade_jq_remote_helper.py)

### 2. 在策略里填写两个值

```python
import bullet_trade_jq_remote_helper as bt


def process_initialize(context):
    bt.configure(
        host="你的公网地址或域名",
        token="与 QMT_SERVER_TOKEN 相同",
    )
```

`58620` 是默认端口，不需要填写。只有服务端改过端口时，才增加 `port=新端口`。

查询账户和下单仍然使用 helper：

```python
account = bt.get_account()
positions = bt.get_positions()

# 第一次调用请使用仿真账号
order_id = bt.order("000001.XSHE", 100, price=10.00)
```

完整示例：[jq_remote_strategy_example.py](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/jq_remote_strategy_example.py)

<a id="local-route"></a>

## 路线二：策略在本地运行

```mermaid
flowchart LR
    A[本地策略文件] -->|bullet-trade live| B[qmt-remote 客户端]
    B -->|本机 58620| C[BulletTrade server]
    C -->|本机 9000| D[大 QMT 网关策略]
    D --> E[QMT 账号与交易通道]
```

适合希望策略、日志和运行环境都由自己管理的用户。

先保持上一节的本地服务正在运行，然后执行：

```powershell
bullet-trade --env-file .env.bigqmt live my_strategy.py --broker qmt-remote
```

因为策略、BulletTrade 服务和大 QMT 都在同一台机器，客户端地址默认就是 `127.0.0.1:58620`，无需再配置 host 和 port。

如果策略还希望通过大 QMT 读取行情，可在 `.env.bigqmt` 额外增加：

```env
DEFAULT_DATA_PROVIDER=qmt-remote
```

这不是下单所必需的配置；继续使用 JQData 或其他数据源时不要添加。

## 最小验收顺序

不要把“端口能连接”当成已经可以实盘。按下面顺序验证：

1. `admin.health` 显示 `backend_type=big_qmt` 且后端可用。
2. 能读取当前行情、资金和持仓。
3. 能读取当日委托与成交。
4. 在仿真账号提交一笔最小限价单。
5. 根据返回的明确订单号测试撤单，再核对委托状态和成交结果。

当前大 QMT 适配仍按接口矩阵逐项补齐。某项能力在 `admin.health` 中显示 unavailable 时，应停止该项调用，不要假设它与 MiniQMT 完全等价。

<a id="common-questions"></a>

## 常见问题

### 日志只有 `module loaded`，没有 `init starting`

说明启动方式没有调用大 QMT 策略生命周期。请从“策略交易”运行项启动，并确认没有勾选“启动本地 Python”。

### `9000` 能访问，但策略连不上

策略不应连接 `9000`。请检查 BulletTrade 服务是否已启动，并连接 `58620`。

### 密码为什么有两个

`GATEWAY_PASSWORD` / `BIG_QMT_GATEWAY_PASSWORD` 是本机内部密码；`QMT_SERVER_TOKEN` 是客户端令牌。前一组必须互相一致，不能拿客户端令牌替代。

### 什么时候需要改更多配置

只有改端口、跨机器部署、启用 TLS/IP 白名单、配置多个账号或排查超时时，才需要查看 [配置总览](config.md) 和 [QMT 服务配置](qmt-server.md)。第一次接入不要复制整份 `.env`。

### MiniQMT 和华鑫在哪里

MiniQMT 仍作为兼容方案保留在高级接入文档中；华鑫 TORA 是面向自备 SDK 的专业接入，不作为新手主线。
