# 聚宽运行策略

[返回新手选型](beginner-guide.md)

这条路线让策略继续在聚宽运行。聚宽负责选股和产生交易动作，自己的 Windows 机器运行 BulletTrade 与大 QMT，负责账户查询和下单。

```text
聚宽策略 -> 互联网 58620 -> BulletTrade server -> 本机 9000 -> 大 QMT -> 券商
```

## 适合谁

- 现有聚宽策略已经稳定运行，希望尽量少改代码。
- 策略依赖聚宽数据、财务、因子或平台研究环境。
- 暂时不想把完整策略迁移到本地。

## 1. 先准备 Windows 与大 QMT

在一台长期在线的 Windows 机器上完成 [大 QMT：两种接入方式](big-qmt-server.md) 的公共步骤：

1. 在大 QMT 中运行 `big_qmt_gateway_strategy_sample.py`。
2. 准备只有三个值的 `.env.bigqmt`。
3. 启动对外服务：

```powershell
bullet-trade --env-file .env.bigqmt server --server-type big_qmt --listen 0.0.0.0
```

只对外开放 `58620`，不要开放大 QMT 内部的 `9000`。不要把 `58620` 作为裸 TCP 直接暴露到公网；跨互联网访问应使用 VPN、加密隧道，或正确配置 TLS 与 IP 白名单。

## 2. 上传一个 helper

将下面文件上传到聚宽研究根目录：

```text
helpers/bullet_trade_jq_remote_helper.py
```

[从 GitHub 查看 helper](https://github.com/BulletTrade/bullet-trade/blob/main/helpers/bullet_trade_jq_remote_helper.py)

## 3. 只填写地址和 token

先在聚宽研究环境中做只读验证：

```python
import bullet_trade_jq_remote_helper as bt

bt.configure(
    host="你的公网地址或域名",
    token="与 QMT_SERVER_TOKEN 相同",
)

account = bt.get_account()
positions = bt.get_positions()
print(account.available_cash, len(positions))
```

默认端口是 `58620`，不需要填写。只有服务端改过端口时才增加 `port=新端口`。

## 4. 接入现有聚宽策略

在策略的 `process_initialize` 中配置连接：

```python
import bullet_trade_jq_remote_helper as bt


def process_initialize(context):
    bt.configure(
        host="你的公网地址或域名",
        token="与 QMT_SERVER_TOKEN 相同",
    )
```

接下来有两种改法：

- [显式调用 helper](joinquant-helper-explicit.md)：把下单处改为 `bt.order(...)` 等函数，行为最直观。
- [接管聚宽函数](joinquant-live-takeover-usage.md)：尽量保留原来的 `order(...)` 和 `context.portfolio` 写法。

第一次联调建议先显式调用 helper；存量策略改动很多时，再评估接管方式。

## 5. 按顺序验收

1. 聚宽研究环境能读取真实账户和持仓。
2. Windows 的 BulletTrade 服务日志出现对应请求。
3. 策略能产生预期信号。
4. 在仿真账号中提交最小限价单并按订单号撤单。
5. 在大 QMT 和券商侧核对委托、成交与持仓。

完整截图、两张结构图和常见问题见 [大 QMT 两种接入方式](big-qmt-server.md)。
