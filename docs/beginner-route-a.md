# 本地运行策略

[返回新手选型](beginner-guide.md)

这条路线把策略、Python 环境和日志都放在自己的 Windows 机器上，大 QMT 只负责行情与交易通道。

```text
my_strategy.py -> bullet-trade live -> 127.0.0.1:58620 -> 大 QMT -> 券商
```

## 适合谁

- 希望自己管理策略文件、日志和运行时间。
- 策略主要使用量价数据和通用聚宽 API。
- 不依赖聚宽平台独有的财务、因子或研究环境。

## 1. 先启动大 QMT 网关

先完成 [大 QMT：两种接入方式](big-qmt-server.md) 中的公共步骤：

1. 在大 QMT 新建策略并粘贴 `helpers/big_qmt_gateway_strategy_sample.py`。
2. 创建“策略交易”运行项，不勾选“启动本地 Python”。
3. 启动网关，确认本机 `127.0.0.1:9000` 正常。

helper 中普通下单和按明确订单号撤单已经默认开启，无需额外配置或再次开启。

## 2. 准备三个配置

将 `env.bigqmt.example` 复制为 `.env.bigqmt`，只填写：

```env
QMT_SERVER_TOKEN=请生成一个新的客户端令牌
QMT_ACCOUNT_ID=你的QMT资金账号
BIG_QMT_GATEWAY_PASSWORD=与helper顶部完全一致
```

端口、主机、账户类型和超时都有默认值，不需要写。

## 3. 启动本地服务

```powershell
bullet-trade --env-file .env.bigqmt server --server-type big_qmt --listen 127.0.0.1
```

保持这个窗口运行。策略不会直接连接大 QMT 的 `9000`，而是连接 BulletTrade 的 `58620`。

## 4. 运行自己的策略

打开另一个命令行窗口：

```powershell
bullet-trade --env-file .env.bigqmt live my_strategy.py --broker qmt-remote
```

`QMT_SERVER_HOST` 和 `QMT_SERVER_PORT` 默认就是 `127.0.0.1:58620`，本机使用时不要重复配置。

如果策略还希望从大 QMT 读取行情，再增加一项：

```env
DEFAULT_DATA_PROVIDER=qmt-remote
```

如果策略使用 JQData 或其他数据源，这一项不需要写。

## 5. 按顺序验收

1. 确认服务 health 和大 QMT 后端可用。
2. 读取资金、持仓、当日委托和成交。
3. 确认策略正常调度。
4. 在仿真账号提交一笔最小限价单。
5. 核对 QMT 委托、撤单、成交和最终持仓。

遇到 `9000`、密码、启动方式等问题，统一查看 [大 QMT 常见问题](big-qmt-server.md#common-questions)。
