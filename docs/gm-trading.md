# 掘金本地交易候选版

版本：`0.11.0rc1`，2026-10-08。支持独立 GM 数据源与 Windows 本地 `GmBroker`。远程 GM Server 尚未交付；本版本不是全品种、全交易场景的正式验收。

## 已完成实测

Windows 虚拟仿真验收已完成银华日利 100 股买入和卖出、终态委托、成交费用、现金守恒及重新连接只读核对。实际账户、余额、委托明细和精确成交时间仅保留在私有验收材料，不纳入开源仓库。

原生逐笔展示价格与成交金额可能有浮点精度差异，资金核对保留 `gross_amount`。逐笔 `commission=0` 而委托最终 `filled_commission` 非零时，须在终态和完整成交量对齐后分摊费用。

`tests/fixtures/gm_trading_20261008.json` 为合成回归样本：身份、时间、价格和余额均为虚构，只保留上述协议结构及边界行为，不能当作实机成交证据。

## 安装与配置

在 Windows 上安装本版本代码和匹配终端的 GM SDK。此次实测为 Python 3.12、GM SDK 3.0.186；SDK 不需要装在 Mac 策略开发机上。当前 SDK worker 使用已核验的原生会话入口，其他 SDK/终端版本需先做只读探针验证。

复制 `env.gm.example` 到私密环境文件，至少填写：

```dotenv
DEFAULT_DATA_PROVIDER=gm
DEFAULT_BROKER=gm
GM_TOKEN=填写自己的用户身份密钥
GM_STRATEGY_ID=填写自己的策略身份
GM_ACCOUNT_ID=填写已确认的资金账户ID
GM_SERV_ADDR=127.0.0.1:7001
GM_JOURNAL_PATH=C:/bullettrade-runtime/gm/orders.sqlite
GM_ENABLE_TRADING=false
GM_TRADE_TIMEOUT=20
```

首次启动保持只读。账户、策略身份、终端状态及报价验证后，按自己的交易授权把 `GM_ENABLE_TRADING` 改为 `true`。每个账户固定使用同一个日志路径，保留 SQLite 文件，不要为了重试未知订单而删除日志或换路径。

`GM_PYTHON_EXECUTABLE` 可指定 SDK 解释器，该解释器必须也能导入同版本 BulletTrade。账户凭据仅经子进程 stdin 传入，不放进进程命令行。

## 标准 Broker 使用

```python
import asyncio
from bullet_trade.broker.registry import create_broker
from bullet_trade.utils.env_loader import load_env, get_broker_config

load_env('.env.gm')
broker = create_broker('gm', get_broker_config())
try:
    broker.connect()
    print(broker.get_account_info())
    print(broker.get_positions())
    print(broker.get_orders())
    print(broker.get_trades())
    # 已明确启用交易时，可调用标准 buy/sell；必须提供限价和稳定幂等键。
    # oid = asyncio.run(broker.buy('511880.XSHG', 100, 100.900,
    #     extra={'idempotency_key': '自己的唯一业务请求ID'}))
finally:
    broker.disconnect()
```

上例价格仅演示参数，实际订单须从新鲜的未复权行情决定价格。相同幂等键及相同参数不会重复提交；参数冲突会报错。

`bullet-trade live strategy.py --broker gm` 已通过注册表接入现有执行器。实机成交测试直接调用标准 Broker；完整策略调度、风险控制及所有策略兼容性仍需按策略另行验收。

## 首版边界与恢复

- 当前限制为已实现申报规则的普通主板股票及基金代码段、100 股整数倍限价单。科创板、创业板、其他品种、零股及市价单明确拒绝。代码段校验并不替代交易品种权限；实机成交已验证的品种为银华日利。
- 最新报价默认最多 5 秒，限价偏离最新价格超过 1% 会拒绝；数量、最小报价单位及账户资金/可卖数量均检查。
- 所有 SDK 调用在隔离 worker 的主线程串行执行。当前以查询快照同步委托和成交，没有宣称完成高频推送订阅。
- 写入前持久化请求；写入响应不确定时返回稳定的 `new` 待核对订单，带 `submission_state=submit_unknown`。阻止进一步新订单，并保留身份待人工/券商事实核对；不会自动重发。
- 已取得原生订单身份的委托可跨重连恢复。尚未取得原生身份的未知提交不能自动关联或自动判为失败；本版没有自动恢复写入的管理命令。
- 撤单返回 `True` 必须查询到最终已撤/部撤；撤单请求返回本身不算撤成。本次实机没有额外制造撤单、拒单和部分成交，相关行为以离线测试为依据。
- 成交回报需要稳定 `exec_id`。逐笔费用为零而委托有费用时，只有终态委托与完整成交量匹配才分摊最终费用；部分成交费用未确认时，成交列表显式报错等待对齐，委托/持仓查询仍可用。该限制不适合依赖每笔部分成交实时费用的策略。
- 实盘账户、多进程并发交易、多账户 Server、自动重连恢复写入和不确定订单自动解析均未实机验收。准备稳定版时需补齐这些合同或明确产品范围。

## 测试脚本

仓库提供 `scripts/gm_roundtrip_smoke.py`。它需要显式 `--execute`、已确认账户指纹和独立运行 ID，配置从 stdin 读取；只允许专用空账户的银华日利 100 股买卖。已有委托时拒绝再次测试。脚本不是常驻策略，不自动重新买卖；未成交时只尝试撤销本次原委托。

开源仓库不保存账户实测原始记录；测试使用合成账户样本，私密配置应存放在仓库外。
