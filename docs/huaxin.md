# 华鑫 TORA 接入

!!! warning "当前状态：代码已接通，真实环境仍须逐环境验收"
    当前版本已经接通 TORA Trader、独立 Python 3.7 XMD Level 1 sidecar、
    `bullet-trade server --server-type huaxin` 与上层 `qmt-remote` 客户端。
    本地自动化测试通过不等于特定仿真或生产柜台已经验收；每个部署仍需使用对应
    Git commit、私密配置、SDK 制品和运行日志完成只读与受控写验证。

    当前不包含华鑫 Level 2，也不在 BulletTrade server 内维护虚拟子账户账本。
    多虚拟账户由 AIStocks V2 等上层网关负责，华鑫 server 只提供父账户行情和交易事实。

## 1. 架构与职责

华鑫适配属于同一个开源 `bullet-trade` 包，不存在另一个运行框架：

```text
bullet-trade live 策略（qmt-remote）
              │
              ▼
bullet-trade server --server-type huaxin
        ├── HuaxinBrokerAdapter ── TORA Trader native bridge
        └── HuaxinDataAdapter   ── Python 3.7 XMD L1 sidecar
```

- Trader 负责账户、资金、持仓、委托、成交、限价单、已支持的市价类型和撤单。
- XMD 只负责新鲜 L1 快照；历史 K 线、复权、财务和因子仍由显式选择的其他 Provider 提供。
- server 不复制券商账本，也不在华鑫路径创建 SQLite 数据库。
- AIStocks V2 若提供两个虚拟账号，它才是虚拟现金、冻结、持仓、订单和成交的权威 owner。

## 2. SDK 与私密资产边界

通用 wheel/sdist 只包含 BulletTrade 自研代码、C ABI、bridge 源码和测试桩，不包含华鑫厂商 SDK。
厂商 SDK 必须由有权用户从外部目录显式提供。

以下内容不得提交到公开 Git、打入 PyPI 包或写入公开日志：

- 厂商头文件、动态库、PDF 和示例包；
- 账号、密码、动态口令、TerminalInfo、MAC、IP 和硬盘序列号；
- 柜台、行情、代理地址和端口；
- 生产服务器、VPN、SDK 与部署目录；
- 未脱敏的行情、订单、成交、资金和持仓记录。

代码不内置生产或仿真前置地址。Trader 的 `HUAXIN_TRADE_FRONT` 和 XMD 的
`HUAXIN_XMD_FRONT` 都必须来自部署机器上已被 Git 忽略的私密 env 文件。

## 3. 构建与诊断

离线 fake bridge 可用于安装、ABI 和队列合同回归：

```bash
bullet-trade huaxin build --prefix <BUILD_PREFIX> --offline-fake --build-type Release
bullet-trade huaxin doctor --bundle <BUNDLE_DIR>
```

真实 Trader bridge 必须显式选择 trader 模式并提供外部 SDK：

```bash
bullet-trade huaxin build \
  --prefix <BUILD_PREFIX> \
  --trader \
  --sdk-dir <PRIVATE_TORA_TRADER_SDK_DIR> \
  --build-type Release
```

构建成功只证明制品生成；只有 native 加载、登录和四项基线查询均成功，Trader 查询状态才是
`ready`。XMD sidecar 的加载、登录和新鲜快照是另一套独立门禁，二者不能互相替代。

## 4. Server 最小配置

以下只展示变量名，不提供真实值。完整值必须放入私密 env：

```dotenv
QMT_SERVER_TYPE=huaxin
QMT_SERVER_LISTEN=<LISTEN_ADDRESS>
QMT_SERVER_PORT=<SERVER_PORT>
QMT_SERVER_TOKEN=<FIXED_PRIVATE_TOKEN>
QMT_SERVER_ENABLE_BROKER=true
QMT_SERVER_ENABLE_DATA=true
QMT_SERVER_ACCOUNTS=default=<PRIVATE_ACCOUNT_ID>:stock

HUAXIN_NATIVE_BUNDLE=<PRIVATE_TRADER_BUNDLE_PATH>
HUAXIN_TRADE_FRONT=<PRIVATE_TRADE_TCP_FRONT>
HUAXIN_LOGIN_ACCOUNT=<PRIVATE_LOGIN_ACCOUNT>
HUAXIN_PASSWORD=<PRIVATE_PASSWORD>
HUAXIN_MAC_ADDRESS=<PRIVATE_MAC>
HUAXIN_USER_PRODUCT_INFO=<PRIVATE_PRODUCT_INFO>
HUAXIN_TERMINAL_INFO=<PRIVATE_TERMINAL_INFO>
HUAXIN_ENABLE_TRADING=false
HUAXIN_ENABLE_CANCEL=false

HUAXIN_XMD_BACKEND=python37_sidecar
HUAXIN_XMD_PYTHON=<PRIVATE_PYTHON37_EXECUTABLE>
HUAXIN_XMD_SDK_DIR=<PRIVATE_XMD_SDK_DIR>
HUAXIN_XMD_FRONT=<PRIVATE_XMD_TCP_FRONT>
HUAXIN_XMD_MAX_AGE_SECONDS=30
HUAXIN_XMD_SIMULATION_REPLAY=false
```

生产环境必须保持 ``HUAXIN_XMD_SIMULATION_REPLAY=false``，继续按交易所时间严格校验。
只有厂商 7×24 仿真环境使用人工交易日回放行情时才设置为 ``true``；此时快照保留原始
``TradingDay``/``UpdateTime``，但以本机接收时间执行 30 秒新鲜度门禁。

启动命令：

```bash
bullet-trade --env-file <PRIVATE_ENV_FILE> server --server-type huaxin
```

非回环监听还必须同时配置 TLS 证书、TLS 私钥、固定 token 和来源 allowlist；缺少任一项都会
fail closed。公开示例不应提供可直接连接的地址。

## 5. 客户端与策略

策略继续使用统一的远程协议：

```dotenv
DEFAULT_DATA_PROVIDER=qmt-remote
DEFAULT_BROKER=qmt-remote
QMT_SERVER_HOST=<PRIVATE_SERVER_HOST>
QMT_SERVER_PORT=<SERVER_PORT>
QMT_SERVER_TOKEN=<FIXED_PRIVATE_TOKEN>
```

```bash
bullet-trade --env-file <PRIVATE_CLIENT_ENV> live <STRATEGY_FILE> --broker qmt-remote
```

策略可沿用原有 `get_current_data()`、订单查询、限价/市价下单与撤单接口。server 只对协议做
适配，不另建一套华鑫专用策略框架。

## 6. 新鲜度、幂等与恢复语义

- XMD 快照默认最大年龄是 30 秒，配置只能收紧，不能超过 30 秒；过期快照直接报错，不用历史
  数据或其他行情源静默补齐。
- 每次远程下单和撤单都必须携带有效 `idempotency_key`。Server 使用进程内结果缓存与
  柜台订单事实，不创建或要求任何幂等数据库、文件路径或第二启用开关。
- Trader 登录后以柜台 `MaxOrderRef` 为基线，运行中只在内存中单调递增；server 重启不会复制
  一份柜台订单账本。
- Trader 或 XMD 断线时，现有 adapter watchdog 在同一 server 进程内重建会话。重连期间查询和
  写入快速失败；XMD 重新订阅原标的并等待新鲜快照后才恢复 `ready`。
- watchdog 只做连接、登录、查询、订阅和新鲜度检查，绝不自动重放下单或撤单。提交结果未知时
  必须查询柜台事实后由上层决定，不能盲目补单。
- 夜间启动时，Trader 柜台或 XMD 尚未开放可以进入等待/降级状态；是否达到跨夜生产验收，仍须
  用实际部署版本完成夜间启动到次日恢复的运行证据。

## 7. 已做与未做

| 能力 | 当前结论 |
| --- | --- |
| Trader 账户/资金/持仓/委托/成交查询 | 已编码并有自动化测试；每个真实环境仍需验收 |
| 限价单、市价单、撤单 | 已编码并有 fake/native 合同测试；真实写入必须单独授权 |
| XMD Level 1 与 `get_current_data()` 兼容字段 | 已编码；真实行情需用新鲜快照验证 |
| Server + qmt-remote 策略 | 已编码；仿真和生产分别使用各自私密 env |
| 运行中 Trader/XMD 自动重连 | 已编码；仍需跨夜/跨窗口环境测试 |
| 华鑫路径 SQLite 幂等账本 | 不做；保留幂等键和柜台事实边界 |
| AIStocks V2 虚拟子账户 | 不属于本 adapter，由 AIStocks V2 负责 |
| 华鑫全市场 Level 2 | 本轮不做，后续独立需求和验收 |

## 8. 安全门禁

默认原则是 fail closed：环境身份不明时按生产处理；未获明确生产写授权时只允许登录、健康检查、
查询和行情观察。仿真或生产写测试都必须同时限定账号、标的、数量、时间窗和写开关。进程存活、
登录成功、收到一条行情或一次成交，都不能外推为全部接口、跨夜恢复或生产长期运行已经完成。
