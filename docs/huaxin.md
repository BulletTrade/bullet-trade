# 华鑫 TORA 接入（第一阶段）

!!! warning "当前状态：仅离线基础切片"
    当前版本提供 BulletTrade 自研 flat C ABI 的源码、`offline_fake` bridge、显式构建/离线诊断，并已把默认拒绝写入的 HuaxinBroker 骨架接入券商注册表和 LiveEngine preflight。真实 TORA Trader、柜台查询/交易、L1/L2 Feed、完整 capability 配置和远程 server 尚未接通。

    `pip install bullet-trade` 成功、Python `import` 成功、fake bridge 构建成功，均不表示华鑫 native、行情或交易已经 ready。当前 `doctor` 即使验证 fake bundle 成功，也必须保持 `native_ready=false`，只允许报告 `offline_bridge_ready=true`。

## 安装与资产边界

华鑫适配属于同一个开源 `bullet-trade` Git 仓库和 PyPI distribution，不需要也不应寻找单独的 `bullet-trade-huaxin` 包或额外仓库：

```bash
pip install bullet-trade
```

通用 wheel/sdist 只包含 BulletTrade 自研 Python 代码、C ABI 声明、bridge 源码和测试桩，不包含华鑫厂商 SDK。正式接入遵循 BYO SDK（Bring Your Own SDK）：用户应通过有权渠道自行取得兼容的华鑫 TORA C++ SDK，并把它保存在开源仓库和 Python 安装目录之外。

以下资产不得提交到公开 Git、打入 PyPI 包或写进公开 Issue/日志：

- 厂商头文件、动态库、PDF 和示例包；
- 账号、密码、动态口令、TerminalInfo；
- 柜台、行情或代理地址及端口；
- 生产服务器、VPN、文件系统和 SDK 的真实路径；
- 未脱敏的行情、订单、成交、资金和持仓记录。

正式 SDK 构建能力尚未在当前第一阶段开放。未来版本会继续采用“显式 build → 显式 doctor”的流程，并要求操作员提供外部 SDK 目录；不会在 pip 安装、普通 import、策略启动或重连时下载 SDK、隐式编译或扫描当前工作目录。

## 当前可用的离线 build / doctor

当前显式入口是：

```bash
bullet-trade huaxin doctor
bullet-trade huaxin build \
  --prefix <BUILD_PREFIX> \
  --offline-fake \
  --build-type Release
bullet-trade huaxin doctor \
  --bundle <BUNDLE_DIR>
```

请把 `<BUILD_PREFIX>` 替换为站点包和源码仓之外的专用可写目录。`build` 返回 JSON，其中的 `bundle_path` 才是后续 `<BUNDLE_DIR>`。bundle 采用内容指纹目录；源码、BulletTrade 版本、manifest 或制品 hash 不一致时必须拒绝复用。

`build` 成功 JSON 包含本机构建路径，不要原样粘贴到公开 Issue 或日志。SHA-256 只能证明 bundle 内部自洽，不能证明二进制来源；`doctor --load` 只应用于可信账号在本机构建、权限受控且未经第三方替换的 bundle。真实发布前还需增加签名/证明、所有者和目录权限验证。

如果需要验证动态库可以由当前进程加载，可显式增加 `--load`：

```bash
bullet-trade huaxin doctor \
  --bundle <BUNDLE_DIR> \
  --load
```

`--load` 会在完整性校验后 `dlopen` BulletTrade 自研的离线 fake bridge，但不会创建 runtime、连接网络、调用 TORA `Create/Init` 或触发交易。没有 `--bundle` 时，`doctor` 返回非零退出码和 `BRIDGE_BUNDLE_MISSING` 是当前阶段的预期行为。

当前命令还有这些明确边界：

- `build` 必须显式给出 `--offline-fake`；真实 SDK 模式会受控拒绝。
- 可选构建类型为 `Release`、`RelWithDebInfo`、`Debug`。
- build 只在显式 prefix 下调用本地 CMake/C++ 编译器，不修改 wheel、`site-packages` 或厂商 SDK 目录。
- `doctor` 输出脱敏 JSON；不得把真实配置补进诊断结果。

## 三种产品角色

华鑫不是一个笼统的“信号生成器开关”。它的执行事实和实时微观结构数据很有价值，但不能独自覆盖完整量化研究所需的历史与静态数据。

| 角色 | 华鑫最终承担的能力 | 仍需的其他能力 | 当前第一阶段状态 |
| --- | --- | --- | --- |
| 信号执行器 `execution_only` | 账户、资金、持仓、订单、成交、限价/已验收市价意图、撤单和异步回报 | 明确股数的限价单可不依赖历史数据；金额下单、市价保护价或价格偏离检查需要新鲜实时价格 | Broker 与柜台尚未接通，不能用于真实下单或撤单 |
| L1/L2 实时信号输入 `realtime_microstructure_signal` | L1/L2 快照、逐笔成交、逐笔委托、委托队列、市场/证券状态和 IOPV | 可靠交易日；全市场策略还需完整证券主数据；历史窗口需额外 bars 或历史 tick | 行情 Feed 与 TORA callback 尚未接通，当前 fake 不产生真实行情 |
| 组合完整信号系统 `research_live` | 华鑫提供实时事实和执行闭环 | 历史 K 线、历史 tick、复权/公司行为、财务、因子、行业/概念、指数成分/权重、证券主数据等显式 Provider | capability router 与组合验收仍在开发，不能因 bridge 可加载就宣称完整可用 |

固定股数限价执行不应被无关的财务、历史 K 线或 L2 缺失阻塞；反过来，使用 L2、趋势窗口、基本面或金额下单的策略，必须在 callback 或写请求前确认精确依赖已经 ready。

## 数据能力必须显式路由

华鑫实时能力与历史/静态 Provider 必须分开。华鑫不提供或不应冒充以下能力：

- 历史日线、分钟 K 线和跨日窗口；
- 历史 tick；
- 前复权、后复权、分红送股和其他公司行为；
- 财务报表、估值历史和因子库；
- 交易日、证券主数据、指数成分/权重、行业和概念等 reference 数据。

这些数据应由用户明确选择的 JQData、Tushare、bullet-data 或其他实现相同语义的 Provider 提供。当前已有纯 Python manifest/router 和 LiveEngine 构造器注入的两阶段 preflight，但 CLI 还没有 route 配置构建器，订单实时价读取也仍是旧 DataProvider 路径。下列配置仅表达目标边界，不代表当前已完全接线：

```dotenv
DEFAULT_DATA_PROVIDER=<HISTORICAL_AND_REFERENCE_PROVIDER>
DEFAULT_MARKET_DATA_FEED=<REALTIME_FEED_NAME>
DATA_CAPABILITY_ROUTES=<STRUCTURED_ROUTE_CONFIGURATION>
```

运行时断线、超时、鉴权失败、权限不足、stale、degraded、异常或空结果，都不能触发静默跨源切换。只有在调用前已经确认某项 capability 为 `unsupported`，且配置了同时间域、同语义的备用 Provider 时，才可以显式 fallback。L2 不能静默降级成 L1，实时推送不能静默变成历史 bar 或轮询代理。

## 三类容易误用的数据

以下数据可以辅助交易或实时策略，但不能冒充更完整的数据能力：

1. **Trader metadata**：交易所、市场、证券和股东号等信息用于登录、代码映射与报单校验，不具备公共 reference Provider 所需的 as-of、版本、完整性和历史语义。
2. **L1 中的 PE 等字段**：它只是当前行情时点的厂商字段，不能替代 point-in-time 财务报表、估值历史或因子库。
3. **实时 session bars**：未来若由 L1/L2 聚合，只能标为带 source epoch、warm-up、gap 和 completeness 的 computed realtime capability；不能满足历史 K 线、复权、回测或跨日窗口。

兼容 `tick` 也只是有损投影。完整华鑫能力应保留 typed L1/L2 事件、厂商扩展和经许可的 raw 字段，不能为了兼容旧策略而删除源事实。

## 生产与仿真安全门禁

默认原则是只读和 fail closed。

### 生产环境

首先必须通过柜台/网关返回的环境事实确认当前是生产环境，不能只相信主机名、文件名或历史标签。环境身份不明时一律按生产处理。

在没有一次性、明确的生产写授权时，只允许：

- 编译和校验 BulletTrade 自研 bridge；
- 显式加载 bridge 和厂商 SDK；
- 登录及 health/readiness 检查；
- 账户、资金、持仓、订单和成交查询；
- L1/L2/IOPV/状态行情的订阅与只读观察。

生产配置必须保持交易和撤单关闭。登录成功、进程存活、行情收到一条或查询成功，都不等于允许写入。

### 仿真写测试

只有同时满足以下四组门禁，才可以在已证明的仿真环境进行最小写测试：

1. **环境身份**：通过柜台/网关事实证明是仿真环境，不能只相信文件名或配置标签。
2. **账号门禁**：当前账号位于本次测试账号 allowlist。
3. **标的与数量门禁**：标的位于 allowlist，数量不超过本次明确上限。
4. **时间窗与开关门禁**：处于批准的测试时间窗，并同时打开独立的测试写、交易或撤单开关。

任一门禁缺失即不得调用写接口。提交结果未知时不得自动重发，也不得切换到另一券商补单。

## 环境变量模板

当前离线 build/doctor 通过命令行参数工作，不读取柜台凭据。HuaxinBroker 骨架已经消费下列最小变量，但真实 Trader 尚未实现，因此选择 `huaxin` 仍会在 `initialize` 前 fail closed：

```dotenv
DEFAULT_BROKER=huaxin
HUAXIN_ACCOUNT_ID=<PRIVATE_ACCOUNT_ID>
HUAXIN_ACCOUNT_TYPE=stock
HUAXIN_NATIVE_BUNDLE=<PRIVATE_CONTENT_ADDRESSED_BUNDLE_PATH>
HUAXIN_RUNTIME_MODE=server
HUAXIN_ENABLE_TRADING=false
HUAXIN_ENABLE_CANCEL=false
```

`HUAXIN_RUNTIME_MODE` 当前只进入配置骨架，尚未切换真实 embedded/server backend。`HUAXIN_SERVER_HOST`、全市场订阅开关和仿真写 allowlist 等仍是后续设计，尚未接线，不应据此判断功能已经可用。

真实 SDK 路径、bundle 路径、账号、TerminalInfo、柜台地址、token、测试标的、数量和时间窗只能放在私有且已忽略的配置中，并使用对应发布版本实际支持的变量名；不要把真实值补到 `env.example`、本文或源码。

## 怎样判断“可用”

至少应区分这些状态：

| 状态 | 能说明什么 | 不能说明什么 |
| --- | --- | --- |
| pip/install/import 成功 | 主包在当前 Python 环境可用 | native、SDK、行情、Broker 均不一定可用 |
| `offline_bridge_ready=true` | 自研 fake bundle 的指纹和制品通过离线校验 | 真实 TORA SDK 未被检查，`native_ready` 仍为 false |
| native/SDK preflight ready | 平台、ABI、依赖、bridge 与外部 SDK 匹配 | 柜台登录、权限和业务能力尚未证明 |
| 模块登录与只读查询通过 | 对应模块在该环境可读 | 下单/撤单仍默认禁止 |
| 仿真写测试通过 | 已验收的账号、标的、数量、时间窗组合可测试 | 不代表生产写权限或所有市价类型可用 |
| 生产 shadow/canary 通过 | 仅能支持已审核的最小范围 | 扩账户、扩策略、全市场 L2 仍需独立评审 |

当前第一阶段只覆盖前两行中的离线基础能力。后续实现和验收进度应以版本化 capability、自动化测试和真实环境的脱敏证据为准，而不是以文档描述替代。
