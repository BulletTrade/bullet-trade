# 掘金环境准备与接入方案

当前提供 `bullet-trade[gm]` 可选依赖、`bullet-trade gm doctor` 环境诊断、
`bullet-trade gm probe` 只读联调、配置模板和 Windows 安装脚本。
已注册 `gm` 数据源（别名 `goldminer` / `掘金`），可通过标准数据 API 使用。
2026-10-08 已新增本地 `GmBroker` 候选版，并完成银华日利仿真往返交易，见[本地交易与验收](gm-trading.md)。
GM Server adapter 尚未实现，不能使用 `--server-type gm`。
数据口径、标准接口范围和实测结果见[掘金数据层适配](gm-data-adapter.md)。
2026-10-04 已采集样本的实用数据验收为 52 通过、0 失败；相关离线回归为 612 通过、0 失败。
下一阶段的模块、实施顺序及交易验收条件见[完整接入计划](gm-integration-plan.md)。

## 环境分工

| 环境 | 当前可执行 | 需要验证的事项 |
| --- | --- | --- |
| macOS 开发机 | 安装核心与 dev 依赖、运行诊断与离线回归 | 官方没有 macOS gm wheel，不能在 Mac 验收 SDK |
| Windows x64 | 安装 gm、显式导入 SDK | 目标 Windows Server、券商终端版本与登录状态 |
| Linux x64 | 公共 SDK 有安装包 | 本阶段不作为目标部署，未验证连接券商 Windows 终端 |

推荐 Python 3.11 x64。2026-10-02 核对公共 SDK 最新版本为 `gm 3.0.187`，于
2026-09-28 发布。版本和安装包证据见 [官方维护的 PyPI 项目](https://pypi.org/project/gm/)。
券商指定的 SDK 版本应单独核实，不由公共最新版推断终端兼容性。
2026-10-03 已在 Windows 仿真机使用现有 `gm 3.0.186` 完成 SDK 导入及有限只读查询，
因此最低版本调整为 `3.0.186`，不要求为基础联调强制升级已有 SDK。

## macOS 本地开发

在 clone 的仓库根目录执行：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m pip check
.venv/bin/bullet-trade --version
.venv/bin/bullet-trade gm doctor
```

Mac 上 doctor 显示 `sdk_platform_supported=false`，退出码为 2，这是 SDK 平台限制；
它不代表 BulletTrade 开发环境不可用。`gm` 是带平台条件的可选依赖，基础安装和 QMT
接入不会自动导入或初始化掘金 SDK。

## Windows SDK 环境

先安装匹配的 Windows 券商版掘金终端和 Python 3.11 x64。终端由券商提供，
BulletTrade 包不附带券商安装包、文档或凭据。

在 Windows clone 的仓库根目录手动运行：

```powershell
.\scripts\setup_gm_environment.ps1
```

如果不使用 Python Launcher：

```powershell
.\scripts\setup_gm_environment.ps1 -PythonExe "C:\Python311\python.exe"
```

脚本创建或复用 `.venv-gm`，安装 `.[gm]` 和指定版本 SDK，运行 pip check 及离线 doctor。
它不启动终端或交易服务。手动执行的等价步骤为：

```powershell
py -3.11 -m venv .venv-gm
.\.venv-gm\Scripts\python.exe -m pip install -e ".[gm]" "gm==3.0.187"
.\.venv-gm\Scripts\python.exe -m pip check
.\.venv-gm\Scripts\bullet-trade.exe gm doctor
```

安装元信息通过后，显式验证 SDK 能否导入：

```powershell
.\.venv-gm\Scripts\bullet-trade.exe gm doctor --load-sdk --timeout 10
```

`--load-sdk` 在同一解释器的子进程中导入 `gm.api` 并核对基础函数。它不调用
`set_token`、`run`、行情查询、下单或撤单，不将 SDK 原始 stdout/stderr 透传到诊断结果。
如果 native 导入失败、进程退出或超时，doctor 返回失败；不能据安装元信息跳过导入验证。

doctor 退出码为 0 只表示对应环境检查通过。`terminal_connection` 和 `account_connection`
均为 `not_checked`；SDK 可导入也不证明行情、账户或交易通道已可用。

## 私密配置

复制 `env.gm.example` 为 `.env.gm`，填写自己的值。`.env.gm` 已被 Git 忽略。

| 配置 | 用途 |
| --- | --- |
| `GM_TOKEN` | 用户身份密钥 |
| `GM_STRATEGY_ID` | 完整 SDK 实时会话的策略身份 |
| `GM_ACCOUNT_ID` | 明确选择的资金账户 ID |
| `GM_SERV_ADDR` | 终端服务地址；留空使用 SDK 默认本机地址 |

```powershell
.\.venv-gm\Scripts\bullet-trade.exe --env-file .env.gm gm doctor
```

当前诊断只报告这些配置是否存在，不输出值，也不使用它们认证。
数据接口只需要 `GM_TOKEN` 及匹配的终端地址，可设置 `DEFAULT_DATA_PROVIDER=gm`。
`GM_ACCOUNT_ID` 不参与数据接口，仍不能设置 `DEFAULT_BROKER=gm`。

## 显式只读联调

在安装好 SDK 的 Windows 环境中，配置自己的 `.env.gm` 后执行：

```powershell
python -m bullet_trade --env-file .env.gm gm probe --start 2026-09-28 --end 2026-09-30
python -m bullet_trade --env-file .env.gm gm probe --start 2026-09-28 --end 2026-09-30 --account
```

默认查询 `SHSE.510300` 的日线及快照，可用 `--symbol` 指定其他 SHSE/SZSE 六位代码。
必须明确日期范围，最多 31 个自然日；`--timeout` 默认为 30 秒，最多 120 秒。
配置中的账户 ID 只有在显式使用 `--account` 时才会用于资金、持仓及连接状态查询。
探针不执行现有策略文件、不调用 `run`、不下单或撤单；凭据仅经子进程标准输入传递。

行情输出包含来源时间，假期快照可能仍为上一交易日的数据。
`ok=true` 只表示本次有限查询通过；不能据此认定实时推送或交易能力已验收。
带 `--account` 时还必须确认 SDK 状态为 `3`（已登录），否则返回退出码 2。
初始化资金记录可能完整但全零、无更新时间；这不是账号已接通的证据。
`account_type` 始终为 `not_checked`，探针不能从 UUID、名称或零持仓判断账户是否仿真。

账户状态查询复用已验证 SDK 的 `Context._get_account_info` 内部 protobuf 协议。
如果后续 SDK 不再提供这些内部类型或函数，探针返回失败，不退化成“连接已通过”。
当前通用探针不查询委托及成交。新增验收 worker 使用已验证的 SDK 原生协议，
按显式账户 ID 查询并检查错误码，避免公用 `get_orders()` 遍历空的会话账户集合后误报成功。

## 2026-10-03 早期 Windows 验证记录（账号尚未连接时）

| 检查 | 已取得的证据 | 验收含义 |
| --- | --- | --- |
| 终端与后台 | 华鑫掘金 3.20.3.6、`gmterm-serv`、`ds-proxy` 在运行，本地 7001 等端口监听 | 本机 SDK 服务存在 |
| SDK | Python 3.12.12 x64 可导入用户级安装的 `gm 3.0.186`；修正后的 doctor 实机通过 | 不是独立 SDK 环境，尚未安装新的 venv |
| 历史行情 | `SHSE.510300` 返回 9 月 28—30 日的 3 根日线 | 有限历史查询通过 |
| 快照 | 返回 9 月 30 日 15:01:43 的记录 | 假期历史快照，不是当天实时行情 |
| 账号状态 | Bus 与 RemoteV5 通道均返回 state=5、error_code=0 | 目前 SDK 观察到账号已断开，不能由错误码 0 推断已登录 |
| 资金与持仓 | 显式指定 RemoteV5 ID：资金记录全零、无更新时间/通道标识，持仓 0 | 初始化缓存，不能据此确认上游资金同步 |
| 委托/成交 | SDK 底层按该 ID 查询委托、未结委托、成交，均返回状态码 0、记录 0 | 验证了只读协议返回；账号断开时不能作为上游交易验收 |
| SDK 初始化 | 独立短时 SDK 初始化返回 0，但该账号仍为 state=5 | SDK 初始化与账户登录是不同检查 |
| 上游 TCP | RemoteV5 已配置端口可达；Bus 已配置端口在 3 秒内连接超时 | 网络探针不证明登录成功；不能仅据一次超时判断服务长期不可用 |
| Windows 脚本 | 使用 PowerShell 5.1 解析准备脚本 | 只验证语法，没有执行远端安装或升级 |

尚需确认 RemoteV5 账号与用户创建的虚拟账号对应，再在终端连接该虚拟账号，
取得 state=3 及实际资金同步证据。当前没有执行实盘或仿真下单、撤单，
也没有改动远端账户配置、升级 Python/SDK 或部署仓库。
状态 5 的定义见 [官方枚举常量](https://emquant.18.cn/help/doc/python/python_enum_constant.html)。
本次证据未确定账号断开的原因，不能仅根据假期推断。

## 2026-10-03 账户连接后的数据与 API 验收

后续读取已确认唯一 RemoteV5 候选账户 state=3、有实际资金更新时间，资金已同步，
持仓、委托和成交查询成功且为空。原生行情已与聚宽 RPC 开展全字段、完整日期对照。
复权成交量、部分历史价格、分钟金额和停牌填充有差异；分红 RPC 被服务端线程限制阻塞，
基金增值接口缺少权限。不能据连接成功宣称已经兼容 BulletTrade 标准接口。

已补充只读矩阵脚本、严格比较器、实测数据重放和逐项报告验收。
详细范围、复现方法和未通过事项见 [数据与账户验收记录](gm-validation-20261003.md)。

## 与现有 QMT 接入的对应关系

目标是让自己的策略继续使用 BulletTrade 数据、交易 API，新增掘金后端：

```text
策略 / 执行器
  -> BulletTrade 标准接口
  -> GmDataProvider（已实现） / GmBroker（本地候选版）
  -> gm.api SDK 会话
  -> 掘金终端及行情、交易服务
```

外部 SDK 运行入口由掘金原生提供，参见 [SDK 基础函数](https://emt.18.cn/api/quant-help/python/python_basic.html)。
不需要先复制大 QMT 的终端内 HTTP helper。策略身份、token、终端及账户登录仍需正确配置。

现有数据查询已采用隔离的短时 worker。交易计划采用隔离的常驻 worker 管理回报，
先验证数据和交易会话能否共存；若存在策略身份冲突，再由常驻 worker 承接两类调用。
目前没有常驻交易会话或交易服务，具体设计和阶段验收见[完整接入计划](gm-integration-plan.md)。
远程执行器可复用 BulletTrade Server 的长度前缀 JSON/TCP 协议，增加掘金 adapter。

后续改动应沿现有扩展点展开：

| 层次 | 现有入口 | 掘金待实现内容 |
| --- | --- | --- |
| 数据 | `data/providers/base.py`、`data/api.py` | 已实现 `GmDataProvider` 与创建/配置；补齐运行行情与远程验收 |
| 交易 | `broker/base.py`、`broker/registry.py` | `GmBroker` 与券商注册/配置 |
| 远程服务 | `server/adapters/base.py`、`server/adapters/__init__.py` | 掘金数据和交易 adapter、Server 配置 |
| 策略运行 | `core/live_engine.py` | 通过既有标准接口使用新后端 |

数据适配先对齐代码（例如 `510300.XSHG` 与 `SHSE.510300`）、K 线起止时间、周期、
成交量/成交额、复权参考日及停牌填充；实现不了的能力明确报错。数据源支持需按既有
`DataProvider` 合同逐项验收，不能只包装一个 history 就称全接口兼容。

交易适配须明确账户，映射资金、持仓、T+1 可卖数量、委托身份、拒单与部分成交状态。
按账户和 `cl_ord_id` 关联订单、按成交身份去重；断线后查询原委托，不因超时重发。
普通买卖接口与 TWAP/POV 等算法母单服务分别适配，前者不应依赖算法母单权限。

## 下一阶段验证顺序

1. 核对开发基线、实际账户身份、SDK 交易合同与会话共存。
2. 实现常驻交易 worker 和 `GmBroker`，先验收标准账户查询。
3. 完成限价交易、定向撤单、订单/成交同步、故障恢复与 LiveEngine 接入。
4. 在交易日通过指定仿真账户验收实际成交和账户变化。
5. 增加 GM Server adapter 和远程客户端入口，再完成集成回归及发布准备。

本阶段不选 `gmtrade` 作为默认依赖。它确有独立交易接口，但当前公开版本与
券商端兼容性需要另外核实，见 [官方文档](https://sim.myquant.cn/sim/help/Python.html) 和
[发行包](https://pypi.org/project/gmtrade/)。
