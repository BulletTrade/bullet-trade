# 掘金数据及账户验收：2026-10-03

结论：Windows 外部 SDK 的行情与账户读取已通，但目前不能把掘金原生返回直接视为
BulletTrade 的聚宽兼容数据。这是原始 SDK 验收时的状态；随后已注册 `GmDataProvider`，新的适配验收见 [数据层适配](gm-data-adapter.md)。`GmBroker` 和 Server adapter 仍未注册。
本次工作为只读验收和测试补全，不是部署或交易验收。

## 版本与范围

- 当前 clone：`b2b1fa1`，测试包含当前未提交的 GM 验收改动。
- Windows：Python 3.12.12、GM SDK 3.0.186、华鑫掘金 3.20.3.6。
- 聚宽基准：现有研究环境 RPC；没有用镜像或直接 JQDataSDK 查询替代。
- 本地客户端通过 SSH 把有限探针送入 Windows 内存，未升级服务器代码、修改终端配置，
  未运行现有策略，未调用 `run`、下单、撤单、转账或持久化订阅。
- SDK token 只从已有项目静态解析，未执行项目。账户 ID 仅在显式账户批次内使用；
  从日志选择唯一 RemoteV5 通道，不查询实盘 Bus 通道。
- SDK 资金的账户名称为 UUID，不能直接核实终端展示名。账户类型未自动判定；
  本次读取只证明该候选账户已登录并能查询。

## 用例补全

旧测试存在直接 SDK 鉴权、环境缺失时跳过、只比较收盘价或只比较重合日期的情况。
现有分红黄金样例及撮合测试主要检查内部单位与记账，不替代在线数据基准。

新脚本覆盖日线、1 分钟与 5 分钟线、股票/ETF/指数、现金分红、现金加送转、
基金年末分红、前后复权、复权因子、count、假期空区间、停牌跳过与填充、
交易日、证券上市日期、指数成分、涨跌停、停牌标志、快照、历史 tick、
多证券查询及账户只读接口。证券和历史窗口固定；前复权参考日采用执行当天的上海日期，
对应 RPC 默认当日参考日，报告中记录 `anchor`。

比较器不以 inner join 丢掉日期，不忽略缺字段、重复行、NaN/Inf 或错误证券。
阈值在请求前固定：价格一个报价刻度（ETF 0.001、股票/指数 0.01），成交量 1 股/份，
金额 `max(0.01 元, |基准| × 1e-8)`；没有为了通过测试提高阈值。
这些是本次验收阈值，不是完整产品精度承诺。价格通过与全部数据字段通过分别保留。

- `validation.py`：行情、事件单位、tick 时间与账户守恒比较器。
- `validation_worker.py`：SDK 只读白名单，原生委托/成交查询显式限定账户并检查错误码。
- `probe_gm_parity.py`：单连接、顺序访问聚宽 RPC，保存两边原始数据及差异。
- `test_gm_validation.py`：缺口、非有限数、单位、时区、非法动作、错误账户状态等负例。
- `test_gm_saved_data.py`：重放真实样本，保留已经发现的 vendor 差异。
- `test_gm_rpc_parity.py`：逐项验收显式选定的在线采集报告；差异和阻塞均失败，不 skip。

## 已确认的问题

| 项目 | 实测证据 | 对标准适配的影响 |
| --- | --- | --- |
| 复权成交量 | 中国平安 2024-07-25，GM 前复权量 44,047,429，RPC 量 51,035,515 | 原生 GM 保留原量，RPC 使用调整后的量；不能仅重命名字段 |
| 复权因子 | 中国平安样本前复权因子最大绝对差约 2.05e-7 | 即使价格在一个刻度内，也可能造成整数成交量差异 |
| 后复权价格 | 平安银行 2026-09-30，RPC 收盘 1787.64，GM 2239.228 | 历史累积因子体系不同，不是价格舍入即可修复 |
| ETF 历史复权 | 沪深300ETF 2024-12 样本，价格差最大约 0.0024 | 超过 0.001 刻度；不能称完整复权对齐 |
| 分钟金额 | ETF 1 分钟样本最大相差 1 元；银行 5 分钟样本约 0.96 元 | 价格和量一致也不表示全字段通过，需统一数值精度政策 |
| 停牌填充 | 万科 2016-06-27 至 07-01，RPC 有 5 行零量停牌填充，GM 没有 | 适配层需要交易日、停牌状态与前收盘数据；`Last` 重查仍缺行 |
| tick 默认过滤 | RPC 默认最后一条为 14:57:01，GM 为 14:59:58.826；显式 RPC `skip=False` 返回 14:59:58 | 要区分有成交 tick 和盘口快照，保留调用方 skip 语义 |
| 证券类型/名称 | 511880 RPC 类型 `mmf`，GM 类型 2；基金中文简称也不一致 | 证券上市日期核对通过，不表示全部名称/分类已兼容 |
| 基金增值接口 | `fnd_get_dividend`、`fnd_get_adj_factor` 返回 2001，SDK 消息含权限错误 | 账户能连接不代表所有数据权限可用 |
| 分红基准 | RPC Finance 报 `StateException: Interruptingcow can only be used from the MainThread.` | 三项分红对照被基准服务阻塞，不能用固定样例顶替通过 |

GM 旧版 `get_dividend` 可以取得实测事件：中国平安每股 1.5/0.93 元，
宁德时代 2023-04-26 每股 2.52 元加 0.8 转增比例，银华日利 2024-12-31 每份 1.5521 元。
这仅证明 GM 读取与事件单位，聚宽 RPC Finance 未通前不能宣称这些事件完成基准对齐。
宁德时代送转窗口依据发行人公告核实后改为 2023-04-24 至 04-28；最初的 6 月窗口
没有事件，不作为送转通过证据。

GM 文档明确指出 `skip_suspended/fill_missing` 暂不支持，见
[官方查询接口](https://emquant.18.cn/help/doc/python/python_select_api.html)。
股票增值分红接口 `cash_bf_tax` 是每十股，与旧版 `cash_div` 每股单位不同，见
[官方数据结构](https://emquant.18.cn/help/doc/cpp/cxx_object_data.html)。
停牌与送转样本来自发行人公告：[万科](https://static.cninfo.com.cn/finalpage/2016-07-02/1202445846.PDF)、
[宁德时代](https://static.cninfo.com.cn/finalpage/2023-04-18/1216457838.PDF)。

## 账户只读验收

候选账户实际 state=3、error_code=0，资金记录有更新时间。查询资金已同步、
持仓市值 0、持仓数 0；全部委托、未完成委托及成交查询均原生状态码 0、记录 0。
校验资金有限数、净资产=余额+市值、可用资金范围、冻结金额、持仓市值与现金市值一致，
以及查询覆盖和行数有效性。

空账户不能验证 T+1、拒单、成交、部分成交、撤单、重连去重或手续费扣款。
本次没有下单或撤单；也未以资金名称猜测账户类型。

## 完整接口覆盖边界

| BulletTrade 接口 | 本次覆盖 | 仍未验收 |
| --- | --- | --- |
| `get_price` | 日/分钟、三种 fq、count、假期、停牌、多证券原始查询 | 标准 provider 注册、panel 返回、avg 等完整字段、所有周期、指定历史参考日 |
| `get_trade_days` | 明确日期范围，含假期及下一交易日 | count 等组合参数 |
| `get_all_securities/get_security_info` | 4 只样本的上市日期、类型和名称差异 | 全市场历史证券全集与完整 SecurityInfo 模型 |
| `get_index_stocks` | 2026-09-30 沪深300成分全集 | 更早时点的成分变更边界及指数权重 |
| `get_split_dividend` | GM 股票/送转/基金事件实际读取 | RPC Finance 基准阻塞，配股、基金折算及货币基金收益分类 |
| `get_ticks/get_current_tick` | 历史最后一条、skip 差异、假期快照 | 全序列、五档、历史范围分页与订阅 |
| 涨跌停/停牌 | 单一股票固定窗口与 RPC 对照 | 全部标的类型、ST/退市/上市首日规则 |
| `get_extras/get_fundamentals`、行业/概念/期货/期权等可选方法 | 未实现 GM 标准适配 | 不作为本次股票及 ETF 核心矩阵通过项 |
| Broker/Server/LiveEngine | 账户只读原生查询 | 标准交易和远程适配、订单生命周期，均未实现 |

## 重现

当前采集脚本在 macOS/Linux 客户端运行（SSH 与 POSIX 总超时），SDK 查询在 Windows。
先安装本地验收依赖：

```bash
python -m pip install -e '.[dev,gm-validation]'
python scripts/probe_gm_parity.py \
  --rpc-env-file /path/to/private/rpc.env \
  --rpc-module /path/to/research/live/catboost_live/rpc_proxy.py \
  --gm-ssh-host YOUR_TEST_HOST \
  --gm-python '%USERPROFILE%\.conda\envs\bullettrade\python.exe' \
  --account --output-dir artifacts/gm-validation/run
python -m pytest tests/e2e/data/test_gm_rpc_parity.py \
  -m requires_network --no-cov \
  --gm-parity-report artifacts/gm-validation/run/report.json
```

最后一条命令读取已采集报告，不重新执行网络或账户动作。报告含采集时间；
历史报告通过不证明当前时刻连通。采集失败、权限不足、RPC 阻塞和差异会返回非零。
原始证据放在忽略的 `artifacts/gm-validation/` 下，不含 token、账户 ID 或服务地址。
测试 fixture 只保存公开行情和分红样本，账户原值不进入公共 fixture。

Finance RPC 的修复应让查询在服务主线程执行或采用支持线程的超时机制；
不要为了测试删除服务端的超时限制。此次未修改、重启 RPC 服务。

## 验证结果

最终在线矩阵 **49 项：23 通过，21 数据差异，3 基准阻塞，2 GM 权限失败**。
当前 clone 数据/分红/复权/QMT 相关离线回归 **525 项通过，5 项旧联网用例未纳入离线回归**。
新增在线报告验收保留失败，不把权限或 RPC 故障改为 skip。

最终原始差异见本次 `artifacts/gm-validation/20261003-acceptance/report.json`。
在线验收仍有失败项，不构成“GM 数据接口已经完整可用”的发布结论。

旧工作区还包含 clone 基线中没有的复权缓存测试。已以旧工作区 `7976427` 的实际模块
只读运行 `test_adjustment_cache_providers.py`、`test_prefactor_cache_saved_data.py`、
`test_prefactor_event_cache_contract.py`，29 项通过；没有拷贝测试到不具备对应实现的旧 clone，
没有更改旧工作区。这 29 项和当前 clone 的测试属于两个不同版本，分别记账。
