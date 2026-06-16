# 数据源能力矩阵

本页用于说明不同数据源的适用边界。新增 provider 均为显式启用，不会改变默认 `jqdata` 行为。

| Provider | 安装方式 | 前置条件 | 平台 | 历史行情 | 实时行情 | 复权与真实价格 | 当前状态 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| JQData | 默认依赖 | 聚宽账号 | macOS/Linux/Windows | 股票、基金、指数等，依账号权限 | SDK 实时/准实时能力 | 支持 `pre_factor_ref_date`，可用于动态真实价格 | 已有主力数据源 |
| MiniQMT | `bullet-trade[qmt]` | miniQMT/xtquant、本地数据目录 | 主要是 Windows | 依赖本地已下载行情，可自动下载 | 支持 QMT tick/quote | 支持未复权、前复权和动态锚定 | 已有实盘/QMT 主力数据源 |
| RemoteQMT | 默认代码路径 | 远程 bullet-trade server + QMT | 客户端跨平台，服务端 Windows | 由远程 QMT provider 提供 | 由远程 server 转发 | 由远程 QMT provider 能力决定 | 已有远程实盘数据源 |
| Tushare | `bullet-trade[tushare]` | Tushare token | macOS/Linux/Windows | 股票、指数、基金，依积分权限 | 非主力实时源 | 支持未复权、前/后复权；动态锚定依因子 | 已有补充数据源 |
| RQData | `bullet-trade[rqdata]` | RQData license 或账号 | macOS/Linux/Windows | 理论支持股票/基金/指数，待真实账号验收 | `get_live_current` mock 已覆盖，真实待验收 | 实现未复权 + factor + `pre_factor_ref_date` 手工锚定 | Beta，mock 已覆盖，真实账号待验证 |
| easy_tdx | `bullet-trade[tdx]` | Python 3.10+、网络可连通达信行情服务器 | macOS/Linux/Windows online；离线文件另议 | 实测样例日线到 1991-04-03，1m 到 2026-01-19，5m 到 2024-05-29；以服务器返回为准 | 支持 quote 轮询快照 | 未复权 + TDX 除权除息事件构造 factor，支持日线 `pre_factor_ref_date` 动态前复权；分钟线受历史深度限制 | Beta，本机 online 与 JQData 已做样例对账 |

## 选型建议

- 正式回测优先使用已完成复权和对账验证的数据源：JQData、MiniQMT 或已验证环境下的 Tushare。
- 需要与 QMT 实盘尽量一致时，优先 MiniQMT 或 RemoteQMT。
- easy_tdx 适合作为免费行情源探索和轻量 smoke；日线动态前复权已有样例对账，分钟级长期历史回测仍受免费 online 分钟线保留窗口限制。
- RQData 在拿到 API key 并完成真实对账前，只作为 Beta provider 接入和测试。

## 发布验收要求

新增数据源发布时至少要说明：

- 是否完成真实账号或真实 online smoke。
- 是否按《DATA_PROVIDER_ACCEPTANCE.md》逐函数输出验收结果。
- 是否完成与 JQData/MiniQMT/Tushare 的样例价格、复权、交易日和常用列表接口对账。
- 是否支持 `set_option("use_real_price", True)` 的动态真实价格。
- 失败时是抛错、返回空表还是显式 stub；真实模式不能静默生成假行情。
