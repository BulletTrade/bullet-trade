# 更新日志

本文档记录所有重要的变更。格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

## [0.9.1] - 2026-07-03

### 修复
- **大 QMT 当前价接口**：修复 `data.current_tick` 在 server 分发层把完整 payload 传给 `get_current_tick(symbol)` 导致请求失败的问题；大 QMT 后端现在通过快照路径返回 `sid/last_price/dt`，保持上层策略取当前价语义可用。
- **大 QMT 历史行情缓存**：大 QMT helper 的 `data.history` 默认先调用大 QMT 内置 `download_history_data`，再读取 `get_market_data_ex`，对齐 MiniQMT “先下载/确保缓存，再读取”的安全口径。

### 增强
- **大 QMT 证券信息归一化**：`data.security_info` 补齐 `code/qmt_code/display_name/name/start_date/end_date/type/subtype/parent` 等 MiniQMT 兼容字段，减少上层策略和 V2 网关适配差异。
- **大 QMT 指数成分口径**：`data.get_index_stocks` 优先使用 `download_index_weight` / `get_index_weight`，仅在不可用时回退 sector 列表，避免指数成分与 MiniQMT 口径不一致。

### 文档
- **大 QMT 对齐说明**：补充大 QMT helper 的历史数据预下载、当前价、证券信息和指数成分兼容口径，以及对应顶部配置参数说明。

### 测试
- **大 QMT 对齐回归**：新增 helper 和 server adapter 回归测试，覆盖 history 自动下载失败保护、显式关闭自动下载、`security_info` 标准字段、index weight 优先级和 `data.current_tick` 分发。

## [0.9.0] - 2026-07-03

### 新增
- **大 QMT 网关后端 Beta**：新增 `big_qmt` / `big-qmt` server adapter，通过大 QMT 策略 helper 承接行情、账户、持仓、委托、成交、下单和撤单能力，并继续向上层暴露兼容 `qmt-remote` 的协议。
- **大 QMT helper 策略**：新增 `helpers/big_qmt_gateway_strategy_sample.py`，可复制到大 QMT Python 策略中运行，提供本机 HTTP/JSON gateway、启动日志、健康检查、交易开关、撤单开关和虚拟子账户订单备注映射。

### 增强
- **MiniQMT 缓存预下载接口**：补齐 MiniQMT provider 和 QMT server adapter 的 `ensure_cache` 能力，统一“先下载/确保缓存，再读取历史行情”的数据准备流程。
- **远程 server 状态透出**：`admin.health` 会透出后端类型和大 QMT helper 健康状态，方便部署后确认当前服务实际连接的是 MiniQMT 还是大 QMT。

### 兼容性
- **持仓代码后缀别名**：实盘账户同步后的持仓支持 `.SH/.SZ` 与 `.XSHG/.XSHE` 互相命中，避免大 QMT 返回 QMT 后缀时策略按聚宽后缀读取不到持仓。
- **远程下单未知状态处理**：`qmt-remote` 对 `submit_unknown` 响应优先按提交状态未知处理，即使远端尚未返回订单号，也不会误报成普通“未返回 order_id”。

### 文档
- **大 QMT 图文向导**：新增大 QMT 服务向导和 6 张操作截图，说明大 QMT helper 启动、`--server-type big_qmt`、`BIG_QMT_GATEWAY_*` 配置、方案 A/方案 B 兼容关系，以及 MiniQMT 与大 QMT 两种后端的边界。
- **MiniQMT / 大 QMT 边界说明**：更新新手路线、配置总览、实盘、QMT server 和数据源文档，明确 `qmt` 是 MiniQMT/xtquant 本地直连，`big_qmt` 是大 QMT server 后端，`qmt-remote` 是上层远程协议。

### 测试
- **大 QMT 后端合同测试**：新增 Big QMT adapter 和 helper 单元测试，覆盖数据格式归一化、交易日日期兼容、账户/持仓/订单/成交结构、下单确认、submit_unknown、撤单开关、虚拟子账户备注过滤和 QMT userOrderId 兼容。
- **MiniQMT 与持仓兼容回归**：新增 MiniQMT `ensure_cache`、QMT history adapter、持仓后缀别名和 live 账户同步回归测试。

## [0.8.2] - 2026-06-25

### 修复
- **拆分日持仓估值口径**：回测引擎在拆分/送转生效日记录拆分后的持仓价格口径，当行情源仍返回拆分前旧收盘价时自动折算或保留拆分后估值，避免日终持仓市值出现虚假大幅波动。

### 文档
- **MiniQMT 截图资源更新**：更新 `docs/assets/mini_qmt.png`，同步当前 MiniQMT 相关文档展示素材。

### 测试
- **拆分日旧价回归覆盖**：新增 510500 拆分生效日回归用例，覆盖行情返回旧价时日终估值仍保持拆分后份额口径。

## [0.8.1] - 2026-06-23

### 新增
- **聚宽模拟盘接管兼容层**：新增 `install_jq_compat(...)`，在聚宽回测中保持原平台行为，在模拟盘中接管 `context.portfolio`、`context.subportfolios` 和常用交易函数，把现金、持仓和下单统一到 BulletTrade 远程账户。
- **聚宽风格下单函数补齐**：远程 helper 支持 `order_percent`、`order_target_percent`、`MarketOrderStyle`、`LimitOrderStyle` 和 `value` 参数别名，覆盖 `order(...)`、`order_target_value(...)`、`order_target_percent(...)` 等常见聚宽策略写法。

### 兼容性
- **实盘接管默认同步等待**：`install_jq_compat` 包装后的聚宽同名下单函数默认同步等待 16 秒；低层 `bt.order(...)` 等显式 helper 调用继续保持原有默认异步语义。
- **交易范围显式收口**：第一版兼容股票/ETF 多头常用交易，遇到 `side="short"`、`pindex!=0`、`close_today=True` 或停止单时显式报错，避免静默按错误语义下单。

### 文档
- **聚宽接入方案文档**：新增方案 A 显式调用 helper、方案 B 接管聚宽函数、两种方案对比和模拟盘完全接管设计说明，并同步更新新手路线、交易支撑页和 MkDocs 导航。

### 测试
- **聚宽远程 helper 回归覆盖**：补充回测不接管、模拟盘远程账户代理、默认等待窗口、按比例下单、订单样式映射和不支持参数拒绝等单元测试。

## [0.8.0] - 2026-06-16

### 新增
- **RQData 数据源 Beta**：新增 `RQDataProvider`，通过 `rqdatac` 提供聚宽兼容 `get_price/get_trade_days/get_all_securities/get_index_stocks/get_split_dividend` 等接口，支持 `factor`、`paused`、分钟线涨跌停补齐和 `pre_factor_ref_date` 动态前复权锚定路径；缺依赖或缺账号只影响显式启用的 `rqdata` provider。
- **easy_tdx 通达信数据源 Beta**：新增 `EasyTdxProvider`，通过 `easy-tdx` online 行情服务器提供免费日线、分钟线、实时 quote 和基础证券列表能力；前复权优先用 TDX 除权除息事件构造 `factor` 并支持日线 `pre_factor_ref_date` 动态锚定；真实模式连接失败显式报错，`use_stub=True` 仅用于测试/demo，不会静默返回假行情。
- **数据源可用性探测脚本**：新增 `scripts/probe_easy_tdx_capabilities.py`，用于在本机探测通达信免费行情的日线、分钟线、quote 和证券列表返回范围，便于补充发布前数据使用约束。

### 修复
- **easy_tdx 长区间历史行情截断**：`EasyTdxProvider` 的日期区间请求不再被 8000 条窗口低估，旧日期短窗口也会按距今跨度估算请求条数，允许按更大 `count` 拉取通达信 online 可返回的完整历史窗口；同时在连接前创建 `~/.easy_tdx`，避免上游自动保存 best host 时因目录不存在而偶发失败。
- **远程 QMT 下单等待超时语义**：`broker.place_order` 在已拿到券商委托号但等待终态超时时返回 `open/submitted + timed_out=true` 的可追踪订单信息，避免客户端 RPC 超时掩盖真实委托号。
- **远程请求超时预算**：`qmt-remote` 默认 RPC timeout 调整为 60 秒，并在下单时自动保证请求超时大于 `wait_timeout + QMT_PLACE_ORDER_TIMEOUT_MARGIN`，降低服务端刚返回 open/timed_out 前客户端先断开的概率。
- **服务端请求窗口同步扩展**：server session 默认请求超时保持 60 秒；当 `broker.place_order` 显式传入更长 `wait_timeout` 时，外层请求窗口同步扩展到 `wait_timeout + 30s`，避免 server 先于下单等待窗口超时。
- **远程默认等待窗口一致性**：`qmt-remote` 显式配置默认等待窗口时，会把同一个等待值传给 server 并用于 RPC timeout 预算；未配置时不额外写入 `wait_timeout`，保持旧 server 默认行为。
- **长连接请求超时捕获**：显式捕获 `concurrent.futures.TimeoutError` 并转换为标准 `TimeoutError`，确保 Python 3.9 等环境下远程下单网络超时也能稳定进入 `submit_unknown` 保护路径，同时取消后台 pending future。

### 兼容性
- **新增字段保持可忽略**：`broker.place_order`、`broker.orders`、`broker.trades` 和聚宽 helper 订单对象新增 `timed_out`、`async_tracking`、`last_snapshot`、`sub_account_id` 等可选字段；原有返回结构和订单号字符串返回语义保持不变。
- **未知提交状态保护**：网络超时且未拿到远端订单号时，客户端将该场景标记为 `submit_unknown` 风险并抛出异常，调用方应通过后续订单/成交查询确认，不应直接当作明确失败重复下单。
- **聚宽 helper 超时余量可配置**：`bullet_trade_jq_remote_helper.configure()` 新增可选 `place_order_timeout_margin` 参数，默认 30 秒，旧调用方不需要调整；显式配置为 0 秒时不会被默认值覆盖。

### 测试
- 补充 RQData/easy_tdx provider 离线单元测试，覆盖字段映射、panel 形状、`panel=False` 长表、动态复权锚定、显式 stub 边界、连接失败不返回假行情、实时 quote 和证券类型兜底判断。
- 补充 easy_tdx 与 JQData 的真实 e2e 对账测试，覆盖日线未复权、1m 未复权、日线前复权价格和 `pre_factor_ref_date` 动态前复权；对账窗口固定在 JQData 当前可见日期内。
- 补充远程 QMT 下单等待超时、长连接请求超时清理、订单/成交追踪字段、helper 忽略未知新增字段、RPC timeout 安全余量和 `sub_account_id` list 返回兼容测试。

### 文档
- 新增 RQData/easy_tdx Beta 数据源文档、数据源能力矩阵和 OpenSpec 接入说明，明确安装方式、配置项、复权限制、真实账号/online smoke 验收状态，以及通达信免费数据需要通过探测脚本确认历史深度。

## [0.7.4] - 2026-06-08

### 修复
- **聚宽远程 helper 下单兼容性**：修复 `bullet_trade_jq_remote_helper.py` 在 Python 3.7 以下环境调用 `time.time_ns()` 生成下单幂等键时抛出异常的问题，避免聚宽旧版运行环境在下单请求发出前中断。

### 测试
- **旧版 Python 时间 API 回归覆盖**：新增模拟缺少 `time.time_ns` 的 helper 幂等键生成测试，覆盖聚宽旧环境兼容路径。

## [0.7.3] - 2026-06-03

### 概览
- **发布范围**：自 `v0.7.2` 起共 5 个提交，涉及 19 个文件，累计约 1287 行新增、153 行删除

### 修复
- **限价单与市价保护单语义统一**：`order/order_value/order_target/order_target_value(..., price=...)` 统一恢复为显式限价单；`MarketOrderStyle(limit_price=...)` 明确定义为市价保护单，并在实盘侧保持 `market=True` 下发
- **回测撮合价格边界**：回测先按当前 bar 判断限价/保护价是否可成交，再对滑点后的成交价应用限价/保护价、涨跌停和价格笼子的共同约束，修复合法买单被“滑点价超出限价”误取消的问题
- **目标市值下单取整**：修正浮点数贴近整数时被截断导致的目标股数/目标市值下单数量误差，避免因二进制浮点尾差少买或少卖一股/一手
- **权益事件持仓资格**：修正分红、拆分等权益事件在回测中的持仓资格判断，避免除权日当天或除权日之后新买入的持仓重复享受历史权益事件
- **every_bar 回测语义**：对齐 every_bar 在回测与实盘中的调度语义，减少组合策略中每 bar 任务触发时间不一致的问题

### 增强
- **统一价格边界工具**：新增委托价、保护价和模拟成交价共用的涨跌停/价格笼子边界计算与裁剪逻辑，供回测撮合和实盘保护价路径复用
- **异步调度日志可读性**：优化异步调度注册日志，区分真实注册与已有任务复用，减少排查调度行为时的歧义

### 文档
- **订单 API 说明更新**：更新 `docs/api.md`，明确 `price=` 表示限价单，市价保护单需显式使用 `MarketOrderStyle(limit_price=...)`；同步说明回测成交价会受限价/保护价、涨跌停和价格笼子共同约束

### 测试
- **订单撮合回归覆盖**：新增 `close=81.71`、买入限价 `81.75`、滑点后 `81.75` 应成交的回归用例，并覆盖滑点超过限价时成交价封顶、基础价格高于限价时不成交等场景
- **实盘市价保护单覆盖**：新增 `MarketOrderStyle(limit_price=...)` 在实盘侧保持市价单并裁剪到价格笼子/涨跌停范围内的测试
- **回测与数据边界覆盖**：补充目标市值取整、权益事件资格、every_bar 调度、价格边界工具和 QMT 历史分钟缺涨跌停时按前收兜底的测试

## [0.7.2] - 2026-05-20

### 新增
- **回测数据会话优化**：新增仅限回测生命周期的 `BacktestDataSession`，用于管理一次回测内的行情块、QMT 下载记录、current bar 快照、内存预算和统计；默认关闭，不影响既有回测和实盘
- **回测性能 CLI 开关**：`bullet-trade backtest` 新增 `--backtest-data-session`、`--backtest-price-block-cache`、`--backtest-data-session-manifest` 和 `--backtest-data-session-max-bytes`，也支持 `BT_BACKTEST_DATA_SESSION*` 环境变量
- **QMT 回测下载去重**：MiniQMT 回测模式支持按证券、周期和覆盖区间登记已下载历史数据，覆盖命中时跳过重复 `download_history_data`，并输出下载与缓存统计
- **MiniQMT 分钟线重采样**：MiniQMT 支持从 1 分钟数据重采样生成多分钟 K 线，并按聚宽兼容口径处理 open/high/low/close/volume/money 等字段
- **回测内存行情块缓存**：支持对可证明等价的历史窗口做大块读取和按 `current_dt` 切片；动态前复权场景使用真实价格与复权因子等基础输入重新锚定，避免复权结果跨日期串用
- **JQData pickle 兼容层**：新增 `bullet_trade.data.pickle_compat`，兼容新老 `numpy._core` 与旧 pandas pickle 模块路径，降低不同 JQData/JQDataSDK 环境之间反序列化失败的概率

### 修复
- **远程市价保护价处理**：修复远程 QMT 下单链路中市价单保护价、请求价和服务端风控校验的传递问题，避免保护价被错误解释或丢失
- **QMT 市价单参考价**：QMT broker 下单时优先使用对手方可成交价格作为市价单参考价，提升市价保护价和拆单金额估算的准确性
- **远程 QMT get_price MultiIndex**：远程 QMT 服务端与客户端保留并恢复 DataFrame MultiIndex 列信息，修复多标的/多字段行情返回后列顺序或字段层级异常的问题
- **远程历史行情参数透传**：QMT server adapter 透传 `skip_paused`、`panel`、`fill_paused` 和 `pre_factor_ref_date` 等参数，使远程 `history/get_price` 与本地 provider 行为更一致
- **远程下单等待超时覆盖**：修复单笔订单 `wait_timeout` 被全局配置覆盖的问题；服务端下单链路会透传请求级等待时间
- **聚宽远程 helper 订单兼容**：增强 `bullet_trade_jq_remote_helper.py` 对订单、成交、备注字段和状态查询的兼容处理，补齐 `get_orders/get_open_orders/get_trades` 等常见查询路径
- **JQData 新老环境兼容**：JQData provider 在导入 `jqdatasdk` 前安装 pickle 兼容别名，避免镜像或 SDK 版本差异导致 `numpy._core`、旧 pandas 索引模块缺失
- **QMT 本地行情兼容性**：MiniQMT 兼容本地 K 线数据缺少 `time` 列的返回形态，避免重采样或字段整理时异常退出
- **QMT/JQData 分钟线口径对齐**：修正 QMT 多分钟重采样的成交量、成交额和 1 分钟开盘集合竞价处理，使 QMT 分钟线结果更贴近聚宽 bar 语义
- **Tushare 行情单位与分钟线**：Tushare 日线行情统一转换到聚宽兼容单位（`volume=股`、`money=元`），分钟频率支持 `5m/5min` 等别名并使用 `trade_time` 作为索引；分钟线复权按交易日因子匹配

### 增强
- **实盘隔离保护**：回测数据会话只在 `BacktestEngine.run()` 且显式启用时生效；MiniQMT `mode=live`、QMT server adapter、实时 tick 和实盘 current data 不读取回测下载记录或内存缓存
- **内存预算与降级**：回测缓存支持 `max_cache_bytes` 硬上限、可选最小剩余内存保护、LRU 驱逐、单块过大降级和 manifest 统计，内存不足时优先保证结果正确
- **current data 快照**：回测同一 bar 内可复用 current data 快照，bar 推进后自动失效，减少同一时点重复行情读取
- **下单等待文档化**：明确 `TRADE_MAX_WAIT_TIME` 与 API 级 `wait_timeout` 的关系，`None` 使用全局配置、`>0` 同步等待、`0` 异步立即返回

### 文档
- **回测数据会话文档**：新增 `docs/backtest-data-session.md`，说明启用方式、环境变量、能力边界、实盘隔离、manifest 和回滚方式
- **完整配置参考恢复**：恢复并补全配置总览，覆盖实盘下单等待、风控、MiniQMT、回测数据会话等常用配置项
- **配置与实盘文档补充**：更新 API、实盘、MiniQMT provider 与贡献文档，补充回测数据会话、订单等待超时、QMT 下载去重和远程市价保护价说明
- **文档导航更新**：在 MkDocs 导航中加入回测数据会话专题

### 测试
- **回测数据会话覆盖**：新增/补强回测 session 生命周期、QMT 下载去重、行情块切片、动态复权、future guard、低内存 LRU、实盘隔离和 manifest 相关测试
- **MiniQMT 与多数据源口径回归**：新增 MiniQMT 分钟线重采样、QMT/JQData 分钟线对齐、本地数据缺失 `time` 列、Tushare 单位转换和分钟线复权相关测试
- **远程 QMT 与 helper 回归**：新增 MultiIndex DataFrame payload、远程历史行情参数、市价保护价、订单等待超时、订单/成交查询兼容和 QMT server adapter 下单链路测试
- **JQData 兼容与离线测试隔离**：新增 pickle 兼容测试；真实 JQData/聚宽镜像用例标记为 `requires_network`/`requires_jqdata`，默认测试使用离线 provider，避免 CI 或本地快速测试依赖外部行情服务

## [0.7.1] - 2026-05-09

### 新增
- **新手入门路线**：新增 Python 环境安装、新手总览、方案 A（本地独立运行）和方案 B（聚宽模拟盘发信号、本地 QMT 执行）文档，并在 README 与文档首页补充“新手应该先看什么”
- **最小买入金额风控**：新增 `MIN_BUY_ORDER_VALUE` / `min_buy_order_value`，用于在风控开启后过滤实盘买入小单；默认 `0` 表示不限制，卖出、清仓和降仓不受影响

### 修复
- **MiniQMT 实盘行情补齐**：MiniQMT 数据源未显式配置时默认开启自动下载，实盘模式也会主动补齐缺失行情；仍可通过 `MINIQMT_AUTO_DOWNLOAD=false` 显式关闭
- **Tushare 指数与基金行情路由**：按聚宽代码后缀和证券类型自动选择 Tushare `asset`（股票 `E`、指数 `I`、基金/ETF `FD`），修复指数查询被当成股票处理的问题，并避免对非股票资产套用股票复权逻辑
- **Pandas 兼容性**：约束 `pandas<3.0`，并为年/月度报表自动选择 `YE/ME` 或旧版 `Y/M` 频率别名，减少新版 pandas 下的重采样兼容问题

### 增强
- **调度与持仓输出可读性**：调度日志将重叠策略显示为中文标签，持仓/账户表格改进中文宽度对齐，并减少冗余分隔线
- **MiniQMT 日志降噪**：关闭部分高频 `debug` 输出，降低本地行情读取、停牌日填充等路径的日志噪音（#30）
- **依赖约束整理**：`requirements.txt` 中 `numpy` 下限回到 `>=1.21.0`，与项目声明的 Python 版本和核心依赖范围保持一致

### 文档
- **文档导航重整**：`mkdocs.yml` 新增新手入门、安装环境、方案 A/B 入口，并加入 Mermaid 初始化脚本与配套截图
- **配置说明补充**：更新 MiniQMT、Tushare、实盘、快速上手和配置总览文档，补充自动下载、资产类型判断和最小买入金额风控说明

### 测试
- **回归覆盖扩大**：新增/更新 MiniQMT 实盘自动下载、Tushare 指数资产路由、最小买入金额风控、本地/远程实盘风控、pandas 重采样别名和表格输出相关测试

## [0.7.0] - 2026-04-21

### 新增
- **远程 QMT 运行时探针**：新增 `bullet_trade.server.runtime_probe` 与 `helpers/remote_qmt_runtime_probe.py`，支持对远程 server 的协议、字段、行情、账户、委托/成交链路做巡检，输出 JSON / Markdown 报告，并可选执行最小化下单 smoke
- **服务端撤单风控与幂等保护**：远程服务新增撤单次数/频率限制、下单幂等控制及对应环境变量配置，降低重复下单和高频撤单风险
- **实盘 broker 生命周期钩子**：`BrokerBase` 新增 `before_open/after_close` 可选钩子，`LiveEngine` 在盘前与盘后安全触发，兼容现有 broker 实现
- **回测 benchmark 与超额收益分析**：报告新增 benchmark 收益率、年化 benchmark 收益率、累计超额收益率，并在 HTML 报告中展示 benchmark 叠加曲线与超额收益图表

### 修复
- **实盘启动互斥与配置解析**：实盘启动增加 runtime / 实例级锁，避免重复启动；布尔配置统一走 `parse_bool`，风险控制初始化更稳定
- **订单状态与成交语义**：修复订单成本计算，保留订单结算状态，区分市价单请求价与券商价，并补齐委托价、均价、成交价等订单元数据
- **账户同步准确性**：同步子账户现金、可用/冻结资金与持仓到 `stock` subportfolio，保留持仓 `buy_time/last_buy_time`，优化买入时间处理
- **日志与输出稳定性**：减少同步持仓日志刷屏，`backtest ... --auto-report` 不再覆盖默认 `report.html`

### 增强
- **QMT / 远程适配兼容性**：`QmtDataAdapter` 兼容 `start/end` 与 `start_date/end_date` 字段名；MiniQMT 增强证券类型归一化与基金别名支持；远程 `security_info` 支持扁平化响应
- **下单能力扩展**：新增 `MarketOrderStyle` / `LimitOrderStyle` 支持，并为 broker 适配层补充更多调试信息与订单链路校验
- **回测报告展示**：总资产图新增超额资产叠加曲线，月度/日历视图更紧凑，日志支持紧凑格式并隐藏具体时间戳
- **依赖补全**：补充缺失运行依赖，减少新环境安装遗漏

### 文档
- **文档重整与修正**：更新 `quickstart`、`config`、`live`、`backtest`、`qmt-server` 等文档，补充风控配置、CLI 行为与使用说明，并修正文档错误

### 测试
- **回归覆盖扩大**：新增/补强 runtime probe、live engine、QMT broker/server adapter、撤单风控、报告渲染、CLI `--auto-report`、证券信息兼容等测试

## [0.6.6] - 2026-02-12

### 新增
- **交易日历守卫诊断日志**：`TradingCalendarGuard` 新增非交易日原因与异常诊断日志（含 `reason/query/sample_days` 等上下文），并加入 300 秒日志节流，减少重复刷屏

### 修复
- **attribute_history 时间边界对齐**：对齐聚宽语义，`1d` 日线默认排除当天，`1m` 分钟线默认包含当前分钟，避免历史窗口错位
- **交易日判定鲁棒性**：`TradingCalendarGuard` 对 `get_trade_days` 的空 DataFrame、`None`、空列表、不可迭代与异常返回做了显式处理，交易日确认更稳定
- **远程券商下单参数兼容**：`RemoteQmtBroker.buy/sell` 的 `remark` 参数改为可选默认 `None`，减少调用侧参数不匹配

### 增强
- **future guard 语义优化**：`get_price/get_trade_days` 统一使用 `_should_avoid_future()` 判定，仅在非 live 场景启用 `avoid_future_data`，避免实盘被回测式未来数据校验误拦截

### 测试
- 新增 `test_attribute_history_alignment.py`，覆盖日线/分钟线 `attribute_history` 结束时间对齐
- 新增 `test_data_api_live_boundaries.py`，覆盖 live/backtest 下 `get_trade_days/get_price` 的 `avoid_future_data` 边界行为

## [0.6.5] - 2026-02-09

### 新增
- **券商全量委托查询**：`get_orders` 新增 `from_broker` 参数。默认行为保持不变；当 `from_broker=True` 时，可直接返回券商侧全量订单（包含人工下单或外部系统下单），方便策略对账与接管（#25）
- **远程 helper 同步支持**：`helpers/bullet_trade_jq_remote_helper.py` 的 `get_orders` 同步支持 `from_broker` 并透传到远程服务，聚宽远程运行场景可直接查看券商全量委托（#25）

### 修复
- **远程历史数据字段过滤失效**：`QmtDataAdapter.get_history` 已透传 `fields` 到 `provider.get_price`，远程 `data.history` 现在可按需返回字段，避免字段不一致并减少无关数据返回（#24）
- **非交易日启动稳定性**：实盘引擎新增交易日日历重试等待机制，周末/节假日/盘前启动时不再因日历尚未就绪而误判流程

### 文档
- **开发模式生效说明**：补充“为什么 Jupyter 改了源码能生效但 CLI 不生效”的排查指引，明确 `pip install -e ".[dev]"` 与重启进程/Kernel 的必要性（#25）

### 测试
- 新增 `get_orders(from_broker=True)` 的实盘引擎覆盖用例，验证“默认行为不变 + 可选券商全量”两种路径（#25）
- 新增远程 helper 的 `from_broker` 参数透传测试，确保请求链路完整（#25）
- 补充 `get_history(fields=...)` 参数透传回归测试（#24）

## [0.6.4] - 2026-01-28

### 新增
- **订单备注/策略名**：下单支持 `remark`，新增 `STRATEGY_NAME`，订单与事件携带策略名与备注，便于多策略区分
- **订单字段完善**：`Order` 增加已成交数量 `filled`、买卖方向 `is_buy`，并区分委托价与成交均价；`Order.extra` 补充 `order_remark/strategy_name/order_price`

### 修复
- **远程 QMT get_price datetime 支持**：`start_date/end_date/pre_factor_ref_date` 支持 `datetime`，修复 JSON 序列化异常（#23）
- **分钟级频率时间截断**：`1m/5m` 等分钟级频率保留时分秒，避免被误处理为日线
- **复权参考日期透传**：`pre_factor_ref_date` 透传到远程 `data.history` 处理链路

### 增强
- **远程握手协议提示**：服务端握手返回协议版本，聚宽 helper 每次连接检测版本差异并提示升级

### 文档
- **聚宽 helper 升级提示**：请务必升级到最新版帮助文件，参见 `https://github.com/BulletTrade/bullet-trade/blob/main/helpers/bullet_trade_jq_remote_helper.py`

### 测试
- **订单与助手用例**：新增订单备注/订单查询相关测试与 helper 告警输出测试

## [0.6.3] - 2026-01-25

### 修复
- **周/月任务重启重复**：实盘重启后沿用策略起始日计算周/月序号，避免 `force=True` 因重启重复触发
- **旧运行态兼容**：缺失起始日字段时自动补写，不影响历史 runtime 加载

### 测试
- **起始日恢复用例**：新增策略起始日持久化/恢复与旧运行态补写测试

## [0.6.2] - 2026-01-25

### 修复
- **实盘交易日判定**：交易日列表为 list/可迭代时也校验是否包含当天，避免节假日/盘前误判导致任务异常触发
- **周/月度任务重复**：交易日历缺当天时不再回退为单日历，避免 `force=True` 导致周/月任务连续触发

### 增强
- **实盘缓存强制关闭**：覆盖所有数据源入口，确保实盘模式禁用磁盘缓存，降低延迟与 IO 风险
- **交易日确认节奏**：`CALENDAR_RETRY_MINUTES` 默认改为 1 分钟，盘前更快确认交易日

### 测试
- **交易日守卫用例**：补充 list 返回交易日判定与缺当天日历的调度测试

## [0.6.1] - 2026-01-18

### 新增
- **研究分析模块**：新增因子研究与评估模块，支持因子回测/评价流程
- **数据校验工具**：新增价格精度对比策略与配置，用于核对数据源价格精度一致性
- **当前价限制探测**：新增 current data limit probe 策略与环境配置，辅助验证涨跌停/价格限制基线
- **订单与成交查询**：券商类新增订单/成交查询接口，便于实盘状态核对

### 修复
- **JQData 前复权价格异常**：引入 `force_no_engine` 选项并适配数据源，支持绕过引擎复权问题（#19）
- **MiniQMT 指数成分获取**：修复/增强 MiniQMTProvider 并补充相关测试（#13）

### 增强
- **行情取价稳定性**：价格获取逻辑强化（含未来数据限制与空数据处理），提升回测稳定性与兼容性
- **涨跌停规则兜底**：新增证券涨跌停规则配置与回退逻辑，异常行情处理更稳
- **限制探测日志**：current data limit probe 的参考与日志更清晰，便于排查
- **远程实盘助手可观测性**：`bullet_trade_jq_remote_helper` 日志与调试能力增强

### 测试
- **远程订单状态**：完善远程服务订单状态断言与 stub 警告输出测试
- **行情校验与限制**：补充价格精度对比、limit probe、MiniQMT 指数成分相关测试

## [0.6.0] - 2026-01-01 新年快乐~

### 新增
- **交易日历驱动调度**：`run_weekly/run_monthly` 支持按“当周/当月第 N 个交易日”（含倒数）触发，新增 `reference_security/force` 控制参考标的与起算方式；引入全局交易日历构建/缓存（同步与异步调度共用），`generate_daily_schedule`/异步调度器按日预生成任务表，回测/实盘按真实交易日节奏执行

### 增强
- **数据源直连与 SDK 回退**：`get_data_provider("name")` 可直接拿到指定数据源实例（不修改默认数据源），按名称缓存实例与认证状态；当 provider 未实现方法时，同源 SDK 自动兜底（JQData→`jqdatasdk`、Tushare→`pro_api`、MiniQMT→`xtquant.xtdata`），并补充示例策略、使用指引与测试用例
- **配置与文档**：`.env` 示例新增 `MINIQMT_MARKET` 以明确交易日市场代码，清理未使用的 `CACHE_TTL_DAYS/BACKTEST_OUTPUT_DIR/DEBUG` 等字段；`config.md` 与 env loader 及对应单测同步更新，避免混淆配置

### 修复
- **MiniQMT 停牌判断准确性**：停牌逻辑改为按状态码白名单判定（1/17/20 等停牌、区分休市/集合竞价/波动性中断），避免午休、集合竞价被误判为停牌

---

## [0.5.8] - 2025-12-29




## [0.5.8] - 2025-12-29

### 修复
- **环境变量刷新失效（Fixes #7）**：支持 `.env`/环境变量更新后重载数据源与日志配置，避免配置更新不生效

### 新增
- **数据接口扩展**：新增 `get_bars/get_ticks/get_current_tick`，支持 `dt/df` 参数与回测兜底；`get_security_info/get_all_securities` 支持按日期查询历史口径
- **订单查询能力**：`QmtBroker` 新增 `get_open_orders`，仅返回未完成订单并保留原始状态字段

### 增强
- **交易合规手数规则统一**：下单数量按最小手数+步进取整，买入向下取整、卖出不足最小手数可按可卖余量收尾；不足最小手数直接拒单并返回规则明细
- **普通股票/创业板规则**：未命中前缀/市场规则时走默认规则，最小 100 股、步进 100
- **科创板规则**：`68` 前缀最小 200 股、步进 1；**北交所**：BJ/BSE 市场最小 100 股、步进 1；**可转债**：`110/113/118/123/127/128` 前缀最小 10 张、步进 10
- **市价单合规保护价**：服务端统一计算保护价并裁剪至价格笼子/涨跌停；主板/创业板按 2%/98% + 十档，科创板按 2%/98%，北交所按 5% 或 ±0.1 约束；ETF/B 股最小价差 0.001，A 股按价格分 0.01/0.001
- **Tushare 复权与数据对齐**：事件复权补齐（分红送转）、交易日对齐与最新交易日兜底，因子缺失自动回退事件复权
- **QMT 稳定性与可观测性**：请求超时控制、会话日志完善，QMT 同步调用独立线程池避免阻塞
- **序列化与回测稳定**：QMT dataframe payload 支持 Timestamp/NaT 序列化；回测日报文件动态定位并给出缺失提示

### 测试
- 新增订单手数规则、open orders、Tushare 复权、QMT payload 序列化、数据源一致性等单元/端到端测试

---

## [0.5.7] - 2025-12-26

### 新增
- **JoinQuant 远程实盘交易支持**：新增 `04.joinquant_remote_live_trade.ipynb` Notebook，支持通过 JoinQuant 平台进行远程实盘交易
  - 包含远程服务器连接配置（host、port、token）
  - 提供账户查询、持仓查询、限价单下单与撤单等功能
  - 附带完整的使用文档和辅助脚本引用

### 增强
- **订单处理功能优化**：
  - `QmtBrokerAdapter` 增强订单预处理，新增市场状态检查、价格限制检查和数量调整
  - `RemoteOrder` 类新增 `actual_amount` 和 `actual_price` 字段，记录实际成交信息
  - `RemoteBrokerClient` 订单方法优化，服务端统一处理 100 股取整、停牌检查和价格验证
  - 完善订单方法的文档说明，明确服务端处理逻辑和预期行为

- **TushareProvider 数据源增强**：
  - 新增 Tushare 与 JoinQuant 代码后缀映射字典
  - 实现 `_to_ts_code` 和 `_to_jq_code` 代码转换方法
  - 数据获取方法支持两种代码格式，提升兼容性
  - 优化分红和基金分配数据的解析与验证逻辑

- **持仓数据处理重构**：
  - `RemoteBrokerClient` 持仓数据解析优化，引入独立变量处理 amount、available 和 frozen 值
  - 增强可用/冻结数量的判断逻辑，支持多个可能的服务端响应字段
  - 简化 `RemotePosition` 实例创建逻辑，提升代码可读性和可维护性

### 测试
- 新增 Tushare 价格获取的单元测试，确保功能与数据完整性

---

## [0.5.6] - 2025-12-22

### 修复
- **远程交易助手价格获取问题**：修复 `bullet_trade_jq_remote_helper` 获取价格返回为空的问题（Fixes #4）
  - 添加调试信息，便于排查价格获取失败的原因

### 增强
- **MiniQMTProvider 错误处理优化**：
  - 为数据获取过程添加详细日志记录，捕获参数和错误信息
  - 改进 `_fetch_local_data` 方法，处理缺失 'time' 列等异常情况
  - 完善方法文档字符串，明确参数和返回类型说明

- **QmtDataAdapter 日志增强**：
  - 为 `get_history` 和 `get_snapshot` 方法添加详细日志记录
  - 记录详细的错误信息和请求参数，便于问题排查
  - 改进文档说明，提升代码可读性

---

## [0.5.5] - 2025-12-21

### 修复
- **TushareProvider 类拼写错误修复**：修复 `TushareProvider` 类中的拼写错误（Fixes #5）

### 文档
- **Tushare 配置文档增强**：
  - 增强 Tushare 自定义 URL 配置的文档说明
  - 更新 `docs/config.md` 和 `docs/data/DATA_PROVIDER_TUSHARE.md` 中的相关配置说明

---

## [0.5.4] - 2025-12-21

### 修复
- **碎股卖出问题修复**：修复碎股无法卖出的问题（Fixes #6）
  - 优化 `bullet_trade/core/engine.py` 中的订单处理逻辑，支持碎股（不足100股的股票）的正常卖出

### 致谢
- 感谢 [Sheng Li](https://github.com/mrlouisleel) 贡献的碎股卖出问题修复（PR #6）

---

## [0.5.3] - 2025-12-21

### 新增
- **Tushare 自定义 API URL 支持**：添加对自定义 Tushare API URL 的支持（Fixes #5）
  - 更新环境变量加载器，支持 `TUSHARE_API_URL` 配置
  - 更新示例配置文件 `env.backtest.example`，添加相关配置说明
  - 允许用户自定义 Tushare API 服务地址，提升灵活性

### 增强
- **回测引擎价格获取逻辑重构**：
  - 重构 `BacktestEngine` 中的价格获取逻辑，支持长格式数据框（包含 'code' 和 'close' 列）
  - 增强错误日志记录，当价格数据缺失或列不匹配时提供更详细的错误信息
  - 优化 `JQDataProvider` 的日志级别，将价格引擎回退检测从 info 调整为 debug

### 致谢
- 感谢 [Vanilla_Yukirin](https://github.com/Vanilla_Yukirin) 贡献的 Tushare 自定义 API URL 支持功能（PR #5）

---

## [0.5.2] - 2025-12-10

### 新增
- **交易成本管理功能**：
  - 新增 `set_commission` 函数，支持设置交易佣金
  - 新增 `set_universe` 函数，支持管理资产池
  - `OrderCost` 类新增 `commission_type` 属性，提供更灵活的费率管理

- **滑点类型扩展**：
  - 新增 `PriceRelatedSlippage`（价格相关滑点）类型
  - 新增 `StepRelatedSlippage`（阶梯相关滑点）类型
  - 保留现有的 `FixedSlippage`（固定滑点）类型

### 增强
- **交易引擎优化**：
  - 优化 `BacktestEngine` 和 `LiveEngine` 中的交易设置处理逻辑
  - 增强交易成本计算的准确性和灵活性
  - 改进滑点处理机制，支持多种滑点模型

- **文档和测试**：
  - 更新 API 文档，添加新功能的使用示例
  - 新增滑点行为测试（`tests/core/test_slippage.py`）
  - 增强订单成本测试（`tests/unit/test_order_costs.py`），确保功能正确性

---

## [未发布]

### 计划中
- 更多功能改进...

---

## 版本说明

- **新增**：新功能
- **增强**：现有功能的改进
- **修复**：Bug 修复
- **变更**：破坏性变更或重要行为变更
- **废弃**：即将移除的功能
- **移除**：已移除的功能
- **安全**：安全相关的修复

[0.5.8]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.7...v0.5.8
[0.5.7]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.1...v0.5.2
