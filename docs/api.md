# API 文档

本页基于 BulletTrade 实现整理，设计遵循聚宽风格，但只列出我们真实提供的 API/对象；聚宽有而本项目未实现的接口不会出现在本页。导入方式保持聚宽兼容：`from jqdata import *` 或 `from bullet_trade.compat.api import *`。

## 目录
- [策略入口与全局对象](#策略入口与全局对象)
- [策略生命周期函数](#策略生命周期函数)
- [调度与时间表达式](#调度与时间表达式)
- [风控与设置](#风控与设置)
- [数据接口](#数据接口)
- [订单与组合](#订单与组合)
- [行情订阅（Tick）](#行情订阅tick)
- [数据模型](#数据模型)
- [工具函数与消息](#工具函数与消息)
- [CLI 衔接](#cli-衔接)

## 策略入口与全局对象
- `g`：全局状态容器，可挂载任意可序列化属性（如 `g.target_ratio=0.2`）。回测开始会重置；实盘可使用 `g.live_trade=True` 标记实盘模式。
- `log`：日志对象，支持 `debug/info/warn/error/critical`；`log.set_level(module, level)` 兼容聚宽（`module` 取 `system/strategy`，`strategy` 会调整实际输出级别）。
- 常用模块别名：`datetime/math/random/time/np/pd` 已自动导出，无需额外导入。
- 消息通知：
  - `send_msg(message)`：输出 `[策略消息] ...` 日志，并在存在自定义 handler 时调用；若 `.env` 配置 `MESSAGE_KEY` 或 `WECHAT_MESSAGE_KEY`，会通过企业微信机器人发送（失败仅记录日志，不抛错）。
  - `set_message_handler(handler)`：注册自定义处理函数（例如推送到 IM/邮件），传入 `None` 可清除。

## 策略生命周期函数
- `initialize(context)`：回测/实盘启动时调用，用于注册调度、设置基准等。若 Live 模式检测到已有持久化运行时且元数据可恢复，可能跳过重复执行（保持与聚宽一致行为）。
- `process_initialize(context)`：仅 Live 模式，会在初始化后、券商/账户连接就绪时调用，可放置需要实盘环境的准备逻辑（如订阅账号、同步持仓）。
- `before_trading_start(context)` / `after_trading_end(context)`：分别在开盘前、收盘后调用（回测与 Live 均支持），可与调度 `run_daily` 搭配。
- `handle_data(context, data)`：每个 bar 调用（按回测频率或 Live 调度）；`data` 为行情数据容器。
- `after_code_changed(context)`：仅 Live 模式，当检测到策略文件内容相较上次运行发生变化且存在历史元数据时触发，用于代码热更新后的修复/迁移逻辑（例如调整全局变量、重新注册任务）。回测不会调用。
- 其他聚宽钩子（如 `before_trading_start` 以外的扩展）未实现即不会触发。

## 调度与时间表达式
支持与聚宽一致的定时接口，时间表达式已扩展了常用别名：
- `run_daily(func, time='every_bar')`：日调度。`every_bar` 在分钟回测每分钟执行，在日回测只在开盘触发；`every_minute` 强制每分钟；`'HH:MM'`/`'HH:MM:SS'` 为指定时刻；相对表达式 `open`/`open-30m`/`close+10s`；内置别名 `before_open=open-30m`、`after_close=close+30m`、`morning=08:00`、`night=20:00`。
- `run_weekly(func, weekday, time='09:30')`：`weekday` 0-6（周一-周日），时间表达式同上。
- `run_monthly(func, monthday, time='09:30')`：每月固定日运行；若当月该日非交易日则顺延到后一个交易日首次触发。
- `unschedule_all()`：清空所有已注册任务。
- 差异提示：`every_bar` 在日频回测只触发一次（开盘），与聚宽相同；我们额外允许通过 `.env` 或 `set_option('time_aliases', {...})` 覆写别名。

## 风控与设置
- `set_benchmark(security)`：设置基准标的（如 `000300.XSHG`），用于报告与回测对比。
- `set_option(key, value)`：行为开关（仅支持列举项）：
  - `use_real_price`：开启真实价格/动态复权模式，回测按当日复权因子换算成交；默认 `False`。
  - `avoid_future_data`：避免未来数据，盘中请求当日 `close/high/low` 等会抛 `FutureDataError`；默认 `False`。
  - `order_volume_ratio`：撮合时允许的最大成交量占比（0-1）。
  - `order_match_mode`：撮合时机，`'immediate'`（默认，创建订单即处理）或 `'bar_end'`（等到 bar 结束统一撮合）。
  - `match_by_signal`：限价单资金检查使用信号价(`True`)或撮合价(`False`，默认)。
  - `market_period` / `time_aliases`：可传入自定义交易时段或别名列表，影响调度时间解析。
- `set_slippage(FixedSlippage(value))`：设置固定滑点；内部按双边一半计算（value=0.002 -> 买入+0.001，卖出-0.001）。
- `set_order_cost(OrderCost(...), type='stock')`：设置手续费/印花税，按资产类别覆盖。默认 `stock` 买入佣金万三、卖出佣金万三+千分之一印花税、最小佣金 5 元；`fund/money_market_fund` 默认免印花税。
- `set_data_provider(name|instance, **kwargs)` / `get_data_provider()`：切换或读取当前数据源。内置名称：`jqdata`（默认）、`tushare`、`qmt`/`miniqmt`、`qmt-remote`。切换后自动重新认证并清空缓存。

## 数据接口
- `get_price(security, start_date=None, end_date=None, frequency='daily', fields=None, skip_paused=False, fq='pre', count=None, panel=True, fill_paused=True)`  
  - 支持单标的或列表；`frequency` 可用 `daily/1d/minute/1m`；`fields` 常用 `['open','close','high','low','volume','money','high_limit','low_limit','paused']`。  
  - 回测场景会自动把 `end_date` 限制到当前回测时间；`avoid_future_data=True` 时盘中/盘前取当日未来字段会抛错。  
  - `use_real_price=True` 时，前复权使用当前回测日作为参考，贴近真实盘口。  
  - 多标的返回列为 `MultiIndex(field, code)`，保证 `df['close']` 可直接取出二维矩阵（与聚宽一致但转换逻辑为我们实现）。
- `attribute_history(security, count, unit='1d', fields=None, skip_paused=False, df=True, fq='pre')`  
  回测时会将 `end_date` 向后/前偏移一单位以避免未来数据（分钟多取 1 分钟，日线往前 1 日），底层调用 `get_price`。
- `get_current_data()`  
  返回延迟加载的行情容器：`current_data[code].last_price/high_limit/low_limit/paused`。实盘优先走数据源实时接口或 tick，无法获取实时则回退到 `get_price`。不支持聚宽的分级属性访问（如 `current_data[code].day_open`），仅提供上述字段。
- `get_trade_days(start_date=None, end_date=None, count=None)`：返回交易日列表；在回测中 `end_date` 会被截断到当前时间。
- `get_all_securities(types='stock', date=None)`：返回指定类型的标的信息（DataFrame）；回测默认取当前回测日。
- `get_index_stocks(index_symbol, date=None)`：返回成分股列表；回测默认取当前回测日。
- `get_split_dividend(security, start_date=None, end_date=None)`：统一结构的分红/拆分事件列表，每项包含 `security/date/security_type/scale_factor/bonus_pre_tax/per_base`。非回测场景需要显式提供起止日期；与聚宽不同，我们的结果字段固定为上述键。
- 未提供聚宽的基础面/财务等查询接口（如 `get_fundamentals`），请勿依赖。

## 订单与组合
- 下单函数（与聚宽同名，未实现撤单/批量成交等扩展功能）：  
  - `order(security, amount, price=None, style=None, wait_timeout=None)`：按股数下单；`amount>0` 买入，`<0` 卖出；`price` 或 `style=LimitOrderStyle(price)` 为限价，否则市价。  
  - `order_value(security, value, price=None, style=None, wait_timeout=None)`：按金额下单，数量在撮合时根据价差计算。  
  - `order_target(security, amount, price=None, style=None, wait_timeout=None)`：将持仓调整到目标股数。  
  - `order_target_value(security, value, price=None, style=None, wait_timeout=None)`：将持仓调整到目标市值。  
  - 默认 `set_option('order_match_mode', 'immediate')` 时创建即撮合；否则在 bar 结束批量撮合。
- 价格样式：`MarketOrderStyle(limit_price=None, buy_price_percent=None, sell_price_percent=None)`（市价，可选保护价或价差系数）；`LimitOrderStyle(price)`（限价）。
- 成本/滑点：`OrderCost`、`FixedSlippage` 同上。

## 行情订阅（Tick）
- `subscribe(security|[...], frequency='tick')`：注册 tick 订阅；支持单个代码或列表，也支持市场全量 `['SH','SZ']`。实盘优先通过引擎/券商订阅；无实盘时尝试本地 `xtdata`（可用则订阅）。限制：仅接受 `frequency='tick'`；模拟模式单策略最多订阅 100 个标的；禁止订阅期货主力/指数合约（如 `RB9999.XSGE`、`IF00.CFFEX`）。
- `unsubscribe(security|[...], frequency='tick')`、`unsubscribe_all()`：取消订阅。
- `get_current_tick(security)`：返回最简快照 `{'sid': code, 'last_price': price, 'dt': ts}`，若无数据返回 `None`。
- 差异提示：只提供最小实现；没有聚宽的 `get_ticks`、盘口十档等高级字段。若策略定义了 `handle_tick(context, tick)`，系统会自动绑定为回调。

## 数据模型
- `Context`：`portfolio/current_dt/previous_dt/previous_date/run_params/subportfolios`，策略函数 `initialize/handle_data` 等默认接收此对象。
- `Portfolio` / `SubPortfolio`：账户与子账户信息，含 `total_value/available_cash/locked_cash/positions` 等；`positions` 为 `{code: Position}`。
- `Position`：持仓详情，含 `total_amount/closeable_amount/avg_cost/price/value/side` 等，并记录 `today_buy_t1`（T+1 可用数量）。
- `Order` / `Trade`：委托与成交记录；`OrderStatus`/`OrderStyle` 为枚举；`SecurityUnitData` 为 `current_data` 单标的快照。

## 工具函数与消息
- `print_portfolio_info(context, top_n=None)`：打印账户收益、现金、前 N 大持仓（按市值排序），样式对齐聚宽 CLI。  
  ![print_portfolio_info](assets/print_portfolio_info.png)
- `prettytable_print_df(df, headers='keys', show_index=False, max_rows=50)`：以表格方式输出 DataFrame，便于日志查阅；无 `tabulate` 时退回纯文本表格。
- 消息函数见「策略入口与全局对象」中的 `send_msg/set_message_handler`，常用于推送回测/实盘告警。

## CLI 衔接
- `bullet-trade --help`：查看回测、实盘、server、report 子命令。
- 回测：`bullet-trade backtest ...`；实盘：`bullet-trade live ...`；远程服务：`bullet-trade server ...`；报告：`bullet-trade report ...`。

更多细节或新增 API 需求，欢迎在 [邀请贡献](contributing.md) 中提到的渠道提出。
