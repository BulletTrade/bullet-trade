# Tick 订阅与行情接收指南

面向实盘/远程使用者的 Tick 订阅说明，以及面向回测的历史逐笔回放说明；覆盖订阅、接收、取消、tick 频率回测与当前实现限制。

## Tick 订阅架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Tick 订阅数据流架构                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  场景 A: 本地 xtdata 订阅（Windows + miniQMT 环境）                            │
│                                                                             │
│  ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐   │
│  │   策略代码        │      │   bullet-trade    │     │   miniQMT        │   │
│  │                  │      │   core/api.py    │      │   xtquant SDK    │   │
│  │  subscribe(      │      │                  │      │                  │   │
│  │   ['000001'],    │─────▶│  subscribe()     │─────▶│ xtdata.subscribe │   │
│  │   'tick'         │      │                  │      │ _quote()         │   │
│  │  )               │      │                  │      │                  │   │
│  └──────────────────┘      └──────────────────┘      └────────┬─────────┘   │
│                                                               │             │
│                                                   实时推送 tick│             │
│                                                               ▼             │
│  ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐   │
│  │   handle_tick    │◀─────│   _on_xt_tick    │◀─────│ callback 回调    │   │
│  │   (context,tick) │      │   字段映射转换     │      │ {lastPrice,time} │   │
│  │                  │      │   sid/last_price │      │                  │   │
│  └──────────────────┘      └──────────────────┘      └──────────────────┘   │
│                                                                             │
│  特点：延迟最低（< 100ms），支持全市场订阅 ['SH','SZ']                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  场景 B: LiveEngine + qmt-remote 远程订阅（macOS/Linux 客户端）                 │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  客户端 (macOS/Linux)                                                 │   │
│  │                                                                      │   │
│  │  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────┐  │   │
│  │  │  策略代码     │    │  LiveEngine       │    │ RemoteQmtBroker   │  │   │
│  │  │              │    │                  │    │ RemoteQmtProvider  │  │   │
│  │  │  subscribe(  │───▶│ register_tick_   │───▶│                    │  │   │
│  │  │   [...],     │    │ subscription()   │    │ 记录订阅清单         │  │   │
│  │  │   'tick'     │    │                  │    │                    │  │   │
│  │  │  )           │    │                  │    │                    │  │   │
│  │  └──────────────┘    └──────────────────┘    └────────────────────┘  │   │
│  │                              │                         │             │   │
│  │                   tick_sync_interval                   │             │   │
│  │                   (默认 2 秒轮询)                        │             │   │
│  │                              │                         │             │   │
│  │                              ▼                         │             │   │
│  │                      ┌──────────────────┐              │             │   │
│  │                      │  _tick_loop()    │              │             │   │
│  │                      │  定时拉取快照     │───────────────┘             │   │
│  │                      └────────┬─────────┘                            │   │
│  │                               │                                      │   │
│  │                               │ request("data.snapshot")             │   │
│  └───────────────────────────────┼──────────────────────────────────────┘   │
│                                  │                                          │
│                          ════════╪════════  网络 ════════════════            │
│                                  │                                          │
│  ┌───────────────────────────────┼──────────────────────────────────────┐   │
│  │  服务端 (Windows + miniQMT)    │                                      │   │
│  │                               ▼                                      │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  bullet-trade server                                           │  │   │
│  │  │                                                                │  │   │
│  │  │  ┌────────────────┐       ┌────────────────┐                   │  │   │
│  │  │  │ QmtDataAdapter │◀──────│ data.snapshot  │◀── 客户端请求       │  │   │
│  │  │  │                │       │ 处理器          │                   │  │   │
│  │  │  └───────┬────────┘       └────────────────┘                   │  │   │
│  │  │          │                                                     │  │   │
│  │  │          │ get_current_tick()                                  │  │   │
│  │  │          ▼                                                     │  │   │
│  │  │  ┌────────────────┐                                            │  │   │
│  │  │  │ xtdata.get_    │                                            │  │   │
│  │  │  │ full_tick()    │                                            │  │   │
│  │  │  └───────┬────────┘                                            │  │   │
│  │  │          │                                                     │  │   │
│  │  │          ▼                                                     │  │   │
│  │  │  ┌────────────────┐                                            │  │   │
│  │  │  │ miniQMT        │  实时 tick 数据源                            │  │   │
│  │  │  │ xtquant SDK    │  {lastPrice, time, bidPrice, askPrice...}  │  │   │
│  │  │  └────────────────┘                                            │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  特点：跨平台支持，延迟 = tick_sync_interval + 网络延迟                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  handle_tick 回调数据格式                                                     │
│                                                                             │
│  def handle_tick(context, tick):                                            │
│      # tick 字典包含以下字段（来源于 miniQMT xtdata 推送）                       │
│      tick = {                                                               │
│          'sid': '000001.XSHE',       # 聚宽风格代码                           │
│          'symbol': '000001.SZ',      # QMT 风格代码                          │
│          'last_price': 10.50,        # 最新价                                │
│          'dt': 1700000000000,        # 时间戳(毫秒)                           │
│          # ── 盘口字段（本地 xtdata 模式可用）──                                │
│          'bid1': 10.49,              # 买一价                                │
│          'ask1': 10.51,              # 卖一价                                │
│          'bid1_volume': 1000,        # 买一量                                │
│          'ask1_volume': 800,         # 卖一量                                │
│          'last_close': 10.30,        # 昨收                                  │
│          'open': 10.35,              # 开盘价                                │
│          'high': 10.55,              # 最高价                                │
│          'low': 10.28,               # 最低价                                │
│          'volume': 50000,            # 成交量                                │
│          'amount': 525000.0,         # 成交额                                │
│      }                                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

底层实现主要有两条链路：

- **LiveEngine + qmt-remote（默认）**：`subscribe(...)` 只记录订阅清单，后台按 `tick_sync_interval`（默认 2 秒）轮询券商的 `get_current_tick`。对于远程场景，该调用会通过 `RemoteQmtBroker -> RemoteQmtConnection` 请求 server 的 `data.snapshot`，并在收到后触发策略的 `handle_tick`。
- **本地 xtdata（无 LiveEngine 或设置了 xtdata 数据源）**：`subscribe(...)` 会直接调用 `xtdata.subscribe_quote/subscribe_whole_quote` 由 xtquant 推送，回调 `_on_xt_tick` 统一成 `sid/last_price/dt` 后传给策略的 `handle_tick`。
- **Server 端 data.subscribe**：`bullet-trade server` 内部的 `TickSubscriptionManager` 每隔 1 秒轮询数据适配器的 `get_current_tick`，将结果打包成事件 `{"symbol": "000001.SZ", "sid": "000001.XSHE", "last_price": 10.1, "dt": ...}` 推给远程客户端。当前 LiveEngine 的 qmt-remote 券商并不消费该推送，而是采用上面的轮询模式。

### Tick 负载里有哪些字段？
- 始终有 `sid`（聚宽风格代码，例如 `000001.XSHE`）和 `last_price`、`dt`（时间字符串）。  
- xtdata 推送时原始字段是 `stock_code`/`code` + `lastPrice`/`time`，框架会映射成上述键；server 推送时会额外带 `symbol`（QMT 风格代码，如 `000001.SZ`）。  
- **本地 xtdata 订阅** 会尽量保留盘口与昨收等字段：`bid1/ask1/bid1_volume/ask1_volume/last_close/open/high/low/volume/amount/limit_up/limit_down`，命中即写入 `tick`；五档以上暂未展开。  
- 远程 qmt-remote 目前仍只有基础快照（不含盘口深度）。

## 快速开始：订阅部分标的
1. 准备 `.env.live`（或通过 `--env-file` 指定），关键项：  
   `DEFAULT_BROKER=qmt-remote`、`QMT_SERVER_HOST/PORT/TOKEN`，必要时 `QMT_SERVER_ACCOUNT_KEY`、`QMT_SERVER_SUB_ACCOUNT`、`QMT_SERVER_TLS_CERT`。  
   服务端需已通过 `bullet-trade server --enable-data --enable-broker` 启动。
2. 在策略中声明订阅并实现回调：
   ```python
    import datetime as dt
    from jqdata import *


    def initialize(context):
        pass


    def process_initialize(context):
        subscribe(['000001.XSHE', '000002.XSHE','232592.XSHG','104749.SZ'], 'tick')


    def handle_tick(context, tick):
        """
        假定 tick 是 dict，形如 {code: [payload,...]} 或 {code: payload}。
        只提取最常用字段并打印当前时间与 tick 时间的延迟。
        """
        now = dt.datetime.now()
        if not isinstance(tick, dict):
            log.info(tick)
            return

        for code, payload in tick.items():
            entry = payload[0] if isinstance(payload, (list, tuple)) and payload else payload
            if not isinstance(entry, dict):
                log.info(f"code={code} tick={entry}")
                continue
            ts_raw = entry.get('time') or entry.get('datetime')
            delay = ""
            if isinstance(ts_raw, (int, float)):
                try:
                    delay_val = (now - dt.datetime.fromtimestamp(ts_raw / 1000)).total_seconds()
                    delay = f"[delay=+{delay_val:.3f}s] "
                except Exception:
                    delay = ""
            log.info(
                f"{delay}code={code} last={entry.get('lastPrice')} "
                f"bid={entry.get('bidPrice')} ask={entry.get('askPrice')} "
                f"vol={entry.get('volume')} amt={entry.get('amount')} ts={ts_raw}"
            )

   ```
3. 启动实盘：  
   `bullet-trade live strategies/demo.py --broker qmt-remote --env-file .env.live`
4. 运行中可随时调用 `unsubscribe(['000001.XSHE'], 'tick')` 或 `unsubscribe_all()` 取消。

能从图上看到我们测试环境tick数据的延迟情况
![delay](assets/tick-delay.png)


> 轮询模式意味着订阅数量越多，对 server 的请求压力越大；请留意 `tick_subscription_limit`（默认 100）和 `tick_sync_interval`。

### 重启 / 热更新后的订阅要点
- LiveEngine 会把订阅清单持久化，但券商侧不会自动恢复订阅。若第二次启动时跳过了 `initialize`，订阅不会自动重绑。
- 建议在 `process_initialize`（Live 独有）或 `after_code_changed` 中再调用一次 `subscribe(...)` 以确保重启/热更新后仍然生效；也可在 `initialize` 里保留调用以兼容回测。

## 全市场订阅
- 本地 Windows + xtquant 场景可用：`subscribe(['SH', 'SZ'], 'tick')` 会调用 `xtdata.subscribe_whole_quote`，推送经 `_on_xt_tick` 转发后进入 `handle_tick`。
- **远程 qmt-remote 暂不支持全市场订阅**：server 端未实现 `allow_full_market` 开关，`RemoteQmtBroker` 也未对接推送订阅；需要全市场时只能在本地 xtdata 运行或自行枚举标的列表。

## 取消与查询
- `unsubscribe([...], 'tick')`：取消部分订阅；`unsubscribe_all()`：全部取消。对远程模式会减少 LiveEngine 轮询列表。
- `get_current_tick('000001.XSHE')`：即时拉取一次快照（同样经 server 的 `data.snapshot` 拉取），不依赖订阅状态，可在调试时验证连通性。

## tick 频率回测（历史逐笔回放）

回测引擎支持 `frequency='tick'`，用历史逐笔数据驱动策略，事件语义与实盘 tick 投递保持一致。

```python
from bullet_trade.core.engine import BacktestEngine

engine = BacktestEngine(
    strategy_file="strategy.py",
    start_date="2024-01-02",
    end_date="2024-01-31",
    frequency="tick",          # 也接受 "ticks"
    initial_cash=1_000_000,
)
result = engine.run()
```

CLI 的 `--frequency` 目前只接受 `day` / `minute`，tick 回测需通过 Python 入口启动。

### 事件模型

- 调度时点与逐笔事件合并进**同一条时间轴**，按数据源的原始 float64 时间戳排序后依次执行，不做二次换算，避免同一秒内多笔 tick 顺序漂移。
- `before_trading_start` / `handle_data` / `after_trading_end` 只在**非 tick 事件**的锚点时刻触发。首笔或末笔 tick 可能正好落在这些时刻，加这层条件才不会重复触发。
- 每笔 tick 触发一次 `handle_tick(context, tick)`；`tick` 为 `TickSnapshot`，字段与实盘快照一致（`current` / `high` / `low` / `volume` / `money` / `position` / 买卖一档价量 / `datetime` / 原始 `time`）。
- 策略侧读取当前快照用 `get_current_tick(code)`，返回回放缓冲里的最新一笔，不是实时行情。
- 定时任务在其调度事件上只执行一次。普通同刻事件先执行任务再投递 tick；收盘锚点先投递同刻全部 tick，再执行收盘任务。
- 期货持仓在 `handle_tick` 前按已投递价格更新权益；逐笔估值保留结算基准和现金，日期保证金率变化时单独补退保证金。

### 订阅门控

- 只有已 `subscribe(code, 'tick')` 的合约才会投递；未订阅的合约即使有数据也不进入时间轴。
- 盘前（`before_trading_start` 内）订阅当日即生效；**盘中新增订阅不回填**当日更早的 tick，从订阅时刻之后开始投递。
- `unsubscribe(code, 'tick')` 停止单个合约投递，其余合约不受影响；`unsubscribe_all()` 停止全部投递。
- 未订阅合约的数据缺口不会中止回测；已订阅合约缺当日数据才会失败（见下）。

### 数据获取与失败行为

- 按自然日**整段预取**当日全部已订阅合约，落到本地 parquet 缓存（缓存目录由 `DATA_CACHE_DIR` 控制），tick 循环内不再同步访问上游。
- 已订阅合约在某交易日缺 tick 数据时抛 `TickDataMissingError`，错误信息含合约代码与日期。**不会静默降级**为分钟线/日线代理，也**不会回落**到实时行情接口。
- `avoid_future_data=True` 时，策略侧查询仍受未来数据守卫约束，读不到回放时钟之后的数据。引擎的整日预取属内部行为，取数窗口必然覆盖回放时钟之后的时刻——这不构成泄露，因为策略只能看到已推进到的那一笔；守卫只在预取期间暂停，预取结束立即恢复。
- `day_open` 在首笔 tick 投递前的订阅预取阶段单独加载，只请求未复权的当日开盘字段，并在对应合约出现可见 tick 后公开。盘中新订阅不会同步补取日线开盘价；策略读取未预取的 `day_open` 时明确报错，既不默认为零，也不以首笔 tick 价格代替。

### 回放合成行情与代理标识

tick 回放下，`get_current_data()` 只使用已投递的当前日快照，不读取当日完整 bar 或实时行情。同一时间戳的后续 tick 也会更新查询结果。行情带有以下标识：

| 字段 | 值 |
| --- | --- |
| `source` | `"tick_replay"` |
| `source_time` | 该笔快照的回放时刻 |
| `last_price` | 该笔快照的最新价 |
| `bid_price1` / `ask_price1` / `bid_volume1` / `ask_volume1` | 快照买卖一档，缺值为 `None` |

策略可用 `get_current_data()[code].source == "tick_replay"` 判断回放行情来源。合成行情**不含** `high_limit` / `low_limit` / `paused` 等只有 bar 才提供的字段语义，依赖涨跌停或停牌判断的逻辑需自行取数。

撮合基准与市价单保护价都以**当前这笔 tick** 为准：tick 模式下即使 bar 行情容器里有该合约，其最新价也会被切到当前快照，避免 bar 价与 tick 价的价差把市价单误判为越界而取消。

### 订单与时间戳

- 回测下 `Order.add_time` 是**回放时刻**，不是运行回测时的墙钟时间；同一订单只在首次登记时写入，跨时刻未成交的挂单下单时间不会被后移。
- tick 模式按当前快照撮合；没有可见快照时拒单。日线/分钟期货撮合启用成交量约束后可能部分成交，剩余部分取消，订单的 `filled` 保存实际成交手数。

### 期货回放要点

- 日终按**结算价**盯市，盯市变动结转进现金，浮动盈亏每日归零；保证金同步结转到结算价基准。
- 缺当日结算价时记录为 `missing_settlement` 并告警，保持上一基准，**不做替代估值**。
- 合约乘数、最小变动价位与保证金率按显式覆盖、登记、外部配置、数据源及内置规格的优先级解析，日期费率会作用于存量持仓；手续费率需显式配置。缺少可用规格或费率时明确失败。例如：

```python
from bullet_trade.core.settings import OrderCost, set_option, set_order_cost

set_option('futures_margin_rate', 0.14)
set_order_cost(
    OrderCost(open_commission=0.000023, close_commission=0.000023,
              close_today_commission=0.0023, min_commission=0),
    type='futures',
)
```

- 平今费率按**拆分出的今仓手数**计费：`close_today=True` 时整笔按平今；否则 `pindex=1` 先平今仓（今仓手数 = min(平仓手数, 今仓库存)），`pindex=0` 先平昨仓（今仓手数 = 平仓手数 − 昨仓库存，下限 0）。因此当日开的仓即使不传 `close_today=True` 也会落到今仓并按平今费率计费；`Order.close_today` 字段只反映策略的请求，不代表实际计费口径。
- 期货持仓记在期货账本里，`portfolio.positions` 可能为空而 `portfolio.futures_margin` 非零；多空视图用 `portfolio.long_positions` / `short_positions` 读取。
- 日线/分钟期货市价单用引擎内部的当日日 K 检查量价：无有效日 K、日期不符、零成交量或滑点前基准价超出日高低区间时取消并记录原因。每笔订单以完整日成交量为独立上限，不扣除其他订单成交量。这是使用完整日数据的回测撮合假设，不能解释为当时可观测的流动性；这些字段不写入策略当前行情。tick 撮合不使用此规则。限价单沿用当前价穿价判断，尚未实现挂起订单的逐 bar 量能模型。
- 新期货成交的 `side`、`action`、`multiplier` 字段与手续费原精度随导出保留。统计分别跟踪多头和空头，并把开仓手续费按平仓数量分摊。旧成交缺少方向元数据时只支持可确定的纯多头回合，无法判断的记录明确报错。

### 不支持的场景

- `AsyncBacktestEngine`（异步孪生引擎）不支持 tick 频率，以 `frequency='tick'` 启动会在入口显式报错。

## 已知限制与测试现状
- CLI 的 `--frequency` 尚未开放 `tick`，tick 回测只能通过 Python 入口启动。
- tick 撮合尚未模拟盘口深度、排队和逐笔成交量消耗；跨自然日夜盘尚未全面验收。
- 推送链路与 LiveEngine 脱节：远程模式的订阅不会触发 server 端的 tick 推送，而是客户端每隔 `tick_sync_interval` 主动拉取；标的较多时延迟与带宽占用上升。
- 未实现 Throttler/压缩/批量：server 端对订阅数仅做上限校验，没有批量聚合或压缩，极端高频场景需要谨慎。
- 全市场订阅仅限本地 xtdata；`allow_full_market` 配置未在 server 侧生效。
- 自动补价/停牌检测仅在下单时发生，tick 本身不含这些标志。
- 测试覆盖：实盘侧有 LiveEngine 订阅限额与快照读取的单测（见 `tests/core/test_live_engine.py`）；回测侧有 tick 回放事件循环、订阅门控、缺数据失败、未来数据守卫边界与市价单保护价基准的单测（见 `tests/core/test_engine_tick_replay.py`），期货账本与撮合见 `tests/core/test_futures_account.py`、`tests/core/test_engine_futures_matching.py`。远程 server 的 tick 订阅/推送、重连、全市场等场景仍未覆盖，建议实盘前手工验证。
