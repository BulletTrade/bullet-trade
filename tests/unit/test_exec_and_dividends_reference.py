"""
离线/在线参考回测用例说明

用途
- 离线验证撮合与分红框架（无需网络，使用内置 ReferenceProvider 固定数据）。
- 在线对比真实数据源（jqdata/tushare/miniqmt）与“黄金基准”是否一致（可选，需要网络与凭据）。

如何运行（非常明确）
1) 离线基准（默认，无网络）：
   - 运行任一命令：
     - pytest -q bullet-trade/tests/unit/test_exec_and_dividends_reference.py
     - 或仅选离线用例：pytest -q -m unit bullet-trade/tests/unit/test_exec_and_dividends_reference.py
   - 预期：成交与分红严格匹配给定结果（见下面断言）。

2) 在线对比（需要网络）：
   - 直接运行（可指定 Provider 列表）：
     - pytest -q -m requires_network bullet-trade/tests/unit/test_exec_and_dividends_reference.py --live-providers=jqdata
     - 多 Provider：--live-providers=jqdata,tushare,qmt
   - 想看被跳过的详细原因：追加 -rA -s 查看 skip 信息。

额外注意
- 引擎在加载策略时会 reset_settings()，所以 set_slippage / set_order_cost 必须放在 initialize() 内设置，否则会被重置并回落到默认分类滑点（24.6bps）。
"""

from datetime import datetime, date as Date, time as Time
from typing import Optional, Union, List, Dict, Any

import os
import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.settings import set_slippage, FixedSlippage, set_order_cost, OrderCost
from bullet_trade.data import api as data_api
from bullet_trade.data.providers.base import DataProvider


@pytest.fixture(autouse=True)
def _isolate_provider(request):
    """确保默认情况下使用 ReferenceProvider，避免误触全局真实数据源。
    - 对标记了 requires_network 的在线用例不干预，由用例自行切换 Provider。
    - 其他（离线）用例强制将 provider 置为 ReferenceProvider。
    """
    if request.node.get_closest_marker("requires_network") is not None:
        # 在线用例：不修改，由用例内部 set_data_provider()
        yield
        return
    original_provider = data_api._provider  # type: ignore[attr-defined]
    original_auth = data_api._auth_attempted  # type: ignore[attr-defined]
    original_cache = getattr(data_api, "_security_info_cache", {}).copy()  # type: ignore[attr-defined]
    try:
        data_api._provider = ReferenceProvider()  # type: ignore[attr-defined]
        data_api._auth_attempted = True  # type: ignore[attr-defined]
        # 清空标的信息缓存，避免被其他测试/会话污染
        try:
            data_api._security_info_cache.clear()  # type: ignore[attr-defined]
        except Exception:
            pass
        yield
    finally:
        data_api._provider = original_provider  # type: ignore[attr-defined]
        data_api._auth_attempted = original_auth  # type: ignore[attr-defined]
        try:
            data_api._security_info_cache = original_cache  # type: ignore[attr-defined]
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _disable_engine_default_slippage(monkeypatch, request):
    """在离线用例中，彻底禁用引擎的“分类默认滑点”分支，避免任何价差干扰。
    在线用例（requires_network）不修改，以真实规则校验。
    """
    if request.node.get_closest_marker("requires_network") is not None:
        yield
        return

    def _no_default_slip(self, price: float, is_buy: bool, security: str) -> float:
        return price

    monkeypatch.setattr(
        BacktestEngine, "_calc_trade_price_with_default_slippage", _no_default_slip, raising=True
    )
    yield


@pytest.fixture(autouse=True)
def _disable_market_protect(monkeypatch):
    """参考校验场景中关闭市价单保护价，以匹配基准成交记录。"""
    monkeypatch.setenv("MARKET_BUY_PRICE_PERCENT", "0")
    monkeypatch.setenv("MARKET_SELL_PRICE_PERCENT", "0")
    from bullet_trade.core import pricing

    monkeypatch.setattr(pricing, "compute_market_protect_price", lambda *_, **__: None)
    yield


class ReferenceProvider(DataProvider):
    name = "reference"

    def __init__(self):
        # 预置关键时点的价格（未复权）
        # 日线open/close: 仅填用到的时点
        self.daily: Dict[str, Dict[str, Dict[str, float]]] = {
            "511880.XSHG": {
                "2024-07-10": {"open": 100.973, "close": 101.000},
                "2024-12-31": {"open": 100.093, "close": 100.093},
                "2025-01-10": {"open": 100.111, "close": 100.111},
            },
            "601318.XSHG": {
                "2024-07-10": {"open": 41.120, "close": 41.120},
                "2024-07-26": {"open": 42.000, "close": 42.570},
                "2024-10-18": {"open": 57.000, "close": 57.180},
                "2024-12-31": {"open": 52.650, "close": 53.900},
                "2025-01-09": {"open": 49.810, "close": 49.810},
                "2025-01-10": {"open": 48.820, "close": 48.950},
            },
        }
        # 分钟close（未复权）
        self.minute: Dict[str, Dict[str, float]] = {
            # 保留接口占位，参考用例默认不提供 09:30 分钟线，按日开回退
        }

        # 现金分红事件（标准化结构）
        self.dividends: Dict[str, List[Dict[str, Any]]] = {
            "601318.XSHG": [
                {
                    "security": "601318.XSHG",
                    "date": Date(2024, 7, 26),
                    "security_type": "stock",
                    "scale_factor": 1.0,
                    "bonus_pre_tax": 15.0,  # 每10派15
                    "per_base": 10,
                },
                {
                    "security": "601318.XSHG",
                    "date": Date(2024, 10, 18),
                    "security_type": "stock",
                    "scale_factor": 1.0,
                    "bonus_pre_tax": 9.3,  # 每10派9.3
                    "per_base": 10,
                },
            ],
            "511880.XSHG": [
                {
                    "security": "511880.XSHG",
                    "date": Date(2024, 12, 31),
                    "security_type": "fund",
                    "scale_factor": 1.0,
                    "bonus_pre_tax": 1.5521,  # 每1派 1.5521
                    "per_base": 1,
                }
            ],
        }

    def auth(self, *_, **__):
        return None

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
    ) -> pd.DataFrame:
        # 简化：仅支持单标的，按 end_date 返回最后一条
        sec = security if isinstance(security, str) else security[0]
        ts = pd.to_datetime(end_date) if end_date is not None else pd.Timestamp("2024-07-10 09:30")
        fields_set = set(fields or [])
        # 停牌检测会请求 volume/paused：需要覆盖 start-end 区间内的每一天，且 volume>0/paused=0 才不会被判停牌
        if frequency == "daily" and ({"volume", "paused"} & fields_set):
            sd = pd.to_datetime(start_date).date() if start_date is not None else ts.date()
            ed = pd.to_datetime(end_date).date() if end_date is not None else ts.date()
            days = pd.date_range(sd, ed, freq="D")
            rows = []
            for d in days:
                key = d.date().isoformat()
                base = dict(self.daily.get(sec, {}).get(key, {"open": 1.0, "close": 1.0}))
                base.setdefault("volume", 1.0)
                base.setdefault("paused", 0)
                rows.append(base)
            df = pd.DataFrame(rows, index=[pd.Timestamp(d.date()) for d in days])
            return df
        date_key = ts.date().isoformat()
        if frequency == "daily":
            data = dict(self.daily.get(sec, {}).get(date_key, {}))
            if not data:
                # 给一个默认值，防止空
                data = {"open": 1.0, "close": 1.0}
            # 若处于集合竞价窗口(含 09:30)，且存在 09:30 分钟价，则用其作为当日 open
            try:
                t = ts.time()
                if t >= Time(9, 25) and t < Time(9, 31):
                    minute_key = f"{ts.strftime('%Y-%m-%d')} 09:30"
                    val = self.minute.get((sec, minute_key))
                    if isinstance(val, (int, float)) and val > 0:
                        data["open"] = float(val)
            except Exception:
                pass
            df = pd.DataFrame([data], index=[pd.Timestamp(ts.date())])
            return df
        else:
            # minute
            minute_key = f"{ts.strftime('%Y-%m-%d %H:%M')}"
            val = self.minute.get((sec, minute_key), None)
            # 新撮合合同要求当前分钟价量证据。这里是权益/费用单元夹具，
            # 为已声明的交易日显式构造按开盘价成交、流动性充足的分钟；
            # 不改预期成交、分红或费用断言，也不用于真实行情回退。
            if val is None and "volume" in fields_set:
                val = self.daily.get(sec, {}).get(date_key, {}).get("open")
            if val is not None and "volume" in fields_set:
                return pd.DataFrame(
                    [{"close": float(val), "volume": 1_000_000}], index=[pd.Timestamp(ts)]
                )
            if val is None:
                return pd.DataFrame()
            df = pd.DataFrame([{"close": float(val)}], index=[pd.Timestamp(ts)])
            return df

    def get_trade_days(self, start_date=None, end_date=None, count=None) -> List[datetime]:
        # 生成一个简单的交易日序列（工作日）
        sd = pd.to_datetime(start_date) if start_date else pd.Timestamp("2024-07-10")
        ed = pd.to_datetime(end_date) if end_date else pd.Timestamp("2025-01-31")
        days = pd.bdate_range(sd, ed, freq="C")  # custom business days近似
        return [pd.Timestamp(d).to_pydatetime() for d in days]

    def get_all_securities(
        self, types: Union[str, List[str]] = "stock", date: Optional[Union[str, datetime]] = None
    ) -> pd.DataFrame:
        idx = []
        if types in ("stock", ["stock"]):
            idx = ["601318.XSHG"]
        elif types in ("fund", "etf", ["fund"], ["etf"]):
            idx = ["511880.XSHG"]
        df = pd.DataFrame(index=idx)
        df.index.name = "code"
        return df

    def get_index_stocks(
        self, index_symbol: str, date: Optional[Union[str, datetime]] = None
    ) -> List[str]:
        return []

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        sd = pd.to_datetime(start_date).date() if start_date is not None else Date(2024, 1, 1)
        ed = pd.to_datetime(end_date).date() if end_date is not None else Date(2025, 12, 31)
        evs = []
        for ev in self.dividends.get(security, []):
            if sd <= ev["date"] <= ed:
                evs.append(ev)
        return evs

    def get_security_info(self, security: str) -> Dict[str, Any]:
        # 明确标的分类，避免引擎侧默认分类为 stock 造成取整/费用误差
        if security == "511880.XSHG":
            return {"type": "fund", "subtype": "money_market_fund", "display_name": "货币基金示例"}
        if security == "512670.XSHG":
            return {"type": "etf", "subtype": "fund", "display_name": "除权后买入 ETF 示例"}
        if security == "601318.XSHG":
            return {"type": "stock", "display_name": "中国平安示例"}
        return {}


def _setup_engine(provider: DataProvider, start: str, end: str) -> BacktestEngine:
    # 切换数据源（离线 provider）
    data_api.set_data_provider(provider)
    # 强制关闭分类默认滑点，确保成交价完全由基准价决定
    try:
        data_api.set_security_overrides(
            {
                "by_category": {
                    "stock": {"slippage": 0.0},
                    "fund": {"slippage": 0.0},
                    "money_market_fund": {"slippage": 0.0},
                },
                "by_prefix": {
                    # 将 511 前缀归类为货币基金，避免默认按 stock 处理
                    "511": "money_market_fund",
                },
                "by_code": {
                    # 明确 511880 为货币基金（双保险）
                    "511880.XSHG": {"category": "money_market_fund"},
                },
            }
        )
    except Exception:
        pass

    # 策略：在 initialize 内设置滑点/费用（避免被引擎 reset_settings() 覆盖）并下单
    def initialize(context):
        from jqdata import set_benchmark, run_daily

        set_benchmark("000300.XSHG")

        # 关闭滑点，保证精确复现目标价格
        set_slippage(FixedSlippage(0.0))
        # 设置费用：股票卖出 close_commission=0.0013，以匹配 71.16 手续费；基金免手续费
        set_order_cost(
            OrderCost(
                open_commission=0.0003, close_commission=0.0013, close_tax=0.0, min_commission=5.0
            ),
            type="stock",
        )
        set_order_cost(
            OrderCost(open_commission=0.0, close_commission=0.0, close_tax=0.0, min_commission=0.0),
            type="fund",
        )

        def m_open(ctx):
            d = ctx.current_dt.date()
            if d == Date(2024, 7, 10):
                from jqdata import order

                order("511880.XSHG", 400)  # 基金：成交 100.973
                order("601318.XSHG", 1200)  # 股票：成交 41.120
            if d == Date(2025, 1, 10):
                from jqdata import order

                order("511880.XSHG", -300)  # 成交 100.111
                order("601318.XSHG", -1100)  # 成交 49.760

        run_daily(m_open, time="open")

    eng = BacktestEngine(
        initialize=initialize,
        start_date=start,
        end_date=end,
        initial_cash=100000,
    )
    return eng


def _assert_trade(record, code: str, amount: int, price: float, commission: float):
    assert record.security == code
    assert record.amount == amount
    assert abs(record.price - price) < 1e-6
    assert abs(record.commission - commission) < 1e-2


def _make_512670_provider(action: Dict[str, Any]) -> ReferenceProvider:
    """构造 512670.XSHG 权益事件回归测试用离线数据源。

    参数:
        action: 待注入的分红/拆分事件。

    返回值:
        ReferenceProvider: 已注入 512670 行情和权益事件的离线数据源。

    副作用:
        无。
    """

    provider = ReferenceProvider()
    provider.daily["512670.XSHG"] = {
        "2024-08-19": {"open": 1.00, "close": 1.00},
        "2024-08-20": {"open": 0.50, "close": 0.50},
        "2024-08-21": {"open": 0.51, "close": 0.51},
        "2024-08-22": {"open": 0.50, "close": 0.51},
        "2024-08-23": {"open": 0.52, "close": 0.52},
    }
    provider.dividends["512670.XSHG"] = [action]
    return provider


def _run_512670_order_scenario(
    provider: ReferenceProvider,
    *,
    start: str,
    end: str,
    orders_by_date: Dict[Date, List[int]],
) -> BacktestEngine:
    """运行 512670.XSHG 单标的权益事件回归场景。

    参数:
        provider: 离线数据源。
        start: 回测开始日期。
        end: 回测结束日期。
        orders_by_date: 按日期配置的下单数量列表。

    返回值:
        BacktestEngine: 已运行完成的回测引擎。

    副作用:
        设置当前数据源，并执行回测。
    """

    data_api.set_data_provider(provider)

    def initialize(context):
        """初始化 ETF 权益事件回归策略。

        参数:
            context: 回测上下文。

        返回值:
            无。

        副作用:
            注册每日开盘下单任务，并设置无滑点、基金零费用。
        """

        from jqdata import order, run_daily, set_benchmark

        set_benchmark("000300.XSHG")
        set_slippage(FixedSlippage(0.0))
        set_order_cost(
            OrderCost(
                open_commission=0.0,
                close_commission=0.0,
                close_tax=0.0,
                min_commission=0.0,
            ),
            type="fund",
        )

        def trade(ctx):
            """按日期执行测试订单。

            参数:
                ctx: 回测上下文。

            返回值:
                无。

            副作用:
                在指定日期提交 512670.XSHG 买卖订单。
            """

            for amount in orders_by_date.get(ctx.current_dt.date(), []):
                order("512670.XSHG", amount)

        run_daily(trade, time="open")

    engine = BacktestEngine(
        initialize=initialize,
        start_date=start,
        end_date=end,
        initial_cash=100000,
    )
    engine.run()
    return engine


@pytest.mark.unit
def test_reference_trades_match_prices_and_fees():
    provider = ReferenceProvider()
    try:
        eng = _setup_engine(provider, "2024-07-10", "2025-01-31")
        results = eng.run()

        trades = eng.trades
        # 预期四笔：买入两笔（2024-07-10），卖出两笔（2025-01-10）
        assert len(trades) >= 4
        # 第一笔：511880 买入 400，100.973，手续费 0
        _assert_trade(trades[0], "511880.XSHG", 400, 100.973, 0.0)
        # 第二笔：601318 买入 1200，41.120，手续费 14.80
        _assert_trade(trades[1], "601318.XSHG", 1200, 41.120, 14.80)
        # 第三笔：511880 卖出 -300，100.111，手续费 0
        _assert_trade(trades[2], "511880.XSHG", -300, 100.111, 0.0)
        # 第四笔：601318 卖出 -1100，按日开 48.820，手续费 69.81（按 0.0013）
        _assert_trade(trades[3], "601318.XSHG", -1100, 48.820, 69.81)
    finally:
        data_api.reset_security_overrides()


@pytest.mark.unit
def test_reference_dividends_match_events():
    provider = ReferenceProvider()
    eng = _setup_engine(provider, "2024-07-10", "2024-12-31")
    eng.run()
    events = eng.events
    # 预期三条：07-26（601318），10-18（601318），12-31（511880）
    # 使用简要断言：code, event_date, cash_in, per_base
    found = [
        (
            e.get("code"),
            e.get("event_date"),
            round(float(e.get("cash_in") or 0.0), 2),
            e.get("per_base"),
        )
        for e in events
        if e.get("event_type") == "现金分红"
    ]
    expected = [
        ("601318.XSHG", Date(2024, 7, 26), 1440.00, 10),
        ("601318.XSHG", Date(2024, 10, 18), 892.80, 10),
        ("511880.XSHG", Date(2024, 12, 31), 620.84, 1),
    ]
    # 由于 run 范围到 12-31，只要三条都出现即可
    for exp in expected:
        assert exp in found


@pytest.mark.unit
def test_split_event_on_position_buy_time_is_not_applied():
    """验证除权日当天买入的 ETF 持仓不会重复应用当日拆分事件。

    参数:
        无。

    返回值:
        无。

    副作用:
        使用 ReferenceProvider 运行离线回测，验证同日买入不参与拆分。
    """

    provider = _make_512670_provider(
        {
            "security": "512670.XSHG",
            "date": Date(2024, 8, 20),
            "security_type": "etf",
            "scale_factor": 2.0,
            "bonus_pre_tax": 0.0,
            "per_base": 1,
        }
    )
    engine = _run_512670_order_scenario(
        provider,
        start="2024-08-20",
        end="2024-08-21",
        orders_by_date={Date(2024, 8, 20): [1000], Date(2024, 8, 21): [-1000]},
    )

    assert [(trade.security, trade.amount) for trade in engine.trades] == [
        ("512670.XSHG", 1000),
        ("512670.XSHG", -1000),
    ]
    assert not [event for event in engine.events if event.get("event_type") == "拆分/送转"]
    assert "512670.XSHG" not in engine.context.portfolio.positions


@pytest.mark.unit
def test_split_event_before_position_buy_time_is_applied_once():
    """验证除权日前已持有的 ETF 持仓仍会正常应用拆分事件。

    参数:
        无。

    返回值:
        无。

    副作用:
        使用 ReferenceProvider 运行离线回测，验证符合资格的持仓被拆分一次。
    """

    provider = _make_512670_provider(
        {
            "security": "512670.XSHG",
            "date": Date(2024, 8, 20),
            "security_type": "etf",
            "scale_factor": 2.0,
            "bonus_pre_tax": 0.0,
            "per_base": 1,
        }
    )
    engine = _run_512670_order_scenario(
        provider,
        start="2024-08-19",
        end="2024-08-20",
        orders_by_date={Date(2024, 8, 19): [1000]},
    )

    split_events = [event for event in engine.events if event.get("event_type") == "拆分/送转"]
    assert len(split_events) == 1
    assert split_events[0]["old_amount"] == 1000
    assert split_events[0]["new_amount"] == 2000
    assert engine.context.portfolio.positions["512670.XSHG"].total_amount == 2000


@pytest.mark.unit
def test_cash_dividend_event_on_position_buy_time_is_not_applied():
    """验证除息日当天买入的 ETF 持仓不会获得当日现金分红。

    参数:
        无。

    返回值:
        无。

    副作用:
        使用 ReferenceProvider 运行离线回测，验证同日买入不参与现金分红。
    """

    provider = _make_512670_provider(
        {
            "security": "512670.XSHG",
            "date": Date(2024, 8, 20),
            "security_type": "etf",
            "scale_factor": 1.0,
            "bonus_pre_tax": 0.10,
            "per_base": 1,
        }
    )
    engine = _run_512670_order_scenario(
        provider,
        start="2024-08-20",
        end="2024-08-21",
        orders_by_date={Date(2024, 8, 20): [1000], Date(2024, 8, 21): [-1000]},
    )

    assert not [event for event in engine.events if event.get("event_type") == "现金分红"]
    assert [(trade.security, trade.amount) for trade in engine.trades] == [
        ("512670.XSHG", 1000),
        ("512670.XSHG", -1000),
    ]


@pytest.mark.unit
def test_initial_position_after_split_date_does_not_replay_old_event():
    """验证除权后注入的初始持仓不会回放历史拆分事件。

    参数:
        无。

    返回值:
        无。

    副作用:
        使用 ReferenceProvider 运行离线回测，验证 initial_positions 的事件资格。
    """

    provider = _make_512670_provider(
        {
            "security": "512670.XSHG",
            "date": Date(2024, 8, 20),
            "security_type": "etf",
            "scale_factor": 2.0,
            "bonus_pre_tax": 0.0,
            "per_base": 1,
        }
    )
    data_api.set_data_provider(provider)

    def initialize(context):
        """初始化无交易策略。

        参数:
            context: 回测上下文。

        返回值:
            无。

        副作用:
            设置回测基准。
        """

        from jqdata import set_benchmark

        set_benchmark("000300.XSHG")

    engine = BacktestEngine(
        initialize=initialize,
        start_date="2024-08-22",
        end_date="2024-08-22",
        initial_cash=100000,
        initial_positions=[{"security": "512670.XSHG", "amount": 1000, "avg_cost": 0.50}],
    )
    engine.run()

    assert not [event for event in engine.events if event.get("event_type") == "拆分/送转"]
    position = engine.context.portfolio.positions["512670.XSHG"]
    assert position.total_amount == 1000
    assert position.buy_time.date() == Date(2024, 8, 22)


@pytest.mark.unit
def test_split_event_before_position_buy_time_is_not_applied():
    """验证除权日之后才买入的 ETF 持仓不会重复应用历史拆分事件。

    参数:
        无。

    返回值:
        无。

    副作用:
        使用 ReferenceProvider 运行一段离线回测，验证成交和事件列表。
    """

    provider = ReferenceProvider()
    provider.daily["512670.XSHG"] = {
        "2024-08-20": {"open": 0.50, "close": 0.50},
        "2024-08-22": {"open": 0.50, "close": 0.51},
        "2024-08-23": {"open": 0.52, "close": 0.52},
    }
    provider.dividends["512670.XSHG"] = [
        {
            "security": "512670.XSHG",
            "date": Date(2024, 8, 20),
            "security_type": "etf",
            "scale_factor": 2.0,
            "bonus_pre_tax": 0.0,
            "per_base": 1,
        }
    ]
    data_api.set_data_provider(provider)

    def initialize(context):
        """初始化测试策略。

        参数:
            context: 回测上下文。

        返回值:
            无。

        副作用:
            注册每日开盘买卖任务，并设置无滑点、基金零费用。
        """

        from jqdata import order, run_daily, set_benchmark

        set_benchmark("000300.XSHG")
        set_slippage(FixedSlippage(0.0))
        set_order_cost(
            OrderCost(open_commission=0.0, close_commission=0.0, close_tax=0.0, min_commission=0.0),
            type="fund",
        )

        def trade(ctx):
            """按日期执行 ETF 买卖。

            参数:
                ctx: 回测上下文。

            返回值:
                无。

            副作用:
                2024-08-22 买入，2024-08-23 卖出。
            """

            day = ctx.current_dt.date()
            if day == Date(2024, 8, 22):
                order("512670.XSHG", 1000)
            if day == Date(2024, 8, 23):
                order("512670.XSHG", -1000)

        run_daily(trade, time="open")

    try:
        engine = BacktestEngine(
            initialize=initialize,
            start_date="2024-08-22",
            end_date="2024-08-23",
            initial_cash=100000,
        )
        engine.run()
        assert [(trade.security, trade.amount) for trade in engine.trades] == [
            ("512670.XSHG", 1000),
            ("512670.XSHG", -1000),
        ]
        assert not [event for event in engine.events if event.get("event_type") == "拆分/送转"]
    finally:
        data_api.reset_security_overrides()


@pytest.mark.requires_network
def test_live_provider_trades_and_dividends_match_reference(provider_name):
    """
    在线数据源校验：校验“撮合规则一致性”（而非硬性等于金标数值）。
    规则：
      - 09:25–09:30 成交价 = 当日不复权 open（若 09:30 分钟存在，可按分钟 close；多数 provider 首分钟为 09:31）
      - 09:31–15:00 成交价 = 当分钟不复权 close
      - 手续费/税：按本测试中 set_order_cost 配置计算
    """
    try:
        from bullet_trade.utils.env_loader import get_env
    except Exception:
        get_env = lambda k, d=None: os.environ.get(k, d)

    if provider_name == "jqdata":
        if not (get_env("JQDATA_USERNAME") and get_env("JQDATA_PASSWORD")):
            pytest.skip("缺少 JQDATA_USERNAME/JQDATA_PASSWORD，或 .env 未被加载")
        try:
            import jqdatasdk  # noqa: F401
        except Exception:
            pytest.skip("未安装 jqdatasdk，跳过 jqdata 在线校验")
    elif provider_name == "tushare":
        if not get_env("TUSHARE_TOKEN"):
            pytest.skip("缺少 TUSHARE_TOKEN，跳过 tushare 在线校验")

    try:
        data_api.set_data_provider(provider_name)
    except Exception as exc:
        pytest.skip(f"provider {provider_name} setup failed: {exc}")

    def initialize(context):
        from jqdata import set_benchmark, run_daily

        set_benchmark("000300.XSHG")
        set_slippage(FixedSlippage(0.0))
        set_order_cost(
            OrderCost(
                open_commission=0.0003, close_commission=0.0013, close_tax=0.0, min_commission=5.0
            ),
            type="stock",
        )
        set_order_cost(
            OrderCost(open_commission=0.0, close_commission=0.0, close_tax=0.0, min_commission=0.0),
            type="fund",
        )

        def m_open(ctx):
            d = ctx.current_dt.date()
            if d == Date(2024, 7, 10):
                from jqdata import order

                order("511880.XSHG", 400)
                order("601318.XSHG", 1200)
            if d == Date(2025, 1, 10):
                from jqdata import order

                order("511880.XSHG", -300)
                order("601318.XSHG", -1100)

        run_daily(m_open, time="open")

    eng = BacktestEngine(
        initialize=initialize,
        start_date="2024-07-10",
        end_date="2025-01-31",
        initial_cash=100000,
    )

    eng.run()
    trades = eng.trades
    assert len(trades) >= 4

    # 构造规则一致性的期望生成器（按 provider 数据计算期望撮合价与费用）
    def infer_category(code: str) -> str:
        info = data_api.get_security_info(code) or {}
        subtype = str(info.get("subtype") or "").lower()
        primary = str(info.get("type") or "").lower()
        if subtype in ("mmf", "money_market_fund"):
            return "money_market_fund"
        if primary == "fund":
            return "fund"
        if code.split(".", 1)[0].startswith("511"):
            return "money_market_fund"
        return "stock"

    def round_to_tick(price: float, cat: str) -> float:
        step = 0.001 if cat in ("fund", "money_market_fund") else 0.01
        ticks = price / step
        import math

        ticks_rounded = math.floor(ticks + 0.5)
        return round(ticks_rounded * step, 3 if step == 0.001 else 2)

    def fen(x: float) -> float:
        from decimal import Decimal, ROUND_HALF_UP

        return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def expected_price(code: str, dt: datetime) -> float:
        t = dt.time()
        if t >= Time(9, 25) and t < Time(9, 31):
            # 先尝试 09:30 分钟，否则回退到日开
            try:
                dfm = data_api.get_price(
                    code,
                    start_date=dt.replace(hour=9, minute=25),
                    end_date=dt,
                    frequency="minute",
                    fields=["close"],
                    fq="none",
                    count=None,
                )
            except Exception:
                dfm = pd.DataFrame()
            if isinstance(dfm, pd.DataFrame) and not dfm.empty and dt in dfm.index:
                base = float(dfm.loc[dt]["close"])
            else:
                dfd = data_api.get_price(
                    code, end_date=dt, frequency="daily", fields=["open"], fq="none", count=1
                )
                base = float(dfd.iloc[-1]["open"]) if not dfd.empty else 0.0
        elif t >= Time(9, 31) and t < Time(15, 0):
            dfm = data_api.get_price(
                code, end_date=dt, frequency="minute", fields=["close"], fq="none", count=1
            )
            base = (
                float(dfm.iloc[-1]["close"])
                if (isinstance(dfm, pd.DataFrame) and not dfm.empty)
                else 0.0
            )
        else:
            dfd = data_api.get_price(
                code, end_date=dt, frequency="daily", fields=["close"], fq="none", count=1
            )
            base = float(dfd.iloc[-1]["close"]) if not dfd.empty else 0.0
        cat = infer_category(code)
        return round_to_tick(base, cat)

    def expected_commission(code: str, amount: int, price: float) -> float:
        # 与测试内 set_order_cost 对齐：stock 万3/卖万13/最小5；fund 0
        cat = infer_category(code)
        value = abs(amount) * price
        if cat in ("fund", "money_market_fund"):
            return 0.0
        # 买入：万3
        if amount > 0:
            return fen(max(value * 0.0003, 5.0))
        # 卖出：万13
        return fen(max(value * 0.0013, 5.0))

    # 验证四笔成交（规则一致性）
    for tr in trades:
        code = tr.security
        dt = tr.time
        price_exp = expected_price(code, dt)
        comm_exp = expected_commission(code, tr.amount, price_exp)
        assert (
            abs(tr.price - price_exp) < 1e-6
        ), f"{code} price mismatch: got {tr.price}, exp {price_exp} at {dt}"
        assert (
            abs(tr.commission - comm_exp) < 1e-2
        ), f"{code} commission mismatch: got {tr.commission}, exp {comm_exp} at {dt}"
