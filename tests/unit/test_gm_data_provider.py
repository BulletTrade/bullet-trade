"""作者: BruceLee
职责: GM 正式数据接口的时间、缺失、停牌、量价复权和 SDK 隔离回归测试。
输入: 有明确事实的只读客户端；输出: 断言；不联网、不访问账户。
"""

import json
import subprocess
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.data.providers.gm import GmDataProvider
from bullet_trade.integrations.gm.data_client import GmDataClient, GmDataError
from bullet_trade.integrations.gm.data_worker import execute


class Client:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def auth(self):
        pass

    def query(self, method, **kwargs):
        self.calls.append((method, kwargs))
        return self.handler(method, kwargs)


INFO = dict(
    symbol="SHSE.601318",
    sec_type=1,
    sec_type1=1010,
    sec_type2=101001,
    sec_name="中国平安",
    sec_abbr="ZGPA",
    price_tick=0.01,
    board=10100101,
    listed_date="2007-03-01",
    delisted_date="2038-01-01",
)


def bar(day, close=10, volume=100, symbol="SHSE.601318"):
    return dict(
        symbol=symbol,
        eob=day,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        amount=close * volume,
    )


def make(handler):
    def query(method, kwargs):
        if method in {"get_instrumentinfos", "get_symbol_infos"}:
            return [INFO]
        return handler(method, kwargs)

    return GmDataProvider({"client": Client(query)})


def test_registered_provider_constructs_without_sdk_or_network(monkeypatch):
    from bullet_trade.data.api import _create_provider, _normalize_provider_name

    assert _normalize_provider_name("掘金") == "gm"
    assert isinstance(
        _create_provider("goldminer", {"client": Client(lambda *args: [])}), GmDataProvider
    )


def test_standard_raw_shapes_and_amount_mapping():
    p = make(lambda m, k: [bar("2024-07-25")])
    frame = p.get_price("601318.XSHG", "2024-07-25", "2024-07-25", fq=None, skip_paused=True)
    assert frame.index[0] == pd.Timestamp("2024-07-25")
    assert frame.money.iloc[0] == 1000
    long = p.get_price(
        ["601318.XSHG"], "2024-07-25", "2024-07-25", fq=None, skip_paused=True, panel=False
    )
    assert list(long.columns) == ["time", "open", "high", "low", "close", "volume", "money", "code"]
    wide = p.get_price(["601318.XSHG"], "2024-07-25", "2024-07-25", fq=None, skip_paused=True)
    assert ("close", "601318.XSHG") in wide.columns


@pytest.mark.parametrize("mutation", ["duplicate", "wrong_security", "nonfinite", "unsorted"])
def test_corrupt_bars_fail_closed(mutation):
    rows = [bar("2024-07-25"), bar("2024-07-26")]
    if mutation == "duplicate":
        rows[1]["eob"] = rows[0]["eob"]
    if mutation == "wrong_security":
        rows[1]["symbol"] = "SHSE.600000"
    if mutation == "nonfinite":
        rows[1]["close"] = float("nan")
    if mutation == "unsorted":
        rows.reverse()
    p = make(lambda m, k: rows)
    with pytest.raises(GmDataError):
        p.get_price("601318.XSHG", "2024-07-25", "2024-07-26", fq=None, skip_paused=True)


def test_halt_uses_old_actual_close_and_never_future_backfill():
    def query(method, kw):
        if method == "history":
            return [bar("2016-07-04", 20)]
        if method == "get_trading_dates":
            return ["2016-07-01", "2016-07-04"]
        if method == "get_history_instruments":
            return [dict(symbol="SHSE.601318", trade_date="2016-07-01", is_suspended=1)]
        if method == "history_n":
            return [bar("2015-12-17", 24.43)]
        raise AssertionError(method)

    p = make(query)
    f = p.get_price(
        "601318.XSHG",
        "2016-07-01",
        "2016-07-04",
        fq=None,
        fields=["close", "volume", "money", "paused"],
    )
    assert list(f.close) == [24.43, 20]
    assert list(f.volume) == [0, 100]
    assert list(f.paused) == [1, 0]
    calls = [k for m, k in p._client.calls if m == "history_n"]
    assert pd.Timestamp(calls[0]["end_time"]) < pd.Timestamp("2016-07-01")


def test_unknown_missing_bar_is_not_treated_as_halt():
    p = make(lambda m, k: [] if m in {"history", "get_history_instruments"} else ["2024-07-25"])
    with pytest.raises(GmDataError, match="未确认全天停牌"):
        p.get_price("601318.XSHG", "2024-07-25", "2024-07-25", fq=None)


def test_count_cannot_reach_before_listing():
    def query(m, k):
        if m == "get_trading_dates":
            return [d for d in ["2007-02-28", "2007-03-01"] if d >= k["start_date"]]
        if m == "history":
            return [bar("2007-03-01")]
        raise AssertionError(m)

    p = make(query)
    f = p.get_price("601318.XSHG", end_date="2007-03-01", count=2, fq=None)
    assert list(f.index) == [pd.Timestamp("2007-03-01")]


def test_dividend_stock_per_ten_and_rights_not_ordinary_dividend():
    row = dict(
        symbol="SHSE.601318",
        created_at="2024-07-26",
        cash_div=1.5,
        share_div_ratio=0.0,
        share_trans_ratio=0.8,
        allotment_ratio=0.0,
        allotment_price=0.0,
    )
    p = make(lambda m, k: [row])
    event = p.get_split_dividend("601318.XSHG", "2024-07-25", "2024-07-26")[0]
    assert (event["bonus_pre_tax"], event["per_base"], event["scale_factor"]) == (15, 10, 1.8)
    row["allotment_ratio"] = 0.1
    with pytest.raises(NotImplementedError):
        p.get_split_dividend("601318.XSHG", "2024-07-25", "2024-07-26")


def tick(stamp, price=10, volume=100):
    return dict(
        symbol="SHSE.601318",
        created_at=stamp,
        price=price,
        cum_volume=volume,
        cum_amount=volume * price,
    )


def test_tick_skip_applies_before_count():
    rows = [
        tick("2026-09-30 14:56:58.826", 9, 90),
        tick("2026-09-30 14:57:01.826"),
        tick("2026-09-30 14:59:58.826"),
    ]
    p = make(lambda m, k: rows)
    frame = p.get_ticks(
        "601318.XSHG",
        "2026-09-30 15:00:00",
        count=1,
        skip=True,
        fields=["time", "current", "volume", "money"],
        df=True,
    )
    assert frame.time.iloc[0] == 20260930145701.0
    assert (
        p.get_ticks(
            "601318.XSHG", "2026-09-30 15:00:00", count=1, skip=False, fields=["time"], df=True
        ).time.iloc[0]
        == 20260930145958.0
    )


def test_historical_tick_does_not_call_current():
    p = make(lambda m, k: [tick("2026-09-30 14:59:58.826")])
    assert p.get_current_tick("601318.XSHG", dt="2026-09-30 15:00:00")["current"] == 10
    assert all(m != "current" for m, k in p._client.calls)


@pytest.mark.parametrize("method", ["order_volume", "get_cash", "run", "set_token"])
def test_worker_rejects_non_data_before_auth(method):
    calls = []
    api = SimpleNamespace(set_token=lambda *a: calls.append(a))
    with pytest.raises(ValueError):
        execute(api, dict(method=method, token="private", kwargs={}))
    assert calls == []


def test_data_client_timeout_and_secret_logs_do_not_escape(monkeypatch):
    secret = "private-value"

    def run(*a, **k):
        raise subprocess.TimeoutExpired("sdk", 30, output=secret)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(GmDataError) as e:
        GmDataClient({"token": secret}).query("current", symbols="SHSE.601318")
    assert secret not in str(e.value)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(
            returncode=0,
            stdout=secret + "\nBT_GM_DATA=" + json.dumps(dict(status="error", error_code=2001)),
        ),
    )
    with pytest.raises(GmDataError, match="2001") as e:
        GmDataClient({"token": secret}).query("current", symbols="SHSE.601318")
    assert secret not in str(e.value)


def test_minute_amount_truncates_rather_than_rounding_up():
    row = bar("2026-09-30 13:05:00")
    row["amount"] = 5850386.959999979
    p = make(lambda m, k: ["2026-09-30"] if m == "get_trading_dates" else [row])
    assert (
        p.get_price(
            "601318.XSHG",
            "2026-09-30 13:05:00",
            "2026-09-30 13:05:00",
            frequency="5m",
            fq=None,
            skip_paused=True,
        ).money.iloc[0]
        == 5850386
    )


def test_source_errors_not_replaced_with_fake_empty():
    def fail(*a):
        raise GmDataError("GM 数据查询失败，错误码 2001")

    p = make(fail)
    with pytest.raises(GmDataError, match="2001"):
        p.get_price("601318.XSHG", "2024-07-25", "2024-07-25", fq=None)


def factored(factor=2.0, anchor=4.0, corrupt=False):
    def handler(method, kwargs):
        if method in {"history", "history_n"}:
            return [bar("2024-07-25")]
        if method == "get_trading_dates":
            return ["2024-07-30"]
        if method == "get_history_instruments":
            day = kwargs["start_date"]
            return [
                dict(
                    symbol=INFO["symbol"],
                    trade_date="2024-07-26" if corrupt and day == "2024-07-25" else day,
                    adj_factor=anchor if day == "2024-07-30" else factor,
                )
            ]
        raise AssertionError(method)

    return make(handler)


def test_gm_factor_changes_prices_and_volume_keeps_daily_money():
    p = factored()
    frame = p.get_price(
        "601318.XSHG",
        "2024-07-25",
        "2024-07-25",
        skip_paused=True,
        pre_factor_ref_date="2024-07-30",
        fields=["close", "volume", "money", "factor"],
    )
    assert list(frame.iloc[0]) == [5.0, 200.0, 1000.0, 0.5]
    assert set(frame.attrs["field_sources"].values()) == {"gm"}
    assert frame.attrs["price_factor_source"] == "gm"


@pytest.mark.parametrize("fq", ["pre", "post"])
def test_index_has_identity_factor_without_stock_adjustment_status(fq):
    info = dict(INFO, sec_type=3, sec_type1=1060, sec_type2=106001)

    def query(method, kwargs):
        if method in {"get_instrumentinfos", "get_symbol_infos"}:
            return [info]
        if method == "history":
            return [bar("2024-07-25")]
        pytest.fail("指数不应查询股票复权因子")

    provider = GmDataProvider({"client": Client(query)})
    frame = provider.get_price(
        "601318.XSHG",
        "2024-07-25",
        "2024-07-25",
        fields=["close", "volume", "factor"],
        fq=fq,
        skip_paused=True,
    )
    assert frame.iloc[0].tolist() == [10.0, 100.0, 1.0]


@pytest.mark.parametrize("panel", [True, False])
def test_public_api_backtest_cache_preserves_gm_price_volume_and_reference(monkeypatch, panel):
    """公开策略入口开启动态复权缓存时，仍由 GM 按回测参考日同步复权量价。"""
    from bullet_trade.core.settings import reset_settings, set_option
    from bullet_trade.data import api as data_api
    from bullet_trade.data.backtest_session import (
        create_backtest_data_session,
        reset_current_backtest_data_session,
        set_current_backtest_data_session,
    )

    provider = factored()
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    monkeypatch.setattr(
        data_api,
        "_current_context",
        SimpleNamespace(current_dt=pd.Timestamp("2024-07-30 14:50"), run_params={}),
    )
    reset_settings()
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2024-07-25",
        end_date="2024-07-31",
        provider_name="gm",
    )
    token = set_current_backtest_data_session(session)
    try:
        frame = data_api.get_price(
            "601318.XSHG" if panel else ["601318.XSHG"],
            end_date="2024-07-25",
            count=1,
            fields=["close", "volume", "money"],
            fq="pre",
            skip_paused=True,
            panel=panel,
        )
        assert list(frame[["close", "volume", "money"]].iloc[0]) == [5.0, 200.0, 1000.0]
        if not panel:
            assert frame.code.tolist() == ["601318.XSHG"]
            assert frame.time.tolist() == [pd.Timestamp("2024-07-25")]
        anchors = [
            k
            for m, k in provider._client.calls
            if m == "get_history_instruments" and k["start_date"] == "2024-07-30"
        ]
        assert len(anchors) == 1 and anchors[0]["end_date"] == "2024-07-30"
        assert session.stats.cache_writes == 0
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        reset_settings()


@pytest.mark.parametrize(
    "factor,anchor,corrupt",
    [
        (0.0, 4.0, False),
        (-1.0, 4.0, False),
        (float("nan"), 4.0, False),
        (2.0, 0.0, False),
        (2.0, 4.0, True),
    ],
)
def test_invalid_gm_factor_never_silently_falls_back(factor, anchor, corrupt):
    p = factored(factor, anchor, corrupt)
    with pytest.raises(GmDataError):
        p.get_price(
            "601318.XSHG",
            "2024-07-25",
            "2024-07-25",
            skip_paused=True,
            pre_factor_ref_date="2024-07-30",
        )


def test_read_only_fallback_cannot_resolve_trade_sdk():
    from bullet_trade.data.api import _bind_sdk_fallback

    p = make(lambda *a: [])
    _bind_sdk_fallback(p, "gm")
    with pytest.raises(AttributeError):
        getattr(p, "order_volume")


def test_exact_minute_start_requests_left_context_and_keeps_first_bar():
    p = make(
        lambda m, k: ["2026-09-30"] if m == "get_trading_dates" else [bar("2026-09-30 09:31:00")]
    )
    f = p.get_price(
        "601318.XSHG",
        "2026-09-30 09:31:00",
        "2026-09-30 09:31:00",
        frequency="1m",
        fq=None,
        skip_paused=True,
    )
    assert list(f.index) == [pd.Timestamp("2026-09-30 09:31:00")]
    assert [k for m, k in p._client.calls if m == "history"][0][
        "start_time"
    ] == "2026-09-30 09:30:59"


def test_multiminute_aggregates_from_requested_start_and_keeps_partial_end():
    rows = [bar(f"2026-09-30 13:{n:02d}:00", close=n, volume=n) for n in range(5, 11)]
    p = make(lambda m, k: ["2026-09-30"] if m == "get_trading_dates" else rows)
    f = p.get_price(
        "601318.XSHG",
        "2026-09-30 13:05:00",
        "2026-09-30 13:10:00",
        frequency="5m",
        fq=None,
        skip_paused=True,
    )
    assert list(f.index) == list(pd.to_datetime(["2026-09-30 13:09:00", "2026-09-30 13:10:00"]))
    assert list(f.open) == [5.0, 10.0] and list(f.close) == [9.0, 10.0]
    assert list(f.volume) == [35.0, 10.0]
    assert [k for m, k in p._client.calls if m == "history"][0]["frequency"] == "60s"


def test_minute_pre_close_uses_previous_minute_instead_of_daily_reference():
    def handler(method, kw):
        if method == "get_trading_dates":
            return ["2026-09-30"]
        if method == "history":
            return [bar("2026-09-30 13:01:00", 11), bar("2026-09-30 13:02:00", 12)]
        if method == "history_n":
            return [bar("2026-09-30 11:30:00", 10)]
        if method == "get_history_instruments":
            return [
                dict(
                    symbol=INFO["symbol"],
                    trade_date="2026-09-30",
                    pre_close=9.0,
                    upper_limit=9.9,
                    lower_limit=8.1,
                )
            ]
        raise AssertionError(method)

    p = make(handler)
    f = p.get_price(
        "601318.XSHG",
        "2026-09-30 13:00:00",
        "2026-09-30 13:02:00",
        frequency="1m",
        fields=["close", "pre_close"],
        fq=None,
        skip_paused=True,
    )
    assert list(f.pre_close) == [10.0, 11.0]


def test_minute_halt_fills_only_confirmed_calendar_minutes():
    def query(m, k):
        if m == "history":
            return []
        if m == "get_trading_dates":
            return ["2016-07-01"]
        if m == "get_history_instruments":
            return [dict(symbol=INFO["symbol"], trade_date="2016-07-01", is_suspended=1)]
        if m == "history_n":
            return [bar("2015-12-17", 24.43)]
        raise AssertionError(m)

    p = make(query)
    f = p.get_price(
        "601318.XSHG",
        "2016-07-01 09:30:00",
        "2016-07-01 09:33:00",
        frequency="1m",
        fq=None,
        fields=["close", "volume", "paused"],
    )
    assert (
        len(f) == 3
        and list(f.close) == [24.43] * 3
        and list(f.volume) == [0.0] * 3
        and list(f.paused) == [1.0] * 3
    )
