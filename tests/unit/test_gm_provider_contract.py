"""GM 全字段契约矩阵：合成 SDK 输入与手算预期，不代表在线供应商等价。

复用其他源的固定停牌断言；行情输入只含 GM 字段，不向运行链传入聚宽结果。
"""

import inspect

import numpy as np
import pandas as pd
import pytest

from bullet_trade.data.providers.base import DataProvider
from bullet_trade.data.providers.gm import GmDataProvider, SUPPORTED_FIELDS
from bullet_trade.integrations.gm.data_client import GmDataError
from tests.price_pause_contract import CASES, assert_pause_contract
from tests.unit.test_gm_data_provider import Client, INFO, bar

FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "factor",
    "pre_close",
    "high_limit",
    "low_limit",
    "avg",
    "paused",
]


class Market:
    """小型确定性市场：除权前累计因子 2，除权后 4，价格减半，成交股数翻倍。"""

    def __init__(self, minute=False):
        self.minute = minute
        self.calls = []

    def query(self, method, **kw):
        self.calls.append((method, kw))
        symbol = kw.get("symbol", kw.get("symbols", INFO["symbol"]))
        if method in {"get_instrumentinfos", "get_symbol_infos"}:
            return [dict(INFO, symbol=symbol)]
        if method == "get_trading_dates":
            return [
                d for d in ["2024-07-25", "2024-07-26"] if kw["start_date"] <= d <= kw["end_date"]
            ]
        if method == "get_history_instruments":
            return [
                dict(
                    symbol=symbol,
                    trade_date=d,
                    adj_factor=f,
                    pre_close=18 / scale,
                    upper_limit=22 / scale,
                    lower_limit=16 / scale,
                    is_suspended=0,
                )
                for d, f, scale in [("2024-07-25", 2, 1), ("2024-07-26", 4, 2)]
                if kw["start_date"] <= d <= kw["end_date"]
            ]
        if method in {"history", "history_n"}:
            rows = []
            for d, scale in [("2024-07-25", 1), ("2024-07-26", 2)]:
                times = (
                    pd.date_range(d + " 09:31", periods=120, freq="min").append(
                        pd.date_range(d + " 13:01", periods=120, freq="min")
                    )
                    if self.minute
                    else [pd.Timestamp(d)]
                )
                rows.extend(
                    dict(
                        symbol=symbol,
                        eob=str(t),
                        open=18 / scale,
                        high=22 / scale,
                        low=16 / scale,
                        close=20 / scale,
                        volume=100 * scale,
                        amount=1900,
                    )
                    for t in times
                )
            rows = [r for r in rows if pd.Timestamp(r["eob"]) <= pd.Timestamp(kw["end_time"])]
            if method == "history_n":
                return rows[-kw["count"] :]
            return [r for r in rows if pd.Timestamp(r["eob"]) >= pd.Timestamp(kw["start_time"])]
        raise AssertionError(method)

    def auth(self):
        pass


@pytest.mark.parametrize("frequency", ["daily", "1m", "5m", "15m", "30m", "60m"])
@pytest.mark.parametrize(
    "fq,anchor,factors",
    [
        (None, "2024-07-26", [1, 1]),
        ("none", "2024-07-26", [1, 1]),
        ("pre", "2024-07-25", [1, 2]),
        ("pre", "2024-07-26", [0.5, 1]),
        ("post", "2024-07-25", [2, 4]),
        ("post", "2024-07-26", [2, 4]),
    ],
)
@pytest.mark.parametrize("selection", ["all", "each"])
def test_all_price_fields_adjustments_and_reference_dates(
    frequency, fq, anchor, factors, selection
):
    """所有支持价格字段及量额；后复权不依赖参考日，前复权只由选定参考因子决定。"""
    minute = frequency != "daily"
    size = int(frequency[:-1]) if minute else 1
    p = GmDataProvider({"client": Market(minute)})
    expected = []
    for scale, factor in zip([1, 2], factors):
        count = 240 // size if minute else 1
        for i in range(count):
            previous = (18 if i == 0 or not minute else 20) / scale * factor
            expected.append(
                [
                    18 / scale * factor,
                    22 / scale * factor,
                    16 / scale * factor,
                    20 / scale * factor,
                    100 * scale / factor * size,
                    1900 * size,
                    factor,
                    previous,
                    22 / scale * factor,
                    16 / scale * factor,
                    19 / scale * factor,
                    0,
                ]
            )
    for fields in ([FIELDS] if selection == "all" else [[f] for f in FIELDS]):
        actual = p.get_price(
            "601318.XSHG",
            "2024-07-25",
            "2024-07-26",
            frequency=frequency,
            fields=fields,
            skip_paused=True,
            fq=fq,
            pre_factor_ref_date=anchor,
        )
        positions = [FIELDS.index(f) for f in fields]
        np.testing.assert_allclose(actual, np.array(expected)[:, positions], rtol=0, atol=1e-12)
        assert actual.columns.tolist() == fields
        assert set(actual.attrs["field_sources"].values()) == {"gm"}
    assert all(k.get("adjust", 0) == 0 for m, k in p._client.calls)
    assert set(FIELDS) == SUPPORTED_FIELDS


@pytest.mark.parametrize("fq", [None, "pre", "post"])
@pytest.mark.parametrize("panel", [False, True])
def test_multi_security_matches_independent_single_security(fq, panel):
    p = GmDataProvider({"client": Market()})
    codes = ["601318.XSHG", "600000.XSHG"]
    kw = dict(
        start_date="2024-07-25",
        end_date="2024-07-26",
        fields=FIELDS,
        skip_paused=True,
        fq=fq,
        pre_factor_ref_date="2024-07-26",
    )
    actual = p.get_price(codes, panel=panel, **kw)
    for code in codes:
        expected = p.get_price(code, **kw)
        part = (
            actual.xs(code, axis=1, level=1)
            if panel
            else actual.loc[actual.code == code].set_index("time")[FIELDS]
        )
        pd.testing.assert_frame_equal(part, expected, check_names=False)


@pytest.mark.parametrize("frequency", ["daily", "1m", "5m", "15m", "30m", "60m"])
@pytest.mark.parametrize("df", [False, True])
@pytest.mark.parametrize("ref", [None, "2024-07-26"])
def test_bars_count_and_record_output(frequency, df, ref):
    p = GmDataProvider({"client": Market(frequency != "daily")})
    result = p.get_bars(
        "601318.XSHG",
        1,
        unit=frequency,
        end_dt="2024-07-26",
        fields=["date", "open", "close", "volume"],
        fq_ref_date=ref,
        df=df,
    )
    assert len(result) == 1
    assert list(result.columns if df else result.dtype.names) == ["date", "open", "close", "volume"]
    row = result.iloc[0] if df else result[0]
    assert row["open"] == 9
    assert row["close"] == 10
    assert row["volume"] == 200 * (1 if frequency == "daily" else int(frequency[:-1]))


def pause_provider(case):
    """合成输入仅验证转换契约；盘中暂停有源端零成交 bar，不假装证明 GM 真实覆盖。"""
    req = case["request"]
    symbol = ("SZSE." if req["security"].endswith("XSHE") else "SHSE.") + req["security"][:6]
    minute = req["frequency"] == "1m"
    days = case["days"]
    rows = []
    for day in days:
        stamps = (
            pd.date_range(day + " 09:31", day + " 11:30", freq="min").append(
                pd.date_range(day + " 13:01", day + " 15:00", freq="min")
            )
            if minute
            else [pd.Timestamp(day)]
        )
        for t in stamps:
            paused = pd.Timestamp(case["pause_start"]) <= t <= pd.Timestamp(case["pause_end"])
            if paused and case["full_day"]:
                continue
            rows.append(
                bar(str(t), close=case["pause_price"], volume=0 if paused else 100, symbol=symbol)
            )

    def query(method, kw):
        if method in {"get_instrumentinfos", "get_symbol_infos"}:
            return [dict(INFO, symbol=symbol, price_tick=0.01 if case["full_day"] else 0.001)]
        if method == "get_trading_dates":
            return [d for d in days if kw["start_date"] <= d <= kw["end_date"]]
        if method == "get_history_instruments":
            return [
                dict(symbol=symbol, trade_date=d, is_suspended=int(d == "2026-06-15")) for d in days
            ]
        if method == "history_n" and kw["frequency"] == "1d" and minute:
            return [bar("2026-06-12", close=9.48, symbol=symbol)]
        if method in {"history", "history_n"}:
            values = [r for r in rows if pd.Timestamp(r["eob"]) <= pd.Timestamp(kw["end_time"])]
            if method == "history_n":
                return values[-kw["count"] :]
            return [r for r in values if pd.Timestamp(r["eob"]) >= pd.Timestamp(kw["start_time"])]
        raise AssertionError(method)

    return GmDataProvider({"client": Client(query)})


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_shared_pause_contract_with_synthetic_gm_payload(case):
    assert_pause_contract(pause_provider(case).get_price(**case["request"]), case)


# 每个基础接口必须属于已覆盖能力或明确未实现，新增基础接口会使此表测试失败。
SUPPORTED = {
    "auth",
    "get_price",
    "get_trade_days",
    "get_all_securities",
    "get_index_stocks",
    "get_split_dividend",
    "get_security_info",
    "get_bars",
    "get_ticks",
    "get_current_tick",
}
UNSUPPORTED = {
    "get_extras": ("is_st", ["601318.XSHG"]),
    "get_fundamentals": (None,),
    "get_fundamentals_continuously": (None,),
    "get_index_weights": ("000300.XSHG",),
    "get_industry_stocks": ("example",),
    "get_industry": ("601318.XSHG",),
    "get_concept_stocks": ("example",),
    "get_concept": ("601318.XSHG",),
    "get_fund_info": ("511880.XSHG",),
    "get_margincash_stocks": (),
    "get_marginsec_stocks": (),
    "get_dominant_future": ("IF",),
    "get_future_contracts": ("IF",),
    "get_futures_info": (["IF"],),
    "get_billboard_list": (),
    "get_locked_shares": ([],),
    "get_trade_day": ("601318.XSHG", "2024-07-25"),
    "subscribe_ticks": ([],),
    "subscribe_markets": ([],),
    "unsubscribe_ticks": (),
    "unsubscribe_markets": (),
}


def test_every_base_public_method_has_explicit_capability_classification():
    methods = {
        name
        for name, value in inspect.getmembers(DataProvider, inspect.isfunction)
        if not name.startswith("_")
    }
    assert methods == SUPPORTED | UNSUPPORTED.keys()


@pytest.mark.parametrize("method,args", UNSUPPORTED.items())
def test_unsupported_interfaces_fail_explicitly_without_sdk(method, args):
    p = GmDataProvider({"client": Client(lambda *a: pytest.fail("unsupported must not query SDK"))})
    with pytest.raises(NotImplementedError):
        getattr(p, method)(*args)


def test_count_query_cannot_return_future_bar():
    p = GmDataProvider({"client": Client(lambda *a: [bar("2024-07-26")])})
    with pytest.raises(GmDataError, match="未来"):
        p.get_price("601318.XSHG", end_date="2024-07-25", count=1, skip_paused=True, fq=None)


@pytest.mark.parametrize("bad", [0, -1, True, 1.5])
@pytest.mark.parametrize("method", ["get_price", "get_trade_days", "get_ticks"])
def test_invalid_counts_rejected_before_sdk(bad, method):
    p = GmDataProvider({"client": Client(lambda *a: pytest.fail("invalid request called SDK"))})
    kw = {"count": bad}
    if method == "get_price":
        kw.update(security="601318.XSHG", end_date="2024-07-26")
    elif method == "get_ticks":
        kw.update(security="601318.XSHG", end_dt="2024-07-26")
    with pytest.raises(ValueError):
        getattr(p, method)(**kw)


@pytest.mark.parametrize("days", [["2024-07-25"] * 2, ["2024-07-26", "2024-07-25"], ["2024-07-29"]])
def test_bad_calendar_never_silently_sorts_or_clips(days):
    p = GmDataProvider({"client": Client(lambda *a: days)})
    with pytest.raises(GmDataError):
        p.get_trade_days("2024-07-25", "2024-07-26")


@pytest.mark.parametrize(
    "kind,group,detail,board",
    [
        ("stock", 1010, 101001, 10100101),
        ("etf", 1020, 102001, 10200101),
        ("mmf", 1020, 102001, 10200105),
        ("lof", 1020, 102002, 10200201),
        ("index", 1060, 106001, 10600101),
    ],
)
def test_security_types_and_listing_boundaries(kind, group, detail, board):
    info = dict(
        INFO,
        sec_type={1010: 1, 1020: 2, 1060: 3}[group],
        sec_type1=group,
        sec_type2=detail,
        board=board,
        listed_date="2024-07-25",
        delisted_date="2024-07-27",
    )
    p = GmDataProvider({"client": Client(lambda *a: [info])})
    assert p.get_security_info("601318.XSHG", "2024-07-25")["type"] == kind
    assert list(p.get_all_securities(kind, "2024-07-26").index) == ["601318.XSHG"]
    for day in ["2024-07-24", "2024-07-27"]:
        assert p.get_all_securities(kind, day).empty
        with pytest.raises(ValueError):
            p.get_security_info("601318.XSHG", day)


@pytest.mark.parametrize("sec_type,base", [(1, 10), (2, 1)])
def test_cash_stock_transfer_dividend_units_and_date_filter(sec_type, base):
    def query(method, kw):
        if method in {"get_symbol_infos", "get_instrumentinfos"}:
            return [dict(INFO, sec_type=sec_type)]
        assert method == "get_dividend"
        return [
            dict(
                symbol=INFO["symbol"],
                created_at=day,
                cash_div=0.25,
                share_div_ratio=0.2,
                share_trans_ratio=0.3,
                allotment_ratio=0,
            )
            for day in ["2024-07-24", "2024-07-25", "2024-07-27"]
        ]

    p = GmDataProvider({"client": Client(query)})
    events = p.get_split_dividend("601318.XSHG", "2024-07-25", "2024-07-26")
    assert len(events) == 1
    assert events[0]["per_base"] == base
    assert events[0]["bonus_pre_tax"] == 0.25 * base
    assert events[0]["scale_factor"] == 1.5


@pytest.mark.parametrize("offset", [-6, 2, 0])
def test_live_current_freshness_and_limits(monkeypatch, offset):
    from bullet_trade.data.providers import gm

    original = gm._timestamp
    now = pd.Timestamp("2024-07-26 10:30:00")
    monkeypatch.setattr(
        gm, "_timestamp", lambda value=None, **kw: now if value is None else original(value, **kw)
    )

    def query(method, kw):
        if method == "current":
            return [
                dict(
                    symbol=INFO["symbol"],
                    created_at=str(now + pd.Timedelta(seconds=offset)),
                    price=10,
                    cum_volume=200,
                    cum_amount=1900,
                )
            ]
        return Market().query(method, **kw)

    p = GmDataProvider({"client": Client(query)})
    if offset:
        with pytest.raises(GmDataError, match="过期|未来"):
            p.get_live_current("601318.XSHG")
    else:
        result = p.get_live_current("601318.XSHG")
        assert (
            result["last_price"],
            result["high_limit"],
            result["low_limit"],
            result["paused"],
        ) == (10, 11, 8, False)


@pytest.mark.parametrize("payload", ["valid", "empty", "duplicate"])
def test_index_constituents_use_last_trading_day_and_reject_bad_snapshot(payload):
    market = Market()

    def query(method, kw):
        if method == "stk_get_index_constituents":
            assert kw == {"index": "SHSE.000300", "trade_date": "2024-07-26"}
            rows = [{"symbol": "SHSE.601318"}, {"symbol": "SZSE.000001"}]
            return [] if payload == "empty" else rows + rows if payload == "duplicate" else rows
        return market.query(method, **kw)

    p = GmDataProvider({"client": Client(query)})
    if payload == "valid":
        assert p.get_index_stocks("000300.XSHG", "2024-07-28") == ["000001.XSHE", "601318.XSHG"]
    else:
        with pytest.raises(GmDataError):
            p.get_index_stocks("000300.XSHG", "2024-07-28")


@pytest.mark.parametrize("df", [False, True])
@pytest.mark.parametrize("missing_book", [False, True])
def test_tick_five_levels_and_output_shapes(df, missing_book):
    quotes = [
        dict(ask_p=10 + i * 0.01, bid_p=10 - i * 0.01, ask_v=100 * i, bid_v=200 * i)
        for i in range(1, 6)
    ]
    row = dict(
        symbol=INFO["symbol"],
        created_at="2024-07-26 10:30:00+08:00",
        price=10,
        cum_volume=200,
        cum_amount=1900,
        quotes=quotes[:1] if missing_book else quotes,
    )
    p = GmDataProvider({"client": Client(lambda *a: [row])})
    fields = ["current", "a5_p", "a5_v", "b5_p", "b5_v"]
    if missing_book:
        with pytest.raises(GmDataError, match="盘口"):
            p.get_ticks("601318.XSHG", "2024-07-26 10:30", count=1, fields=fields, df=df)
    else:
        actual = p.get_ticks("601318.XSHG", "2024-07-26 10:30", count=1, fields=fields, df=df)
        values = actual.iloc[0].tolist() if df else list(actual[0])
        assert values == [10, 10.05, 500, 9.95, 1000]


@pytest.mark.parametrize("method", ["history", "attribute_history", "get_bars"])
def test_public_api_history_routes_through_gm_provider(monkeypatch, method):
    from types import SimpleNamespace
    from bullet_trade.data import api
    from bullet_trade.core.settings import reset_settings

    reset_settings()
    p = GmDataProvider({"client": Market()})
    monkeypatch.setattr(api, "_provider", p)
    monkeypatch.setattr(api, "_auth_attempted", False)
    monkeypatch.setattr(
        api,
        "_current_context",
        SimpleNamespace(current_dt=pd.Timestamp("2024-07-27 10:00"), run_params={}),
    )
    try:
        if method == "history":
            actual = api.history(1, field="close", security_list=["601318.XSHG"], fq=None)
            assert actual.iloc[0, 0] == 10
        elif method == "attribute_history":
            actual = api.attribute_history("601318.XSHG", 1, fields=["close"], fq=None)
            assert actual.close.tolist() == [10]
        else:
            actual = api.get_bars("601318.XSHG", 1, fields=["close"], fq_ref_date=None, df=True)
            assert actual.close.tolist() == [10]
    finally:
        reset_settings()
