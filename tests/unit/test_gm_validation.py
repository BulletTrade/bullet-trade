"""GM 数据验收负例：行缺口、非有限数、单位、时区和账户错误不得误报通过。"""

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from bullet_trade.integrations.gm.validation import (
    BAR_FIELDS,
    compare_bars,
    compare_dividends,
    gm_bars,
    gm_symbol,
    jq_symbol,
    jq_tick_time,
    legacy_dividends,
    local_time,
    validate_account,
)
from bullet_trade.integrations.gm.validation_worker import execute


def prices():
    return pd.DataFrame(
        {k: [10.0, 12.0] for k in BAR_FIELDS}, index=pd.to_datetime(["2024-07-25", "2024-07-26"])
    )


def test_full_fields_exact_dates_and_asia_timezone():
    left = prices()
    right = left.copy()
    right.index = right.index.tz_localize("Asia/Shanghai").tz_convert("UTC")
    assert compare_bars(left, right, 0.01)["ok"]
    assert local_time("2024-07-25T16:00:00Z") == pd.Timestamp("2024-07-26")


@pytest.mark.parametrize(
    "kind",
    [
        "missing_date",
        "extra_date",
        "duplicate",
        "unsorted",
        "missing_field",
        "nan",
        "inf",
        "string",
        "wrong_volume_unit",
        "price_diff",
        "money_diff",
    ],
)
def test_bad_data_fails_without_intersection_masking(kind):
    left = prices()
    right = left.copy()
    if kind == "missing_date":
        right = right.iloc[:1]
    elif kind == "extra_date":
        right.loc[pd.Timestamp("2024-07-29")] = right.iloc[-1]
    elif kind == "duplicate":
        right = pd.concat([right, right.iloc[:1]])
    elif kind == "unsorted":
        right = right.iloc[::-1]
    elif kind == "missing_field":
        right = right.drop(columns="money")
    elif kind == "nan":
        right.iloc[0, 0] = np.nan
    elif kind == "inf":
        right.iloc[0, 0] = np.inf
    elif kind == "string":
        right["open"] = ["bad", "12"]
    elif kind == "wrong_volume_unit":
        right["volume"] *= 100
    elif kind == "price_diff":
        right["close"] += 0.02
    elif kind == "money_diff":
        right["money"] += 1
    assert not compare_bars(left, right, 0.01)["ok"]


def test_empty_data_requires_predeclared_holiday_case():
    empty = prices().iloc[:0]
    assert not compare_bars(empty, empty, 0.01)["ok"]
    assert compare_bars(empty, empty, 0.01, allow_empty=True)["ok"]
    assert not compare_bars(empty, prices(), 0.01, allow_empty=True)["ok"]


def test_normalization_checks_security_and_does_not_change_units():
    row = {
        "symbol": "SHSE.510300",
        "eob": "2026-09-30T00:00:00+08:00",
        "open": 4.0,
        "high": 4.0,
        "low": 4.0,
        "close": 4.0,
        "volume": 1200,
        "amount": 4800,
    }
    frame = gm_bars([row], "510300.XSHG")
    assert frame.volume.iloc[0] == 1200
    assert frame.money.iloc[0] == 4800
    with pytest.raises(ValueError):
        gm_bars([row], "000001.XSHE")
    with pytest.raises(ValueError):
        gm_bars([{k: v for k, v in row.items() if k != "eob"}], "510300.XSHG")
    assert jq_symbol(gm_symbol("000001.XSHG")) == "000001.XSHG"
    with pytest.raises(ValueError):
        gm_symbol("000001")


def test_dividend_cash_per_share_and_transfer_are_not_tenfold():
    events = legacy_dividends(
        [
            {
                "symbol": "SHSE.601318",
                "created_at": "2024-07-26",
                "cash_div": 1.5,
                "share_div_ratio": 0.2,
                "share_trans_ratio": 0.6,
            }
        ],
        "601318.XSHG",
    )
    assert events[0]["date"] == "2024-07-26"
    assert events[0]["cash_per_share"] == 1.5
    assert events[0]["scale_factor"] == pytest.approx(1.8)
    assert compare_dividends(events, events)["ok"]
    assert not compare_dividends(events, [{**events[0], "cash_per_share": 15}])["ok"]
    assert not compare_dividends(events, events + events)["ok"]
    assert not compare_dividends(events, [{**events[0], "cash_per_share": float("nan")}])["ok"]
    assert not compare_dividends([], [])["ok"]
    with pytest.raises(NotImplementedError):
        legacy_dividends([{"symbol": "SHSE.601318", "allotment_ratio": 0.1}], "601318.XSHG")


def account():
    return (
        {"state": 3, "error_code": 0},
        {
            "nav": 1000000.0,
            "balance": 1000000.0,
            "available": 1000000.0,
            "market_value": 0.0,
            "frozen": 0.0,
            "updated_at": "2026-09-30",
        },
        [],
        {
            k: {"status_code": 0, "rows": 0}
            for k in ["orders", "unfinished_orders", "execution_reports"]
        },
    )


def test_empty_logged_in_account_passes_readonly_but_is_not_fill_evidence():
    r = validate_account(*account())
    assert r["ok"]
    assert "empty_account_does_not_validate_fills_or_T1" in r["limitations"]


@pytest.mark.parametrize(
    "kind",
    [
        "offline",
        "cash_nan",
        "missing_timestamp",
        "broken_nav",
        "negative_available",
        "missing_query",
        "native_error",
        "wrong_position",
        "missing_market_value",
        "infinite_position",
        "missing_query_rows",
        "negative_frozen",
    ],
)
def test_account_false_positive_cases(kind):
    state, cash, pos, queries = account()
    if kind == "offline":
        state["state"] = 5
    elif kind == "cash_nan":
        cash["balance"] = float("nan")
    elif kind == "missing_timestamp":
        cash.pop("updated_at")
    elif kind == "broken_nav":
        cash["nav"] += 100
    elif kind == "negative_available":
        cash["available"] = -1
    elif kind == "missing_query":
        queries.pop("orders")
    elif kind == "native_error":
        queries["orders"]["status_code"] = 5
    elif kind == "wrong_position":
        pos = [{"volume": 100, "available": 200}]
    elif kind == "missing_market_value":
        cash["market_value"] = 100
    elif kind == "infinite_position":
        pos = [{"volume": float("inf"), "available": 100, "market_value": 0}]
    elif kind == "missing_query_rows":
        queries["orders"].pop("rows")
    elif kind == "negative_frozen":
        cash["frozen"] = -100
    assert not validate_account(state, cash, pos, queries)["ok"]


def test_worker_rejects_trading_before_any_sdk_action():
    api = SimpleNamespace()
    for method in ["run", "order_volume", "order_cancel", "universe_set", "set_token"]:
        with pytest.raises(ValueError):
            execute(api, {"cases": [{"id": "bad", "method": method, "kwargs": {}}]})


def test_worker_enforces_explicit_account_and_unique_request_ids():
    with pytest.raises(ValueError):
        execute(
            SimpleNamespace(),
            {"cases": [{"id": "account", "method": "account_readonly", "kwargs": {}}]},
        )
    with pytest.raises(ValueError):
        execute(SimpleNamespace(), {"cases": [{"id": "x", "method": "history", "kwargs": {}}] * 2})


def test_worker_masks_sdk_error_details_and_tokens():
    def fail(**_):
        raise RuntimeError("private-token account-123")

    api = SimpleNamespace(
        set_token=lambda _: None,
        set_serv_addr=lambda _: None,
        get_version=lambda: "3.0.186",
        history=fail,
    )
    value = execute(
        api, {"token": "private-token", "cases": [{"id": "h", "method": "history", "kwargs": {}}]}
    )
    assert value["cases"]["h"] == {
        "status": "error",
        "error_type": "RuntimeError",
        "error_code": None,
        "reason": "sdk_error",
    }
    assert "private-token" not in json.dumps(value)
    assert "account-123" not in json.dumps(value)


@pytest.mark.parametrize("value", [20260930145701.0, 20260930145701, "20260930145701"])
def test_rpc_numeric_tick_time_is_not_unix_nanoseconds(value):
    assert jq_tick_time(value) == pd.Timestamp("2026-09-30 14:57:01")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 1.5, "202609"])
def test_rpc_bad_tick_time_fails(value):
    with pytest.raises(ValueError):
        jq_tick_time(value)


def test_rpc_tick_fractional_seconds_are_preserved():
    assert jq_tick_time(20260930145701.5) == pd.Timestamp("2026-09-30 14:57:01.500")
