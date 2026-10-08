"""实用容差不是直接改通过标志：小差异可接受，结构/量纲/大偏差必须失败。"""

import numpy as np
import pandas as pd
import pytest

from bullet_trade.integrations.gm.acceptance import compare_practical, normalize_post
from bullet_trade.integrations.gm.validation import compare_bars


def frame():
    return pd.DataFrame(
        dict(
            open=[3.809, 3.81],
            high=[3.809, 3.81],
            low=[3.809, 3.81],
            close=[3.809, 3.81],
            volume=[51_035_515.0, 50_000_000.0],
            money=[45_504_167.0, 40_000_000.0],
        ),
        index=pd.to_datetime(["2024-07-25", "2024-07-26"]),
    )


def test_authorized_small_differences_pass_but_remain_visible_in_strict_diagnostic():
    reference = frame()
    observed = reference.copy()
    observed["close"] -= 0.002
    observed["volume"] += 12
    observed["money"] += 1
    assert not compare_bars(reference, observed, 0.001)["ok"]
    result = compare_practical(reference, observed, 0.001)
    assert result["ok"]
    assert result["fields"]["volume"]["max_abs_diff"] == 12
    assert result["fields"]["money"]["max_abs_diff"] == 1


@pytest.mark.parametrize(
    "kind",
    [
        "missing_row",
        "extra_row",
        "duplicate",
        "unsorted",
        "time_shift",
        "missing_field",
        "extra_field",
        "duplicate_column",
        "nan",
        "inf",
        "negative",
        "text",
        "wrong_volume_unit",
        "large_price",
        "large_volume",
        "large_money",
        "zero_reference",
    ],
)
def test_invalid_or_materially_different_data_still_fails(kind):
    reference = frame()
    observed = reference.copy()
    if kind == "missing_row":
        observed = observed.iloc[:1]
    elif kind == "extra_row":
        observed.loc[pd.Timestamp("2024-07-29")] = observed.iloc[-1]
    elif kind == "duplicate":
        observed = pd.concat([observed, observed.iloc[:1]])
    elif kind == "unsorted":
        observed = observed.iloc[::-1]
    elif kind == "time_shift":
        observed.index += pd.Timedelta(minutes=1)
    elif kind == "missing_field":
        observed = observed.drop(columns="volume")
    elif kind == "extra_field":
        observed["unknown"] = 1
    elif kind == "duplicate_column":
        observed = pd.concat([observed, observed[["close"]]], axis=1)
    elif kind == "nan":
        observed.iloc[0, 0] = np.nan
    elif kind == "inf":
        observed.iloc[0, 0] = np.inf
    elif kind == "negative":
        observed.iloc[0, 0] = -0.001
    elif kind == "text":
        observed["close"] = ["invalid", "3.81"]
    elif kind == "wrong_volume_unit":
        observed["volume"] *= 100
    elif kind == "large_price":
        observed["close"] *= 1.01
    elif kind == "large_volume":
        observed["volume"] *= 1.01
    elif kind == "large_money":
        observed["money"] += 100
    elif kind == "zero_reference":
        reference["volume"] = 0
        observed["volume"] = 100
    assert not compare_practical(reference, observed, 0.001)["ok"]


@pytest.mark.parametrize("relative_change,expected", [(0.001, True), (0.001001, False)])
def test_relative_tolerance_boundary(relative_change, expected):
    reference = frame()
    observed = reference.copy()
    observed["volume"] *= 1 + relative_change
    assert compare_practical(reference, observed, 0.001)["ok"] is expected


@pytest.mark.parametrize(
    "kind", ["different_status", "invalid_status", "nonzero_volume", "nonzero_money"]
)
def test_halt_fact_is_exact_and_turnover_must_be_zero(kind):
    reference = frame()
    reference["paused"] = 1
    reference[["volume", "money"]] = 0.0
    observed = reference.copy()
    if kind == "different_status":
        observed["paused"] = 0
    elif kind == "invalid_status":
        observed["paused"] = 0.5
    elif kind == "nonzero_volume":
        observed["volume"] = 0.1
    elif kind == "nonzero_money":
        observed["money"] = 0.1
    assert not compare_practical(reference, observed, 0.001)["ok"]


def test_empty_is_accepted_only_for_declared_holiday():
    empty = frame().iloc[:0]
    assert not compare_practical(empty, empty, 0.001)["ok"]
    assert compare_practical(empty, empty, 0.001, allow_empty=True)["ok"]
    assert not compare_practical(empty, frame(), 0.001, allow_empty=True)["ok"]


@pytest.mark.parametrize("factor", [0, -1, np.nan, np.inf])
def test_invalid_post_basis_is_not_normalized(factor):
    with pytest.raises(ValueError):
        normalize_post(frame(), factor)


def post_frames():
    raw = frame()
    reference, observed = raw.copy(), raw.copy()
    reference_factor = np.array([10.0, 11.0])
    observed_factor = 1.25 * reference_factor
    for value, factors in [(reference, reference_factor), (observed, observed_factor)]:
        value[["open", "high", "low", "close"]] *= factors[:, None]
        value["volume"] /= factors
        value["factor"] = factors
    return reference, observed


def test_common_basis_cancels_constant_factor_scale_without_changing_money_or_dates():
    reference, observed = post_frames()
    before = observed.copy()
    a, b = normalize_post(reference, 10.0), normalize_post(observed, 12.5)
    assert compare_practical(a, b, 0.001)["ok"]
    assert b.index.equals(observed.index) and b.money.equals(observed.money)
    pd.testing.assert_frame_equal(observed, before)


def test_common_basis_does_not_hide_wrong_event_growth():
    reference, observed = post_frames()
    observed.loc[observed.index[-1], ["close", "factor"]] *= 1.1
    assert not compare_practical(
        normalize_post(reference, 10.0), normalize_post(observed, 12.5), 0.001
    )["ok"]


def test_common_basis_does_not_fit_price_ratio_to_make_wrong_prices_pass():
    reference, observed = post_frames()
    observed["close"] *= 1.25
    assert not compare_practical(
        normalize_post(reference, 10.0), normalize_post(observed, 12.5), 0.001
    )["ok"]
