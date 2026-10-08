from copy import deepcopy

import pytest

from bullet_trade.integrations.ths.windows.driver import DriverBlocked, stable_funds_data


def records():
    result = []
    for label_id, label, value_id, value, y in (
        (2388, "资金余额", 1012, "100.00", 0),
        (1686, "冻结金额", 1013, "20.00", 30),
        (2005, "可用金额", 1016, "80.00", 60),
        (1688, "总 资 产", 1015, "150.00", 90),
    ):
        for cid, text, rect in ((label_id, label, [0, y, 100, y+20]),
                                (value_id, value, [105, y, 180, y+20])):
            result.append(dict(control_id=cid, text=text, rectangle=rect,
                               main_hwnd=77, class_name="Static", visible=True))
    return result


def test_intraday_valuation_changes_without_cash_change():
    first = records()
    second = deepcopy(first)
    second[-1]["text"] = "151.25"
    assert stable_funds_data(first, list(reversed(second)), 77) == {
        "cash_balance": "100.00", "available_cash": "80.00",
        "frozen_cash": "20.00", "total_value": "151.25"}


@pytest.mark.parametrize("change", ["cash", "freeze", "label", "geometry", "window", "duplicate", "invalid_total"])
def test_intraday_valuation_does_not_relax_cash_or_identity(change):
    first = records()
    second = deepcopy(first)
    if change == "cash":
        second[1]["text"], second[5]["text"] = "101.00", "81.00"
    elif change == "freeze":
        second[3]["text"], second[5]["text"] = "21.00", "79.00"
    elif change == "label":
        second[-2]["text"] = "总资产"
    elif change == "geometry":
        second[-2]["rectangle"] = [1, 90, 101, 110]
        second[-1]["rectangle"] = [106, 90, 181, 110]
    elif change == "window":
        second[-1]["main_hwnd"] = 78
    elif change == "duplicate":
        second.append(deepcopy(second[-1]))
    else:
        second[-1]["text"] = "99.00"
    with pytest.raises(DriverBlocked):
        stable_funds_data(first, second, 77)


def test_invalid_first_frame_is_not_hidden_by_valid_second():
    first, second = records(), records()
    first[3]["text"] = "21.00"
    with pytest.raises(DriverBlocked):
        stable_funds_data(first, second, 77)
