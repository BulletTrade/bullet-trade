"""资金股票页控件快照的纯离线解析；采样与账户/刷新证明由调用方负责。"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Mapping, Sequence


# 2026-09-28 只读枚举的固定映射；控件 ID 本身不是账户身份或刷新证明。
FIELDS = (
    ("balance", 2388, "资金余额", 1012),
    ("frozen", 1686, "冻结金额", 1013),
    ("available", 2005, "可用金额", 1016),
)
AMOUNT = re.compile(r"(?:0|[1-9][0-9]*|[1-9][0-9]{0,2}(?:,[0-9]{3})+)(?:\.[0-9]+)?\Z")


class FundsControlUnverified(ValueError):
    """原因码不含控件原文、账号或金额。"""


@dataclass(frozen=True)
class FundsControlReading:
    balance: str
    available: str
    frozen: str
    complete: bool
    reason: str


def _rect(record: Mapping[str, object]) -> tuple[int, int, int, int]:
    value = record.get("rectangle")
    if (not isinstance(value, (list, tuple)) or len(value) != 4
            or any(type(part) is not int for part in value)):
        raise FundsControlUnverified("invalid_control_rectangle")
    left, top, right, bottom = value
    if left >= right or top >= bottom:
        raise FundsControlUnverified("invalid_control_rectangle")
    return left, top, right, bottom


def _amount(record: Mapping[str, object]) -> tuple[str, Decimal]:
    raw = record.get("text")
    if not isinstance(raw, str) or AMOUNT.fullmatch(raw.strip()) is None:
        raise FundsControlUnverified("invalid_funds_amount")
    normalized = raw.strip().replace(",", "")
    try:
        value = Decimal(normalized)
    except InvalidOperation as exc:
        raise FundsControlUnverified("invalid_funds_amount") from exc
    if not value.is_finite() or value < 0:
        raise FundsControlUnverified("invalid_funds_amount")
    return normalized, value


def read_funds_controls(
    records: Sequence[Mapping[str, object]], *, main_hwnd: int,
    page: str, login_state: str, account_verified: bool,
    sample_current: bool, zero_balance_confirmed: bool = False,
) -> FundsControlReading:
    """解析同一主窗的显式 Static 标签/数值对。

    每条记录须由采样器附上 ``main_hwnd``；调用方须独立证明当前登录
    身份和本次刷新。这里不从掩码账户名或金额推断这些事实。
    """
    if type(main_hwnd) is not int or main_hwnd <= 0:
        raise FundsControlUnverified("main_window_unverified")
    if page != "资金股票":
        raise FundsControlUnverified("funds_page_unverified")
    if not isinstance(records, (list, tuple)):
        raise FundsControlUnverified("controls_unverified")
    wanted = {item for _, label_id, _, value_id in FIELDS for item in (label_id, value_id)}
    found: dict[int, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise FundsControlUnverified("controls_unverified")
        control_id = record.get("control_id")
        if type(control_id) is not int or control_id not in wanted:
            continue
        if control_id in found:
            raise FundsControlUnverified("funds_control_not_unique")
        if record.get("main_hwnd") != main_hwnd:
            raise FundsControlUnverified("funds_control_window_mismatch")
        if record.get("class_name") != "Static" or record.get("visible") is not True:
            raise FundsControlUnverified("funds_control_not_visible_static")
        found[control_id] = record
    if set(found) != wanted:
        raise FundsControlUnverified("funds_control_missing")

    parsed: dict[str, tuple[str, Decimal]] = {}
    rows = []
    for field, label_id, label_text, value_id in FIELDS:
        label, value = found[label_id], found[value_id]
        if label.get("text") != label_text:
            raise FundsControlUnverified("funds_label_mismatch")
        lx1, ly1, lx2, ly2 = _rect(label)
        vx1, vy1, vx2, vy2 = _rect(value)
        overlap = min(ly2, vy2) - max(ly1, vy1)
        if (not 0 <= vx1 - lx2 <= 12 or overlap <= 0
                or overlap * 5 < min(ly2 - ly1, vy2 - vy1) * 4):
            raise FundsControlUnverified("funds_control_position_mismatch")
        rows.append((ly1, ly2))
        parsed[field] = _amount(value)
    if not (rows[0][1] < rows[1][0] and rows[1][1] < rows[2][0]):
        raise FundsControlUnverified("funds_control_position_mismatch")
    if parsed["balance"][1] != parsed["available"][1] + parsed["frozen"][1]:
        raise FundsControlUnverified("funds_amount_relation_mismatch")

    complete = (login_state == "logged_in" and account_verified is True
                and sample_current is True and
                (any(item[1] != 0 for item in parsed.values())
                 or zero_balance_confirmed is True))
    return FundsControlReading(
        balance=parsed["balance"][0], available=parsed["available"][0],
        frozen=parsed["frozen"][0], complete=complete,
        reason="independently_verified" if complete else "identity_or_refresh_unverified",
    )
