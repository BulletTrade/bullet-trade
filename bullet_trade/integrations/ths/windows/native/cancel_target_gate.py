"""离线精确撤单目标门禁；许可仅针对给定快照，不代表柜台受理或终态。

调用方须独立核实账户、交易日、可撤页面身份、全量覆盖及数据新鲜度，
并在 GUI 中核对目标控件。table_snapshot.parse_table 的 unknown 结果不能放行。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


class CancelTargetError(ValueError):
    """目标或快照不满足精确撤单前置条件。"""


@dataclass(frozen=True)
class CancelableOrder:
    account: str
    trade_day: str
    contract_no: str  # 柜台原始文本，绝不可转整数或剥去前导零。
    security: str
    side: str
    order_quantity: int
    filled_quantity: int
    canceled_quantity: int
    remaining_quantity: int
    status: str


@dataclass(frozen=True)
class CancelableOrdersSnapshot:
    account: str
    trade_day: str
    rows: tuple[CancelableOrder, ...]
    completeness: str
    page_identity: str
    coverage_evidence_ref: str


@dataclass(frozen=True)
class ExactCancelPermit:
    account: str
    trade_day: str
    contract_no: str
    security: str
    side: str
    order_quantity: int
    remaining_quantity: int
    coverage_evidence_ref: str
    decision: str = "may_initiate_exact_cancel"


# 仅接纳明确处于可撤阶段的状态。遇到客户端新增或模糊状态时先拒绝。
_CANCELABLE_STATUSES = frozenset({"已报", "部成"})
_SIDES = frozenset({"买入", "卖出"})


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CancelTargetError(f"{name}必须是未经变形的非空文本")
    return value


def _day(value: object) -> str:
    value = _text(value, "交易日")
    try:
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError
    except ValueError as exc:
        raise CancelTargetError("交易日必须为 YYYY-MM-DD") from exc
    return value


def _quantity(value: object, name: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise CancelTargetError(f"{name}必须是合法整数")
    return value


def permit_exact_cancel(
    *, account: str, trade_day: str, contract_no: str, security: str,
    side: str, order_quantity: int, snapshot: CancelableOrdersSnapshot,
) -> ExactCancelPermit:
    """只在完整快照中唯一、完全匹配且仍有可撤余额时给出发起许可。"""
    account = _text(account, "账户")
    trade_day = _day(trade_day)
    contract_no = _text(contract_no, "原始合同号")
    security = _text(security, "证券")
    side = _text(side, "方向")
    if side not in _SIDES:
        raise CancelTargetError("方向不可识别")
    order_quantity = _quantity(order_quantity, "委托量", positive=True)
    if not isinstance(snapshot, CancelableOrdersSnapshot):
        raise CancelTargetError("需要规范化的可撤订单快照")
    if snapshot.completeness != "complete" or snapshot.page_identity != "cancelable":
        raise CancelTargetError("可撤订单快照的完整性或页面身份未核实")
    if (_text(snapshot.account, "快照账户"), _day(snapshot.trade_day)) != (account, trade_day):
        raise CancelTargetError("快照账户或交易日不符")
    _text(snapshot.coverage_evidence_ref, "全量覆盖证据引用")
    if not isinstance(snapshot.rows, tuple):
        raise CancelTargetError("快照订单行必须为不可变序列")

    candidates: list[CancelableOrder] = []
    for row in snapshot.rows:
        if not isinstance(row, CancelableOrder):
            raise CancelTargetError("可撤订单行格式不符")
        if (_text(row.account, "行账户"), _day(row.trade_day)) != (account, trade_day):
            raise CancelTargetError("快照混有其他账户或交易日")
        _text(row.contract_no, "行原始合同号")
        _text(row.security, "行证券")
        _text(row.side, "行方向")
        _quantity(row.order_quantity, "行委托量", positive=True)
        _quantity(row.filled_quantity, "行已成量")
        _quantity(row.canceled_quantity, "行已撤量")
        _quantity(row.remaining_quantity, "行剩余量")
        _text(row.status, "行状态")
        if row.contract_no == contract_no:
            candidates.append(row)

    if len(candidates) != 1:
        raise CancelTargetError("目标合同号缺失或存在多匹配")
    row = candidates[0]
    if (row.security, row.side, row.order_quantity) != (security, side, order_quantity):
        raise CancelTargetError("目标证券、方向或委托量不符")
    if row.order_quantity != row.filled_quantity + row.canceled_quantity + row.remaining_quantity:
        raise CancelTargetError("已成、已撤和剩余数量不守恒")
    if (row.status == "部成" and row.filled_quantity == 0
            or row.status == "已报" and (row.filled_quantity != 0
                                        or row.canceled_quantity != 0)):
        raise CancelTargetError("目标状态与已成已撤数量不一致")
    if row.status not in _CANCELABLE_STATUSES or row.remaining_quantity == 0:
        raise CancelTargetError("目标状态不可撤或无剩余量")
    return ExactCancelPermit(account, trade_day, contract_no, security, side,
                             order_quantity, row.remaining_quantity,
                             snapshot.coverage_evidence_ref)
