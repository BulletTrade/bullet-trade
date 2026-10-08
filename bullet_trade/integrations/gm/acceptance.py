"""GM 实用验收的纯计算规则；仅比较结果，不向数据适配器提供聚宽数据。

价格/量/因子允许千分之一相对差，金额允许一元或百万分之一。
后复权先用各自来源的同日因子归一化；结构、停牌和非有限数不豁免。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .validation import local_time

PRICE_FIELDS = frozenset(
    {"open", "high", "low", "close", "pre_close", "high_limit", "low_limit", "avg"}
)
PRACTICAL_RULES = {
    "price_relative": 0.001,
    "volume_relative": 0.001,
    "volume_absolute": 1.0,
    "money_relative": 1e-6,
    "money_absolute": 1.0,
    "factor_relative": 0.001,
    "factor_absolute": 1e-8,
    "paused_absolute": 0.0,
}


def normalize_post(frame: pd.DataFrame, anchor_factor: float) -> pd.DataFrame:
    """以本来源的同日累计因子统一后复权尺度；不能由两份价格反推比例。"""
    if not math.isfinite(anchor_factor) or anchor_factor <= 0:
        raise ValueError("后复权参考因子必须是有限正数")
    result = frame.copy()
    for field in PRICE_FIELDS | {"factor"}:
        if field in result:
            result[field] = pd.to_numeric(result[field], errors="raise") / anchor_factor
    if "volume" in result:
        result["volume"] = pd.to_numeric(result["volume"], errors="raise") * anchor_factor
    return result


def compare_practical(
    reference: pd.DataFrame,
    observed: pd.DataFrame,
    tick: float,
    *,
    allow_empty: bool = False,
) -> dict:
    """先验证结构，再逐行用 max(绝对容差, 基准绝对值 × 相对容差) 检查。"""
    if not math.isfinite(tick) or tick <= 0:
        raise ValueError("报价刻度必须为有限正数")
    left, right = reference.copy(), observed.copy()
    try:
        left.index = pd.DatetimeIndex([local_time(x) for x in left.index])
        right.index = pd.DatetimeIndex([local_time(x) for x in right.index])
    except (TypeError, ValueError):
        return {"ok": False, "reason": "invalid_time"}
    if (
        left.index.has_duplicates
        or right.index.has_duplicates
        or not left.index.is_monotonic_increasing
        or not right.index.is_monotonic_increasing
        or not left.index.equals(right.index)
        or left.columns.has_duplicates
        or right.columns.has_duplicates
        or set(left.columns) != set(right.columns)
    ):
        return {"ok": False, "reason": "structure_mismatch"}
    if not len(left):
        return {"ok": allow_empty and len(left.columns) > 0, "reason": "empty"}
    if not len(left.columns):
        return {"ok": False, "reason": "missing_fields"}
    checks = {}
    for field in left.columns:
        if field in PRICE_FIELDS:
            absolute, relative = tick, PRACTICAL_RULES["price_relative"]
        elif field in {"volume", "money", "factor"}:
            absolute = PRACTICAL_RULES[field + "_absolute"]
            relative = PRACTICAL_RULES[field + "_relative"]
        elif field == "paused":
            absolute, relative = 0.0, 0.0
        else:
            return {"ok": False, "reason": "unsupported_field"}
        try:
            a = pd.to_numeric(left[field], errors="raise").to_numpy(dtype=float)
            b = pd.to_numeric(right[field], errors="raise").to_numpy(dtype=float)
        except (TypeError, ValueError):
            return {"ok": False, "reason": "non_numeric"}
        if not np.isfinite(a).all() or not np.isfinite(b).all():
            return {"ok": False, "reason": "nonfinite"}
        if (a < 0).any() or (b < 0).any():
            return {"ok": False, "reason": "negative_value"}
        if field == "factor" and ((a <= 0).any() or (b <= 0).any()):
            return {"ok": False, "reason": "invalid_factor"}
        if field == "paused" and (not np.isin(a, [0, 1]).all() or not np.isin(b, [0, 1]).all()):
            return {"ok": False, "reason": "invalid_paused"}
        difference = np.abs(a - b)
        tolerance = np.maximum(absolute, np.abs(a) * relative)
        bad = difference > tolerance + 1e-10
        relative_error = np.divide(
            difference, np.abs(a), out=np.zeros_like(difference), where=a != 0
        )
        checks[field] = {
            "ok": bool(not bad.any()),
            "mismatch_rows": int(bad.sum()),
            "max_abs_diff": float(difference.max()),
            "max_relative_diff": float(relative_error.max()),
            "absolute_tolerance": absolute,
            "relative_tolerance": relative,
        }
    # 停牌时量额必须为零，不能因数值容差放行幽灵成交。
    for frame in (left, right):
        if "paused" in frame:
            halted = frame.paused == 1
            for field in ("volume", "money"):
                if field in frame and frame.loc[halted, field].ne(0).any():
                    return {"ok": False, "reason": "halted_nonzero_turnover"}
    return {"ok": all(c["ok"] for c in checks.values()), "fields": checks}
