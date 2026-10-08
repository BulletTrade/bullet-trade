"""掘金只读验收契约：时间、字段、分红单位和账户资产核对。

输入来自真实 GM 查询与聚宽 RPC；本模块无 SDK、网络或交易副作用。
缺字段、重复行、非有限数和缺失日期均失败，不用 inner join 掩盖缺口。
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd

BAR_FIELDS = ("open", "high", "low", "close", "volume", "money")


def gm_symbol(security: str) -> str:
    """转换已明确交易所的代码，禁止猜测六位代码的市场。"""
    match = re.fullmatch(r"(\d{6})\.(XSHG|XSHE)", security)
    if not match:
        raise ValueError("需要六位证券代码及 XSHG/XSHE 后缀")
    return ("SHSE." if match[2] == "XSHG" else "SZSE.") + match[1]


def jq_symbol(security: str) -> str:
    match = re.fullmatch(r"(SHSE|SZSE)\.(\d{6})", security)
    if not match:
        raise ValueError("无效掘金证券代码")
    return match[2] + (".XSHG" if match[1] == "SHSE" else ".XSHE")


def local_time(value: Any) -> pd.Timestamp:
    """统一为上海市场无时区时间；保留分钟，不做日期截断或聚合。"""
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("行情时间缺失")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return stamp


def jq_tick_time(value: Any) -> pd.Timestamp:
    """解析 RPC YYYYmmddHHMMSS 数值时间，拒绝非有限数，避免误当 Unix 纳秒。"""
    if isinstance(value, (int, float, np.integer, np.floating)):
        if not math.isfinite(value):
            raise ValueError("无效聚宽 tick 时间")
        number = Decimal(str(value))
        integer = int(number)
        digits = str(integer)
        if len(digits) == 14 and digits.isdigit():
            return pd.Timestamp(
                datetime.strptime(digits, "%Y%m%d%H%M%S")
                + timedelta(seconds=float(number - integer))
            )
        if number != integer:
            raise ValueError("无效聚宽 tick 毫秒时间")
        value = digits
    if isinstance(value, str) and value.isdigit():
        if len(value) not in (14, 17):
            raise ValueError("聚宽 tick 时间必须为秒或毫秒年月日格式")
        fmt = "%Y%m%d%H%M%S" + ("%f" if len(value) == 17 else "")
        return pd.Timestamp(datetime.strptime(value, fmt))
    return local_time(value)


def gm_bars(rows: List[Dict[str, Any]], security: str) -> pd.DataFrame:
    """转换 SDK 日线/分钟原行，成交量为股/份，amount 为元。"""
    frame = pd.DataFrame(rows)
    if not rows:
        return pd.DataFrame(columns=BAR_FIELDS, index=pd.DatetimeIndex([]))
    if "symbol" not in frame or not frame.symbol.eq(gm_symbol(security)).all():
        raise ValueError("行情行证券代码与请求不一致")
    if "eob" not in frame:
        raise ValueError("行情行缺少结束时间")
    frame.index = pd.DatetimeIndex([local_time(v) for v in frame.pop("eob")])
    return frame.rename(columns={"amount": "money"}).drop(columns=["symbol"])


def compare_bars(
    reference: pd.DataFrame, observed: pd.DataFrame, tick: float, allow_empty: bool = False
) -> Dict[str, Any]:
    """固定阈值逐字段核对，列出缺口；价格最多一个报价刻度，量差一份，金额1分+1e-8相对差。"""
    if not math.isfinite(tick) or tick <= 0:
        raise ValueError("无效报价刻度")
    left, right = reference.copy(), observed.copy()
    left.index = pd.DatetimeIndex([local_time(x) for x in left.index])
    right.index = pd.DatetimeIndex([local_time(x) for x in right.index])
    result: Dict[str, Any] = {
        "reference_rows": len(left),
        "observed_rows": len(right),
        "duplicate_reference": bool(left.index.has_duplicates),
        "duplicate_observed": bool(right.index.has_duplicates),
        "reference_sorted": bool(left.index.is_monotonic_increasing),
        "observed_sorted": bool(right.index.is_monotonic_increasing),
        "missing": [str(x) for x in left.index.difference(right.index)],
        "extra": [str(x) for x in right.index.difference(left.index)],
        "fields": {},
    }
    structure_ok = not (
        result["duplicate_reference"]
        or result["duplicate_observed"]
        or result["missing"]
        or result["extra"]
    )
    structure_ok = structure_ok and result["reference_sorted"] and result["observed_sorted"]
    common = left.index.intersection(right.index)
    for field in BAR_FIELDS:
        check: Dict[str, Any] = {"ok": False}
        if field not in left or field not in right:
            check["reason"] = "missing_field"
        elif not structure_ok:
            check["reason"] = "index_mismatch"
        elif not len(common):
            check.update(ok=allow_empty and len(left) == len(right) == 0, reason="empty")
        else:
            try:
                a = pd.to_numeric(left.loc[common, field], errors="raise").astype(float)
                b = pd.to_numeric(right.loc[common, field], errors="raise").astype(float)
                diff = (a - b).abs()
                tolerance = (
                    np.maximum(0.01, a.abs() * 1e-8)
                    if field == "money"
                    else 1.0 if field == "volume" else tick
                )
                valid = np.isfinite(a).all() and np.isfinite(b).all()
                bad = ~np.isfinite(a) | ~np.isfinite(b) | (diff > tolerance + 1e-10)
                worst = diff.idxmax() if diff.notna().any() else common[0]
                check.update(
                    ok=bool(valid and not bad.any()),
                    mismatch_rows=int(bad.sum()),
                    max_abs_diff=float(diff.max()) if valid else None,
                    worst_time=str(worst),
                    reference=float(a.loc[worst]) if valid else None,
                    observed=float(b.loc[worst]) if valid else None,
                )
            except (ValueError, TypeError):
                check["reason"] = "non_numeric"
        result["fields"][field] = check
    result["ok"] = bool(
        structure_ok
        and (len(left) or allow_empty)
        and all(v["ok"] for v in result["fields"].values())
    )
    return result


def legacy_dividends(rows: Iterable[Dict[str, Any]], security: str) -> List[Dict[str, Any]]:
    """旧版 GM get_dividend 单位为每股现金与每股送转比例，不乘十。"""
    result = []
    for row in rows:
        if row.get("symbol") != gm_symbol(security):
            raise ValueError("分红证券不一致")
        if float(row.get("allotment_ratio", 0)) != 0:
            raise NotImplementedError("配股不能冒充普通送转分红")
        result.append(
            {
                "date": local_time(row["created_at"]).date().isoformat(),
                "cash_per_share": float(row["cash_div"]),
                "scale_factor": 1
                + float(row.get("share_div_ratio", 0))
                + float(row.get("share_trans_ratio", 0)),
            }
        )
    return result


def compare_dividends(
    reference: List[Dict[str, Any]], observed: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """比较除权日、每股税前现金和送转倍率，重复日期不能合并掩盖。"""
    a, b = pd.DataFrame(reference), pd.DataFrame(observed)
    required = {"date", "cash_per_share", "scale_factor"}
    if not reference or not observed:
        return {
            "ok": False,
            "reason": "event_sample_required",
            "reference_rows": len(a),
            "observed_rows": len(b),
        }
    if not required.issubset(a) or not required.issubset(b):
        return {"ok": False, "reason": "missing_field"}
    if a.date.duplicated().any() or b.date.duplicated().any():
        return {"ok": False, "reason": "duplicate_event_date"}
    a, b = a.set_index("date"), b.set_index("date")
    missing, extra = a.index.difference(b.index), b.index.difference(a.index)
    result: Dict[str, Any] = {"missing": list(missing), "extra": list(extra), "fields": {}}
    common = a.index.intersection(b.index)
    for key, tol in [("cash_per_share", 1e-8), ("scale_factor", 1e-8)]:
        x, y = a.loc[common, key].astype(float), b.loc[common, key].astype(float)
        result["fields"][key] = bool(
            len(common)
            and np.isfinite(x).all()
            and np.isfinite(y).all()
            and ((x - y).abs() <= tol).all()
        )
    result["ok"] = not len(missing) and not len(extra) and all(result["fields"].values())
    return result


def validate_account(
    status: Dict[str, Any],
    cash: Dict[str, Any],
    positions: List[Dict[str, Any]],
    queries: Dict[str, Any],
) -> Dict[str, Any]:
    """只读普通证券账户核对；空持仓允许，初始化资金和非零原生错误码不算通过。"""
    values: Dict[str, Any] = {
        k: cash.get(k) for k in ("nav", "balance", "available", "market_value", "frozen")
    }
    finite = all(isinstance(v, (float, int)) and math.isfinite(v) for v in values.values())
    checks = {
        "logged_in": status.get("state") == 3 and status.get("error_code") == 0,
        "cash_finite": finite,
        "cash_timestamp": bool(cash.get("updated_at")),
        "nav_equals_balance_plus_market_value": bool(
            finite and abs(values["nav"] - values["balance"] - values["market_value"]) <= 0.01
        ),
        "available_nonnegative": bool(
            finite and 0 <= values["available"] <= values["balance"] + 0.01
        ),
        "positions_nonnegative": all(
            all(
                isinstance(p.get(k), (int, float)) and math.isfinite(p[k])
                for k in ("available", "volume", "market_value")
            )
            and 0 <= p["available"] <= p["volume"]
            and p["market_value"] >= 0
            for p in positions
        ),
        "position_market_value_sum": bool(
            finite
            and all(
                isinstance(p.get("market_value"), (int, float)) and math.isfinite(p["market_value"])
                for p in positions
            )
            and abs(sum(p["market_value"] for p in positions) - values["market_value"]) <= 0.01
        ),
        "frozen_nonnegative": bool(finite and values["frozen"] >= 0),
        "empty_positions_market_value_zero": bool(
            positions or (finite and abs(values["market_value"]) <= 0.01)
        ),
        "query_codes": all(q.get("status_code") == 0 for q in queries.values()),
        "query_rows_finite": all(
            isinstance(q.get("rows"), int) and q["rows"] >= 0 for q in queries.values()
        ),
        "query_coverage": set(queries) == {"orders", "unfinished_orders", "execution_reports"},
    }
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "position_rows": len(positions),
        "query_rows": {k: q.get("rows") for k, q in queries.items()},
        "limitations": ["empty_account_does_not_validate_fills_or_T1", "trading_not_tested"],
    }
