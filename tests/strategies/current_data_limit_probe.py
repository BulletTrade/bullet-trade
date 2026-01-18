"""
current_data 涨跌停与最新价探针（用于回测/实盘对比）。
"""
import os

from jqdata import *  # noqa: F401,F403


def _env_bool(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


USE_REAL_PRICE = _env_bool("BT_USE_REAL_PRICE", True)
FORCE_NO_ENGINE = _env_bool("BT_FORCE_NO_ENGINE", True)
AVOID_FUTURE_DATA = _env_bool("BT_AVOID_FUTURE_DATA", True)
DEBUG_META = _env_bool("BT_DEBUG_META", True)
EPSILON = 1e-6




TARGETS = [
    "510300.XSHG",
    "510500.XSHG",
    "159919.XSHE",
    "000001.XSHE",
]


BASELINE = {
    False: {
        "14:25": {
            "510300.XSHG": {"last": 4.778, "high": 5.239, "low": 4.287},
            "510500.XSHG": {"last": 7.539, "high": 8.241, "low": 6.742},
            "159919.XSHE": {"last": 4.858, "high": 5.322, "low": 4.354},
            "000001.XSHE": {"last": 11.47, "high": 12.72, "low": 10.4},
        },
        "15:00": {
            "510300.XSHG": {"last": 4.773, "high": 5.239, "low": 4.287},
            "510500.XSHG": {"last": 7.525, "high": 8.241, "low": 6.742},
            "159919.XSHE": {"last": 4.855, "high": 5.322, "low": 4.354},
            "000001.XSHE": {"last": 11.48, "high": 12.72, "low": 10.4},
        },
    },
    True: {
        "14:25": {
            "510300.XSHG": {"last": 4.778, "high": 5.239, "low": 4.287},
            "510500.XSHG": {"last": 7.595, "high": 8.303, "low": 6.793},
            "159919.XSHE": {"last": 4.858, "high": 5.322, "low": 4.354},
            "000001.XSHE": {"last": 11.47, "high": 12.72, "low": 10.4},
        },
        "15:00": {
            "510300.XSHG": {"last": 4.773, "high": 5.239, "low": 4.287},
            "510500.XSHG": {"last": 7.581, "high": 8.303, "low": 6.793},
            "159919.XSHE": {"last": 4.855, "high": 5.322, "low": 4.354},
            "000001.XSHE": {"last": 11.48, "high": 12.72, "low": 10.4},
        },
    },
}


def _init_results():
    g.results = {
        "checks": [],
        "errors": [],
    }
    g._meta_logged = set()


def _record_error(message):
    g.results["errors"].append(message)
    log.error(message)


def _compare_with_baseline(code, phase, last_price, high_limit, low_limit):
    baseline = BASELINE.get(bool(USE_REAL_PRICE), {})
    phase_map = baseline.get(phase)
    if not phase_map:
        return
    expected = phase_map.get(code)
    if not expected:
        return

    def _check(field, actual):
        exp = expected.get(field)
        if exp is None:
            return
        diff = abs(actual - float(exp))
        if diff > EPSILON:
            _record_error(
                f"{code} {phase} {field} 不匹配 actual={actual:.6f} expected={float(exp):.6f} diff={diff:.6f}"
            )

    _check("last", last_price)
    _check("high", high_limit)
    _check("low", low_limit)

def _maybe_log_meta(code, phase):
    if not DEBUG_META:
        return
    key = (code, phase)
    if key in g._meta_logged:
        return
    g._meta_logged.add(key)
    info = get_security_info(code)
    resolved_decimals = None
    try:
        resolved_decimals = get_tick_decimals(code)
    except Exception:
        resolved_decimals = None
    log.info(
        "[涨跌停探针][元数据][%s] %s type=%s subtype=%s category=%s tick_size=%s price_decimals=%s tick_decimals=%s resolved_tick_decimals=%s",
        phase,
        code,
        getattr(info, "type", None) or info.get("type"),
        getattr(info, "subtype", None) or info.get("subtype"),
        getattr(info, "category", None) or info.get("category"),
        getattr(info, "tick_size", None) or info.get("tick_size"),
        getattr(info, "price_decimals", None) or info.get("price_decimals"),
        getattr(info, "tick_decimals", None) or info.get("tick_decimals"),
        resolved_decimals,
    )

def initialize(context):
    set_option("avoid_future_data", AVOID_FUTURE_DATA)
    set_option("force_no_engine", FORCE_NO_ENGINE)
    set_option("use_real_price", USE_REAL_PRICE)
    set_benchmark("000300.XSHG")
    _init_results()
    g.targets = list(TARGETS)
    run_daily(check_current_data, time="14:25")
    run_daily(check_current_data, time="15:00")
    log.info(
        "[涨跌停探针] 使用基准分支 use_real_price=%s force_no_engine=%s avoid_future_data=%s",
        USE_REAL_PRICE,
        FORCE_NO_ENGINE,
        AVOID_FUTURE_DATA,
    )


def _validate_snapshot(code, snap, phase):
    last_price = float(getattr(snap, "last_price", 0.0) or 0.0)
    high_limit = float(getattr(snap, "high_limit", 0.0) or 0.0)
    low_limit = float(getattr(snap, "low_limit", 0.0) or 0.0)
    paused = bool(getattr(snap, "paused", False))
    payload = {
        "code": code,
        "phase": phase,
        "last_price": last_price,
        "high_limit": high_limit,
        "low_limit": low_limit,
        "paused": paused,
    }
    g.results["checks"].append(payload)
    log.info(
        "[涨跌停探针][%s] %s last=%.6f high=%.6f low=%.6f paused=%s",
        phase,
        code,
        last_price,
        high_limit,
        low_limit,
        paused,
    )
    _maybe_log_meta(code, phase)

    if last_price <= 0:
        _record_error(f"{code} {phase} 最新价为0或缺失")
    if high_limit <= 0:
        _record_error(f"{code} {phase} 涨停价为0或缺失")
    if low_limit <= 0:
        _record_error(f"{code} {phase} 跌停价为0或缺失")
    if high_limit > 0 and low_limit > 0 and last_price > 0:
        if not (low_limit <= last_price <= high_limit):
            _record_error(
                f"{code} {phase} 价格越界 last={last_price} low={low_limit} high={high_limit}"
            )

    _compare_with_baseline(code, phase, last_price, high_limit, low_limit)


def check_current_data(context):
    phase = context.current_dt.strftime("%H:%M")
    current_data = get_current_data()
    for code in g.targets:
        try:
            snap = current_data[code]
        except Exception as exc:
            _record_error(f"{code} {phase} 获取 current_data 失败: {exc}")
            continue
        _validate_snapshot(code, snap, phase)
