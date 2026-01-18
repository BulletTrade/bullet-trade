"""
current_data 涨跌停与最新价探针（用于回测/实盘对比）。
"""
from jqdata import *  # noqa: F401,F403


TARGETS = [
    "510300.XSHG",
    "510500.XSHG",
    "159919.XSHE",
]


def _init_results():
    g.results = {
        "checks": [],
        "errors": [],
    }


def _record_error(message):
    g.results["errors"].append(message)
    log.error(message)


def initialize(context):
    set_option("avoid_future_data", True)
    set_benchmark("000300.XSHG")
    _init_results()
    g.targets = list(TARGETS)
    run_daily(check_current_data, time="14:25")
    run_daily(check_current_data, time="15:00")


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
