"""
作者: BruceLee
文件职责:
    验证 tick 取数的文件缓存口径：二次查询不触上游、时间戳逐值不变、历史窗口永久缓存。

主要输入:
    内存构造的单日 tick 表与本地临时缓存目录，配合计数型取数函数。

主要输出:
    pytest 断言结果，确认缓存命中不再回源、float64 时间戳往返无损、
    历史 end_dt 判为永久缓存而当日 end_dt 走 TTL。

上下游关系:
    上游覆盖 `bullet_trade.data.cache.CacheManager`；
    下游保护 tick 回放按日预取时不会重复打数据源，也不会因缓存改写时间戳而错序。

关键环境或配置约定:
    缓存目录由用例显式传入 tmp_path，不读写 DATA_CACHE_DIR，也不访问任何外部数据源。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from bullet_trade.data.cache import CacheManager

CODE = "LH2109.XDCE"
DAY = date(2021, 6, 8)
TICK_PARAMS = {
    "security": CODE,
    "start_dt": datetime(2021, 6, 8, 0, 0, 0),
    "end_dt": datetime(2021, 6, 8, 23, 59, 59),
    "count": None,
    "fields": None,
    "skip": False,
}


def _tick_frame() -> pd.DataFrame:
    """构造含 .500 与量化小数秒时间戳的单日 tick 表。

    Returns:
        pd.DataFrame: 列为 time/current，time 为 float64 源始时间戳。
    """

    return pd.DataFrame(
        {
            "time": [20210608085900.02, 20210608090000.5, 20210608143000.0],
            "current": [17000.0, 17005.0, 16995.0],
        }
    )


@pytest.fixture
def cache(tmp_path):
    """构造启用状态的缓存管理器。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        CacheManager: 缓存目录指向临时目录的实例。
    """

    return CacheManager(provider_name="tick-test", cache_dir=str(tmp_path))


def test_second_call_does_not_touch_upstream(cache):
    """同一窗口的第二次查询应命中缓存，不再回源。"""
    calls = []

    def fetch(kwargs):
        calls.append(dict(kwargs))
        return _tick_frame()

    first = cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    second = cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")

    assert len(calls) == 1
    assert calls[0]["security"] == CODE
    assert isinstance(first, pd.DataFrame) and isinstance(second, pd.DataFrame)
    assert len(second) == 3


def test_cache_roundtrip_keeps_float_timestamps_bitwise(cache):
    """缓存往返后时间戳必须逐值不变，否则回放顺序与回放时钟都会被改写。"""

    def fetch(kwargs):
        return _tick_frame()

    original = _tick_frame()
    restored = cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    from_disk = cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")

    assert restored["time"].dtype == "float64"
    assert list(from_disk["time"]) == list(original["time"])
    assert list(from_disk["current"]) == list(original["current"])
    # .500 与被量化的小数秒都要原样落盘
    assert from_disk["time"].iloc[1] == 20210608090000.5
    assert from_disk["time"].iloc[0] == original["time"].iloc[0]


def test_cache_key_separates_window_and_fields(cache):
    """不同窗口或不同字段列表不得共用同一份缓存。"""
    calls = []

    def fetch(kwargs):
        calls.append(dict(kwargs))
        return _tick_frame()

    other_window = dict(
        TICK_PARAMS, end_dt=datetime(2021, 6, 9, 23, 59, 59), start_dt=datetime(2021, 6, 9)
    )
    other_fields = dict(TICK_PARAMS, fields=["time", "current", "volume"])

    cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    cache.cached_call("get_ticks", other_window, fetch, result_type="df")
    cache.cached_call("get_ticks", other_fields, fetch, result_type="df")

    assert len(calls) == 3


def test_historical_end_dt_is_cached_permanently(cache):
    """历史窗口的 tick 属永久缓存，当日窗口才走 TTL。"""
    historical = cache._normalize_params(dict(TICK_PARAMS))
    assert cache._infer_ttl_days(historical) is None

    today_params = dict(TICK_PARAMS)
    today_params["end_dt"] = datetime.combine(date.today(), datetime.min.time()).replace(
        hour=23, minute=59, second=59
    )
    today_params["start_dt"] = datetime.combine(date.today(), datetime.min.time())
    normalized_today = cache._normalize_params(today_params)
    assert cache._infer_ttl_days(normalized_today) == cache.expire_days


def test_start_dt_end_dt_are_normalized_to_seconds(cache):
    """窗口参数需归一化，避免同日不同时刻的窗口互相覆盖。"""
    normalized = cache._normalize_params(dict(TICK_PARAMS))
    # 零点窗口归一化到日级，非零点窗口保留到秒
    assert normalized["start_dt"] == "2021-06-08"
    assert normalized["end_dt"] == "2021-06-08T23:59:59"


def test_disabled_cache_always_calls_upstream(tmp_path):
    """未配置缓存目录时不得落盘，每次都回源。"""
    cache = CacheManager(provider_name="tick-test", cache_dir="")
    calls = []

    def fetch(kwargs):
        calls.append(kwargs)
        return _tick_frame()

    assert not cache.enabled
    cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    cache.cached_call("get_ticks", TICK_PARAMS, fetch, result_type="df")
    assert len(calls) == 2


def test_window_helper_matches_cached_params():
    """按日窗口与缓存键使用的窗口必须一致，否则预取与回放会各查一份。"""
    from bullet_trade.data.tick_replay import day_tick_window

    start, end = day_tick_window(DAY)
    assert start == TICK_PARAMS["start_dt"]
    assert end == TICK_PARAMS["end_dt"]
    assert end + timedelta(seconds=1) == datetime(2021, 6, 9, 0, 0, 0)
