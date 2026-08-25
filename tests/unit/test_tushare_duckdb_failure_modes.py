"""
tushare_duckdb 数据源离线验收测试：失败模式、降级行为与未实现接口。

只依赖本机 tushare / duckdb 安装，不访问网络。对应
bullet_trade/tushare_duckdb_test.md 中「不支持的接口也要测试」的离线部分：
- auth 缺 token 必须有清晰错误；
- 未实现接口必须稳定抛 NotImplementedError，不能静默返回假数据；
- tushare_duckdb 模块本地未命中时必须稳定回退远程（不抛错、不返回假数据）。
"""

from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from bullet_trade.data.providers import tushare_duckdb
from bullet_trade.data.providers.tushare import TushareProvider

#: 两个数据源配置：原生 tushare（tushare_duckdb_path=None）与本地 DuckDB 源。
_PROVIDER_CONFIGS = (
    {"cache_dir": None},
    {"cache_dir": None, "tushare_duckdb_path": "/tmp/nonexistent_test.duckdb"},
)


def _make_providers():
    return [TushareProvider(cfg) for cfg in _PROVIDER_CONFIGS]


# ---------------------------------------------------------------------- #
# auth 缺 token
# ---------------------------------------------------------------------- #
@pytest.mark.unit
def test_auth_missing_token_raises_clear_error(monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    for provider in _make_providers():
        provider._token = None
        with pytest.raises(RuntimeError, match="token"):
            provider.auth()


# ---------------------------------------------------------------------- #
# 未实现接口稳定抛 NotImplementedError
# ---------------------------------------------------------------------- #
# (方法名, 位置参数) —— 全部为基类 DataProvider 未覆盖的扩展接口，
# 双数据源都必须稳定抛 NotImplementedError，不能静默返回假数据。
_NOT_IMPLEMENTED_METHODS = [
    ("get_extras", ("is_st", ["000001.XSHE"])),
    ("get_fundamentals", (object(),)),
    ("get_fundamentals_continuously", (object(),)),
    ("get_ticks", ("000001.XSHE", "2024-01-02")),
    ("get_current_tick", ("000001.XSHE",)),
    ("get_industry", ("000001.XSHE",)),
    ("get_industry_stocks", ("A01",)),
    ("get_concept", ("000001.XSHE",)),
    ("get_concept_stocks", ("GN",)),
    ("get_margincash_stocks", ()),
    ("get_marginsec_stocks", ()),
    ("get_dominant_future", ("IF",)),
    ("get_future_contracts", ("IF",)),
    ("get_billboard_list", ()),
    ("get_locked_shares", (["000001.XSHE"],)),
    ("get_bars", ("000001.XSHE", 5)),
]


@pytest.mark.unit
@pytest.mark.parametrize("method,args", _NOT_IMPLEMENTED_METHODS)
def test_unimplemented_interfaces_raise_not_implemented(method, args):
    for provider in _make_providers():
        with pytest.raises(NotImplementedError):
            getattr(provider, method)(*args)


# ---------------------------------------------------------------------- #
# tushare_duckdb 模块降级行为（本地未命中 → 稳定回退远程）
# ---------------------------------------------------------------------- #
@pytest.mark.unit
def test_query_local_no_filter_returns_none():
    """无任何筛选条件时不查本地（避免全表扫描），直接回退远程。"""
    result = tushare_duckdb._query_local(
        tushare_duckdb._STK_DAILY_TABLE, tushare_duckdb._DAILY_COLUMNS, "", {}
    )
    assert result is None


@pytest.mark.unit
def test_query_local_missing_db_returns_none(monkeypatch):
    """本地库文件不存在时回退远程，不抛错。"""
    monkeypatch.setattr(tushare_duckdb, "_db_path", "/nonexistent_dir/x.duckdb")
    kwargs = {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240131"}
    result = tushare_duckdb._query_local(
        tushare_duckdb._STK_DAILY_TABLE, tushare_duckdb._DAILY_COLUMNS, "", kwargs
    )
    assert result is None


def _create_temp_db(tmp_path, table_sql, table_name, rows):
    db_path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(table_sql)
    for row in rows:
        con.execute(f"INSERT INTO {table_name} VALUES ({row})")
    con.close()
    return db_path


_STK_TABLE_SQL = (
    "CREATE TABLE stk_daily (ts_code VARCHAR, trade_date DATE, open DOUBLE, "
    "high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE, change DOUBLE, "
    "pct_chg DOUBLE, vol DOUBLE, amount DOUBLE)"
)


@pytest.mark.unit
def test_query_local_hit_returns_rows_with_string_date(tmp_path, monkeypatch):
    """本地命中：返回行且 trade_date 从 DATE 转回 YYYYMMDD 字符串。"""
    db_path = _create_temp_db(
        tmp_path,
        _STK_TABLE_SQL,
        "stk_daily",
        ["'000001.SZ', DATE '2024-01-02', 10.0, 11.0, 9.0, 10.5, 10.2, 0.3, 2.94, 1000.0, 10500.0"],
    )
    monkeypatch.setattr(tushare_duckdb, "_db_path", str(db_path))

    df = tushare_duckdb._query_local(
        tushare_duckdb._STK_DAILY_TABLE,
        tushare_duckdb._DAILY_COLUMNS,
        "",
        {"ts_code": "000001.SZ", "start_date": "20240101", "end_date": "20240131"},
    )
    assert df is not None and len(df) == 1
    assert df.iloc[0]["trade_date"] == "20240102"
    assert float(df.iloc[0]["close"]) == 10.5


@pytest.mark.unit
def test_query_local_empty_falls_back_to_remote(tmp_path, monkeypatch):
    """本地表存在但查询无结果：_local_common 回退远程父逻辑。"""
    db_path = _create_temp_db(tmp_path, _STK_TABLE_SQL, "stk_daily", [])
    monkeypatch.setattr(tushare_duckdb, "_db_path", str(db_path))

    calls = {}

    class FakePro:
        def daily(self, fields="", **kwargs):
            calls["kwargs"] = kwargs
            return "REMOTE_RESULT"

    monkeypatch.setattr(tushare_duckdb._ts, "pro_api", lambda *a, **k: FakePro())

    result = tushare_duckdb.daily(
        ts_code="000001.SZ", start_date="20240101", end_date="20240131"
    )
    assert result == "REMOTE_RESULT"
    assert calls["kwargs"]["ts_code"] == "000001.SZ"


@pytest.mark.unit
def test_resolve_select_columns_filters_unknown_fields():
    cols = tushare_duckdb._resolve_select_columns(
        "ts_code,close,nonexistent", tushare_duckdb._DAILY_COLUMNS
    )
    assert cols == ["ts_code", "close"]
    # 全部非法时回退默认列，避免 SELECT 空列列表
    assert tushare_duckdb._resolve_select_columns(
        "bogus", tushare_duckdb._DAILY_COLUMNS
    ) == tushare_duckdb._DAILY_COLUMNS


@pytest.mark.unit
def test_to_date_converts_yyyymmdd_to_date_literal():
    assert tushare_duckdb._to_date("20240102") == "2024-01-02"
    assert tushare_duckdb._to_date("2024-01-02") == "2024-01-02"
    assert tushare_duckdb._to_date(None) is None


# ---------------------------------------------------------------------- #
# pro_bar 短路条件：只有日线且 asset∈{E,FD} 才走本地
# ---------------------------------------------------------------------- #
@pytest.mark.unit
def test_pro_bar_non_shortcut_conditions_fall_back_to_remote(monkeypatch):
    """分钟线 / 指数不满足短路条件，必须回退远程 ts.pro_bar。"""
    captured = {}
    monkeypatch.setattr(tushare_duckdb._ts, "pro_api", lambda *a, **k: SimpleNamespace())

    def fake_pro_bar(**kwargs):
        captured["kw"] = kwargs
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(tushare_duckdb._ts, "pro_bar", fake_pro_bar)

    tushare_duckdb.pro_bar(
        ts_code="000001.SZ", freq="1min", asset="E",
        start_date="20240101", end_date="20240102",
    )
    assert captured["kw"]["freq"] == "1min"

    tushare_duckdb.pro_bar(
        ts_code="000300.SH", freq="D", asset="I",
        start_date="20240101", end_date="20240102",
    )
    assert captured["kw"]["asset"] == "I"


@pytest.mark.unit
def test_pro_bar_shortcut_local_hit_skips_remote(monkeypatch):
    """日线 asset='E' 短路条件满足且本地命中时，直接返回本地结果，不碰远程。"""
    calls = {}

    def fake_daily(**kwargs):
        calls["daily"] = kwargs
        return pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "close": [10.5]}
        )

    monkeypatch.setattr(tushare_duckdb, "daily", fake_daily)
    monkeypatch.setattr(
        tushare_duckdb._ts,
        "pro_bar",
        lambda **kw: (_ for _ in ()).throw(AssertionError("短路命中不应回退远程")),
    )
    monkeypatch.setattr(tushare_duckdb._ts, "pro_api", lambda *a, **k: SimpleNamespace())

    df = tushare_duckdb.pro_bar(
        ts_code="000001.SZ", freq="D", asset="E",
        start_date="20240101", end_date="20240102",
    )
    assert not df.empty
    assert "daily" in calls
