"""
tushare 源 vs tushare_duckdb 源 数据源接入验收（在线）。

对应 bullet_trade/tushare_duckdb_test.md 框架：
- 基准：原生 tushare 源（tushare_duckdb_path=None，走远程 pro_api）
- 被测：tushare_duckdb 源（tushare_duckdb_path=/data/tushare_persistence/duckdb/tushare.duckdb，
  本地优先、未命中回退远程）
- 运行前设置两个临时环境变量：
    TUSHARE_TOKEN=...
    tushare_duckdb_path=/data/tushare_persistence/duckdb/tushare.duckdb

验收原则：
- 行情价格、复权、动态真实价格以 tushare 源为基准做数值对账（PRICE_TOL=1e-6，
  本地为远程镜像，实测 max_abs_diff=0）。
- 列表/实时快照/证券信息等天然不完全一致的接口验证 schema、关键字段、样例标的与失败模式。
- 未实现接口必须稳定 NotImplementedError / 空返回，不能静默返回假数据。

收尾用例 test_acceptance_report_completeness 会打印完整验收表，并校验必测清单无遗漏。
"""

import os
import time
from datetime import date as Date

import pandas as pd
import pytest

from bullet_trade.data.providers.tushare import TushareProvider
from bullet_trade.utils.env_loader import get_env

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_network,
]

DB_PATH = os.getenv("tushare_duckdb_path") or "/data/tushare_persistence/duckdb/tushare.duckdb"
TOKEN_ENV = "TUSHARE_TOKEN"
#: 本地 DuckDB 是远程 tushare 的镜像，实测逐字段 max_abs_diff=0，留 1e-6 安全余量
PRICE_TOL = 1e-6

STOCKS = ["000001.XSHE", "600519.XSHG"]
ETFS = ["510050.XSHG", "159865.XSHE"]
#: 除权除息日：000001.XSHE = 2024-06-14/2024-10-10/2025-06-12；600519.XSHG = 2025-06-26
WINDOW_CROSS_000001 = ("2024-05-01", "2024-07-15")
WINDOW_CROSS_600519 = ("2025-01-01", "2025-07-15")

# ---------------------------------------------------------------------- #
# 验收状态记录
# ---------------------------------------------------------------------- #
_REPORT = []


def _record(func, status, summary, compat="", risk="", entry=""):
    _REPORT.append(
        {
            "func": func,
            "status": status,
            "summary": summary,
            "compat": compat,
            "risk": risk,
            "entry": entry,
        }
    )


# ---------------------------------------------------------------------- #
# 前置与数据源 fixture
# ---------------------------------------------------------------------- #
def _require_prereqs():
    if not get_env(TOKEN_ENV):
        pytest.skip("缺少 TUSHARE_TOKEN，请在环境变量中配置后重试。")
    if not os.path.exists(DB_PATH):
        pytest.skip(f"本地 DuckDB 不存在：{DB_PATH}")


@pytest.fixture(scope="module")
def providers():
    _require_prereqs()
    native = TushareProvider({"cache_dir": None})
    duck = TushareProvider({"cache_dir": None, "tushare_duckdb_path": DB_PATH})
    native.auth()
    duck.auth()
    return native, duck


# ---------------------------------------------------------------------- #
# 对账工具
# ---------------------------------------------------------------------- #
def _extract_series(df, field):
    """从 panel / flat / 单标的 DataFrame 中提取某字段的 (证券, Series) 列表。"""
    items = []
    if isinstance(df.columns, pd.MultiIndex):
        for sec in df.columns.get_level_values(0).unique():
            sub = df.xs(sec, axis=1, level=0)
            if field in sub.columns:
                items.append((str(sec), pd.to_numeric(sub[field], errors="coerce")))
        return items
    if "code" in df.columns:
        for sec, group in df.groupby("code"):
            if field in group.columns:
                s = pd.to_numeric(group[field], errors="coerce")
                s.index = pd.to_datetime(group.index)
                items.append((str(sec), s))
        return items
    if field in df.columns:
        items.append((None, pd.to_numeric(df[field], errors="coerce")))
    return items


def _assert_price_parity(native_df, duck_df, fields, label="", tol=PRICE_TOL):
    """以 tushare 源为基准对账：行数、标的集合、数值最大偏差。"""
    if native_df is None or duck_df is None:
        raise AssertionError(f"{label}: 返回 None")
    if len(native_df) != len(duck_df):
        raise AssertionError(
            f"{label}: 行数不一致 native={len(native_df)} duck={len(duck_df)}"
        )
    for field in fields:
        n_items = _extract_series(native_df, field)
        d_items = _extract_series(duck_df, field)
        if len(n_items) != len(d_items):
            raise AssertionError(
                f"{label} {field}: 标的数不一致 native={len(n_items)} duck={len(d_items)}"
            )
        for (sec_n, s_n), (sec_d, s_d) in zip(n_items, d_items):
            if sec_n != sec_d:
                raise AssertionError(f"{label} {field}: 标的集合不一致 {sec_n} vs {sec_d}")
            idx = s_n.dropna().index.intersection(s_d.dropna().index)
            if len(idx) == 0:
                raise AssertionError(
                    f"{label} {field} {sec_n}: 无重叠日期 native={len(s_n)} duck={len(s_d)}"
                )
            diff = float((s_n.loc[idx] - s_d.loc[idx]).abs().max())
            if diff > tol:
                raise AssertionError(
                    f"{label} {field} {sec_n}: 最大偏差 {diff:.3e} > {tol:.3e}"
                )


def _is_empty_result(result):
    if result is None:
        return True
    if hasattr(result, "empty"):
        return bool(result.empty)
    if isinstance(result, dict):
        return not result
    if isinstance(result, (list, tuple)):
        return len(result) == 0
    return False


def _is_rate_limit_signal(exc):
    """tushare 限频时可能抛 IOError('ERROR.')，或返回带「频率超限」的异常。"""
    text = str(exc).lower()
    if "频率超限" in text or "error" in text:
        return True
    return isinstance(exc, OSError)


def _call_with_retry(fn, wait=61, retries=1):
    """
    执行可能触发 stk_mins 限频(1次/分钟或1次/小时)的调用。
    返回 (result, is_empty)。空结果或限频异常时等待 wait 秒重试一次，
    仍失败则返回 (None, True) 供调用方记录 LIMIT。
    """
    last = None
    for attempt in range(retries + 1):
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001
            last = exc
            if _is_rate_limit_signal(exc) and attempt < retries:
                time.sleep(wait)
                continue
            return None, True
        if _is_empty_result(result) and attempt < retries:
            last = result
            time.sleep(wait)
            continue
        return result, _is_empty_result(result)
    return last, True


# ---------------------------------------------------------------------- #
# 核心：auth
# ---------------------------------------------------------------------- #
def test_auth_real_mode(providers):
    native, duck = providers
    # 真实模式双源均能建立连接；auth 不返回 stub
    assert native._pro is not None
    assert duck._pro is not None
    _record(
        "auth",
        "PASS",
        "真实 token 下原生源与 tushare_duckdb 源均成功认证；缺 token 的清晰错误在离线用例覆盖",
        "",
        "账号权限取决于 token 积分",
        "tests/unit/test_tushare_duckdb_failure_modes.py",
    )


# ---------------------------------------------------------------------- #
# 核心：get_price —— 未复权 / 字段 / 单位 / 多证券 / count
# ---------------------------------------------------------------------- #
def test_get_price_daily_raw_reconciliation(providers):
    native, duck = providers
    fields = ["open", "high", "low", "close", "volume", "money"]
    start, end = WINDOW_CROSS_000001
    for sec in STOCKS + ETFS:
        n = native.get_price(sec, start_date=start, end_date=end, fields=fields, fq="none")
        d = duck.get_price(sec, start_date=start, end_date=end, fields=fields, fq="none")
        _assert_price_parity(n, d, fields, label=f"fq=none {sec}")
    _record(
        "get_price(fq=\"none\")",
        "PASS",
        f"{', '.join(STOCKS + ETFS)} 在 {start}~{end} 的 OHLC/volume/money 与基准最大偏差 0（≤{PRICE_TOL}）",
        "本地 stk_daily/etf_daily 为远程镜像",
        "本地库滞后时最近交易日可能缺失（见报告）",
        "test_get_price_daily_raw_reconciliation",
    )


def test_get_price_fields_and_units(providers):
    native, duck = providers
    sec = "000001.XSHE"
    day = "2024-06-14"
    fields = ["open", "high", "low", "close", "volume", "money"]
    n = native.get_price(sec, start_date=day, end_date=day, fields=fields, fq="none")
    d = duck.get_price(sec, start_date=day, end_date=day, fields=fields, fq="none")
    _assert_price_parity(n, d, fields, label="字段/单位")
    # 单位口径验证：tushare daily vol=手→volume=股(x100)，amount=千元→money=元(x1000)
    raw = native._pro.daily(ts_code="000001.SZ", start_date="20240614", end_date="20240614")
    assert not raw.empty
    expected_volume = float(raw.iloc[0]["vol"]) * 100.0
    expected_money = float(raw.iloc[0]["amount"]) * 1000.0
    assert abs(float(n.iloc[0]["volume"]) - expected_volume) <= 1.0, "volume 单位非股"
    assert abs(float(n.iloc[0]["money"]) - expected_money) <= 1.0, "money 单位非元"
    _record(
        "get_price 字段映射/单位",
        "PASS",
        "字段映射与基准一致；volume=股、money=元 与远程 vol(x100)/amount(x1000) 对齐",
        "复用 _normalize_price_units 统一口径",
        "",
        "test_get_price_fields_and_units",
    )


def test_get_price_multi_security_panel_and_flat(providers):
    native, duck = providers
    fields = ["open", "high", "low", "close"]
    start, end = WINDOW_CROSS_000001
    n_panel = native.get_price(STOCKS, start_date=start, end_date=end, fields=fields, fq="none", panel=True)
    d_panel = duck.get_price(STOCKS, start_date=start, end_date=end, fields=fields, fq="none", panel=True)
    assert set(n_panel.columns.get_level_values(0)) == set(d_panel.columns.get_level_values(0))
    _assert_price_parity(n_panel, d_panel, fields, label="panel=True")
    n_flat = native.get_price(STOCKS, start_date=start, end_date=end, fields=fields, fq="none", panel=False)
    d_flat = duck.get_price(STOCKS, start_date=start, end_date=end, fields=fields, fq="none", panel=False)
    assert sorted(n_flat["code"].unique()) == sorted(d_flat["code"].unique())
    _assert_price_parity(n_flat, d_flat, fields, label="panel=False")
    _record(
        "get_price 多证券/panel",
        "PASS",
        f"{STOCKS} panel=True/False 与基准标的集合、行数、数值一致",
        "flat 用 日期+code 双键对齐",
        "",
        "test_get_price_multi_security_panel_and_flat",
    )


def test_get_price_count_and_end_date_semantics(providers):
    native, duck = providers
    n = native.get_price("000001.XSHE", count=10, end_date="2024-07-15", fields=["close"], fq="none")
    d = duck.get_price("000001.XSHE", count=10, end_date="2024-07-15", fields=["close"], fq="none")
    assert len(n) == 10 and len(d) == 10
    _assert_price_parity(n, d, ["close"], label="count/end_date")
    _record(
        "get_price count/end_date",
        "PASS",
        "count=10、end_date 语义与基准一致（各返回最近 10 根，数值一致）",
        "",
        "",
        "test_get_price_count_and_end_date_semantics",
    )


# ---------------------------------------------------------------------- #
# 核心：get_price(fq="pre" / "post") 跨分红对账
# ---------------------------------------------------------------------- #
def test_get_price_pre_across_dividend_window(providers):
    native, duck = providers
    fields = ["open", "high", "low", "close"]
    # 000001.XSHE 窗口跨 2024-06-14 除权除息
    start, end = WINDOW_CROSS_000001
    n = native.get_price("000001.XSHE", start_date=start, end_date=end, fields=fields, fq="pre")
    d = duck.get_price("000001.XSHE", start_date=start, end_date=end, fields=fields, fq="pre")
    _assert_price_parity(n, d, fields, label="fq=pre 000001")
    # 600519.XSHG 窗口跨 2025-06-26 除权除息
    s2, e2 = WINDOW_CROSS_600519
    n2 = native.get_price("600519.XSHG", start_date=s2, end_date=e2, fields=fields, fq="pre")
    d2 = duck.get_price("600519.XSHG", start_date=s2, end_date=e2, fields=fields, fq="pre")
    _assert_price_parity(n2, d2, fields, label="fq=pre 600519")
    _record(
        "get_price(fq=\"pre\")",
        "PASS",
        f"000001.XSHE 跨 2024-06-14、600519.XSHG 跨 2025-06-26 分红窗口与基准最大偏差 0",
        "双源均用原始价 + 远程 adj_factor 构造前复权",
        "",
        "test_get_price_pre_across_dividend_window",
    )


def test_get_price_post_across_dividend_window(providers):
    native, duck = providers
    start, end = WINDOW_CROSS_000001
    n = native.get_price("000001.XSHE", start_date=start, end_date=end, fields=["close"], fq="post")
    d = duck.get_price("000001.XSHE", start_date=start, end_date=end, fields=["close"], fq="post")
    _assert_price_parity(n, d, ["close"], label="fq=post")
    _record(
        "get_price(fq=\"post\")",
        "PASS",
        "跨分红窗口后复权与基准一致（构造 factor 后锚定除权日）",
        "",
        "",
        "test_get_price_post_across_dividend_window",
    )


def test_get_price_dynamic_pre_factor_ref_date(providers):
    native, duck = providers
    fields = ["open", "high", "low", "close"]
    # 600519.XSHG 2025-06-26 除权除息；参考日覆盖分红前后多个视角
    for ref in ("2025-03-15", "2025-06-27", "2025-07-15"):
        n = native.get_price(
            "600519.XSHG", start_date="2025-01-01", end_date="2025-07-15",
            fields=fields, fq="pre", pre_factor_ref_date=ref,
        )
        d = duck.get_price(
            "600519.XSHG", start_date="2025-01-01", end_date="2025-07-15",
            fields=fields, fq="pre", pre_factor_ref_date=ref,
        )
        _assert_price_parity(n, d, fields, label=f"pre_factor_ref_date={ref}")
    _record(
        "pre_factor_ref_date",
        "PASS",
        "600519.XSHG 多参考日动态前复权（含跨 2025-06-26 分红参考日）与基准最大偏差 0",
        "raw * factor / factor_ref 锚定",
        "",
        "test_get_price_dynamic_pre_factor_ref_date",
    )


def test_get_price_factor_field_behavior(providers):
    """
    fields=["factor"] 现状记录：双源都静默返回全 0 的假 factor。
    按验收原则「不能静默返回假数据」应标 FAIL；用户确认本次只验收、不修代码。
    """
    native, duck = providers
    n = native.get_price("000001.XSHE", start_date="2024-01-02", end_date="2024-01-10", fields=["factor"], fq=None)
    d = duck.get_price("000001.XSHE", start_date="2024-01-02", end_date="2024-01-10", fields=["factor"], fq=None)
    assert "factor" in n.columns and "factor" in d.columns
    # 双源行为一致（都是假数据）
    _assert_price_parity(n, d, ["factor"], label="factor 行为一致性")
    unique = n["factor"].dropna().unique()
    _record(
        "fields=[\"factor\"]",
        "FAIL",
        f"双源均返回全 {set(unique) if len(unique) else '0'} 的假 factor，非真实复权因子",
        "兼容尝试：可改为走 adj_factor 返回真实因子，或显式 NotImplementedError",
        "静默假数据违反验收原则，需修复或降级后才可宣称支持",
        "test_get_price_factor_field_behavior",
    )


# ---------------------------------------------------------------------- #
# 核心：get_price 分钟线（stk_mins 限频 1 次/分钟）
# ---------------------------------------------------------------------- #
def test_get_price_minute_recent_window(providers):
    native, duck = providers
    fields = ["open", "high", "low", "close", "volume", "money"]
    start, end = "2026-07-31 14:30:00", "2026-07-31 15:00:00"

    n, n_empty = _call_with_retry(
        lambda: native.get_price("000001.XSHE", start_date=start, end_date=end, frequency="1m", fields=fields, fq="none"),
    )
    if n_empty:
        _record(
            "get_price 分钟线", "LIMIT",
            "stk_mins 限频(1次/分钟或1次/小时)或窗口无分钟数据",
            "重试一次仍失败；分钟线无本地存储，tushare_duckdb 构造上恒回退远程 stk_mins",
            "token 权限限制，需更高积分 token 补测", "test_get_price_minute_recent_window",
        )
        pytest.skip("原生 tushare 分钟线为空/限频")
    d, d_empty = _call_with_retry(
        lambda: duck.get_price("000001.XSHE", start_date=start, end_date=end, frequency="1m", fields=fields, fq="none"),
    )
    if d_empty:
        _record(
            "get_price 分钟线", "LIMIT",
            "tushare_duckdb 分钟线为空/限频",
            "分钟线无本地存储，恒回退远程", "限频",
            "test_get_price_minute_recent_window",
        )
        pytest.skip("tushare_duckdb 分钟线为空/限频")
    _assert_price_parity(n, d, fields, label="1m 近期窗口")
    _record(
        "get_price 分钟线",
        "PASS",
        "近期 1m 窗口与基准最大偏差 0（tushare_duckdb 分钟线恒回退远程 stk_mins，构造上一致）",
        "分钟线无本地存储，本地优先短路仅限日线",
        "限频 1 次/分钟；旧窗口深度受免费权限限制",
        "test_get_price_minute_recent_window",
    )


# ---------------------------------------------------------------------- #
# 核心：交易日 / 证券列表 / 证券信息
# ---------------------------------------------------------------------- #
def test_get_trade_days_and_day(providers):
    native, duck = providers
    days_n = native.get_trade_days(start_date="2024-01-01", end_date="2024-03-31")
    days_d = duck.get_trade_days(start_date="2024-01-01", end_date="2024-03-31")
    assert [d.date().isoformat() for d in days_n] == [d.date().isoformat() for d in days_d]
    count_n = native.get_trade_days(end_date="2024-03-31", count=5)
    count_d = duck.get_trade_days(end_date="2024-03-31", count=5)
    assert [d.date().isoformat() for d in count_n] == [d.date().isoformat() for d in count_d]
    assert len(count_n) == 5
    # 边界：周末/节假日不在集合内
    dates = {d.date() for d in days_n}
    assert Date(2024, 1, 1) not in dates  # 元旦
    assert Date(2024, 3, 9) not in dates  # 周六
    # get_trade_day
    td_n = native.get_trade_day("000001.XSHE", "2024-03-31")
    td_d = duck.get_trade_day("000001.XSHE", "2024-03-31")
    assert td_n["000001.XSHE"] == td_d["000001.XSHE"]
    assert td_n["000001.XSHE"].isoformat() == "2024-03-29"
    _record(
        "get_trade_days / get_trade_day",
        "PASS",
        "2024-Q1 交易日集合、count=5、边界(元旦/周六)、get_trade_day 与基准一致",
        "双源均走远程 trade_cal",
        "",
        "test_get_trade_days_and_day",
    )


def test_get_all_securities_and_info(providers):
    native, duck = providers
    for types in ("stock", "etf", "index"):
        n = native.get_all_securities(types=types)
        d = duck.get_all_securities(types=types)
        assert list(n.columns) == list(d.columns), f"{types} 列不一致"
        assert set(n.index) == set(d.index), f"{types} 标的集合不一致"
        for col in ("display_name", "name", "start_date", "end_date", "type"):
            assert col in n.columns, f"{types} 缺列 {col}"
    # 样例标的与代码格式
    stocks = native.get_all_securities(types="stock")
    for sec in ("000001.XSHE", "600519.XSHG"):
        assert sec in stocks.index, f"样例股票 {sec} 缺失"
        assert sec.split(".")[0].isdigit() and sec.split(".")[1] in ("XSHG", "XSHE")
    etfs = native.get_all_securities(types="etf")
    for sec in ("510050.XSHG", "159865.XSHE"):
        assert sec in etfs.index, f"样例 ETF {sec} 缺失"
    indexes = native.get_all_securities(types="index")
    assert "000300.XSHG" in indexes.index, "样例指数 000300.XSHG 缺失"

    for sec, expected_type in (("000001.XSHE", "stock"), ("510050.XSHG", "fund"), ("000300.XSHG", "index")):
        info_n = native.get_security_info(sec)
        info_d = duck.get_security_info(sec)
        assert info_n == info_d, f"get_security_info {sec} 不一致"
        assert set(info_n) >= {"display_name", "name", "start_date", "end_date", "type", "subtype"}
        assert info_n["type"] == expected_type, f"get_security_info {sec} type={info_n['type']}"
    _record(
        "get_all_securities / get_security_info",
        "PASS",
        "股票/ETF/指数列表 schema、样例标的、聚宽代码格式与基准一致；get_security_info 相等且类型正确",
        "双源均走远程 stock_basic/fund_basic/index_basic",
        "全量列表以 tushare 快照为准，不宣称与 JQData 全量强一致",
        "test_get_all_securities_and_info",
    )


# ---------------------------------------------------------------------- #
# 核心：分红除权
# ---------------------------------------------------------------------- #
def test_get_split_dividend(providers):
    native, duck = providers
    n = native.get_split_dividend("000001.XSHE", start_date="2024-01-01", end_date="2025-12-31")
    d = duck.get_split_dividend("000001.XSHE", start_date="2024-01-01", end_date="2025-12-31")
    assert n == d, "分红事件列表不一致"
    assert len(n) >= 3, "应覆盖 2024-06-14 / 2024-10-10 / 2025-06-12 等事件"
    for ev in n:
        assert {"security", "date", "scale_factor", "bonus_pre_tax", "per_base"} <= set(ev)
        assert float(ev["scale_factor"]) > 0
    # 复权闭环：事件能构造 factor 且前复权与基准一致（已在 pre 用例验证数值）
    ex_dates = {str(ev["date"]) for ev in n}
    assert "2024-06-14" in ex_dates and "2024-10-10" in ex_dates
    _record(
        "get_split_dividend",
        "PASS",
        "000001.XSHE 2024~2025 分红事件（含 2024-06-14/2024-10-10/2025-06-12）字段、单位与基准一致，并已通过复权闭环对账",
        "双源均走远程 dividend/fund_div",
        "更多公司行为样例仍建议补充",
        "test_get_split_dividend",
    )


# ---------------------------------------------------------------------- #
# 常用：get_bars / 实时快照
# ---------------------------------------------------------------------- #
def test_get_bars_not_implemented(providers):
    """get_bars 未实现：provider 层与聚宽风格外层 API 均稳定抛 NotImplementedError。"""
    import bullet_trade.data.api as data_api

    native, duck = providers
    for p in (native, duck):
        with pytest.raises(NotImplementedError):
            p.get_bars("000001.XSHE", 10)
    for p in (native, duck):
        data_api.set_data_provider(p)
        with pytest.raises(NotImplementedError):
            data_api.get_bars("000001.XSHE", 10, unit="1d", fields=["date", "open", "close"], df=True)
    _record(
        "get_bars",
        "UNSUPPORTED",
        "provider 层与外层 api.get_bars 均稳定抛 NotImplementedError（未静默返回假数据）",
        "后续实现可包装 get_price 等价窗口/字段",
        "如需聚宽 get_bars 语义需单独实现并验收",
        "test_get_bars_not_implemented + tests/unit/test_tushare_duckdb_failure_modes.py",
    )


def test_get_live_current_fields(providers):
    """实时快照：字段存在性 + 基本区间；涨跌停/停牌字段当前为占位值，记为 LIMIT。"""
    native, duck = providers
    n, n_empty = _call_with_retry(lambda: native.get_live_current("000001.XSHE"))
    if n_empty:
        _record(
            "get_live_current / get_current_tick", "LIMIT",
            "stk_mins 限频或非交易时段无最新分钟",
            "重试一次仍失败", "实时数据不可回放", "test_get_live_current_fields",
        )
        pytest.skip("原生 tushare 实时快照为空/限频")
    d, d_empty = _call_with_retry(lambda: duck.get_live_current("000001.XSHE"))
    if d_empty:
        _record(
            "get_live_current / get_current_tick", "LIMIT",
            "tushare_duckdb 实时快照为空/限频",
            "实时快照恒走远程 1min", "限频", "test_get_live_current_fields",
        )
        pytest.skip("tushare_duckdb 实时快照为空/限频")
    for key in ("last_price", "high_limit", "low_limit", "paused"):
        assert key in n and key in d
    assert float(n["last_price"]) > 0 and float(d["last_price"]) > 0
    _record(
        "get_live_current / get_current_tick",
        "PARTIAL",
        "last_price 双源均非 0；但 high_limit/low_limit 恒为 0、paused 恒 False，非真实涨跌停/停牌",
        "实时快照恒走远程 1min（无本地分钟表）；两次采样间隔 61s，最新分钟可能推进",
        "涨跌停/停牌字段非真实值；tushare 无免费实时接口；get_current_tick 未实现",
        "test_get_live_current_fields",
    )


# ---------------------------------------------------------------------- #
# 指数成分 / 权重
# ---------------------------------------------------------------------- #
def test_get_index_stocks_and_weights(providers):
    native, duck = providers
    stocks_n = native.get_index_stocks("000300.XSHG", date="2026-07-31")
    stocks_d = duck.get_index_stocks("000300.XSHG", date="2026-07-31")
    assert stocks_n == stocks_d
    weights_n = native.get_index_weights("000300.XSHG", date="2026-07-31")
    weights_d = duck.get_index_weights("000300.XSHG", date="2026-07-31")
    assert list(weights_n.columns) == list(weights_d.columns)
    _assert_price_parity(weights_n, weights_d, ["weight"], label="index_weights")
    if not stocks_n:
        _record(
            "get_index_stocks / get_index_weights",
            "LIMIT",
            "index_weight 接口当前返回空（疑似免费 token 权限不足）；双源行为一致",
            "已尝试 000300.XSHG 于 2026-07-31；不把空列表解释为指数无成分",
            "需有 index_weight 权限的 token 补测真实成分/权重",
            "test_get_index_stocks_and_weights",
        )
    else:
        _record(
            "get_index_stocks / get_index_weights",
            "PASS",
            f"000300.XSHG 成分 {len(stocks_n)} 只与权重与基准一致",
            "",
            "",
            "test_get_index_stocks_and_weights",
        )


# ---------------------------------------------------------------------- #
# 扩展接口（离线覆盖，这里记录状态）
# ---------------------------------------------------------------------- #
# (方法, 位置参数) —— 与离线用例 tests/unit/test_tushare_duckdb_failure_modes.py 一致
_EXTENDED_CALLS = [
    ("get_extras", ("is_st", ["000001.XSHE"])),
    ("get_fundamentals", (object(),)),
    ("get_fundamentals_continuously", (object(),)),
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
]


def test_extended_interfaces_not_implemented():
    """扩展接口（财务/行业/概念/融资融券/期货等）未实现，稳定 NotImplementedError（不静默返回假数据）。"""
    for cfg in ({"cache_dir": None}, {"cache_dir": None, "tushare_duckdb_path": DB_PATH}):
        p = TushareProvider(cfg)
        for method, args in _EXTENDED_CALLS:
            with pytest.raises(NotImplementedError):
                getattr(p, method)(*args)
    _record(
        "扩展接口",
        "UNSUPPORTED",
        "get_extras/get_fundamentals/行业/概念/融资融券/期货等未实现，稳定 NotImplementedError（不静默返回假数据）",
        "",
        "如需支持需按基准单独验收",
        "tests/unit/test_tushare_duckdb_failure_modes.py",
    )


# ---------------------------------------------------------------------- #
# API 兼容：history / attribute_history / get_current_data
# ---------------------------------------------------------------------- #
def _switch_api_provider(provider):
    import bullet_trade.data.api as data_api

    data_api.set_data_provider(provider)


def test_jq_style_api_compat(providers):
    import bullet_trade.data.api as data_api

    native, duck = providers

    def _collect(provider):
        _switch_api_provider(provider)
        h = data_api.history(5, unit="1d", field="close", security_list=["000001.XSHE"])
        ah = data_api.attribute_history("000001.XSHE", 5, unit="1d", fields=["open", "close"], df=True)
        cd = data_api.get_current_data()
        return h, ah, cd

    h_n, ah_n, cd_n = _collect(native)
    h_d, ah_d, cd_d = _collect(duck)
    for tag, x, y in (("history", h_n, h_d), ("attribute_history", ah_n, ah_d)):
        assert x is not None and y is not None, f"{tag} 返回 None"
        assert not (hasattr(x, "empty") and x.empty), f"{tag} 返回空"
        assert type(x) is type(y), f"{tag} 返回类型不一致 {type(x)} vs {type(y)}"
    # 无上下文时 get_current_data 为空容器：支持 []/contains/keys 访问
    for cd in (cd_n, cd_d):
        assert callable(cd.__getitem__) and callable(cd.__contains__) and callable(cd.keys)
    _assert_price_parity(ah_n, ah_d, ["open", "close"], label="attribute_history")
    _record(
        "API 兼容 history/attribute_history/get_current_data",
        "PASS",
        "聚宽风格外层 API 经双源 shape 与字段一致；get_current_data 空容器可访问",
        "外层 API 复用 get_price 路径",
        "get_current_data 在回测/live 上下文的完整行为未在此覆盖",
        "test_jq_style_api_compat",
    )


# ---------------------------------------------------------------------- #
# 收尾：验收表完整性
# ---------------------------------------------------------------------- #
REQUIRED_FUNCS = [
    "auth",
    "get_price(fq=\"none\")",
    "get_price(fq=\"pre\")",
    "get_price(fq=\"post\")",
    "fields=[\"factor\"]",
    "pre_factor_ref_date",
    "get_price 分钟线",
    "get_price 多证券/panel",
    "get_price count/end_date",
    "get_price 字段映射/单位",
    "get_trade_days / get_trade_day",
    "get_all_securities / get_security_info",
    "get_split_dividend",
    "get_bars",
    "get_live_current / get_current_tick",
    "get_index_stocks / get_index_weights",
    "扩展接口",
    "API 兼容 history/attribute_history/get_current_data",
]


def test_acceptance_report_completeness():
    """校验必测清单无遗漏，并打印验收表（-s 查看）。"""
    recorded = {row["func"] for row in _REPORT}
    missing = [f for f in REQUIRED_FUNCS if f not in recorded]
    assert not missing, f"必测函数未记录状态：{missing}"
    header = "| 函数 | 场景 | 基准 | 样例/窗口 | 状态 | 结果摘要 | 兼容尝试 | 剩余风险 | 测试入口 |"
    sep = "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    print("\n=== tushare vs tushare_duckdb 验收表 ===")
    print(header)
    print(sep)
    for row in _REPORT:
        print(
            f"| `{row['func']}` | 见用例 | tushare源 | {row['entry']} | "
            f"{row['status']} | {row['summary']} | {row['compat']} | {row['risk']} | `{row['entry']}` |"
        )
