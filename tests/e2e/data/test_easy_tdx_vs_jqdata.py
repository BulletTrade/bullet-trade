"""
easy_tdx 与 JQData 数据口径对账测试

文件职责：用真实 online easy_tdx 和真实 JQData 对比样例证券行情口径。
主要输入：JQData 账号环境变量、easy-tdx online 行情服务器、样例证券和时间窗口。
主要输出：pytest 断言结果，覆盖未复权、前复权、分钟线和动态前复权限制。
上下游关系：属于可选 e2e 测试，不参与默认离线单元测试；用于 Beta 发布前验收。
关键环境或配置约定：需要 `JQDATA_USERNAME/JQDATA_PASSWORD`，并安装 `easy-tdx`。
"""

from __future__ import annotations

import importlib
import os
from typing import Iterable

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from bullet_trade.data.providers.jqdata import JQDataProvider
from bullet_trade.utils.env_loader import load_env

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_network,
    pytest.mark.requires_jqdata,
    pytest.mark.requires_tdx,
]

SECURITY = "000001.XSHE"
LONG_SECURITIES = ["000001.XSHE", "600519.XSHG", "510050.XSHG"]
DAILY_START = "2026-06-10"
DAILY_END = "2026-06-12"
LONG_DAILY_START = "2024-01-01"
LONG_DAILY_END = "2026-06-12"
MINUTE_START = "2026-06-12 14:31:00"
MINUTE_END = "2026-06-12 15:00:00"
FIELDS = ["open", "high", "low", "close", "volume", "money"]
PRICE_FIELDS = ["open", "high", "low", "close"]


def _ensure_module(name: str, install_hint: str) -> None:
    """确认依赖模块存在；缺失时跳过可选 e2e 测试。"""
    try:
        importlib.import_module(name)
    except ImportError as exc:
        pytest.skip(f"{name} 未安装（{exc}），请执行 `{install_hint}` 后重试。")


def _check_prerequisites() -> None:
    """加载环境并检查 JQData/easy_tdx 对账前置条件。"""
    load_env()
    _ensure_module("jqdatasdk", "pip install jqdatasdk")
    _ensure_module("easy_tdx", "pip install easy-tdx")
    if not os.getenv("JQDATA_USERNAME") or not os.getenv("JQDATA_PASSWORD"):
        pytest.skip("缺少 JQDATA_USERNAME/JQDATA_PASSWORD，跳过 easy_tdx vs JQData 对账。")


def _providers() -> tuple[JQDataProvider, EasyTdxProvider]:
    """创建并认证 JQData 与 easy_tdx provider。"""
    _check_prerequisites()
    jq = JQDataProvider({"cache_dir": None})
    try:
        jq.auth()
    except Exception as exc:
        pytest.skip(f"JQData 认证失败：{exc}")
    tdx = EasyTdxProvider({"timeout": 15.0})
    try:
        tdx.auth()
    except Exception as exc:
        pytest.skip(f"easy_tdx 连接失败：{exc}")
    return jq, tdx


def _align_frames(jq_df: pd.DataFrame, tdx_df: pd.DataFrame) -> pd.DataFrame:
    """按索引对齐两个 provider 的 DataFrame，并返回差值表。"""
    if jq_df.empty or tdx_df.empty:
        pytest.skip(f"对账数据为空：JQData rows={len(jq_df)} easy_tdx rows={len(tdx_df)}")
    jq_norm = jq_df.copy()
    tdx_norm = tdx_df.copy()
    jq_norm.index = pd.to_datetime(jq_norm.index)
    tdx_norm.index = pd.to_datetime(tdx_norm.index)
    common_index = jq_norm.index.intersection(tdx_norm.index)
    if common_index.empty:
        pytest.skip("JQData 与 easy_tdx 没有可对齐的时间索引。")
    columns = [field for field in FIELDS if field in jq_norm.columns and field in tdx_norm.columns]
    diff = (jq_norm.loc[common_index, columns] - tdx_norm.loc[common_index, columns]).abs()
    return diff


def _assert_close_enough(
    diff: pd.DataFrame, price_tol: float, volume_tol: float, money_tol: float
) -> None:
    """按字段类型检查对齐后的最大偏差。"""
    for field in diff.columns:
        max_diff = float(diff[field].max())
        if field == "volume":
            tolerance = volume_tol
        elif field == "money":
            tolerance = money_tol
        else:
            tolerance = price_tol
        assert max_diff <= tolerance, f"{field} 最大偏差 {max_diff} 超过阈值 {tolerance}"


def _print_diff_summary(label: str, diff: pd.DataFrame, fields: Iterable[str]) -> None:
    """输出对账差异摘要，便于真实数据失败时定位字段。"""
    summary = {field: float(diff[field].max()) for field in fields if field in diff.columns}
    print(f"[DEBUG] {label} diff max: {summary}")


def _max_diff_by_field(diff: pd.DataFrame, fields: Iterable[str]) -> dict[str, float]:
    """返回字段最大差异，便于长窗口复权验收做显式断言。"""
    return {field: float(diff[field].max()) for field in fields if field in diff.columns}


def test_easy_tdx_daily_raw_matches_jqdata_recent_window() -> None:
    """easy_tdx 近期日线未复权价格应与 JQData 基本一致。"""
    jq, tdx = _providers()

    jq_df = jq.get_price(
        SECURITY,
        start_date=DAILY_START,
        end_date=DAILY_END,
        frequency="daily",
        fields=FIELDS,
        fq="none",
    )
    tdx_df = tdx.get_price(
        SECURITY,
        start_date=DAILY_START,
        end_date=DAILY_END,
        frequency="daily",
        fields=FIELDS,
        fq="none",
    )

    diff = _align_frames(jq_df, tdx_df)
    _print_diff_summary("daily raw", diff, FIELDS)
    _assert_close_enough(diff, price_tol=0.02, volume_tol=2000.0, money_tol=200000.0)


def test_easy_tdx_long_daily_raw_matches_jqdata_across_adjustment_window() -> None:
    """easy_tdx 长窗口未复权日线应与 JQData 对齐，作为复权验收基础。"""
    jq, tdx = _providers()

    for security in LONG_SECURITIES:
        jq_df = jq.get_price(
            security,
            start_date=LONG_DAILY_START,
            end_date=LONG_DAILY_END,
            frequency="daily",
            fields=FIELDS,
            fq="none",
        )
        tdx_df = tdx.get_price(
            security,
            start_date=LONG_DAILY_START,
            end_date=LONG_DAILY_END,
            frequency="daily",
            fields=FIELDS,
            fq="none",
        )

        diff = _align_frames(jq_df, tdx_df)
        assert len(diff) >= 500
        _print_diff_summary(f"{security} long daily raw", diff, FIELDS)
        _assert_close_enough(diff, price_tol=0.02, volume_tol=5000.0, money_tol=200000.0)


def test_easy_tdx_daily_pre_adjustment_is_comparable_with_jqdata() -> None:
    """easy_tdx 近期日线前复权应与 JQData 处在可比范围。"""
    jq, tdx = _providers()

    jq_df = jq.get_price(
        SECURITY,
        start_date=DAILY_START,
        end_date=DAILY_END,
        frequency="daily",
        fields=["open", "close"],
        fq="pre",
    )
    tdx_df = tdx.get_price(
        SECURITY,
        start_date=DAILY_START,
        end_date=DAILY_END,
        frequency="daily",
        fields=["open", "close"],
        fq="pre",
    )

    diff = _align_frames(jq_df, tdx_df)
    _print_diff_summary("daily pre", diff, ["open", "close"])
    _assert_close_enough(diff, price_tol=0.02, volume_tol=0.0, money_tol=0.0)


def test_easy_tdx_long_daily_pre_adjustment_matches_jqdata_with_constructed_factor() -> None:
    """easy_tdx 长窗口前复权应使用构造 factor 与 JQData 保持可比。"""
    jq, tdx = _providers()

    for security in LONG_SECURITIES:
        jq_df = jq.get_price(
            security,
            start_date=LONG_DAILY_START,
            end_date=LONG_DAILY_END,
            frequency="daily",
            fields=PRICE_FIELDS,
            fq="pre",
            pre_factor_ref_date=LONG_DAILY_END,
        )
        tdx_df = tdx.get_price(
            security,
            start_date=LONG_DAILY_START,
            end_date=LONG_DAILY_END,
            frequency="daily",
            fields=PRICE_FIELDS,
            fq="pre",
            pre_factor_ref_date=LONG_DAILY_END,
        )

        diff = _align_frames(jq_df, tdx_df)
        max_diff = _max_diff_by_field(diff, PRICE_FIELDS)
        _print_diff_summary(f"{security} long daily pre", diff, PRICE_FIELDS)
        assert max(max_diff.values()) <= 0.11


def test_easy_tdx_recent_minute_raw_matches_jqdata() -> None:
    """easy_tdx 最近 1m 未复权分钟线应与 JQData 基本一致。"""
    jq, tdx = _providers()

    jq_df = jq.get_price(
        SECURITY,
        start_date=MINUTE_START,
        end_date=MINUTE_END,
        frequency="1m",
        fields=FIELDS,
        fq="none",
    )
    tdx_df = tdx.get_price(
        SECURITY,
        start_date=MINUTE_START,
        end_date=MINUTE_END,
        frequency="1m",
        fields=FIELDS,
        fq="none",
    )

    diff = _align_frames(jq_df, tdx_df)
    _print_diff_summary("minute raw", diff, FIELDS)
    _assert_close_enough(diff, price_tol=0.02, volume_tol=2000.0, money_tol=200000.0)


def test_easy_tdx_dynamic_pre_factor_ref_date_matches_jqdata_daily() -> None:
    """easy_tdx 动态前复权日线应按指定参考日与 JQData 对齐。"""
    jq, tdx = _providers()
    ref_date = "2025-06-27"

    jq_df = jq.get_price(
        "600519.XSHG",
        start_date=LONG_DAILY_START,
        end_date=LONG_DAILY_END,
        frequency="daily",
        fields=PRICE_FIELDS,
        fq="pre",
        pre_factor_ref_date=ref_date,
    )
    tdx_df = tdx.get_price(
        "600519.XSHG",
        start_date=LONG_DAILY_START,
        end_date=LONG_DAILY_END,
        frequency="daily",
        fields=PRICE_FIELDS,
        fq="pre",
        pre_factor_ref_date=ref_date,
    )

    diff = _align_frames(jq_df, tdx_df)
    max_diff = _max_diff_by_field(diff, PRICE_FIELDS)
    _print_diff_summary("600519.XSHG dynamic daily pre", diff, PRICE_FIELDS)
    assert max(max_diff.values()) <= 0.11
