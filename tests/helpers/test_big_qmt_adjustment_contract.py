"""大 QMT helper 完整除权事实与显式复权参数的离线契约回归。

作者：BruceLee
职责：验证七项 QMT 事件的保真传输、旧字段兼容、输入错误及行情参数优先级。
输入：本地 helper 源码与 fake ContextInfo；不读取真实账户、行情或配置。
输出：pytest 断言；事件事实可传输不代表本地算法已支持全部公司行为。
上下游：调用 helper 数据分派入口，不依赖 QMT 或聚宽运行环境。
环境：仅依赖 pandas/pytest 和 helper 已有依赖，不联网、不启停服务、不调用交易。
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest


_BUILD_ID = "20260908_dividend_facts_v1"
_SCHEMA = "big-qmt-dividend-events/v1"
_SOURCE = "ContextInfo.get_divid_factors"
_CASH_ROW = [0.149, 0.0, 0.0, 0.0, 0.0, 0, 0.98]
_RAW_FIELDS = ["interest", "stockBonus", "stockGift", "allotNum", "allotPrice", "gugai", "dr"]


@pytest.fixture
def helper():
    """加载本地 GBK helper；无参数，返回模块，不调用初始化或 HTTP 启动入口。"""
    path = Path(__file__).parents[2] / "helpers" / "big_qmt_gateway_strategy_sample.py"
    spec = importlib.util.spec_from_file_location("big_qmt_adjustment_contract_helper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _DividendContext:
    """提供可记录调用的假事件接口，保存预定结果或异常，不持有任何外部连接。"""

    def __init__(self, result):
        """保存接口响应；输入映射或异常，返回 None，仅初始化本地测试状态。"""
        self.result = result
        self.calls = []

    def get_divid_factors(self, security):
        """模拟单参数事件查询；输入证券，返回预定结果或抛出预定异常，记录调用。"""
        self.calls.append(security)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _HistoryContext:
    """提供可记录参数的假行情接口，只返回本地构造的单根日线。"""

    def __init__(self):
        """初始化行情调用记录；无参数，返回 None，不产生外部副作用。"""
        self.calls = []

    def get_market_data_ex(self, fields, securities, **kwargs):
        """模拟行情查询；输入字段、证券和参数，返回本地 DataFrame 并记录参数。"""
        self.calls.append((fields, securities, kwargs))
        return {
            securities[0]: pd.DataFrame(
                {"open": [1.25], "close": [1.35]},
                index=pd.Index(["20260907"], name="stime"),
            )
        }


def _query(helper, result, **payload):
    """通过真实数据分派查询假事件；输入模块、假结果和参数，返回响应与调用记录对象。"""
    context = _DividendContext(result)
    request = {"security": "510500.XSHG", "request_id": "event-contract", **payload}
    response = helper._dispatch_qmt_action(context, "split_dividend", request)
    return response, context


def _epoch_key(iso_utc, milliseconds=False):
    """生成固定 UTC 时刻的时间戳；输入 ISO 字符串及毫秒标志，返回字符串，无副作用。"""
    seconds = int(datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc).timestamp())
    return str(seconds * 1000 if milliseconds else seconds)


@pytest.mark.unit
@pytest.mark.parametrize(
    "security,canonical,security_type,per_base",
    [
        ("000001.SZ", "000001.XSHE", "stock", 10),
        ("510500.SH", "510500.XSHG", "fund", 1),
    ],
)
def test_complete_event_preserves_legacy_fields_and_raw_seven_items(
    helper, security, canonical, security_type, per_base
):
    """验证新旧字段兼容；输入证券与单位，断言七项事实和旧六字段均保真，无返回值。"""
    row = [0.362, 0.1, 0.2, 0.3, 4.5, 1, 0.98]
    response, context = _query(helper, {"20260907": row}, security=security)

    assert response["ok"] is True
    value = response["value"]
    assert value["schema"] == _SCHEMA
    assert value["source"] == _SOURCE
    assert value["gateway_build_id"] == _BUILD_ID
    assert value["raw_event_count"] == 1
    assert value["event_fields_complete"] is True
    assert value["history_completeness_verified"] is False
    event = value["events"][0]
    assert event["security"] == canonical
    assert event["date"] == "2026-09-07"
    assert event["security_type"] == security_type
    assert event["per_base"] == per_base
    assert event["bonus_pre_tax"] == pytest.approx(row[0] * per_base)
    assert event["scale_factor"] == pytest.approx(1.6)
    assert event["cash_per_share"] == row[0]
    assert event["gift"] == row[1]
    assert event["transfer"] == row[2]
    assert event["rights"] == row[3]
    assert event["rights_price"] == row[4]
    assert event["share_reform"] is True
    assert event["qmt_dr"] == row[6]
    assert event["qmt_raw"] == dict(zip(_RAW_FIELDS, row))
    assert event["source_timestamp"] == "20260907"
    assert context.calls == [security]


@pytest.mark.unit
@pytest.mark.parametrize("field_index", [1, 2])
def test_negative_etf_share_change_is_preserved_when_total_scale_is_positive(helper, field_index):
    """验证 ETF 拆并事实保真；输入负送转所在项，断言负值不被改零，不宣称算法支持。"""
    row = _CASH_ROW.copy()
    row[field_index] = -0.75
    response, _ = _query(helper, {"20260907": tuple(row)})

    assert response["ok"] is True
    event = response["value"]["events"][0]
    assert event["scale_factor"] == 0.25
    assert event["qmt_raw"][_RAW_FIELDS[field_index]] == -0.75
    assert event["gift" if field_index == 1 else "transfer"] == -0.75


@pytest.mark.unit
def test_event_keeps_nonzero_rights_price_without_inventing_a_rights_issue(helper):
    """验证事实层不代替数学校验；输入无配股但有配股价的事件，断言原值保留。"""
    row = _CASH_ROW.copy()
    row[4] = 3.5
    response, _ = _query(helper, {"20260907": row})

    assert response["ok"] is True
    event = response["value"]["events"][0]
    assert event["rights"] == 0
    assert event["rights_price"] == 3.5
    assert event["qmt_raw"]["allotPrice"] == 3.5


@pytest.mark.unit
@pytest.mark.parametrize("row", [None, [], [1, 2, 3], [0] * 6, [0] * 8, {}, "0,0,0,0,0,0,1"])
def test_event_rejects_incomplete_or_non_sequence_native_rows(helper, row):
    """拒绝不完整事件；输入非法原生行，断言明确数据错误而非自动补零成功。"""
    response, context = _query(helper, {"20260907": row})

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "field_index,value",
    [
        (0, -0.1),
        (3, -0.1),
        (4, -0.1),
        (6, 0),
        (6, -1),
        (0, float("nan")),
        (1, float("inf")),
        (2, float("-inf")),
        (3, float("nan")),
        (4, float("inf")),
        (5, float("nan")),
        (6, float("inf")),
        (5, 2),
        (5, -1),
        (5, 0.5),
        (1, -1),
        (2, -2),
    ],
)
def test_event_rejects_invalid_numeric_domains(helper, field_index, value):
    """验证事件数值边界；输入非法项和值，断言非有限数、非法比例或标记明确失败。"""
    row = _CASH_ROW.copy()
    row[field_index] = value
    response, _ = _query(helper, {"20260907": row})

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"


@pytest.mark.unit
@pytest.mark.parametrize("field_index", [0, 1, 2, 3, 4, 6])
def test_event_rejects_boolean_numbers_outside_share_reform_flag(helper, field_index):
    """拒绝布尔冒充数值；输入非股改字段索引，断言该字段为 bool 时明确失败。"""
    row = _CASH_ROW.copy()
    row[field_index] = True
    response, _ = _query(helper, {"20260907": row})

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"


@pytest.mark.unit
@pytest.mark.parametrize("flag", [False, True])
def test_event_accepts_boolean_share_reform_flags(helper, flag):
    """允许明确股改布尔值；输入 False 或 True，断言规范字段与原始字段均保留。"""
    row = _CASH_ROW.copy()
    row[5] = flag
    response, _ = _query(helper, {"20260907": row})

    assert response["ok"] is True
    assert response["value"]["events"][0]["share_reform"] is flag
    assert response["value"]["events"][0]["qmt_raw"]["gugai"] == flag


@pytest.mark.unit
@pytest.mark.parametrize("raw", [None, [], "invalid"])
def test_event_api_invalid_container_is_not_a_successful_empty_result(helper, raw):
    """区分无效返回和合法空事件；输入非映射结果，断言数据错误而非成功空列表。"""
    response, _ = _query(helper, raw)

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"


@pytest.mark.unit
def test_empty_event_dictionary_does_not_claim_complete_history(helper):
    """验证合法空返回；输入空字典，断言成功但事件字段与历史完整性均不冒充已验证。"""
    response, context = _query(helper, {})

    assert response["ok"] is True
    assert response["value"]["events"] == []
    assert response["value"]["raw_event_count"] == 0
    assert response["value"]["event_fields_complete"] is False
    assert response["value"]["history_completeness_verified"] is False
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
def test_event_api_exception_is_not_converted_to_no_events(helper):
    """保留 API 查询异常；输入预定异常，断言明确失败，不能伪装没有公司行为。"""
    response, context = _query(helper, RuntimeError("fake data unavailable"))

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_FAILED"
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
def test_missing_event_api_reports_not_ready(helper):
    """区分接口缺失；输入没有事件方法的假上下文，断言 API 未就绪，不尝试替代数据源。"""
    response = helper._dispatch_qmt_action(object(), "split_dividend", {"security": "510500.XSHG"})

    assert response["ok"] is False
    assert response["code"] == "QMT_API_NOT_READY"


@pytest.mark.unit
@pytest.mark.parametrize(
    "key,expected_date",
    [
        ("20260907", "2026-09-07"),
        ("20260907091500", "2026-09-07"),
        ("2026-09-07", "2026-09-07"),
        ("2026-09-07T09:15:00", "2026-09-07"),
        (_epoch_key("2026-09-06T16:00:00"), "2026-09-07"),
        (_epoch_key("2026-09-06T16:00:00", milliseconds=True), "2026-09-07"),
        ("673113600", "1991-05-02"),
        ("673113600000", "1991-05-02"),
        ("31536000000", "1971-01-01"),
    ],
)
def test_event_date_formats_use_fixed_china_timezone(helper, key, expected_date):
    """验证日期和时间戳合同；输入合法原始键及日期，断言固定东八区日期和原键保留。"""
    response, _ = _query(helper, {key: _CASH_ROW.copy()})

    assert response["ok"] is True
    event = response["value"]["events"][0]
    assert event["date"] == expected_date
    assert event["source_timestamp"] == str(key)


@pytest.mark.unit
@pytest.mark.parametrize(
    "key", ["", "garbage", "20260230", "20261301", "20260907250000", "2026-02-30"]
)
def test_event_rejects_invalid_calendar_keys_even_outside_requested_window(helper, key):
    """拒绝非法日期漏过筛选；输入非法原始键，断言窄窗口也不能把它静默丢弃。"""
    response, _ = _query(helper, {key: _CASH_ROW.copy()}, start="2026-09-01", end="2026-09-07")

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"


@pytest.mark.unit
def test_event_rejects_duplicate_normalized_dates(helper):
    """拒绝同日重复事件；输入两种格式指向同一日期的键，断言不能重复计入公司行为。"""
    response, _ = _query(helper, {"20260907": _CASH_ROW.copy(), "2026-09-07": _CASH_ROW.copy()})

    assert response["ok"] is False
    assert response["code"] == "SPLIT_DIVIDEND_INVALID_DATA"


@pytest.mark.unit
def test_out_of_window_duplicate_dates_and_invalid_rows_do_not_block_requested_events(helper):
    """隔离窗口外重复事件；输入旧日重复键和坏行，断言窗口内完整事件仍可正常返回。"""
    response, context = _query(
        helper,
        {
            "20000101": None,
            "2000-01-01": [float("nan")],
            "20260907": _CASH_ROW.copy(),
        },
        start="2026-09-07",
        end="2026-09-07",
    )

    assert response["ok"] is True
    assert [event["date"] for event in response["value"]["events"]] == ["2026-09-07"]
    assert response["value"]["raw_event_count"] == 3
    assert response["value"]["event_fields_complete"] is True
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
def test_event_filters_numeric_rows_after_validating_all_dates(helper):
    """验证筛选边界；输入窗口外坏数值与窗口内完整事件，断言只校验所请求行的数值。"""
    response, context = _query(
        helper,
        {"20260101": [float("nan")], "20260907": _CASH_ROW.copy()},
        start="2026-09-07",
        end="2026-09-07",
    )

    assert response["ok"] is True
    assert [event["date"] for event in response["value"]["events"]] == ["2026-09-07"]
    assert response["value"]["raw_event_count"] == 2
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
def test_events_are_sorted_by_normalized_date_not_native_key_string(helper):
    """验证日期排序；输入混合格式且乱序的键，断言按规范日期返回，不按字符串顺序。"""
    response, _ = _query(
        helper,
        {
            "20260907": _CASH_ROW.copy(),
            "2026-01-16": _CASH_ROW.copy(),
            "20250116": _CASH_ROW.copy(),
        },
    )

    assert response["ok"] is True
    assert [event["date"] for event in response["value"]["events"]] == [
        "2025-01-16",
        "2026-01-16",
        "2026-09-07",
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "bounds",
    [
        {"start": "20260230"},
        {"end": "not-a-date"},
        {"start": "2026-09-08", "end": "2026-09-07"},
    ],
)
def test_invalid_event_query_bounds_fail_before_calling_qmt(helper, bounds):
    """提前拒绝非法查询边界；输入非法或倒序日期，断言 BAD_REQUEST 且不调用事件 API。"""
    response, context = _query(helper, {"20260907": _CASH_ROW.copy()}, **bounds)

    assert response["ok"] is False
    assert response["code"] == "BAD_REQUEST"
    assert context.calls == []


@pytest.mark.unit
@pytest.mark.parametrize("bounds", [{}, {"start": None, "end": None}, {"start": "", "end": ""}])
def test_missing_event_query_bounds_remain_compatible(helper, bounds):
    """保持可选日期兼容；输入缺省、空串或 None 边界，断言按单参数 API 查询成功。"""
    response, context = _query(helper, {"20260907": _CASH_ROW.copy()}, **bounds)

    assert response["ok"] is True
    assert context.calls == ["510500.SH"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "parameters,expected",
    [
        ({}, "follow"),
        ({"fq": None}, "none"),
        ({"dividend_type": None}, "none"),
        ({"fq": "none"}, "none"),
        ({"fq": "pre"}, "front_ratio"),
        ({"fq": "qfq"}, "front_ratio"),
        ({"fq": "post"}, "back_ratio"),
        ({"fq": "hfq"}, "back_ratio"),
        ({"fq": "front"}, "front"),
        ({"fq": "front_ratio"}, "front_ratio"),
        ({"fq": "back"}, "back"),
        ({"fq": "back_ratio"}, "back_ratio"),
        ({"fq": "follow"}, "follow"),
        ({"dividend_type": "front"}, "front"),
        ({"fq": None, "dividend_type": "front"}, "none"),
        ({"fq": "pre", "dividend_type": "back"}, "front_ratio"),
    ],
)
def test_history_distinguishes_explicit_fq_from_missing_parameter(
    helper, monkeypatch, parameters, expected
):
    """验证复权参数优先级；输入参数组合，断言显式 None 不丢失且省略仍保持 follow。"""
    context = _HistoryContext()
    ensure = Mock(return_value={})
    monkeypatch.setattr(helper, "_auto_ensure_history_cache", ensure)

    response = helper._dispatch_qmt_action(
        context, "history", {"security": "000001.XSHE", "fields": ["close"], **parameters}
    )

    assert response["ok"] is True
    assert len(context.calls) == 1
    assert context.calls[0][2]["dividend_type"] == expected
    assert context.calls[0][2]["subscribe"] is False
    ensure.assert_called_once()


@pytest.mark.unit
@pytest.mark.parametrize("value", ["", "unknown", 0, 1, False, [], {}])
def test_history_invalid_fq_fails_before_download_or_query(helper, monkeypatch, value):
    """提前拒绝非法复权参数；输入非法 fq，断言不下载、不查询，也不采用次优字段。"""
    context = _HistoryContext()
    ensure = Mock(return_value={})
    monkeypatch.setattr(helper, "_auto_ensure_history_cache", ensure)

    response = helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fq": value, "dividend_type": "front"},
    )

    assert response["ok"] is False
    assert response["code"] == "BAD_REQUEST"
    assert context.calls == []
    ensure.assert_not_called()


@pytest.mark.unit
def test_health_exposes_dividend_contract_and_build_without_starting_service(helper):
    """验证运行版本可追溯；输入已加载模块，断言 health 报告事件协议但不启动 HTTP。"""
    runtime = helper._GatewayRuntime()
    health = runtime.health()

    assert helper.GATEWAY_BUILD_ID == _BUILD_ID
    assert helper.DIVIDEND_EVENT_SCHEMA == _SCHEMA
    assert health["gateway_build_id"] == _BUILD_ID
    assert health["dividend_event_schema"] == _SCHEMA
    assert runtime.http_server is None
    assert runtime.http_thread is None
    assert runtime.ioloop is None


@pytest.mark.unit
def test_event_query_has_no_download_trade_or_lifecycle_side_effects(helper, monkeypatch):
    """验证事件查询副作用边界；输入假完整事件，断言无下载、交易、撤单或服务启动调用。"""
    guarded_names = [
        "_auto_ensure_history_cache",
        "_call_download_history_data",
        "_place_order",
        "_cancel_order",
        "_cancel_by_rule",
        "_call_passorder",
        "_start_http_server",
        "_start_http_server_background",
        "_tornado_thread_main",
    ]
    guards = []
    for name in guarded_names:
        guard = Mock(side_effect=AssertionError("事件查询不得调用 " + name))
        monkeypatch.setattr(helper, name, guard)
        guards.append(guard)

    response, context = _query(helper, {"20260907": _CASH_ROW.copy()})

    assert response["ok"] is True
    assert context.calls == ["510500.SH"]
    for guard in guards:
        guard.assert_not_called()
