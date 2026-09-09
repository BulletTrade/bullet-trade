"""2026-09-09 仿真实机分钟边界的离线回放。

作者：BruceLee
职责：对原始QMT输入独立计算，并将实际公开响应作为重构前合同。
输入：脱敏仿真行情/元数据、既有完整除权事实及前收盘依赖；输出：逐格和完整时间轴断言。
上下游：真实 BigQmtDataAdapter/RemoteQmtProvider，既有白名单内存Gateway；不联网、不交易。
环境：本地pytest，固定上海时间；聚宽只比较已记录时间轴，不用源数值差异放宽QMT手算。
"""

import json
from pathlib import Path

import pandas as pd
import pytest
from test_big_qmt_history_real_minutes import _aggregate, _expected_basic, _MinuteReplayGateway
from test_big_qmt_history_standardization import _client

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.server.adapters import big_qmt as module

pytestmark = pytest.mark.unit
_DIR = Path(__file__).parents[1] / "fixtures"
_FIELDS = ["open", "high", "low", "close", "volume", "money"]


def load_capture():
    """读取仿真固定样本；无输入，返回字典，仅访问本地纳管JSON。"""
    return json.loads((_DIR / "big_qmt_sim_intraday_20260909.json").read_text("utf-8"))


def make_gateway(capture):
    """将新原始分钟放入已有事实网关；输入样本，返回内存网关，不制造事件或前收盘。"""

    def read(name):
        """读取已纳管依赖；输入文件名，返回JSON，不调用外部数据源。"""
        return json.loads((_DIR / name).read_text("utf-8"))

    gateway = _MinuteReplayGateway(
        read("big_qmt_real_minutes_20260908.json"),
        read("big_qmt_history_facts_20260908.json"),
        read("big_qmt_post_dependencies_20260908.json"),
    )
    for security in ("000001.XSHE", "510500.XSHG"):
        facts = {item["id"]: item["result"] for item in capture["facts"]}
        source = facts[security + "-raw_minutes"]
        raw = pd.DataFrame(source["records"], columns=source["columns"]).set_index("stime")
        raw.index = pd.to_datetime(raw.index, format="%Y%m%d%H%M%S")
        gateway.minutes[security] = raw.rename_axis("time")
        event_fields = (
            "date",
            "cash_per_share",
            "gift",
            "transfer",
            "rights",
            "rights_price",
            "share_reform",
        )
        old_events = gateway.facts[security]["events"]["events"]
        fresh_events = facts[security + "-events"]["events"]
        assert [{key: event[key] for key in event_fields} for event in old_events] == [
            {key: event[key] for key in event_fields} for event in fresh_events
        ]
        gateway.facts[security]["events"] = facts[security + "-events"]
        gateway.facts[security]["info"] = facts[security + "-info"]
        days = pd.DatetimeIndex(pd.to_datetime(facts[security + "-calendar"]["values"]))
        gateway.calendars[security] = gateway.calendars[security].union(days).sort_values()
    return gateway


def independent_expected(gateway, request):
    """独立重建公开价格；输入网关和请求，返回期望表，不调用adapter或纯生产算法。"""
    security, mode = request["security"], request["fq"]
    if mode == "pre":
        left = gateway.minutes[security].index[0].normalize()
        assert not any(
            left < pd.Timestamp(event["date"]) <= pd.Timestamp("2026-09-09")
            for event in gateway.facts[security]["events"]["events"]
        )
    frame = _expected_basic(gateway, security, "post" if mode == "post" else None)[_FIELDS]
    frame = frame.loc[: pd.Timestamp(request["end_date"])]
    size = int(request["frequency"][:-1])
    if request.get("count"):
        frame = frame.tail(request["count"] * size)
    else:
        frame = frame.loc[pd.Timestamp(request["start_date"]) :]
    return _aggregate(frame, size)


@pytest.fixture(scope="module")
def capture():
    """提供共享只读样本；无输入，返回字典，不修改文件。"""
    return load_capture()


@pytest.fixture
def gateway(capture, monkeypatch):
    """固定回放时钟并建立隔离网关；输入夹具，返回内存网关，无外部动作。"""

    def frozen_now():
        """读取样本之后固定时钟；无输入，返回时间戳，无副作用。"""
        return pd.Timestamp("2026-09-09 12:50:00")

    def frozen_today():
        """固定前复权参考日期；无输入，返回样本日期，避免依赖测试运行当天。"""
        return frozen_now().date()

    monkeypatch.setattr(module, "_history_now", frozen_now)
    monkeypatch.setattr(module, "_history_today", frozen_today)
    return make_gateway(capture)


@pytest.mark.parametrize("position", range(52))
def test_sim_public_response_matches_independent_raw(capture, gateway, position):
    """验收现场响应数值；输入样本网关和序号，无返回，完整逐格比较独立原始事实算法。"""
    case = capture["public"][position]
    assert case["status"] == "captured", case["id"]
    actual = _dataframe_from_payload(case["result"])
    expected = independent_expected(gateway, case["request"])
    pd.testing.assert_frame_equal(
        actual, expected, check_names=False, check_dtype=False, check_exact=True
    )


@pytest.mark.parametrize("position", range(52))
def test_current_adapter_replays_sim_public_contract(capture, gateway, position):
    """验收重构前输出合同；输入样本网关和序号，无返回，经真实provider/adapter重放现场输出。"""
    case = capture["public"][position]
    provider, _ = _client(gateway)
    actual = provider.get_price(**case["request"])
    expected = _dataframe_from_payload(case["result"])
    pd.testing.assert_frame_equal(
        actual, expected, check_names=False, check_dtype=False, check_exact=True
    )


def test_jq_time_axes_match_without_claiming_equal_source_values(capture):
    """验证聚宽时间轴合同；输入样本，无返回，只比较完整索引，源值差异另行报告。"""
    by_id = {case["id"]: case for case in capture["public"]}
    assert len(capture["jq_reference"]) == 24
    for reference in capture["jq_reference"]:
        key = reference["id"]
        key += "-None" if key.endswith(("-start", "-count")) else ""
        actual = _dataframe_from_payload(by_id[key]["result"])
        expected = pd.to_datetime(reference["result"]["index"])
        assert actual.index.tolist() == expected.tolist(), key


def test_saved_before_after_and_readonly_interface_contract(capture):
    """验证原始失败和只读接口证据完整；输入样本，无返回，不把预期未找到订单当接口故障。"""
    assert len(capture["before_reload"]) == 2
    assert all(case["status"] == "empty" and case["rows"] == 0 for case in capture["before_reload"])
    by_id = {case["id"]: case for case in capture["public"]}
    for security in ("000001.XSHE", "510500.XSHG"):
        first = _dataframe_from_payload(by_id[security + "-same-first"]["result"])
        repeat = _dataframe_from_payload(by_id[security + "-same-repeat"]["result"])
        assert len(first) == 7
        pd.testing.assert_frame_equal(first, repeat, check_exact=True)
    assert len(capture["interface_checks"]) == 12
    for case in capture["interface_checks"]:
        assert case["explicit_existing_account"] is True
        if case["id"] == "/order_status":
            assert case["error"]["code"] == "ORDER_NOT_FOUND"
        else:
            assert case["status"] == "captured"
