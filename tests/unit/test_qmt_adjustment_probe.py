"""复权取数工具的路由、失败及凭据边界测试；作者：BruceLee。

输入为注入的假helper响应和临时输出路径，输出pytest断言；不访问网络或真实账户。
上下游为server.adjustment_probe，不启动QMT、服务或任何策略。
"""

import json
from copy import deepcopy

import pytest

from bullet_trade.server.adjustment_probe import (
    FIELDS,
    MODES,
    capture_adjustment_inputs,
    main,
    summarize_capture,
)


def _frame():
    """构造含完整量价的一行假行情；无输入，返回新字典，无外部副作用。"""

    return {
        "dtype": "dataframe",
        "columns": ["time", *FIELDS],
        "records": [["2026-09-07", 10, 11, 9, 10, 100, 1000, 10]],
    }


def _request_recorder():
    """创建记录调用的假读取函数；无输入，返回函数和调用列表，不访问网络。"""

    calls = []

    def request(path, payload):
        """记录路径及参数，返回健康、压缩事件或行情假响应；不调用任何外部接口。"""

        calls.append((path, deepcopy(payload)))
        if path == "/health":
            return {"ready": True, "qmt_api_ready": True, "account_id": "do-not-save"}
        if path == "/data/split_dividend":
            return {
                "events": [
                    {"date": "2026-09-07", "scale_factor": 1, "bonus_pre_tax": 1, "per_base": 10}
                ]
            }
        return _frame()

    return request, calls


def _capture(request, **kwargs):
    """以固定安全范围调用采集函数；输入假request和覆盖项，返回报告，无真实网络。"""

    params = dict(
        securities=["000001.SZ"],
        start="2026-09-04",
        end="2026-09-07",
        reference_date="2026-09-07",
        frequencies=["1d"],
        modes=["none"],
    )
    params.update(kwargs)
    return capture_adjustment_inputs(request, **params)


def test_capture_only_reads_three_routes_and_explicit_modes():
    """验证路由白名单和确定性参数；无输入，无返回，不产生网络/交易/订阅。"""

    request, calls = _request_recorder()
    report = _capture(request, modes=MODES, frequencies=["1m", "1d"])
    assert {path for path, _ in calls} == {"/health", "/data/history", "/data/split_dividend"}
    history = [payload for path, payload in calls if path == "/data/history"]
    assert len(history) == 11
    assert all(item["subscribe"] is False and item["fill_data"] is False for item in history)
    assert all(item["fields"] == list(FIELDS) for item in history)
    assert history[0]["start"] == "2026-08-21"
    assert {item["fq"] for item in history} == set(MODES)
    assert report["capture_ok"] is True
    assert report["adjustment_accepted"] is False
    assert "do-not-save" not in str(report)
    event = report["cases"][0]
    assert event["event_fields_complete"] is False
    assert event["history_completeness_verified"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"securities": []},
        {"securities": ["bad"]},
        {"securities": ["000001.SZ"] * 2},
        {"start": "2026-09-08"},
        {"start": "2026-09-04T00:00:00+08:00"},
        {"reference_date": "2026-09-06"},
        {"frequencies": ["tick"]},
        {"frequencies": ["1d", "1d"]},
        {"frequencies": []},
        {"modes": ["follow"]},
        {"modes": []},
        {"modes": ["none", "none"]},
    ],
)
def test_invalid_scope_fails_before_network(change):
    """输入非法范围，断言采集前失败且调用列表为空；无返回或外部副作用。"""

    request, calls = _request_recorder()
    with pytest.raises(ValueError):
        _capture(request, **change)
    assert calls == []


@pytest.mark.parametrize(
    "health", [{}, {"ready": False}, {"ready": True, "qmt_api_ready": False}, []]
)
def test_unready_health_does_not_request_history(health):
    """输入未就绪健康结果，只允许一次health读取；无返回，不请求行情。"""

    calls = []

    def request(path, payload):
        """返回参数化健康数据并记录路径；输入路径/参数，无网络副作用。"""

        calls.append(path)
        return health

    with pytest.raises(ValueError):
        _capture(request)
    assert calls == ["/health"]


def test_exception_is_recorded_without_secret_or_retry():
    """确认异常只保存类型而非正文，且不重试；无输入/返回，不访问真实helper。"""

    good, calls = _request_recorder()

    def request(path, payload):
        """对事件路径抛含敏感文本的假异常；输入路径/参数，其他返回假数据。"""

        if path == "/data/split_dividend":
            calls.append((path, payload))
            raise RuntimeError("secret=must-not-leak")
        return good(path, payload)

    report = _capture(request)
    assert report["capture_ok"] is False
    assert "must-not-leak" not in str(report)
    assert sum(path == "/data/split_dividend" for path, _ in calls) == 1
    assert len(report["errors"]) == 1


def test_capture_keeps_helper_build_and_complete_event_fields():
    """核对新版helper元数据和七字段完整性；无输入/返回，不访问网络或记录账户。"""

    good, _ = _request_recorder()

    def request(path, payload):
        """给假健康/事件增加版本及完整事实；输入路径参数，返回字典，无外部副作用。"""
        response = good(path, payload)
        if path == "/health":
            response.update(
                gateway_build_id="20260908_dividend_facts_v1",
                dividend_event_schema="big-qmt-dividend-events/v1",
            )
        if path == "/data/split_dividend":
            response["events"][0].update(
                cash_per_share=0.1,
                gift=0,
                transfer=0,
                rights=0,
                rights_price=0,
                share_reform=False,
                qmt_dr=1.01,
            )
        return response

    report = _capture(request)
    for stage in ("health_before", "health_after"):
        assert report[stage]["gateway_build_id"] == "20260908_dividend_facts_v1"
        assert report[stage]["dividend_event_schema"] == "big-qmt-dividend-events/v1"
        assert "account_id" not in report[stage]
    assert report["cases"][0]["event_fields_complete"] is True
    assert report["cases"][0]["history_completeness_verified"] is False
    assert report["adjustment_accepted"] is False


def test_capture_preserves_invalid_event_error_code_without_message():
    """核对坏事件错误码不会被吞掉且正文仍脱敏；无输入/返回，不访问真实helper。"""

    good, _ = _request_recorder()

    def request(path, payload):
        """对事件返回约定错误码；输入路径参数，返回假结果或异常，无外部副作用。"""
        if path == "/data/split_dividend":
            error = RuntimeError("PRIVATE_TEXT_MUST_NOT_LEAK")
            error.code = "SPLIT_DIVIDEND_INVALID_DATA"
            raise error
        return good(path, payload)

    report = _capture(request)
    assert report["capture_ok"] is False
    assert report["errors"][0]["code"] == "SPLIT_DIVIDEND_INVALID_DATA"
    assert "PRIVATE_TEXT_MUST_NOT_LEAK" not in str(report)


@pytest.mark.parametrize("bad_value", [None, float("nan"), float("inf"), -1, 0])
def test_invalid_price_preserves_response_and_fails_capture(bad_value):
    """输入缺失/非法价，断言保留响应并标采集失败；无返回，无网络。"""

    good, _ = _request_recorder()

    def request(path, payload):
        """返回含参数化错误价格的假行情或正常假响应；无外部副作用。"""

        response = good(path, payload)
        if path == "/data/history":
            response["records"][0][1] = bad_value
        return response

    report = _capture(request)
    assert report["capture_ok"] is False
    assert len(report["errors"]) == 2
    assert "response" in report["cases"][1]


def test_end_health_error_does_not_discard_data():
    """结束健康请求失败仍返回已采行情；无输入/返回，无网络。"""

    good, _ = _request_recorder()
    count = 0

    def request(path, payload):
        """在第二次health请求抛异常；输入路径/参数，返回假数据或受控异常。"""

        nonlocal count
        if path == "/health":
            count += 1
            if count == 2:
                raise RuntimeError("private")
        return good(path, payload)

    report = _capture(request)
    assert report["capture_ok"] is False
    assert len(report["cases"]) == 3
    assert report["errors"][0]["kind"] == "health_after"


def test_summary_does_not_claim_price_acceptance():
    """摘要必须区分采集成功与算法验收；无输入/返回，不修改报告。"""

    request, _ = _request_recorder()
    report = _capture(request)
    original = deepcopy(report)
    summary = summarize_capture(report)
    assert summary["capture_ok"] is True
    assert summary["adjustment_accepted"] is False
    assert summary["cases"][1]["rows"] == 1
    assert report == original


def test_cli_never_overwrites_existing_file(tmp_path):
    """输入已存在路径，命令行在读取认证和联网前退出；不覆盖临时数据。"""

    output = tmp_path / "capture.json"
    output.touch()
    with pytest.raises(SystemExit) as raised:
        main(
            [
                "--security",
                "000001.SZ",
                "--start",
                "2026-09-04",
                "--end",
                "2026-09-07",
                "--reference-date",
                "2026-09-07",
                "--output",
                str(output),
            ]
        )
    assert raised.value.code == 2
    assert output.read_bytes() == b""


@pytest.mark.parametrize("kind", ["missing_time", "duplicate_time", "unsorted", "duplicate_column"])
def test_capture_rejects_invalid_time_axis(kind):
    """输入缺失/重复/乱序时间或字段，断言采集不伪成功；无返回，无网络。"""

    good, _ = _request_recorder()

    def request(path, payload):
        """返回有指定结构错误的假行情；输入路径/参数，返回字典，无外部副作用。"""

        value = good(path, payload)
        if path != "/data/history":
            return value
        if kind == "missing_time":
            value["columns"][0] = "unknown"
        elif kind == "duplicate_column":
            value["columns"][-1] = "close"
        else:
            value["records"].append(value["records"][0][:])
            if kind == "unsorted":
                value["records"][-1][0] = "2026-09-04"
        return value

    assert _capture(request)["capture_ok"] is False


@pytest.mark.parametrize("failure", ["nan", "inf", "health_secret"])
def test_cli_serializes_bad_data_and_redacts_initial_failure(
    tmp_path, monkeypatch, capsys, failure
):
    """注入非法浮点或含密钥的初始异常，断言CLI留下完整脱敏JSON而非半份文件。"""

    from bullet_trade.server.adapters.big_qmt import BigQmtGatewayClient

    good, _ = _request_recorder()

    def fake_request(self, path, payload, method, timeout_seconds=None):
        """替代HTTP函数；输入客户端请求，返回假数据或含私密正文/错误码的异常，不联网。"""

        if failure == "health_secret":
            error = RuntimeError("secret=must-not-leak")
            error.code = "PASSWORD_MUST_NOT_LEAK"
            raise error
        value = good(path, payload)
        if path == "/data/history":
            value["records"][0][1] = float(failure)
        return value

    monkeypatch.setattr(BigQmtGatewayClient, "request_json", fake_request)
    output = tmp_path / "result.json"
    code = main(
        [
            "--security",
            "000001.SZ",
            "--start",
            "2026-09-04",
            "--end",
            "2026-09-07",
            "--reference-date",
            "2026-09-07",
            "--mode",
            "none",
            "--output",
            str(output),
        ]
    )
    text = output.read_text(encoding="utf-8")
    report = json.loads(text)
    assert code == 2 and report["capture_ok"] is False
    assert "must-not-leak" not in text
    assert "PASSWORD_MUST_NOT_LEAK" not in text
    assert "must-not-leak" not in capsys.readouterr().out
    if failure != "health_secret":
        assert report["cases"][1]["response"]["records"][0][1] == {"invalid_number": failure}


@pytest.mark.parametrize("field", ["events", "records"])
def test_summary_keeps_malformed_response_error(field):
    """输入已记错的空结构，摘要仍可生成并保留错误；无返回，不访问网络。"""

    good, _ = _request_recorder()

    def request(path, payload):
        """将指定响应字段改成None；输入路径/参数，返回假响应，无外部副作用。"""

        response = good(path, payload)
        if field in response:
            response[field] = None
        return response

    report = _capture(request)
    summary = summarize_capture(report)
    assert summary["capture_ok"] is False
    assert summary["errors"]
    json.dumps(summary, allow_nan=False)
