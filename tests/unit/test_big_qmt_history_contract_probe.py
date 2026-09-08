"""大 QMT 公开历史采集脚本的离线边界测试。

作者：BruceLee
职责：验证固定矩阵、公开 API 到真实 Provider 的路由、错误脱敏和独占输出。
输入：内存 DataFrame、临时配置和假 TCP 连接；不读取账户配置，不连接任何服务器。
输出：pytest 断言，不把合成数据称作实测行情或算法验收。
上下游：真实 data.api / RemoteQmtProvider / wire 编解码，只有底层连接替换为白名单桩。
环境：PYTHONPATH=. pytest tests/unit/test_big_qmt_history_contract_probe.py；只写 pytest 临时目录。
"""

from __future__ import annotations

import json
import logging
import os
from argparse import Namespace
from datetime import date

import numpy as np
import pandas as pd
import pytest

from bullet_trade.core.settings import get_settings
from bullet_trade.data import api
from bullet_trade.data.providers import remote_qmt
from bullet_trade.server.adapters.qmt import dataframe_to_payload
from scripts import probe_big_qmt_history_contract as probe

pytestmark = pytest.mark.unit


def _frame(case):
    """构造非真实行情支架；输入案例，返回一行有限数字表，只验证采集契约，不验算法。"""
    fields = case["request"]["fields"]
    return pd.DataFrame(
        [[1.0] * len(fields)], columns=fields, index=pd.DatetimeIndex(["2026-06-12"], name="time")
    )


class _Connection:
    """只读连接桩；记录调用和关闭状态，仅允许 data.history，不接网络或执行交易。"""

    instances = []
    failure = False

    def __init__(self, *args, **kwargs):
        """接收但不保存敏感构造参数；输入兼容参数，创建请求和关闭状态，无网络副作用。"""
        self.calls = []
        self.closed = False
        self.__class__.instances.append(self)

    def add_event_listener(self, event, callback):
        """验证本地事件注册；输入事件和回调，返回 None，不发送订阅。"""
        assert event == "tick"

    def start(self):
        """模拟客户端启动；无输入输出，不启动线程或远程服务。"""

    def request(self, action, payload):
        """记录唯一允许的只读 action；输入 action/payload，返回合成 wire 或含秘密的测试异常。"""
        assert action == "data.history"
        self.calls.append((action, payload))
        if self.failure:
            raise RuntimeError("NEVER_PRINT_THIS_TOKEN_OR_REMOTE_MESSAGE")
        frame = _frame({"request": payload})
        return dataframe_to_payload(frame)

    def close(self):
        """只标记自身连接关闭；无输入输出，不发 unsubscribe 或任何服务管理 action。"""
        self.closed = True


@pytest.fixture
def isolated_client(monkeypatch):
    """隔离全局 API 状态和真实 Provider 的底层连接；返回连接桩类，结束由 monkeypatch 恢复。"""
    for name in ("HOST", "PORT", "TOKEN", "TLS_CERT"):
        monkeypatch.delenv("QMT_SERVER_" + name, raising=False)
    monkeypatch.setenv("QMT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("QMT_SERVER_PORT", "58620")
    monkeypatch.setenv("QMT_SERVER_TOKEN", "NEVER_PRINT_THIS_TOKEN_OR_REMOTE_MESSAGE")
    monkeypatch.setattr(api, "_provider", None)
    monkeypatch.setattr(api, "_current_context", None)
    monkeypatch.setattr(api, "_provider_cache", {})
    monkeypatch.setattr(api, "_provider_auth_attempted", {})
    monkeypatch.setattr(api, "_auth_attempted", False)
    monkeypatch.setattr(get_settings(), "options", dict(get_settings().options))
    monkeypatch.setattr(_Connection, "instances", [])
    monkeypatch.setattr(_Connection, "failure", False)
    monkeypatch.setattr(remote_qmt, "RemoteQmtConnection", _Connection)
    return _Connection


def test_fixed_matrix_and_reference_windows():
    """验证固定矩阵；无输入，断言 48 主请求和 6 重叠请求及周期字段边界，无副作用。"""
    cases = probe.build_cases()
    assert len(cases) == 54
    assert len({case["id"] for case in cases}) == 54
    for case in cases:
        request = case["request"]
        assert request["fq"] in {"none", "pre", "post"}
        assert not (request["start_date"] and request["count"])
        assert request["panel"] is True
        if request["frequency"] in ("1w", "1mon"):
            assert request["count"] == 1
            assert request["end_date"] == "2026-05-03 15:00:00"
        if request["frequency"] not in ("1d", "1m"):
            assert request["fields"] == probe._FIELDS


@pytest.mark.parametrize(
    "security,start,end",
    [
        ("000001.XSHE", "2026-06-11", "2026-06-12"),
        ("510500.XSHG", "2026-07-14", "2026-07-15"),
    ],
)
@pytest.mark.parametrize("fq", ["none", "pre", "post"])
def test_daily_window_preserves_two_midnight_bars(security, start, end, fq):
    """回归日线起点误用 09:30；输入证券/日期/模式，断言两日日线保留且分钟仍取日内窗口。"""
    cases = {case["id"]: case["request"] for case in probe.build_cases()}
    daily = cases[f"{security}-{fq}-1d"]
    assert (daily["start_date"], daily["end_date"]) == (start, end)
    midnight_bars = pd.to_datetime([start, end])
    selected = midnight_bars[
        (midnight_bars >= pd.Timestamp(daily["start_date"]))
        & (midnight_bars <= pd.Timestamp(daily["end_date"]))
    ]
    assert selected.equals(midnight_bars)
    for frequency in ("1m", "5m", "15m", "30m", "60m"):
        minute = cases[f"{security}-{fq}-{frequency}"]
        assert (minute["start_date"], minute["end_date"]) == (
            start + " 09:30:00",
            end + " 15:00:00",
        )
    overlap = cases[f"{security}-{fq}-1d-overlap"]
    assert (overlap["start_date"], overlap["end_date"], overlap["count"]) == (None, end, 1)


def test_real_public_api_provider_route_and_safe_output(tmp_path, isolated_client, capsys):
    """走真实公开 API 和 Provider；输入临时目录及底层桩，验证白名单、基准和完整输出，不联网。"""
    output = tmp_path / "capture.json"
    assert probe.main(["--output", str(output)]) == 0
    report = json.loads(output.read_text())
    assert len(report["cases"]) == 54
    assert all(case["status"] == "captured" for case in report["cases"])
    assert report["reference_intent"]["pre_factor_ref_date"] == "2026-09-07"
    assert report["acceptance"].startswith("capture_only")
    assert isinstance(api.get_data_provider(), remote_qmt.RemoteQmtProvider)
    connection = isolated_client.instances[0]
    assert connection.closed
    assert len(connection.calls) == 54
    for action, payload in connection.calls:
        assert action == "data.history"
        assert payload["pre_factor_ref_date"] == (
            date(2026, 9, 7) if payload["fq"] == "pre" else None
        )
        if payload["frequency"] == "1d":
            start, end = (
                ("2026-06-11", "2026-06-12")
                if payload["security"] == "000001.XSHE"
                else ("2026-07-14", "2026-07-15")
            )
            assert payload["start"] == (None if payload["count"] else start)
            assert payload["end"] == end
    result = report["cases"][0]["result"]
    assert result["index_type"] == "DatetimeIndex"
    assert result["records"][0][0] == "2026-06-12T00:00:00"
    assert report["source"]["files"][1]["path"] == api.__file__
    assert all(len(item["sha256"]) == 64 for item in report["source"]["files"])
    printed = capsys.readouterr()
    assert "NEVER_PRINT" not in output.read_text() + printed.out + printed.err
    progress = [json.loads(line) for line in printed.out.splitlines()]
    assert len(progress) == 55
    assert all(set(line) == {"id", "status", "rows"} for line in progress[:-1])
    assert [line["id"] for line in progress[:-1]] == [case["id"] for case in report["cases"]]
    assert all(line["status"] == "captured" and line["rows"] == 1 for line in progress[:-1])
    assert progress[-1]["acceptance"] == "capture_only"


def test_public_api_swallowed_error_is_not_success(tmp_path, isolated_client, monkeypatch, capsys):
    """验证公开 API 吞异常后的空表；输入连接故障，返回失败退出码且不暴露异常正文。"""
    monkeypatch.setattr(isolated_client, "failure", True)
    output = tmp_path / "failed.json"
    assert probe.main(["--output", str(output)]) == 1
    report = json.loads(output.read_text())
    assert all(case["status"] == "empty" for case in report["cases"])
    assert isolated_client.instances[0].closed
    printed = capsys.readouterr()
    assert "NEVER_PRINT" not in output.read_text() + printed.out + printed.err


def test_exclusive_output_never_connects(tmp_path, isolated_client):
    """验证已有输出不覆盖；输入已存在路径，返回启动失败且不创建客户端。"""
    output = tmp_path / "exists.json"
    output.write_text("保留原文件", encoding="utf-8")
    assert probe.main(["--output", str(output)]) == 2
    assert output.read_text() == "保留原文件"
    assert isolated_client.instances == []


@pytest.mark.parametrize("defect", ["empty", "duplicate", "unordered", "nan", "inf", "columns"])
def test_invalid_frames_are_not_captured(defect):
    """验证不完整/不可信结构；输入异常类型，断言不算 captured 且输出仍为严格 JSON。"""
    case = probe.build_cases()[0]
    frame = _frame(case)
    if defect == "empty":
        frame = frame.iloc[:0]
    elif defect in ("duplicate", "unordered"):
        frame = pd.concat([frame, frame])
        if defect == "unordered":
            frame.index = pd.to_datetime(["2026-06-12", "2026-06-11"])
    elif defect in ("nan", "inf"):
        frame.iloc[0, 0] = np.nan if defect == "nan" else np.inf
    else:
        frame = frame.drop(columns="close")

    def get_price(**kwargs):
        """返回测试结构；输入公开 API 参数，返回当前测试表，无副作用。"""
        return frame

    result = probe.capture_case(case, get_price, dataframe_to_payload)
    assert result["status"] == ("empty" if defect == "empty" else "invalid")
    assert result["issues"]
    json.dumps(result, allow_nan=False)


def test_exception_summary_whitelists_codes():
    """验证异常摘要；输入含秘密异常正文与未知代码，只保留类型及白名单代码。"""
    exc = RuntimeError("NEVER_PRINT")
    exc.code = "HISTORY_FAILED"
    assert probe._safe_error(exc) == {"type": "RuntimeError", "code": "HISTORY_FAILED"}
    exc.code = "NEVER_PRINT"
    assert probe._safe_error(exc)["code"] == "UNCLASSIFIED"
    exc.code = {"NEVER_PRINT": "NEVER_PRINT"}
    assert probe._safe_error(exc)["code"] == "UNCLASSIFIED"


def test_capture_propagated_error_and_unserializable_result():
    """验证异常和不可编码返回值；无输入，断言保留安全错误且坏结果不破坏整个报告。"""
    case = probe.build_cases()[0]

    def failed(**kwargs):
        """模拟未被 API 吞掉的错误；输入任意参数，抛出含敏感正文异常，无副作用。"""
        raise NotImplementedError("NEVER_PRINT")

    result = probe.capture_case(case, failed, dataframe_to_payload)
    assert result["status"] == "error"
    assert result["error"]["type"] == "NotImplementedError"

    def get_price(**kwargs):
        """返回数值支架；输入公开参数，返回单行表，无副作用。"""
        return _frame(case)

    def invalid_encoder(frame):
        """模拟不可序列化载荷；输入表，返回带裸对象字典，无副作用。"""
        return {"records": [object()]}

    result = probe.capture_case(case, get_price, invalid_encoder)
    assert result["status"] == "error"
    assert "result" not in result
    assert "NEVER_PRINT" not in json.dumps(result, allow_nan=False)


def test_env_file_whitelist_and_precedence(tmp_path, monkeypatch):
    """验证配置输入；输入 env 文件和覆盖值，返回限定四键，不把其他配置注入进程。"""
    for name in ("HOST", "PORT", "TOKEN", "TLS_CERT"):
        monkeypatch.delenv("QMT_SERVER_" + name, raising=False)
    env = tmp_path / "probe.env"
    env.write_text(
        "QMT_SERVER_HOST=file-host\nQMT_SERVER_PORT=1234\n"
        "QMT_SERVER_TOKEN=file-token\nIGNORED_PROBE_SECRET=unused\n"
    )
    monkeypatch.setenv("QMT_SERVER_PORT", "2345")
    config = probe._connection_config(Namespace(env_file=str(env), host="cli-host", port=None))
    assert config == {"host": "cli-host", "port": 2345, "token": "file-token", "tls_cert": None}
    assert "IGNORED_PROBE_SECRET" not in os.environ
    with pytest.raises(ValueError):
        probe._connection_config(Namespace(env_file=str(env), host=None, port=0))


def test_quiet_logging_is_restored():
    """验证日志范围；无输入，断言临时静默覆盖并恢复本进程设置，不改变生产日志配置。"""
    logger = logging.getLogger("jq_strategy")
    old = (logger.disabled, logging.root.manager.disable)
    with probe._quiet_library():
        assert logger.disabled
        assert logging.root.manager.disable == logging.CRITICAL
    assert (logger.disabled, logging.root.manager.disable) == old
