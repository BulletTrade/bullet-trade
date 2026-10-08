"""
作者: BruceLee
文件职责: 验证掘金只读探针的账户选择、动作边界和秘密输出。
主要输入: SDK 替身、平台元信息、私密配置和子进程结果。
主要输出: 离线断言；不访问真实终端、账户或网络。
上下游关系: pytest 调用 probe、probe_worker 和 gm CLI。
关键配置: 未显式请求账户时禁止账户调用；任何交易 API 调用立即失败。
"""

import json
import subprocess
from types import SimpleNamespace

import pytest

from bullet_trade.integrations.gm import probe as probe_module
from bullet_trade.integrations.gm.cli import main
from bullet_trade.integrations.gm.probe_worker import execute


class FakeSdk:
    """记录读取动作的 SDK 替身，禁止所有未列出的函数。"""

    def __init__(self, empty=False):
        """初始化调用记录和是否返回空历史记录。"""
        self.calls = []
        self.empty = empty

    def __getattr__(self, name):
        """拦截 run、下单、撤单及其他未允许的 SDK 动作。"""
        raise AssertionError("forbidden SDK action: " + name)

    def set_token(self, token):
        """记录认证动作但不保存凭据。"""
        self.calls.append("token")

    def set_serv_addr(self, addr):
        """记录当前进程的服务地址配置。"""
        self.calls.append("address")

    def history(self, **kwargs):
        """返回带日期的历史数据替身。"""
        self.calls.append("history")
        return [] if self.empty else [{"eob": "2026-09-30 00:00:00+08:00"}]

    def current(self, **kwargs):
        """返回带来源时间的快照替身。"""
        self.calls.append("current")
        return [{"created_at": "2026-09-30 15:01:43+08:00"}]

    def get_cash(self, account_id):
        """验证显式账号传递，并在返回值中注入不能被透传的私密字段。"""
        assert account_id == "selected-account"
        self.calls.append("cash")
        return {"account_id": account_id, "account_name": "private-label", "available": 1}

    def get_position(self, account_id):
        """验证空持仓查询只对明确账号执行。"""
        assert account_id == "selected-account"
        self.calls.append("positions")
        return []


def request(account=False):
    """生成测试用私密输入，凭据不得出现在任何结果中。"""
    return {
        "symbol": "SHSE.510300",
        "start": "2026-09-28",
        "end": "2026-09-30",
        "token": "private-token",
        "serv_addr": "private-address",
        "account_id": "selected-account" if account else "",
    }


@pytest.mark.parametrize("account", [False, True])
def test_worker_calls_only_readonly_functions_and_explicit_account(account):
    """验证账户选择、查询动作和输出中不含私密数据。"""
    sdk = FakeSdk()
    result = execute(sdk, request(account), status_reader=lambda _: {"state": 3, "error_code": 0})
    assert sdk.calls == ["token", "address", "history", "current"] + (
        ["cash", "positions"] if account else []
    )
    assert result["ok"] is True
    assert result["orders"] == result["executions"] == "not_checked_session_required"
    assert result["account_type"] == result["trading"] == "not_checked"
    assert result["account_connection"] == ("logged_in" if account else "not_checked")
    if account:
        assert result["calls"]["cash"]["last_time"] is None
        assert result["calls"]["cash"]["channel_present"] is False
    output = json.dumps(result)
    assert not any(
        value in output
        for value in ["private-token", "private-address", "private-label", "selected-account"]
    )


@pytest.mark.parametrize("state", [0, 1, 2, 4, 5, 6])
def test_disconnected_or_not_logged_in_account_is_not_ready(state):
    """接口可返回初始化资金记录，但未登录账号不得被判为通过。"""
    result = execute(
        FakeSdk(), request(True), status_reader=lambda _: {"state": state, "error_code": 0}
    )
    assert result["calls"]["cash"]["record_present"] is True
    assert result["ok"] is False
    assert result["account_connection"] != "logged_in"


def test_empty_history_and_failed_query_are_not_connection_success():
    """验证空行情与 SDK 异常不会被报告为连接成功或泄漏详情。"""
    assert execute(FakeSdk(empty=True), request())["ok"] is False
    sdk = FakeSdk()

    def fail(**kwargs):
        """模拟包含私密值的 SDK 查询异常。"""
        raise RuntimeError("private-token selected-account")

    sdk.history = fail
    result = execute(sdk, request())
    assert result["ok"] is False
    assert result["calls"]["history"] == {"status": "error", "error_type": "RuntimeError"}
    assert "private-token" not in json.dumps(result)


def test_missing_account_config_never_starts_sdk(monkeypatch):
    """显式账户查询缺少账户 ID 时必须在启动 SDK 前拒绝。"""
    monkeypatch.setenv("GM_TOKEN", "private-token")
    monkeypatch.delenv("GM_ACCOUNT_ID", raising=False)

    def forbidden(*args, **kwargs):
        """阻止配置不全时发生任何子进程动作。"""
        raise AssertionError("unexpected process")

    monkeypatch.setattr(subprocess, "run", forbidden)
    result = probe_module.probe("SHSE.510300", "2026-09-28", "2026-09-30", account=True)
    assert result == {"ok": False, "status": "configuration_missing", "missing": ["GM_ACCOUNT_ID"]}


@pytest.mark.parametrize("timeout", [0, -1, 121, float("inf"), float("nan")])
def test_invalid_timeout_cannot_start_process(timeout):
    """验证查询进程必须受有限超时约束。"""
    with pytest.raises(ValueError):
        probe_module.probe("SHSE.510300", "2026-09-28", "2026-09-30", timeout=timeout)


def test_parent_keeps_secrets_off_command_line_and_hides_sdk_logs(monkeypatch, capsys):
    """验证父进程仅通过 stdin 传递凭据且不透传 SDK 日志。"""
    monkeypatch.setenv("GM_TOKEN", "private-token")
    monkeypatch.setenv("GM_ACCOUNT_ID", "selected-account")
    monkeypatch.setattr(probe_module, "doctor", lambda: {"environment_available": True})

    def process(command, **kwargs):
        """核对有界进程协议并模拟 SDK 原始日志。"""
        assert "private-token" not in " ".join(command)
        payload = json.loads(kwargs["input"])
        assert payload["token"] == "private-token"
        assert payload["account_id"] == ""  # 环境里有账号也不能隐式查询。
        assert kwargs["timeout"] == 30
        return SimpleNamespace(
            returncode=0,
            stdout='private-token\nBT_GM_READONLY={"ok": true}\n',
            stderr="selected-account",
        )

    monkeypatch.setattr(subprocess, "run", process)
    assert main(["probe", "--start", "2026-09-28", "--end", "2026-09-30"]) == 0
    output = capsys.readouterr().out
    assert "private-token" not in output and "selected-account" not in output


def test_sdk_timeout_is_reported_without_partial_success(monkeypatch):
    """SDK 卡住时整个探针必须结束且不报告连接通过。"""
    monkeypatch.setenv("GM_TOKEN", "private-token")
    monkeypatch.setattr(probe_module, "doctor", lambda: {"environment_available": True})

    def process(command, **kwargs):
        """模拟联网查询超时。"""
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", process)
    assert probe_module.probe("SHSE.510300", "2026-09-28", "2026-09-30") == {
        "ok": False,
        "status": "timeout",
    }
