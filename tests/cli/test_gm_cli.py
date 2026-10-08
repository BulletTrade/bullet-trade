"""
作者: BruceLee
文件职责: 验证掘金环境诊断的 SDK 加载、秘密输出、进程超时和现有注册边界。
主要输入: 模拟的运行平台、安装元信息和 SDK 导入进程结果。
主要输出: 离线回归断言，不需要掘金 SDK 或终端。
上下游关系: pytest 调用主 CLI 和 gm environment；不执行真实业务 API。
关键配置: 全部 SDK 行为为测试替身，禁止隐式网络与厂商运行时动作。
"""

import builtins
import json
import socket
import subprocess
from types import SimpleNamespace

import pytest

from bullet_trade.cli.main import create_parser, main
from bullet_trade.integrations.gm import environment


@pytest.fixture
def windows_sdk(monkeypatch):
    """模拟 Windows x64 和可用的 SDK 安装元信息。

    参数:
        monkeypatch: pytest 属性替换夹具。
    返回:
        None；仅替换当前测试中的元信息，不导入真实 SDK。
    """

    monkeypatch.setattr(environment.platform, "system", lambda: "Windows")
    monkeypatch.setattr(environment.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(environment.struct, "calcsize", lambda _: 8)
    monkeypatch.setattr(environment.sys, "version_info", (3, 11, 13))
    monkeypatch.setattr(environment.metadata, "version", lambda _: "3.0.187")


def _forbid_action(*args, **kwargs):
    """遇到隐式网络、SDK 或子进程动作时让测试失败。

    参数:
        args: 被拦截调用的位置参数。
        kwargs: 被拦截调用的关键字参数。
    返回:
        不返回，始终抛出 AssertionError。
    """

    raise AssertionError("环境元信息检查不应执行外部动作")


def test_parser_and_default_doctor_do_not_import_sdk_or_start_process(monkeypatch, windows_sdk):
    """验证 CLI 解析和默认诊断不导入 SDK、联网或启动进程。

    参数:
        monkeypatch: 属性替换夹具。
        windows_sdk: Windows 与 SDK 元信息替身。
    返回:
        None，违反动作边界时断言失败。
    """

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        """拦截厂商 SDK 导入，其他导入交回 Python。

        参数:
            name: 模块名；args 和 kwargs 为标准导入参数。
        返回:
            正常模块；尝试导入 gm 时抛出 AssertionError。
        """

        if name == "gm" or name.startswith("gm."):
            return _forbid_action()
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(socket, "create_connection", _forbid_action)
    monkeypatch.setattr(subprocess, "run", _forbid_action)
    args = create_parser().parse_args(["gm", "doctor"])
    result = environment.doctor()
    assert args.load_sdk is False
    assert result["environment_available"] is True
    assert result["sdk_import"]["status"] == "not_requested"
    assert result["terminal_connection"] == result["account_connection"] == "not_checked"


def test_doctor_never_prints_credentials_or_claims_business_ready(monkeypatch, windows_sdk, capsys):
    """验证私密配置只显示存在性，SDK 安装不被报告为交易接通。

    参数:
        monkeypatch: 属性和环境替换夹具。
        windows_sdk: SDK 元信息替身。
        capsys: CLI 输出捕获夹具。
    返回:
        None，凭据泄漏或业务状态错误时断言失败。
    """

    private_values = {
        "GM_TOKEN": "private-token-for-test",
        "GM_STRATEGY_ID": "private-strategy-for-test",
        "GM_ACCOUNT_ID": "private-account-for-test",
        "GM_SERV_ADDR": "private-terminal-for-test",
    }
    for key, value in private_values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("sys.argv", ["bullet-trade", "gm", "doctor"])
    assert main() == 0
    output = capsys.readouterr().out
    result = json.loads(output)
    assert all(result["configuration_present"].values())
    assert all(value not in output for value in private_values.values())
    assert result["terminal_connection"] == "not_checked"
    assert result["account_connection"] == "not_checked"
    assert result["broker_implemented"] is True
    assert result["data_provider_implemented"] is True
    assert result["server_adapter_implemented"] is False


@pytest.mark.parametrize(
    "system,machine,bits,python_version,expected",
    [
        ("Darwin", "arm64", 64, (3, 11), False),
        ("Linux", "aarch64", 64, (3, 11), False),
        ("Windows", "AMD64", 64, (3, 11), True),
        ("Windows", "x86", 32, (3, 12), False),
        ("Linux", "x86_64", 64, (3, 11), True),
    ],
)
def test_platform_matrix_and_unavailable_sdk_never_start_import(
    monkeypatch, windows_sdk, system, machine, bits, python_version, expected
):
    """验证平台安装包边界及不可用平台不运行导入探针。

    参数:
        monkeypatch/windows_sdk: 环境替换夹具。
        system/machine/bits/python_version: 模拟运行平台。
        expected: 当前平台应否提供 SDK 环境。
    返回:
        None，平台误判或隐式运行时失败。
    """

    monkeypatch.setattr(environment.platform, "system", lambda: system)
    monkeypatch.setattr(environment.platform, "machine", lambda: machine)
    monkeypatch.setattr(environment.struct, "calcsize", lambda _: bits // 8)
    monkeypatch.setattr(environment.sys, "version_info", python_version)
    assert environment.doctor()["environment_available"] is expected
    if not expected:
        monkeypatch.setattr(environment, "_check_sdk_import", _forbid_action)
        assert environment.doctor(load_sdk=True)["sdk_import"]["status"] == "unavailable"


@pytest.mark.parametrize("sdk_version", [None, "3.0.160", "3.1.0", "invalid"])
def test_missing_or_incompatible_sdk_is_not_available(monkeypatch, windows_sdk, sdk_version):
    """验证缺包和未支持版本不会被当成可用环境。

    参数:
        monkeypatch/windows_sdk: 元信息替换夹具。
        sdk_version: 模拟的 SDK 版本，None 表示未安装。
    返回:
        None，SDK 误判可用时失败。
    """

    def installed_version(name):
        """返回指定 SDK 元信息或抛出包缺失异常。

        参数:
            name: 查询的发行包名称。
        返回:
            模拟版本；没有 SDK 时抛出 PackageNotFoundError。
        """

        if sdk_version is None:
            raise environment.metadata.PackageNotFoundError(name)
        return sdk_version

    monkeypatch.setattr(environment.metadata, "version", installed_version)
    monkeypatch.setattr(environment, "_check_sdk_import", _forbid_action)
    result = environment.doctor(load_sdk=True)
    assert result["environment_available"] is False
    assert result["sdk_import"]["status"] == "unavailable"


def test_observed_simulation_sdk_version_is_available(monkeypatch, windows_sdk):
    """保留 Windows 实机只读验证使用的 3.0.186，无需强制升级。

    参数:
        monkeypatch/windows_sdk: SDK 元信息和平台替身。
    返回:
        None；实机版本被错误拒绝时断言失败。
    """

    monkeypatch.setattr(environment.metadata, "version", lambda _: "3.0.186")
    assert environment.doctor()["environment_available"] is True


def test_explicit_import_discards_native_sdk_output(monkeypatch, windows_sdk):
    """验证子进程中 SDK 的原始日志和错误输出不出现在诊断中。

    参数:
        monkeypatch/windows_sdk: 进程和平台替换夹具。
    返回:
        None，原始日志泄漏时失败。
    """

    completed = SimpleNamespace(
        returncode=0,
        stdout='private-token-log\nBT_GM_PROBE={"status": "ok", "missing_api": []}\n',
        stderr="private-account-log",
    )
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: completed)
    result = environment.doctor(load_sdk=True)
    assert result["sdk_import"]["status"] == "ok"
    assert "private-" not in json.dumps(result)
    assert result["account_connection"] == "not_checked"


@pytest.mark.parametrize("native_exit", [False, True])
def test_import_probe_does_not_call_business_api(monkeypatch, windows_sdk, tmp_path, native_exit):
    """用独立测试 SDK 验证实际子进程只导入、不调用任何业务函数。

    参数:
        monkeypatch/windows_sdk: 环境与安装元信息替身。
        tmp_path: 创建独立测试 SDK 的临时目录。
        native_exit: 是否模拟 native SDK 在清理阶段直接结束进程。
    返回:
        None；任何业务 API 被调用都会使导入探针失败。
    """

    sdk = tmp_path / "gm"
    sdk.mkdir()
    (sdk / "__init__.py").write_text("", encoding="utf-8")
    (sdk / "api.py").write_text(
        "def forbidden(*args, **kwargs):\n"
        "    raise AssertionError('business API called')\n"
        "run = set_token = history = current = order_volume = order_cancel = forbidden\n",
        encoding="utf-8",
    )
    if native_exit:
        with (sdk / "api.py").open("a", encoding="utf-8") as stream:
            stream.write("import atexit, os\natexit.register(lambda: os._exit(0))\n")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    assert environment.doctor(load_sdk=True)["sdk_import"]["status"] == "ok"


def test_explicit_import_timeout_is_bounded(monkeypatch, windows_sdk):
    """验证 SDK 卡住时诊断返回超时，且没有业务就绪声明。

    参数:
        monkeypatch/windows_sdk: 进程与平台替换夹具。
    返回:
        None，超时未正确处理时失败。
    """

    def timeout_process(command, **kwargs):
        """模拟 SDK 导入超时。

        参数:
            command: 子进程参数。
            kwargs: 含 timeout 的启动参数。
        返回:
            不返回，抛出 TimeoutExpired。
        """

        assert kwargs["timeout"] == 0.5
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", timeout_process)
    assert environment.doctor(load_sdk=True, timeout=0.5)["sdk_import"]["status"] == "timeout"


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_cannot_start_sdk(monkeypatch, windows_sdk, timeout):
    """验证无效超时在执行探针前失败。

    参数:
        monkeypatch/windows_sdk: 进程与平台替换夹具。
        timeout: 非法超时值。
    返回:
        None，无效超时未被拒绝时失败。
    """

    monkeypatch.setattr(subprocess, "run", _forbid_action)
    with pytest.raises(ValueError):
        environment.doctor(load_sdk=True, timeout=timeout)


def test_gm_registers_local_broker_only():
    """验证本地 gm Broker 已注册，远程 GM adapter 尚未注册。

    参数:
        无。
    返回:
        None，注册范围不符合当前交付状态时断言失败。
    """

    from bullet_trade.broker.registry import list_brokers
    from bullet_trade.server.adapters import list_adapters

    assert "gm" in list_brokers()
    assert "gm" not in list_adapters()
