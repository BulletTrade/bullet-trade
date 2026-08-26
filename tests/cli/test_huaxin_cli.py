"""验证华鑫主 CLI 的显式动作边界和离线命令分派。"""

import ctypes
import socket
import subprocess

import pytest

from bullet_trade.cli.main import create_parser, main


def test_main_parser_exposes_huaxin_doctor_without_native_actions(monkeypatch) -> None:
    """验证解析主 CLI 的华鑫 doctor 不会加载动态库、联网或启动编译器。"""

    def forbidden_action(*args, **kwargs):
        """在测试发现任何隐式 native、网络或子进程动作时立即失败。

        Args:
            *args: 被拦截调用的位置参数。
            **kwargs: 被拦截调用的关键字参数。

        Returns:
            本函数不会正常返回。

        Raises:
            AssertionError: 每次被调用都抛出。
        """

        raise AssertionError("CLI 参数解析触发了被禁止的隐式动作")

    monkeypatch.setattr(ctypes, "CDLL", forbidden_action)
    monkeypatch.setattr(socket, "create_connection", forbidden_action)
    monkeypatch.setattr(subprocess, "run", forbidden_action)

    arguments = create_parser().parse_args(["huaxin", "doctor"])

    assert arguments.command == "huaxin"
    assert arguments.huaxin_command == "doctor"
    assert arguments.bundle is None
    assert arguments.load is False


def test_main_huaxin_doctor_without_bundle_is_fail_closed(monkeypatch, capsys) -> None:
    """验证无 bundle 的全局 doctor 返回未就绪而不是隐式构建。"""

    monkeypatch.setattr(
        "sys.argv",
        ["bullet-trade", "huaxin", "doctor"],
    )

    result = main()
    captured = capsys.readouterr()

    assert result == 2
    assert '"native_ready": false' in captured.out
    assert '"reason_code": "BRIDGE_BUNDLE_MISSING"' in captured.out


@pytest.mark.parametrize("argument", ["--help", "--version"])
def test_main_metadata_with_huaxin_installed_does_not_load_native(
    monkeypatch,
    argument,
) -> None:
    """验证主 CLI 元命令导入华鑫解析器时仍不会 dlopen。

    Args:
        monkeypatch: pytest 提供的替换工具。
        argument: 本次验证的主 CLI 元参数。
    """

    def forbidden_cdll(*args, **kwargs):
        """在元命令尝试 dlopen 时立即失败。

        Args:
            *args: ctypes 调用的位置参数。
            **kwargs: ctypes 调用的关键字参数。

        Returns:
            本函数不会正常返回。

        Raises:
            AssertionError: 每次被调用都抛出。
        """

        raise AssertionError("CLI 元命令不应加载 native bridge")

    monkeypatch.setattr(ctypes, "CDLL", forbidden_cdll)
    parser = create_parser()

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args([argument])

    assert exc_info.value.code == 0
