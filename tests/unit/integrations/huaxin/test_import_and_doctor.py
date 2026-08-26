"""
作者: BruceLee
文件职责: 验证华鑫集成普通 import 无 native 副作用，并检查离线 doctor 语义。
主要输入: 隔离 Python 子进程、可选 fake bundle 与 CLI 参数。
主要输出: pytest 断言，保护 no-SDK import 和 readiness 分层。
上游关系: BulletTrade 包导入、华鑫模块 CLI 与 doctor 公共 API。
下游关系: 不访问厂商 SDK；仅对共享 fake bundle 做只读完整性校验和可选 dlopen。
关键环境或配置: 测试不读取环境凭据、不联网、不创建真实 runtime 或交易连接。
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bullet_trade.integrations.huaxin import BuildResult, build_native_bridge, doctor
from bullet_trade.integrations.huaxin.cli import main
from bullet_trade.integrations.huaxin.errors import (
    BRIDGE_BUNDLE_MISSING,
    BUILD_PREFIX_UNSAFE,
    OFFLINE_FAKE_ONLY,
    HuaxinBuildError,
)


@pytest.mark.unit
def test_import_does_not_build_or_dlopen() -> None:
    """
    验证导入华鑫模块不会隐式调用编译器或 ctypes.CDLL。

    参数:
        无。
    返回:
        无；隔离子进程成功退出即通过。
    副作用:
        启动一个本地 Python 子进程，不访问网络或 native bridge。
    """

    script = """
import ctypes
import subprocess
import bullet_trade

def forbidden(*args, **kwargs):
    '''拒绝普通 import 期间发生 native 或构建调用。'''
    raise AssertionError("普通 import 不得调用 native 或构建进程")

ctypes.CDLL = forbidden
subprocess.run = forbidden
import bullet_trade.integrations.huaxin
print("import-ok")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip().endswith("import-ok")


@pytest.mark.unit
def test_doctor_without_bundle_fails_closed() -> None:
    """
    验证未指定 bundle 时 doctor 不会误报 native 或离线 bridge 就绪。

    参数:
        无。
    返回:
        无；readiness 与稳定原因码符合预期即通过。
    """

    report = doctor()

    assert report.native_ready is False
    assert report.offline_bridge_ready is False
    assert report.bridge_loadable is None
    assert report.reason_code == BRIDGE_BUNDLE_MISSING


@pytest.mark.unit
def test_build_rejects_current_source_checkout() -> None:
    """验证显式构建不能把任何产物写入当前源码仓。

    参数:
        无。
    返回:
        无；在工具链检查和任何目录创建前抛出稳定错误即通过。
    副作用:
        无；被测函数必须在写文件前拒绝。
    """

    project_root = Path(__file__).resolve().parents[4]
    with pytest.raises(HuaxinBuildError) as exc_info:
        build_native_bridge(prefix=project_root / "forbidden-huaxin-build")

    assert exc_info.value.code == BUILD_PREFIX_UNSAFE
    assert exc_info.value.details == {"path_class": "source_checkout"}


@pytest.mark.unit
def test_doctor_loads_only_verified_offline_bundle(offline_bundle: BuildResult) -> None:
    """
    验证显式 load 可加载 fake bridge，但不会把它冒充生产 native readiness。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
    返回:
        无；离线和生产 readiness 正确分离即通过。
    副作用:
        完整性校验后显式 dlopen 自研动态库，不创建 runtime。
    """

    report = doctor(bundle_path=offline_bundle.bundle_path, load=True)

    assert report.native_ready is False
    assert report.offline_bridge_ready is True
    assert report.bridge_loadable is True
    assert report.reason_code == OFFLINE_FAKE_ONLY
    assert report.bundle_fingerprint == offline_bundle.fingerprint


@pytest.mark.unit
def test_cli_doctor_outputs_json_and_nonzero_without_bundle(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    验证 CLI doctor 缺少 bundle 时输出结构化 JSON 并返回非零退出码。

    参数:
        capsys: pytest 标准输出捕获器。
    返回:
        无；JSON 和退出码符合 fail-closed 约定即通过。
    副作用:
        调用纯 Python CLI 入口并捕获标准输出。
    """

    exit_code = main(["doctor"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["native_ready"] is False
    assert payload["offline_bridge_ready"] is False
    assert payload["reason_code"] == BRIDGE_BUNDLE_MISSING


@pytest.mark.unit
def test_cli_doctor_accepts_verified_bundle(
    offline_bundle: BuildResult,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    验证 CLI doctor 对有效 fake bundle 返回离线成功而非生产就绪。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
        capsys: pytest 标准输出捕获器。
    返回:
        无；CLI readiness 分层和退出码符合预期即通过。
    副作用:
        显式 dlopen 自研 fake 动态库，不创建 runtime。
    """

    exit_code = main(["doctor", "--bundle", str(offline_bundle.bundle_path), "--load"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["native_ready"] is False
    assert payload["offline_bridge_ready"] is True
    assert payload["bridge_loadable"] is True
