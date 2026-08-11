"""
作者: BruceLee
文件职责: 验证华鑫 native bundle 的 manifest、artifact 和 dlopen 前完整性边界。
主要输入: 共享 fake bundle 的隔离副本与受控篡改字节。
主要输出: pytest 断言，确保内容变化稳定 fail-closed 且不会执行动态库。
上游关系: verify_bundle 与 NativeBridge.load 的安全前置检查。
下游关系: 仅操作 pytest 临时副本，不修改已发布 bundle 或厂商文件。
关键环境或配置: 不访问网络、凭据、SDK 或服务器；篡改只发生在测试临时目录。
"""

from __future__ import annotations

import ctypes
import json
import shutil
from pathlib import Path

import pytest

from bullet_trade.integrations.huaxin import BuildResult, NativeBridge, verify_bundle
from bullet_trade.integrations.huaxin.errors import (
    BRIDGE_ARTIFACT_HASH_MISMATCH,
    BRIDGE_BUNDLE_INVALID,
    BUILD_FINGERPRINT_MISMATCH,
    HuaxinBundleError,
)


def _copy_bundle(offline_bundle: BuildResult, tmp_path: Path) -> Path:
    """
    把共享 bundle 复制到保持指纹目录名的测试隔离位置。

    参数:
        offline_bundle: 会话级原始 fake bundle。
        tmp_path: 当前测试的独立临时目录。
    返回:
        可安全篡改的 bundle 副本路径。
    副作用:
        复制自研 fake bundle 到 pytest 临时目录。
    """

    copied = tmp_path / "isolated" / offline_bundle.fingerprint
    copied.parent.mkdir(parents=True)
    shutil.copytree(offline_bundle.bundle_path, copied)
    return copied


@pytest.mark.unit
def test_manifest_tamper_fails_closed(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证 manifest 任一受指纹保护字段变化都会被拒绝。

    参数:
        offline_bundle: 会话级原始 fake bundle。
        tmp_path: 当前测试隔离目录。
    返回:
        无；抛出 BUILD_FINGERPRINT_MISMATCH 即通过。
    副作用:
        仅改写临时副本的 manifest.json。
    """

    copied = _copy_bundle(offline_bundle, tmp_path)
    manifest_path = copied / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source"]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(HuaxinBundleError) as raised:
        verify_bundle(copied)

    assert raised.value.code == BUILD_FINGERPRINT_MISMATCH


@pytest.mark.unit
def test_artifact_tamper_fails_closed(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证动态库字节变化会触发稳定 artifact hash 错误。

    参数:
        offline_bundle: 会话级原始 fake bundle。
        tmp_path: 当前测试隔离目录。
    返回:
        无；抛出 BRIDGE_ARTIFACT_HASH_MISMATCH 即通过。
    副作用:
        仅向临时动态库副本追加测试字节。
    """

    copied = _copy_bundle(offline_bundle, tmp_path)
    manifest = json.loads((copied / "manifest.json").read_text(encoding="utf-8"))
    artifact = copied / manifest["bridge"]["artifact"]
    with artifact.open("ab") as stream:
        stream.write(b"test-tamper")

    with pytest.raises(HuaxinBundleError) as raised:
        verify_bundle(copied)

    assert raised.value.code == BRIDGE_ARTIFACT_HASH_MISMATCH


@pytest.mark.unit
def test_tampered_artifact_is_rejected_before_dlopen(
    offline_bundle: BuildResult,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 NativeBridge.load 在 ctypes.CDLL 前完成 artifact 校验。

    参数:
        offline_bundle: 会话级原始 fake bundle。
        tmp_path: 当前测试隔离目录。
        monkeypatch: pytest 提供的属性补丁工具。
    返回:
        无；篡改错误发生且 CDLL 未被调用即通过。
    副作用:
        修改临时副本，并在测试期间替换 ctypes.CDLL。
    """

    copied = _copy_bundle(offline_bundle, tmp_path)
    manifest = json.loads((copied / "manifest.json").read_text(encoding="utf-8"))
    artifact = copied / manifest["bridge"]["artifact"]
    with artifact.open("ab") as stream:
        stream.write(b"test-tamper-before-load")
    called = {"value": False}

    def forbidden_cdll(*args: object, **kwargs: object) -> None:
        """
        记录并拒绝任何不应发生的 ctypes.CDLL 调用。

        参数:
            args: ctypes.CDLL 的位置参数，仅用于捕获意外调用。
            kwargs: ctypes.CDLL 的关键字参数，仅用于捕获意外调用。
        返回:
            无；若被调用则立即使测试失败。
        副作用:
            把本地 called 标记改为 True。
        """

        del args, kwargs
        called["value"] = True
        raise AssertionError("篡改 bundle 不得进入 dlopen")

    monkeypatch.setattr(ctypes, "CDLL", forbidden_cdll)

    with pytest.raises(HuaxinBundleError) as raised:
        NativeBridge.load(copied)

    assert raised.value.code == BRIDGE_ARTIFACT_HASH_MISMATCH
    assert called["value"] is False


@pytest.mark.unit
def test_artifact_symlink_is_rejected_even_when_target_hash_matches(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证 artifact 即使指向 bundle 内同字节文件，也不能绕过普通文件约束。

    参数:
        offline_bundle: 会话级原始 fake bundle。
        tmp_path: 当前测试隔离目录。
    返回:
        无；符号链接触发 BRIDGE_BUNDLE_INVALID 即通过。
    副作用:
        在临时 bundle 副本内创建一个符号链接。
    """

    copied = _copy_bundle(offline_bundle, tmp_path)
    manifest = json.loads((copied / "manifest.json").read_text(encoding="utf-8"))
    artifact = copied / manifest["bridge"]["artifact"]
    target = artifact.with_name(f"verified-{artifact.name}")
    shutil.copy2(artifact, target)
    artifact.unlink()
    artifact.symlink_to(target.name)

    with pytest.raises(HuaxinBundleError) as raised:
        verify_bundle(copied)

    assert raised.value.code == BRIDGE_BUNDLE_INVALID
