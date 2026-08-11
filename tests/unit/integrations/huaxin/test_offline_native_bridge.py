"""
作者: BruceLee
文件职责: 验证自研华鑫 fake/offline C ABI 的构建、生命周期、health 与有界 drain。
主要输入: 会话级内容寻址 fake bundle 和测试队列容量。
主要输出: pytest 断言，证明显式构建/加载与同步队列合同可运行。
上游关系: build_native_bridge、NativeBridge 和 NativeRuntime 公共接口。
下游关系: 包内自研 C++ 动态库，不包含或调用华鑫厂商资产。
关键环境或配置: 仅在本机临时目录编译；不联网、不访问账号、不产生交易请求。
"""

from __future__ import annotations

import json

import pytest

from bullet_trade.integrations.huaxin import ABI_VERSION, BuildResult, NativeBridge


@pytest.mark.unit
def test_build_manifest_is_external_content_addressed_and_vendor_free(
    offline_bundle: BuildResult,
) -> None:
    """
    验证显式构建产物使用内容寻址，且 manifest 不宣称包含厂商 SDK。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
    返回:
        无；bundle 布局、指纹和无厂商资产声明正确即通过。
    """

    manifest = json.loads(offline_bundle.manifest_path.read_text(encoding="utf-8"))
    serialized = json.dumps(manifest, ensure_ascii=False, sort_keys=True)

    assert offline_bundle.bundle_path.name == offline_bundle.fingerprint
    assert manifest["fingerprint"]["value"] == offline_bundle.fingerprint
    assert manifest["bridge"]["abi_version"] == ABI_VERSION
    assert manifest["bridge"]["vendor_sdk_linked"] is False
    assert manifest["vendor_sdk"] == {"included": False, "status": "not_used"}
    assert manifest["integrity_scope"] == "self_consistency_not_provenance"
    assert manifest["runtime"] == {
        "inspection_status": "not_inspected",
        "dynamic_dependencies": None,
        "rpath": None,
    }
    assert str(offline_bundle.bundle_path.parent.parent) not in serialized
    assert "password" not in serialized.lower()
    assert "account_id" not in serialized.lower()
    assert "terminalinfo" not in serialized.lower()


@pytest.mark.unit
def test_fake_bridge_health_and_bounded_drain(offline_bundle: BuildResult) -> None:
    """
    验证 fake runtime 创建两个生命周期事件，并严格遵守 drain 上限。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
    返回:
        无；health、事件顺序、批次边界和关闭语义正确即通过。
    副作用:
        显式 dlopen 自研动态库并在进程内创建、销毁 opaque handle。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)

    assert bridge.abi_version() == ABI_VERSION
    assert bridge.bridge_version() == "bullet-trade-huaxin-offline-fake/1"
    runtime = bridge.create(queue_capacity=2)
    assert runtime.health().queue_capacity == 2
    assert runtime.health().queue_size == 2
    assert runtime.health().dropped_events == 0

    first = runtime.drain(1)
    second = runtime.drain(1)
    empty = runtime.drain(1)

    assert len(first) == 1
    assert len(second) == 1
    assert empty == []
    assert [first[0].sequence, second[0].sequence] == [1, 2]
    assert json.loads(first[0].payload.decode("utf-8"))["event"] == "bridge_created"
    assert json.loads(second[0].payload.decode("utf-8"))["event"] == "offline_ready"

    runtime.close()
    runtime.close()
    with pytest.raises(RuntimeError, match="已关闭"):
        runtime.health()


@pytest.mark.unit
@pytest.mark.parametrize("invalid_capacity", [0, 1, 1_000_001])
def test_fake_bridge_rejects_unbounded_capacity(
    offline_bundle: BuildResult,
    invalid_capacity: int,
) -> None:
    """
    验证 Python 边界拒绝过小或无控制上限的 native 队列容量。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
        invalid_capacity: 越出允许范围的测试容量。
    返回:
        无；创建前抛出 ValueError 即通过。
    副作用:
        仅显式加载自研动态库，不创建无效 native handle。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)

    with pytest.raises(ValueError, match="queue_capacity"):
        bridge.create(queue_capacity=invalid_capacity)


@pytest.mark.unit
@pytest.mark.parametrize("invalid_batch", [0, 4097])
def test_fake_bridge_rejects_unbounded_drain(
    offline_bundle: BuildResult,
    invalid_batch: int,
) -> None:
    """
    验证 Python 边界拒绝空批次或超过固定上限的 drain。

    参数:
        offline_bundle: 会话级已校验 fake bundle。
        invalid_batch: 越出允许范围的批次大小。
    返回:
        无；drain 前抛出 ValueError 即通过。
    副作用:
        创建并通过上下文管理器销毁一个 fake native handle。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    with bridge.create(queue_capacity=2) as runtime:
        with pytest.raises(ValueError, match="max_events"):
            runtime.drain(invalid_batch)
