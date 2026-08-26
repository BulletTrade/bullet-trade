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

import ctypes
import json

import pytest

import bullet_trade.integrations.huaxin.native as native_module
from bullet_trade.integrations.huaxin import (
    ABI_VERSION,
    FIELD_SET_VERSION,
    VENDOR_SCHEMA_ID,
    BuildResult,
    HuaxinAbiError,
    NativeBridge,
)


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
    assert manifest["bridge"]["vendor_schema_id"] == VENDOR_SCHEMA_ID
    assert manifest["bridge"]["field_set_version"] == FIELD_SET_VERSION
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
    assert bridge.bridge_version() == "bullet-trade-huaxin-offline-fake/2"
    assert bridge.vendor_schema_id() == VENDOR_SCHEMA_ID
    assert bridge.field_set_version() == FIELD_SET_VERSION
    runtime = bridge.create(queue_capacity=2)
    assert runtime.health().queue_capacity == 2
    assert runtime.health().queue_size == 2
    assert runtime.health().dropped_events == 0
    assert runtime.health().vendor_schema_id == VENDOR_SCHEMA_ID
    assert runtime.health().field_set_version == FIELD_SET_VERSION

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
def test_flat_abi_rejects_old_version_size_and_schema(offline_bundle: BuildResult) -> None:
    """验证旧 ABI、旧尺寸和错误 schema 在创建 handle 前 fail closed。

    Args:
        offline_bundle: 会话级已校验 fake bundle。

    Returns:
        None；三类不兼容输入返回各自稳定错误码且不产生 handle 即通过。

    Side Effects:
        显式加载离线动态库，只调用 fake create 负例，不连接厂商 SDK。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    cases = (
        ("abi_version", ABI_VERSION - 1, native_module.NATIVE_RESULT_ABI_INCOMPATIBLE),
        (
            "struct_size",
            ctypes.sizeof(native_module._CreateOptions) - 1,
            native_module.NATIVE_RESULT_STRUCT_SIZE_INCOMPATIBLE,
        ),
    )
    for field_name, invalid_value, expected_result in cases:
        options = native_module._CreateOptions(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._CreateOptions),
            queue_capacity=2,
            reserved=0,
            schema=native_module._schema_identity(),
        )
        setattr(options, field_name, invalid_value)
        handle = ctypes.c_void_p()
        result = int(bridge._library.bt_huaxin_create(ctypes.byref(options), ctypes.byref(handle)))
        assert result == expected_result
        assert not handle.value

    options = native_module._CreateOptions(
        abi_version=ABI_VERSION,
        struct_size=ctypes.sizeof(native_module._CreateOptions),
        queue_capacity=2,
        reserved=0,
        schema=native_module._schema_identity(),
    )
    options.schema.field_set_version[0] = ord("x")
    handle = ctypes.c_void_p()
    result = int(bridge._library.bt_huaxin_create(ctypes.byref(options), ctypes.byref(handle)))
    assert result == native_module.NATIVE_RESULT_SCHEMA_INCOMPATIBLE
    assert not handle.value


@pytest.mark.unit
def test_pod_request_preserves_binary_payload_and_uint64_id(
    offline_bundle: BuildResult,
) -> None:
    """验证 POD 请求和批量回执保留二进制 bytes 与 uint64 request ID。

    Args:
        offline_bundle: 会话级已校验 fake bundle。

    Returns:
        None；payload、request ID、schema 与事件顺序完整往返即通过。

    Side Effects:
        在离线 fake runtime 内入队并 drain 一个只读 ping 回执。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    with bridge.create(queue_capacity=3) as runtime:
        runtime.drain(2)
        request_id = (1 << 63) + 7
        payload = b"a\x00b\xff"
        runtime.submit_request(request_id=request_id, payload=payload)
        events = runtime.drain(1)

    assert len(events) == 1
    assert events[0].request_id == request_id
    assert events[0].payload == payload
    assert events[0].vendor_schema_id == VENDOR_SCHEMA_ID
    assert events[0].field_set_version == FIELD_SET_VERSION


@pytest.mark.unit
def test_copied_batch_descriptor_cannot_double_free(offline_bundle: BuildResult) -> None:
    """验证 bridge registry 原子拒绝复制描述符导致的第二次 free。

    Args:
        offline_bundle: 会话级已校验 fake bundle。

    Returns:
        None；原描述符释放成功、复制描述符返回稳定 ownership 错误即通过。

    Side Effects:
        从离线 fake 队列取得一块 bridge-owned 内存并经 C ABI 释放。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    with bridge.create(queue_capacity=2) as runtime:
        batch = native_module._EventBatch(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._EventBatch),
            schema=native_module._schema_identity(),
        )
        result = int(
            bridge._library.bt_huaxin_drain_event_batch(
                runtime._handle, ctypes.c_uint32(1), ctypes.byref(batch)
            )
        )
        assert result == 0
        copied = native_module._EventBatch()
        ctypes.memmove(ctypes.byref(copied), ctypes.byref(batch), ctypes.sizeof(batch))
        assert int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(batch))) == 0
        assert (
            int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(copied)))
            == native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR
        )


@pytest.mark.unit
def test_stale_token_cannot_claim_a_new_batch_at_a_reused_address(
    offline_bundle: BuildResult,
) -> None:
    """验证旧 allocation ID 即使配上新 buffer 地址也不能释放新批次。

    Args:
        offline_bundle: 会话级已校验 fake bundle。

    Returns:
        None: 旧 token 被拒绝且新批次仍能读取、正常释放时返回。

    Side Effects:
        通过离线 C ABI drain 两个批次，并在测试描述符中模拟 allocator 地址复用。
    """
    bridge = NativeBridge.load(offline_bundle.bundle_path)
    with bridge.create(queue_capacity=2) as runtime:
        first = native_module._EventBatch(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._EventBatch),
            schema=native_module._schema_identity(),
        )
        assert (
            int(
                bridge._library.bt_huaxin_drain_event_batch(
                    runtime._handle,
                    ctypes.c_uint32(1),
                    ctypes.byref(first),
                )
            )
            == 0
        )
        stale_token = int(first.ownership_token)
        assert int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(first))) == 0

        second = native_module._EventBatch(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._EventBatch),
            schema=native_module._schema_identity(),
        )
        assert (
            int(
                bridge._library.bt_huaxin_drain_event_batch(
                    runtime._handle,
                    ctypes.c_uint32(1),
                    ctypes.byref(second),
                )
            )
            == 0
        )
        stale = native_module._EventBatch(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._EventBatch),
            event_count=int(second.event_count),
            event_stride=int(second.event_stride),
            schema=native_module._schema_identity(),
            events=second.events,
            ownership_token=stale_token,
        )
        assert (
            int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(stale)))
            == native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR
        )
        assert (
            json.loads(bytes(second.events[0].payload[: second.events[0].payload_size]))["event"]
            == "offline_ready"
        )
        assert int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(second))) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("damage", "expected_result"),
    (
        ("schema", native_module.NATIVE_RESULT_SCHEMA_INCOMPATIBLE),
        ("metadata", native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR),
    ),
)
def test_free_reports_corruption_after_reclaiming_registered_batch(
    offline_bundle: BuildResult,
    damage: str,
    expected_result: int,
) -> None:
    """验证损坏描述符仍回收 registry buffer，并返回精确负码。

    Args:
        offline_bundle: 会话级已校验 fake bundle。
        damage: 本例破坏的 schema 或所有权 metadata 类型。
        expected_result: 对应损坏类型的稳定 C ABI 返回码。

    Returns:
        None；首次 free 返回精确错误且原 allocation ID 无法再次认领即通过。

    Side Effects:
        从离线 fake 队列取得一块 bridge-owned 内存并故意损坏调用方描述符。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    with bridge.create(queue_capacity=2) as runtime:
        batch = native_module._EventBatch(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(native_module._EventBatch),
            schema=native_module._schema_identity(),
        )
        assert (
            int(
                bridge._library.bt_huaxin_drain_event_batch(
                    runtime._handle,
                    ctypes.c_uint32(1),
                    ctypes.byref(batch),
                )
            )
            == 0
        )
        stale = native_module._EventBatch()
        ctypes.memmove(ctypes.byref(stale), ctypes.byref(batch), ctypes.sizeof(batch))
        if damage == "schema":
            batch.schema.field_set_version[0] = ord("x")
        else:
            batch.event_count += 1

        assert (
            int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(batch))) == expected_result
        )
        assert (
            int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(stale)))
            == native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR
        )


@pytest.mark.unit
def test_python_drain_frees_batch_when_event_schema_decode_fails(
    offline_bundle: BuildResult,
) -> None:
    """验证 Python 在事件 schema 校验失败时仍通过 finally 释放 native batch。

    Args:
        offline_bundle: 会话级已校验 fake bundle。

    Returns:
        None；解码受控失败后旧 token 已从 registry 移除即通过。

    Side Effects:
        测试期间临时包装一个 ctypes 符号以破坏 fake 事件 schema，随后恢复原符号。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    original_drain = bridge._library.bt_huaxin_drain_event_batch
    allocation = {}

    def corrupt_event_schema(handle: object, max_events: object, batch_pointer: object) -> int:
        """调用真实 fake drain 后破坏首个事件 schema 以触发 Python 负例。

        Args:
            handle: opaque fake runtime handle。
            max_events: C ABI 批次上限。
            batch_pointer: caller-owned batch 描述符指针。

        Returns:
            int: 原生 drain 的稳定返回码。

        Side Effects:
            成功时记录分配描述符，并只修改测试事件内的 field-set bytes。
        """

        result = int(original_drain(handle, max_events, batch_pointer))
        batch = ctypes.cast(batch_pointer, ctypes.POINTER(native_module._EventBatch)).contents
        if result == 0 and batch.event_count:
            allocation.update(
                events=ctypes.cast(batch.events, ctypes.c_void_p).value,
                token=int(batch.ownership_token),
                count=int(batch.event_count),
                stride=int(batch.event_stride),
            )
            batch.events[0].schema.field_set_version[0] = ord("x")
        return result

    bridge._library.bt_huaxin_drain_event_batch = corrupt_event_schema
    try:
        with bridge.create(queue_capacity=2) as runtime:
            with pytest.raises(HuaxinAbiError):
                runtime.drain(1)
    finally:
        bridge._library.bt_huaxin_drain_event_batch = original_drain

    stale = native_module._EventBatch(
        abi_version=ABI_VERSION,
        struct_size=ctypes.sizeof(native_module._EventBatch),
        event_count=allocation["count"],
        event_stride=allocation["stride"],
        schema=native_module._schema_identity(),
        events=ctypes.cast(allocation["events"], ctypes.POINTER(native_module._Event)),
        ownership_token=allocation["token"],
    )
    assert (
        int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(stale)))
        == native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR
    )


@pytest.mark.unit
@pytest.mark.parametrize("damage", ("schema", "struct_size"))
def test_python_drain_frees_batch_when_outer_descriptor_is_corrupt(
    offline_bundle: BuildResult,
    damage: str,
) -> None:
    """验证 Python 在 batch 外层描述符损坏时仍用可信 cleanup 描述符回收。

    Args:
        offline_bundle: 会话级已校验 fake bundle。
        damage: 本例破坏的外层 schema 或 struct_size 字段。

    Returns:
        None；Python 受控失败且 allocation ID 已从 registry 移除即通过。

    Side Effects:
        测试期间临时包装 ctypes drain 符号，记录并破坏返回描述符，随后恢复。
    """

    bridge = NativeBridge.load(offline_bundle.bundle_path)
    original_drain = bridge._library.bt_huaxin_drain_event_batch
    allocation = {}

    def corrupt_batch_descriptor(
        handle: object,
        max_events: object,
        batch_pointer: object,
    ) -> int:
        """调用真实 fake drain 后破坏 batch 外层字段。

        Args:
            handle: opaque fake runtime handle。
            max_events: C ABI 批次上限。
            batch_pointer: caller-owned batch 描述符指针。

        Returns:
            int: 原生 drain 的稳定返回码。

        Side Effects:
            成功时记录 allocation，并修改测试描述符的 schema 或 struct_size。
        """

        result = int(original_drain(handle, max_events, batch_pointer))
        batch = ctypes.cast(batch_pointer, ctypes.POINTER(native_module._EventBatch)).contents
        if result == 0 and batch.event_count:
            allocation.update(
                events=ctypes.cast(batch.events, ctypes.c_void_p).value,
                token=int(batch.ownership_token),
                count=int(batch.event_count),
                stride=int(batch.event_stride),
            )
            if damage == "schema":
                batch.schema.field_set_version[0] = ord("x")
            else:
                batch.struct_size -= 1
        return result

    bridge._library.bt_huaxin_drain_event_batch = corrupt_batch_descriptor
    try:
        with bridge.create(queue_capacity=2) as runtime:
            with pytest.raises(HuaxinAbiError):
                runtime.drain(1)
    finally:
        bridge._library.bt_huaxin_drain_event_batch = original_drain

    stale = native_module._EventBatch(
        abi_version=ABI_VERSION,
        struct_size=ctypes.sizeof(native_module._EventBatch),
        event_count=allocation["count"],
        event_stride=allocation["stride"],
        schema=native_module._schema_identity(),
        events=ctypes.cast(allocation["events"], ctypes.POINTER(native_module._Event)),
        ownership_token=allocation["token"],
    )
    assert (
        int(bridge._library.bt_huaxin_free_event_batch(ctypes.byref(stale)))
        == native_module.NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR
    )


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
