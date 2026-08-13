"""
作者: BruceLee
文件职责: 通过显式 ctypes 加载和调用 BulletTrade 自研华鑫 flat C ABI。
主要输入: 已通过内容指纹校验的 native bundle、队列容量和 drain 上限。
主要输出: bridge 版本、结构化 health、opaque runtime 生命周期和批量事件。
上游关系: doctor/CLI 或未来 Huaxin Broker/Realtime Feed 在 preflight 后显式调用。
下游关系: native_src 中的自研 fake/offline bridge；未来可替换为同 ABI 的真实 bridge。
关键环境或配置: import 本模块不执行 dlopen；只有 NativeBridge.load 才显式加载动态库。
"""

from __future__ import annotations

import ctypes
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from .errors import (
    HUAXIN_NATIVE_UNAVAILABLE,
    NATIVE_ABI_INCOMPATIBLE,
    NATIVE_CALL_FAILED,
    VENDOR_SCHEMA_INCOMPATIBLE,
    HuaxinAbiError,
    HuaxinNativeCallError,
    HuaxinNativeUnavailableError,
)

ABI_VERSION = 2
VENDOR_SCHEMA_ID = "bullet_trade.huaxin.offline_fake.v1"
FIELD_SET_VERSION = "1"
VENDOR_SCHEMA_ID_CAPACITY = 64
FIELD_SET_VERSION_CAPACITY = 32
EVENT_PAYLOAD_CAPACITY = 192
REQUEST_PAYLOAD_CAPACITY = 192
MAX_DRAIN_EVENTS = 4096
REQUEST_TYPE_PING = 1

NATIVE_RESULT_ABI_INCOMPATIBLE = -2
NATIVE_RESULT_STRUCT_SIZE_INCOMPATIBLE = -3
NATIVE_RESULT_SCHEMA_INCOMPATIBLE = -6
NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR = -7


class _SchemaIdentity(ctypes.Structure):
    """映射固定长度、显式字节数的 C ABI schema 身份。

    该结构只嵌入其他 POD，不独立传给 native；关键状态是不依赖 NUL 结尾的
    vendor schema ID 和 field-set version。
    """

    _fields_ = [
        ("vendor_schema_id_size", ctypes.c_uint32),
        ("field_set_version_size", ctypes.c_uint32),
        ("vendor_schema_id", ctypes.c_uint8 * VENDOR_SCHEMA_ID_CAPACITY),
        ("field_set_version", ctypes.c_uint8 * FIELD_SET_VERSION_CAPACITY),
    ]


class _CreateOptions(ctypes.Structure):
    """映射 caller-owned 的 C ABI v2 runtime 创建参数结构。

    由 ``NativeBridge.create`` 在栈式 ctypes 内存中构造；native 只在调用期间读取，
    关键状态包含精确 ABI/结构大小、容量和 schema 身份。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("queue_capacity", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("schema", _SchemaIdentity),
    ]


class _Health(ctypes.Structure):
    """映射 caller-owned 的 C ABI v2 离线 health 输出结构。

    Python 先初始化 ABI/大小/schema，native 再填充水位；关键状态不包含任何账号、
    SDK 路径或厂商对象。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("state", ctypes.c_int32),
        ("queue_capacity", ctypes.c_uint32),
        ("queue_size", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("dropped_events", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
    ]


class _Request(ctypes.Structure):
    """映射 caller-owned 的只读 fake POD 请求结构。

    由 ``NativeRuntime.submit_request`` 构造并仅在同步 C 调用期间借给 bridge；关键状态
    包含稳定 request ID、请求类型、schema 以及不依赖文本终止符的 bytes payload。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("request_type", ctypes.c_uint32),
        ("payload_size", ctypes.c_uint32),
        ("request_id", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
        ("payload", ctypes.c_uint8 * REQUEST_PAYLOAD_CAPACITY),
    ]


class _Event(ctypes.Structure):
    """映射 bridge-owned batch 内的无指针 C ABI v2 事件 POD。

    由 native 连续缓冲区持有直至显式 free；关键状态包含 request/sequence/int64 时间、
    schema 身份和显式长度 payload，Python 必须先复制再释放 batch。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("event_type", ctypes.c_uint32),
        ("payload_size", ctypes.c_uint32),
        ("sequence", ctypes.c_uint64),
        ("received_ns", ctypes.c_int64),
        ("request_id", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
        ("payload", ctypes.c_uint8 * EVENT_PAYLOAD_CAPACITY),
    ]


class _EventBatch(ctypes.Structure):
    """映射需显式释放的 bridge-owned 批量事件描述符。

    Python 负责描述符内存，bridge 负责 ``events`` 指向的连续数组；关键状态包含数量、
    stride、schema 和不可复制的 ownership token，生命周期以 free 函数闭环。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("event_count", ctypes.c_uint32),
        ("event_stride", ctypes.c_uint32),
        ("schema", _SchemaIdentity),
        ("events", ctypes.POINTER(_Event)),
        ("ownership_token", ctypes.c_uint64),
    ]


@dataclass(frozen=True)
class NativeHealth:
    """表示 fake/offline runtime 的不可变 health 快照。

    由 ``NativeRuntime.health`` 从严格校验后的 C POD 创建；关键状态包含有界队列水位、
    丢弃计数和显式 vendor schema/field-set，不持有 native 内存。
    """

    state: int
    queue_capacity: int
    queue_size: int
    dropped_events: int
    vendor_schema_id: str
    field_set_version: str


@dataclass(frozen=True)
class NativeEvent:
    """表示从 native 有界队列 drain 出的 Python 自有事件副本。

    由 runtime 在释放 bridge-owned batch 前逐字段复制；关键状态保留 uint64 request/sequence、
    int64 接收时间、schema 身份及原始 bytes payload。
    """

    event_type: int
    sequence: int
    received_ns: int
    request_id: int
    vendor_schema_id: str
    field_set_version: str
    payload: bytes


def _schema_identity() -> _SchemaIdentity:
    """构造当前 wrapper 明确要求的 schema 身份 POD。

    Args:
        无。

    Returns:
        _SchemaIdentity: 带显式长度且未依赖 NUL 结尾的 C ABI 字节结构。

    Side Effects:
        仅分配 Python 管理的 ctypes 内存，不调用 native。
    """

    vendor_schema = VENDOR_SCHEMA_ID.encode("utf-8")
    field_set = FIELD_SET_VERSION.encode("utf-8")
    if len(vendor_schema) > VENDOR_SCHEMA_ID_CAPACITY:
        raise RuntimeError("VENDOR_SCHEMA_ID 超过 C ABI 固定容量")
    if len(field_set) > FIELD_SET_VERSION_CAPACITY:
        raise RuntimeError("FIELD_SET_VERSION 超过 C ABI 固定容量")
    identity = _SchemaIdentity(
        vendor_schema_id_size=len(vendor_schema),
        field_set_version_size=len(field_set),
    )
    identity.vendor_schema_id[: len(vendor_schema)] = vendor_schema
    identity.field_set_version[: len(field_set)] = field_set
    return identity


def _decode_schema_identity(identity: _SchemaIdentity, operation: str) -> tuple:
    """严格解码 native 返回的 schema 身份并拒绝越界长度。

    Args:
        identity: 来自 health、event 或 batch 的嵌入 schema POD。
        operation: 当前检查阶段，用于脱敏诊断。

    Returns:
        tuple: ``(vendor_schema_id, field_set_version)`` 两个 UTF-8 文本。

    Raises:
        HuaxinAbiError: 长度越界、UTF-8 非法或值与 wrapper 合同不一致。

    Side Effects:
        无。
    """

    vendor_size = int(identity.vendor_schema_id_size)
    field_set_size = int(identity.field_set_version_size)
    if vendor_size > VENDOR_SCHEMA_ID_CAPACITY or field_set_size > FIELD_SET_VERSION_CAPACITY:
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native schema 身份长度超过 C ABI 固定容量",
            {"operation": operation},
        )
    try:
        vendor_schema_id = bytes(identity.vendor_schema_id[:vendor_size]).decode(
            "utf-8", errors="strict"
        )
        field_set_version = bytes(identity.field_set_version[:field_set_size]).decode(
            "utf-8", errors="strict"
        )
    except UnicodeDecodeError as exc:
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native schema 身份不是合法 UTF-8",
            {"operation": operation},
        ) from exc
    if vendor_schema_id != VENDOR_SCHEMA_ID or field_set_version != FIELD_SET_VERSION:
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native vendor schema 或 field-set 与 wrapper 不一致",
            {
                "operation": operation,
                "expected_vendor_schema_id": VENDOR_SCHEMA_ID,
                "actual_vendor_schema_id": vendor_schema_id,
                "expected_field_set_version": FIELD_SET_VERSION,
                "actual_field_set_version": field_set_version,
            },
        )
    return vendor_schema_id, field_set_version


def _validate_native_struct(
    value: ctypes.Structure,
    structure_type: Type[ctypes.Structure],
    operation: str,
) -> None:
    """验证 native 返回 POD 的 ABI major、精确结构大小和 schema。

    Args:
        value: 已由 native 填充的 ctypes 结构。
        structure_type: 当前合同预期的 ctypes 结构类型。
        operation: 当前操作名称，用于脱敏错误详情。

    Returns:
        None。

    Raises:
        HuaxinAbiError: ABI、结构大小或 schema 任一不匹配。

    Side Effects:
        无。
    """

    actual_abi = int(getattr(value, "abi_version"))
    actual_size = int(getattr(value, "struct_size"))
    expected_size = ctypes.sizeof(structure_type)
    if actual_abi != ABI_VERSION or actual_size != expected_size:
        raise HuaxinAbiError(
            NATIVE_ABI_INCOMPATIBLE,
            "native 返回结构的 ABI 或大小不兼容",
            {
                "operation": operation,
                "expected_abi": ABI_VERSION,
                "actual_abi": actual_abi,
                "expected_size": expected_size,
                "actual_size": actual_size,
            },
        )
    _decode_schema_identity(getattr(value, "schema"), operation)


def _configure_signatures(library: ctypes.CDLL) -> None:
    """
    为显式加载的自研动态库设置 ctypes 参数和返回类型。

    参数:
        library: 已由调用方显式 dlopen 的 ctypes 动态库对象。
    返回:
        无返回值。
    副作用:
        修改 ctypes 函数对象的 argtypes/restype，避免隐式整数截断。
    异常:
        AttributeError: 动态库缺少必需 C ABI 符号时抛出。
    """

    library.bt_huaxin_abi_version.argtypes = []
    library.bt_huaxin_abi_version.restype = ctypes.c_uint32
    library.bt_huaxin_bridge_version.argtypes = []
    library.bt_huaxin_bridge_version.restype = ctypes.c_char_p
    library.bt_huaxin_vendor_schema_id.argtypes = []
    library.bt_huaxin_vendor_schema_id.restype = ctypes.c_char_p
    library.bt_huaxin_field_set_version.argtypes = []
    library.bt_huaxin_field_set_version.restype = ctypes.c_char_p
    library.bt_huaxin_error_message.argtypes = [ctypes.c_int32]
    library.bt_huaxin_error_message.restype = ctypes.c_char_p
    library.bt_huaxin_create.argtypes = [
        ctypes.POINTER(_CreateOptions),
        ctypes.POINTER(ctypes.c_void_p),
    ]
    library.bt_huaxin_create.restype = ctypes.c_int32
    library.bt_huaxin_destroy.argtypes = [ctypes.c_void_p]
    library.bt_huaxin_destroy.restype = ctypes.c_int32
    library.bt_huaxin_get_health.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Health)]
    library.bt_huaxin_get_health.restype = ctypes.c_int32
    library.bt_huaxin_submit_request.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_Request),
    ]
    library.bt_huaxin_submit_request.restype = ctypes.c_int32
    library.bt_huaxin_drain_event_batch.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_EventBatch),
    ]
    library.bt_huaxin_drain_event_batch.restype = ctypes.c_int32
    library.bt_huaxin_free_event_batch.argtypes = [ctypes.POINTER(_EventBatch)]
    library.bt_huaxin_free_event_batch.restype = ctypes.c_int32


class NativeBridge:
    """代表已通过 bundle 与 ABI v2 身份校验的显式 C bridge。

    核心协作对象是 ``verify_bundle``、ctypes 动态库和 ``NativeRuntime``；关键状态包含
    制品路径、不可变 manifest 视图及配置好签名的库对象，构造本身不创建 runtime。
    """

    def __init__(
        self,
        library_path: Path,
        library: ctypes.CDLL,
        manifest: Dict[str, Any],
    ) -> None:
        """
        保存已加载动态库和已验证 manifest。

        参数:
            library_path: bundle 内自研动态库的绝对路径。
            library: 已配置函数签名的 ctypes.CDLL 对象。
            manifest: 已通过指纹和 artifact hash 校验的 manifest。
        返回:
            无返回值；初始化 bridge 对象。
        """

        self.library_path = library_path
        self._library = library
        self.manifest = dict(manifest)

    @classmethod
    def load(cls: Type["NativeBridge"], bundle_path: Path) -> "NativeBridge":
        """
        校验 bundle 后显式加载自研 native bridge。

        参数:
            bundle_path: 含 manifest.json 和自研动态库的 bundle 目录。
        返回:
            可创建 opaque runtime 的 NativeBridge。
        副作用:
            校验成功后调用 ctypes.CDLL；不会编译、联网或加载厂商 SDK。
        异常:
            HuaxinBundleError: manifest、指纹或 artifact 校验失败。
            HuaxinNativeUnavailableError: 动态库无法由当前平台加载。
            HuaxinAbiError: 动态库 ABI major 与 wrapper 不一致。
        """

        from .build import verify_bundle

        manifest, artifact_path = verify_bundle(bundle_path)
        try:
            library = ctypes.CDLL(str(artifact_path))
            _configure_signatures(library)
        except (OSError, AttributeError) as exc:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "自研华鑫 bridge 无法在当前进程显式加载",
                {"error_type": type(exc).__name__},
            ) from exc

        actual_abi = int(library.bt_huaxin_abi_version())
        if actual_abi != ABI_VERSION:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "Python wrapper 与 native bridge ABI 不一致",
                {"expected": ABI_VERSION, "actual": actual_abi},
            )
        actual_vendor_schema = cls._decode_static_text(
            library.bt_huaxin_vendor_schema_id(), "vendor_schema_id"
        )
        actual_field_set = cls._decode_static_text(
            library.bt_huaxin_field_set_version(), "field_set_version"
        )
        if actual_vendor_schema != VENDOR_SCHEMA_ID or actual_field_set != FIELD_SET_VERSION:
            raise HuaxinAbiError(
                VENDOR_SCHEMA_INCOMPATIBLE,
                "Python wrapper 与 native bridge schema 身份不一致",
                {
                    "expected_vendor_schema_id": VENDOR_SCHEMA_ID,
                    "actual_vendor_schema_id": actual_vendor_schema,
                    "expected_field_set_version": FIELD_SET_VERSION,
                    "actual_field_set_version": actual_field_set,
                },
            )
        return cls(artifact_path, library, manifest)

    @staticmethod
    def _decode_static_text(raw: Optional[bytes], field_name: str) -> str:
        """解码由 bridge 静态存储持有的非空 UTF-8 文本。

        Args:
            raw: ``c_char_p`` 返回的 bytes 或空指针对应的 None。
            field_name: 当前字段名，用于受控诊断。

        Returns:
            str: 解码后的文本。

        Raises:
            HuaxinAbiError: 指针为空或 bytes 不是合法 UTF-8。

        Side Effects:
            无；静态文本归 bridge 所有，Python 不释放。
        """

        if raw is None:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native bridge 缺少必需静态身份文本",
                {"field": field_name},
            )
        try:
            return raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native bridge 静态身份文本不是合法 UTF-8",
                {"field": field_name},
            ) from exc

    def abi_version(self) -> int:
        """
        读取已加载 bridge 的 ABI major。

        参数:
            无。
        返回:
            native bridge 报告的 ABI 整数。
        """

        return int(self._library.bt_huaxin_abi_version())

    def bridge_version(self) -> str:
        """
        读取自研 bridge 的非敏感版本标识。

        参数:
            无。
        返回:
            UTF-8 版本字符串；空指针时返回空字符串。
        """

        raw = self._library.bt_huaxin_bridge_version()
        return self._decode_static_text(raw, "bridge_version")

    def vendor_schema_id(self) -> str:
        """读取 bridge 明确声明的 vendor schema ID。

        Args:
            无。

        Returns:
            str: 与所有 v2 POD 共同使用的 schema ID。

        Side Effects:
            无；返回值来自 bridge 静态存储的 Python 副本。
        """

        return self._decode_static_text(
            self._library.bt_huaxin_vendor_schema_id(), "vendor_schema_id"
        )

    def field_set_version(self) -> str:
        """读取 bridge 明确声明的 field-set version。

        Args:
            无。

        Returns:
            str: 与所有 v2 POD 共同使用的字段集版本。

        Side Effects:
            无；返回值来自 bridge 静态存储的 Python 副本。
        """

        return self._decode_static_text(
            self._library.bt_huaxin_field_set_version(), "field_set_version"
        )

    def create(self, queue_capacity: int = 64) -> "NativeRuntime":
        """
        创建一个只含 fake/offline 有界队列的 opaque runtime。

        参数:
            queue_capacity: native 队列最大事件数，必须位于 2 到 1,000,000。
        返回:
            需要显式 close 或使用上下文管理器的 NativeRuntime。
        副作用:
            在当前进程 native 堆上分配一个 handle；不创建线程或网络连接。
        异常:
            ValueError: 队列容量越界。
            HuaxinNativeCallError: C ABI 创建失败。
        """

        if queue_capacity < 2 or queue_capacity > 1_000_000:
            raise ValueError("queue_capacity 必须位于 2 到 1,000,000")
        options = _CreateOptions(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(_CreateOptions),
            queue_capacity=queue_capacity,
            reserved=0,
            schema=_schema_identity(),
        )
        handle = ctypes.c_void_p()
        result = int(self._library.bt_huaxin_create(ctypes.byref(options), ctypes.byref(handle)))
        self._raise_for_result(result, "create")
        if not handle.value:
            raise HuaxinNativeCallError(
                NATIVE_CALL_FAILED,
                "native create 返回成功但未提供 handle",
                {"operation": "create"},
            )
        return NativeRuntime(self, handle)

    def _raise_for_result(self, result: int, operation: str) -> None:
        """
        将 C ABI 非零返回码转换为稳定 Python 异常。

        参数:
            result: C ABI 返回的整数错误码。
            operation: 当前操作名称，仅用于脱敏诊断。
        返回:
            返回码为零时无返回值。
        异常:
            HuaxinAbiError: native 报告 ABI/struct size 不兼容。
            HuaxinNativeCallError: 其他受控 native 错误。
        """

        if result == 0:
            return
        raw_message = self._library.bt_huaxin_error_message(result)
        native_message = raw_message.decode("utf-8", errors="replace") if raw_message else "unknown"
        details = {
            "operation": operation,
            "native_code": result,
            "native_message": native_message,
        }
        if result in (
            NATIVE_RESULT_ABI_INCOMPATIBLE,
            NATIVE_RESULT_STRUCT_SIZE_INCOMPATIBLE,
        ):
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native C ABI 版本或结构大小不兼容",
                details,
            )
        if result == NATIVE_RESULT_SCHEMA_INCOMPATIBLE:
            raise HuaxinAbiError(
                VENDOR_SCHEMA_INCOMPATIBLE,
                "native vendor schema 或 field-set 不兼容",
                details,
            )
        raise HuaxinNativeCallError(
            NATIVE_CALL_FAILED,
            "native C ABI 调用失败",
            details,
        )


class NativeRuntime:
    """管理一个 opaque native handle，并提供同步 health 与有界批量 drain。"""

    def __init__(self, bridge: NativeBridge, handle: ctypes.c_void_p) -> None:
        """
        保存 bridge、opaque handle 和 Python 侧生命周期锁。

        参数:
            bridge: 创建当前 handle 的已加载 bridge。
            handle: C ABI 返回的非空 opaque 指针。
        返回:
            无返回值；初始化 runtime 对象。
        """

        self._bridge = bridge
        self._handle: Optional[ctypes.c_void_p] = handle
        self._lock = threading.RLock()

    def _require_handle(self) -> ctypes.c_void_p:
        """
        返回仍有效的 opaque handle。

        参数:
            无。
        返回:
            尚未关闭的 ctypes.c_void_p。
        异常:
            RuntimeError: runtime 已经关闭。
        """

        if self._handle is None or not self._handle.value:
            raise RuntimeError("NativeRuntime 已关闭")
        return self._handle

    def health(self) -> NativeHealth:
        """
        读取 runtime 的离线状态和有界队列水位。

        参数:
            无。
        返回:
            不含路径、账号或网络信息的 NativeHealth 快照。
        副作用:
            获取短时 Python 生命周期锁并调用同步 C ABI。
        """

        with self._lock:
            handle = self._require_handle()
            raw = _Health(
                abi_version=ABI_VERSION,
                struct_size=ctypes.sizeof(_Health),
                state=0,
                queue_capacity=0,
                queue_size=0,
                reserved=0,
                dropped_events=0,
                schema=_schema_identity(),
            )
            result = int(self._bridge._library.bt_huaxin_get_health(handle, ctypes.byref(raw)))
            self._bridge._raise_for_result(result, "health")
            _validate_native_struct(raw, _Health, "health")
            vendor_schema_id, field_set_version = _decode_schema_identity(raw.schema, "health")
            return NativeHealth(
                state=int(raw.state),
                queue_capacity=int(raw.queue_capacity),
                queue_size=int(raw.queue_size),
                dropped_events=int(raw.dropped_events),
                vendor_schema_id=vendor_schema_id,
                field_set_version=field_set_version,
            )

    def submit_request(
        self,
        request_id: int,
        payload: bytes = b"",
        request_type: int = REQUEST_TYPE_PING,
    ) -> None:
        """同步提交一个版本化 fake POD 请求。

        Args:
            request_id: 非零 uint64 请求标识，由调用方生成并用于关联回执。
            payload: 最多 192 字节的原始二进制负载，可包含 NUL。
            request_type: 稳定请求类型；当前离线合同只支持 ping。

        Returns:
            None。

        Raises:
            TypeError: payload 不是 bytes。
            ValueError: request_id 或 payload 越出固定合同范围。
            HuaxinNativeCallError: native 拒绝请求或队列已满。

        Side Effects:
            获取生命周期锁并让 native fake runtime 入队一个请求完成事件；不联网、不交易。
        """

        if not isinstance(payload, bytes):
            raise TypeError("payload 必须为 bytes")
        if request_id < 1 or request_id > (1 << 64) - 1:
            raise ValueError("request_id 必须为非零 uint64")
        if len(payload) > REQUEST_PAYLOAD_CAPACITY:
            raise ValueError(f"payload 不能超过 {REQUEST_PAYLOAD_CAPACITY} 字节")
        raw = _Request(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(_Request),
            request_type=request_type,
            payload_size=len(payload),
            request_id=request_id,
            schema=_schema_identity(),
        )
        raw.payload[: len(payload)] = payload
        with self._lock:
            handle = self._require_handle()
            result = int(self._bridge._library.bt_huaxin_submit_request(handle, ctypes.byref(raw)))
            self._bridge._raise_for_result(result, "submit_request")

    def drain(self, max_events: int) -> List[NativeEvent]:
        """
        从 native 队列最多复制指定数量的事件。

        参数:
            max_events: 本次最多返回的事件数，必须位于 1 到 4096。
        返回:
            Python 自有的 NativeEvent 列表，长度不超过 max_events。
        副作用:
            从 native 有界队列移除已复制事件。
        异常:
            ValueError: max_events 越界。
            HuaxinNativeCallError: native drain 失败。
        """

        if max_events < 1 or max_events > MAX_DRAIN_EVENTS:
            raise ValueError(f"max_events 必须位于 1 到 {MAX_DRAIN_EVENTS}")
        with self._lock:
            handle = self._require_handle()
            batch = _EventBatch(
                abi_version=ABI_VERSION,
                struct_size=ctypes.sizeof(_EventBatch),
                event_count=0,
                event_stride=0,
                schema=_schema_identity(),
                events=None,
                ownership_token=0,
            )
            try:
                result = int(
                    self._bridge._library.bt_huaxin_drain_event_batch(
                        handle,
                        ctypes.c_uint32(max_events),
                        ctypes.byref(batch),
                    )
                )
                self._bridge._raise_for_result(result, "drain_event_batch")
                _validate_native_struct(batch, _EventBatch, "drain_event_batch")
                count = int(batch.event_count)
                if count > max_events:
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native batch 数量超过调用方上限",
                        {"operation": "drain_event_batch"},
                    )
                event_address = ctypes.cast(batch.events, ctypes.c_void_p).value
                ownership_token = int(batch.ownership_token)
                if count == 0:
                    if event_address or ownership_token:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native 空 batch 携带了所有权指针",
                            {"operation": "drain_event_batch"},
                        )
                    return []
                if (
                    int(batch.event_stride) != ctypes.sizeof(_Event)
                    or not event_address
                    or not ownership_token
                ):
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native batch stride 或所有权描述符不兼容",
                        {"operation": "drain_event_batch"},
                    )

                events: List[NativeEvent] = []
                for index in range(count):
                    raw = batch.events[index]
                    _validate_native_struct(raw, _Event, "drain_event")
                    payload_size = int(raw.payload_size)
                    if payload_size > EVENT_PAYLOAD_CAPACITY:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native event payload 长度超过固定容量",
                            {"operation": "drain_event"},
                        )
                    vendor_schema_id, field_set_version = _decode_schema_identity(
                        raw.schema, "drain_event"
                    )
                    events.append(
                        NativeEvent(
                            event_type=int(raw.event_type),
                            sequence=int(raw.sequence),
                            received_ns=int(raw.received_ns),
                            request_id=int(raw.request_id),
                            vendor_schema_id=vendor_schema_id,
                            field_set_version=field_set_version,
                            payload=bytes(raw.payload[:payload_size]),
                        )
                    )
                return events
            finally:
                cleanup_batch = _EventBatch(
                    abi_version=ABI_VERSION,
                    struct_size=ctypes.sizeof(_EventBatch),
                    event_count=int(batch.event_count),
                    event_stride=int(batch.event_stride),
                    schema=_schema_identity(),
                    events=batch.events,
                    ownership_token=int(batch.ownership_token),
                )
                free_result = int(
                    self._bridge._library.bt_huaxin_free_event_batch(ctypes.byref(cleanup_batch))
                )
                if free_result != 0:
                    self._bridge._raise_for_result(free_result, "free_event_batch")

    def close(self) -> None:
        """
        幂等销毁 opaque native handle。

        参数:
            无。
        返回:
            无返回值。
        副作用:
            释放 native 堆对象，并使后续 health/drain 受控失败。
        """

        with self._lock:
            if self._handle is None:
                return
            handle = self._handle
            self._handle = None
            result = int(self._bridge._library.bt_huaxin_destroy(handle))
            self._bridge._raise_for_result(result, "destroy")

    def __enter__(self) -> "NativeRuntime":
        """
        进入 runtime 上下文。

        参数:
            无。
        返回:
            当前 NativeRuntime。
        """

        self._require_handle()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        """
        离开上下文并确保 native handle 被释放。

        参数:
            exc_type: 上下文内异常类型或 None。
            exc_value: 上下文内异常实例或 None。
            traceback: 上下文内异常 traceback 或 None。
        返回:
            无返回值，不吞掉上下文内异常。
        副作用:
            调用 close 释放 native handle。
        """

        self.close()
