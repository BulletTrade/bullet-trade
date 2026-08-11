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
    HuaxinAbiError,
    HuaxinNativeCallError,
    HuaxinNativeUnavailableError,
)

ABI_VERSION = 1
EVENT_PAYLOAD_CAPACITY = 192
MAX_DRAIN_EVENTS = 4096


class _CreateOptions(ctypes.Structure):
    """映射 C ABI 的 runtime 创建参数结构。"""

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("queue_capacity", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class _Health(ctypes.Structure):
    """映射 C ABI 的离线 health 结构。"""

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("state", ctypes.c_int32),
        ("queue_capacity", ctypes.c_uint32),
        ("queue_size", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("dropped_events", ctypes.c_uint64),
    ]


class _Event(ctypes.Structure):
    """映射 C ABI 的固定容量事件结构。"""

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("event_type", ctypes.c_uint32),
        ("payload_size", ctypes.c_uint32),
        ("sequence", ctypes.c_uint64),
        ("received_ns", ctypes.c_int64),
        ("payload", ctypes.c_uint8 * EVENT_PAYLOAD_CAPACITY),
    ]


@dataclass(frozen=True)
class NativeHealth:
    """表示 fake/offline runtime 的不可变 health 快照。"""

    state: int
    queue_capacity: int
    queue_size: int
    dropped_events: int


@dataclass(frozen=True)
class NativeEvent:
    """表示从 native 有界队列 drain 出的自有事件副本。"""

    event_type: int
    sequence: int
    received_ns: int
    payload: bytes


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
    library.bt_huaxin_drain.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_Event),
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    library.bt_huaxin_drain.restype = ctypes.c_int32


class NativeBridge:
    """代表一个已通过 bundle 完整性校验并被显式加载的 C ABI bridge。"""

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
        return cls(artifact_path, library, manifest)

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
        return raw.decode("utf-8", errors="strict") if raw else ""

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
        if result in (-2, -3):
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native C ABI 版本或结构大小不兼容",
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
            )
            result = int(self._bridge._library.bt_huaxin_get_health(handle, ctypes.byref(raw)))
            self._bridge._raise_for_result(result, "health")
            return NativeHealth(
                state=int(raw.state),
                queue_capacity=int(raw.queue_capacity),
                queue_size=int(raw.queue_size),
                dropped_events=int(raw.dropped_events),
            )

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
            event_array = (_Event * max_events)()
            count = ctypes.c_uint32(0)
            result = int(
                self._bridge._library.bt_huaxin_drain(
                    handle,
                    event_array,
                    ctypes.c_uint32(max_events),
                    ctypes.byref(count),
                )
            )
            self._bridge._raise_for_result(result, "drain")
            events: List[NativeEvent] = []
            for index in range(int(count.value)):
                raw = event_array[index]
                payload_size = min(int(raw.payload_size), EVENT_PAYLOAD_CAPACITY)
                events.append(
                    NativeEvent(
                        event_type=int(raw.event_type),
                        sequence=int(raw.sequence),
                        received_ns=int(raw.received_ns),
                        payload=bytes(raw.payload[:payload_size]),
                    )
                )
            return events

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
