"""
作者: BruceLee
文件职责: 提供 BulletTrade 第一方华鑫集成的纯 Python 公共入口。
主要输入: 调用方对 doctor、显式构建或 native wrapper 类型的按需导入。
主要输出: 稳定错误类型、离线诊断、构建函数和显式 native loader。
上游关系: CLI、测试和未来 Huaxin Broker/Realtime Feed 使用本入口。
下游关系: errors.py、build.py、native.py；当前切片不连接 LiveEngine。
关键环境或配置: 普通 import 不得调用编译器、ctypes.CDLL、网络或厂商 SDK。
"""

from .build import BuildResult, DoctorReport, build_native_bridge, doctor, verify_bundle
from .errors import (
    HuaxinAbiError,
    HuaxinBuildError,
    HuaxinBundleError,
    HuaxinError,
    HuaxinNativeCallError,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)
from .native import ABI_VERSION, NativeBridge, NativeEvent, NativeHealth, NativeRuntime

__all__ = [
    "ABI_VERSION",
    "BuildResult",
    "DoctorReport",
    "HuaxinAbiError",
    "HuaxinBuildError",
    "HuaxinBundleError",
    "HuaxinError",
    "HuaxinNativeCallError",
    "HuaxinNativeUnavailableError",
    "HuaxinTradingDisabledError",
    "NativeBridge",
    "NativeEvent",
    "NativeHealth",
    "NativeRuntime",
    "build_native_bridge",
    "doctor",
    "verify_bundle",
]
