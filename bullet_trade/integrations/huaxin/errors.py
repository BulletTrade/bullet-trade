"""
作者: BruceLee
文件职责: 定义华鑫第一方集成的稳定错误码和异常边界。
主要输入: native 构建、制品校验、ABI 加载与调用阶段的失败信息。
主要输出: 可供 CLI、doctor 和未来运行时统一处理的结构化异常。
上游关系: build.py、native.py 和 cli.py 在受控失败时创建这些异常。
下游关系: 调用方通过 code/details 判断 readiness，不解析易变的错误文本。
关键环境或配置: details 不得包含密码、TerminalInfo、柜台地址或未脱敏 SDK 路径。
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

HUAXIN_NATIVE_UNAVAILABLE = "HUAXIN_NATIVE_UNAVAILABLE"
BRIDGE_BUNDLE_MISSING = "BRIDGE_BUNDLE_MISSING"
BRIDGE_BUNDLE_INVALID = "BRIDGE_BUNDLE_INVALID"
BUILD_FINGERPRINT_MISMATCH = "BUILD_FINGERPRINT_MISMATCH"
BRIDGE_ARTIFACT_HASH_MISMATCH = "BRIDGE_ARTIFACT_HASH_MISMATCH"
NATIVE_ABI_INCOMPATIBLE = "NATIVE_ABI_INCOMPATIBLE"
VENDOR_SCHEMA_INCOMPATIBLE = "VENDOR_SCHEMA_INCOMPATIBLE"
NATIVE_CALL_FAILED = "NATIVE_CALL_FAILED"
BUILD_TOOL_MISSING = "BUILD_TOOL_MISSING"
BUILD_FAILED = "BUILD_FAILED"
BUILD_PREFIX_UNSAFE = "BUILD_PREFIX_UNSAFE"
SDK_BUILD_NOT_IMPLEMENTED = "SDK_BUILD_NOT_IMPLEMENTED"
OFFLINE_FAKE_ONLY = "OFFLINE_FAKE_ONLY"
HUAXIN_BACKEND_NOT_IMPLEMENTED = "HUAXIN_BACKEND_NOT_IMPLEMENTED"
HUAXIN_TRADING_DISABLED = "HUAXIN_TRADING_DISABLED"
HUAXIN_CANCEL_DISABLED = "HUAXIN_CANCEL_DISABLED"


class HuaxinError(RuntimeError):
    """华鑫集成的基础结构化异常，保存稳定错误码和脱敏详情。"""

    def __init__(
        self,
        code: str,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        创建结构化华鑫异常。

        参数:
            code: 供程序判断的稳定错误码。
            message: 面向操作员的中文错误说明。
            details: 不含敏感值的补充诊断字段。
        返回:
            无返回值；初始化当前异常对象。
        副作用:
            初始化 RuntimeError 的可读文本。
        """

        self.code = code
        self.message = message
        self.details: Dict[str, Any] = dict(details or {})
        super().__init__(f"{code}: {message}")

    def to_dict(self) -> Dict[str, Any]:
        """
        把异常转换为可安全序列化的字典。

        参数:
            无。
        返回:
            包含 code、message 和脱敏 details 的字典。
        """

        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


class HuaxinNativeUnavailableError(HuaxinError):
    """表示华鑫 native bridge 或其受控前置条件尚未就绪。"""


class HuaxinBundleError(HuaxinError):
    """表示 native bundle 缺失、格式错误或完整性校验失败。"""


class HuaxinBuildError(HuaxinError):
    """表示显式 native 构建阶段发生受控失败。"""


class HuaxinAbiError(HuaxinError):
    """表示 Python wrapper 与 native C ABI 不兼容。"""


class HuaxinNativeCallError(HuaxinError):
    """表示 fake/offline native C ABI 调用返回稳定非零错误码。"""


class HuaxinTradingDisabledError(HuaxinError):
    """表示华鑫交易或撤单被默认关闭的本地硬门禁拒绝。"""
