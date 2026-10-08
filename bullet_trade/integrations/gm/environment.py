"""
作者: BruceLee
文件职责: 检查掘金 SDK 环境，并在显式请求时隔离验证 gm.api 导入。
主要输入: 当前 Python/平台、gm 安装元信息、配置是否存在、SDK 导入超时。
主要输出: 脱敏诊断字典；不读取或输出真实账户、token 和服务地址的值。
上下游关系: gm doctor 调用本模块；本模块仅依赖标准库和可选 SDK。
关键配置: 默认不加载 SDK；显式导入探针不调用 run、认证、数据或交易函数。
"""

from __future__ import annotations

import json
import os
import platform
import re
import struct
import subprocess
import sys
from importlib import metadata
from typing import Any, Dict

# 子进程输出只包含导入结果；SDK 自己的 stdout/stderr 不向用户透传。
_IMPORT_PROBE = """
import contextlib
import importlib
import io
import json

try:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        api = importlib.import_module('gm.api')
    required = ('run', 'set_token', 'history', 'current', 'order_volume', 'order_cancel')
    missing = [name for name in required if not callable(getattr(api, name, None))]
    result = {'status': 'missing_api' if missing else 'ok', 'missing_api': missing}
except Exception:
    result = {'status': 'import_failed'}
print('BT_GM_PROBE=' + json.dumps(result), flush=True)
"""


def _check_sdk_import(timeout: float) -> Dict[str, Any]:
    """在同一解释器的子进程中显式检查 SDK 导入。

    参数:
        timeout: 子进程运行的最大秒数，必须为正数。
    返回:
        导入状态字典，异常和 SDK 输出不包含在结果中。
    副作用:
        启动并等待子进程；仅导入 gm.api，不执行认证或业务 API。
    """

    try:
        completed = subprocess.run(
            [sys.executable, "-c", _IMPORT_PROBE],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    except OSError:
        return {"status": "process_failed"}
    if completed.returncode != 0:
        return {"status": "process_failed"}
    for line in reversed(completed.stdout.splitlines()):
        if line.startswith("BT_GM_PROBE="):
            try:
                result = json.loads(line.split("=", 1)[1])
            except (ValueError, TypeError):
                break
            if isinstance(result, dict) and result.get("status") in {
                "ok",
                "missing_api",
                "import_failed",
            }:
                # 白名单字段，避免把第三方异常或输出带入诊断。
                return {
                    "status": result["status"],
                    "missing_api": result.get("missing_api", []),
                }
    return {"status": "invalid_probe_result"}


def doctor(load_sdk: bool = False, timeout: float = 10.0) -> Dict[str, Any]:
    """收集当前环境信息，区分 SDK 安装、导入和业务连接状态。

    参数:
        load_sdk: 是否显式在子进程中导入 SDK，默认只检查安装元信息。
        timeout: 导入探针的超时秒数，必须为正数。
    返回:
        可 JSON 序列化的脱敏诊断；终端和账户连接始终标记为未验证。
    异常:
        ValueError: 超时不是有效正数。
    副作用:
        load_sdk 为真且当前平台有安装包、SDK 已安装时才启动导入子进程。
    """

    if not 0 < timeout < float("inf"):
        raise ValueError("SDK 导入超时必须是有限的正数")
    system = platform.system()
    machine = platform.machine().lower()
    bits = struct.calcsize("P") * 8
    python_version = sys.version_info[:2]
    python_supported = (3, 8) <= python_version <= (3, 14)
    architecture_supported = (bits == 64 and machine in {"amd64", "x86_64"}) or (
        system == "Windows" and bits == 32 and python_version <= (3, 11)
    )
    sdk_platform_supported = system in {"Windows", "Linux"} and architecture_supported
    try:
        sdk_version = metadata.version("gm")
    except metadata.PackageNotFoundError:
        sdk_version = None
    sdk_installed = sdk_version is not None
    version_match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", sdk_version or "")
    sdk_version_supported = bool(
        version_match and (3, 0, 186) <= tuple(map(int, version_match.groups())) < (3, 1, 0)
    )
    environment_available = (
        sdk_platform_supported and python_supported and sdk_installed and sdk_version_supported
    )
    sdk_import = {"status": "not_requested"}
    if load_sdk:
        sdk_import = (
            _check_sdk_import(timeout) if environment_available else {"status": "unavailable"}
        )
    return {
        "integration": "gm",
        "stage": "environment_preparation",
        "python": platform.python_version(),
        "platform": system,
        "architecture": machine,
        "bits": bits,
        "python_supported": python_supported,
        "sdk_platform_supported": sdk_platform_supported,
        "sdk_installed": sdk_installed,
        "sdk_version": sdk_version,
        "sdk_version_supported": sdk_version_supported,
        "environment_available": environment_available,
        "sdk_import": sdk_import,
        "configuration_present": {
            key: bool(os.environ.get(key, "").strip())
            for key in ("GM_TOKEN", "GM_STRATEGY_ID", "GM_ACCOUNT_ID", "GM_SERV_ADDR")
        },
        "terminal_connection": "not_checked",
        "account_connection": "not_checked",
        "data_provider_implemented": True,
        "broker_implemented": True,
        "server_adapter_implemented": False,
    }


__all__ = ["doctor"]
