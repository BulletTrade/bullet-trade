"""作者: BruceLee
职责: 隔离 GM 原生 SDK 的只读数据请求，凭据仅经 stdin 进入有界子进程。
输入: GM 配置、白名单方法及参数；输出: JSON 数据或脱敏异常。
上下游: GmDataProvider 调用本客户端；SDK 自身退出和日志不影响策略主进程。
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

DATA_METHODS = frozenset(
    {
        "history",
        "history_n",
        "current",
        "get_trading_dates",
        "get_instrumentinfos",
        "get_history_instruments",
        "stk_get_index_constituents",
        "get_dividend",
        "get_symbol_infos",
    }
)


class GmDataError(RuntimeError):
    """SDK 连接、权限、覆盖和数据格式错误；不包含 SDK 原始异常或私密配置。"""


class GmDataClient:
    """按需启动短时 worker，不启动终端、策略、账户或交易连接。"""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._token = config.get("token") or os.environ.get("GM_TOKEN", "")
        self._serv_addr = config.get("serv_addr") or os.environ.get("GM_SERV_ADDR", "")
        self._python = config.get("python_executable") or sys.executable
        self._timeout = float(config.get("timeout") or 30)
        if not math.isfinite(self._timeout) or not 0 < self._timeout <= 120:
            raise ValueError("GM 数据查询超时必须为 0 至 120 秒之间的有限数")

    def auth(self) -> None:
        """检查必要配置；连接与权限在首次只读查询时由 worker 实际验证。"""
        if not self._token:
            raise GmDataError("GM 数据源需要配置 GM_TOKEN")

    def query(self, method: str, **kwargs: Any) -> Any:
        """执行单次有界读取；不重试权限/参数错误，不把错误变成空行情。"""
        if method not in DATA_METHODS:
            raise NotImplementedError("GM 数据客户端仅支持明确的数据读取方法")
        self.auth()
        request = dict(token=self._token, serv_addr=self._serv_addr, method=method, kwargs=kwargs)
        source = Path(__file__).with_name("data_worker.py").read_text(encoding="utf8")
        try:
            proc = subprocess.run(
                [self._python, "-c", source],
                input=json.dumps(request, default=str),
                text=True,
                encoding="utf8",
                errors="replace",
                capture_output=True,
                timeout=self._timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise GmDataError("GM 数据查询超时") from None
        except OSError:
            raise GmDataError("无法启动 GM SDK 数据 worker") from None
        lines = [x for x in proc.stdout.splitlines() if x.startswith("BT_GM_DATA=")]
        if proc.returncode != 0 or len(lines) != 1:
            raise GmDataError("GM 数据 worker 未正常完成")
        try:
            result = json.loads(lines[0].split("=", 1)[1])
        except (ValueError, TypeError):
            raise GmDataError("GM 数据 worker 响应格式错误") from None
        if result.get("status") != "ok":
            code = result.get("error_code")
            safe_code = str(code) if isinstance(code, int) else "unknown"
            raise GmDataError("GM 数据查询失败，错误码 " + safe_code)
        return result["data"]
