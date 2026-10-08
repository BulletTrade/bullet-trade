"""
作者: BruceLee
文件职责: 验证并隔离执行掘金 SDK 只读连接探针。
主要输入: 明确的证券和日期范围、是否查询账户、GM 私密环境变量。
主要输出: 脱敏结构化状态，不把 SDK 输出、异常详情或凭据带回 CLI。
上下游关系: gm CLI 调用本模块；本模块启动 probe_worker。
关键配置: 凭据仅通过子进程标准输入传递；整体查询有有限超时。
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import date
from typing import Any, Dict

from .environment import doctor


def probe(
    symbol: str, start: str, end: str, account: bool = False, timeout: float = 30
) -> Dict[str, Any]:
    """在显式请求时验证行情以及可选账户的只读连接。

    参数:
        symbol: SHSE/SZSE 格式的 A 股或 ETF 代码。
        start/end: 日线查询日期，最多 31 个自然日。
        account: 是否用 GM_ACCOUNT_ID 显式查询资金和持仓。
        timeout: 子进程最大运行秒数。
    返回:
        脱敏查询结果；失败时 ok 为 False。
    异常:
        ValueError: 查询参数非法。
    副作用:
        必要配置和 SDK 平台通过后，启动短时联网子进程。
    """

    if not re.fullmatch(r"(?:SHSE|SZSE)\.\d{6}", symbol):
        raise ValueError("请指定 SHSE/SZSE 六位证券代码")
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if not 0 <= (last - first).days <= 30:
        raise ValueError("日期范围必须按先后顺序，最多 31 个自然日")
    if not 0 < timeout <= 120:
        raise ValueError("探针超时必须大于 0 且不超过 120 秒")
    required = ["GM_TOKEN"] + (["GM_ACCOUNT_ID"] if account else [])
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        return {"ok": False, "status": "configuration_missing", "missing": missing}
    if not doctor()["environment_available"]:
        return {"ok": False, "status": "environment_unavailable"}
    request = {
        "symbol": symbol,
        "start": first.isoformat(),
        "end": last.isoformat(),
        "token": os.environ["GM_TOKEN"],
        "serv_addr": os.environ.get("GM_SERV_ADDR", ""),
        "account_id": os.environ["GM_ACCOUNT_ID"] if account else "",
    }
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "bullet_trade.integrations.gm.probe_worker"],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "status": "timeout"}
    except OSError:
        return {"ok": False, "status": "process_failed"}
    if completed.returncode != 0:
        return {"ok": False, "status": "process_failed"}
    for line in reversed(completed.stdout.splitlines()):
        if not line.startswith("BT_GM_READONLY="):
            continue
        try:
            value = json.loads(line.split("=", 1)[1])
            if isinstance(value, dict) and isinstance(value.get("ok"), bool):
                return value
        except (ValueError, TypeError):
            pass
        break
    return {"ok": False, "status": "invalid_probe_result"}
