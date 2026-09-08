#!/usr/bin/env python3
"""采集已运行大 QMT 服务的公开历史行情接口，不进行数值验收。

作者：BruceLee
职责：固定两证券、三种复权及八种周期的请求，保留完整公开返回值和时间轴。
输入：命令行 host/port、环境变量或显式 env 文件中的 QMT_SERVER_HOST/PORT/TOKEN，
以及可选 QMT_SERVER_TLS_CERT；token 不接受命令行参数，也不写入输出。
输出：独占新建的严格 JSON 和安全摘要；captured 只表示结构可读，不表示算法通过。
上下游：公开 data.api.get_price → RemoteQmtProvider → 现有 TCP data.history；
只建立本进程的客户端连接，不启动服务、策略或引擎，不订阅行情、不交易、不写库。
环境：从待验证 Git 工作树执行 PYTHONPATH=. python scripts/probe_big_qmt_history_contract.py
--env-file /配置路径 --output /已存在目录/新文件.json。脚本修改本进程的数据源、上下文、
现有 use_real_price 选项并静默库日志；仅用于独立进程，退出时关闭自己的 TCP 连接。
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import math
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

__all__ = ["build_cases", "capture_case", "main"]

_REFERENCE = "2026-09-07 15:00:00"
_FIELDS = ["open", "high", "low", "close", "volume", "money"]
_FREQUENCIES = ("1d", "1m", "5m", "15m", "30m", "60m", "1w", "1mon")
_ERROR_CODES = frozenset(
    {
        "BAD_REQUEST",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "AUTH_FAILED",
        "UNAUTHORIZED",
        "QMT_API_NOT_READY",
        "HISTORY_FAILED",
        "SPLIT_DIVIDEND_FAILED",
        "SPLIT_DIVIDEND_INVALID_DATA",
        "BIG_QMT_GATEWAY_ERROR",
        "BIG_QMT_GATEWAY_UNAVAILABLE",
        "BIG_QMT_GATEWAY_TIMEOUT",
    }
)


def build_cases():
    """构造固定采集矩阵；无输入，返回 48 个主窗口和 6 个同基准重叠窗口，无副作用。"""
    cases = []
    for security, start, end in (
        ("000001.XSHE", "2026-06-11", "2026-06-12"),
        ("510500.XSHG", "2026-07-14", "2026-07-15"),
    ):
        for fq in ("none", "pre", "post"):
            for frequency in _FREQUENCIES:
                calendar = frequency in ("1w", "1mon")
                daily = frequency == "1d"
                fields = _FIELDS + (
                    ["factor", "pre_close", "paused"] if frequency in ("1d", "1m") else []
                )
                request = {
                    "security": security,
                    "start_date": None if calendar else (start if daily else start + " 09:30:00"),
                    "end_date": (
                        "2026-05-03 15:00:00" if calendar else (end if daily else end + " 15:00:00")
                    ),
                    "frequency": frequency,
                    "fields": fields,
                    "fq": fq,
                    "count": 1 if calendar else None,
                    "skip_paused": False,
                    "fill_paused": True,
                    "panel": True,
                }
                cases.append({"id": f"{security}-{fq}-{frequency}", "request": request})
            overlap = dict(cases[-len(_FREQUENCIES)]["request"])
            overlap.update(start_date=None, count=1)
            cases.append({"id": f"{security}-{fq}-1d-overlap", "request": overlap})
    return cases


def _safe_error(exc):
    """提取安全异常摘要；输入异常，返回类型和白名单代码，不读取或保存异常正文。"""
    code = getattr(exc, "code", None)
    return {
        "type": type(exc).__name__,
        "code": code if isinstance(code, str) and code in _ERROR_CODES else "UNCLASSIFIED",
    }


def _strict_values(value):
    """规范 wire 中非有限数；输入嵌套值，返回严格 JSON 值，NaN 为 null、无穷带标签。"""
    if isinstance(value, float) and not math.isfinite(value):
        return None if math.isnan(value) else {"invalid_number": "+inf" if value > 0 else "-inf"}
    if isinstance(value, dict):
        return {key: _strict_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_strict_values(item) for item in value]
    return value


def capture_case(case, get_price, encode_frame):
    """采集一个公开请求；输入案例、公开函数和既有 wire 编码器，返回完整结果及结构状态。

    调用会读取远端行情；空表、非数值、非有限值或不唯一/无序时间轴不计 captured。
    不对齐、补值、舍入或比较价格；异常正文不进入结果。公开 API 吞掉的异常按空表记录。
    """
    import numpy as np
    import pandas as pd

    output = {"id": case["id"], "request": case["request"]}
    started = time.monotonic()
    try:
        frame = get_price(**case["request"])
        if not isinstance(frame, pd.DataFrame):
            output.update(
                status="invalid", error={"type": "InvalidResult", "code": "NOT_DATAFRAME"}
            )
        else:
            output["rows"] = len(frame)
            output["result"] = _strict_values(encode_frame(frame))
            json.dumps(output["result"], allow_nan=False)
            issues = []
            if list(frame.columns) != case["request"]["fields"]:
                issues.append("UNEXPECTED_COLUMNS")
            if not isinstance(frame.index, pd.DatetimeIndex) or frame.index.hasnans:
                issues.append("INVALID_TIME_INDEX")
            elif not frame.index.is_unique or not frame.index.is_monotonic_increasing:
                issues.append("UNORDERED_OR_DUPLICATE_INDEX")
            if not all(pd.api.types.is_numeric_dtype(dtype) for dtype in frame.dtypes):
                issues.append("NONNUMERIC_DATA")
            elif not frame.empty and not np.isfinite(frame.to_numpy(dtype=float)).all():
                issues.append("NONFINITE_DATA")
            output["status"] = "empty" if frame.empty else ("invalid" if issues else "captured")
            output["issues"] = (["EMPTY_RESULT"] if frame.empty else []) + issues
    except Exception as exc:
        output.pop("result", None)
        output.update(status="error", error=_safe_error(exc))
    output["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return output


@contextlib.contextmanager
def _quiet_library():
    """静默本进程库日志及标准流；无输入，返回上下文，结束时恢复，不影响远端日志。

    导入前装 NullHandler，避免默认策略日志创建 app.log；已存在日志也全局暂时禁用，
    防止公开 API 和连接后台线程把远端异常正文、会话或端点输出。
    """
    logger = logging.getLogger("jq_strategy")
    previous_disabled = logger.disabled
    previous_level = logging.root.manager.disable
    temporary = logging.NullHandler() if not logger.handlers else None
    if temporary is not None:
        logger.addHandler(temporary)
    logger.disabled = True
    logging.disable(logging.CRITICAL)
    try:
        with open(os.devnull, "w") as sink, contextlib.redirect_stdout(
            sink
        ), contextlib.redirect_stderr(sink):
            yield
    finally:
        logger.disabled = previous_disabled
        logging.disable(previous_level)
        if temporary is not None:
            logger.removeHandler(temporary)


def _connection_config(args):
    """读取白名单连接配置；输入 CLI 参数，返回配置，缺项/无效端口抛异常且不输出值。

    host/port 优先 CLI，其次进程环境，最后显式 env 文件；token 仅后两者。
    env 文件不整体注入进程，不输出路径和连接地址；读取本地配置是唯一副作用。
    """
    values = {}
    if args.env_file:
        from dotenv import dotenv_values

        env_path = Path(args.env_file)
        if not env_path.is_file():
            raise FileNotFoundError()
        values = dotenv_values(env_path, interpolate=False)
    config = {}
    for key, suffix in (
        ("host", "HOST"),
        ("port", "PORT"),
        ("token", "TOKEN"),
        ("tls_cert", "TLS_CERT"),
    ):
        name = "QMT_SERVER_" + suffix
        config[key] = os.environ.get(name) or values.get(name)
    config["host"] = args.host or config["host"]
    config["port"] = args.port if args.port is not None else config["port"]
    if not all(config[key] for key in ("host", "port", "token")):
        raise ValueError()
    config["port"] = int(config["port"])
    if not 1 <= config["port"] <= 65535:
        raise ValueError()
    return config


def _source_info(modules):
    """记录本地实际源码；输入已加载模块，返回绝对路径、文件 SHA256 和 Git HEAD。

    仅读本地文件及 git rev-parse，不联网；这些信息不能证明远端进程加载版本。
    """
    paths = [Path(__file__).resolve()] + [Path(module.__file__).resolve() for module in modules]
    files = [
        {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in paths
    ]
    result = subprocess.run(
        ["git", "-C", str(paths[0].parent.parent), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    head = result.stdout.strip()
    return {
        "files": files,
        "git_head": head
        if len(head) == 40 and all(c in "0123456789abcdef" for c in head)
        else None,
        "scope": "local_client_only; remote_loaded_version_not_verified",
    }


def _collect(config, progress_stream=None):
    """运行固定公开 API 采集；输入连接配置及可选进度流，返回安全报告，finally 仅关自身 TCP。

    独立进程中设置固定上下文和已有 use_real_price 选项；不创建回测会话或执行策略，
    不调用 provider 的订阅/关闭便利方法。初始化异常向 main 传播但不输出正文。
    每案例只向显式进度流写固定案例 ID、状态和行数，不受库日志静默影响，不带端点或错误正文。
    """
    from bullet_trade.core.settings import set_option
    from bullet_trade.data import api
    from bullet_trade.data.backtest_session import get_current_backtest_data_session
    from bullet_trade.data.providers import remote_qmt
    from bullet_trade.remote import connection
    from bullet_trade.server.adapters.qmt import dataframe_to_payload

    if get_current_backtest_data_session() is not None:
        raise RuntimeError()
    source = _source_info([api, remote_qmt, connection])
    api.set_current_context(SimpleNamespace(current_dt=datetime.fromisoformat(_REFERENCE)))
    set_option("use_real_price", True)
    provider = remote_qmt.RemoteQmtProvider(config)
    try:
        api.set_data_provider(provider)
        cases = []
        for case in build_cases():
            result = capture_case(case, api.get_price, dataframe_to_payload)
            cases.append(result)
            if progress_stream is not None:
                print(
                    json.dumps({key: result.get(key) for key in ("id", "status", "rows")}),
                    file=progress_stream,
                    flush=True,
                )
        return {
            "schema": "big-qmt-public-history-capture/v1",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "reference_intent": {
                "source": "public_context.current_dt",
                "current_dt": _REFERENCE,
                "timezone": "Asia/Shanghai",
                "use_real_price": True,
                "pre_factor_ref_date": _REFERENCE[:10],
            },
            "acceptance": "capture_only; numeric_algorithm_not_verified",
            "cases": cases,
        }
    finally:
        provider._connection.close()


def main(argv=None):
    """解析 CLI 并独占写结果；输入参数列表或 None，返回 0=全采集、1=有失败、2=启动/写出失败。

    写文件前先用 x 模式保留新路径，已有文件绝不覆盖也不连接远端；不创建父目录。
    所有错误只输出安全类型/代码。成功退出不代表数值、远端版本或业务验收通过。
    """
    parser = argparse.ArgumentParser(description="只读采集现有大 QMT 的公开历史接口")
    parser.add_argument("--env-file")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    progress_stream = sys.stdout
    try:
        with open(args.output, "x", encoding="utf-8") as output:
            with _quiet_library():
                try:
                    report = _collect(_connection_config(args), progress_stream=progress_stream)
                except Exception as exc:
                    report = {
                        "schema": "big-qmt-public-history-capture/v1",
                        "cases": [],
                        "fatal_error": _safe_error(exc),
                    }
            text = json.dumps(report, ensure_ascii=False, allow_nan=False, indent=2)
            output.write(text + "\n")
    except Exception as exc:
        print(json.dumps({"status": "fatal", "error": _safe_error(exc)}, ensure_ascii=False))
        return 2
    counts = dict(Counter(case["status"] for case in report["cases"]))
    summary = {"acceptance": "capture_only", "counts": counts}
    if "fatal_error" in report:
        summary["fatal_error"] = report["fatal_error"]
    print(json.dumps(summary, ensure_ascii=False, allow_nan=False))
    return 2 if "fatal_error" in report else int(counts.get("captured", 0) != len(report["cases"]))


if __name__ == "__main__":
    sys.exit(main())
