#!/usr/bin/env python
"""easy_tdx 与聚宽固定历史窗口验收。

作者: BruceLee
日期: 2026-09-29
职责: 对相同证券和固定日期逐行比较原始、前复权、后复权，保存可重放证据。
输入: 仓库外 env 中的聚宽连接信息、输出目录；easy_tdx 自动选主机。
输出: JSON 差异摘要和每组原始 CSV；非零退出码表示至少一项未通过。
上下游: 两个只读 provider，不导入账户、策略执行或信号发布路径。
环境: Python 3.10+、easy-tdx、jqdatasdk；EASY_TDX_CONFIG_DIR 应指向隔离目录。
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from datetime import datetime
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import dotenv_values

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from bullet_trade.data.providers.jqdata import JQDataProvider

SYMBOLS = ("000001.XSHE", "600519.XSHG", "510050.XSHG")
FIELDS = ["open", "high", "low", "close", "volume", "money"]
START = "2025-06-02"
END = "2025-06-30"
REFERENCE = "2025-06-30"


def compare_frames(reference, observed, security, mode):
    """比较两个 DataFrame；输入基准、观测、证券与模式，返回缺口/误差，无副作用。"""
    missing = reference.index.difference(observed.index)
    extra = observed.index.difference(reference.index)
    common = reference.index.intersection(observed.index)
    result = {
        "reference_rows": len(reference),
        "observed_rows": len(observed),
        "missing_dates": [str(x) for x in missing],
        "extra_dates": [str(x) for x in extra],
        "fields": {},
    }
    fields_ok = True
    price_ok = True
    for field in FIELDS:
        if field not in reference or field not in observed or not len(common):
            result["fields"][field] = {"ok": False, "reason": "missing_field_or_rows"}
            fields_ok = False
            if field in FIELDS[:4]:
                price_ok = False
            continue
        left = reference.loc[common, field].astype(float)
        right = observed.loc[common, field].astype(float)
        diff = (left - right).abs()
        base = 0.001 if security.startswith("51") else 0.01
        # 阈值预先固定；价格与全部字段分别验收，不能把价格通过称为数据完全对齐。
        tolerance = base + (left.abs() * 1e-4 if mode != "raw" else 0.0)
        if field == "volume":
            tolerance = np.maximum(100.0, left.abs() * 1e-5)
        elif field == "money":
            tolerance = np.maximum(100.0, left.abs() * 1e-5)
        ok = bool(
            np.isfinite(left).all() and np.isfinite(right).all() and (diff <= tolerance).all()
        )
        result["fields"][field] = {
            "ok": ok,
            "max_abs_diff": float(diff.max()),
            "worst_date": str(diff.idxmax()),
        }
        fields_ok = fields_ok and ok
        if field in FIELDS[:4]:
            price_ok = price_ok and ok
    result["ok"] = bool(len(reference) and not len(missing) and not len(extra) and fields_ok)
    result["price_ok"] = bool(len(reference) and not len(missing) and not len(extra) and price_ok)
    return result


def main():
    """读取 CLI 与凭据执行有界只读对账；返回退出码，副作用仅本地证据文件和网络读取。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--rpc-env-file", help="显式使用聚宽 RPC；按单连接顺序查询")
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    env = dotenv_values(args.env_file, interpolate=False)
    jq = JQDataProvider({"cache_dir": None})
    tdx = EasyTdxProvider({"timeout": 5.0, "use_stub": False})
    rpc = None
    report = {
        "generated_at": datetime.now().isoformat(),
        "easy_tdx": version("easy-tdx"),
        "window": [START, END],
        "reference_date": REFERENCE,
        "cases": [],
    }
    try:
        if args.rpc_env_file:
            sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
            from research.live.catboost_live.rpc_proxy import RPCProxy

            rpc_env = dotenv_values(args.rpc_env_file, interpolate=False)
            with contextlib.redirect_stderr(io.StringIO()):
                rpc = RPCProxy(
                    rpc_env["JQ_PROXY_HOST"],
                    int(rpc_env["JQ_PROXY_PORT"]),
                    rpc_env["JQ_PROXY_AUTHKEY"],
                    recv_timeout=15,
                    max_retries=1,
                )
            jq = rpc
            report["reference_source"] = "JoinQuant RPC"
            report["reference_date"] = "RPC 默认当日参考日，与 easy_tdx 默认参考日一致"
        # SDK 鉴权 stdout 可能包含端点，禁止写入报告或终端。
        if rpc is None:
            with contextlib.redirect_stdout(io.StringIO()):
                jq.auth(
                    user=env.get("JQDATA_USERNAME"),
                    pwd=env.get("JQDATA_PASSWORD"),
                    host=env.get("JQDATA_SERVER"),
                    port=int(env.get("JQDATA_PORT") or 0) or None,
                )
        tdx.auth()
        report["selected_host"] = getattr(tdx._client, "_host", None)
        for symbol in SYMBOLS:
            for mode, fq in (("raw", None), ("pre", "pre"), ("post", "post")):
                started = time.monotonic()
                case = {"security": symbol, "mode": mode}
                try:
                    kwargs = dict(
                        start_date=START, end_date=END, fields=FIELDS, frequency="daily", fq=fq
                    )
                    if mode == "pre" and rpc is None:
                        kwargs["pre_factor_ref_date"] = REFERENCE
                    left = jq.get_price(symbol, **kwargs)
                    if isinstance(left, Exception):
                        raise left
                    right = tdx.get_price(symbol, **kwargs)
                    left.to_csv(output / f"{symbol}_{mode}_jq.csv")
                    right.to_csv(output / f"{symbol}_{mode}_tdx.csv")
                    case.update(compare_frames(left, right, symbol, mode))
                except Exception as exc:
                    case.update(ok=False, error_type=type(exc).__name__)
                case["seconds"] = round(time.monotonic() - started, 3)
                report["cases"].append(case)
                print(json.dumps(case, ensure_ascii=False), flush=True)
                (output / "report.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2)
                )
        from easy_tdx.codec.bitmap import FieldBit, PresetField

        selected = list(PresetField.COMMON.value) + [
            FieldBit.SERVER_UPDATE_DATE,
            FieldBit.SERVER_UPDATE_TIME,
        ]
        raw = tdx._client.get_stock_quotes([(0, "000001"), (1, "510050")], fields=selected)
        report["sdk_quote_time_fields"] = raw.reindex(
            columns=["code", "server_update_date", "server_update_time"]
        ).to_dict("records")
    except Exception as exc:
        report["fatal_error_type"] = type(exc).__name__
        print("probe_error=" + type(exc).__name__, flush=True)
    finally:
        if tdx._client is not None:
            tdx._client.close()
        if rpc is not None:
            rpc.close()
        (output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str)
        )
    return (
        0
        if len(report["cases"]) == 9
        and all(x["ok"] for x in report["cases"])
        and "fatal_error_type" not in report
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
