#!/usr/bin/env python3
"""Read-only probe for slow bullet-trade QMT data APIs.

This script does not call any broker/trading API. It checks TCP, handshake,
admin.health, and a small set of data APIs that are useful for isolating
network issues from MiniQMT/xtquant slowness.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from bullet_trade.remote import RemoteQmtConnection


def elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 3)


def shape(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        result: Dict[str, Any] = {"keys": sorted(str(k) for k in value.keys())[:30]}
        for key in ("records", "data", "values"):
            item = value.get(key)
            if isinstance(item, list):
                result[key + "_len"] = len(item)
        wrapped = value.get("value")
        if isinstance(wrapped, list):
            result["value_len"] = len(wrapped)
        elif isinstance(wrapped, dict):
            result["value_keys"] = sorted(str(k) for k in wrapped.keys())[:30]
        elif wrapped is not None:
            result["value_type"] = type(wrapped).__name__
        return result
    if isinstance(value, list):
        return {"list_len": len(value)}
    return {"type": type(value).__name__}


def tcp_probe(host: str, port: int, timeout: float) -> Dict[str, Any]:
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return {"ok": True, "elapsed_ms": elapsed_ms(start)}
    except Exception as exc:
        return {
            "ok": False,
            "elapsed_ms": elapsed_ms(start),
            "error": "%s: %s" % (type(exc).__name__, exc),
        }


def run_case(
    conn: RemoteQmtConnection,
    name: str,
    action: str,
    payload: Optional[Dict[str, Any]],
    timeout: float,
) -> Dict[str, Any]:
    start = time.perf_counter()
    try:
        response = conn.request(action, payload or {}, timeout=timeout)
        item = {
            "name": name,
            "action": action,
            "ok": True,
            "elapsed_ms": elapsed_ms(start),
            "shape": shape(response),
        }
    except Exception as exc:
        item = {
            "name": name,
            "action": action,
            "ok": False,
            "elapsed_ms": elapsed_ms(start),
            "error": "%s: %s" % (type(exc).__name__, exc),
        }
    extra = item.get("shape") or {"error": item.get("error")}
    print(
        "{:<24} ok={:<5} elapsed_ms={:<10} {}".format(
            item["name"],
            str(item["ok"]),
            item["elapsed_ms"],
            json.dumps(extra, ensure_ascii=False),
        ),
        flush=True,
    )
    return item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only slow data probe for bullet-trade QMT server.")
    parser.add_argument("--host", default=os.getenv("QMT_SERVER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("QMT_SERVER_PORT", "58620")))
    parser.add_argument("--token", default=os.getenv("QMT_SERVER_TOKEN", ""))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("QMT_PROBE_TIMEOUT", "130")))
    parser.add_argument("--security", default=os.getenv("QMT_PROBE_SECURITY", "000300.XSHG"))
    parser.add_argument("--output", default=os.getenv("QMT_PROBE_OUTPUT", ""))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.token:
        print("ERROR: missing token. Pass --token or set QMT_SERVER_TOKEN.", file=sys.stderr)
        return 2

    summary: Dict[str, Any] = {
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "host": args.host,
        "port": args.port,
        "timeout": args.timeout,
        "security": args.security,
        "tcp": None,
        "connect": None,
        "cases": [],
    }

    print("target=%s:%s timeout=%ss security=%s" % (args.host, args.port, args.timeout, args.security))
    tcp_result = tcp_probe(args.host, args.port, min(args.timeout, 10.0))
    summary["tcp"] = tcp_result
    print(
        "tcp_connect             ok={:<5} elapsed_ms={:<10} {}".format(
            str(tcp_result["ok"]),
            tcp_result["elapsed_ms"],
            json.dumps({"error": tcp_result.get("error")} if not tcp_result["ok"] else {}, ensure_ascii=False),
        )
    )
    if not tcp_result["ok"]:
        write_output(args.output, summary)
        return 1

    conn = RemoteQmtConnection(args.host, args.port, args.token, request_timeout=args.timeout)
    try:
        start = time.perf_counter()
        try:
            conn.start()
            summary["connect"] = {"ok": True, "elapsed_ms": elapsed_ms(start)}
            print("bt_handshake            ok=True  elapsed_ms=%s" % summary["connect"]["elapsed_ms"])
        except Exception as exc:
            summary["connect"] = {
                "ok": False,
                "elapsed_ms": elapsed_ms(start),
                "error": "%s: %s" % (type(exc).__name__, exc),
            }
            print("bt_handshake            ok=False elapsed_ms=%s %s" % (
                summary["connect"]["elapsed_ms"],
                summary["connect"]["error"],
            ))
            write_output(args.output, summary)
            return 1

        cases = [
            ("health", "admin.health", {}, min(args.timeout, 30.0)),
            ("security_info", "data.security_info", {"security": args.security}, min(args.timeout, 60.0)),
            ("trade_days", "data.trade_days", {"security": args.security, "count": 3}, min(args.timeout, 60.0)),
            ("all_securities_stock", "data.get_all_securities", {"types": ["stock"]}, args.timeout),
            ("all_securities_etf", "data.get_all_securities", {"types": ["etf"]}, args.timeout),
            ("index_stocks_000300", "data.get_index_stocks", {"index_symbol": "000300.XSHG"}, args.timeout),
            ("index_stocks_000905", "data.get_index_stocks", {"index_symbol": "000905.XSHG"}, args.timeout),
        ]
        for name, action, payload, timeout in cases:
            summary["cases"].append(run_case(conn, name, action, payload, timeout))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    write_output(args.output, summary)
    return 1 if any(not item.get("ok") for item in summary["cases"]) else 0


def write_output(output: str, summary: Dict[str, Any]) -> None:
    if not output:
        print("JSON_RESULT_BEGIN")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print("JSON_RESULT_END")
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote=%s" % path)


if __name__ == "__main__":
    raise SystemExit(main())
