"""执行有界验收请求的 SDK 白名单 worker。凭据来自 stdin，SDK 日志由父进程屏蔽。

该模块不导入策略、不调用 run 或任何交易写接口；账户请求必须显式指定 ID。
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict

ALLOWED = {
    "history",
    "history_n",
    "current",
    "get_trading_dates",
    "get_instrumentinfos",
    "get_history_instruments",
    "stk_get_index_constituents",
    "get_history_constituents",
    "get_dividend",
    "stk_get_dividend",
    "fnd_get_dividend",
    "stk_get_adj_factor",
    "fnd_get_adj_factor",
}


def account_queries(api: Any, account_id: str) -> Dict[str, Any]:
    """限定单个账户；原生委托/成交查询显式检查错误码，避开 SDK 默认全账户遍历。"""
    if not account_id:
        raise ValueError("需要明确账户 ID")
    from gm.api import trade as trade
    from gm.csdk.c_sdk import py_gmi_get_account_status
    from gm.pb.account_pb2 import AccountStatuses
    from gm.pb.tradegw_service_pb2 import GetAccountStatusesReq

    req = GetAccountStatusesReq()
    req.account_ids.append(account_id)
    code, data = py_gmi_get_account_status(req.SerializeToString())
    if code != 0:
        return {"status": "error", "status_code": code}
    response = AccountStatuses()
    response.ParseFromString(data)
    matches = [x for x in response.data if x.account_id == account_id]
    if len(matches) != 1:
        raise RuntimeError("未返回唯一账户状态")
    status = {"state": matches[0].status.state, "error_code": matches[0].status.error.code}
    if status["state"] != 3 or status["error_code"] != 0:
        return {"status": "not_logged_in", "connection": status}
    cash = api.get_cash(account_id=account_id)
    positions = api.get_position(account_id=account_id)
    if cash.get("account_id") != account_id:
        raise RuntimeError("资金记录账户与请求不一致")
    safe_cash = {
        k: cash[k]
        for k in [
            "nav",
            "balance",
            "available",
            "market_value",
            "frozen",
            "order_frozen",
            "cum_inout",
            "updated_at",
            "created_at",
        ]
        if k in cash
    }
    safe_positions = []
    for p in positions:
        if p.get("account_id") != account_id:
            raise RuntimeError("持仓记录账户与请求不一致")
        safe_positions.append(
            {k: p[k] for k in ["symbol", "volume", "available", "market_value"] if k in p}
        )
    queries = {}
    for name, req_type, fn, res_type in [
        ("orders", trade.GetOrdersReq, trade.py_gmi_get_orders, trade.Orders),
        (
            "unfinished_orders",
            trade.GetUnfinishedOrdersReq,
            trade.py_gmi_get_unfinished_orders,
            trade.Orders,
        ),
        (
            "execution_reports",
            trade.GetExecrptsReq,
            trade.py_gmi_get_execution_reports,
            trade.ExecRpts,
        ),
    ]:
        request = req_type()
        request.account_id = account_id
        code, payload = fn(request.SerializeToString())
        result = res_type()
        if code == 0 and payload:
            result.ParseFromString(payload)
        queries[name] = {"status_code": code, "rows": len(result.data) if code == 0 else None}
    return {
        "status": "ok",
        "connection": status,
        "cash": safe_cash,
        "positions": safe_positions,
        "queries": queries,
    }


def execute(api: Any, request: Dict[str, Any]) -> Dict[str, Any]:
    """运行严格白名单读取；异常不透传秘密，只保留 SDK 类型和结构化错误码。"""
    cases = request["cases"]
    if not cases or len(cases) > 60:
        raise ValueError("每批需要 1 至 60 个查询")
    if any(c["method"] not in ALLOWED | {"account_readonly"} for c in cases):
        raise ValueError("仅允许只读验收方法")
    if any(c["method"] == "account_readonly" for c in cases) and not request.get("account_id"):
        raise ValueError("需要明确账户 ID")
    if len({c["id"] for c in cases}) != len(cases):
        raise ValueError("查询 ID 不得重复")
    api.set_token(request["token"])
    api.set_serv_addr(request.get("serv_addr") or "127.0.0.1:7001")
    result: Dict[str, Any] = {"sdk_version": api.get_version(), "cases": {}}
    for case in cases:
        try:
            if case["method"] == "account_readonly":
                value = account_queries(api, request["account_id"])
            else:
                value = getattr(api, case["method"])(**case["kwargs"])
            if hasattr(value, "to_dict"):
                value = value.to_dict("records")
            result["cases"][case["id"]] = {"status": "ok", "data": value}
        except Exception as exc:
            # GmError.args 通常含 (status_code, detail)，只取数值，绝不透传 detail。
            codes = [v for v in exc.args if isinstance(v, int)]
            result["cases"][case["id"]] = {
                "status": "error",
                "error_type": type(exc).__name__,
                "error_code": codes[0] if codes else getattr(exc, "status", None),
                "reason": (
                    "permission_denied"
                    if any(
                        k in str(getattr(exc, "message", "")).lower()
                        for k in ["权限", "授权", "permission", "denied"]
                    )
                    else "sdk_error"
                ),
            }
    return result


def main() -> None:
    try:
        request = json.load(sys.stdin)
        import gm.api as api

        result = execute(api, request)
    except Exception as exc:
        result = {"status": "worker_failed", "error_type": type(exc).__name__}
    print("BT_GM_VALIDATION=" + json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    main()
