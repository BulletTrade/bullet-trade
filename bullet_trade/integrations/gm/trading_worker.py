"""隔离的 GM 实时交易会话。stdin/stdout JSON 协议，所有 SDK 调用在主线程。"""

from __future__ import annotations

import json
import queue
import sys
import threading
import time

PREFIX = "BT_GM_TRADE="


def emit(value):
    print(PREFIX + json.dumps(value, default=str, ensure_ascii=True), flush=True)


def account_rows(account_id, kind):
    """显式账户、检查原生返回码；不使用吞掉查询错误的全账户 API。"""
    from gm.api import trade

    specs = {
        "orders": ("GetOrdersReq", "py_gmi_get_orders", "Orders"),
        "trades": ("GetExecrptsReq", "py_gmi_get_execution_reports", "ExecRpts"),
    }
    req_name, fn_name, resp_name = specs[kind]
    req = getattr(trade, req_name)()
    req.account_id = account_id
    code, payload = getattr(trade, fn_name)(req.SerializeToString())
    if code != 0:
        raise RuntimeError("account_query_failed")
    response = getattr(trade, resp_name)()
    if payload:
        response.ParseFromString(payload)
    rows = [trade.protobuf_to_dict(x, including_default_value_fields=True) for x in response.data]
    if any(x.get("account_id") != account_id for x in rows):
        raise RuntimeError("account_mismatch")
    return rows


def main():
    config = json.loads(sys.stdin.readline())
    import gm.api as api
    import gm.api.basic as basic
    from bullet_trade.integrations.gm.probe_worker import read_account_status

    account_id = config["account_id"]
    api.set_token(config["token"])
    api.set_serv_addr(config.get("serv_addr") or "127.0.0.1:7001")
    api.set_account_id(account_id)
    basic.py_gmi_set_strategy_id(config["strategy_id"])
    basic.gmi_set_mode(api.MODE_LIVE)
    basic.context.mode = api.MODE_LIVE
    basic.context.strategy_id = config["strategy_id"]
    basic.context.init_fun = lambda context: None
    basic.py_gmi_set_data_callback(basic.callback_controller)
    basic.check_gm_status(basic.gmi_init())
    pending = queue.Queue()

    def reader():
        for line in sys.stdin:
            pending.put(line)
        pending.put(None)

    threading.Thread(target=reader, daemon=True).start()
    emit({"id": "ready", "ok": True, "sdk_version": api.get_version()})
    while True:
        basic.gmi_poll()
        try:
            line = pending.get_nowait()
        except queue.Empty:
            time.sleep(0.01)
            continue
        if line is None:
            return
        req = json.loads(line)
        method = req["method"]
        if method == "close":
            emit({"id": req["id"], "ok": True, "data": True})
            return
        try:
            if method == "status":
                result = read_account_status(account_id)
            elif method == "cash":
                result = api.get_cash(account_id=account_id)
                if not result or result.get("account_id") != account_id:
                    raise RuntimeError("cash_account_mismatch")
            elif method == "positions":
                result = api.get_position(account_id=account_id)
                if any(x.get("account_id") != account_id for x in result):
                    raise RuntimeError("position_account_mismatch")
            elif method in ("orders", "trades"):
                result = account_rows(account_id, method)
            elif method == "quote":
                result = api.current(symbols=req["symbol"])
            elif method in ("place", "cancel"):
                if not config.get("enable_trading"):
                    raise RuntimeError("trading_disabled")
                status = read_account_status(account_id)
                if status != {"state": 3, "error_code": 0}:
                    raise RuntimeError("account_not_ready")
                if method == "place":
                    result = api.order_volume(
                        symbol=req["symbol"],
                        volume=req["volume"],
                        price=req["price"],
                        side=api.OrderSide_Buy if req["side"] == "buy" else api.OrderSide_Sell,
                        order_type=api.OrderType_Limit,
                        position_effect=(
                            api.PositionEffect_Open
                            if req["side"] == "buy"
                            else api.PositionEffect_Close
                        ),
                        account=account_id,
                    )
                else:
                    rows = account_rows(account_id, "orders")
                    matches = [x for x in rows if x["cl_ord_id"] == req["cl_ord_id"]]
                    if len(matches) != 1:
                        raise RuntimeError("cancel_order_not_found")
                    api.order_cancel([{"account_id": account_id, "cl_ord_id": req["cl_ord_id"]}])
                    result = True  # 仅提交；Broker 通过查询判断最终撤单结果。
            else:
                raise ValueError("unsupported_method")
            emit({"id": req["id"], "ok": True, "data": result})
        except Exception as exc:
            # 不输出 SDK 原始异常，写操作由父进程按未知提交处理。
            emit({"id": req["id"], "ok": False, "error_type": type(exc).__name__})


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        emit({"id": "ready", "ok": False, "error_type": type(exc).__name__})
