"""
作者: BruceLee
文件职责: 在隔离子进程中执行掘金 SDK 的有限只读查询。
主要输入: 标准输入 JSON 中的 token、服务地址、行情范围及可选账户 ID。
主要输出: 白名单查询结果，不输出凭据或 SDK 原始日志。
上下游关系: probe 启动本模块；本模块只调用认证、行情、资金及持仓查询。
关键配置: 不调用 run、下单、撤单或修改账户；委托查询留待初始化会话后验证。
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional


def read_account_status(account_id: str) -> Dict[str, int]:
    """复用 SDK Context 使用的状态协议，显式查询单一账号。

    参数:
        account_id: 用户明确选择的账号 ID。
    返回:
        SDK 连接状态与错误码；3 为已登录，5 为已断开。
    异常:
        RuntimeError: SDK 调用失败或未返回唯一匹配账号。
    副作用:
        导入 SDK 内部协议类型并执行一次只读查询，不初始化策略会话。
    """

    from gm.csdk.c_sdk import py_gmi_get_account_status
    from gm.pb.account_pb2 import AccountStatuses
    from gm.pb.tradegw_service_pb2 import GetAccountStatusesReq

    request = GetAccountStatusesReq()
    request.account_ids.append(account_id)
    status, data = py_gmi_get_account_status(request.SerializeToString())
    if status != 0 or not data:
        raise RuntimeError("账户状态查询失败")
    response = AccountStatuses()
    response.ParseFromString(data)
    matches = [item for item in response.data if item.account_id == account_id]
    if len(matches) != 1:
        raise RuntimeError("账户状态未返回唯一匹配记录")
    return {"state": matches[0].status.state, "error_code": matches[0].status.error.code}


def execute(
    api: Any, request: Dict[str, Any], status_reader: Optional[Any] = None
) -> Dict[str, Any]:
    """执行有限只读查询并将输出限制在安全字段。

    参数:
        api: 已加载的 gm.api 或测试替身。
        request: 含显式查询范围和私密连接配置的字典。
        status_reader: 可选的状态查询替身；默认使用 SDK 自身协议。
    返回:
        查询状态、记录数量、公开行情时间和账户记录是否存在。
    副作用:
        在本进程设置 SDK 认证，并访问行情和显式指定的账户。
    """

    api.set_token(request["token"])
    if request.get("serv_addr"):
        api.set_serv_addr(request["serv_addr"])
    result: Dict[str, Any] = {
        "calls": {},
        "account_type": "not_checked",
        "account_connection": "not_checked",
        "orders": "not_checked_session_required",
        "executions": "not_checked_session_required",
        "trading": "not_checked",
    }

    def query(name: str, action: Any) -> Any:
        """运行一次查询，异常只保留类型，防止 SDK 错误泄漏认证信息。

        参数:
            name/action: 查询名及无参数可调用对象。
        返回:
            成功结果，失败时为 None；状态写入 result。
        """

        try:
            value = action()
        except Exception as exc:
            result["calls"][name] = {"status": "error", "error_type": type(exc).__name__}
            return None
        result["calls"][name] = {"status": "ok"}
        return value

    bars = query(
        "history",
        lambda: api.history(
            symbol=request["symbol"],
            frequency="1d",
            start_time=request["start"] + " 00:00:00",
            end_time=request["end"] + " 23:59:59",
            fields="symbol,eob,open,high,low,close,volume,amount",
            df=False,
        ),
    )
    if bars is not None:
        result["calls"]["history"].update(
            rows=len(bars), last_time=str(bars[-1].get("eob")) if bars else None
        )
    snapshots = query(
        "current",
        lambda: api.current(symbols=request["symbol"], fields="symbol,created_at,price"),
    )
    if snapshots is not None:
        result["calls"]["current"].update(
            rows=len(snapshots),
            last_time=str(snapshots[-1].get("created_at")) if snapshots else None,
        )
    if request.get("account_id"):
        account_status = query(
            "account_status", lambda: (status_reader or read_account_status)(request["account_id"])
        )
        if account_status is not None:
            result["calls"]["account_status"].update(account_status)
            result["account_connection"] = {
                0: "unknown",
                1: "connecting",
                2: "connected_not_logged_in",
                3: "logged_in",
                4: "disconnecting",
                5: "disconnected",
                6: "error",
            }.get(account_status["state"], "unknown")
        cash = query("cash", lambda: api.get_cash(account_id=request["account_id"]))
        if cash is not None:
            result["calls"]["cash"].update(
                record_present=bool(cash),
                last_time=str(cash["updated_at"]) if cash.get("updated_at") else None,
                channel_present=bool(cash.get("channel_id")),
            )
        positions = query("positions", lambda: api.get_position(account_id=request["account_id"]))
        if positions is not None:
            result["calls"]["positions"].update(rows=len(positions))
    # 空历史/快照/资金不能证明连接就绪；空持仓是有效查询结果。
    result["ok"] = all(call["status"] == "ok" for call in result["calls"].values()) and bool(
        bars and snapshots
    )
    if request.get("account_id"):
        result["ok"] = (
            result["ok"]
            and result["calls"].get("cash", {}).get("record_present", False)
            and result["account_connection"] == "logged_in"
        )
    return result


def main() -> None:
    """读取私密输入并执行探针，保证结果在 SDK 清理前刷新。

    返回:
        None；父进程根据结果决定退出码并屏蔽 SDK 原始输出。
    副作用:
        显式导入 SDK、执行有限只读查询、输出白名单 JSON。
    """

    try:
        request = json.load(sys.stdin)
        import gm.api as api

        result = execute(api, request)
    except Exception:
        result = {"ok": False, "status": "sdk_failed"}
    print("BT_GM_READONLY=" + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
