"""作者: BruceLee
职责: 只读 GM 数据 worker；stdin 输入配置和查询，stdout 仅输出带标记的 JSON。
上下游: data_client 在临时子进程执行本模块；不导入策略、不访问交易/账户接口。
"""

import json
import sys

ALLOWED = {
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


def execute(api, request):
    """先校验白名单，再认证和取数；异常仅保留错误码，不泄露 SDK 日志/凭据。"""
    if request.get("method") not in ALLOWED:
        raise ValueError("仅支持数据读取")
    api.set_token(request["token"])
    if request.get("serv_addr"):
        api.set_serv_addr(request["serv_addr"])
    value = getattr(api, request["method"])(**request["kwargs"])
    if hasattr(value, "to_dict"):
        value = value.to_dict("records")
    return {"status": "ok", "data": value}


def main():
    try:
        request = json.load(sys.stdin)
        if request.get("method") not in ALLOWED:
            raise ValueError("仅支持数据读取")
        import gm.api as api

        result = execute(api, request)
    except Exception as exc:
        codes = [v for v in exc.args if isinstance(v, int)]
        result = {"status": "error", "error_code": codes[0] if codes else None}
    print("BT_GM_DATA=" + json.dumps(result, default=str, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
