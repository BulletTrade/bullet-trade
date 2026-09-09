"""冻结大 QMT helper 现有 HTTP 路由合同，为后续重构提供无网络基准。

作者：BruceLee
职责：覆盖全部显式路由的分派、认证、请求标识、返回封装和错误状态。
输入：GBK helper、虚构请求和业务函数替身；输出：pytest 合同断言。
上下游：真实 HTTP handler/路由/action 分派；QMT 查询与交易函数全部替换为 mock。
环境：不监听端口，不初始化 QMT，不读凭据，不执行下单、撤单、下载或订阅。
本文件验证传输合同；原生参数/数值合同复用已有 helper 与 adapter 测试，不能代替实机验收。
"""

import ast
import inspect
import textwrap
from copy import deepcopy
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

import pytest
from test_big_qmt_gateway_strategy_sample import _load_helper
from tornado.httputil import HTTPHeaders

pytestmark = pytest.mark.unit

# 显式清单让新增或删除路由必须同步合同，不让测试从实现自动推导正确答案。
_ROUTES = {
    "/data/history": "history",
    "/data/snapshot": "snapshot",
    "/data/current_tick": "current_tick",
    "/data/live_current": "live_current",
    "/data/trade_days": "trade_days",
    "/data/security_info": "security_info",
    "/data/ensure_cache": "ensure_cache",
    "/data/all_securities": "all_securities",
    "/data/index_stocks": "index_stocks",
    "/data/split_dividend": "split_dividend",
    "/account": "account",
    "/positions": "positions",
    "/orders": "orders",
    "/trades": "trades",
    "/order_status": "order_status",
    "/place_order": "place_order",
    "/cancel_order": "cancel_order",
    "/debug/trade_detail": "debug_trade_detail",
    "/debug/qmt_trade_detail": "debug_trade_detail",
    "/api/holding": "positions",
    "/api/money/total": "money_total",
    "/api/money/available": "money_available",
    "/api/market/full_tick": "snapshot",
    "/api/order/status": "orders",
    "/api/order/cancel_all": "cancel_all",
    "/api/order/cancel_order": "rule_cancel",
    "/api/order/buy": "place_order",
    "/api/order/sell": "place_order",
}
_CONTEXT_QUERIES = {
    "history": "_query_history",
    "trade_days": "_query_trade_days",
    "security_info": "_query_security_info",
    "all_securities": "_query_all_securities",
    "index_stocks": "_query_index_stocks",
    "split_dividend": "_query_split_dividend",
    "place_order": "_place_order",
    "cancel_order": "_cancel_order",
    "rule_cancel": "_cancel_by_rule",
}


@pytest.fixture
def helper(monkeypatch):
    """加载隔离 helper；输入 monkeypatch，返回模块与虚构函数，不初始化服务或 QMT 上下文。"""
    module = _load_helper()
    monkeypatch.setattr(module, "GATEWAY_PASSWORD", "contract-password")
    monkeypatch.setattr(module, "GATEWAY_SECRET", "contract-secret")
    monkeypatch.setattr(module, "ENABLE_CANCEL_ALL", False)
    monkeypatch.setattr(module, "_emit", Mock())
    return module


def _handler(helper, route, method, *, authenticated=True):
    """建立内存 HTTP handler；输入模块路由方法和认证标记，返回对象，不连接 socket。"""
    payload = {
        "security": "000001.XSHE",
        "account_id": "contract-account",
        "order_id": "mock-order",
    }
    headers = HTTPHeaders(
        {
            "X-BulletTrade-Password": "contract-password" if authenticated else "wrong",
            "X-BulletTrade-Secret": "contract-secret",
        }
    )
    handler = SimpleNamespace(
        request=SimpleNamespace(path=route, headers=headers, remote_ip="127.0.0.1"),
        _query_payload=Mock(return_value=deepcopy(payload) if method == "get" else {}),
        _read_json=Mock(return_value=deepcopy(payload)),
        _request_id=Mock(return_value="contract-request"),
        _send_json=Mock(),
    )
    handler._handle_action = MethodType(helper._GatewayHandler._handle_action, handler)
    return handler


def _install_business_mocks(helper, monkeypatch, action):
    """替换所有可能业务调用；输入模块夹具及动作，返回对应 mock 和预期值，绝不接触真实 API。"""
    value = {"fixture": action}
    target = None
    for name in list(_CONTEXT_QUERIES.values()) + ["_ensure_cache", "_query_trade_detail_debug"]:
        mock = Mock(return_value=helper._ok(value, "contract-request"))
        monkeypatch.setattr(helper, name, mock)
        if name == _CONTEXT_QUERIES.get(action) or (action, name) in {
            ("ensure_cache", "_ensure_cache"),
            ("debug_trade_detail", "_query_trade_detail_debug"),
        }:
            target = mock
    account = {"total_value": 1234.5, "available_cash": 678.9}
    position = {"security": "000001.XSHE", "amount": 100}
    order = {"order_id": "mock-order", "status": "filled"}
    trade = {"trade_id": "mock-trade", "amount": 100}
    query_values = {
        "_get_full_tick": value,
        "_query_account": account,
        "_query_positions": [position],
        "_query_orders": [order],
        "_query_trades": [trade],
    }
    action_target = {
        "snapshot": "_get_full_tick",
        "current_tick": "_get_full_tick",
        "live_current": "_get_full_tick",
        "account": "_query_account",
        "money_total": "_query_account",
        "money_available": "_query_account",
        "positions": "_query_positions",
        "orders": "_query_orders",
        "order_status": "_query_orders",
        "trades": "_query_trades",
    }
    for name, result in query_values.items():
        mock = Mock(return_value=result)
        monkeypatch.setattr(helper, name, mock)
        if name == action_target.get(action):
            target = mock
    expected = {
        "account": account,
        "money_total": {"total_value": 1234.5, "account": account},
        "money_available": {"available_cash": 678.9, "account": account},
        "positions": {"positions": [position]},
        "orders": {"orders": [order]},
        "order_status": {"order": order},
        "trades": {"trades": [trade]},
    }.get(action, value)
    return target, expected


@pytest.mark.parametrize("route,action", list(_ROUTES.items()))
@pytest.mark.parametrize("method", ["get", "post"])
def test_all_routes_dispatch_and_preserve_response(helper, monkeypatch, route, action, method):
    """验证每条现有路由；输入路由动作方法与夹具，无返回，断言实际分派和业务封装不变。"""
    target, expected = _install_business_mocks(helper, monkeypatch, action)
    context = object()

    def dispatch(received, payload):
        """执行真实 action 分派；输入动作载荷，返回 mock 业务响应，无外部副作用。"""
        return helper._dispatch_qmt_action(context, received, payload)

    submit = Mock(side_effect=dispatch)
    monkeypatch.setattr(helper, "RUNTIME", SimpleNamespace(submit=submit))
    handler = _handler(helper, route, method)
    getattr(helper._GatewayHandler, method)(handler)
    received_action, payload = submit.call_args.args
    assert received_action == action
    assert payload["request_id"] == "contract-request"
    assert payload["security"] == "000001.XSHE"
    if route in {"/api/order/buy", "/api/order/sell"}:
        assert payload["side"] == ("BUY" if route.endswith("buy") else "SELL")
    status, response = handler._send_json.call_args.args
    assert response["request_id"] == "contract-request"
    if action == "cancel_all":
        assert status == 400 and response["code"] == "DANGEROUS_OPERATION_DISABLED"
    else:
        assert status == 200 and response["ok"] is True and response["value"] == expected
        target.assert_called_once()
        if action in _CONTEXT_QUERIES or action in {"snapshot", "current_tick", "live_current"}:
            target.assert_called_once_with(context, payload)
        elif action in {"ensure_cache", "debug_trade_detail"}:
            target.assert_called_once_with(payload)


@pytest.mark.parametrize("route", list(_ROUTES))
@pytest.mark.parametrize("method", ["get", "post"])
def test_every_action_requires_authentication(helper, monkeypatch, route, method):
    """拒绝每条业务路由的未认证请求；输入路由方法与夹具，无返回，确认分派零调用。"""
    submit = Mock(side_effect=AssertionError("未认证请求不能分派"))
    monkeypatch.setattr(helper, "RUNTIME", SimpleNamespace(submit=submit))
    handler = _handler(helper, route, method, authenticated=False)
    getattr(helper._GatewayHandler, method)(handler)
    status, response = handler._send_json.call_args.args
    assert status == 401 and response["code"] == "AUTH_FAILED"
    submit.assert_not_called()


@pytest.mark.parametrize(
    "code,status", [("QMT_API_NOT_READY", 400), ("NOT_FOUND", 404), ("NOT_IMPLEMENTED", 404)]
)
def test_error_code_and_broker_boundary_are_not_lost(helper, monkeypatch, code, status):
    """保留失败响应和交易调用边界；输入错误码状态，无返回，HTTP 不吞掉 broker_called。"""
    response = helper._error(code, "测试错误", "contract-request")
    response["broker_called"] = False
    monkeypatch.setattr(helper, "RUNTIME", SimpleNamespace(submit=Mock(return_value=response)))
    handler = _handler(helper, "/place_order", "post")
    helper._GatewayHandler.post(handler)
    handler._send_json.assert_called_once_with(status, response)


def test_health_and_unknown_routes(helper, monkeypatch):
    """固定健康与未知接口行为；输入夹具，无返回，健康不交易，未知路由返回404。"""
    runtime = SimpleNamespace(health=Mock(return_value={"ready": True}), submit=Mock())
    monkeypatch.setattr(helper, "RUNTIME", runtime)
    handler = _handler(helper, "/health", "get", authenticated=False)
    helper._GatewayHandler.get(handler)
    handler._send_json.assert_called_once()
    status, response = handler._send_json.call_args.args
    assert status == 200 and response["ok"] is True
    assert response["value"] == {"ready": True}
    assert response["request_id"] == "contract-request"
    assert isinstance(response["ts"], (int, float)) and response["ts"] > 0
    for method in ("get", "post"):
        handler = _handler(helper, "/data/unknown", method)
        getattr(helper._GatewayHandler, method)(handler)
        assert handler._send_json.call_args.args[0] == 404
    runtime.submit.assert_not_called()


def test_route_inventory_is_complete(helper):
    """检测接口清单遗漏；输入 helper，无返回，从源码提取路由仅核对集合，不推导期望分派。"""
    tree = ast.parse(textwrap.dedent(inspect.getsource(helper._route_to_action)))
    routes = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith("/")
    }
    assert routes == set(_ROUTES)


def test_correct_password_with_wrong_secret_is_rejected(helper, monkeypatch):
    """验证密钥不是只检查口令；输入夹具，无返回，错误secret不能进入任何业务分派。"""
    submit = Mock(side_effect=AssertionError("错误密钥不能分派"))
    monkeypatch.setattr(helper, "RUNTIME", SimpleNamespace(submit=submit))
    handler = _handler(helper, "/data/history", "post")
    handler.request.headers["X-BulletTrade-Secret"] = "wrong-secret"
    helper._GatewayHandler.post(handler)
    assert handler._send_json.call_args.args[0] == 401
    submit.assert_not_called()


def test_order_status_does_not_return_another_order(helper, monkeypatch):
    """验证单订单接口精确匹配；输入夹具，无返回，不能用另一订单冒充查询目标。"""
    monkeypatch.setattr(helper, "_query_orders", Mock(return_value=[{"order_id": "other-order"}]))
    response = helper._dispatch_qmt_action(
        object(),
        "order_status",
        {
            "account_id": "contract-account",
            "order_id": "mock-order",
            "request_id": "contract-request",
        },
    )
    assert response["ok"] is False and response["code"] == "ORDER_NOT_FOUND"
    assert response["request_id"] == "contract-request"


def test_json_response_preserves_chinese_and_byte_length(helper):
    """验证GBK源码的HTTP输出仍为UTF-8 JSON；输入模块，无返回，按字节校验长度和结束发送。"""
    handler = SimpleNamespace(
        request=SimpleNamespace(path="/data/security_info"),
        set_status=Mock(),
        set_header=Mock(),
        write=Mock(),
        finish=Mock(),
    )
    response = helper._ok({"display_name": "中文证券"}, "contract-request")
    helper._GatewayHandler._send_json(handler, 200, response)
    body = handler.write.call_args.args[0]
    assert "中文证券" in body
    handler.set_header.assert_any_call("Content-Length", str(len(body.encode("utf-8"))))
    handler.set_header.assert_any_call("Connection", "close")
    handler.set_status.assert_called_once_with(200)
    handler.finish.assert_called_once()
