"""作者：BruceLee。
职责：按柜台原始状态定义检查大QMT公共查单和订单列表，防止部分成交被当撤单。
输入：48—57及255原始订单状态；输出：规范状态及原始码断言。
上下游：模拟Helper只读响应，经过BigQmtBrokerAdapter；不访问网络、数据库或真实券商。
"""

import pytest

from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter
from bullet_trade.server.config import AccountConfig, ServerConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["single", "list"])
@pytest.mark.parametrize(
    "raw_status,expected",
    [
        (48, "open"),
        (49, "open"),
        (50, "open"),
        (51, "canceling"),
        (52, "canceling"),
        (53, "partly_canceled"),
        (54, "canceled"),
        (55, "partly_filled"),
        (56, "filled"),
        (57, "rejected"),
        (255, "unknown"),
    ],
)
async def test_native_big_qmt_order_status_contract(query, raw_status, expected):
    """输入查询类型、柜台状态及独立预期；返回无，断言规范状态且保留原码，无外部调用。"""
    row = {
        "order_id": "nov05-513100",
        "security": "513100.XSHG",
        "raw_status": raw_status,
        "amount": 591200,
        "filled": 359900,
    }

    class ReadOnlyHelper:
        """返回固定订单事实，协作对象为公共适配器，无写入或认证状态。"""

        async def post(self, path, payload):
            """输入路径及查询载荷，返回订单或列表；断言只调用两类只读接口。"""
            assert path in {"/order_status", "/orders"}
            return dict(row) if path == "/order_status" else {"orders": [dict(row)]}

    config = ServerConfig(
        server_type="big_qmt",
        token="test-only",
        accounts=[AccountConfig(key="default", account_id="test-only")],
    )
    router = AccountRouter(config.accounts)
    adapter = BigQmtBrokerAdapter(config, router, ReadOnlyHelper())
    if query == "single":
        result = await adapter.get_order_status(router.get("default"), row["order_id"])
    else:
        result = (await adapter.list_orders(router.get("default")))[0]
    assert result["raw_status"] == raw_status
    assert result["status"] == expected
