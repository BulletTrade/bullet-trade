"""验证 HuaxinBroker 对公开 NativeRuntime 的 Trader-only 合同。"""

import asyncio
import os
from types import SimpleNamespace

import pytest

from bullet_trade.core.models import Order, OrderStatus
from bullet_trade.core.orders import MarketOrderStyle
from bullet_trade.core.orders import cancel_order as strategy_cancel_order
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.integrations.huaxin import broker as broker_module
from bullet_trade.integrations.huaxin.broker import HuaxinBroker
from bullet_trade.integrations.huaxin.errors import (
    HUAXIN_MARKET_ORDER_DISABLED,
    HUAXIN_NATIVE_UNAVAILABLE,
    NATIVE_CALL_FAILED,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)
from bullet_trade.integrations.huaxin.order_journal import ToraOrderIdentityJournal


class _FakeRuntime:
    """按 request_id 生成 record/query_end/写响应的公开 Runtime 替身。"""

    def __init__(self, *, session_id=8):
        """创建全 readiness、空事件队列的替身。

        Args:
            session_id: 登录和订单回报使用的有符号 int32 SessionID。

        Returns:
            None。
        """

        self.events = []
        self.started = None
        self.place_calls = []
        self.cancel_calls = []
        self.closed = False
        self.dropped_events = 0
        self.query_end_record_count_delta = 0
        self.query_end_request_type_override = None
        self.max_order_ref = 40
        self.session_id = session_id

    def start_session(self, config):
        """记录会话配置。

        Args:
            config: 测试会话配置。

        Returns:
            None。
        """

        self.started = config
        self.events.append(
            self._event(
                0,
                "login",
                {
                    "front_id": 7,
                    "session_id": self.session_id,
                    "max_order_ref": self.max_order_ref,
                },
            )
        )

    def stop_session(self):
        """记录会话停止。

        Returns:
            None。
        """

        self.closed = True

    def close(self):
        """记录 runtime 释放。

        Returns:
            None。
        """

        self.closed = True

    def health(self):
        """返回全部 Trader readiness 为真的脱敏 health。

        Returns:
            SimpleNamespace: Broker 所需固定 health 字段。
        """

        return SimpleNamespace(
            state=6,
            queue_capacity=64,
            queue_size=len(self.events),
            dropped_events=self.dropped_events,
            vendor_schema_id="test",
            field_set_version="1",
            transport_connected=True,
            logged_in=True,
            ready_for_queries=True,
            ready_for_new_orders=True,
            ready_for_cancel=True,
            session_epoch=1,
            last_error_id=0,
        )

    def drain(self, max_events):
        """批量移出不超过 max_events 的测试事件。

        Args:
            max_events: 最大事件数。

        Returns:
            list: 本批事件。
        """

        batch = self.events[:max_events]
        del self.events[:max_events]
        return batch

    def query_trading_accounts(self, request_id):
        """生成资金记录和 query_end。

        Args:
            request_id: 请求标识。

        Returns:
            None。
        """

        self._query(
            request_id,
            "trading_account",
            {"account_id": "acct", "available_cash": 1000, "frozen_cash": 20},
        )

    def query_positions(self, request_id):
        """生成持仓记录和 query_end。

        Args:
            request_id: 请求标识。

        Returns:
            None。
        """

        self._query(
            request_id,
            "position",
            {
                "exchange": "SSE",
                "security": "511880",
                "current_position": 100,
                "available_position": 80,
                "total_cost": 10020,
            },
        )

    def query_orders(self, request_id):
        """生成已缓存限价单的订单记录和 query_end。

        Args:
            request_id: 请求标识。

        Returns:
            None。
        """

        request = self.place_calls[-1][1] if self.place_calls else None
        if request is None:
            self.events.append(
                self._event(
                    request_id,
                    "query_end",
                    {
                        "error_id": 0,
                        "request_type": broker_module.native_api.REQUEST_QUERY_ORDER,
                        "record_count": 0,
                    },
                )
            )
            return
        self._query(request_id, "order", self._order_data(request, "open"))

    def query_trades(self, request_id):
        """生成成交记录和 query_end。

        Args:
            request_id: 请求标识。

        Returns:
            None。
        """

        self._query(
            request_id,
            "trade",
            {
                "exchange": "SSE",
                "security": "511880",
                "direction": "buy",
                "trade_id": "T1",
                "order_sys_id": "SYS1",
                "price": 100.2,
                "amount": 100,
            },
        )

    def query_shareholder_accounts(self, request_id):
        """生成股东身份和 query_end。

        Args:
            request_id: 请求标识。

        Returns:
            None。
        """

        self._query(
            request_id,
            "shareholder_account",
            {
                "exchange": "SSE",
                "investor_id": "investor",
                "shareholder_id": "shareholder",
            },
        )

    def query_security(self, request_id, exchange="", security=""):
        """生成目标证券的完整限价/市价申报约束。

        Args:
            request_id: 请求标识。
            exchange: 交易所过滤。
            security: 证券代码过滤。

        Returns:
            None。
        """

        self._query(
            request_id,
            "security",
            {
                "exchange": exchange or "SSE",
                "security": security or "511880",
                "security_type": 1,
                "order_unit": 1,
                "limit_buy_unit": 100,
                "limit_sell_unit": 1,
                "min_limit_buy": 100,
                "max_limit_buy": 1_000_000,
                "min_limit_sell": 1,
                "max_limit_sell": 1_000_000,
                "market_buy_unit": 100,
                "market_sell_unit": 1,
                "min_market_buy": 100,
                "max_market_buy": 1_000_000,
                "min_market_sell": 1,
                "max_market_sell": 1_000_000,
                "volume_multiple": 1,
                "price_tick": 0.001,
                "security_status": 0,
                "has_price_limit": True,
                "upper_limit_price": 120.0,
                "lower_limit_price": 80.0,
            },
        )

    def place_order(self, request_id, request):
        """记录统一限价/市价请求并生成订单事实和成功响应。

        Args:
            request_id: 请求标识。
            request: canonical 统一订单请求。

        Returns:
            None。
        """

        self.place_calls.append((request_id, request))
        self.events.extend(
            [
                self._event(request_id, "order", self._order_data(request, "open")),
                self._event(request_id, "order_insert_response", {"error_id": 0}),
            ]
        )

    def place_limit(self, request_id, request):
        """记录限价请求并生成订单事实和成功响应。

        Args:
            request_id: 请求标识。
            request: 限价请求对象。

        Returns:
            None。
        """

        self.place_calls.append((request_id, request))
        self.events.extend(
            [
                self._event(request_id, "order", self._order_data(request, "open")),
                self._event(request_id, "order_insert_response", {"error_id": 0}),
            ]
        )

    def cancel_order(self, request_id, request):
        """记录撤单请求并生成精确已撤事实和成功响应。

        Args:
            request_id: 请求标识。
            request: 撤单请求对象。

        Returns:
            None。
        """

        self.cancel_calls.append((request_id, request))
        limit_request = self.place_calls[-1][1]
        self.events.extend(
            [
                self._event(request_id, "order", self._order_data(limit_request, "canceled")),
                self._event(request_id, "order_action_response", {"error_id": 0}),
            ]
        )

    def _query(self, request_id, event_name, data):
        """追加单条 record 和 query_end。

        Args:
            request_id: 请求标识。
            event_name: record 事件名。
            data: record 数据。

        Returns:
            None。
        """

        request_types = {
            "security": broker_module.native_api.REQUEST_QUERY_SECURITY,
            "shareholder_account": broker_module.native_api.REQUEST_QUERY_SHAREHOLDER_ACCOUNT,
            "trading_account": broker_module.native_api.REQUEST_QUERY_TRADING_ACCOUNT,
            "position": broker_module.native_api.REQUEST_QUERY_POSITION,
            "order": broker_module.native_api.REQUEST_QUERY_ORDER,
            "trade": broker_module.native_api.REQUEST_QUERY_TRADE,
        }
        request_type = request_types[event_name]
        if self.query_end_request_type_override is not None:
            request_type = self.query_end_request_type_override
        self.events.extend(
            [
                self._event(request_id, event_name, data),
                self._event(
                    request_id,
                    "query_end",
                    {
                        "error_id": 0,
                        "request_type": request_type,
                        "record_count": 1 + self.query_end_record_count_delta,
                    },
                ),
            ]
        )

    @staticmethod
    def _event(request_id, event_name, data):
        """构造公开 NativeEvent 等价对象。

        Args:
            request_id: 请求标识。
            event_name: 稳定事件名。
            data: 解码字段。

        Returns:
            SimpleNamespace: 测试事件。
        """

        return SimpleNamespace(request_id=request_id, event_name=event_name, data=dict(data))

    def _order_data(self, request, status):
        """由限价请求构造精确 TORA 订单记录。

        Args:
            request: 限价请求对象。
            status: 订单状态。

        Returns:
            dict: 订单事件 data。
        """

        return {
            "exchange": request.exchange,
            "security": request.security,
            "direction": request.direction,
            "limit_price": request.limit_price,
            "amount": request.amount,
            "filled": 0,
            "canceled": request.amount if status == "canceled" else 0,
            "front_id": 7,
            "session_id": self.session_id,
            "order_ref": request.order_ref,
            "order_local_id": "LOCAL1",
            "order_sys_id": "SYS1",
            "order_status": status,
            "order_price_type": getattr(request, "order_price_type", "limit"),
            "time_condition": getattr(request, "time_condition", "gfd"),
            "volume_condition": getattr(request, "volume_condition", "any"),
        }


def _broker_for_runtime(runtime, **flags):
    """构造使用 fake runtime、但尚未连接的测试 broker。

    Args:
        runtime: FakeRuntime 实例。
        **flags: 写门禁覆盖。

    Returns:
        HuaxinBroker: 尚未连接的 broker。
    """

    config = {
        "account_id": "acct",
        "flow_path": "/tmp/test",
        "trade_front": "tcp://test",
        "password": "secret",
        "terminal_info": "test",
        "mac_address": "00-11-22-33-44-55",
        "user_product_info": "BT",
        "connect_timeout": 0,
        "query_timeout": 0,
        "write_response_timeout": 0,
    }
    config.update(flags)
    return HuaxinBroker("acct", config=config, runtime_factory=lambda _: runtime)


def _connected_broker(runtime, **flags):
    """构造并连接使用 fake runtime 的测试 broker。

    Args:
        runtime: FakeRuntime 实例。
        **flags: 写门禁覆盖。

    Returns:
        HuaxinBroker: 已连接 broker。
    """

    broker = _broker_for_runtime(runtime, **flags)
    assert broker.connect() is True
    return broker


@pytest.fixture
def order_identity_journal_path(tmp_path):
    """创建权限满足生产 journal 门禁的临时 SQLite 路径。

    Args:
        tmp_path: pytest 私有临时目录。

    Returns:
        str: 尚未创建的订单身份 SQLite 绝对路径。

    Side Effects:
        把测试目录权限收紧为 0700。
    """

    os.chmod(tmp_path, 0o700)
    return str(tmp_path / "huaxin-order-identities.sqlite3")


@pytest.mark.parametrize(
    ("config", "required_field"),
    [
        ({}, "HUAXIN_USER_PRODUCT_INFO"),
        ({"user_product_info": "BT"}, "HUAXIN_TERMINAL_INFO"),
    ],
)
def test_connect_requires_login_metadata_before_runtime_start(config, required_field) -> None:
    """验证缺少终端身份字段时不会创建或启动 Trader runtime。

    Args:
        config: 待验证的最小 broker 配置。
        required_field: 预期被拒绝的环境配置字段名。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    factory_calls = []

    def _runtime_factory(config):
        """记录 runtime 工厂调用。

        Args:
            config: Broker 会话配置。

        Returns:
            _FakeRuntime: 测试 runtime。
        """

        factory_calls.append(config)
        return runtime

    broker = HuaxinBroker("acct", config=config, runtime_factory=_runtime_factory)

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.connect()

    assert exc_info.value.code == HUAXIN_NATIVE_UNAVAILABLE
    assert exc_info.value.details == {"required_config_field": required_field}
    assert factory_calls == []
    assert runtime.started is None


@pytest.mark.parametrize(
    ("config", "required_field", "max_bytes", "actual_bytes"),
    [
        (
            {
                "user_product_info": "BulletTrade",
                "terminal_info": "masked-terminal",
            },
            "HUAXIN_USER_PRODUCT_INFO",
            10,
            11,
        ),
        (
            {"user_product_info": "华鑫产品", "terminal_info": "masked-terminal"},
            "HUAXIN_USER_PRODUCT_INFO",
            10,
            12,
        ),
        (
            {
                "user_product_info": "BT",
                "terminal_info": "A" * 513,
            },
            "HUAXIN_TERMINAL_INFO",
            512,
            513,
        ),
    ],
)
def test_connect_rejects_login_metadata_over_utf8_limits(
    config, required_field, max_bytes, actual_bytes
) -> None:
    """验证终端信息和产品标识按 UTF-8 字节长度失败关闭。

    Args:
        config: 含越界字段的 broker 配置。
        required_field: 预期被拒绝的环境配置字段名。
        max_bytes: 官方字段最大字节数。
        actual_bytes: 测试值编码后的实际字节数。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    broker = HuaxinBroker("acct", config=config, runtime_factory=lambda _: runtime)

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.connect()

    assert exc_info.value.code == HUAXIN_NATIVE_UNAVAILABLE
    assert exc_info.value.details == {
        "required_config_field": required_field,
        "max_bytes": max_bytes,
        "actual_bytes": actual_bytes,
    }
    assert runtime.started is None


@pytest.mark.parametrize("session_id", (-(1 << 31), -1, 0, (1 << 31) - 1))
def test_login_accepts_full_signed_int32_session_id(session_id: int) -> None:
    """验证登录事件接受 TORA 官方有符号 int32 SessionID 全范围。

    Args:
        session_id: 待验证的有符号 int32 边界值。

    Returns:
        None。
    """

    broker = _connected_broker(_FakeRuntime(session_id=session_id))
    assert broker._login_session_id == session_id


@pytest.mark.parametrize("session_id", (None, -(1 << 31) - 1, 1 << 31))
def test_login_rejects_missing_or_out_of_range_session_id(session_id) -> None:
    """验证登录事件拒绝缺失或越出有符号 int32 的 SessionID。

    Args:
        session_id: 缺失或越界的 SessionID 反例。

    Returns:
        None。
    """

    runtime = _FakeRuntime(session_id=session_id)
    with pytest.raises(ValueError, match="session_id"):
        _connected_broker(runtime)
    assert runtime.closed is True


def test_queries_wait_for_query_end_and_normalize() -> None:
    """验证资金、持仓、委托、成交均由 record+query_end 收口。"""

    runtime = _FakeRuntime()
    broker = _connected_broker(runtime)

    assert broker.get_account_info()["available_cash"] == 1000.0
    assert broker.get_positions()[0]["security"] == "511880.XSHG"
    assert broker.get_positions()[0]["closeable_amount"] == 80
    assert broker.get_orders() == []
    assert broker.get_trades()[0]["trade_id"] == "T1"
    assert broker.health_snapshot()["baseline_query_ready"] is True
    assert broker.health_snapshot()["baseline_queries_completed"] == [
        "account",
        "orders",
        "positions",
        "trades",
    ]


def test_account_query_requires_exact_target_identity() -> None:
    """验证其他资金账号不能冒充目标账号或完成账户基线。"""

    class _OtherAccountRuntime(_FakeRuntime):
        """返回非目标资金账号的 Trader runtime 替身。"""

        def query_trading_accounts(self, request_id):
            """生成一个身份不匹配的资金记录。

            Args:
                request_id: 请求标识。

            Returns:
                None。
            """

            self._query(
                request_id,
                "trading_account",
                {"account_id": "other-account", "available_cash": 1000},
            )

    runtime = _OtherAccountRuntime()
    broker = _broker_for_runtime(runtime)

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.connect()

    assert exc_info.value.code == NATIVE_CALL_FAILED
    assert exc_info.value.details == {
        "operation": "query_trading_accounts",
        "record_count": 1,
        "target_config_field": "HUAXIN_ACCOUNT_ID",
    }
    assert "other-account" not in str(exc_info.value.to_dict())
    assert broker.health_snapshot()["baseline_query_ready"] is False
    assert "account" not in broker.health_snapshot()["baseline_queries_completed"]


def test_health_stays_degraded_until_all_four_baseline_queries_succeed() -> None:
    """验证连接预热会完成四类 broker 基线查询证据。"""

    broker = _connected_broker(_FakeRuntime())

    assert broker.health_snapshot()["baseline_query_ready"] is True
    assert broker.health_snapshot()["baseline_queries_completed"] == [
        "account",
        "orders",
        "positions",
        "trades",
    ]


def test_tora_exchange_and_order_status_projection() -> None:
    """验证深圳交易所与官方 v4.1.8 订单/提交状态不会漂移。"""

    assert broker_module._split_security("000001.XSHE") == ("SZSE", "000001")
    assert {
        value: HuaxinBroker._normalize_order_status(value)
        for value in ("0", "1", "2", "3", "4", "5", "6", "7", "#")
    } == {
        "0": "new",
        "1": "new",
        "2": "open",
        "3": "filling",
        "4": "filled",
        "5": "partly_canceled",
        "6": "canceled",
        "7": "rejected",
        "#": "open",
    }
    assert {
        value: HuaxinBroker._normalize_order_submit_status(value)
        for value in ("0", "1", "2", "3", "4", "5")
    } == {
        "0": "insert_unsubmitted",
        "1": "insert_submitted",
        "2": "cancel_unsubmitted",
        "3": "cancel_submitted",
        "4": "cancel_rejected",
        "5": "cancel_deleted",
    }
    assert HuaxinBroker._normalize_order_status("future-value") == "new"


def test_cancel_rejected_submit_status_does_not_reject_original_order() -> None:
    """验证 CancelRejected 只描述撤单提交，不污染原委托状态。"""

    broker = HuaxinBroker("acct")
    order = broker._normalize_order(
        {
            "order_sys_id": "SYS1",
            "order_status": "2",
            "submit_status": "4",
            "security": "511880",
            "exchange": "SSE",
        }
    )

    assert order["status"] == "open"
    assert order["normalized_submit_status"] == "cancel_rejected"
    assert order["raw_submit_status"] == "4"


def test_dropped_native_event_fails_closed() -> None:
    """验证事件队列一旦丢包就不能继续宣称 Trader 查询就绪。"""

    runtime = _FakeRuntime()
    runtime.dropped_events = 1

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        _connected_broker(runtime)

    assert exc_info.value.code == NATIVE_CALL_FAILED


def test_query_drop_growth_fails_closed_after_query_end() -> None:
    """验证查询提交至 query_end 期间 drop 计数增长会拒绝快照。"""

    class _DropDuringQueryRuntime(_FakeRuntime):
        """在生成完整查询事件后模拟队列 drop 计数增长。"""

        def __init__(self):
            """创建默认不注入丢包的查询替身。

            Returns:
                None。
            """

            super().__init__()
            self.drop_during_query = False

        def query_trading_accounts(self, request_id):
            """生成资金查询后增加 drop 计数。

            Args:
                request_id: 请求标识。

            Returns:
                None。
            """

            super().query_trading_accounts(request_id)
            if self.drop_during_query:
                self.dropped_events += 1

    runtime = _DropDuringQueryRuntime()
    broker = _connected_broker(runtime)
    runtime.drop_during_query = True

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.get_account_info()

    assert exc_info.value.code == NATIVE_CALL_FAILED
    assert exc_info.value.details["dropped_before"] == 0
    assert exc_info.value.details["dropped_after"] == 1


@pytest.mark.parametrize("corruption", ["record_count", "request_type"])
def test_query_end_integrity_mismatch_fails_closed(corruption) -> None:
    """验证 query_end 的记录数或请求类型不匹配都会拒绝快照。

    Args:
        corruption: 要注入的 query_end 损坏类型。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    if corruption == "record_count":
        runtime.query_end_record_count_delta = 1
    else:
        runtime.query_end_request_type_override = broker_module.native_api.REQUEST_QUERY_POSITION
    broker = _broker_for_runtime(runtime)

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.connect()

    assert exc_info.value.code == NATIVE_CALL_FAILED


@pytest.mark.parametrize("drain_max_events", [None, 1024])
def test_query_bucket_over_512_fails_instead_of_truncating(drain_max_events) -> None:
    """验证默认分批和单批 drain 下的 513 条 record 都会失败关闭。

    Args:
        drain_max_events: ``None`` 使用真实默认 256，1024 覆盖单批溢出路径。

    Returns:
        None。
    """

    class _OverflowRuntime(_FakeRuntime):
        """生成超过 broker 单请求安全上限的持仓查询。"""

        def query_positions(self, request_id):
            """生成 513 条持仓记录和匹配的 query_end。

            Args:
                request_id: 请求标识。

            Returns:
                None。
            """

            for index in range(513):
                self.events.append(
                    self._event(
                        request_id,
                        "position",
                        {
                            "exchange": "SSE",
                            "security": str(index).zfill(6),
                            "current_position": 1,
                            "available_position": 1,
                        },
                    )
                )
            self.events.append(
                self._event(
                    request_id,
                    "query_end",
                    {
                        "error_id": 0,
                        "request_type": broker_module.native_api.REQUEST_QUERY_POSITION,
                        "record_count": 513,
                    },
                )
            )

    runtime = _OverflowRuntime()
    broker = _broker_for_runtime(
        runtime,
        query_timeout=0.1,
        **({} if drain_max_events is None else {"drain_max_events": drain_max_events}),
    )

    with pytest.raises(HuaxinNativeUnavailableError) as exc_info:
        broker.connect()

    assert exc_info.value.code == NATIVE_CALL_FAILED
    assert exc_info.value.details["event_count"] == 513


@pytest.mark.asyncio
async def test_limit_place_and_exact_cancel_contract(order_identity_journal_path) -> None:
    """验证未显式市价类型拒绝、限价字段和精确撤单身份完整透传。

    Args:
        order_identity_journal_path: 私有订单身份 journal 路径。

    Returns:
        None。
    """

    runtime = _FakeRuntime(session_id=-2_000_000_001)
    broker = _connected_broker(
        runtime,
        enable_trading=True,
        enable_cancel=True,
    )

    for invalid_amount in (0, 1.5, 2_147_483_648, float("inf")):
        with pytest.raises(ValueError, match="数量"):
            await broker.buy(
                "511880.XSHG",
                invalid_amount,
                100.2,
            )
    for invalid_price in (float("nan"), float("inf"), -1.0):
        with pytest.raises(ValueError, match="价格"):
            await broker.buy(
                "511880.XSHG",
                100,
                invalid_price,
            )
    assert runtime.place_calls == []

    order_id = await broker.buy(
        "511880.XSHG",
        100,
        100.2,
    )
    result = broker.get_last_order_wait_result(order_id)
    assert result["submission_state"] == "accepted"
    request = runtime.place_calls[0][1]
    assert (request.exchange, request.security, request.direction) == ("SSE", "511880", "buy")
    assert (request.limit_price, request.amount) == (100.2, 100)
    assert (request.order_price_type, request.time_condition, request.volume_condition) == (
        "limit",
        "gfd",
        "any",
    )

    assert await broker.cancel_order(order_id) is True
    cancel = runtime.cancel_calls[0][1]
    assert cancel.order_sys_id == "SYS1"


def test_market_order_style_keeps_legacy_positional_price_and_explicit_type() -> None:
    """验证新增 market_type 不改变 MarketOrderStyle 的旧位置参数语义。

    Returns:
        None。
    """

    legacy = MarketOrderStyle(10.5)
    explicit = MarketOrderStyle(10.5, market_type="opponent_best")

    assert legacy.limit_price == 10.5
    assert legacy.market_type is None
    assert explicit.limit_price == 10.5
    assert explicit.market_type == "opponent_best"


@pytest.mark.asyncio
async def test_pure_memory_order_ref_atomic_increment() -> None:
    """验证纯内存模式下 OrderRef 以登录 MaxOrderRef 为基准原子自增。"""
    runtime = _FakeRuntime()
    runtime.max_order_ref = 48
    broker = _connected_broker(runtime, enable_trading=True)

    first_order_id = await broker.buy("511880.XSHG", 100, 100.2)
    first_result = broker.get_last_order_wait_result(first_order_id)
    assert first_result["order_ref"] == 49

    second_order_id = await broker.buy("511880.XSHG", 100, 100.2)
    second_result = broker.get_last_order_wait_result(second_order_id)
    assert second_result["order_ref"] == 50
    assert len(runtime.place_calls) == 2

    # 重启后以新登录 max_order_ref 递增
    restarted_runtime = _FakeRuntime()
    restarted_runtime.max_order_ref = 100
    restarted = _connected_broker(restarted_runtime, enable_trading=True)
    third_id = await restarted.buy("511880.XSHG", 100, 100.2)
    assert restarted.get_last_order_wait_result(third_id)["order_ref"] == 101


@pytest.mark.asyncio
async def test_strategy_cancel_inside_engine_loop_is_non_blocking(monkeypatch) -> None:
    """验证策略在 Engine 自身事件循环调用撤单时只投递任务而不等待自身。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    completed = asyncio.Event()

    class _AsyncCancelBroker:
        """用一次事件循环切换模拟异步券商撤单。"""

        async def cancel_order(self, order_id):
            """让出事件循环后确认已接受撤单。

            Args:
                order_id: 精确券商订单号。

            Returns:
                bool: 始终返回 True。
            """

            await asyncio.sleep(0)
            completed.set()
            return True

    def _forbid_self_wait(*args, **kwargs):
        """禁止回归路径再次对当前事件循环调用线程安全同步等待。

        Args:
            *args: 被禁止调用的原始位置参数。
            **kwargs: 被禁止调用的原始关键字参数。

        Returns:
            None；总是抛出断言。

        Raises:
            AssertionError: 一旦回归到 run_coroutine_threadsafe 自锁路径。
        """

        raise AssertionError("不得同步等待当前 Engine 事件循环")

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _forbid_self_wait)
    order = Order(order_id="local-1", security="511880.XSHG", amount=100)
    order._broker_order_id = "SYS1"
    set_current_engine(
        SimpleNamespace(
            broker=_AsyncCancelBroker(),
            _loop=asyncio.get_running_loop(),
        )
    )
    try:
        assert strategy_cancel_order(order) is True
        await asyncio.wait_for(completed.wait(), timeout=0.5)
        await asyncio.sleep(0)
        assert order.status == OrderStatus.canceling
    finally:
        set_current_engine(None)


@pytest.mark.parametrize(
    ("security", "market_type", "price", "expected"),
    [
        ("511880.XSHG", "home_best", 100.2, ("home_best", "gfd", "any")),
        ("511880.XSHG", "opponent_best", 100.2, ("opponent_best", "gfd", "any")),
        ("511880.XSHG", "five_level_ioc", 100.2, ("five_level", "ioc", "any")),
        ("511880.XSHG", "five_level_to_limit", 100.2, ("five_level", "gfd", "any")),
        ("159001.XSHE", "home_best", None, ("home_best", "gfd", "any")),
        ("159001.XSHE", "opponent_best", None, ("opponent_best", "gfd", "any")),
        ("159001.XSHE", "five_level_ioc", None, ("five_level", "ioc", "any")),
        ("159001.XSHE", "immediate_or_cancel", None, ("any_price", "ioc", "any")),
        ("159001.XSHE", "fill_or_kill", None, ("any_price", "ioc", "all")),
    ],
)
@pytest.mark.asyncio
async def test_market_order_matrix_maps_exchange_specific_conditions(
    security, market_type, price, expected, order_identity_journal_path
) -> None:
    """验证沪深市价白名单映射，公共相同组合复用同一 canonical 三元组。

    Args:
        security: 带交易所后缀的证券代码。
        market_type: 公共原生市价类型。
        price: 上交所保护价或深市可选保护价。
        expected: 预期 native 价格/时间/数量三元组。
        order_identity_journal_path: 私有订单身份 journal 路径。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    broker = _connected_broker(
        runtime,
        enable_trading=True,
        order_identity_journal_path=order_identity_journal_path,
    )
    order_id = await broker.buy(
        security,
        100,
        price,
        market=True,
        extra={
            "idempotency_key": f"market-{security}-{market_type}",
            "market_type": market_type,
            "investor_id": "investor",
            "shareholder_id": "shareholder",
        },
    )

    request = runtime.place_calls[0][1]
    assert (request.order_price_type, request.time_condition, request.volume_condition) == expected
    assert request.limit_price == pytest.approx(price or 0.0)
    result = broker.get_last_order_wait_result(order_id)
    assert result["style_type"] == "market"
    assert result["market_type"] == market_type
    assert broker.health_snapshot()["security_order_constraints_ready"] is True


@pytest.mark.parametrize(
    ("security", "market_type"),
    [
        ("511880.XSHG", "immediate_or_cancel"),
        ("511880.XSHG", "fill_or_kill"),
        ("159001.XSHE", "five_level_to_limit"),
        ("159001.XSHE", "unknown_type"),
    ],
)
@pytest.mark.asyncio
async def test_market_order_matrix_rejects_cross_exchange_and_unknown_types(
    security, market_type, order_identity_journal_path
) -> None:
    """验证跨交易所或未知市价类型不会降级并且零 native 写调用。

    Args:
        security: 带交易所后缀的证券代码。
        market_type: 不适用或未知类型。
        order_identity_journal_path: 私有订单身份 journal 路径。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    broker = _connected_broker(
        runtime,
        enable_trading=True,
        order_identity_journal_path=order_identity_journal_path,
    )

    with pytest.raises(HuaxinTradingDisabledError) as exc_info:
        await broker.buy(
            security,
            100,
            100.2,
            market=True,
            extra={"idempotency_key": "bad-market", "market_type": market_type},
        )

    assert exc_info.value.code == HUAXIN_MARKET_ORDER_DISABLED
    assert runtime.place_calls == []


@pytest.mark.asyncio
async def test_sse_market_order_requires_positive_protection_price_before_native_query(
    order_identity_journal_path,
) -> None:
    """验证上交所市价缺少保护价时在证券查询和写调用前同步拒绝。

    Args:
        order_identity_journal_path: 私有订单身份 journal 路径。

    Returns:
        None。
    """

    runtime = _FakeRuntime()
    broker = _connected_broker(
        runtime,
        enable_trading=True,
        order_identity_journal_path=order_identity_journal_path,
    )

    with pytest.raises(ValueError, match="保护限价"):
        await broker.sell(
            "511880.XSHG",
            100,
            None,
            market=True,
            extra={"idempotency_key": "missing-protect", "market_type": "opponent_best"},
        )

    assert runtime.place_calls == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("limit_buy_unit", 0, "limit_buy_unit"),
        ("min_limit_buy", 200, "数量"),
        ("price_tick", 0.0, "price_tick"),
        ("upper_limit_price", 99.0, "必须位于"),
    ],
)
@pytest.mark.asyncio
async def test_security_constraints_fail_closed_before_order_write(
    field, value, message, order_identity_journal_path
) -> None:
    """验证证券单位、数量、状态、tick 和涨跌停异常均零 native 写。

    Args:
        field: 要破坏的 query_security 字段。
        value: 注入的非法值。
        message: 预期错误文本片段。
        order_identity_journal_path: 私有订单身份 journal 路径。

    Returns:
        None。
    """

    class _ConstraintRuntime(_FakeRuntime):
        """为单个证券约束字段注入异常的 Runtime 替身。"""

        def query_security(self, request_id, exchange="", security=""):
            """返回一条带指定损坏字段的证券记录。

            Args:
                request_id: 请求标识。
                exchange: 交易所过滤。
                security: 证券代码过滤。

            Returns:
                None。
            """

            row = {
                "exchange": exchange,
                "security": security,
                "security_type": 1,
                "order_unit": 1,
                "limit_buy_unit": 100,
                "limit_sell_unit": 1,
                "min_limit_buy": 100,
                "max_limit_buy": 1_000_000,
                "min_limit_sell": 1,
                "max_limit_sell": 1_000_000,
                "market_buy_unit": 100,
                "market_sell_unit": 1,
                "min_market_buy": 100,
                "max_market_buy": 1_000_000,
                "min_market_sell": 1,
                "max_market_sell": 1_000_000,
                "volume_multiple": 1,
                "price_tick": 0.001,
                "security_status": 0,
                "has_price_limit": True,
                "upper_limit_price": 120.0,
                "lower_limit_price": 80.0,
            }
            row[field] = value
            self._query(request_id, "security", row)

    runtime = _ConstraintRuntime()
    broker = _connected_broker(
        runtime,
        enable_trading=True,
        order_identity_journal_path=order_identity_journal_path,
    )

    with pytest.raises((ValueError, HuaxinTradingDisabledError), match=message):
        await broker.buy(
            "511880.XSHG",
            100,
            100.2,
            extra={"idempotency_key": f"constraint-{field}"},
        )

    assert runtime.place_calls == []


def test_security_status_mask_is_not_filtered_by_client_allowlist() -> None:
    """验证可解码的柜台状态位掩码不会被客户端自建白名单误拦截。"""

    broker = HuaxinBroker("acct")
    broker._validate_security_order_constraints(
        {
            "exchange": "SSE",
            "security": "511880",
            "security_type": 1,
            "order_unit": 1,
            "limit_buy_unit": 100,
            "limit_sell_unit": 1,
            "min_limit_buy": 100,
            "max_limit_buy": 1_000_000,
            "min_limit_sell": 1,
            "max_limit_sell": 1_000_000,
            "market_buy_unit": 100,
            "market_sell_unit": 1,
            "min_market_buy": 100,
            "max_market_buy": 1_000_000,
            "min_market_sell": 1,
            "max_market_sell": 1_000_000,
            "volume_multiple": 1,
            "price_tick": 0.001,
            "security_status": 30_064_771_072,
            "has_price_limit": True,
            "upper_limit_price": 120.0,
            "lower_limit_price": 80.0,
        },
        exchange="SSE",
        security="511880",
        side="buy",
        quantity=100,
        price=100.2,
        market=False,
    )

    assert broker._security_order_constraints_ready is True
