"""
作者: BruceLee
文件职责: 验证 Trader-only Python 合同、沪深委托矩阵、定长 POD 和结构化事件解码。
主要输入: 脱敏会话配置、受控委托/撤单请求与伪造证券、持仓和委托事件。
主要输出: pytest 断言，保护凭据、官方字段宽度、写请求矩阵和事件 shape。
上下游关系: native.py 公开 dataclass 与 flat ABI 解码器；不加载厂商 SDK。
关键环境或配置: 纯离线单测，不 dlopen、不连网、不创建 TORA runtime。
"""

from __future__ import annotations

import ctypes

import pytest

import bullet_trade.integrations.huaxin.native as native_module
from bullet_trade.integrations.huaxin import (
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_SECURITY,
    NativeCancelOrderRequest,
    NativeEvent,
    NativeLimitOrderRequest,
    NativeOrderRequest,
    NativeSessionConfig,
)


@pytest.mark.unit
def test_session_config_mapping_hides_credentials_from_repr() -> None:
    """验证配置映射回退 account_id，且 repr 不泄露凭据和终端身份。

    Returns:
        None；必填映射和脱敏 repr 满足合同即通过。
    """

    config = NativeSessionConfig.from_mapping(
        {
            "flow_path": "/opt/bullet-trade/flow",
            "trade_front": "tcp://127.0.0.1:7000",
            "account_id": "masked-account",
            "password": "masked-password",
            "dynamic_password": "masked-dynamic",
            "terminal_info": "masked-terminal",
            "mac_address": "00-11-22-33-44-55",
            "user_product_info": "BT",
        }
    )

    rendered = repr(config)
    assert config.login_account == "masked-account"
    assert "masked-password" not in rendered
    assert "masked-dynamic" not in rendered
    assert "masked-terminal" not in rendered
    assert "00-11-22-33-44-55" not in rendered

    raw = native_module._session_config_to_raw(
        config,
        native_module.TRADER_VENDOR_SCHEMA_ID,
        native_module.TRADER_FIELD_SET_VERSION,
    )
    assert ctypes.sizeof(raw) == 1312
    assert raw.struct_size == 1312
    assert native_module._SessionConfig.user_product_info_size.offset == 740
    assert native_module._SessionConfig.terminal_info_size.offset == 792
    assert native_module._SessionConfig.mac_address_size.offset == 1052
    assert native_module._SessionConfig.interface_address_size.offset == 1076
    assert bytes(raw.user_product_info[: raw.user_product_info_size]) == b"BT"
    assert bytes(raw.terminal_info[: raw.terminal_info_size]) == b"masked-terminal"
    assert bytes(raw.mac_address[: raw.mac_address_size]) == b"00-11-22-33-44-55"
    assert config.interface_product_info == ""
    assert raw.interface_product_info_size == 0
    native_module._clear_structure(raw)
    assert native_module._structure_bytes(raw) == bytes(1312)


@pytest.mark.unit
@pytest.mark.parametrize("missing_field", ("user_product_info", "terminal_info"))
def test_session_config_requires_official_real_login_identity(missing_field: str) -> None:
    """验证真实登录不再给 UserProductInfo 默认值，且 TerminalInfo 独立必填。

    Args:
        missing_field: 本例删除的官方实盘登录必填字段。

    Returns:
        None；缺失字段在构造 C ABI 前被拒绝即通过。
    """

    config = {
        "flow_path": "/opt/bullet-trade/flow",
        "trade_front": "tcp://127.0.0.1:7000",
        "account_id": "masked-account",
        "password": "masked-password",
        "terminal_info": "masked-terminal",
        "user_product_info": "BT",
    }
    del config[missing_field]

    with pytest.raises(ValueError, match=missing_field):
        NativeSessionConfig.from_mapping(config)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field_name", "field_value", "expected_limit"),
    (
        ("user_product_info", "BulletTrade", 10),
        ("mac_address", "A" * 21, 20),
        ("terminal_info", "T" * 256, 255),
    ),
)
def test_session_config_rejects_text_wider_than_official_login_fields(
    field_name: str,
    field_value: str,
    expected_limit: int,
) -> None:
    """验证 Python 在调用 native 前执行官方 ``char[N]`` 的 N-1 字节门禁。

    Args:
        field_name: 本例越界的会话字段。
        field_value: 超过官方有效字节数的 ASCII 文本。
        expected_limit: 错误信息中应出现的最大字节数。

    Returns:
        None；UserProductInfo/MacAddress/TerminalInfo 越界均被拒绝即通过。
    """

    values = {
        "flow_path": "/opt/bullet-trade/flow",
        "trade_front": "tcp://127.0.0.1:7000",
        "account_id": "masked-account",
        "password": "masked-password",
        "terminal_info": "masked-terminal",
        "mac_address": "00-11-22-33-44-55",
        "user_product_info": "BT",
    }
    values[field_name] = field_value
    config = NativeSessionConfig.from_mapping(values)

    with pytest.raises(ValueError, match=f"不能超过 {expected_limit} 字节"):
        native_module._session_config_to_raw(
            config,
            native_module.TRADER_VENDOR_SCHEMA_ID,
            native_module.TRADER_FIELD_SET_VERSION,
        )


@pytest.mark.unit
def test_limit_and_cancel_payloads_fit_flat_request_and_reject_partial_identity() -> None:
    """验证写请求不越出 192-byte 合同，且撤单不接受部分会话身份。

    Returns:
        None；大小和负例均满足即通过。
    """

    order_payload = native_module._limit_order_payload(
        NativeLimitOrderRequest(
            exchange="SSE",
            investor_id="investor",
            shareholder_id="shareholder",
            security="511880",
            direction="buy",
            limit_price=100.0,
            amount=100,
            order_ref=1,
        )
    )
    cancel_payload = native_module._cancel_order_payload(
        NativeCancelOrderRequest(exchange="SSE", order_sys_id="system-order")
    )

    assert len(order_payload) <= native_module.REQUEST_PAYLOAD_CAPACITY
    assert len(cancel_payload) <= native_module.REQUEST_PAYLOAD_CAPACITY
    with pytest.raises(ValueError, match="必须同时完整"):
        native_module._cancel_order_payload(
            NativeCancelOrderRequest(
                exchange="SSE",
                order_sys_id="system-order",
                front_id=1,
            )
        )


@pytest.mark.unit
@pytest.mark.parametrize("session_id", (-1, -(1 << 31), (1 << 31) - 1))
def test_cancel_payload_preserves_signed_int32_session_id(session_id: int) -> None:
    """验证撤单会话三元组允许非零有符号 SessionID。

    Args:
        session_id: 待验证的有符号 int32 边界值。

    Returns:
        None；二进制 payload 保留原始符号即通过。
    """

    payload = native_module._cancel_order_payload(
        NativeCancelOrderRequest(
            exchange="SSE",
            front_id=7,
            session_id=session_id,
            order_ref=41,
        )
    )
    raw = native_module._CancelOrderRequest.from_buffer_copy(payload)
    assert (raw.front_id, raw.session_id, raw.order_ref) == (7, session_id, 41)


@pytest.mark.unit
def test_cancel_payload_rejects_zero_or_out_of_range_session_id() -> None:
    """验证完整撤单三元组拒绝零和越界 SessionID。

    Returns:
        None；所有非法值都在调用 native 前失败即通过。
    """

    with pytest.raises(ValueError, match="必须同时完整"):
        native_module._cancel_order_payload(
            NativeCancelOrderRequest(exchange="SSE", front_id=7, session_id=0, order_ref=41)
        )
    with pytest.raises(ValueError, match="有符号 int32"):
        native_module._cancel_order_payload(
            NativeCancelOrderRequest(
                exchange="SSE",
                front_id=7,
                session_id=1 << 31,
                order_ref=41,
            )
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    (
        "exchange",
        "order_price_type",
        "time_condition",
        "volume_condition",
        "expected_codes",
    ),
    (
        ("SSE", "limit", "gfd", "any", (1, 1, 1)),
        ("SSE", "home_best", "gfd", "any", (2, 1, 1)),
        ("SSE", "opponent_best", "gfd", "any", (3, 1, 1)),
        ("SSE", "five_level", "ioc", "any", (4, 2, 1)),
        ("SSE", "five_level", "gfd", "any", (4, 1, 1)),
        ("SZSE", "limit", "gfd", "any", (1, 1, 1)),
        ("SZSE", "home_best", "gfd", "any", (2, 1, 1)),
        ("SZSE", "opponent_best", "gfd", "any", (3, 1, 1)),
        ("SZSE", "five_level", "ioc", "any", (4, 2, 1)),
        ("SZSE", "any_price", "ioc", "any", (5, 2, 1)),
        ("SZSE", "any_price", "ioc", "all", (5, 2, 2)),
        ("BSE", "limit", "gfd", "any", (1, 1, 1)),
    ),
)
def test_order_payload_accepts_only_official_exchange_matrix(
    exchange: str,
    order_price_type: str,
    time_condition: str,
    volume_condition: str,
    expected_codes: tuple,
) -> None:
    """验证沪深官方矩阵和既有限价合同编码为稳定自有枚举。

    Args:
        exchange: 本例交易所。
        order_price_type: canonical 价格类型。
        time_condition: canonical 时效条件。
        volume_condition: canonical 成交量条件。
        expected_codes: C ABI 中预期的三个稳定整数。

    Returns:
        None；全部受支持组合均保持 160-byte POD 且不携带厂商原始字符即通过。
    """

    payload = native_module._order_payload(
        NativeOrderRequest(
            exchange=exchange,
            investor_id="investor",
            shareholder_id="shareholder",
            security="511880" if exchange == "SSE" else "000001",
            direction="buy",
            order_price_type=order_price_type,
            time_condition=time_condition,
            volume_condition=volume_condition,
            limit_price=100.0,
            amount=100,
            order_ref=1,
        )
    )
    raw = native_module._OrderRequest.from_buffer_copy(payload)

    assert len(payload) == ctypes.sizeof(native_module._LimitOrderRequest) == 160
    assert len(payload) <= native_module.REQUEST_PAYLOAD_CAPACITY
    assert native_module._OrderRequest.direction.offset == 20
    assert native_module._OrderRequest.order_price_type.offset == 21
    assert native_module._OrderRequest.time_condition.offset == 22
    assert native_module._OrderRequest.volume_condition.offset == 23
    assert native_module._OrderRequest.limit_price.offset == 24
    assert native_module._OrderRequest.exchange.offset == 40
    assert (
        raw.order_price_type,
        raw.time_condition,
        raw.volume_condition,
    ) == expected_codes


@pytest.mark.unit
@pytest.mark.parametrize(
    ("exchange", "order_price_type", "time_condition", "volume_condition"),
    (
        ("SSE", "any_price", "ioc", "any"),
        ("SSE", "home_best", "ioc", "any"),
        ("SZSE", "five_level", "gfd", "any"),
        ("SZSE", "any_price", "gfd", "all"),
        ("BSE", "home_best", "gfd", "any"),
    ),
)
def test_order_payload_rejects_cross_exchange_or_unknown_combinations(
    exchange: str,
    order_price_type: str,
    time_condition: str,
    volume_condition: str,
) -> None:
    """验证跨交易所或非官方组合在构造 native request 前失败。

    Args:
        exchange: 本例交易所。
        order_price_type: canonical 价格类型。
        time_condition: canonical 时效条件。
        volume_condition: canonical 成交量条件。

    Returns:
        None；组合均被稳定拒绝即通过。
    """

    order = NativeOrderRequest(
        exchange=exchange,
        investor_id="investor",
        shareholder_id="shareholder",
        security="000001",
        direction="buy",
        order_price_type=order_price_type,
        time_condition=time_condition,
        volume_condition=volume_condition,
        limit_price=100.0,
        amount=100,
        order_ref=1,
    )

    with pytest.raises(ValueError, match="当前交易所不支持"):
        native_module._order_payload(order)


@pytest.mark.unit
def test_place_order_rejects_unknown_combo_before_native_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证未知组合不会进入统一 native submit 函数。

    Args:
        monkeypatch: pytest 临时替换 runtime submit 的夹具。

    Returns:
        None；调用计数保持零即通过。
    """

    runtime = object.__new__(native_module.NativeRuntime)
    calls = []
    monkeypatch.setattr(runtime, "_submit_payload", lambda *args: calls.append(args))
    order = NativeOrderRequest(
        exchange="SSE",
        investor_id="investor",
        shareholder_id="shareholder",
        security="511880",
        direction="buy",
        order_price_type="any_price",
        time_condition="ioc",
        volume_condition="any",
        limit_price=100.0,
        amount=100,
        order_ref=1,
    )

    with pytest.raises(ValueError, match="当前交易所不支持"):
        runtime.place_order(1, order)
    assert calls == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("exchange", "order_price_type", "time_condition", "volume_condition", "price", "valid"),
    (
        ("SSE", "home_best", "gfd", "any", 0.0, False),
        ("SSE", "five_level", "ioc", "any", 0.0, False),
        ("SZSE", "any_price", "ioc", "any", 0.0, True),
        ("SZSE", "any_price", "ioc", "all", 0.0, True),
        ("SZSE", "limit", "gfd", "any", 0.0, False),
        ("SZSE", "home_best", "gfd", "any", -0.01, False),
    ),
)
def test_order_payload_enforces_exchange_protection_price_contract(
    exchange: str,
    order_price_type: str,
    time_condition: str,
    volume_condition: str,
    price: float,
    valid: bool,
) -> None:
    """验证沪市市价必须保护价而深市官方矩阵允许零价格。

    Args:
        exchange: 本例交易所。
        order_price_type: canonical 价格类型。
        time_condition: canonical 时效条件。
        volume_condition: canonical 成交量条件。
        price: 本例 LimitPrice/保护价。
        valid: 是否应通过 payload 校验。

    Returns:
        None；价格合同与官方说明一致即通过。
    """

    order = NativeOrderRequest(
        exchange=exchange,
        investor_id="investor",
        shareholder_id="shareholder",
        security="511880" if exchange == "SSE" else "000001",
        direction="buy",
        order_price_type=order_price_type,
        time_condition=time_condition,
        volume_condition=volume_condition,
        limit_price=price,
        amount=100,
        order_ref=1,
    )

    if valid:
        assert len(native_module._order_payload(order)) == 160
    else:
        with pytest.raises(ValueError, match="limit_price"):
            native_module._order_payload(order)


@pytest.mark.unit
def test_order_payload_rejects_vendor_raw_enum_values() -> None:
    """验证调用方不能把 TORA 原始字符直接塞入稳定委托请求。

    Returns:
        None；原始 `3/1/1` 字符串在 C ABI 之前被拒绝即通过。
    """

    order = NativeOrderRequest(
        exchange="SSE",
        investor_id="investor",
        shareholder_id="shareholder",
        security="511880",
        direction="buy",
        order_price_type="3",
        time_condition="1",
        volume_condition="1",
        limit_price=100.0,
        amount=100,
        order_ref=1,
    )

    with pytest.raises(ValueError, match="order_price_type 不在受控枚举"):
        native_module._order_payload(order)


@pytest.mark.unit
@pytest.mark.parametrize("invalid_amount", (True, False, 0, -1, 1.0, 1 << 31, (1 << 32) - 1))
def test_limit_order_rejects_non_int32_amount(invalid_amount: object) -> None:
    """验证数量只接受非 bool 的 Python 整数 1..INT32_MAX。

    Args:
        invalid_amount: bool、非整数或越出正 int32 的数量。

    Returns:
        None；所有值均在 ctypes uint32 截断前被拒绝即通过。
    """

    order = NativeLimitOrderRequest(
        exchange="SSE",
        investor_id="investor",
        shareholder_id="shareholder",
        security="511880",
        direction="buy",
        limit_price=100.0,
        amount=invalid_amount,  # type: ignore[arg-type]
        order_ref=1,
    )

    with pytest.raises(ValueError, match="1..INT32_MAX.*非 bool 整数"):
        native_module._limit_order_payload(order)


@pytest.mark.unit
@pytest.mark.parametrize(
    "invalid_price",
    (float("nan"), float("inf"), float("-inf"), 0.0, -0.01),
)
def test_limit_order_rejects_non_finite_or_non_positive_price(invalid_price: float) -> None:
    """验证 NaN、无穷值、零和负数不会进入 C ABI 限价委托。

    Args:
        invalid_price: 非有限或非正价格。

    Returns:
        None；所有值均被统一价格门禁拒绝即通过。
    """

    order = NativeLimitOrderRequest(
        exchange="SSE",
        investor_id="investor",
        shareholder_id="shareholder",
        security="511880",
        direction="buy",
        limit_price=invalid_price,
        amount=100,
        order_ref=1,
    )

    with pytest.raises(ValueError, match="有限且大于 0"):
        native_module._limit_order_payload(order)


@pytest.mark.unit
def test_limit_order_accepts_exact_int32_max_amount() -> None:
    """验证 INT32_MAX 是可传给官方 ``VolumeTotalOriginal`` 的精确上界。

    Returns:
        None；边界值在 C ABI payload 中保持不变即通过。
    """

    payload = native_module._limit_order_payload(
        NativeLimitOrderRequest(
            exchange="SSE",
            investor_id="investor",
            shareholder_id="shareholder",
            security="511880",
            direction="buy",
            limit_price=100.0,
            amount=(1 << 31) - 1,
            order_ref=1,
        )
    )
    raw = native_module._LimitOrderRequest.from_buffer_copy(payload)

    assert raw.amount == (1 << 31) - 1


@pytest.mark.unit
def test_security_event_exposes_limit_and_market_write_constraints() -> None:
    """验证证券查询回报完整暴露写前数量、价格和状态约束。

    Returns:
        None；限价/市价单位、上下限价和 int64 状态均逐值透传即通过。
    """

    raw = native_module._SecurityEvent(
        market_id=1,
        security_type=2,
        order_unit=3,
        limit_buy_unit=100,
        limit_sell_unit=100,
        min_limit_buy=100,
        max_limit_buy=1_000_000,
        min_limit_sell=100,
        max_limit_sell=1_000_000,
        market_buy_unit=100,
        market_sell_unit=100,
        min_market_buy=100,
        max_market_buy=500_000,
        min_market_sell=100,
        max_market_sell=500_000,
        volume_multiple=100,
        has_price_limit=1,
        day_trading=1,
        security_status=(1 << 40) + 7,
        price_tick=0.001,
        pre_close_price=101.1,
        upper_limit_price=111.2,
        lower_limit_price=91.0,
    )
    for field_name, text, capacity in (
        ("exchange", "SSE", native_module.EXCHANGE_CAPACITY),
        ("security", "511880", native_module.SECURITY_CAPACITY),
        ("security_name", "YH", native_module.SECURITY_NAME_CAPACITY),
        ("short_name", "YH", native_module.SECURITY_NAME_CAPACITY),
    ):
        native_module._assign_text(raw, field_name, text, capacity)
    payload = native_module._structure_bytes(raw)
    data = native_module._decode_event_data(EVENT_SECURITY, payload)

    assert len(payload) == ctypes.sizeof(native_module._SecurityEvent) == 360
    assert native_module._SecurityEvent.market_buy_unit.offset == 284
    assert native_module._SecurityEvent.security_status.offset == 320
    assert native_module._SecurityEvent.upper_limit_price.offset == 344
    assert native_module._SecurityEvent.lower_limit_price.offset == 352
    assert data["market_buy_unit"] == 100
    assert data["market_sell_unit"] == 100
    assert data["min_market_buy"] == 100
    assert data["max_market_buy"] == 500_000
    assert data["min_market_sell"] == 100
    assert data["max_market_sell"] == 500_000
    assert data["volume_multiple"] == 100
    assert data["has_price_limit"] is True
    assert data["day_trading"] is True
    assert data["security_status"] == (1 << 40) + 7
    assert data["price_tick"] == 0.001
    assert data["upper_limit_price"] == 111.2
    assert data["lower_limit_price"] == 91.0


@pytest.mark.unit
def test_position_event_preserves_authoritative_balance_and_in_flight_fields() -> None:
    """验证持仓总量、可用量、冻结和在途字段逐项忠实透传。

    Returns:
        None；权威字段未被昨仓、今仓和冻结量重新推导即通过。
    """

    raw = native_module._PositionEvent(
        current_position=901,
        available_position=407,
        history_position=701,
        history_frozen=11,
        today_bs=113,
        today_bs_frozen=13,
        today_pr=17,
        today_pr_frozen=19,
        total_cost=67890.5,
        today_sm=23,
        today_sm_frozen=29,
        pre_position=659,
        pre_frozen=31,
        repay_untrade_volume=37,
        repay_transfer_untrade_volume=41,
        collateral_buy_untrade_volume=43,
        credit_buy_untrade_volume=47,
        credit_sell_untrade_volume=53,
        history_position_price=61.25,
        open_position_cost=71.75,
        collateral_buy_untrade_amount=73.25,
        credit_buy_untrade_amount=79.5,
        credit_sell_untrade_amount=83.75,
    )
    for field_name, text, capacity in (
        ("exchange", "SZSE", native_module.EXCHANGE_CAPACITY),
        ("investor_id", "investor", native_module.INVESTOR_CAPACITY),
        ("shareholder_id", "shareholder", native_module.SHAREHOLDER_CAPACITY),
        ("security", "000001", native_module.SECURITY_CAPACITY),
        ("trading_day", "20260817", native_module.DATE_CAPACITY),
    ):
        native_module._assign_text(raw, field_name, text, capacity)
    payload = native_module._structure_bytes(raw)
    data = native_module._decode_event_data(EVENT_POSITION, payload)

    assert len(payload) == 248
    assert native_module._PositionEvent.current_position.offset == 124
    assert native_module._PositionEvent.total_cost.offset == 160
    assert native_module._PositionEvent.today_sm.offset == 168
    assert native_module._PositionEvent.history_position_price.offset == 208
    assert len(payload) <= native_module.OWNED_EVENT_PAYLOAD_CAPACITY
    assert data["current_position"] == 901
    assert data["current_position"] != 701 + 113 + 17 + 23
    assert data["available_position"] == 407
    assert data["available_position"] != 901 - 11 - 13 - 19 - 29
    assert data["today_pr"] == 17
    assert data["today_sm"] == 23
    assert data["history_frozen"] == 11
    assert data["today_bs_frozen"] == 13
    assert data["today_pr_frozen"] == 19
    assert data["today_sm_frozen"] == 29
    assert data["pre_position"] == 659
    assert data["pre_frozen"] == 31
    assert data["repay_untrade_volume"] == 37
    assert data["repay_transfer_untrade_volume"] == 41
    assert data["collateral_buy_untrade_volume"] == 43
    assert data["credit_buy_untrade_volume"] == 47
    assert data["credit_sell_untrade_volume"] == 53
    assert data["collateral_buy_untrade_amount"] == 73.25
    assert data["credit_buy_untrade_amount"] == 79.5
    assert data["credit_sell_untrade_amount"] == 83.75


@pytest.mark.unit
def test_order_event_decodes_to_stable_adapter_shape() -> None:
    """验证委托 typed payload 保留关联身份并提供稳定 event_name/data。

    Returns:
        None；解码后的委托字段与输入一致即通过。
    """

    raw = native_module._OrderEvent(
        direction=0,
        order_price_type=3,
        time_condition=1,
        volume_condition=1,
        order_status=ord("3"),
        submit_status=ord("0"),
        limit_price=100.5,
        amount=100,
        filled=20,
        canceled=0,
        front_id=7,
        session_id=8,
        order_ref=9,
    )
    for field_name, text, capacity in (
        ("exchange", "SSE", native_module.EXCHANGE_CAPACITY),
        ("investor_id", "investor", native_module.INVESTOR_CAPACITY),
        ("shareholder_id", "shareholder", native_module.SHAREHOLDER_CAPACITY),
        ("security", "511880", native_module.SECURITY_CAPACITY),
        ("order_local_id", "local", native_module.ORDER_LOCAL_ID_CAPACITY),
        ("order_sys_id", "system", native_module.ORDER_SYS_ID_CAPACITY),
        ("trading_day", "20260817", native_module.DATE_CAPACITY),
        ("insert_time", "09:31:00", native_module.TIME_CAPACITY),
        ("status_message", "accepted", native_module.ERROR_MESSAGE_CAPACITY),
    ):
        native_module._assign_text(raw, field_name, text, capacity)
    payload = native_module._structure_bytes(raw)
    data = native_module._decode_event_data(EVENT_ORDER, payload)
    event = NativeEvent(
        event_type=EVENT_ORDER,
        sequence=1,
        received_ns=2,
        request_id=3,
        vendor_schema_id=native_module.TRADER_VENDOR_SCHEMA_ID,
        field_set_version=native_module.TRADER_FIELD_SET_VERSION,
        payload=payload,
        data=data,
    )

    assert len(payload) == ctypes.sizeof(native_module._OrderEvent) == 504
    assert native_module._OrderEvent.direction.offset == 124
    assert native_module._OrderEvent.order_price_type.offset == 125
    assert native_module._OrderEvent.time_condition.offset == 126
    assert native_module._OrderEvent.volume_condition.offset == 127
    assert native_module._OrderEvent.limit_price.offset == 136
    assert event.event_name == "order"
    assert event.data["security"] == "511880"
    assert event.data["direction"] == "buy"
    assert event.data["order_price_type"] == "opponent_best"
    assert event.data["time_condition"] == "gfd"
    assert event.data["volume_condition"] == "any"
    assert event.data["order_sys_id"] == "system"
    assert event.data["status_msg"] == "accepted"
