"""期货账本机制测试：全部数值可手算，不依赖外部数据源。

合约口径：LH 乘数 16 吨/手、保证金率 0.14。
单手保证金 = 价×16×0.14；单手盈亏 = 价差×16。
"""

import datetime as dt

import pytest

from bullet_trade.core.futures_account import (
    ContractSpec,
    ContractSpecError,
    ContractSpecTable,
    FuturesAccount,
    InsufficientMarginError,
    OverCloseError,
    futures_product,
    is_futures_security,
)
from bullet_trade.core.contract_specs import FuturesSpecConfig, MarginRateRule

LH = "LH2109.XDCE"
CASH = 1_000_000.0
MULTIPLIER = 16.0
MARGIN_RATE = 0.14


def make_account(cash: float = CASH, margin_rate: float = MARGIN_RATE) -> FuturesAccount:
    return FuturesAccount(
        cash=cash,
        starting_cash=cash,
        spec_table=ContractSpecTable(margin_rate=margin_rate),
    )


def test_security_classification():
    assert is_futures_security("LH2109.XDCE") is True
    assert is_futures_security("IF2109.CCFX") is True
    assert is_futures_security("002714.XSHE") is False
    assert is_futures_security("not-a-code") is False
    assert futures_product("LH2109.XDCE") == "LH"
    assert futures_product("lh2109.xdce") == "LH"


def test_spec_table_resolution_and_failure():
    table = ContractSpecTable()
    assert table.multiplier(LH) == MULTIPLIER
    assert table.tick_size(LH) == 5.0
    with pytest.raises(ContractSpecError):
        table.margin_rate(LH)  # 未提供保证金率时必须显式失败
    table.set_margin_rate(0.14)
    assert table.margin_rate(LH) == 0.14
    with pytest.raises(ContractSpecError):
        table.multiplier("ZZ2109.XDCE")  # 未登记品种不得退化为乘数 1


def test_spec_table_registration_overrides_builtin():
    table = ContractSpecTable(specs=[ContractSpec("LH", 10.0, 1.0, 0.2)])
    assert table.multiplier(LH) == 10.0
    assert table.margin_rate(LH) == 0.2
    table.register("ZZ", 5.0)
    assert table.multiplier("ZZ2501.XDCE") == 5.0


def test_open_freezes_exact_margin():
    account = make_account()
    # 单手保证金 = 27000 × 16 × 0.14 = 60480
    account.open(LH, "long", 1, 27000.0, commission=9.94)
    assert account.margin == pytest.approx(60480.0)
    assert account.cash == pytest.approx(CASH - 60480.0 - 9.94)
    position = account.get_position(LH, "long")
    assert position.amount == 1
    assert position.today_amount == 1
    assert position.yesterday_amount == 0
    assert position.open_price == pytest.approx(27000.0)
    assert position.prev_settlement == pytest.approx(27000.0)
    # 权益恒等式：开仓只减少手续费
    assert account.total_value == pytest.approx(CASH - 9.94)


def test_open_average_price_and_margin_scale_with_lots():
    account = make_account()
    account.open(LH, "long", 2, 27000.0)
    account.open(LH, "long", 3, 28000.0)
    position = account.get_position(LH, "long")
    # 均价 = (2×27000 + 3×28000) / 5 = 27600
    assert position.amount == 5
    assert position.open_price == pytest.approx(27600.0)
    # 保证金按各笔开仓成交价累计 = 2×27000×16×0.14 + 3×28000×16×0.14
    assert position.margin == pytest.approx(
        2 * 27000.0 * MULTIPLIER * MARGIN_RATE + 3 * 28000.0 * MULTIPLIER * MARGIN_RATE
    )


def test_insufficient_margin_rejected():
    account = make_account(cash=50_000.0)
    with pytest.raises(InsufficientMarginError):
        account.open(LH, "long", 1, 27000.0)
    assert account.cash == pytest.approx(50_000.0)
    assert account.get_position(LH, "long") is None


def test_close_long_same_price_returns_margin_and_zero_pnl():
    account = make_account()
    account.open(LH, "long", 1, 27000.0, commission=9.94)
    result = account.close(LH, "long", 1, 27000.0, commission=9.94)
    assert result.released_margin == pytest.approx(60480.0)
    assert result.variation_pnl == pytest.approx(0.0)
    assert result.realized_pnl == pytest.approx(0.0)
    assert result.cash_delta == pytest.approx(60480.0 - 9.94)
    assert account.cash == pytest.approx(CASH - 9.94 - 9.94)
    assert account.get_position(LH, "long") is None
    assert account.margin == pytest.approx(0.0)


def test_close_long_profit_hand_computed():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    # 盈亏 = (27500-27000) × 16 = 8000
    result = account.close(LH, "long", 1, 27500.0)
    assert result.realized_pnl == pytest.approx(8000.0)
    assert result.variation_pnl == pytest.approx(8000.0)
    assert result.cash_delta == pytest.approx(60480.0 + 8000.0)
    assert account.cash == pytest.approx(CASH + 8000.0)
    assert account.total_value == pytest.approx(CASH + 8000.0)


def test_close_short_profit_sign_hand_computed():
    account = make_account()
    account.open(LH, "short", 2, 27000.0)
    # 空头盈亏 = (27000-26000) × 2 × 16 = 32000
    result = account.close(LH, "short", 2, 26000.0)
    assert result.realized_pnl == pytest.approx(32000.0)
    assert result.variation_pnl == pytest.approx(-(-32000.0))
    assert account.cash == pytest.approx(CASH + 32000.0)


def test_floating_pnl_and_equity_while_holding():
    account = make_account()
    account.open(LH, "long", 3, 27000.0)
    account.mark_prices({LH: 27200.0})
    position = account.get_position(LH, "long")
    # 浮盈 = 200 × 3 × 16 = 9600
    assert position.floating_pnl == pytest.approx(9600.0)
    assert account.floating_pnl == pytest.approx(9600.0)
    # 保证金仍是开仓时按成交价计收的额度，不随最新价重估
    assert account.margin == pytest.approx(3 * 27000.0 * MULTIPLIER * MARGIN_RATE)
    assert account.total_value == pytest.approx(CASH + 9600.0)


def test_partial_close_keeps_average_price():
    account = make_account()
    account.open(LH, "long", 4, 27000.0)
    result = account.close(LH, "long", 1, 27500.0)
    assert result.realized_pnl == pytest.approx(8000.0)
    position = account.get_position(LH, "long")
    assert position.amount == 3
    assert position.open_price == pytest.approx(27000.0)


@pytest.mark.parametrize("side, execution_price", [("long", 1007.0), ("short", 1013.0)])
def test_partial_close_slippage_does_not_reprice_remaining_lots(side, execution_price):
    account = make_account()
    account.open(LH, side, 4, 1000.0)
    account.mark_to_market({LH: 1010.0})
    equity_before = account.total_value

    account.close(LH, side, 1, execution_price, commission=2.0)

    position = account.get_position(LH, side)
    assert position.amount == 3
    assert position.last_price == pytest.approx(1010.0)
    assert account.total_value == pytest.approx(equity_before - 3.0 * MULTIPLIER - 2.0)


def test_over_close_rejected():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    with pytest.raises(OverCloseError):
        account.close(LH, "long", 2, 27000.0)
    with pytest.raises(OverCloseError):
        account.close(LH, "short", 1, 27000.0)
    assert account.get_position(LH, "long").amount == 1


def test_settlement_marks_to_market_and_rolls_today_lots():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    account.mark_prices({LH: 27300.0})
    equity_before = account.total_value
    result = account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
    # 盯市变动 = (27300-27000) × 16 = 4800 进现金；保证金基准结转到结算价需追加
    # (27300-27000) × 16 × 0.14 = 672，由现金吸收，故结算不改变权益
    assert result.variation_margin == pytest.approx(4800.0)
    assert account.cash == pytest.approx(CASH - 60480.0 + 4800.0 - 672.0)
    position = account.get_position(LH, "long")
    assert position.prev_settlement == pytest.approx(27300.0)
    assert position.today_amount == 0
    assert position.yesterday_amount == 1
    assert position.margin == pytest.approx(27300.0 * MULTIPLIER * MARGIN_RATE)
    # 结算不改变权益
    assert account.total_value == pytest.approx(equity_before)
    assert result.missing_settlement == ()


def test_new_lots_are_marked_from_open_price_not_previous_settlement():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
    equity_before = account.total_value  # 1004800

    # 次日在 28000 加仓 1 手，盯市基准按手数合并 = (27300+28000)/2 = 27650
    account.open(LH, "long", 1, 28000.0)
    position = account.get_position(LH, "long")
    assert position.prev_settlement == pytest.approx(27650.0)
    # 保证金仍只按成交价冻结 = 28000 × 16 × 0.14 = 62720
    assert account.cash == pytest.approx(equity_before - 61152.0 - 62720.0)

    result = account.settle_day({LH: 28000.0}, day=dt.date(2021, 4, 2))
    # 昨仓盯市 (28000-27300)×16 = 11200；今仓盯市 0
    assert result.variation_margin == pytest.approx(11200.0)
    # 保证金追加 = 2×28000×16×0.14 - (61152+62720) = 1568
    assert account.cash == pytest.approx(equity_before - 61152.0 - 62720.0 + 11200.0 - 1568.0)
    assert account.total_value == pytest.approx(equity_before + 11200.0)


def test_next_day_close_uses_settlement_as_cash_base():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
    result = account.close(LH, "long", 1, 27500.0)
    # 现金变动 = 释放保证金(按昨结 27300) + 盯市(27500-27300)×16
    assert result.released_margin == pytest.approx(27300.0 * MULTIPLIER * MARGIN_RATE)
    assert result.variation_pnl == pytest.approx(200.0 * MULTIPLIER)
    # 统计口径的已实现盈亏仍相对开仓均价 = (27500-27000)×16
    assert result.realized_pnl == pytest.approx(8000.0)
    assert account.cash == pytest.approx(CASH + 8000.0)


def test_missing_settlement_price_is_reported_not_substituted():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    result = account.settle_day({}, day=dt.date(2021, 4, 1))
    assert result.missing_settlement == (LH,)
    assert result.variation_margin == pytest.approx(0.0)
    assert account.cash == pytest.approx(CASH - 60480.0)
    assert account.get_position(LH, "long").prev_settlement == pytest.approx(27000.0)


def test_delivery_day_closes_position_and_releases_margin():
    account = make_account()
    account.open(LH, "long", 2, 27000.0, end_date=dt.date(2021, 9, 27))
    account.settle_day({LH: 26000.0}, day=dt.date(2021, 9, 27))
    # 到期了结：盯市 (26000-27000)×2×16 = -32000 进现金，保证金基准结转到结算价后
    # 又全额返还，故最终现金只剩盯市亏损 CASH - 32000
    assert account.get_position(LH, "long") is None
    assert account.cash == pytest.approx(CASH - 32000.0)
    assert account.margin == pytest.approx(0.0)


def test_day_start_resets_today_counters():
    account = make_account()
    account.open(LH, "long", 1, 27000.0, commission=9.94)
    account.close(LH, "long", 1, 27500.0, commission=9.94)
    assert account.today_realized_pnl == pytest.approx(8000.0)
    assert account.today_commission == pytest.approx(19.88)
    account.on_day_start()
    assert account.today_realized_pnl == pytest.approx(0.0)
    assert account.today_commission == pytest.approx(0.0)
    assert account.realized_pnl == pytest.approx(8000.0)
    assert account.commission_paid == pytest.approx(19.88)


def test_long_and_short_positions_are_separate():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    account.open(LH, "short", 1, 27000.0)
    assert account.get_position(LH, "long").amount == 1
    assert account.get_position(LH, "short").amount == 1
    assert len(account.positions) == 2
    assert account.margin == pytest.approx(2 * 60480.0)


def test_summary_snapshot_fields():
    account = make_account()
    account.open(LH, "long", 1, 27000.0, commission=9.94)
    snapshot = account.summary()
    assert snapshot["cash"] == pytest.approx(CASH - 60480.0 - 9.94)
    assert snapshot["margin"] == pytest.approx(60480.0)
    assert snapshot["floating_pnl"] == pytest.approx(0.0)
    assert snapshot["total_value"] == pytest.approx(CASH - 9.94)
    assert snapshot["position_count"] == 1.0


def test_intraday_mark_moves_variation_into_cash():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    # 单手保证金 60480，盯市前权益 = 939520 + 60480 = 1000000
    result = account.mark_to_market({LH: 27300.0})
    # 盯市 = (27300-27000)×16 = 4800；保证金结转到 27300 需追加 (300)×16×0.14 = 672
    assert result.variation_margin == pytest.approx(4800.0)
    assert result.margin == pytest.approx(61152.0)
    assert result.missing_price == ()
    assert account.cash == pytest.approx(CASH - 60480.0 + 4800.0 - 672.0)
    position = account.get_position(LH, "long")
    assert position.prev_settlement == pytest.approx(27300.0)
    assert position.margin_held == pytest.approx(61152.0)
    # 盯市基准结转到重估价后，权益里的浮动项归零（已进现金）；
    # 相对开仓均价的展示口径浮盈仍保留
    assert account.mark_to_market_pnl == pytest.approx(0.0)
    assert account.floating_pnl == pytest.approx(4800.0)
    # 权益与只更新最新价、不搬现金的 mark_prices 口径相同：1000000 + 4800
    assert account.total_value == pytest.approx(CASH + 4800.0)


def test_intraday_mark_short_direction_sign():
    account = make_account()
    account.open(LH, "short", 1, 27000.0)
    result = account.mark_to_market({LH: 26700.0})
    # 空头盯市 = (26700-27000)×16×(-1) = 4800；保证金结转释放 300×16×0.14 = 672
    assert result.variation_margin == pytest.approx(4800.0)
    assert result.margin == pytest.approx(59808.0)
    assert account.cash == pytest.approx(CASH - 60480.0 + 4800.0 + 672.0)
    assert account.total_value == pytest.approx(CASH + 4800.0)


def test_intraday_mark_without_price_keeps_previous_basis():
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    cash_before = account.cash
    result = account.mark_to_market({})
    assert result.missing_price == (LH,)
    assert result.variation_margin == pytest.approx(0.0)
    assert account.cash == pytest.approx(cash_before)
    assert account.get_position(LH, "long").prev_settlement == pytest.approx(27000.0)


def test_intraday_mark_keeps_today_lots():
    """盘中重估不是日切，当日开仓手数必须保留，否则当天平仓会被按平昨计费。"""

    account = make_account()
    account.open(LH, "long", 2, 27000.0)
    account.mark_to_market({LH: 27300.0})
    position = account.get_position(LH, "long")
    assert position.today_amount == 2
    assert position.yesterday_amount == 0
    # 只有日终结算才结转今昨仓
    account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
    assert position.today_amount == 0
    assert position.yesterday_amount == 2


def test_intraday_mark_is_additive_with_settlement():
    """盯市变动对区间可加：盘中重估不得改变日终的结算基准与现金净变动。"""

    def build(with_mark: bool) -> FuturesAccount:
        account = make_account()
        account.open(LH, "long", 1, 27000.0)
        account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
        account.on_day_start()
        if with_mark:
            # (27500-27300)×16 = 3200 进现金，保证金追加 200×16×0.14 = 448
            account.mark_to_market({LH: 27500.0})
            assert account.cash == pytest.approx(943648.0 + 3200.0 - 448.0)
        account.settle_day({LH: 27600.0}, day=dt.date(2021, 4, 2))
        return account

    marked, plain = build(True), build(False)
    assert marked.cash == pytest.approx(plain.cash)
    assert marked.margin == pytest.approx(plain.margin)
    assert marked.total_value == pytest.approx(plain.total_value)
    assert marked.get_position(LH, "long").prev_settlement == pytest.approx(
        plain.get_position(LH, "long").prev_settlement
    )
    # 27300 → 27600 共 (300)×16 = 4800 盯市，与是否盘中重估无关
    assert marked.cash == pytest.approx(943648.0 + 4800.0 - 672.0)


def test_intraday_mark_is_additive_with_close():
    """盘中重估后平仓，现金与已实现盈亏应与不重估路径逐值相同。"""

    def build(with_mark: bool) -> FuturesAccount:
        account = make_account()
        account.open(LH, "long", 1, 27000.0)
        account.settle_day({LH: 27300.0}, day=dt.date(2021, 4, 1))
        if with_mark:
            account.mark_to_market({LH: 27500.0})
        account.close(LH, "long", 1, 27600.0)
        return account

    marked, plain = build(True), build(False)
    # 已实现盈亏始终相对开仓均价 = (27600-27000)×16
    assert marked.realized_pnl == pytest.approx(9600.0)
    assert marked.cash == pytest.approx(plain.cash)
    assert marked.cash == pytest.approx(943648.0 + 61152.0 + 4800.0)
    assert marked.positions == {}


@pytest.mark.parametrize("side", ["long", "short"])
def test_default_partial_close_preserves_today_lots_for_next_close(side):
    account = make_account()
    account.open(LH, side, 1, 27000.0)
    account.settle_day({LH: 27000.0}, day=dt.date(2021, 4, 1))
    account.open(LH, side, 1, 27000.0)

    account.close(LH, side, 1, 27000.0, commission=9.94)
    position = account.get_position(LH, side)
    assert position.amount == 1
    assert position.today_amount == 1
    assert position.yesterday_amount == 0

    account.close(LH, side, 1, 27000.0, commission=993.60, close_today=True)
    assert account.positions == {}
    assert account.commission_paid == pytest.approx(1003.54)
    assert account.cash == pytest.approx(CASH - 1003.54)
    assert account.realized_pnl == pytest.approx(0.0)


@pytest.mark.parametrize("side", ["long", "short"])
def test_explicit_today_lots_controls_successive_partial_closes(side):
    account = make_account()
    account.open(LH, side, 2, 27000.0)
    account.settle_day({LH: 27000.0}, day=dt.date(2021, 4, 1))
    account.open(LH, side, 2, 27000.0)

    account.close(LH, side, 1, 27000.0, today_lots=1)
    position = account.get_position(LH, side)
    assert (position.today_amount, position.yesterday_amount) == (1, 2)
    account.close(LH, side, 2, 27000.0, today_lots=1)
    assert (position.today_amount, position.yesterday_amount) == (0, 1)
    account.close(LH, side, 1, 27000.0, today_lots=0)
    assert account.positions == {}
    assert account.cash == pytest.approx(CASH)


@pytest.mark.parametrize(
    "lots,today_lots,close_today,error",
    [
        (1, -1, False, ValueError),
        (1, 2, False, ValueError),
        (1, 0.5, False, ValueError),
        (1, 0, True, ValueError),
        (2, 2, False, OverCloseError),
        (2, 0, False, OverCloseError),
    ],
)
def test_invalid_close_allocation_does_not_change_account(lots, today_lots, close_today, error):
    account = make_account()
    account.open(LH, "long", 1, 27000.0)
    account.settle_day({LH: 27000.0}, day=dt.date(2021, 4, 1))
    account.open(LH, "long", 1, 27000.0)
    position = account.get_position(LH, "long")
    before_summary = account.summary()
    before_position = vars(position).copy()

    with pytest.raises(error):
        account.close(
            LH, "long", lots, 28000.0, commission=20.0,
            today_lots=today_lots, close_today=close_today,
        )

    assert account.summary() == before_summary
    assert vars(position) == before_position


def make_dated_margin_account():
    config = FuturesSpecConfig(
        margin_rules={
            "LH": MarginRateRule(
                product="LH",
                rate=0.10,
                effective=(
                    (dt.date(2021, 4, 2), 0.20),
                    (dt.date(2021, 4, 5), 0.05),
                ),
            ),
        },
    )
    return FuturesAccount(
        cash=CASH,
        spec_table=ContractSpecTable(config=config, load_config=False),
    )


@pytest.mark.parametrize("method", ["settle_day", "mark_to_market"])
def test_dated_margin_rate_updates_existing_long_and_short_positions(method):
    account = make_dated_margin_account()
    for side in ("long", "short"):
        account.open(LH, side, 1, 27000.0, trade_time=dt.datetime(2021, 4, 1, 10))
    assert account.margin == pytest.approx(86400.0)
    revalue = getattr(account, method)

    revalue({LH: 27000.0}, day=dt.date(2021, 4, 2))
    assert account.margin == pytest.approx(172800.0)
    assert account.cash == pytest.approx(CASH - 172800.0)
    assert account.total_value == pytest.approx(CASH)
    for position in account.iter_positions():
        assert position.margin_rate == pytest.approx(0.20)
        assert position.today_amount == (1 if method == "mark_to_market" else 0)

    revalue({LH: 27000.0}, day=dt.date(2021, 4, 5))
    assert account.margin == pytest.approx(43200.0)
    assert account.cash == pytest.approx(CASH - 43200.0)
    assert account.total_value == pytest.approx(CASH)
    for position in account.iter_positions():
        assert position.margin_rate == pytest.approx(0.05)

    account.close(LH, "long", 1, 27000.0)
    account.close(LH, "short", 1, 27000.0)
    assert account.cash == pytest.approx(CASH)


@pytest.mark.parametrize("method", ["settle_day", "mark_to_market"])
def test_dated_margin_missing_price_keeps_position_and_cash(method):
    account = make_dated_margin_account()
    position = account.open(LH, "long", 1, 27000.0, trade_time=dt.datetime(2021, 4, 1, 10))
    before_cash = account.cash
    before_position = vars(position).copy()

    result = getattr(account, method)({}, day=dt.date(2021, 4, 2))

    missing_field = "missing_settlement" if method == "settle_day" else "missing_price"
    assert getattr(result, missing_field) == (LH,)
    assert account.cash == before_cash
    assert vars(position) == before_position


@pytest.mark.parametrize("method", ["settle_day", "mark_to_market"])
def test_revaluation_without_day_keeps_effective_margin_rate(method):
    account = make_dated_margin_account()
    position = account.open(LH, "long", 1, 27000.0, trade_time=dt.datetime(2021, 4, 2, 10))
    before_cash = account.cash

    getattr(account, method)({LH: 27000.0})

    assert position.margin_rate == pytest.approx(0.20)
    assert account.margin == pytest.approx(86400.0)
    assert account.cash == before_cash
    assert account.total_value == pytest.approx(CASH)
