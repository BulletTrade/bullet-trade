"""作者：BruceLee。

职责：验收实时仿真纯撮合合同；输入固定时钟、虚构报价/规则/委托，输出状态和守恒断言。
上下游：SimulationMatcher；不运行策略，不连接行情服务或真实券商，不写用户账本。
约定：所有政策数值只是测试夹具；测试通过不等于账户结算、恢复或正式仿真验收。
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from bullet_trade.broker.simulation_matcher import (
    SimulationMatcher,
    SimulationPolicy,
    SimulationQuote,
)


class FixedClock:
    """显式推进测试时间，不使用墙钟；同一分钟可按顺序发多个报价事件。"""

    def __init__(self):
        """无输入返回；创建包含时区的测试开盘时间。"""
        self.at = datetime(2026, 10, 9, 9, 30, tzinfo=timezone(timedelta(hours=8)))

    def __call__(self):
        """无输入，返回当前测试时间，不自动推进。"""
        return self.at


def matcher(clock, account="fixture", trading=True):
    """输入固定时钟、模拟账户和时段结果，返回纯撮合器；政策数值仅用于金样。"""
    return SimulationMatcher(
        account_id=account,
        policy=SimulationPolicy("fixture-last-price-v1", timedelta(seconds=10), 300),
        clock=clock,
        is_trading_time=lambda security, at: trading,
    )


def submit(engine, clock, **overrides):
    """输入引擎、时钟和字段覆盖，返回已受理委托；假定上游已预约，不在本测试伪造结算。"""
    fields = dict(
        order_id="o1",
        security="510300.XSHG",
        side="buy",
        quantity=500,
        protection_price=Decimal("4.000"),
        order_type="limit",
        expires_at=clock.at + timedelta(minutes=1),
        quantity_step=100,
        price_tick=Decimal("0.001"),
        rule_version="fixture-rule",
        fee_version="fixture-fee",
    )
    fields.update(overrides)
    return engine.submit(**fields)


def quote(clock, **overrides):
    """输入时钟和报价覆盖，返回有时间/来源的虚构事件；数量是模型容量而非盘口深度。"""
    fields = dict(
        quote_id="q1",
        security="510300.XSHG",
        price=Decimal("3.990"),
        event_at=clock.at,
        received_at=clock.at,
        quantity=500,
        source="fixture-only",
        kind="snapshot",
        data_version="fixture-data-v1",
        paused=False,
    )
    fields.update(overrides)
    return SimulationQuote(**fields)


@pytest.mark.unit
class TestSimulationMatcher:
    """检查状态、行情可见性、幂等、数量共享及终态，不替代正式账户集成测试。"""

    def test_observed_quote_is_readonly_and_restored(self):
        """无输入返回；原报价查询不推进时钟或命令，恢复后仍返回首次接收事实。"""
        clock = FixedClock()
        engine = matcher(clock)
        before = engine.checkpoint()
        assert engine.observed_quote("missing") is None
        assert engine.checkpoint() == before
        observed = quote(clock)
        engine.process_quote(observed)
        before = engine.checkpoint()
        assert engine.observed_quote(observed.quote_id) == observed
        assert engine.checkpoint() == before
        restored = engine.restore(
            before,
            account_id=engine.account_id,
            policy=engine.policy,
            clock=clock,
            is_trading_time=lambda security, at: True,
        )
        assert restored.observed_quote(observed.quote_id) == observed
        with pytest.raises(FrozenInstanceError):
            restored.observed_quote(observed.quote_id).price = Decimal("99")

    @pytest.mark.parametrize("identifier", [None, "", " ", 1, True])
    def test_observed_quote_rejects_invalid_identity(self, identifier):
        """输入无效身份，无正常返回；不推进模拟时钟或更改检查点。"""
        engine = matcher(FixedClock())
        before = engine.checkpoint()
        with pytest.raises(ValueError):
            engine.observed_quote(identifier)
        assert engine.checkpoint() == before

    def test_acceptance_is_not_a_fill_and_reads_are_immutable(self):
        """无输入返回；没有行情不成交、不生成费用，读出的视图不能反向修改内部状态。"""
        clock = FixedClock()
        engine = matcher(clock)
        order = submit(engine, clock)
        assert order.status == "accepted" and order.filled_quantity == 0
        assert engine.executions() == ()
        assert engine.events()[0].reason == "ACCEPTED_WAITING_FOR_QUOTE"
        with pytest.raises(FrozenInstanceError):
            engine.orders()[0].status = "filled"
        with pytest.raises(AttributeError):
            engine.account_id = "another-account"
        with pytest.raises(AttributeError):
            engine.policy = replace(engine.policy, version="another-policy")

    def test_partial_then_full_and_filled_cannot_cancel(self):
        """无输入返回；每事件上限产生逐笔部分成交，再全成，已成交终态不能撤回。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock)
        first = engine.process_quote(quote(clock))
        assert first[0].quantity == 300
        assert engine.orders()[0].status == "partially_filled"
        second = engine.process_quote(quote(clock, quote_id="q2"))
        assert second[0].quantity == 200
        assert sum(fill.quantity for fill in engine.executions()) == 500
        assert len({fill.execution_id for fill in engine.executions()}) == 2
        assert not engine.cancel("o1")
        assert engine.orders()[0].status == "filled"
        assert first[0].source == "fixture-only"
        assert first[0].quote_kind == "snapshot" and first[0].data_version == "fixture-data-v1"
        assert first[0].policy_version == "fixture-last-price-v1"
        assert first[0].rule_version == "fixture-rule" and first[0].fee_version == "fixture-fee"

    def test_cancel_only_remainder_is_idempotent(self):
        """无输入返回；部分成交后取消余量，重复取消不添事件，已成事实与数量保留。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock)
        engine.process_quote(quote(clock, quantity=100))
        assert engine.cancel("o1")
        events = engine.events()
        assert engine.cancel("o1") and engine.events() == events
        assert engine.orders()[0].filled_quantity == 100
        assert engine.process_quote(quote(clock, quote_id="q2")) == ()
        assert len(engine.executions()) == 1 and not engine.cancel("missing")

    def test_price_crossing_and_protected_market_both_honor_bound(self):
        """无输入返回；买价不超保护上限，卖价不低保护下限，不支持无保护的市价。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock)
        submit(engine, clock, order_id="o2", side="sell", order_type="protected_market")
        assert engine.process_quote(quote(clock, price=Decimal("4.001")))[0].order_id == "o2"
        assert engine.orders()[0].filled_quantity == 0
        assert (
            engine.process_quote(quote(clock, quote_id="q2", price=Decimal("3.999")))[0].order_id
            == "o1"
        )
        with pytest.raises(ValueError, match="保护价"):
            submit(engine, clock, order_id="o3", order_type="market")

    @pytest.mark.parametrize(
        "kind", ["stale", "paused", "closed", "zero", "under_step", "wrong_security", "off_tick"]
    )
    def test_invalid_market_conditions_do_not_fill(self, kind):
        """输入行情条件，无返回；旧价、停牌、休市、无容量、证券不符或价量步长不符不成交。"""
        clock = FixedClock()
        engine = matcher(clock, trading=kind != "closed")
        submit(engine, clock)
        overrides = {
            "stale": dict(event_at=clock.at - timedelta(seconds=11)),
            "paused": dict(paused=True),
            "zero": dict(quantity=0),
            "under_step": dict(quantity=99),
            "wrong_security": dict(security="511880.XSHG"),
            "off_tick": dict(price=Decimal("3.9901")),
        }.get(kind, {})
        assert engine.process_quote(quote(clock, **overrides)) == ()
        assert engine.orders()[0].status == "accepted"

    def test_quote_and_intent_idempotency_with_conflict_rejection(self):
        """无输入返回；相同报价和委托只处理一次，同身份不同经济事实拒绝且原事实不变。"""
        clock = FixedClock()
        engine = matcher(clock)
        original = submit(engine, clock)
        assert submit(engine, clock) == original
        first = engine.process_quote(quote(clock))
        assert first and engine.process_quote(quote(clock)) == ()
        assert submit(engine, clock) == engine.orders()[0]
        with pytest.raises(ValueError, match="经济输入"):
            submit(engine, clock, quantity=600)
        with pytest.raises(ValueError, match="不同事实"):
            engine.process_quote(quote(clock, quantity=600))
        assert len(engine.executions()) == 1

    def test_quote_capacity_is_shared_and_duplicate_concurrency_is_safe(self):
        """无输入返回；同报价跨委托共享容量，并发重复事件不重复消耗容量或生成成交。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock, quantity=200)
        submit(engine, clock, order_id="o2", quantity=200)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(engine.process_quote, [quote(clock)] * 20))
        assert sum(fill.quantity for group in results for fill in group) == 300
        assert [order.filled_quantity for order in engine.orders()] == [200, 100]
        assert len(engine.executions()) == 2

    def test_old_or_future_quote_and_reversed_clock_do_not_create_fills(self):
        """无输入返回；未来收到时间拒绝，受理前报价及乱序旧事件不成交，时钟逆行拒绝。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock)
        with pytest.raises(ValueError, match="未来报价"):
            engine.process_quote(quote(clock, received_at=clock.at + timedelta(seconds=1)))
        old = clock.at - timedelta(seconds=1)
        assert engine.process_quote(quote(clock, event_at=old, received_at=old)) == ()
        engine.process_quote(quote(clock, quote_id="q2", price=Decimal("4.010")))
        assert engine.process_quote(quote(clock, quote_id="q3", event_at=old)) == ()
        clock.at -= timedelta(seconds=1)
        with pytest.raises(ValueError, match="逆行"):
            engine.advance()
        assert engine.executions() == ()

    def test_expiry_without_quotes_or_at_boundary_preserves_partial(self):
        """无输入返回；没有报价也可到期，到期边界不成交，已成部分不能丢失。"""
        clock = FixedClock()
        engine = matcher(clock)
        order = submit(engine, clock)
        engine.process_quote(quote(clock, quantity=100))
        clock.at = order.expires_at
        assert engine.process_quote(quote(clock, quote_id="q2")) == ()
        assert engine.orders()[0].status == "expired"
        assert engine.orders()[0].filled_quantity == 100
        assert not engine.cancel("o1")
        assert engine.advance() == ()
        assert submit(engine, clock, expires_at=order.expires_at).status == "expired"

    def test_time_read_once_for_each_event(self):
        """无输入返回；移动时钟每事件只读取一次，事件和撮合时点一致，不先到期再回填成交。"""
        clock = FixedClock()
        engine = matcher(clock)
        submit(engine, clock)
        calls = []

        def moving():
            """无输入，返回本次时间并推进一分钟；用于识别一个事件重复取时。"""
            calls.append(clock.at)
            result = clock.at
            clock.at += timedelta(minutes=1)
            return result

        engine._clock = moving
        engine.process_quote(quote(clock))
        assert len(calls) == 1
        assert engine.events()[-1].at == engine.executions()[-1].matched_at == calls[0]

    def test_fact_identity_does_not_collide_across_account_order_delimiters(self):
        """无输入返回；账户和委托中含分隔符仍保持全范围身份，不以拼接短编号冒充唯一事实。"""
        clock = FixedClock()
        left, right = matcher(clock, "a:b"), matcher(clock, "a")
        submit(left, clock, order_id="c")
        submit(right, clock, order_id="b:c")
        assert (
            left.process_quote(quote(clock))[0].execution_id
            != right.process_quote(quote(clock))[0].execution_id
        )

    @pytest.mark.parametrize(
        "field,value",
        [
            ("quantity", True),
            ("quantity", -1),
            ("quantity", 50),
            ("protection_price", 4.0),
            ("protection_price", Decimal("NaN")),
            ("protection_price", Decimal("4.0001")),
            ("rule_version", ""),
            ("fee_version", ""),
            ("quantity_step", 0),
            ("price_tick", Decimal("0")),
        ],
    )
    def test_invalid_order_never_creates_a_fact(self, field, value):
        """输入无效字段，无返回；失败不产生委托、成交或状态事实，不静默转换浮点或数量。"""
        clock = FixedClock()
        engine = matcher(clock)
        with pytest.raises(ValueError):
            submit(engine, clock, **{field: value})
        assert engine.orders() == engine.executions() == engine.events() == ()

    def test_policy_and_quote_validation(self):
        """无输入返回；缺政策、无时区、无来源和非有限价格显式拒绝，没有 10 元替代价。"""
        clock = FixedClock()
        with pytest.raises(ValueError):
            SimulationPolicy("", timedelta(seconds=1), 100)
        with pytest.raises(ValueError):
            quote(clock, event_at=clock.at.replace(tzinfo=None))
        with pytest.raises(ValueError):
            quote(clock, source="")
        with pytest.raises(ValueError):
            quote(clock, price=Decimal("Infinity"))

    @pytest.mark.parametrize(
        "values", [{"paused": None}, {"kind": "daily_bar"}, {"data_version": ""}]
    )
    def test_unknown_state_or_bar_input_cannot_be_used_as_an_instant_quote(self, values):
        """输入未知停牌状态或 bar/无版本，无返回；适配器必须明确报价语义，不能默认正常可交易。"""
        clock = FixedClock()
        with pytest.raises(ValueError):
            quote(clock, **values)
