"""作者：BruceLee。股票/ETF bar 回测的限价挂单与分钟成交证据。

输入：BacktestEngine、订单、当时行情与已完成分钟 bar；输出：订单状态、
Trade 和账户更新。由 engine 驱动，订单撤销 API 释放冻结资源。
仅用于历史 bar 回测，不接券商；即时单按日量、挂单按分钟量分别限量。
内部读取全天原始量仅用于聚宽兼容撮合，不向策略开放未来数据。
时间缺失、未来、过期及明确停牌不成交，普通零量分钟不直接视为停牌。
"""

import math

import pandas as pd

from .models import OrderStatus, Position, Trade
from .exceptions import BacktestDataError
from .settings import get_settings

__all__ = ["EquityLimitBook"]


class EquityLimitBook:
    """管理同日限价挂单及资源；协作engine，保存原限价、余量与每单撮合时刻。"""

    def __init__(self, engine):
        """输入所属回测引擎，建立空订单簿；无返回，不读取外部数据。"""
        self.engine = engine
        self.pending = {}
        self.states = {}
        self.clock = None
        self.bars = {}
        self.volume_day = None
        self.daily_volumes = {}

    def bar(self, security, now, fq):
        """读取当前已完成分钟；输入代码/时刻/复权，返回价量字典或 None，缓存本时刻。"""
        from .engine import api_get_price

        if now != self.clock:
            self.clock = now
            self.bars.clear()
        if security in self.bars:
            return self.bars[security]
        result = None
        try:
            frame = api_get_price(
                security=security,
                end_date=now,
                frequency="minute",
                fields=["close", "high", "low", "volume"],
                count=1,
                fq=fq,
            )
            if not frame.empty:
                row = frame.iloc[-1]
                stamp = row.get("time")
                if stamp is None and isinstance(frame.index, pd.DatetimeIndex):
                    stamp = frame.index[-1]
                stamp = pd.Timestamp(stamp)
                price, volume = float(row["close"]), float(row["volume"])
                if (
                    stamp == pd.Timestamp(now)
                    and math.isfinite(price)
                    and price > 0
                    and math.isfinite(volume)
                    and volume >= 0
                ):
                    result = {
                        "price": price,
                        "volume": int(volume),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "paused": row.get("paused", False) == 1,
                    }
        except Exception:
            self.engine._raise_if_backtest_data_error()
        self.bars[security] = result
        return result

    def volume_limit(self, volume):
        """输入原始成交量，返回每笔比例限额；非法比例抛ValueError，不修改状态。"""
        ratio = float(get_settings().options.get("order_volume_ratio", 1.0))
        if not math.isfinite(ratio) or ratio < 0:
            raise ValueError("order_volume_ratio必须是非负有限数")
        return int(volume * ratio)

    def daily_volume(self, security, now):
        """输入代码和撮合时刻，返回当日原始总量或None；仅内部读provider并按日缓存。"""
        from .engine import get_data_provider

        if self.volume_day != now.date():
            self.volume_day = now.date()
            self.daily_volumes.clear()
        if security in self.daily_volumes:
            return self.daily_volumes[security]
        try:
            frame = get_data_provider().get_price(
                security=security,
                start_date=now.replace(hour=0, minute=0, second=0, microsecond=0),
                end_date=now.replace(hour=15, minute=0, second=0, microsecond=0),
                frequency="daily",
                fields=["volume"],
                fq=None,
                skip_paused=False,
            )
            if frame.empty:
                raise ValueError("当日成交量为空")
            row = frame.iloc[-1]
            stamp = pd.Timestamp(row.get("time", frame.index[-1]))
            volume = float(row["volume"])
            if stamp.date() == now.date() and math.isfinite(volume) and volume >= 0:
                self.daily_volumes[security] = int(volume)
                return int(volume)
            raise ValueError("当日成交量日期错误或数值无效")
        except Exception as exc:
            self.engine._raise_if_backtest_data_error()
            if self.engine.strict_data:
                failure = BacktestDataError(
                    f"Required daily volume unavailable: security={security}, time={now}"
                )
                self.engine.context._backtest_data_error = failure
                raise failure from exc
        return None

    def can_trade(self, security, now, bar):
        """输入当前分钟，返回是否有可见交易依据；零量时只查截至当前的日内量。"""
        from .engine import api_get_price

        if bar is None or bar["paused"]:
            return False
        if bar["volume"] > 0:
            return True
        try:
            frame = api_get_price(
                security=security,
                start_date=now.replace(hour=9, minute=30, second=0, microsecond=0),
                end_date=now,
                frequency="minute",
                fields=["volume"],
                fq=None,
            )
            stamps = pd.to_datetime(frame["time"] if "time" in frame else frame.index)
            visible = (
                stamps >= pd.Timestamp(now).normalize() + pd.Timedelta(hours=9, minutes=30)
            ) & (stamps <= now)
            volumes = pd.to_numeric(frame.loc[visible, "volume"], errors="coerce")
            return bool((volumes.where(volumes.map(math.isfinite), 0) > 0).any())
        except Exception:
            self.engine._raise_if_backtest_data_error()
            return False

    def fees(self, state, value):
        """输入订单状态及累计成交额，返回累计佣金/税费，沿用引擎现金舍入。"""
        rnd = self.engine._round_equity_cash
        return (
            rnd(max(value * state["rate"], state["minimum"])) if value else 0.0,
            rnd(value * state["tax_rate"]),
        )

    def reserve_amount(self, state, remaining):
        """输入状态及未成交数量，返回原限价下剩余资金需求，含未支付累计费用。"""
        value = remaining * state["limit"]
        commission, tax = self.fees(state, state["value"] + value)
        return self.engine._round_equity_cash(
            value + commission + tax - state["commission"] - state["tax"]
        )

    def accept(self, order, quote, info):
        """校验并冻结新限价单；输入订单/行情/证券信息，返回是否接收，修改账户及订单。"""
        engine = self.engine
        portfolio = engine.context.portfolio
        limit = float(order.style.price)
        if not math.isfinite(limit) or limit <= 0:
            order.status = OrderStatus.rejected
            order.extra["rejection_reason"] = "invalid_limit_price"
            return False
        rounded = engine._round_to_tick(limit, order.security)
        bounds = [getattr(quote, name, 0) or 0 for name in ("low_limit", "high_limit")]
        if (
            abs(rounded - limit) > 1e-9
            or (bounds[0] > 0 and limit < bounds[0])
            or (bounds[1] > 0 and limit > bounds[1])
        ):
            order.status = OrderStatus.rejected
            order.extra["rejection_reason"] = "invalid_limit_bounds_or_tick"
            return False
        reference = float(quote.last_price or limit)
        if not math.isfinite(reference) or reference <= 0:
            reference = limit
        amount = engine._calculate_order_amount(order, reference)
        buy = amount > 0
        config = engine._get_order_cost_config(order.security)
        state = dict(
            limit=limit,
            buy=buy,
            value=0.0,
            commission=0.0,
            tax=0.0,
            reserved=0.0,
            shares=0,
            tplus=engine._infer_tplus_from_info(info),
            rate=(config.open_commission if buy else config.close_commission) if config else 0.0003,
            tax_rate=(
                (config.open_tax if buy else config.close_tax)
                if config
                else (0.0 if buy else 0.001)
            ),
            minimum=config.min_commission if config else 5.0,
        )
        quantity = abs(amount)
        if buy:
            low, high = 0, quantity // 100
            while low < high:
                mid = (low + high + 1) // 2
                if self.reserve_amount(state, mid * 100) <= portfolio.available_cash + 1e-9:
                    low = mid
                else:
                    high = mid - 1
            quantity = low * 100
        else:
            pos = portfolio.positions.get(order.security)
            available = pos.closeable_amount if pos else 0
            quantity = min(quantity, available)
            if quantity < available:
                quantity = quantity // 100 * 100
        if quantity <= 0:
            order.status = OrderStatus.canceled
            order.extra["cancel_reason"] = "no_orderable_quantity"
            return False
        order.extra["requested_amount"] = amount
        order.extra["order_price"] = limit
        order.extra["matching_model"] = "completed_minute_limit"
        order.amount = quantity if buy else -quantity
        order.is_buy = buy
        order.price = limit
        if buy:
            state["reserved"] = self.reserve_amount(state, quantity)
            portfolio.available_cash -= state["reserved"]
            portfolio.locked_cash += state["reserved"]
        else:
            state["shares"] = quantity
            portfolio.positions[order.security].closeable_amount -= quantity
        self.states[order.order_id] = state
        self.pending[order.order_id] = order
        portfolio.update_value()
        return True

    def cancel(self, order_id, reason="user_cancel"):
        """输入订单ID及原因，返回是否撤销；释放剩余现金/份额，保留已成交事实。"""
        order = self.pending.pop(str(order_id), None)
        if order is None:
            return False
        state = self.states.pop(order.order_id)
        portfolio = self.engine.context.portfolio
        portfolio.available_cash += state["reserved"]
        portfolio.locked_cash -= state["reserved"]
        if state["shares"]:
            portfolio.positions[order.security].closeable_amount += state["shares"]
        order.status = OrderStatus.canceled
        order.extra["cancel_reason"] = reason
        portfolio.update_value()
        return True

    def expire(self):
        """日终取消所有剩余限价单；无参数，返回数量，并释放全部冻结资源。"""
        ids = list(self.pending)
        for order_id in ids:
            self.cancel(order_id, "day_expired")
        return len(ids)

    def process(self, order, quote, info, now, fq):
        """输入订单、行情、信息、时刻及复权，返回无值；接收或重试，不改原限价。"""
        new_order = order.order_id not in self.pending
        if new_order and not self.accept(order, quote, info):
            return
        if order.add_time.date() != now.date():
            self.cancel(order.order_id, "day_expired")
            return
        state = self.states[order.order_id]
        if state.get("last_match") == now:
            return
        state["last_match"] = now
        if quote.paused:
            order.extra["wait_reason"] = "paused"
            return
        bar = self.bar(order.security, now, fq)
        if bar is None or bar["paused"]:
            order.extra["wait_reason"] = "missing_or_zero_volume_minute"
            return
        if new_order:
            if not self.can_trade(order.security, now, bar):
                order.extra["wait_reason"] = "no_visible_session_volume"
                return
            category = self.engine._infer_security_category(order.security, info)
            price = (
                bar["price"]
                if category == "money_market_fund"
                else self.engine._apply_slippage_price(bar["price"], state["buy"], order.security)
            )
            price = self.engine._round_to_tick(price, order.security)
            if (state["buy"] and price > state["limit"]) or (
                not state["buy"] and price < state["limit"]
            ):
                order.extra["wait_reason"] = "limit_not_reached"
                return
            boundary = getattr(quote, "high_limit" if state["buy"] else "low_limit", 0) or 0
            if boundary > 0 and (price >= boundary if state["buy"] else price <= boundary):
                order.extra["wait_reason"] = "one_sided_price_limit"
                return
            volume = self.daily_volume(order.security, now)
            if volume is None:
                order.extra["wait_reason"] = "missing_daily_volume"
                return
        else:
            extreme = bar["low"] if state["buy"] else bar["high"]
            if extreme is None or not math.isfinite(float(extreme)) or float(extreme) <= 0:
                order.extra["wait_reason"] = "missing_bar_extreme"
                return
            if (state["buy"] and state["limit"] <= float(extreme)) or (
                not state["buy"] and state["limit"] >= float(extreme)
            ):
                order.extra["wait_reason"] = "limit_not_reached"
                return
            price, volume = state["limit"], bar["volume"]
        remaining = abs(order.amount) - order.filled
        quantity = min(remaining, self.volume_limit(volume))
        if not quantity:
            return
        self.fill(order, state, quantity, price, now)

    def fill(self, order, state, quantity, price, now):
        """按已验证价格数量结算；输入订单/状态/量价/时刻，无返回，更新累计费用与持仓。"""
        engine = self.engine
        portfolio = engine.context.portfolio
        value = quantity * price
        cumulative = state["value"] + value
        commission, tax = self.fees(state, cumulative)
        fee, tax_delta = commission - state["commission"], tax - state["tax"]
        portfolio.available_cash += state["reserved"]
        portfolio.locked_cash -= state["reserved"]
        state["reserved"] = 0.0
        if state["buy"]:
            portfolio.available_cash -= engine._round_equity_cash(value + fee + tax_delta)
            pos = portfolio.positions.setdefault(order.security, Position(security=order.security))
            if pos.total_amount == 0:
                pos.buy_time = now
            pos.last_buy_time = now
            pos.update_position(quantity, price)
            if state["tplus"] == 1:
                pos.today_buy_t1 += quantity
                pos.closeable_amount -= quantity
            order.avg_cost = cumulative / (order.filled + quantity)
        else:
            pos = portfolio.positions[order.security]
            order.avg_cost = pos.avg_cost
            pos.closeable_amount += quantity
            state["shares"] -= quantity
            pos.update_position(-quantity, price)
            portfolio.available_cash += engine._round_equity_cash(value - fee - tax_delta)
            if not pos.total_amount:
                del portfolio.positions[order.security]
        pos.update_price(price)
        order.filled += quantity
        order.price = cumulative / order.filled
        order.commission = commission + tax
        state.update(value=cumulative, commission=commission, tax=tax)
        order.extra.pop("wait_reason", None)
        order.extra["fill_price"] = price
        engine._trade_seq += 1
        engine.trades.append(
            Trade(
                order_id=order.order_id,
                security=order.security,
                amount=quantity if state["buy"] else -quantity,
                price=price,
                time=now,
                commission=fee,
                tax=tax_delta,
                trade_id=f"T{engine._trade_seq:08d}",
            )
        )
        remaining = abs(order.amount) - order.filled
        if remaining:
            order.status = OrderStatus.filling
            if state["buy"]:
                state["reserved"] = self.reserve_amount(state, remaining)
                portfolio.available_cash -= state["reserved"]
                portfolio.locked_cash += state["reserved"]
        else:
            order.status = OrderStatus.filled
            self.pending.pop(order.order_id)
            self.states.pop(order.order_id)
        portfolio.update_value()
