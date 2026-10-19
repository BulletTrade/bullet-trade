"""作者：BruceLee。

职责：提供实时仿真的确定性限价/保护价撮合合同，不承担资金和持仓结算。
输入：注入时钟、交易时段判断、显式版本政策、已预约委托和带来源的报价事件。
输出：不可变订单视图、状态事件与逐笔模拟成交，供唯一账户账本幂等结算。
上下游：未来 Simulator 适配器调用；不导入数据源、真实券商或私有应用模型。
约定：尚未接入默认 Simulator；价格/数量参数必须显式传入，不代表真实盘口能力。
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from json import dumps
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import NAMESPACE_URL, uuid5

__all__ = [
    "SimulationPolicy",
    "SimulationQuote",
    "SimulationOrder",
    "SimulationExecution",
    "SimulationEvent",
    "SimulationMatcher",
]


def _aware(value: datetime) -> datetime:
    """输入时间，返回原时间；要求有时区，避免机器本地时间或日期被隐式解释。"""
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("仿真事件时间必须包含时区")
    return value


def _positive_decimal(value: Decimal, label: str) -> Decimal:
    """输入 Decimal 和字段名称，返回有效正值；拒绝浮点、NaN 和无限值，不做精度猜测。"""
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ValueError(f"{label}必须是有限的正 Decimal")
    return value


def _integer(value: int, label: str, minimum: int = 1) -> int:
    """输入整数、名称和下限，返回原值；拒绝布尔值和非整数，且不静默取整。"""
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{label}必须是大于等于 {minimum} 的整数")
    return value


def _json_value(value: Any) -> Any:
    """输入执行侧事实，返回无对象执行能力的 JSON 值；保留 Decimal 与时区，不写 pickle。"""
    if isinstance(value, (datetime, Decimal)):
        return str(value) if isinstance(value, Decimal) else value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _digest(value: Any) -> str:
    """输入 JSON 值，返回规范序列化 SHA256；用于损坏检测，不作为文件来源认证。"""
    return sha256(dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SimulationPolicy:
    """显式测试/业务政策；最后价交叉撮合，每事件共享数量上限，不模拟真实排队。"""

    version: str
    max_quote_age: timedelta
    max_fill_quantity: int

    def __post_init__(self) -> None:
        """无额外输入返回；验证政策版本、有效期和数量上限，不注入默认费用或流动性。"""
        if not self.version.strip():
            raise ValueError("需要显式仿真政策版本")
        if not isinstance(self.max_quote_age, timedelta) or self.max_quote_age <= timedelta(0):
            raise ValueError("需要显式正数报价有效期")
        _integer(self.max_fill_quantity, "每事件成交上限")


@dataclass(frozen=True)
class SimulationQuote:
    """单个可审计报价事件；数量是调用方按政策确认的模型容量，不冒充盘口深度。"""

    quote_id: str
    security: str
    price: Decimal
    event_at: datetime
    received_at: datetime
    quantity: int
    source: str
    kind: str
    data_version: str
    paused: bool

    def __post_init__(self) -> None:
        """无额外输入返回；校验身份、价量、时区及时间先后，不读取其他报价作为兜底。"""
        if not self.quote_id.strip() or not self.security.strip() or not self.source.strip():
            raise ValueError("报价需要事件身份、证券和来源")
        if self.kind not in {"snapshot", "trade_tick", "l1"} or not self.data_version.strip():
            raise ValueError("需要明确即时报价粒度和数据版本，不支持未完成或完整历史 bar 撮合")
        _positive_decimal(self.price, "报价")
        _integer(self.quantity, "报价模型容量", 0)
        if _aware(self.event_at) > _aware(self.received_at):
            raise ValueError("报价发生时间不能晚于收到时间")
        if not isinstance(self.paused, bool):
            raise ValueError("停牌状态必须为布尔值")


@dataclass(frozen=True)
class SimulationOrder:
    """单一模拟账户中的不可变委托视图；现金、T+及费用预约由上游共同账本校验。"""

    order_id: str
    security: str
    side: str
    quantity: int
    protection_price: Decimal
    order_type: str
    submitted_at: datetime
    expires_at: datetime
    quantity_step: int
    price_tick: Decimal
    rule_version: str
    fee_version: str
    status: str = "accepted"
    filled_quantity: int = 0


@dataclass(frozen=True)
class SimulationExecution:
    """逐笔模拟成交事实；稳定身份、行情依据和政策版本供账本去重，未在此扣款。"""

    execution_id: str
    account_id: str
    order_id: str
    security: str
    side: str
    quantity: int
    price: Decimal
    matched_at: datetime
    quote_id: str
    quote_event_at: datetime
    quote_received_at: datetime
    source: str
    quote_kind: str
    data_version: str
    policy_version: str
    rule_version: str
    fee_version: str


@dataclass(frozen=True)
class SimulationEvent:
    """有序状态事实；成交终态不能撤回，取消和到期只终止剩余量。"""

    sequence: int
    order_id: str
    status: str
    at: datetime
    filled_quantity: int
    reason: str


class SimulationMatcher:
    """单账户纯撮合器；串行处理意图与行情，只产出事实，不连接任何真实 Broker。"""

    def __init__(
        self,
        *,
        account_id: str,
        policy: SimulationPolicy,
        clock: Callable[[], datetime],
        is_trading_time: Callable[[str, datetime], bool],
    ) -> None:
        """输入模拟账户、政策、时钟和纯时段判断，无返回；初始化空事实，不持久化或联网。"""
        if not account_id.strip():
            raise ValueError("需要模拟账户身份")
        if not isinstance(policy, SimulationPolicy):
            raise ValueError("需要显式仿真政策对象")
        self._account_id, self._policy = account_id, policy
        self._clock, self._is_trading_time = clock, is_trading_time
        self._orders: Dict[str, SimulationOrder] = {}
        self._executions: List[SimulationExecution] = []
        self._events: List[SimulationEvent] = []
        self._quotes: Dict[str, SimulationQuote] = {}
        self._fill_checks: Dict[str, Dict[str, bool]] = {}
        self._latest: Dict[str, Tuple[datetime, datetime]] = {}
        self._commands: List[Dict[str, Any]] = []
        self._last_time = None
        self._lock = RLock()

    @property
    def account_id(self) -> str:
        """无输入，返回固定模拟账户身份；同实例不能改成另一个账本。"""
        return self._account_id

    @property
    def policy(self) -> SimulationPolicy:
        """无输入，返回固定不可变政策；更新政策必须新建撮合实例，不改变原委托事实。"""
        return self._policy

    def _now(self) -> datetime:
        """无输入，返回注入时钟时间；逆行时报错，不使用墙钟替代。"""
        value = _aware(self._clock())
        if self._last_time is not None and value < self._last_time:
            raise ValueError("仿真时钟不能逆行")
        self._last_time = value
        return value

    def _record(self, order: SimulationOrder, at: datetime, reason: str) -> None:
        """输入新订单视图、时间和原因，无返回；在锁内保存投影及追加状态事实。"""
        self._orders[order.order_id] = order
        self._events.append(
            SimulationEvent(
                len(self._events) + 1,
                order.order_id,
                order.status,
                at,
                order.filled_quantity,
                reason,
            )
        )

    def _command(self, operation: str, at: datetime, arguments: Dict[str, Any]) -> None:
        """输入已完成操作、观测时间和参数，无返回；锁内保存恢复日志，不调用账本或网络。"""
        self._commands.append(
            {"operation": operation, "at": at.isoformat(), "arguments": _json_value(arguments)}
        )

    def submit(
        self,
        *,
        order_id: str,
        security: str,
        side: str,
        quantity: int,
        protection_price: Decimal,
        order_type: str,
        expires_at: datetime,
        quantity_step: int,
        price_tick: Decimal,
        rule_version: str,
        fee_version: str,
    ) -> SimulationOrder:
        """输入已预约的委托和固定规则，返回已受理视图；同身份幂等，改变经济输入则拒绝，不成交。"""
        with self._lock:
            at = self._now()
            if not order_id.strip() or not security.strip() or side not in {"buy", "sell"}:
                raise ValueError("委托身份、证券或方向无效")
            if order_type not in {"limit", "protected_market"}:
                raise ValueError("只支持限价或明确保护价的模拟委托")
            _integer(quantity, "委托数量")
            _integer(quantity_step, "成交数量步长")
            _positive_decimal(protection_price, "限价/保护价")
            _positive_decimal(price_tick, "价格步长")
            if quantity % quantity_step or protection_price % price_tick:
                raise ValueError("委托数量或价格不符合已确认交易单位")
            if not rule_version.strip() or not fee_version.strip():
                raise ValueError("需要证券和费用规则版本")
            if _aware(expires_at) <= at and order_id not in self._orders:
                raise ValueError("新委托有效期必须晚于受理时间")
            candidate = SimulationOrder(
                order_id,
                security,
                side,
                quantity,
                protection_price,
                order_type,
                at,
                expires_at,
                quantity_step,
                price_tick,
                rule_version,
                fee_version,
            )
            previous = self._orders.get(order_id)
            if previous is not None:
                original = replace(previous, status="accepted", filled_quantity=0)
                if replace(candidate, submitted_at=previous.submitted_at) != original:
                    raise ValueError("相同委托身份不能修改经济输入")
                return previous
            self._record(candidate, at, "ACCEPTED_WAITING_FOR_QUOTE")
            arguments = asdict(candidate)
            for field in ("submitted_at", "status", "filled_quantity"):
                arguments.pop(field)
            self._command("submit", at, arguments)
            return candidate

    def advance(self) -> Tuple[SimulationEvent, ...]:
        """无输入，按注入时间终止到期余量并返回新事件；没有行情也可到期，不改已成交数量。"""
        with self._lock:
            start, at = len(self._events), self._now()
            self._expire(at)
            self._command("advance", at, {})
            return tuple(self._events[start:])

    def _expire(self, at: datetime) -> None:
        """输入同一事务观测时间，无返回；锁内终止到期余量，避免单事件多次读取移动时钟。"""
        for order in tuple(self._orders.values()):
            if order.status in {"accepted", "partially_filled"} and at >= order.expires_at:
                self._record(replace(order, status="expired"), at, "VALIDITY_EXPIRED")

    def observed_quote(self, quote_id: str) -> SimulationQuote | None:
        """输入报价身份，返回已消费的不可变原报价或空；只读且不推进时钟、容量或命令。

        供实时读取适配在重启后保留第一次接收时间，不以新轮询时间重新定义同一源事件。
        不返回内部可变容器，不替代调用方对价、量、来源及政策一致性的检查。
        """
        if not isinstance(quote_id, str) or not quote_id.strip():
            raise ValueError("请提供明确报价身份")
        with self._lock:
            return self._quotes.get(quote_id)

    def process_quote(
        self,
        quote: SimulationQuote,
        *,
        can_fill: Optional[Callable[[SimulationOrder, int, Decimal], bool]] = None,
    ) -> Tuple[SimulationExecution, ...]:
        """输入报价及可选纯候选核对，返回成交；拒绝候选不耗容量，判断随命令持久保存。

        can_fill 接收原单、候选数量及价格并返回严格布尔值，不能联网或改账本。
        恢复只使用原判断；未注入时原日志格式和撮合行为不变，回调异常不产生成交事实。
        """
        if can_fill is not None and not callable(can_fill):
            raise ValueError("候选成交核对必须可调用")
        with self._lock:
            at = self._now()
            if quote.received_at > at:
                raise ValueError("不能用尚未收到的未来报价撮合")
            previous = self._quotes.get(quote.quote_id)
            if previous is not None:
                if previous != quote:
                    raise ValueError("相同报价身份包含不同事实")
                return ()
            order_key = (quote.event_at, quote.received_at)
            out_of_order = order_key < self._latest.get(quote.security, order_key)
            eligible = (
                not out_of_order
                and not quote.paused
                and (at - quote.event_at <= self.policy.max_quote_age)
            )
            # 先取得纯时段判断；外部判断异常不得留下半个报价或到期事实。
            trading = self._is_trading_time(quote.security, at) if eligible else None
            if trading is not None and not isinstance(trading, bool):
                raise ValueError("交易时段判断必须返回布尔值")
            remaining = min(quote.quantity, self.policy.max_fill_quantity)
            planned = []
            checks = {}
            for order in tuple(self._orders.values()):
                if not eligible or not trading or at >= order.expires_at:
                    continue
                if order.security != quote.security or order.status not in {
                    "accepted",
                    "partially_filled",
                }:
                    continue
                if quote.received_at < order.submitted_at or quote.price % order.price_tick:
                    continue
                if (order.side == "buy" and quote.price > order.protection_price) or (
                    order.side == "sell" and quote.price < order.protection_price
                ):
                    continue
                quantity = min(remaining, order.quantity - order.filled_quantity)
                quantity -= quantity % order.quantity_step
                if not quantity:
                    continue
                if can_fill is not None:
                    decision = can_fill(order, quantity, quote.price)
                    if not isinstance(decision, bool):
                        raise ValueError("候选成交核对必须返回布尔值")
                    checks[order.order_id] = decision
                    if not decision:
                        continue
                planned.append((order, quantity))
                remaining -= quantity
            self._expire(at)
            self._quotes[quote.quote_id] = quote
            arguments = {"quote": asdict(quote), "trading": trading}
            if can_fill is not None:
                arguments["fill_checks"] = checks
                self._fill_checks[quote.quote_id] = dict(checks)
            self._command("quote", at, arguments)
            if out_of_order:
                return ()
            self._latest[quote.security] = order_key
            start = len(self._executions)
            for order, quantity in planned:
                filled = order.filled_quantity + quantity
                status = "filled" if filled == order.quantity else "partially_filled"
                execution = SimulationExecution(
                    str(
                        uuid5(
                            NAMESPACE_URL,
                            dumps(
                                ["simulation", self.account_id, order.order_id, quote.quote_id],
                                ensure_ascii=True,
                                separators=(",", ":"),
                            ),
                        )
                    ),
                    self.account_id,
                    order.order_id,
                    order.security,
                    order.side,
                    quantity,
                    quote.price,
                    at,
                    quote.quote_id,
                    quote.event_at,
                    quote.received_at,
                    quote.source,
                    quote.kind,
                    quote.data_version,
                    self.policy.version,
                    order.rule_version,
                    order.fee_version,
                )
                self._executions.append(execution)
                self._record(
                    replace(order, status=status, filled_quantity=filled), at, "QUOTE_MATCHED"
                )
            return tuple(self._executions[start:])

    def candidate_checks(self, quote_id: str) -> Optional[Dict[str, bool]]:
        """输入原报价号，返回当时候选核对副本；旧无核对返回None，不推进时钟或重新判断。"""
        with self._lock:
            checks = self._fill_checks.get(quote_id)
            return dict(checks) if checks is not None else None

    def cancel(self, order_id: str) -> bool:
        """输入原委托身份，返回是否已确认取消余量；已成交/到期或不存在返回 False，不逆转成交。"""
        with self._lock:
            at = self._now()
            self._expire(at)
            self._command("cancel", at, {"order_id": order_id})
            order = self._orders.get(order_id)
            if order is None or order.status in {"filled", "expired"}:
                return False
            if order.status == "cancelled":
                return True
            self._record(replace(order, status="cancelled"), at, "REMAINDER_CANCELLED")
            return True

    def orders(self) -> Tuple[SimulationOrder, ...]:
        """无输入，返回不可变订单视图副本；读取不推进时钟、到期或账本。"""
        with self._lock:
            return tuple(self._orders.values())

    def executions(self) -> Tuple[SimulationExecution, ...]:
        """无输入，返回不可变逐笔事实副本；费用计算和账户结算由上游完成。"""
        with self._lock:
            return tuple(self._executions)

    def events(self) -> Tuple[SimulationEvent, ...]:
        """无输入，返回不可变事件副本；状态投影不能替代逐笔成交或现金流水。"""
        with self._lock:
            return tuple(self._events)

    def checkpoint(self) -> Dict[str, Any]:
        """无输入，返回独立 JSON 执行快照；读操作不推进时间，不包含现金、持仓或结算游标。"""
        with self._lock:
            age = self.policy.max_quote_age
            payload = {
                "schema": 1,
                "environment": "simulation",
                "account_id": self.account_id,
                "policy": {
                    "version": self.policy.version,
                    "max_quote_age_us": ((age.days * 86400 + age.seconds) * 1000000)
                    + age.microseconds,
                    "max_fill_quantity": self.policy.max_fill_quantity,
                },
                "last_time": self._last_time.isoformat() if self._last_time else None,
                "commands": deepcopy(self._commands),
                "facts_digest": _digest(
                    _json_value(
                        {
                            "orders": [asdict(item) for item in self._orders.values()],
                            "executions": [asdict(item) for item in self._executions],
                            "events": [asdict(item) for item in self._events],
                        }
                    )
                ),
            }
            return {"payload": payload, "sha256": _digest(payload)}

    @classmethod
    def restore(
        cls,
        checkpoint: Dict[str, Any],
        *,
        account_id: str,
        policy: SimulationPolicy,
        clock: Callable[[], datetime],
        is_trading_time: Callable[[str, datetime], bool],
    ) -> "SimulationMatcher":
        """输入快照及原账户/政策和新时钟，返回恢复实例；损坏或身份/政策不符拒绝，不改旧实例。

        重建仅使用已记录命令、原观测时间与时段判断，不拉行情、不调用新日历或账本，
        不重新对外发出成交。恢复后 executions() 保留原稳定事实供共同账本去重。
        """
        try:
            document = deepcopy(checkpoint)
            if set(document) != {"payload", "sha256"}:
                raise ValueError("仿真快照结构无效")
            payload = document["payload"]
            if _digest(payload) != document["sha256"]:
                raise ValueError("仿真快照校验失败")
            if (
                set(payload)
                != {
                    "schema",
                    "environment",
                    "account_id",
                    "policy",
                    "last_time",
                    "commands",
                    "facts_digest",
                }
                or type(payload["schema"]) is not int
                or payload["schema"] != 1
            ):
                raise ValueError("不支持的仿真快照版本")
            if payload["environment"] != "simulation" or payload["account_id"] != account_id:
                raise ValueError("仿真快照环境或账户不符")
            observed_time = [None]

            def replay_clock() -> datetime:
                """无输入，返回当前日志时间；恢复过程中不读取系统或新运行时钟。"""
                return observed_time[0]

            restored = cls(
                account_id=account_id,
                policy=policy,
                clock=replay_clock,
                is_trading_time=is_trading_time,
            )
            if restored.checkpoint()["payload"]["policy"] != payload["policy"]:
                raise ValueError("仿真快照政策不符")
            if not isinstance(payload["commands"], list):
                raise ValueError("仿真恢复日志无效")
            for command in payload["commands"]:
                if set(command) != {"operation", "at", "arguments"}:
                    raise ValueError("仿真恢复命令无效")
                observed_time[0] = _aware(datetime.fromisoformat(command["at"]))
                arguments = deepcopy(command["arguments"])
                operation = command["operation"]
                if operation == "submit":
                    for field in ("protection_price", "price_tick"):
                        arguments[field] = Decimal(arguments[field])
                    arguments["expires_at"] = datetime.fromisoformat(arguments["expires_at"])
                    restored.submit(**arguments)
                elif operation == "quote":
                    if set(arguments) not in (
                        {"quote", "trading"},
                        {"quote", "trading", "fill_checks"},
                    ):
                        raise ValueError("仿真报价命令无效")
                    data = arguments["quote"]
                    data["price"] = Decimal(data["price"])
                    for field in ("event_at", "received_at"):
                        data[field] = datetime.fromisoformat(data[field])
                    decision = arguments["trading"]

                    def replay_trading(security: str, at: datetime) -> bool:
                        """输入日志证券和时间，返回原判断；缺判断则拒绝恢复，不套用新市场规则。"""
                        if not isinstance(decision, bool):
                            raise ValueError("仿真报价缺少原交易时段判断")
                        return decision

                    restored._is_trading_time = replay_trading
                    checks = arguments.get("fill_checks")
                    seen = set()
                    if "fill_checks" in arguments and (
                        not isinstance(checks, dict)
                        or any(not isinstance(value, bool) for value in checks.values())
                    ):
                        raise ValueError("仿真报价缺少明确的原成交核对")

                    def replay_fill(order: SimulationOrder, quantity: int, price: Decimal) -> bool:
                        """输入重建候选，返回原布尔判断；不读取新费用，缺项或重复项拒绝恢复。"""
                        if order.order_id not in checks or order.order_id in seen:
                            raise ValueError("仿真候选成交核对与原命令不一致")
                        seen.add(order.order_id)
                        return checks[order.order_id]

                    restored.process_quote(
                        SimulationQuote(**data),
                        can_fill=replay_fill if checks is not None else None,
                    )
                    if checks is not None and seen != set(checks):
                        raise ValueError("仿真候选成交核对包含多余原单")
                elif operation == "cancel":
                    if set(arguments) != {"order_id"}:
                        raise ValueError("仿真撤单命令无效")
                    restored.cancel(arguments["order_id"])
                elif operation == "advance" and arguments == {}:
                    restored.advance()
                else:
                    raise ValueError("不支持的仿真恢复命令")
            last_time = payload["last_time"]
            if last_time is not None:
                last_time = _aware(datetime.fromisoformat(last_time))
                if restored._last_time is not None and last_time < restored._last_time:
                    raise ValueError("仿真快照时间水位逆行")
            elif restored._last_time is not None:
                raise ValueError("仿真快照缺少时间水位")
            restored._last_time = last_time
            if restored.checkpoint()["payload"] != payload:
                raise ValueError("仿真恢复事实与日志不一致")
            restored._clock, restored._is_trading_time = clock, is_trading_time
            return restored
        except (TypeError, KeyError, AttributeError, ArithmeticError) as exc:
            raise ValueError("仿真快照内容无效") from exc
