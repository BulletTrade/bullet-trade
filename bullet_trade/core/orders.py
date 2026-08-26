"""
作者：BruceLee

文件职责：提供聚宽兼容的下单、目标下单和撤单 API，
并管理进程内待处理订单队列。
主要输入：证券代码、数量或价值、价格风格、等待超时，
以及当前 Engine 运行状态。
主要输出：Order 对象、撤单结果，
以及供回测或实盘 Engine 原子取走的订单批次。
上下游关系：上游为策略公开 API；下游为 BacktestEngine、LiveEngine 与 Broker。
关键约定：全局队列的追加、取走、拒绝分区和清空由 threading.RLock 保护；
本模块不访问网络，实际交易写操作由 LiveEngine/Broker 完成。
"""

import asyncio
import contextvars
import inspect
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, Dict, Iterator, List, Optional, Union

from .globals import log
from .models import Order, OrderStatus, OrderStyle
from .runtime import get_current_engine, process_orders_now
from .settings import get_settings

# 全局订单队列
_order_queue: List[Order] = []
_order_queue_lock = threading.RLock()
_current_order_batch: contextvars.ContextVar[
    Optional["_OrderBatchContext"]
] = contextvars.ContextVar("bullet_trade_order_batch", default=None)


@dataclass
class _OrderBatchContext:
    """保存一次安全调度批次在派生执行上下文中的共享状态。

    ``ContextVar`` 复制时仍引用同一个本对象，因此异步子任务和已显式
    传播 context 的同步 worker 能看到批次后续的成功或失败终态。
    """

    token: str
    state: str = "active"
    rejection_reason: Optional[str] = None


def _mark_order_rejected(order_obj: Order, reason: str) -> None:
    """为未提交订单写入稳定拒绝状态和原因。

    Args:
        order_obj: 尚未提交 Broker 的订单对象。
        reason: 稳定、可审计的拒绝原因。

    Returns:
        None。

    Side Effects:
        更新订单状态及 ``extra.rejection_reason``。
    """
    order_obj.status = OrderStatus.rejected
    order_obj.extra["rejection_reason"] = reason


def _enqueue_order(order_obj: Order) -> bool:
    """原子检查实盘失败门禁并将获准订单加入待处理队列。

    Args:
        order_obj: 已完成普通参数和能力校验的订单对象。

    Returns:
        bool: 成功入队返回 True；批次或引擎门禁拒绝时返回 False。

    Side Effects:
        为当前调度上下文记录批次 token；获准时追加队列，拒绝时更新
        订单状态和原因。检查门禁与入队在同一 RLock 临界区完成。
    """
    with _order_queue_lock:
        engine = get_current_engine()
        engine_rejection_reason = None
        if getattr(engine, "is_live", False):
            engine_rejection_reason = getattr(
                engine,
                "_schedule_batch_failed_reason",
                None,
            )

        batch = _current_order_batch.get()
        if batch is not None:
            setattr(order_obj, "_schedule_batch_token", batch.token)

        rejection_reason = engine_rejection_reason
        if rejection_reason is None and batch is not None and batch.state != "active":
            rejection_reason = batch.rejection_reason or "scheduler_batch_closed"
        if rejection_reason is not None:
            _mark_order_rejected(order_obj, str(rejection_reason))
            return False

        _order_queue.append(order_obj)
        return True


@contextmanager
def _order_batch_scope() -> Iterator[_OrderBatchContext]:
    """建立并传播一次唯一的安全调度订单批次上下文。

    Returns:
        Iterator[_OrderBatchContext]: 供 ``with`` 使用的共享批次状态。

    Side Effects:
        临时设置当前 ``ContextVar``，退出时恢复调用方原上下文。
    """
    batch = _OrderBatchContext(token=str(uuid.uuid4()))
    reset_token = _current_order_batch.set(batch)
    try:
        yield batch
    finally:
        _current_order_batch.reset(reset_token)


def _complete_order_batch(batch: _OrderBatchContext) -> None:
    """原子关闭成功批次，拒绝之后才迟到的派生订单。

    Args:
        batch: 当前安全调度批次的共享状态对象。

    Returns:
        None。

    Side Effects:
        将仍为 active 的批次改为 closed；已在队列中的批次订单不变。
    """
    with _order_queue_lock:
        if batch.state == "active":
            batch.state = "closed"
            batch.rejection_reason = "scheduler_batch_closed"


def _reject_order_batch(
    batch: _OrderBatchContext,
    engine: object,
    reason: str,
) -> int:
    """原子锁死引擎写入并拒绝指定失败批次的已入队订单。

    Args:
        batch: 当前安全调度批次的共享状态对象。
        engine: 拥有本批次且即将进入失败态的 LiveEngine。
        reason: 稳定、可审计的调度失败原因。

    Returns:
        int: 本次从队列拒绝并移除的同批次订单数量。

    Side Effects:
        在队列锁保护下设置引擎持久失败门禁、关闭批次，并原子更新
        队列；批次外订单保持原状态和顺序。迟到派生订单将看到终态。
    """
    with _order_queue_lock:
        batch.state = "failed"
        batch.rejection_reason = reason
        setattr(engine, "_schedule_batch_failed_reason", reason)

        retained_orders: List[Order] = []
        rejected_count = 0
        for queued_order in _order_queue:
            if getattr(queued_order, "_schedule_batch_token", None) != batch.token:
                retained_orders.append(queued_order)
                continue
            _mark_order_rejected(queued_order, reason)
            rejected_count += 1
        _order_queue[:] = retained_orders
        return rejected_count


def _drain_order_queue() -> List[Order]:
    """原子取出并清空当前订单队列。

    Returns:
        List[Order]: 原子交换前按原顺序排列的订单快照。

    Side Effects:
        在队列锁保护下原地清空；之后的并发新订单保留在
        同一队列对象中。
    """
    with _order_queue_lock:
        drained = list(_order_queue)
        _order_queue.clear()
        return drained


@dataclass
class MarketOrderStyle:
    """描述策略公共市价意图及可选券商原生类型。

    ``limit_price`` 和买卖价差保持既有位置参数顺序；``market_type`` 只承载
    home_best 等公共 canonical 名称，由具体券商按交易所白名单解释并失败关闭。
    """

    limit_price: Optional[float] = None
    buy_price_percent: Optional[float] = None
    sell_price_percent: Optional[float] = None
    market_type: Optional[str] = None


@dataclass
class LimitOrderStyle:
    """限价单参数：显式给出委托价格。"""

    price: float


def _generate_order_id() -> str:
    """生成唯一订单 ID。

    Returns:
        str: UUID4 字符串。

    Side Effects:
        调用系统随机源生成 UUID。
    """
    return str(uuid.uuid4())


def _trigger_order_processing(wait_timeout: Optional[float] = None) -> None:
    """按当前引擎语义触发订单队列处理。

    Args:
        wait_timeout: 实盘等待 broker 处理的超时秒数；None 使用原有阻塞语义。

    Returns:
        None。严格 checkpoint 实盘引擎只保留队列，由分钟持久化成功后统一排空；
        其他实盘与回测保持原有投递语义。

    Side Effects:
        实盘时可向事件循环投递 ``_process_orders``；回测即时撮合时可
        直接处理队列。安全调度批次中仅保留入队状态。
    """
    try:
        settings = get_settings()
        engine = get_current_engine()
        if getattr(engine, "is_live", False):
            if getattr(engine, "defer_order_processing", False):
                return
            defer_check = getattr(engine, "should_defer_order_processing", None)
            if callable(defer_check) and defer_check():
                return
            loop = getattr(engine, "_loop", None)
            if loop and loop.is_running():
                wait_for_result = wait_timeout is None or wait_timeout > 0
                try:
                    try:
                        running_loop = asyncio.get_running_loop()
                    except RuntimeError:
                        running_loop = None

                    if wait_for_result and running_loop is None:
                        fut = asyncio.run_coroutine_threadsafe(
                            engine._process_orders(engine.context.current_dt),
                            loop,
                        )
                        fut.result(
                            timeout=wait_timeout if wait_timeout and wait_timeout > 0 else None
                        )
                    else:
                        loop.call_soon_threadsafe(
                            lambda: asyncio.create_task(
                                engine._process_orders(engine.context.current_dt)
                            )
                        )
                except Exception as exc:
                    log.debug(f"投递实盘订单处理任务失败: {exc}")
                return
        if settings.options.get("order_match_mode") == "immediate":
            process_orders_now()
    except Exception as e:
        log.warning(f"触发订单处理失败，保留到队列: {e}")


def _register_order_snapshot(order_obj: Order) -> None:
    """将新建订单快照注册到当前 Engine。

    Args:
        order_obj: 刚创建并已入队的订单对象。

    Returns:
        None。

    Side Effects:
        当 Engine 提供 ``_register_order`` 时更新其订单索引。
    """
    engine = get_current_engine()
    if not engine:
        return
    register = getattr(engine, "_register_order", None)
    if callable(register):
        try:
            register(order_obj)
        except Exception:
            pass


def _format_order_price(value: Optional[float]) -> str:
    """将订单价格格式化为日志文本。

    Args:
        value: 可选价格数值。

    Returns:
        str: 四位小数价格或“未指定”。

    Side Effects:
        无。
    """
    if value is None:
        return "未指定"
    return f"{float(value):.4f}"


def _describe_order_style(style: object) -> str:
    """生成订单价格风格的稳定日志描述。

    Args:
        style: 市价、限价或其他兼容风格对象。

    Returns:
        str: 包含关键价格参数的风格描述。

    Side Effects:
        无。
    """
    if isinstance(style, LimitOrderStyle):
        return f"LimitOrderStyle(price={_format_order_price(style.price)})"
    if isinstance(style, MarketOrderStyle):
        market_type = str(style.market_type or "").strip().lower()
        if style.limit_price is not None:
            suffix = f", market_type={market_type}" if market_type else ""
            return (
                "MarketOrderStyle(" f"limit_price={_format_order_price(style.limit_price)}{suffix})"
            )
        if market_type:
            return f"MarketOrderStyle(market_type={market_type})"
        return "MarketOrderStyle(market)"
    return style.__class__.__name__


def _resolve_log_price(
    price: Optional[float],
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]],
) -> Optional[float]:
    """解析用于日志和订单元数据的用户请求价格。

    Args:
        price: 公开 API 的显式价格。
        style: 可选市价或限价风格。

    Returns:
        Optional[float]: 显式限价或保护价；纯市价意图返回 None。

    Side Effects:
        无。
    """
    if isinstance(style, LimitOrderStyle):
        return float(style.price)
    if isinstance(style, MarketOrderStyle) and style.limit_price is not None:
        return float(style.limit_price)
    return float(price) if price is not None else None


def _record_requested_order_price(
    order_obj: Order,
    price: Optional[float],
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]],
) -> None:
    """把用户请求价格记录到订单扩展字段。

    Args:
        order_obj: 待记录的订单对象。
        price: 公开 API 的显式价格。
        style: 可选市价或限价风格。

    Returns:
        None。

    Side Effects:
        有明确价格时更新 ``order_obj.extra``。
    """
    requested_price = _resolve_log_price(price, style)
    if requested_price is None:
        return
    extra = getattr(order_obj, "extra", None)
    if extra is None:
        order_obj.extra = {}
        extra = order_obj.extra
    extra["order_price"] = float(requested_price)
    extra.setdefault("requested_order_price", float(requested_price))


def _record_market_order_type(
    order_obj: Order,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]],
) -> None:
    """把显式原生市价类型记录到订单扩展字段。

    Args:
        order_obj: 待记录的订单对象。
        style: 可选市价或限价风格。

    Returns:
        None。

    Side Effects:
        仅当 ``MarketOrderStyle.market_type`` 非空时更新 ``order_obj.extra``，
        供 Live broker 和远程 adapter 透传而不改变其他券商的默认市价语义。
    """

    if not isinstance(style, MarketOrderStyle):
        return
    market_type = str(style.market_type or "").strip().lower()
    if market_type:
        order_obj.extra["market_type"] = market_type


def _finish_scheduled_broker_cancel(
    task: Any,
    *,
    order_obj: Optional[Order],
    broker_id: str,
) -> None:
    """收口同一事件循环内非阻塞投递的券商撤单结果。

    Args:
        task: ``asyncio.Task`` 或等价 future。
        order_obj: 可选策略订单对象。
        broker_id: 已提交的精确券商订单号。

    Returns:
        None。

    Side Effects:
        异步撤单被券商确认接受时把订单标为 canceling；异常或 False 仅记录告警，
        不在事件循环回调中再次发起撤单。
    """

    try:
        accepted = bool(task.result())
    except asyncio.CancelledError:
        log.warning(f"券商撤单任务已取消: {broker_id}")
        return
    except Exception as exc:
        log.warning(f"券商撤单失败 {broker_id}: {exc}")
        return
    if not accepted:
        log.warning(f"券商未确认接受撤单: {broker_id}")
        return
    log.info(f"券商撤单已提交: {broker_id}")
    if order_obj is not None:
        try:
            order_obj.status = OrderStatus.canceling
        except Exception:
            pass


def _validate_live_order_request(requires_realtime_snapshot: bool) -> None:
    """在订单入队前调用 LiveEngine 的启动阶段和数据能力门禁。

    Args:
        requires_realtime_snapshot: 订单数量或市价保护价是否依赖新鲜快照。

    Returns:
        None；回测引擎、旧兼容引擎或预检通过时正常返回。

    Raises:
        RuntimeError: LiveEngine 尚处于 initialize/预检/关闭阶段时抛出。
        DataCapabilityError: 启用能力合同且实时快照 owner 不可用时抛出。

    Side Effects:
        可在 LiveEngine 中记录一个 RouteDecision，不会把订单加入队列。
    """

    engine = get_current_engine()
    if not engine or not getattr(engine, "is_live", False):
        return
    validator = getattr(engine, "validate_order_request", None)
    if callable(validator):
        validator(requires_realtime_snapshot)


def order(
    security: str,
    amount: int,
    price: Optional[float] = None,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = None,
    wait_timeout: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[Order]:
    """
    按股数下单

    Args:
        security: 标的代码
        amount: 股数，正数表示买入，负数表示卖出
        price: 委托价格，None表示市价单
        style: 下单方式或市价参数（策略覆写）
        wait_timeout: 实盘下单等待超时（秒）；
            None（默认）使用全局 TRADE_MAX_WAIT_TIME（默认16秒）；
            >0 同步等待指定秒数；0 异步立即返回。
            回测模式下此参数无效。
        extra: 传给 live broker 的订单扩展字段，例如 order_remark / strategy_name。

    Returns:
        Order对象，如果下单失败返回None

    Side Effects:
        原子追加全局订单队列、注册 Engine 快照，并可触发撮合或
        LiveEngine 订单处理。
    """
    if isinstance(price, (MarketOrderStyle, LimitOrderStyle)):
        style = price
        price = None

    if amount == 0:
        log.warning(f"下单数量为0，忽略订单: {security}")
        return None

    if style is not None:
        resolved_style: object = style
    elif price is not None:
        # 与聚宽语义保持一致：price 参数表示显式限价。
        resolved_style = LimitOrderStyle(price)
    else:
        resolved_style = MarketOrderStyle()

    _validate_live_order_request(not isinstance(resolved_style, LimitOrderStyle))

    order_obj = Order(
        order_id=_generate_order_id(),
        security=security,
        amount=abs(amount),
        price=price if price is not None else 0.0,
        status=OrderStatus.open,
        add_time=datetime.now(),
        is_buy=(amount > 0),
        style=resolved_style,
        wait_timeout=wait_timeout,
    )
    _record_requested_order_price(order_obj, price, resolved_style)
    if extra:
        order_obj.extra.update(dict(extra))
    # 显式 style 是权威语义，不能被旧 extra 中同名字段覆盖。
    _record_market_order_type(order_obj, resolved_style)

    enqueued = _enqueue_order(order_obj)
    _register_order_snapshot(order_obj)
    log.debug(
        f"创建订单: {security}, 数量: {amount}, 风格: {_describe_order_style(resolved_style)}, "
        f"价格: {_format_order_price(_resolve_log_price(price, resolved_style))}"
    )
    if enqueued:
        _trigger_order_processing(wait_timeout)

    return order_obj


def cancel_order(order_or_id: Union[Order, str]) -> bool:
    """撤销本地待处理订单或向 broker 提交已落柜撤单。

    Args:
        order_or_id: Order 对象或订单 ID。

    Returns:
        bool: 本地订单已移除或 broker 接受撤单时为 True。

    Raises:
        RuntimeError: 严格 checkpoint 模式检测到已提交 broker 的订单时抛出，
        防止撤单在本分钟 ``g`` 保存前形成外部副作用。

    Side Effects:
        可在队列锁保护下移除订单并更新状态；已提交订单可产生
        Broker 撤单写操作。
    """

    target_id = order_or_id.order_id if isinstance(order_or_id, Order) else str(order_or_id)
    engine = get_current_engine()
    broker_id = (
        getattr(order_or_id, "_broker_order_id", None)
        if isinstance(order_or_id, Order)
        else str(order_or_id)
    )
    if getattr(engine, "defer_order_processing", False) and broker_id:
        raise RuntimeError("严格 checkpoint 模式拒绝直接撤销已提交 broker 的订单；" "请在受 checkpoint 保护的执行层处理撤单")

    removed = False
    with _order_queue_lock:
        for idx, queued in list(enumerate(_order_queue)):
            if queued.order_id == target_id:
                _order_queue.pop(idx)
                log.info(f"本地队列撤单成功: {target_id}")
                try:
                    queued.status = OrderStatus.canceled
                except Exception:
                    pass
                removed = True
                break
    if engine and getattr(engine, "broker", None) and broker_id:
        write_validator = getattr(engine, "validate_broker_write_request", None)
        if callable(write_validator):
            write_validator("cancel_order")
        try:
            result = engine.broker.cancel_order(str(broker_id))
            if inspect.isawaitable(result):
                loop = getattr(engine, "_loop", None)
                if loop and loop.is_running():
                    try:
                        running_loop = asyncio.get_running_loop()
                    except RuntimeError:
                        running_loop = None
                    if running_loop is loop:
                        task = loop.create_task(result)
                        task.add_done_callback(
                            partial(
                                _finish_scheduled_broker_cancel,
                                order_obj=(order_or_id if isinstance(order_or_id, Order) else None),
                                broker_id=str(broker_id),
                            )
                        )
                        log.info(f"券商撤单已投递事件循环: {broker_id}")
                        return True
                    fut = asyncio.run_coroutine_threadsafe(result, loop)
                    result = fut.result()
                else:
                    result = asyncio.run(result)
            if result:
                log.info(f"券商撤单已提交: {broker_id}")
                if isinstance(order_or_id, Order):
                    try:
                        order_or_id.status = OrderStatus.canceling
                    except Exception:
                        pass
                return True
        except Exception as exc:
            log.warning(f"券商撤单失败 {broker_id}: {exc}")
    return removed


def cancel_all_orders() -> int:
    """原子取消本地队列的所有订单。

    Returns:
        int: 被标记为 canceled 并移出队列的订单数量。

    Side Effects:
        在队列锁保护下更新订单状态并原地清空全局队列。
    """
    with _order_queue_lock:
        count = len(_order_queue)
        for queued in _order_queue:
            try:
                queued.status = OrderStatus.canceled
            except Exception:
                pass
        _order_queue.clear()
    if count:
        log.info(f"已清空本地订单队列，共 {count} 笔")
    return count


def order_value(
    security: str,
    value: float,
    price: Optional[float] = None,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = None,
    wait_timeout: Optional[float] = None,
) -> Optional[Order]:
    """
    按价值下单

    Args:
        security: 标的代码
        value: 目标价值，正数表示买入，负数表示卖出
        price: 委托价格，None表示市价单
        style: 下单方式或市价参数（策略覆写）。
        wait_timeout: 实盘下单等待超时（秒）；
            None（默认）使用全局 TRADE_MAX_WAIT_TIME（默认16秒）；
            >0 同步等待指定秒数；0 异步立即返回。
            回测模式下此参数无效。

    Returns:
        Order对象，如果下单失败返回None

    Side Effects:
        原子追加全局订单队列、注册 Engine 快照，并可触发后续处理。

    Note:
        实际数量会在撮合时根据当前价格计算
    """
    if isinstance(price, (MarketOrderStyle, LimitOrderStyle)):
        style = price
        price = None

    if value == 0:
        log.warning(f"下单价值为0，忽略订单: {security}")
        return None

    # 临时订单，amount会在撮合时计算
    if style is not None:
        resolved_style: object = style
    elif price is not None:
        resolved_style = LimitOrderStyle(price)
    else:
        resolved_style = MarketOrderStyle()

    _validate_live_order_request(True)

    order_obj = Order(
        order_id=_generate_order_id(),
        security=security,
        amount=0,  # 会在撮合时计算
        price=price if price is not None else 0.0,
        status=OrderStatus.open,
        add_time=datetime.now(),
        is_buy=(value > 0),
        style=resolved_style,
        wait_timeout=wait_timeout,
    )
    _record_requested_order_price(order_obj, price, resolved_style)
    _record_market_order_type(order_obj, resolved_style)

    # 存储目标价值，用于撮合时计算
    order_obj._target_value = abs(value)  # type: ignore

    enqueued = _enqueue_order(order_obj)
    _register_order_snapshot(order_obj)
    log.debug(
        f"创建订单（按价值）: {security}, 价值: {value}, 风格: {_describe_order_style(resolved_style)}, "
        f"价格: {_format_order_price(_resolve_log_price(price, resolved_style))}"
    )
    if enqueued:
        _trigger_order_processing(wait_timeout)

    return order_obj


def order_target(
    security: str,
    amount: int,
    price: Optional[float] = None,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = None,
    wait_timeout: Optional[float] = None,
) -> Optional[Order]:
    """
    目标股数下单（调整持仓到目标数量）

    Args:
        security: 标的代码
        amount: 目标股数
        price: 委托价格，None表示市价单
        style: 下单方式或市价参数（策略覆写）
        wait_timeout: 实盘下单等待超时（秒）；
            None（默认）使用全局 TRADE_MAX_WAIT_TIME（默认16秒）；
            >0 同步等待指定秒数；0 异步立即返回。
            回测模式下此参数无效。

    Returns:
        Optional[Order]: 已入队的目标股数订单。

    Side Effects:
        原子追加全局订单队列、注册 Engine 快照，并可触发后续处理。
    """
    if isinstance(price, (MarketOrderStyle, LimitOrderStyle)):
        style = price
        price = None

    if style is not None:
        resolved_style: object = style
    elif price is not None:
        resolved_style = LimitOrderStyle(price)
    else:
        resolved_style = MarketOrderStyle()

    _validate_live_order_request(not isinstance(resolved_style, LimitOrderStyle))

    order_obj = Order(
        order_id=_generate_order_id(),
        security=security,
        amount=abs(amount),
        price=price if price is not None else 0.0,
        status=OrderStatus.open,
        add_time=datetime.now(),
        is_buy=True,
        style=resolved_style,
        wait_timeout=wait_timeout,
    )
    _record_requested_order_price(order_obj, price, resolved_style)
    _record_market_order_type(order_obj, resolved_style)

    order_obj._is_target_amount = True  # type: ignore
    order_obj._target_amount = amount  # type: ignore

    enqueued = _enqueue_order(order_obj)
    _register_order_snapshot(order_obj)
    log.debug(
        f"创建订单（目标股数）: {security}, 目标数量: {amount}, 风格: {_describe_order_style(resolved_style)}, "
        f"价格: {_format_order_price(_resolve_log_price(price, resolved_style))}"
    )
    if enqueued:
        _trigger_order_processing(wait_timeout)

    return order_obj


def order_target_value(
    security: str,
    value: float,
    price: Optional[float] = None,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = None,
    wait_timeout: Optional[float] = None,
) -> Optional[Order]:
    """
    目标价值下单（调整持仓到目标价值）

    Args:
        security: 标的代码
        value: 目标价值
        price: 委托价格，None表示市价单
        style: 下单方式或市价参数（策略覆写）
        wait_timeout: 实盘下单等待超时（秒）；
            None（默认）使用全局 TRADE_MAX_WAIT_TIME（默认16秒）；
            >0 同步等待指定秒数；0 异步立即返回。
            回测模式下此参数无效。

    Returns:
        Optional[Order]: 已入队的目标价值订单。

    Side Effects:
        原子追加全局订单队列、注册 Engine 快照，并可触发后续处理。
    """
    if isinstance(price, (MarketOrderStyle, LimitOrderStyle)):
        style = price
        price = None

    if style is not None:
        resolved_style: object = style
    elif price is not None:
        resolved_style = LimitOrderStyle(price)
    else:
        resolved_style = MarketOrderStyle()

    _validate_live_order_request(True)

    order_obj = Order(
        order_id=_generate_order_id(),
        security=security,
        amount=0,
        price=price if price is not None else 0.0,
        status=OrderStatus.open,
        add_time=datetime.now(),
        is_buy=True,
        style=resolved_style,
        wait_timeout=wait_timeout,
    )
    _record_requested_order_price(order_obj, price, resolved_style)
    _record_market_order_type(order_obj, resolved_style)

    order_obj._is_target_value = True  # type: ignore
    order_obj._target_value = value  # type: ignore

    enqueued = _enqueue_order(order_obj)
    _register_order_snapshot(order_obj)
    log.debug(
        f"创建订单（目标价值）: {security}, 目标价值 {value}, 风格: {_describe_order_style(resolved_style)}, "
        f"价格: {_format_order_price(_resolve_log_price(price, resolved_style))}"
    )
    if enqueued:
        _trigger_order_processing(wait_timeout)

    return order_obj


def get_order_queue() -> List[Order]:
    """获取当前订单队列的只读用途浅快照。

    Returns:
        List[Order]: 按当前顺序排列的订单对象浅拷贝。

    Side Effects:
        无；在队列锁保护下复制列表。修改返回列表不会绕过内部锁或
        改写全局订单队列。
    """
    with _order_queue_lock:
        return list(_order_queue)


def clear_order_queue() -> None:
    """原子清空订单队列。

    Returns:
        None。

    Side Effects:
        在队列锁保护下原地清空全局订单队列，并保留列表对象身份。
    """
    with _order_queue_lock:
        _order_queue.clear()


__all__ = [
    "order",
    "order_value",
    "order_target",
    "order_target_value",
    "get_order_queue",
    "clear_order_queue",
    "MarketOrderStyle",
    "LimitOrderStyle",
]
