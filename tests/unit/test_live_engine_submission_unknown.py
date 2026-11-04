"""作者：BruceLee。

职责：验收实时引擎提交未知的原号与状态，不把含糊结果当作拒单或重发。
输入：公共order、真实LiveEngine、明确未知的异步Broker夹具；输出：单次写调用与保留语义。
上下游：RemoteSubmissionUnknownError由原适配器产生；本测试不连接SDK或任何服务。
约定：只检查一次明确意图，不禁止策略下一次明确产生新意图。
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bullet_trade.core.globals import reset_globals
from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.live_runtime import init_live_runtime
from bullet_trade.core.models import OrderStatus
from bullet_trade.core.orders import LimitOrderStyle, clear_order_queue, order
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.remote import RemoteSubmissionUnknownError


def test_unknown_keeps_original_key_and_is_not_requeued(tmp_path):
    """输入临时目录，无返回；实际订单处理两轮只写一次，未知保持待确认而非拒绝。"""
    reset_globals()
    clear_order_queue()
    path = tmp_path / "strategy.py"
    path.write_text("def initialize(context):\n    pass\n")
    runtime = str(tmp_path / "runtime")
    init_live_runtime(runtime)
    unknown = RemoteSubmissionUnknownError("broker.place_order", "original-key", {})
    broker = SimpleNamespace(
        buy=AsyncMock(side_effect=unknown),
        get_positions=lambda: [],
        get_orders=lambda: [],
        get_trades=lambda: [],
        supports_account_sync=lambda: False,
    )
    engine = LiveEngine(
        path,
        broker_factory=lambda: broker,
        live_config={
            "runtime_dir": runtime,
            "risk_check_enabled": False,
            "g_autosave_enabled": False,
        },
    )
    engine.broker = broker
    engine.context.portfolio.available_cash = 100000
    engine.context.portfolio.total_value = 100000
    set_current_engine(engine)
    try:
        request = order("510300.XSHG", 100, LimitOrderStyle(3))

        async def exercise():
            """无输入返回；反复处理真实队列，原未知意图不能被再次提交。"""
            await engine._process_orders(datetime(2026, 10, 9, 10))
            await engine._process_orders(datetime(2026, 10, 9, 10, 1))

        asyncio.run(exercise())
        assert broker.buy.await_count == 1
        assert request.status is OrderStatus.new
        assert request.extra["submission_state"] == "unknown"
        assert request.extra["idempotency_key"] == "original-key"
    finally:
        set_current_engine(None)
        clear_order_queue()
        reset_globals()
