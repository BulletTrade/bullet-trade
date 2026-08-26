"""
作者: BruceLee

文件职责: 验证 LiveEngine 两阶段策略能力预检和订单按需实时价门禁。
主要输入: 临时策略、合成 capability manifest、纯内存券商和订单意图。
主要输出: 启动顺序、RouteDecision、写请求拒绝与券商调用次数断言。
上游关系: 覆盖 core.live_engine、core.orders 和 market_data.capability 的组合合同。
下游关系: 作为 Huaxin 实盘接入前的无 SDK、无网络安全回归门禁。
关键配置约定: 全部用例离线运行，不加载厂商库、不连柜台且不触发真实交易。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.core.async_scheduler import AsyncScheduler
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.orders import clear_order_queue, get_order_queue, order, order_value
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.data.api import set_current_context
from bullet_trade.market_data import (
    CapabilityDeclaration,
    CapabilityManifest,
    CapabilityReadiness,
    CapabilitySupport,
    DataCapabilityUnavailableError,
    DataSourceRouter,
    ProviderLocation,
    RouteRule,
    StrategyCapabilityProfile,
    StrategyCapabilityRequirements,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _isolate_live_globals(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """隔离实盘锁目录、当前引擎、数据上下文和全局订单队列。

    Args:
        monkeypatch: pytest 环境变量与属性替换工具。
        tmp_path: pytest 临时目录。

    Returns:
        Iterator[None]: fixture 在测试前后自动执行。

    Side Effects:
        将 ``BULLET_TRADE_HOME`` 指向临时目录并清理进程级测试状态。
    """

    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path / "bullet-trade-home"))
    clear_order_queue()
    set_current_engine(None)
    set_current_context(None)
    yield
    clear_order_queue()
    set_current_engine(None)
    set_current_context(None)


class _ReadyBroker(BrokerBase):
    """记录启动和订单调用的纯内存券商。"""

    def __init__(self) -> None:
        """初始化未连接且无委托的测试券商。

        Returns:
            None。
        """

        super().__init__("capability-test")
        self.preflight_calls = 0
        self.connect_calls = 0
        self.orders: List[Dict[str, Any]] = []

    def preflight(self) -> None:
        """记录一次离线前置检查。

        Returns:
            None。
        """

        self.preflight_calls += 1

    def connect(self) -> bool:
        """记录测试连接。

        Returns:
            始终为 True。
        """

        self.connect_calls += 1
        self._connected = True
        return True

    def disconnect(self) -> bool:
        """清理内存连接状态。

        Returns:
            始终为 True。
        """

        self._connected = False
        return True

    def get_account_info(self) -> Dict[str, Any]:
        """返回无持仓的测试账户快照。

        Returns:
            包含资金和空持仓的字典。
        """

        return {
            "account_id": self.account_id,
            "available_cash": 100_000.0,
            "total_value": 100_000.0,
            "positions": [],
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        """返回空持仓。

        Returns:
            空列表。
        """

        return []

    async def buy(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """记录一笔测试买入意图。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 委托限价或保护价。
            wait_timeout: 可选等待时间。
            remark: 可选备注。
            market: 是否为市价意图。
            extra: 可选扩展字段。

        Returns:
            稳定测试订单标识。

        Side Effects:
            把脱敏参数追加到内存 ``orders`` 列表。
        """

        del wait_timeout, remark, extra
        self.orders.append(
            {
                "side": "buy",
                "security": security,
                "amount": amount,
                "price": price,
                "market": market,
            }
        )
        return "buy-test-order"

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """记录一笔测试卖出意图。

        Args:
            security: 标的代码。
            amount: 委托数量。
            price: 委托限价或保护价。
            wait_timeout: 可选等待时间。
            remark: 可选备注。
            market: 是否为市价意图。
            extra: 可选扩展字段。

        Returns:
            稳定测试订单标识。

        Side Effects:
            把脱敏参数追加到内存 ``orders`` 列表。
        """

        del wait_timeout, remark, extra
        self.orders.append(
            {
                "side": "sell",
                "security": security,
                "amount": amount,
                "price": price,
                "market": market,
            }
        )
        return "sell-test-order"

    async def cancel_order(self, order_id: str) -> bool:
        """接受测试撤单。

        Args:
            order_id: 测试订单标识。

        Returns:
            始终为 True。
        """

        del order_id
        return True

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """返回最小测试订单状态。

        Args:
            order_id: 测试订单标识。

        Returns:
            含订单标识的字典。
        """

        return {"order_id": order_id}


def _build_ready_router(capability_ids: Sequence[str]) -> DataSourceRouter:
    """构造所有指定能力均 ready 的本地测试 Router。

    Args:
        capability_ids: 需注册和设置唯一路由的 capability ID。

    Returns:
        DataSourceRouter: 不含网络或 SDK 副作用的 Router。
    """

    declarations = {
        capability_id: CapabilityDeclaration(
            capability_id=capability_id,
            semantic_class=capability_id,
            support=CapabilitySupport.SUPPORTED,
            readiness=CapabilityReadiness.READY,
        )
        for capability_id in capability_ids
    }
    router = DataSourceRouter()
    router.register_provider(
        CapabilityManifest(
            provider="ready-owner",
            manifest_version="test-v1",
            location=ProviderLocation.LOCAL,
            capabilities=declarations,
        ),
        object(),
    )
    for capability_id in capability_ids:
        router.set_route(
            RouteRule(
                capability_id=capability_id,
                primary="ready-owner",
                rule_id=f"route-{capability_id}",
            )
        )
    return router


def _live_config(tmp_path: Path) -> Dict[str, Any]:
    """构造不启动周期任务的离线 LiveEngine 配置。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        Dict[str, Any]: 将所有运行态写入临时目录的配置。
    """

    return {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }


def _prepare_async_engine(engine: LiveEngine) -> None:
    """为直接调用 ``_bootstrap`` 的测试补齐异步运行组件。

    Args:
        engine: 待启动的 LiveEngine。

    Returns:
        None。

    Side Effects:
        绑定当前 pytest 事件循环、EventBus 和 AsyncScheduler。
    """

    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine._order_lock = asyncio.Lock()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()


@pytest.mark.asyncio
async def test_static_requirements_fail_before_initialize_and_broker_connect(
    tmp_path: Path,
) -> None:
    """验证静态必需能力无路由时 initialize 和 Broker.connect 均不可达。

    Args:
        tmp_path: pytest 临时目录。
    """

    marker = tmp_path / "initialize-called"
    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "STRATEGY_CAPABILITY_REQUIREMENTS = {\n"
        "    'schema_version': '1',\n"
        "    'profile': 'execution_only',\n"
        "    'required': ['test.static'],\n"
        "}\n"
        "def initialize(context):\n"
        f"    open({str(marker)!r}, 'w', encoding='utf-8').write('called')\n",
        encoding="utf-8",
    )
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
        data_source_router=DataSourceRouter(),
    )

    with pytest.raises(DataCapabilityUnavailableError) as exc_info:
        await engine._bootstrap()

    assert exc_info.value.capability_id == "test.static"
    assert marker.exists() is False
    assert broker.preflight_calls == 1
    assert broker.connect_calls == 0


@pytest.mark.asyncio
async def test_initialize_addition_is_preflighted_before_process_initialize_and_connect(
    tmp_path: Path,
) -> None:
    """验证 initialize 新增的缺失能力在 process_initialize 和连接前阻断启动。

    Args:
        tmp_path: pytest 临时目录。
    """

    marker = tmp_path / "process-initialize-called"
    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "STRATEGY_CAPABILITY_REQUIREMENTS = {\n"
        "    'schema_version': '1',\n"
        "    'profile': 'execution_only',\n"
        "    'required': [],\n"
        "}\n"
        "def initialize(context):\n"
        "    require_data_capabilities(required=['realtime.snapshot.l2'])\n"
        "def process_initialize(context):\n"
        f"    open({str(marker)!r}, 'w', encoding='utf-8').write('called')\n",
        encoding="utf-8",
    )
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
        data_source_router=DataSourceRouter(),
    )
    _prepare_async_engine(engine)

    try:
        with pytest.raises(DataCapabilityUnavailableError) as exc_info:
            await engine._bootstrap()
    finally:
        await engine._shutdown()
        set_current_engine(None)

    assert exc_info.value.capability_id == "realtime.snapshot.l2"
    assert marker.exists() is False
    assert broker.connect_calls == 0


@pytest.mark.asyncio
async def test_initialize_cannot_queue_order_or_reach_broker(tmp_path: Path) -> None:
    """验证 initialize 中的明确限价订单也会在入队前被写门禁拒绝。

    Args:
        tmp_path: pytest 临时目录。
    """

    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "def initialize(context):\n" "    order('000001.XSHE', 100, price=10.0)\n",
        encoding="utf-8",
    )
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
    )
    _prepare_async_engine(engine)
    clear_order_queue()

    try:
        with pytest.raises(RuntimeError, match="startup_phase=initialize"):
            await engine._bootstrap()
    finally:
        await engine._shutdown()
        clear_order_queue()
        set_current_engine(None)

    assert get_order_queue() == []
    assert broker.connect_calls == 0
    assert broker.orders == []


@pytest.mark.asyncio
async def test_two_stage_success_freezes_routes_before_process_initialize(
    tmp_path: Path,
) -> None:
    """验证静态和 initialize 新增能力都 ready 时才连接并进入 process_initialize。

    Args:
        tmp_path: pytest 临时目录。
    """

    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "STRATEGY_CAPABILITY_REQUIREMENTS = {\n"
        "    'schema_version': '1',\n"
        "    'profile': 'execution_only',\n"
        "    'required': ['test.static'],\n"
        "}\n"
        "def initialize(context):\n"
        "    context.require_data_capabilities(required=['test.dynamic'])\n"
        "def process_initialize(context):\n"
        "    context.capability_process_ready = True\n",
        encoding="utf-8",
    )
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
        data_source_router=_build_ready_router(("test.static", "test.dynamic")),
    )
    _prepare_async_engine(engine)

    try:
        await engine._bootstrap()
        assert engine.strategy_capability_preflight is not None
        assert set(engine.strategy_capability_preflight.required_decisions) == {
            "test.static",
            "test.dynamic",
        }
        assert engine.context.capability_process_ready is True
        assert engine._startup_phase == "ready"
        assert engine._market_callbacks_enabled is True
        assert broker.connect_calls == 1
    finally:
        await engine._shutdown()
        set_current_engine(None)


@pytest.mark.asyncio
async def test_limit_share_order_skips_snapshot_but_value_order_requires_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证明确股数限价不读行情，价值单无实时价 owner 则入队前失败。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: pytest 属性替换工具。
    """

    strategy = tmp_path / "unused.py"
    strategy.write_text("def initialize(context):\n    pass\n", encoding="utf-8")
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
        data_source_router=DataSourceRouter(),
    )
    engine.broker = broker
    engine._startup_phase = "ready"
    engine._order_lock = asyncio.Lock()
    set_current_engine(engine)
    clear_order_queue()
    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data",
        lambda: (_ for _ in ()).throw(AssertionError("限价股数单不应读 current_data")),
    )
    monkeypatch.setattr("bullet_trade.core.orders._trigger_order_processing", lambda _timeout: None)

    try:
        limit_order = order("000001.XSHE", 100, price=10.0, wait_timeout=0)
        assert limit_order is not None
        await engine._process_orders(engine.context.current_dt)
        assert broker.orders == [
            {
                "side": "buy",
                "security": "000001.XSHE",
                "amount": 100,
                "price": 10.0,
                "market": False,
            }
        ]

        with pytest.raises(DataCapabilityUnavailableError) as exc_info:
            order_value("000001.XSHE", 1_000.0, price=10.0, wait_timeout=0)
        assert exc_info.value.capability_id == "realtime.snapshot.l1"
        assert get_order_queue() == []
    finally:
        clear_order_queue()
        set_current_engine(None)


@pytest.mark.asyncio
async def test_mixed_queue_keeps_limit_order_when_snapshot_fetch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证混合队列行情读取失败时只拒绝需快照订单，不丢失股数限价单。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: pytest 属性替换工具。
    """

    strategy = tmp_path / "unused.py"
    strategy.write_text("def initialize(context):\n    pass\n", encoding="utf-8")
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
    )
    engine.broker = broker
    engine._startup_phase = "ready"
    engine._order_lock = asyncio.Lock()
    set_current_engine(engine)
    monkeypatch.setattr("bullet_trade.core.orders._trigger_order_processing", lambda _timeout: None)
    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data",
        lambda: (_ for _ in ()).throw(RuntimeError("snapshot unavailable")),
    )

    limit_order = order("000001.XSHE", 100, price=10.0, wait_timeout=0)
    market_order = order("000002.XSHE", 100, wait_timeout=0)
    assert limit_order is not None
    assert market_order is not None

    await engine._process_orders(engine.context.current_dt)

    assert broker.orders == [
        {
            "side": "buy",
            "security": "000001.XSHE",
            "amount": 100,
            "price": 10.0,
            "market": False,
        }
    ]
    assert str(market_order.status.value) == "rejected"


def test_dynamic_requirements_survive_metadata_restore(tmp_path: Path) -> None:
    """验证 initialize 合并后的能力集在同一策略运行态恢复时不会丢失。

    Args:
        tmp_path: pytest 临时目录。
    """

    strategy = tmp_path / "unused.py"
    strategy.write_text("def initialize(context):\n    pass\n", encoding="utf-8")
    original = LiveEngine(
        strategy_file=strategy,
        live_config=_live_config(tmp_path),
        strategy_capability_requirements=StrategyCapabilityRequirements(
            profile=StrategyCapabilityProfile.EXECUTION_ONLY,
            required=("test.dynamic",),
            optional=("test.optional",),
        ),
    )
    payload = original._serialize_strategy_capability_requirements()
    assert payload is not None

    restored = LiveEngine(
        strategy_file=strategy,
        live_config=_live_config(tmp_path),
        strategy_capability_requirements=StrategyCapabilityRequirements(
            profile=StrategyCapabilityProfile.EXECUTION_ONLY,
            required=("test.static",),
        ),
    )
    applied = restored._restore_strategy_metadata(
        {
            "version": 1,
            "strategy_capability_requirements": payload,
            "settings": {},
            "tasks": [],
        }
    )

    assert applied is True
    assert restored.strategy_capability_requirements is not None
    assert set(restored.strategy_capability_requirements.required) == {
        "test.static",
        "test.dynamic",
    }
    assert restored.strategy_capability_requirements.optional == ("test.optional",)


@pytest.mark.asyncio
async def test_legacy_metadata_reruns_initialize_when_dynamic_capability_is_declared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证首次升级时旧元数据不会让动态能力声明被跳过。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: pytest 属性替换工具。
    """

    strategy = tmp_path / "strategy.py"
    strategy.write_text(
        "def initialize(context):\n"
        "    context.require_data_capabilities(required=['test.dynamic'])\n"
        "    context.legacy_initialize_reran = True\n",
        encoding="utf-8",
    )
    broker = _ReadyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=_live_config(tmp_path),
        data_source_router=_build_ready_router(("test.dynamic",)),
    )
    _prepare_async_engine(engine)
    monkeypatch.setattr("bullet_trade.core.live_engine.runtime_restored", lambda: True)
    monkeypatch.setattr(
        "bullet_trade.core.live_engine.load_strategy_metadata",
        lambda: {"version": 1, "settings": {}, "tasks": []},
    )

    try:
        await engine._bootstrap()
        assert engine.context.legacy_initialize_reran is True
        assert engine.strategy_capability_preflight is not None
        assert set(engine.strategy_capability_preflight.required_decisions) == {"test.dynamic"}
        assert broker.connect_calls == 1
    finally:
        await engine._shutdown()
        set_current_engine(None)


def test_jqdata_star_contract_exports_capability_declaration_api() -> None:
    """验证聚宽兼容层显式导出 LiveEngine 能力声明 API。"""

    from bullet_trade.compat import jqdata

    assert "require_data_capabilities" in jqdata.__all__
    assert callable(jqdata.require_data_capabilities)
