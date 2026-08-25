"""作者: BruceLee
文件职责: 验证 LiveEngine 会把运行态持久化故障升级为进程级失败。
主要输入: 受控健康锁存异常和最终保存异常。
主要输出: 主循环抛错与同步入口非零退出码断言。
上下游关系: 连接 ``live_runtime`` 健康锁存和 ``LiveEngine`` 进程语义。
关键环境或配置约定: 不初始化券商、数据源或网络连接。
"""

from __future__ import annotations

import asyncio
import json
import pickle
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bullet_trade.core import live_engine
from bullet_trade.core.globals import g
from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.live_runtime import (
    LiveRuntimePersistenceError,
    init_live_runtime,
    load_scheduler_cursor,
)
from bullet_trade.core.orders import cancel_order, clear_order_queue, order
from bullet_trade.core.runtime import process_orders_now, set_current_engine


def _scheduled_noop(_context=None) -> None:
    """作为周/月调度元数据 1:1 恢复测试的可解析 callable。

    Args:
        _context: 兼容策略调度回调上下文，本测试不使用。

    Returns:
        None。
    """


class _CheckpointBroker:
    """记录严格 checkpoint 测试中的真实 broker 提交次数。"""

    def __init__(self) -> None:
        """初始化空订单记录。

        Args:
            无。

        Returns:
            None。创建空列表。
        """

        self.orders: list[tuple[str, int, str]] = []
        self.cancel_calls: list[str] = []

    async def buy(
        self,
        security: str,
        amount: int,
        price=None,
        wait_timeout=None,
        remark=None,
        *,
        market: bool = False,
    ) -> str:
        """记录买单并返回测试订单号。

        Args:
            security: 证券代码。
            amount: 买入数量。
            price: 委托价格。
            wait_timeout: 等待超时。
            remark: 订单备注。
            market: 是否市价单。

        Returns:
            str: 固定格式测试订单号。
        """

        _ = price, wait_timeout, remark, market
        self.orders.append((security, amount, "buy"))
        return f"buy-{len(self.orders)}"

    async def sell(
        self,
        security: str,
        amount: int,
        price=None,
        wait_timeout=None,
        remark=None,
        *,
        market: bool = False,
    ) -> str:
        """记录卖单并返回测试订单号。

        Args:
            security: 证券代码。
            amount: 卖出数量。
            price: 委托价格。
            wait_timeout: 等待超时。
            remark: 订单备注。
            market: 是否市价单。

        Returns:
            str: 固定格式测试订单号。
        """

        _ = price, wait_timeout, remark, market
        self.orders.append((security, amount, "sell"))
        return f"sell-{len(self.orders)}"

    def supports_orders_sync(self) -> bool:
        """报告测试 broker 不支持订单轮询。

        Args:
            无。

        Returns:
            bool: 固定返回 False。
        """

        return False

    def supports_account_sync(self) -> bool:
        """报告测试 broker 不支持账户同步。

        Args:
            无。

        Returns:
            bool: 固定返回 False。
        """

        return False

    def cancel_order(self, order_id: str) -> bool:
        """记录不应绕过 checkpoint 的 broker 撤单。

        Args:
            order_id: broker 订单号。

        Returns:
            bool: 固定返回 True。
        """

        self.cancel_calls.append(order_id)
        return True


def _build_checkpoint_engine(tmp_path) -> tuple[LiveEngine, _CheckpointBroker]:
    """构造不连接网络的严格 checkpoint LiveEngine。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        tuple[LiveEngine, _CheckpointBroker]: 已初始化内存依赖的引擎和测试 broker。
    """

    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    engine = LiveEngine(
        strategy_file=strategy_path,
        live_config={
            "runtime_dir": str(tmp_path / "runtime"),
            "checkpoint_persistence_enabled": True,
            "g_autosave_enabled": True,
        },
    )
    broker = _CheckpointBroker()
    engine._loop = asyncio.get_running_loop()
    engine._stop_event = asyncio.Event()
    engine._order_lock = asyncio.Lock()
    engine.broker = broker  # type: ignore[assignment]
    engine._risk = None
    engine.context.portfolio.available_cash = 100_000.0
    engine.context.portfolio.total_value = 100_000.0
    engine._maybe_emit_market_events = AsyncMock()
    engine._maybe_handle_data = AsyncMock()
    init_live_runtime(engine.config.runtime_dir)
    return engine, broker


class _PriceSnapshot:
    """提供订单计划所需的固定行情字段。"""

    paused = False
    last_price = 10.0
    high_limit = 10.5
    low_limit = 9.5


@pytest.mark.asyncio
async def test_run_loop_rejects_latched_persistence_failure(monkeypatch) -> None:
    """验证主循环在处理下一分钟之前检查并抛出后台保存故障。

    Args:
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过异常操作名称断言表达成功条件。
    """

    engine = object.__new__(LiveEngine)
    engine._loop = asyncio.get_running_loop()
    engine._stop_event = asyncio.Event()

    def _raise_latched_failure() -> None:
        """模拟自动保存线程已锁存持久化故障。

        Args:
            无。

        Returns:
            None。本辅助函数始终抛出运行态异常。

        Raises:
            LiveRuntimePersistenceError: 每次调用均抛出。
        """

        cause = PermissionError("disk unavailable")
        raise LiveRuntimePersistenceError("health_check_after_save_g", "g.pkl", cause)

    monkeypatch.setattr(live_engine, "assert_live_runtime_healthy", _raise_latched_failure)

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        await engine._run_loop()

    assert caught.value.operation == "health_check_after_save_g"


def test_run_returns_failure_when_final_state_save_fails(tmp_path, monkeypatch) -> None:
    """验证引擎业务循环正常结束但最终保存失败时仍返回非零码。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过退出码断言表达成功条件。
    """

    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    engine = object.__new__(LiveEngine)
    engine.strategy_path = strategy_path
    engine._runtime_ready_for_final_save = True

    async def _finish_without_business_error() -> None:
        """模拟业务循环正常结束。

        Args:
            无。

        Returns:
            None。协程立即正常结束。
        """

        return None

    def _raise_final_save_failure() -> None:
        """模拟退出阶段 ``g.pkl`` 保存失败。

        Args:
            无。

        Returns:
            None。本辅助函数始终抛出运行态异常。

        Raises:
            LiveRuntimePersistenceError: 每次调用均抛出。
        """

        cause = PermissionError("disk unavailable")
        raise LiveRuntimePersistenceError("save_g", "g.pkl", cause)

    engine.start = _finish_without_business_error
    monkeypatch.setattr(live_engine, "save_g", _raise_final_save_failure)

    assert engine.run() == 2


def test_run_preserves_corrupt_g_after_bootstrap_failure(tmp_path) -> None:
    """验证启动加载失败后的 finally 保存不会覆盖损坏状态原件。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过退出码和原始字节不变断言表达成功条件。
    """

    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    state_path = runtime / "g.pkl"
    original_bytes = b"not-a-pickle"
    state_path.write_bytes(original_bytes)
    engine = object.__new__(LiveEngine)
    engine.strategy_path = strategy_path

    async def _fail_while_loading_runtime() -> None:
        """模拟 bootstrap 调用严格运行态加载器。

        Args:
            无。

        Returns:
            None。本协程会由加载器异常提前结束。

        Raises:
            LiveRuntimePersistenceError: 损坏 ``g.pkl`` 被读取时抛出。
        """

        init_live_runtime(str(runtime))

    engine.start = _fail_while_loading_runtime

    assert engine.run() == 2
    assert state_path.read_bytes() == original_bytes


def test_checkpoint_config_uses_strategy_runner_key_and_disables_autosave(tmp_path) -> None:
    """验证 strategies 实际传入的键启用严格持久化并关闭 autosave。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过配置和订单延迟标志断言表达成功条件。
    """

    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    engine = LiveEngine(
        strategy_file=strategy_path,
        live_config={
            "checkpoint_persistence_enabled": True,
            "g_autosave_enabled": True,
        },
    )

    assert engine.config.checkpoint_persistence_enabled is True
    assert engine.config.g_autosave_enabled is False
    assert engine.defer_order_processing is True


def _checkpoint_metadata() -> dict:
    """返回结构合法且不含调度任务的 v1 元数据。

    Args:
        无。

    Returns:
        dict: 可用于启动配对测试的最小元数据。
    """

    return {
        "version": 1,
        "strategy_hash": "test-hash",
        "settings": {},
        "tasks": [],
    }


def _build_pair_validation_engine(tmp_path, runtime_name: str = "pair-runtime") -> LiveEngine:
    """构造尚未读取运行态的严格 checkpoint 引擎。

    Args:
        tmp_path: pytest 提供的临时目录。
        runtime_name: 独立运行目录名称。

    Returns:
        LiveEngine: 未执行 bootstrap 或状态初始化的引擎。
    """

    strategy_path = tmp_path / f"{runtime_name}-strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    return LiveEngine(
        strategy_file=strategy_path,
        live_config={
            "runtime_dir": str(tmp_path / runtime_name),
            "checkpoint_persistence_enabled": True,
        },
    )


def _runtime_file_evidence(runtime) -> dict[str, tuple[bytes, int]]:
    """记录运行目录现有状态文件的字节和纳秒 mtime。

    Args:
        runtime: pytest Path 风格的运行目录。

    Returns:
        dict[str, tuple[bytes, int]]: 文件名到原始字节和 mtime 的映射。
    """

    evidence: dict[str, tuple[bytes, int]] = {}
    for filename in ("g.pkl", "live_state.json"):
        path = runtime / filename
        if path.exists():
            evidence[filename] = (path.read_bytes(), path.stat().st_mtime_ns)
    return evidence


def _assert_runtime_file_evidence_unchanged(runtime, evidence) -> None:
    """断言启动配对失败没有改写任何原状态文件。

    Args:
        runtime: pytest Path 风格的运行目录。
        evidence: ``_runtime_file_evidence`` 返回的基线。

    Returns:
        None。通过逐文件字节和 mtime 断言表达成功条件。
    """

    for filename, (raw_bytes, mtime_ns) in evidence.items():
        path = runtime / filename
        assert path.read_bytes() == raw_bytes
        assert path.stat().st_mtime_ns == mtime_ns


@pytest.mark.parametrize(
    ("with_g", "state", "error_pattern"),
    [
        (
            False,
            {
                "strategy": _checkpoint_metadata(),
                "scheduler": {"last_cursor": "2026-08-25T14:50:00"},
            },
            "缺少 g.pkl",
        ),
        (
            False,
            {
                "strategy": _checkpoint_metadata(),
                "subscriptions": {"symbols": ["159915.XSHE"], "markets": []},
            },
            "tick 订阅",
        ),
        (True, {}, "缺少策略元数据"),
        (
            True,
            {"strategy": {"version": 2, "settings": {}, "tasks": []}},
            "元数据无效",
        ),
        (
            True,
            {"scheduler": {"last_cursor": "2026-08-25T14:50:00"}},
            "孤立调度游标",
        ),
    ],
)
def test_checkpoint_runtime_pair_mismatch_fails_without_touching_files(
    tmp_path,
    with_g: bool,
    state: dict,
    error_pattern: str,
) -> None:
    """验证严格 checkpoint 的 g/state 配对反例均只读失败。

    Args:
        tmp_path: pytest 提供的临时目录。
        with_g: 是否创建已有 ``g.pkl``。
        state: 待写入的原始 ``live_state.json``。
        error_pattern: 预期错误消息片段。

    Returns:
        None。通过异常、字节和 mtime 不变断言表达成功条件。
    """

    engine = _build_pair_validation_engine(tmp_path)
    runtime = tmp_path / "pair-runtime"
    runtime.mkdir()
    if with_g:
        with (runtime / "g.pkl").open("wb") as state_file:
            pickle.dump({"trusted": True}, state_file)
    if state:
        (runtime / "live_state.json").write_text(
            json.dumps(state, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
    evidence = _runtime_file_evidence(runtime)

    init_live_runtime(str(runtime))
    with pytest.raises(RuntimeError, match=error_pattern):
        engine._load_and_validate_runtime_snapshot()

    _assert_runtime_file_evidence_unchanged(runtime, evidence)


@pytest.mark.parametrize("case", ["first_start", "metadata_only", "g_and_metadata"])
def test_checkpoint_runtime_pair_allows_explicit_safe_start_states(tmp_path, case: str) -> None:
    """验证首次启动、metadata-only 和完整未调度状态的正例。

    Args:
        tmp_path: pytest 提供的临时目录。
        case: 参数化启动状态名称。

    Returns:
        None。加载并通过配对验证即为成功。
    """

    engine = _build_pair_validation_engine(tmp_path, runtime_name=f"pair-{case}")
    runtime = tmp_path / f"pair-{case}"
    runtime.mkdir()
    if case in {"metadata_only", "g_and_metadata"}:
        (runtime / "live_state.json").write_text(
            json.dumps({"strategy": _checkpoint_metadata()}, sort_keys=True),
            encoding="utf-8",
        )
    if case == "g_and_metadata":
        with (runtime / "g.pkl").open("wb") as state_file:
            pickle.dump({"trusted": True}, state_file)

    init_live_runtime(str(runtime))
    restored, metadata, symbols, markets, cursor = engine._load_and_validate_runtime_snapshot()

    assert restored is (case == "g_and_metadata")
    assert bool(metadata) is (case != "first_start")
    assert symbols == set()
    assert markets == set()
    assert cursor is None


def test_run_pair_mismatch_does_not_final_save_valid_g(tmp_path) -> None:
    """验证 bootstrap 配对失败后同步入口不会在 finally 重写原 g。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过非零退出码及 g 字节、mtime 不变断言表达成功条件。
    """

    engine = _build_pair_validation_engine(tmp_path, runtime_name="run-pair-mismatch")
    runtime = tmp_path / "run-pair-mismatch"
    runtime.mkdir()
    with (runtime / "g.pkl").open("wb") as state_file:
        pickle.dump({"trusted": True}, state_file)
    evidence = _runtime_file_evidence(runtime)

    async def _fail_pair_validation() -> None:
        """模拟同步入口中的早期运行态配对验证。

        Args:
            无。

        Returns:
            None。本协程在配对校验处抛出异常。
        """

        init_live_runtime(str(runtime))
        engine._load_and_validate_runtime_snapshot()

    engine.start = _fail_pair_validation

    assert engine.run() == 2
    _assert_runtime_file_evidence_unchanged(runtime, evidence)


@pytest.mark.asyncio
async def test_bootstrap_validates_runtime_pair_before_initialize_and_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    """验证 bootstrap 在 initialize 与 metadata 写入前完成配对校验。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过钩子零调用、快照零调用及原文件不变断言表达成功条件。
    """

    engine = _build_pair_validation_engine(tmp_path, runtime_name="bootstrap-pair")
    runtime = tmp_path / "bootstrap-pair"
    runtime.mkdir()
    with (runtime / "g.pkl").open("wb") as state_file:
        pickle.dump({"trusted": True}, state_file)
    evidence = _runtime_file_evidence(runtime)
    call_hook = AsyncMock()
    snapshot = AsyncMock()
    monkeypatch.setattr(engine, "_ensure_broker_created", lambda: None)
    monkeypatch.setattr(engine, "_acquire_live_locks", lambda: None)
    monkeypatch.setattr(engine, "_call_hook", call_hook)
    monkeypatch.setattr(engine, "_snapshot_strategy_metadata", snapshot)

    with pytest.raises(RuntimeError, match="缺少策略元数据"):
        await engine._bootstrap()

    call_hook.assert_not_awaited()
    snapshot.assert_not_awaited()
    _assert_runtime_file_evidence_unchanged(runtime, evidence)


def test_generic_metadata_snapshot_write_failure_is_process_visible(
    tmp_path,
    monkeypatch,
) -> None:
    """验证默认 LiveEngine 的元数据写失败也不会继续运行。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过 RuntimeError 断言表达全模式严格写入语义。
    """

    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def initialize(context):\n    return None\n", encoding="utf-8")
    engine = LiveEngine(strategy_file=strategy_path)
    monkeypatch.setattr(engine, "_collect_settings_snapshot", lambda: {})
    monkeypatch.setattr(engine, "_collect_scheduler_tasks_snapshot", lambda: [])

    def _fail_metadata_write(_metadata) -> None:
        """模拟 live_state 元数据原子写失败。

        Args:
            _metadata: 待持久化的元数据。

        Returns:
            None。本辅助函数始终抛出异常。

        Raises:
            LiveRuntimePersistenceError: 每次调用均抛出。
        """

        raise LiveRuntimePersistenceError(
            "write_live_state",
            "live_state.json",
            PermissionError("blocked"),
        )

    monkeypatch.setattr(live_engine, "persist_strategy_metadata", _fail_metadata_write)

    with pytest.raises(RuntimeError, match="策略元数据快照失败"):
        engine._snapshot_strategy_metadata("hash")


@pytest.mark.asyncio
async def test_checkpoint_defers_callback_order_until_g_save_succeeds(
    tmp_path,
    monkeypatch,
) -> None:
    """验证回调创建的订单只在 ``g`` 成功落盘后提交 broker。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过时序、broker 次数和磁盘游标断言表达成功条件。
    """

    engine, broker = _build_checkpoint_engine(tmp_path)
    scheduled = datetime(2026, 8, 25, 14, 50)
    timeline: list[str] = []

    async def _trigger(*_args, **_kwargs):
        """模拟调度回调修改 ``g`` 并创建订单。

        Args:
            *_args: 兼容调度器位置参数。
            **_kwargs: 兼容调度器关键字参数。

        Returns:
            dict: 无错误的调度结果。
        """

        g.decision = "gold"
        order("518880.XSHG", 100)
        assert broker.orders == []
        timeline.append("callback")
        return {}

    original_save = live_engine.save_g

    def _save_checkpoint() -> None:
        """记录并执行真实 ``g`` 保存。

        Args:
            无。

        Returns:
            None。保存成功后返回。
        """

        assert broker.orders == []
        timeline.append("save_g")
        original_save()

    engine.async_scheduler = SimpleNamespace(trigger=_trigger)
    monkeypatch.setattr(live_engine, "save_g", _save_checkpoint)
    monkeypatch.setattr(
        live_engine,
        "get_current_data",
        lambda: {"518880.XSHG": _PriceSnapshot()},
    )
    clear_order_queue()
    set_current_engine(engine)
    try:
        await engine._handle_minute_tick(scheduled)
    finally:
        set_current_engine(None)
        clear_order_queue()

    assert timeline == ["callback", "save_g"]
    assert broker.orders == [("518880.XSHG", 100, "buy")]
    assert load_scheduler_cursor() == scheduled
    assert engine._last_schedule_dt == scheduled


@pytest.mark.asyncio
async def test_checkpoint_save_failure_means_zero_broker_and_zero_cursor(
    tmp_path,
    monkeypatch,
) -> None:
    """验证 ``g`` 保存失败时回调订单不提交且游标不推进。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过异常、broker 和游标断言表达成功条件。
    """

    engine, broker = _build_checkpoint_engine(tmp_path)
    scheduled = datetime(2026, 8, 25, 14, 50)

    async def _trigger(*_args, **_kwargs):
        """模拟产生订单的调度回调。

        Args:
            *_args: 兼容调度器位置参数。
            **_kwargs: 兼容调度器关键字参数。

        Returns:
            dict: 无错误的调度结果。
        """

        g.decision = "gold"
        order("518880.XSHG", 100)
        return {}

    def _fail_save() -> None:
        """模拟 checkpoint 磁盘保存失败。

        Args:
            无。

        Returns:
            None。本辅助函数始终抛出异常。

        Raises:
            LiveRuntimePersistenceError: 每次调用均抛出。
        """

        raise LiveRuntimePersistenceError("save_g", "g.pkl", PermissionError("blocked"))

    engine.async_scheduler = SimpleNamespace(trigger=_trigger)
    monkeypatch.setattr(live_engine, "save_g", _fail_save)
    clear_order_queue()
    set_current_engine(engine)
    try:
        with pytest.raises(LiveRuntimePersistenceError):
            await engine._handle_minute_tick(scheduled)
    finally:
        set_current_engine(None)
        clear_order_queue()

    assert broker.orders == []
    assert load_scheduler_cursor() is None
    assert engine._last_schedule_dt is None


@pytest.mark.asyncio
async def test_checkpoint_callback_cannot_force_process_orders_now(
    tmp_path,
) -> None:
    """验证策略回调不能用即时撮合入口绕过 ``g`` checkpoint。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。回调失败关闭且 broker、磁盘游标均保持未触发即为成功。
    """

    engine, broker = _build_checkpoint_engine(tmp_path)
    engine.config.fail_on_schedule_error = True
    scheduled = datetime(2026, 8, 25, 14, 50)

    async def _trigger(*_args, **_kwargs):
        """模拟先排队订单再强制即时处理的策略回调。

        Args:
            *_args: 兼容调度器位置参数。
            **_kwargs: 兼容调度器关键字参数。

        Returns:
            None。本协程在即时处理入口失败关闭。

        Raises:
            RuntimeError: ``process_orders_now`` 在 checkpoint 模式固定抛出。
        """

        order("518880.XSHG", 100)
        process_orders_now()

    engine.async_scheduler = SimpleNamespace(trigger=_trigger)
    clear_order_queue()
    set_current_engine(engine)
    try:
        with pytest.raises(RuntimeError, match="异步调度器触发失败"):
            await engine._handle_minute_tick(scheduled)
    finally:
        set_current_engine(None)
        clear_order_queue()

    assert broker.orders == []
    assert load_scheduler_cursor() is None
    assert engine._last_schedule_dt is None


@pytest.mark.asyncio
async def test_checkpoint_cancel_allows_local_queue_but_blocks_broker_cancel(tmp_path) -> None:
    """验证 checkpoint 撤单仅允许处理尚未提交的本地队列。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过本地撤单成功、broker 撤单失败关闭及零调用断言表达成功条件。
    """

    engine, broker = _build_checkpoint_engine(tmp_path)
    clear_order_queue()
    set_current_engine(engine)
    try:
        queued_order = order("518880.XSHG", 100)
        assert queued_order is not None
        assert cancel_order(queued_order) is True
        assert broker.cancel_calls == []

        submitted_order = order("518880.XSHG", 100)
        assert submitted_order is not None
        setattr(submitted_order, "_broker_order_id", "counter-123")
        with pytest.raises(RuntimeError, match="拒绝直接撤销"):
            cancel_order(submitted_order)
        assert broker.cancel_calls == []
    finally:
        set_current_engine(None)
        clear_order_queue()


@pytest.mark.asyncio
async def test_latched_failure_after_scheduler_stops_before_save_order_and_cursor(
    tmp_path,
    monkeypatch,
) -> None:
    """验证调度期间出现的后台故障会在 checkpoint 第一关被截住。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过零保存、零 broker、零游标断言表达成功条件。
    """

    engine, broker = _build_checkpoint_engine(tmp_path)
    scheduled = datetime(2026, 8, 25, 14, 50)
    scheduler_finished = False
    save_calls = 0

    async def _trigger(*_args, **_kwargs):
        """模拟调度期间后台保存已经失败。

        Args:
            *_args: 兼容调度器位置参数。
            **_kwargs: 兼容调度器关键字参数。

        Returns:
            dict: 无错误的业务调度结果。
        """

        nonlocal scheduler_finished
        order("518880.XSHG", 100)
        scheduler_finished = True
        return {}

    def _health_check() -> None:
        """在调度完成后模拟健康锁存失败。

        Args:
            无。

        Returns:
            None。调度前健康，调度后抛出异常。

        Raises:
            LiveRuntimePersistenceError: 调度完成后抛出。
        """

        if scheduler_finished:
            raise LiveRuntimePersistenceError(
                "health_check_after_save_g",
                "g.pkl",
                PermissionError("background failure"),
            )

    def _save_spy() -> None:
        """记录不应发生的 checkpoint 保存。

        Args:
            无。

        Returns:
            None。仅增加计数。
        """

        nonlocal save_calls
        save_calls += 1

    engine.async_scheduler = SimpleNamespace(trigger=_trigger)
    monkeypatch.setattr(live_engine, "assert_live_runtime_healthy", _health_check)
    monkeypatch.setattr(live_engine, "save_g", _save_spy)
    clear_order_queue()
    set_current_engine(engine)
    try:
        with pytest.raises(LiveRuntimePersistenceError):
            await engine._handle_minute_tick(scheduled)
    finally:
        set_current_engine(None)
        clear_order_queue()

    assert save_calls == 0
    assert broker.orders == []
    assert load_scheduler_cursor() is None
    assert engine._last_schedule_dt is None


@pytest.mark.asyncio
async def test_cursor_failure_keeps_memory_cursor_old_after_g_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    """验证游标写失败时磁盘已有 ``g``，但内存游标仍保持旧值以便重放。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过 g 文件内容和内存游标断言表达崩溃矩阵语义。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    scheduled = datetime(2026, 8, 25, 14, 50)

    async def _trigger(*_args, **_kwargs):
        """模拟只修改策略状态、不产生订单的调度回调。

        Args:
            *_args: 兼容调度器位置参数。
            **_kwargs: 兼容调度器关键字参数。

        Returns:
            dict: 无错误的调度结果。
        """

        g.decision = "gold"
        return {}

    def _fail_cursor(_value: datetime) -> None:
        """模拟游标原子写失败。

        Args:
            _value: 待写入的调度分钟。

        Returns:
            None。本辅助函数始终抛出异常。

        Raises:
            LiveRuntimePersistenceError: 每次调用均抛出。
        """

        raise LiveRuntimePersistenceError(
            "write_live_state",
            "live_state.json",
            PermissionError("blocked"),
        )

    engine.async_scheduler = SimpleNamespace(trigger=_trigger)
    monkeypatch.setattr(live_engine, "persist_scheduler_cursor", _fail_cursor)

    with pytest.raises(LiveRuntimePersistenceError):
        await engine._handle_minute_tick(scheduled)

    with (tmp_path / "runtime" / "g.pkl").open("rb") as state_file:
        persisted_g = pickle.load(state_file)
    assert persisted_g["decision"] == "gold"
    assert engine._last_schedule_dt is None


@pytest.mark.asyncio
async def test_timeout_checkpoint_saves_g_before_advancing_cursor(tmp_path, monkeypatch) -> None:
    """验证超时丢弃分钟仍遵守 ``save_g`` 成功后再推进游标。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过调用顺序和最终游标断言表达成功条件。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    engine.config.event_time_out = 5
    scheduled = datetime(2026, 8, 25, 14, 50)
    timeline: list[str] = []
    original_save = live_engine.save_g
    original_cursor = live_engine.persist_scheduler_cursor

    def _save() -> None:
        """记录并执行真实 g 保存。

        Args:
            无。

        Returns:
            None。真实保存成功后返回。
        """

        timeline.append("save_g")
        original_save()

    def _cursor(value: datetime) -> None:
        """记录并执行真实游标保存。

        Args:
            value: 待持久化的调度分钟。

        Returns:
            None。真实保存成功后返回。
        """

        timeline.append("cursor")
        original_cursor(value)

    monkeypatch.setattr(live_engine, "save_g", _save)
    monkeypatch.setattr(live_engine, "persist_scheduler_cursor", _cursor)

    await engine._handle_minute_tick(scheduled.replace(second=10))

    assert timeline == ["save_g", "cursor"]
    assert engine._last_schedule_dt == scheduled
    assert load_scheduler_cursor() == scheduled


@pytest.mark.asyncio
async def test_checkpoint_rejects_handle_tick_and_restored_subscriptions(tmp_path) -> None:
    """验证严格分钟 checkpoint 不接受异步 tick 状态来源。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过两个独立拒绝断言表达成功条件。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    engine.handle_tick_func = lambda *_args: None
    with pytest.raises(RuntimeError, match="handle_tick"):
        engine._validate_checkpoint_runtime_constraints(
            restored_runtime=False,
            metadata={},
            cursor=None,
            symbols=[],
            markets=[],
        )
    engine.handle_tick_func = None
    with pytest.raises(RuntimeError, match="tick 订阅"):
        engine._validate_checkpoint_runtime_constraints(
            restored_runtime=False,
            metadata={},
            cursor=None,
            symbols=["159915.XSHE"],
            markets=[],
        )


@pytest.mark.asyncio
async def test_checkpoint_metadata_restore_rejects_missing_task_callable(
    tmp_path,
    monkeypatch,
) -> None:
    """验证严格模式不会把缺失 callable 的历史任务静默丢弃。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过 RuntimeError 断言表达成功条件。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    monkeypatch.setattr(engine, "_resolve_callable", lambda *_args: None)
    tasks = [
        {
            "module": "missing_strategy_module",
            "func": "publish",
            "schedule_type": "daily",
            "time": "14:50",
            "weekday": None,
            "monthday": None,
            "enabled": True,
        }
    ]

    with pytest.raises(RuntimeError, match="无法解析调度任务"):
        engine._apply_scheduler_tasks_snapshot(tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "benchmark",
        "options",
        "order_cost",
        "order_cost_overrides",
        "slippage",
        "slippage_map",
    ],
)
async def test_checkpoint_settings_restore_rejects_any_unrestorable_payload(
    tmp_path,
    monkeypatch,
    case: str,
) -> None:
    """验证六类策略设置任一不可恢复 payload 都会失败关闭。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。
        case: 本轮注入故障的设置类别。

    Returns:
        None。严格恢复抛出 RuntimeError 即为成功。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    live_engine.reset_settings()
    snapshot = engine._collect_settings_snapshot()
    if case == "benchmark":
        snapshot["benchmark"] = "000300.XSHG"

        def _fail_benchmark(_security) -> None:
            """模拟 benchmark setter 失败。

            Args:
                _security: 待恢复的基准代码。

            Returns:
                None。本函数固定抛出。

            Raises:
                ValueError: 每次调用均抛出。
            """

            raise ValueError("unsupported benchmark")

        monkeypatch.setattr(live_engine, "set_benchmark", _fail_benchmark)
    elif case == "options":

        def _fail_option(_key, _value) -> None:
            """模拟 option setter 失败。

            Args:
                _key: 选项键。
                _value: 选项值。

            Returns:
                None。本函数固定抛出。

            Raises:
                ValueError: 每次调用均抛出。
            """

            raise ValueError("unsupported option")

        monkeypatch.setattr(live_engine, "set_option", _fail_option)
    elif case == "order_cost":
        snapshot["order_cost"]["stock"]["unknown_field"] = 1
    elif case == "order_cost_overrides":
        snapshot["order_cost_overrides"] = {
            "invalid-key": dict(snapshot["order_cost"]["stock"]),
        }
    elif case == "slippage":
        snapshot["slippage"] = {"class": "UnsupportedSlippage", "value": 1.0}
    else:
        snapshot["slippage_map"] = {
            "stock": {"class": "UnsupportedSlippage", "value": 1.0},
        }

    try:
        with pytest.raises(RuntimeError, match="严格 checkpoint"):
            engine._apply_settings_snapshot(snapshot)
    finally:
        live_engine.reset_settings()


@pytest.mark.asyncio
async def test_checkpoint_task_snapshot_rejects_unaddressable_callable(
    tmp_path,
    monkeypatch,
) -> None:
    """验证严格快照不会静默跳过缺少 module/name 的 callable。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。严格快照抛出 RuntimeError 即为成功。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    task = SimpleNamespace(
        func=object(),
        schedule_type=SimpleNamespace(value="daily"),
        time="14:50",
        weekday=None,
        monthday=None,
        reference_security=None,
        force=True,
        enabled=True,
    )
    monkeypatch.setattr(live_engine, "get_tasks", lambda: [task])

    with pytest.raises(RuntimeError, match="无 module/name"):
        engine._collect_scheduler_tasks_snapshot()


@pytest.mark.asyncio
async def test_checkpoint_metadata_restore_preserves_original_failure_chain(
    tmp_path,
    monkeypatch,
) -> None:
    """验证严格元数据恢复异常不会被 warning 加 False 吞掉。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。外层 RuntimeError 保留原始 ValueError 为 cause 即为成功。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)

    def _fail_settings(_snapshot) -> None:
        """模拟设置恢复底层故障。

        Args:
            _snapshot: 待恢复设置快照。

        Returns:
            None。本函数固定抛出。

        Raises:
            ValueError: 每次调用均抛出。
        """

        raise ValueError("settings restore failed")

    monkeypatch.setattr(engine, "_apply_settings_snapshot", _fail_settings)
    metadata = {"version": 1, "settings": {}, "tasks": []}

    with pytest.raises(RuntimeError, match="无法完整恢复策略元数据") as caught:
        engine._restore_strategy_metadata(metadata)

    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.asyncio
async def test_checkpoint_scheduler_round_trip_preserves_complete_task_identity(
    tmp_path,
) -> None:
    """验证周/月任务的参考标的、force、enabled 均可 1:1 恢复。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。恢复后重新收集的完整快照与输入相同即为成功。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    module = _scheduled_noop.__module__
    tasks = [
        {
            "module": module,
            "func": "_scheduled_noop",
            "schedule_type": "weekly",
            "time": "10:40",
            "weekday": 2,
            "monthday": None,
            "reference_security": "159915.XSHE",
            "force": False,
            "enabled": False,
        },
        {
            "module": module,
            "func": "_scheduled_noop",
            "schedule_type": "weekly",
            "time": "10:40",
            "weekday": 2,
            "monthday": None,
            "reference_security": "518880.XSHG",
            "force": False,
            "enabled": False,
        },
        {
            "module": module,
            "func": "_scheduled_noop",
            "schedule_type": "weekly",
            "time": "10:40",
            "weekday": 2,
            "monthday": None,
            "reference_security": "518880.XSHG",
            "force": True,
            "enabled": False,
        },
        {
            "module": module,
            "func": "_scheduled_noop",
            "schedule_type": "monthly",
            "time": "14:50",
            "weekday": None,
            "monthday": -1,
            "reference_security": "518880.XSHG",
            "force": True,
            "enabled": True,
        },
    ]

    try:
        engine._apply_scheduler_tasks_snapshot(tasks)
        assert engine._collect_scheduler_tasks_snapshot() == tasks
    finally:
        live_engine.unschedule_all()


@pytest.mark.asyncio
async def test_checkpoint_scheduler_restore_rejects_duplicate_complete_identity(
    tmp_path,
) -> None:
    """验证严格恢复不会把重复 metadata 任务静默折叠。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。检测到重复完整身份并抛出 RuntimeError 即为成功。
    """

    engine, _broker = _build_checkpoint_engine(tmp_path)
    task = {
        "module": _scheduled_noop.__module__,
        "func": "_scheduled_noop",
        "schedule_type": "weekly",
        "time": "10:40",
        "weekday": 2,
        "monthday": None,
        "reference_security": "159915.XSHE",
        "force": False,
        "enabled": True,
    }

    with pytest.raises(RuntimeError, match="重复调度任务"):
        engine._apply_scheduler_tasks_snapshot([task, dict(task)])
