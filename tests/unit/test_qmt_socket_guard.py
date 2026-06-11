"""
作者: BruceLee
日期: 2026-06-09
文件说明:
    QMT socket exhaustion guard 的单元测试。
    主要输入为 fake xtquant、fake QmtBroker、fake MiniQMTProvider 和 QmtAvailabilityGuard 配置。
    主要输出为 pytest 断言结果，覆盖连接失败边界、cooldown、后台恢复和 handshake/health 行为。
    上游由 focused pytest 调用；下游验证 broker/data/server guard 接入是否会误触真实 QMT。
    关键约定: 测试不得连接真实 Windows/QMT，也不得依赖 xtquant 安装环境。
"""

import asyncio
import sys
import threading
import types

import pytest

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.remote import RemoteQmtConnection
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.qmt import QmtBrokerAdapter, QmtDataAdapter
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig
from bullet_trade.server.qmt_guard import (
    QmtAvailabilityGuard,
    QmtGuardConfig,
    QmtGuardError,
    is_qmt_connectivity_error,
    load_qmt_guard_config,
)


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    """等待异步条件满足。

    Args:
        predicate: 返回 bool 的条件函数。
        timeout: 最大等待秒数。

    Returns:
        None。

    Raises:
        AssertionError: 条件在超时前仍未满足。
    """

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待条件超时")


def _run_loop(loop: asyncio.AbstractEventLoop, app: ServerApplication) -> None:
    """在测试线程中运行 server event loop。

    Args:
        loop: 测试用事件循环。
        app: 待启动的 ServerApplication。

    Returns:
        None。

    Side Effects:
        设置当前线程事件循环并启动 server。
    """

    asyncio.set_event_loop(loop)
    loop.create_task(app.start())
    loop.run_forever()


def _install_fake_xtquant(monkeypatch, trader_cls) -> None:
    """安装测试用 xtquant 假模块。

    Args:
        monkeypatch: pytest monkeypatch fixture。
        trader_cls: 替代 XtQuantTrader 的类。

    Returns:
        None。

    Side Effects:
        修改 sys.modules 中的 xtquant 相关模块。
    """

    xtquant = types.ModuleType("xtquant")
    xttrader = types.ModuleType("xtquant.xttrader")
    xttype = types.ModuleType("xtquant.xttype")

    class _CallbackBase:
        """测试用 QMT callback 基类。"""

    class _StockAccount:
        """测试用 QMT 账户对象。"""

        def __init__(self, account_id, account_type=None):
            """保存账户参数。

            Args:
                account_id: 账户 ID。
                account_type: 账户类型。

            Returns:
                None。
            """

            self.account_id = account_id
            self.account_type = account_type

    xttrader.XtQuantTrader = trader_cls
    xttrader.XtQuantTraderCallback = _CallbackBase
    xttype.StockAccount = _StockAccount
    monkeypatch.setitem(sys.modules, "xtquant", xtquant)
    monkeypatch.setitem(sys.modules, "xtquant.xttrader", xttrader)
    monkeypatch.setitem(sys.modules, "xtquant.xttype", xttype)


def _guard_config(
    *,
    initial_delay_seconds: float = 5.0,
    ready_poll_seconds: float = 0.01,
) -> QmtGuardConfig:
    """生成测试用 guard 配置。

    Args:
        initial_delay_seconds: 失败后的初始 cooldown 秒数。
        ready_poll_seconds: 后台探针轮询秒数。

    Returns:
        QmtGuardConfig: 测试用配置。
    """

    return QmtGuardConfig(
        initial_delay_seconds=initial_delay_seconds,
        max_delay_seconds=max(initial_delay_seconds, 0.01),
        backoff_multiplier=1.0,
        ready_poll_seconds=ready_poll_seconds,
        tcp_pressure_threshold=100,
    )


@pytest.mark.unit
def test_qmt_connectivity_error_classifier_is_not_too_broad():
    """验证 QMT 连接错误识别不会覆盖普通数据异常。

    Args:
        None。

    Returns:
        None。
    """

    assert is_qmt_connectivity_error(RuntimeError("xtdata connection refused")) is True
    assert is_qmt_connectivity_error(RuntimeError("QMT 本地数据覆盖不足")) is False


@pytest.mark.unit
def test_qmt_guard_default_max_delay_limits_long_outage_probe_rate(monkeypatch):
    """验证默认最大退避适合长期 QMT 故障保护。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。

    Side Effects:
        临时清理 guard 相关环境变量，避免外部环境影响默认值断言。
    """

    for name in (
        "QMT_GUARD_INITIAL_DELAY_SECONDS",
        "QMT_GUARD_MAX_DELAY_SECONDS",
        "QMT_GUARD_BACKOFF_MULTIPLIER",
        "QMT_GUARD_READY_POLL_SECONDS",
        "QMT_GUARD_TCP_PRESSURE_THRESHOLD",
    ):
        monkeypatch.delenv(name, raising=False)

    cfg = load_qmt_guard_config()

    assert cfg.initial_delay_seconds == 5.0
    assert cfg.max_delay_seconds == 300.0


@pytest.mark.unit
def test_qmt_broker_connect_failure_is_single_attempt_and_cleans_trader(monkeypatch):
    """验证 QMT broker 连接失败不会无限重试，并会清理本次 trader。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    instances = []

    class _FailingTrader:
        """connect 永远失败的测试 trader。"""

        def __init__(self, data_path, session_id):
            """保存初始化参数并记录实例。

            Args:
                data_path: QMT 数据目录。
                session_id: QMT session id。

            Returns:
                None。
            """

            self.data_path = data_path
            self.session_id = session_id
            self.connect_calls = 0
            self.unregister_calls = 0
            self.disconnect_calls = 0
            self.stop_calls = 0
            instances.append(self)

        def register_callback(self, callback):
            """记录 callback 注册。

            Args:
                callback: QMT callback。

            Returns:
                None。
            """

            self.callback = callback

        def unregister_callback(self, callback):
            """记录 callback 注销。

            Args:
                callback: QMT callback。

            Returns:
                None。
            """

            self.unregister_calls += 1

        def start(self):
            """模拟启动成功。

            Args:
                None。

            Returns:
                int: QMT 成功状态码。
            """

            return 0

        def connect(self):
            """模拟连接失败。

            Args:
                None。

            Returns:
                int: QMT 失败状态码。
            """

            self.connect_calls += 1
            return -1

        def disconnect(self):
            """记录断开调用。

            Args:
                None。

            Returns:
                None。
            """

            self.disconnect_calls += 1

        def stop(self):
            """记录停止调用。

            Args:
                None。

            Returns:
                None。
            """

            self.stop_calls += 1

    _install_fake_xtquant(monkeypatch, _FailingTrader)

    broker = QmtBroker(account_id="demo", data_path="C:/qmt")
    with pytest.raises(RuntimeError, match="connect"):
        broker.connect()

    assert len(instances) == 1
    trader = instances[0]
    assert trader.connect_calls == 1
    assert trader.unregister_calls == 1
    assert trader.disconnect_calls == 1
    assert trader.stop_calls == 1
    assert broker._xt_trader is None
    assert broker._xt_account is None
    assert broker._xt_callback is None
    assert broker.is_connected is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_broker_adapter_cooldown_fast_fails_without_extra_connect(monkeypatch):
    """验证 QMT down 后 broker 请求快速失败且不会放大连接尝试。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _FailingBroker:
        """后台探针连接失败的 fake broker。"""

        connect_calls = 0

        def __init__(self, **kwargs):
            """接收真实 broker 初始化参数。

            Args:
                **kwargs: QmtBroker 初始化参数。

            Returns:
                None。
            """

            self.connected = False

        @property
        def is_connected(self) -> bool:
            """返回连接状态。

            Args:
                None。

            Returns:
                bool: 当前是否连接。
            """

            return self.connected

        def connect(self) -> bool:
            """模拟连接失败。

            Args:
                None。

            Returns:
                bool: 永不返回成功。

            Raises:
                RuntimeError: 固定表示 QMT 不可用。
            """

            type(self).connect_calls += 1
            raise RuntimeError("QMT down")

        def disconnect(self) -> bool:
            """模拟断开。

            Args:
                None。

            Returns:
                bool: True。
            """

            self.connected = False
            return True

    async def _run_now(func, *args, **kwargs):
        """立即执行同步函数。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            object: func 的返回值。
        """

        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt.QmtBroker", _FailingBroker)
    monkeypatch.setattr("bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: object())
    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    guard = QmtAvailabilityGuard(config=_guard_config(initial_delay_seconds=5.0), name="test")
    adapter = QmtBrokerAdapter(config, router, guard=guard)
    ctx = router.get("default")

    try:
        await adapter.start()
        await _wait_until(lambda: guard.failure_count >= 1)
        assert _FailingBroker.connect_calls == 1

        async def _request_once() -> None:
            """发起一次应快速失败的 broker 请求。

            Args:
                None。

            Returns:
                None。
            """

            with pytest.raises(QmtGuardError) as exc_info:
                await adapter.list_orders(ctx, {})
            assert exc_info.value.code == "QMT_UNAVAILABLE"

        await asyncio.gather(*(_request_once() for _ in range(5)))
        assert _FailingBroker.connect_calls == 1
    finally:
        await adapter.stop()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_broker_adapter_recovers_after_cooldown(monkeypatch):
    """验证 cooldown 到期后后台探针能把 QMT 标记为 ready。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _RecoveringBroker:
        """第一次连接失败、第二次连接成功的 fake broker。"""

        connect_calls = 0

        def __init__(self, **kwargs):
            """接收真实 broker 初始化参数。

            Args:
                **kwargs: QmtBroker 初始化参数。

            Returns:
                None。
            """

            self.connected = False

        @property
        def is_connected(self) -> bool:
            """返回连接状态。

            Args:
                None。

            Returns:
                bool: 当前是否连接。
            """

            return self.connected

        def connect(self) -> bool:
            """模拟先失败后成功的连接。

            Args:
                None。

            Returns:
                bool: 第二次起返回 True。

            Raises:
                RuntimeError: 第一次表示 QMT 暂不可用。
            """

            type(self).connect_calls += 1
            if type(self).connect_calls == 1:
                raise RuntimeError("QMT down")
            self.connected = True
            return True

        def disconnect(self) -> bool:
            """模拟断开。

            Args:
                None。

            Returns:
                bool: True。
            """

            self.connected = False
            return True

        def get_account_info(self):
            """返回账户信息。

            Args:
                None。

            Returns:
                dict: fake 账户信息。
            """

            return {"account_id": "demo"}

    async def _run_now(func, *args, **kwargs):
        """立即执行同步函数。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            object: func 的返回值。
        """

        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt.QmtBroker", _RecoveringBroker)
    monkeypatch.setattr("bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: object())
    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    guard = QmtAvailabilityGuard(config=_guard_config(initial_delay_seconds=0.01), name="test")
    adapter = QmtBrokerAdapter(config, router, guard=guard)
    ctx = router.get("default")

    try:
        await adapter.start()
        await _wait_until(lambda: guard.ready and _RecoveringBroker.connect_calls >= 2)
        resp = await adapter.get_account_info(ctx)
        assert resp["value"]["account_id"] == "demo"
    finally:
        await adapter.stop()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_adapter_cooldown_blocks_xtdata_calls(monkeypatch):
    """验证 data action 在 cooldown 期间不会触碰 xtdata。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _FakeProvider:
        """记录调用次数的 fake data provider。"""

        def __init__(self):
            """初始化调用计数。

            Args:
                None。

            Returns:
                None。
            """

            self.trade_days_calls = 0

        def get_trade_days(self, start_date=None, end_date=None, count=None):
            """返回交易日并记录调用次数。

            Args:
                start_date: 开始日期。
                end_date: 结束日期。
                count: 返回数量。

            Returns:
                list: fake 交易日列表。
            """

            self.trade_days_calls += 1
            return ["2026-06-09"]

    provider = _FakeProvider()
    monkeypatch.setattr("bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: provider)
    guard = QmtAvailabilityGuard(config=_guard_config(initial_delay_seconds=60.0), name="test")
    guard.mark_failure(RuntimeError("QMT down"), delay=60)
    adapter = QmtDataAdapter(guard, allow_request_probe=False)

    with pytest.raises(QmtGuardError) as exc_info:
        await adapter.get_trade_days({"start": "2026-06-09", "count": 1})

    assert exc_info.value.code == "QMT_UNAVAILABLE"
    assert provider.trade_days_calls == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_business_error_does_not_mark_unavailable(monkeypatch):
    """验证普通数据参数错误不会误触发全局 QMT cooldown。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _BadRequestProvider:
        """抛出普通业务异常的 fake data provider。"""

        def get_price(self, *args, **kwargs):
            """模拟坏参数异常。

            Args:
                *args: 位置参数。
                **kwargs: 关键字参数。

            Returns:
                None。

            Raises:
                ValueError: 固定表示请求参数错误。
            """

            raise ValueError("非法字段: bad_field")

    async def _run_now(func, *args, **kwargs):
        """立即执行同步函数。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            object: func 的返回值。
        """

        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider",
        lambda _cfg: _BadRequestProvider(),
    )
    guard = QmtAvailabilityGuard(config=_guard_config(), name="test")
    adapter = QmtDataAdapter(guard, allow_request_probe=False)

    with pytest.raises(RuntimeError, match="获取历史数据失败"):
        await adapter.get_history({"security": "000001.XSHE", "fields": ["bad_field"]})

    assert guard.ready is True
    assert guard.failure_count == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_data_connectivity_error_marks_unavailable(monkeypatch):
    """验证明确 xtdata 连接错误会触发 QMT cooldown。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _DownProvider:
        """抛出连接错误的 fake data provider。"""

        def get_trade_days(self, start_date=None, end_date=None, count=None):
            """模拟 xtdata 连接失败。

            Args:
                start_date: 开始日期。
                end_date: 结束日期。
                count: 返回数量。

            Returns:
                None。

            Raises:
                RuntimeError: 固定表示 xtdata 连接失败。
            """

            raise RuntimeError("xtdata connection refused")

    async def _run_now(func, *args, **kwargs):
        """立即执行同步函数。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            object: func 的返回值。
        """

        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    monkeypatch.setattr(
        "bullet_trade.server.adapters.qmt.MiniQMTProvider",
        lambda _cfg: _DownProvider(),
    )
    guard = QmtAvailabilityGuard(config=_guard_config(), name="test")
    adapter = QmtDataAdapter(guard, allow_request_probe=False)

    with pytest.raises(QmtGuardError) as exc_info:
        await adapter.get_trade_days({"start": "2026-06-09", "count": 1})

    assert exc_info.value.code == "QMT_UNAVAILABLE"
    assert guard.ready is False
    assert guard.failure_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_live_snapshot_connectivity_error_marks_unavailable(monkeypatch):
    """验证下单前实时行情连接错误会进入 QMT cooldown。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    class _DownProvider:
        """抛出连接错误的 fake live provider。"""

        def get_live_current(self, security):
            """模拟 live current 连接失败。

            Args:
                security: 证券代码。

            Returns:
                None。

            Raises:
                RuntimeError: 固定表示 xtdata 连接失败。
            """

            raise RuntimeError("xtdata connection refused")

    async def _run_now(func, *args, **kwargs):
        """立即执行同步函数。

        Args:
            func: 同步函数。
            *args: 位置参数。
            **kwargs: 关键字参数。

        Returns:
            object: func 的返回值。
        """

        return func(*args, **kwargs)

    monkeypatch.setattr("bullet_trade.server.adapters.qmt._run_in_qmt_executor", _run_now)
    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    guard = QmtAvailabilityGuard(config=_guard_config(), name="test")
    adapter = QmtBrokerAdapter(config, router, guard=guard)
    adapter._data_provider = _DownProvider()

    with pytest.raises(QmtGuardError) as exc_info:
        await adapter._get_live_snapshot("000001.XSHE")

    assert exc_info.value.code == "QMT_UNAVAILABLE"
    assert guard.ready is False
    assert guard.failure_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_qmt_broker_without_accounts_does_not_disable_data_guard(monkeypatch):
    """验证未配置 broker 账户时不会误伤 data guard。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    monkeypatch.setattr("bullet_trade.server.adapters.qmt.MiniQMTProvider", lambda _cfg: object())
    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=True,
        enable_broker=True,
        accounts=[],
    )
    router = AccountRouter(config.accounts)
    guard = QmtAvailabilityGuard(config=_guard_config(), name="test")
    adapter = QmtBrokerAdapter(config, router, guard=guard)

    await adapter.start()
    try:
        assert guard.ready is True
        assert guard.state == "ready"
    finally:
        await adapter.stop()


@pytest.mark.unit
def test_qmt_health_reports_unavailable_without_hiding_features():
    """验证 QMT 不可用时 health 暴露状态，但 features 保持兼容。

    Args:
        None。

    Returns:
        None。
    """

    guard = QmtAvailabilityGuard(config=_guard_config(initial_delay_seconds=60.0), name="test")
    guard.mark_failure(RuntimeError("QMT down"), delay=60)
    data_adapter = QmtDataAdapter.__new__(QmtDataAdapter)
    data_adapter.guard = guard
    data_adapter._allow_request_probe = False
    data_adapter.provider = None

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=True,
        enable_broker=False,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    app = ServerApplication(
        config, router, AdapterBundle(data_adapter=data_adapter, broker_adapter=None)
    )

    health = app._health_snapshot()["value"]
    assert health["process_alive"] is True
    assert health["features"] == ["data"]
    assert health["qmt"]["ready"] is False
    assert health["qmt"]["last_error"] == "QMT down"


@pytest.mark.unit
def test_remote_connection_can_health_check_when_qmt_unavailable():
    """验证老版远程客户端在 QMT down 时仍能握手并查询 health。

    Args:
        None。

    Returns:
        None。
    """

    guard = QmtAvailabilityGuard(config=_guard_config(initial_delay_seconds=60.0), name="test")
    guard.mark_failure(RuntimeError("QMT down"), delay=60)
    data_adapter = QmtDataAdapter.__new__(QmtDataAdapter)
    data_adapter.guard = guard
    data_adapter._allow_request_probe = False
    data_adapter.provider = None

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="t",
        enable_data=True,
        enable_broker=False,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    app = ServerApplication(
        config, router, AdapterBundle(data_adapter=data_adapter, broker_adapter=None)
    )
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_run_loop, args=(loop, app), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(app.wait_started(), loop).result(timeout=5)
    assert app._server is not None
    port = app._server.sockets[0].getsockname()[1]
    conn = RemoteQmtConnection(config.listen, port, config.token)
    try:
        conn.start()
        health = conn.request("admin.health", {})
        value = health["value"]
        assert value["features"] == ["data"]
        assert value["qmt"]["ready"] is False
        assert value["qmt"]["last_error"] == "QMT down"
    finally:
        conn.close()
        asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
