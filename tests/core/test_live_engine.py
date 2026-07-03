"""
LiveEngine 核心行为测试。
"""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import shutil
import sys
from datetime import date, datetime
from datetime import time as Time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.core import pricing
from bullet_trade.core.async_scheduler import AsyncScheduler
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.globals import g, reset_globals
from bullet_trade.core.live_engine import (
    LiveConfig,
    LiveEngine,
    LivePortfolioProxy,
    TradingCalendarGuard,
)
from bullet_trade.core.live_lock import LiveLockBusyError
from bullet_trade.core.live_runtime import (
    load_strategy_metadata,
    load_subscription_state,
    persist_strategy_metadata,
    save_g,
)
from bullet_trade.core.models import Order, OrderStatus
from bullet_trade.core.orders import (
    LimitOrderStyle,
    MarketOrderStyle,
    clear_order_queue,
    order,
)
from bullet_trade.core.risk_control import RiskController
from bullet_trade.core.runtime import set_current_engine
from bullet_trade.data.providers.base import DataProvider

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
V2_BRIDGE_PATH = WORKSPACE_ROOT / "strategies" / "bt_strategies" / "sim" / "common" / "v2_bridge.py"
_V2_SPEC = importlib.util.spec_from_file_location("bt_v2_bridge_live_engine_test", V2_BRIDGE_PATH)
assert _V2_SPEC is not None and _V2_SPEC.loader is not None
_V2_MODULE = importlib.util.module_from_spec(_V2_SPEC)
sys.modules[_V2_SPEC.name] = _V2_MODULE
_V2_SPEC.loader.exec_module(_V2_MODULE)
AiStocksV2Broker = _V2_MODULE.AiStocksV2Broker
V2ClientConfig = _V2_MODULE.V2ClientConfig


class OfflineLiveProvider(DataProvider):
    """
    LiveEngine 测试专用离线数据源。

    职责：为默认测试提供固定交易日、订阅和 tick 接口，避免 LiveEngine 单元测试误连真实行情源。
    核心协作对象：`bullet_trade.data.api` 全局 provider 和 LiveEngine 的交易日/订阅刷新逻辑。
    关键状态：仅维护内存中的 tick 订阅集合，不访问网络或磁盘缓存。
    """

    name = "offline_live"

    def __init__(self) -> None:
        """
        初始化离线 provider。

        Args:
            无。

        Returns:
            None。创建空的 tick 订阅状态。
        """
        self.tick_symbols: set[str] = set()
        self.tick_markets: set[str] = set()

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """
        执行离线认证。

        Args:
            user: 兼容数据源认证签名，测试中不使用。
            pwd: 兼容数据源认证签名，测试中不使用。
            host: 兼容数据源认证签名，测试中不使用。
            port: 兼容数据源认证签名，测试中不使用。

        Returns:
            None。该 provider 不访问外部行情服务。
        """
        return None

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        """
        返回工作日交易日序列。

        Args:
            start_date: 查询开始日期。
            end_date: 查询结束日期。
            count: 需要返回的最近交易日数量。

        Returns:
            List[datetime]: 固定工作日交易日列表。
        """
        end_ts = pd.to_datetime(end_date or "2026-01-01").normalize()
        if count is not None:
            return list(pd.bdate_range(end=end_ts, periods=count).to_pydatetime())
        start_ts = pd.to_datetime(start_date or end_ts).normalize()
        return list(pd.bdate_range(start=start_ts, end=end_ts).to_pydatetime())

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        """
        返回固定行情。

        Args:
            security: 单个或多个证券代码。
            start_date: 行情开始时间。
            end_date: 行情结束时间。
            frequency: 行情频率，测试中不影响固定返回。
            fields: 需要返回的字段。
            skip_paused: 兼容公开 API 参数，测试中不改变返回。
            fq: 兼容复权参数，测试中不改变返回。
            count: 需要返回的最近记录数量。
            panel: 兼容公开 API 参数，多证券时返回 MultiIndex 列。
            fill_paused: 兼容公开 API 参数，测试中不改变返回。
            pre_factor_ref_date: 兼容动态复权参数，测试中不改变返回。
            prefer_engine: 兼容 provider engine 参数，测试中不改变返回。
            force_no_engine: 兼容 provider engine 参数，测试中不改变返回。

        Returns:
            pd.DataFrame: 固定价格行情表。
        """
        _ = (
            frequency,
            skip_paused,
            fq,
            fill_paused,
            pre_factor_ref_date,
            prefer_engine,
            force_no_engine,
        )
        end_ts = pd.to_datetime(end_date or start_date or "2026-01-01").normalize()
        if count is not None:
            index = pd.bdate_range(end=end_ts, periods=count)
        else:
            start_ts = pd.to_datetime(start_date or end_ts).normalize()
            index = pd.bdate_range(start=start_ts, end=end_ts)
        if index.empty:
            index = pd.DatetimeIndex([end_ts])

        requested_fields = fields or ["open", "close", "high", "low", "volume", "money"]
        securities = security if isinstance(security, list) else [security]

        def _field_value(field: str) -> Any:
            """
            返回字段固定值。

            Args:
                field: 行情字段名。

            Returns:
                Any: 与字段类型匹配的固定值。
            """
            values: Dict[str, Any] = {
                "open": 10.0,
                "close": 10.0,
                "high": 10.2,
                "low": 9.8,
                "high_limit": 11.0,
                "low_limit": 9.0,
                "paused": False,
                "volume": 100000,
                "money": 1000000.0,
                "factor": 1.0,
            }
            return values.get(field, 10.0)

        if len(securities) == 1:
            return pd.DataFrame(
                {field: [_field_value(field)] * len(index) for field in requested_fields},
                index=index,
            )

        data = {
            (field, code): [_field_value(field)] * len(index)
            for field in requested_fields
            for code in securities
        }
        return pd.DataFrame(data, index=index)

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """
        返回固定证券列表。

        Args:
            types: 证券类型过滤条件。
            date: 查询日期。

        Returns:
            pd.DataFrame: 包含常用测试证券的证券表。
        """
        _ = types, date
        return pd.DataFrame(
            {"display_name": ["平安银行", "沪深300"], "type": ["stock", "index"]},
            index=["000001.XSHE", "000300.XSHG"],
        )

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        """
        返回固定指数成分。

        Args:
            index_symbol: 指数代码。
            date: 查询日期。

        Returns:
            List[str]: 固定股票列表。
        """
        _ = index_symbol, date
        return ["000001.XSHE"]

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """
        返回权益事件。

        Args:
            security: 证券代码。
            start_date: 查询开始日期。
            end_date: 查询结束日期。

        Returns:
            List[Dict[str, Any]]: LiveEngine 测试不覆盖分红，固定返回空列表。
        """
        _ = security, start_date, end_date
        return []

    def subscribe_ticks(self, symbols: List[str]) -> None:
        """
        记录 tick 订阅。

        Args:
            symbols: 需要订阅的证券代码列表。

        Returns:
            None。仅更新内存集合。
        """
        self.tick_symbols.update(symbols)

    def subscribe_markets(self, markets: List[str]) -> None:
        """
        记录市场订阅。

        Args:
            markets: 需要订阅的市场列表。

        Returns:
            None。仅更新内存集合。
        """
        self.tick_markets.update(markets)

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        """
        取消 tick 订阅。

        Args:
            symbols: 需要取消的证券代码；为 None 时清空全部。

        Returns:
            None。仅更新内存集合。
        """
        if symbols is None:
            self.tick_symbols.clear()
            return
        for symbol in symbols:
            self.tick_symbols.discard(symbol)

    def unsubscribe_markets(self, markets: Optional[List[str]] = None) -> None:
        """
        取消市场订阅。

        Args:
            markets: 需要取消的市场；为 None 时清空全部。

        Returns:
            None。仅更新内存集合。
        """
        if markets is None:
            self.tick_markets.clear()
            return
        for market in markets:
            self.tick_markets.discard(market)

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        返回固定 tick 快照。

        Args:
            security: 证券代码。
            dt: 查询时间。
            df: 是否返回 DataFrame，测试中固定返回 dict。

        Returns:
            Optional[Dict[str, Any]]: 已订阅证券返回快照，未订阅返回 None。
        """
        _ = dt, df
        if security not in self.tick_symbols:
            return None
        return {"sid": security, "last_price": 10.0, "dt": datetime.now().isoformat()}


class DummyBroker(BrokerBase):
    def __init__(self):
        super().__init__("dummy")
        self.orders: list[tuple[str, int, float, str, bool]] = []
        self.account_sync_calls = 0
        self.order_sync_calls = 0
        self.heartbeat_calls = 0
        self._tick_snapshots: dict[str, dict] = {}

    def connect(self) -> bool:
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self._connected = False
        return True

    def get_account_info(self):
        return {
            "account_id": "dummy",
            "account_type": "stock",
            "positions": [],
            "available_cash": 1000.0,
            "total_value": 1000.0,
        }

    def get_positions(self):
        return []

    async def buy(
        self,
        security: str,
        amount: int,
        price: float | None = None,
        wait_timeout: float | None = None,
        remark: str | None = None,
        *,
        market: bool = False,
    ) -> str:
        self.orders.append((security, amount, price or 0.0, "buy", market))
        return f"buy-{len(self.orders)}"

    async def sell(
        self,
        security: str,
        amount: int,
        price: float | None = None,
        wait_timeout: float | None = None,
        remark: str | None = None,
        *,
        market: bool = False,
    ) -> str:
        self.orders.append((security, amount, price or 0.0, "sell", market))
        return f"sell-{len(self.orders)}"

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def get_order_status(self, order_id: str):
        return {}

    def supports_account_sync(self) -> bool:
        return True

    def supports_orders_sync(self) -> bool:
        return True

    def supports_tick_subscription(self) -> bool:
        return True

    def sync_account(self):
        self.account_sync_calls += 1
        return {
            "available_cash": 888.0,
            "total_value": 999.0,
            "positions": [
                {
                    "security": "000001.XSHE",
                    "amount": 100,
                    "avg_cost": 10.0,
                    "current_price": 11.0,
                    "market_value": 1100.0,
                }
            ],
        }

    def sync_orders(self):
        self.order_sync_calls += 1
        return []

    def heartbeat(self):
        self.heartbeat_calls += 1

    def subscribe_ticks(self, symbols):
        for sym in symbols:
            self._tick_snapshots[sym] = {
                "sid": sym,
                "last_price": 1.23,
                "dt": datetime.now().isoformat(),
            }

    def unsubscribe_ticks(self, symbols=None):
        if not symbols:
            self._tick_snapshots.clear()
            return
        for sym in symbols:
            self._tick_snapshots.pop(sym, None)

    def get_current_tick(self, symbol: str):
        return self._tick_snapshots.get(symbol)


class AccountBroker(DummyBroker):
    def __init__(
        self,
        *,
        account_id: str = "dummy",
        account_key: str = "",
        sub_account_id: str = "",
    ):
        super().__init__()
        self.account_id = account_id
        if account_key:
            self.account_key = account_key
        if sub_account_id:
            self.sub_account_id = sub_account_id


class FillAwareBroker(DummyBroker):
    def __init__(self, *, fill_price: float):
        super().__init__()
        self.fill_price = fill_price
        self.last_order_id = ""
        self.submitted_price = 0.0
        self.submitted_amount = 0
        self.submitted_security = ""
        self.submitted_market = False

    async def buy(
        self,
        security: str,
        amount: int,
        price: float | None = None,
        wait_timeout: float | None = None,
        remark: str | None = None,
        *,
        market: bool = False,
    ) -> str:
        order_id = await super().buy(
            security,
            amount,
            price,
            wait_timeout=wait_timeout,
            remark=remark,
            market=market,
        )
        self.last_order_id = order_id
        self.submitted_price = float(price or 0.0)
        self.submitted_amount = int(amount)
        self.submitted_security = security
        self.submitted_market = bool(market)
        return order_id

    def sync_account(self):
        self.account_sync_calls += 1
        if not self.last_order_id:
            return {
                "available_cash": 100000.0,
                "total_value": 100000.0,
                "positions": [],
            }
        reserved_cash = self.submitted_amount * self.submitted_price
        return {
            "available_cash": 100000.0 - reserved_cash,
            "total_value": 100000.0,
            "positions": [
                {
                    "security": self.submitted_security,
                    "amount": self.submitted_amount,
                    "avg_cost": self.submitted_price,
                    "current_price": self.fill_price,
                    "market_value": self.fill_price * self.submitted_amount,
                }
            ],
        }

    def sync_orders(self):
        self.order_sync_calls += 1
        if not self.last_order_id:
            return []
        return [
            {
                "order_id": self.last_order_id,
                "security": self.submitted_security,
                "status": "filled",
                "amount": self.submitted_amount,
                "filled_amount": self.submitted_amount,
                "price": self.submitted_price,
                "avg_cost": self.fill_price,
                "order_price": self.submitted_price,
                "is_buy": True,
            }
        ]

    def get_trades(
        self,
        order_id: str | None = None,
        security: str | None = None,
    ):
        if not self.last_order_id:
            return []
        if order_id and str(order_id) != self.last_order_id:
            return []
        if security and security != self.submitted_security:
            return []
        return [
            {
                "trade_id": f"{self.last_order_id}-T1",
                "order_id": self.last_order_id,
                "security": self.submitted_security,
                "amount": self.submitted_amount,
                "price": self.fill_price,
                "time": datetime.now().isoformat(),
            }
        ]


class SequencedV2Client:
    def __init__(self, responses: dict[str, list[object]]):
        self.responses = {
            action: [copy.deepcopy(item) for item in items] for action, items in responses.items()
        }
        self.calls: list[tuple[str, object, object]] = []

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def health(self):
        return {"ok": True}

    def request(self, action, payload=None, timeout=None):
        self.calls.append((action, copy.deepcopy(payload), timeout))
        queue = self.responses.get(action)
        if not queue:
            raise AssertionError(f"未预置或已耗尽 V2 响应: {action}")
        return copy.deepcopy(queue.pop(0))


def _build_v2_broker(client: SequencedV2Client) -> Any:
    broker = AiStocksV2Broker(
        V2ClientConfig(
            host="127.0.0.1",
            port=59620,
            token="token",
            account_key="lxm_main",
            sub_account_id="btsim_core_etf",
            strategy_name="test-v2",
        )
    )
    broker.client = client
    return broker


def _build_v2_live_engine(tmp_path: Path, broker: BrokerBase) -> LiveEngine:
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime-v2"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=cfg,
    )
    return engine


def _prime_live_engine(engine: LiveEngine, broker: BrokerBase) -> None:
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = broker
    engine._risk = None
    engine.context.current_dt = datetime(2026, 4, 1, 10, 0, 0)
    engine.context.portfolio.available_cash = 100000.0
    engine.context.portfolio.transferable_cash = 100000.0
    engine.context.portfolio.locked_cash = 0.0
    engine.context.portfolio.total_value = 100000.0


class LifecycleBroker(DummyBroker):
    def __init__(self):
        super().__init__()
        self.before_open_calls = 0
        self.after_close_calls = 0

    def before_open(self) -> None:
        self.before_open_calls += 1

    def after_close(self) -> None:
        self.after_close_calls += 1


def _write_strategy(tmp_path: Path) -> Path:
    src = """
from bullet_trade.core.scheduler import run_daily

def initialize(context):
    def every_minute(ctx):
        ctx.minute_calls = getattr(ctx, 'minute_calls', 0) + 1
    run_daily(every_minute, "every_minute")

def handle_data(context, data):
    context.handle_called = getattr(context, 'handle_called', 0) + 1
"""
    path = tmp_path / "strategy_live.py"
    path.write_text(src, encoding="utf-8")
    return path


def _write_strategy_with_hooks(tmp_path: Path, marker: int) -> Path:
    src = f"""
from bullet_trade.core.scheduler import run_daily
from bullet_trade.core.globals import g

def initialize(context):
    g.init_calls = (getattr(g, 'init_calls', 0) or 0) + 1
    run_daily(every_minute, "every_minute")

def process_initialize(context):
    g.proc_calls = (getattr(g, 'proc_calls', 0) or 0) + 1

def after_code_changed(context):
    g.after_calls = (getattr(g, 'after_calls', 0) or 0) + 1
    g.code_marker = {marker}

def every_minute(context):
    pass
"""
    path = tmp_path / "strategy_hooks.py"
    path.write_text(src, encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _isolate_bullet_trade_home(monkeypatch, tmp_path):
    """
    隔离 LiveEngine 测试的运行目录和行情 provider。

    Args:
        monkeypatch: pytest 提供的临时属性替换工具。
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。测试结束后自动恢复环境变量和全局 provider。
    """
    from bullet_trade.data import api as data_api

    provider = OfflineLiveProvider()
    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path))
    monkeypatch.setattr(data_api, "_provider", provider, raising=False)
    monkeypatch.setattr(data_api, "_provider_cache", {provider.name: provider}, raising=False)
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {provider.name: True}, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", True, raising=False)


def test_live_config_defaults_risk_check_to_false(monkeypatch):
    monkeypatch.delenv("RISK_CHECK_ENABLED", raising=False)

    cfg = LiveConfig.load()

    assert cfg.risk_check_enabled is False


def test_live_config_parses_string_bool_overrides(monkeypatch):
    monkeypatch.delenv("RISK_CHECK_ENABLED", raising=False)
    monkeypatch.delenv("ACCOUNT_SYNC_ENABLED", raising=False)

    cfg = LiveConfig.load(
        {
            "risk_check_enabled": "false",
            "account_sync_enabled": "false",
        }
    )

    assert cfg.risk_check_enabled is False
    assert cfg.account_sync_enabled is False


def _build_lock_test_engine(
    tmp_path: Path,
    strategy: Path,
    *,
    runtime_name: str,
    account_id: str = "dummy",
    account_key: str = "",
    sub_account_id: str = "",
) -> LiveEngine:
    cfg = {
        "runtime_dir": str(tmp_path / runtime_name),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    return LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: AccountBroker(
            account_id=account_id,
            account_key=account_key,
            sub_account_id=sub_account_id,
        ),
        live_config=cfg,
    )


def test_live_engine_rejects_duplicate_instance_same_account(monkeypatch, tmp_path):
    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path))
    strategy = _write_strategy(tmp_path)
    engine1 = _build_lock_test_engine(
        tmp_path, strategy, runtime_name="runtime-a", account_id="acct-main"
    )
    engine2 = _build_lock_test_engine(
        tmp_path, strategy, runtime_name="runtime-b", account_id="acct-main"
    )

    engine1._acquire_live_locks()
    try:
        with pytest.raises(LiveLockBusyError, match="重复 live 实例"):
            engine2._acquire_live_locks()
    finally:
        engine2._release_live_locks()
        engine1._release_live_locks()


def test_live_engine_rejects_shared_runtime_dir_across_accounts(monkeypatch, tmp_path):
    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path))
    strategy = _write_strategy(tmp_path)
    engine1 = _build_lock_test_engine(
        tmp_path,
        strategy,
        runtime_name="shared-runtime",
        account_key="parent-a",
        sub_account_id="sub-a",
    )
    engine2 = _build_lock_test_engine(
        tmp_path,
        strategy,
        runtime_name="shared-runtime",
        account_key="parent-a",
        sub_account_id="sub-b",
    )

    engine1._acquire_live_locks()
    try:
        with pytest.raises(LiveLockBusyError, match="RUNTIME_DIR 已被其他 live 实例占用"):
            engine2._acquire_live_locks()
    finally:
        engine2._release_live_locks()
        engine1._release_live_locks()


def test_live_engine_allows_parallel_instances_for_different_accounts(monkeypatch, tmp_path):
    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path))
    strategy = _write_strategy(tmp_path)
    engine1 = _build_lock_test_engine(
        tmp_path,
        strategy,
        runtime_name="runtime-sub-a",
        account_key="parent-a",
        sub_account_id="sub-a",
    )
    engine2 = _build_lock_test_engine(
        tmp_path,
        strategy,
        runtime_name="runtime-sub-b",
        account_key="parent-a",
        sub_account_id="sub-b",
    )

    try:
        engine1._acquire_live_locks()
        engine2._acquire_live_locks()
        assert engine1._runtime_lock is not None
        assert engine1._instance_lock is not None
        assert engine2._runtime_lock is not None
        assert engine2._instance_lock is not None
    finally:
        engine2._release_live_locks()
        engine1._release_live_locks()


@pytest.mark.asyncio
async def test_live_engine_respects_market_session(tmp_path, monkeypatch):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 1, 9, 0, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    assert isinstance(engine.context.portfolio, LivePortfolioProxy)
    await engine._ensure_trading_day(date(2025, 1, 2))

    # 09:31 -> 正常触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 31))
    assert getattr(engine.context, "handle_called", 0) == 1
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 窗口结束 11:30 不应触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 11, 30))
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 午休 11:31 -> 不触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 11, 31))
    assert getattr(engine.context, "handle_called", 0) == 1
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 下午 13:01 -> 再次触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 13, 1))
    assert getattr(engine.context, "handle_called", 0) == 2
    assert getattr(engine.context, "minute_calls", 0) == 2

    # 收盘 18:00 不触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 18, 0))
    assert getattr(engine.context, "minute_calls", 0) == 2

    await engine._shutdown()


@pytest.mark.asyncio
async def test_tick_subscription_and_account_sync(tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_interval": 1,
        "account_sync_enabled": True,
        "order_sync_enabled": False,
        "tick_subscription_limit": 2,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 1, 9, 0, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()

    # tick 订阅写入 runtime
    engine.register_tick_subscription(["000001.XSHE"], [])
    symbols, markets = load_subscription_state()
    assert "000001.XSHE" in symbols

    with pytest.raises(ValueError):
        engine.register_tick_subscription(["000002.XSHE", "000003.XSHE"], [])

    # 手动触发账户同步
    await engine._account_sync_step()
    assert engine.context.portfolio.available_cash == 888.0
    assert "000001.XSHE" in engine.context.portfolio.positions

    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_account_sync_positions_support_broker_suffix_aliases(tmp_path):
    class BrokerSuffixDummy(DummyBroker):
        def sync_account(self):
            self.account_sync_calls += 1
            return {
                "available_cash": 350608.27,
                "total_value": 499979.27,
                "positions": [
                    {
                        "security": "511880.SH",
                        "amount": 500,
                        "closeable_amount": 500,
                        "avg_cost": 100.601,
                        "current_price": 100.598,
                        "market_value": 50299.0,
                    },
                    {
                        "security": "000001.SZ",
                        "amount": 9600,
                        "closeable_amount": 0,
                        "avg_cost": 10.32,
                        "current_price": 10.32,
                        "market_value": 99072.0,
                    },
                ],
            }

    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_interval": 1,
        "account_sync_enabled": True,
        "order_sync_enabled": False,
        "tick_subscription_limit": 2,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    broker = BrokerSuffixDummy()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 1, 9, 0, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    await engine._account_sync_step()

    positions = engine.context.portfolio.positions
    assert positions["511880.XSHG"].total_amount == 500
    assert positions.get("511880.XSHG").closeable_amount == 500
    assert positions["000001.XSHE"].total_amount == 9600
    assert list(positions.keys()) == ["511880.SH", "000001.SZ"]
    assert len(list(positions.values())) == 2
    assert engine.context.portfolio.positions_value == pytest.approx(149371.0)

    stock_sub = engine.context.portfolio.subportfolios["stock"]
    assert stock_sub.positions["511880.XSHG"].total_amount == 500
    assert stock_sub.positions["000001.XSHE"].closeable_amount == 0

    await engine._shutdown()


@pytest.mark.asyncio
async def test_scheduler_resets_future_cursor(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    frozen_now = datetime(2025, 1, 2, 10, 0)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: frozen_now,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))

    # 模拟残留游标在未来
    engine._last_schedule_dt = datetime(2025, 1, 2, 12, 0)
    await engine._handle_minute_tick(datetime(2025, 1, 2, 10, 0, 5))

    assert engine.context.current_dt == datetime(2025, 1, 2, 10, 0)
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 10, 0)

    await engine._shutdown()


@pytest.mark.asyncio
async def test_portfolio_proxy_refresh_on_access(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    broker = DummyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 45),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    broker.account_sync_calls = 0
    _ = engine.context.portfolio.available_cash
    assert broker.account_sync_calls >= 1
    assert engine.context.portfolio.available_cash == 888.0
    assert "000001.XSHE" in engine.context.portfolio.positions
    await engine._shutdown()


@pytest.mark.asyncio
async def test_initialize_skipped_and_after_code_changed(tmp_path):
    runtime_dir = tmp_path / "runtime"
    strategy = _write_strategy_with_hooks(tmp_path, marker=1)
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }

    loop = asyncio.get_running_loop()

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    save_g()
    await engine._shutdown()

    assert (getattr(g, "init_calls", 0) or 0) == 1
    assert (getattr(g, "proc_calls", 0) or 0) == 1
    assert (getattr(g, "after_calls", 0) or 0) == 0

    strategy = _write_strategy_with_hooks(tmp_path, marker=2)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 5),
    )
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    save_g()
    await engine._shutdown()

    assert (getattr(g, "init_calls", 0) or 0) == 1
    assert (getattr(g, "proc_calls", 0) or 0) == 2
    assert (getattr(g, "after_calls", 0) or 0) == 1

    reset_globals()
    shutil.rmtree(runtime_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_strategy_start_date_persisted_and_restored(tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    loop = asyncio.get_running_loop()

    first_day = date(2025, 1, 2)
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    await engine._ensure_trading_day(first_day)
    save_g()
    meta = load_strategy_metadata()
    assert meta.get("strategy_start_date") == first_day.isoformat()
    await engine._shutdown()

    second_day = date(2025, 1, 6)
    engine2 = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 6, 9, 0),
    )
    engine2._loop = loop
    engine2._stop_event = asyncio.Event()
    engine2.event_bus = EventBus(loop)
    engine2.async_scheduler = AsyncScheduler()
    await engine2._bootstrap()
    assert engine2._strategy_start_date == first_day
    await engine2._ensure_trading_day(second_day)
    assert engine2._strategy_start_date == first_day
    await engine2._shutdown()

    meta = load_strategy_metadata()
    meta.pop("strategy_start_date", None)
    persist_strategy_metadata(meta)

    third_day = date(2025, 1, 7)
    engine3 = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 7, 9, 0),
    )
    engine3._loop = loop
    engine3._stop_event = asyncio.Event()
    engine3.event_bus = EventBus(loop)
    engine3.async_scheduler = AsyncScheduler()
    await engine3._bootstrap()
    assert engine3._strategy_start_date is None
    await engine3._ensure_trading_day(third_day)
    meta = load_strategy_metadata()
    assert meta.get("strategy_start_date") == third_day.isoformat()
    await engine3._shutdown()

    reset_globals()
    shutil.rmtree(runtime_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_calendar_guard_skips_weekend(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": True, "calendar_retry_minutes": 15})

    def _raise(*args, **kwargs):
        raise RuntimeError("no data")

    monkeypatch.setattr("bullet_trade.data.api.get_trade_days", _raise, raising=False)
    saturday = datetime(2025, 1, 4, 9, 0)
    result = await guard.ensure_trade_day(saturday)
    assert result is False
    assert guard._next_check == saturday + timedelta(minutes=15)

    monday = datetime(2025, 1, 6, 9, 0)
    result = await guard.ensure_trade_day(monday)
    assert result is True
    assert guard._confirmed_date == monday.date()


@pytest.mark.asyncio
async def test_calendar_guard_weekend_allowed(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": False})

    def _raise(*args, **kwargs):
        raise RuntimeError("no data")

    monkeypatch.setattr("bullet_trade.data.api.get_trade_days", _raise, raising=False)
    sunday = datetime(2025, 1, 5, 9, 0)
    result = await guard.ensure_trade_day(sunday)
    assert result is True
    assert guard._confirmed_date == sunday.date()


@pytest.mark.asyncio
async def test_calendar_guard_list_includes_target(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": True, "calendar_retry_minutes": 1})

    def _fake_days(*_args, **_kwargs):
        return ["2025-01-06"]

    monkeypatch.setattr("bullet_trade.data.api.get_trade_days", _fake_days, raising=False)
    monday = datetime(2025, 1, 6, 9, 0)
    result = await guard.ensure_trade_day(monday)
    assert result is True
    assert guard._confirmed_date == monday.date()


@pytest.mark.asyncio
async def test_calendar_guard_list_missing_target(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": True, "calendar_retry_minutes": 1})

    def _fake_days(*_args, **_kwargs):
        return ["2025-01-03"]

    monkeypatch.setattr("bullet_trade.data.api.get_trade_days", _fake_days, raising=False)
    monday = datetime(2025, 1, 6, 9, 0)
    result = await guard.ensure_trade_day(monday)
    assert result is False
    assert guard._next_check == monday + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_live_engine_waits_for_calendar_retry_when_not_trade_day(tmp_path, monkeypatch):
    strategy = _write_strategy(tmp_path)
    now = datetime(2025, 1, 4, 7, 0, 0)
    sleep_calls: list[float] = []
    stop_event = asyncio.Event()

    async def _fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        stop_event.set()

    async def _always_closed(_self, _target):
        return False

    monkeypatch.setattr(TradingCalendarGuard, "_is_trading_day", _always_closed)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config={"calendar_retry_minutes": 1},
        now_provider=lambda: now,
        sleep_provider=_fake_sleep,
    )
    engine._loop = asyncio.get_running_loop()
    engine._stop_event = stop_event

    await engine._run_loop()

    assert len(sleep_calls) == 1
    assert sleep_calls[0] >= 59.0


@pytest.mark.asyncio
async def test_live_engine_skips_processed_minute_after_restart(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))
    engine._last_schedule_dt = datetime(2025, 1, 2, 9, 40)
    original_dt = engine.context.current_dt

    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 20))
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 40)
    assert engine.context.current_dt == original_dt

    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 41, 0))
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 41)
    assert engine.context.current_dt == datetime(2025, 1, 2, 9, 41)
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_event_timeout_drops_minute(tmp_path, caplog):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
        "event_time_out": 5,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))

    def _fail_handle(*_args, **_kwargs):
        raise AssertionError("handle_data should be skipped when timed-out")

    engine.handle_data_func = _fail_handle
    caplog.set_level("WARNING", logger="jq_strategy")
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 31, 10))
    assert "事件超时丢弃" in caplog.text
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 31)
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_applies_scheduler_override_from_env(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "08:00-09:00,10:00-10:30",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 7, 50),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))
    assert engine._market_periods == [(Time(8, 0), Time(9, 0)), (Time(10, 0), Time(10, 30))]
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_tick_snapshot_and_unsubscribe(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    engine._latest_ticks["000001.XSHE"] = {"sid": "000001.XSHE", "last_price": 10.5}
    snap = engine.get_current_tick_snapshot("000001.XSHE")
    assert snap["last_price"] == 10.5

    engine._latest_ticks.clear()
    engine.broker._tick_snapshots["000001.XSHE"] = {"sid": "000001.XSHE", "last_price": 11.0}  # type: ignore[attr-defined]
    snap = engine.get_current_tick_snapshot("000001.XSHE")
    assert snap["last_price"] == 11.0

    engine.register_tick_subscription(["000001.XSHE"], [])
    assert "000001.XSHE" in engine._tick_symbols
    engine.unsubscribe_all_ticks()
    assert not engine._tick_symbols
    assert not engine._tick_markets
    await engine._shutdown()


@pytest.mark.asyncio
async def test_handle_tick_hook_receives_context(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    payload = {}

    async def _handler(ctx, tick):
        payload["context"] = ctx
        payload["tick"] = tick

    engine.handle_tick_func = _handler
    await engine._call_hook(engine.handle_tick_func, {"sid": "000001.XSHE"})
    assert payload["context"] is engine.context
    assert payload["tick"]["sid"] == "000001.XSHE"
    await engine._shutdown()


@pytest.mark.asyncio
async def test_process_orders_skips_risk_checks_when_disabled(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = DummyBroker()
    engine.context.portfolio.available_cash = 1_000
    engine.context.portfolio.total_value = 1_000
    assert engine._risk is None

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.XSHE": Snap()}
    )

    clear_order_queue()
    set_current_engine(engine)
    try:
        order("159915.XSHE", 100)
        await engine._process_orders(engine.context.current_dt)
    finally:
        set_current_engine(None)

    assert len(engine.broker.orders) == 1
    security, amount, _, side, market = engine.broker.orders[0]
    assert (security, amount, side, market) == ("159915.XSHE", 100, "buy", True)


@pytest.mark.asyncio
async def test_process_orders_rejects_buy_below_min_value_when_risk_enabled(monkeypatch, tmp_path):
    """测试实盘风控开启后会拒绝低于最小金额的买入委托。"""
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    engine.broker = DummyBroker()
    engine.context.portfolio.available_cash = 10_000
    engine.context.portfolio.total_value = 10_000
    engine._risk = RiskController(
        config={
            "max_order_value": 100_000,
            "max_daily_trade_value": 500_000,
            "max_daily_trades": 100,
            "max_daily_cancels": 100,
            "min_cancel_interval_seconds": 0.0,
            "max_cancel_per_order": 3,
            "min_buy_order_value": 2_000.0,
            "max_stock_count": 20,
            "max_position_ratio": 100.0,
            "stop_loss_ratio": 5.0,
        }
    )

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.XSHE": Snap()}
    )

    clear_order_queue()
    order_obj = order("159915.XSHE", 100)

    await engine._process_orders(engine.context.current_dt)

    assert len(engine.broker.orders) == 0
    assert order_obj.status == OrderStatus.rejected


@pytest.mark.asyncio
async def test_market_flag_propagates_to_broker(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    engine.broker = DummyBroker()
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    engine._risk = None

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()}
    )

    clear_order_queue()
    order("000001.XSHE", 100)
    order("000001.XSHE", 100, price=10.5)

    await engine._process_orders(engine.context.current_dt)

    assert len(engine.broker.orders) == 2

    sec1, amt1, price1, side1, market1 = engine.broker.orders[0]
    assert (sec1, amt1, side1, market1) == ("000001.XSHE", 100, "buy", True)
    expected_price = pricing.compute_market_protect_price(
        "000001.XSHE", 10.0, 10.5, 9.5, 0.015, True
    )
    assert price1 == pytest.approx(expected_price)

    sec2, amt2, price2, side2, market2 = engine.broker.orders[1]
    assert (sec2, amt2, side2, market2) == ("000001.XSHE", 100, "buy", False)
    assert price2 == pytest.approx(10.5)


def test_get_orders_from_broker_includes_external_snapshots(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    engine.broker = DummyBroker()
    clear_order_queue()

    local_order = Order(
        order_id="local-1",
        security="000001.XSHE",
        amount=100,
        status=OrderStatus.open,
        is_buy=True,
    )
    engine._orders[local_order.order_id] = local_order
    engine._broker_order_index["B1"] = local_order.order_id

    snapshots = [
        {
            "order_id": "B1",
            "security": "000001.XSHE",
            "status": "open",
            "amount": 100,
            "traded_volume": 30,
            "traded_price": 10.2,
            "order_remark": "bt:alpha:abcd1234",
        },
        {
            "order_id": "B2",
            "security": "000002.XSHE",
            "status": "filled",
            "amount": 200,
            "traded_volume": 200,
            "traded_price": 11.8,
            "order_type": "SELL",
            "strategy_name": "manual",
        },
    ]
    engine._sync_orders_from_broker = lambda **_: snapshots  # type: ignore[assignment]

    default_orders = engine.get_orders()
    assert set(default_orders.keys()) == {"local-1"}

    broker_orders = engine.get_orders(from_broker=True)
    assert set(broker_orders.keys()) == {"B1", "B2"}
    assert broker_orders["B1"].extra.get("engine_order_id") == "local-1"
    assert broker_orders["B1"].extra.get("is_external") is False
    assert broker_orders["B2"].extra.get("is_external") is True
    assert broker_orders["B2"].security == "000002.XSHE"
    assert broker_orders["B2"].status == OrderStatus.filled

    filled_orders = engine.get_orders(from_broker=True, status=OrderStatus.filled)
    assert set(filled_orders.keys()) == {"B2"}

    target_order = engine.get_orders(from_broker=True, order_id="B2")
    assert set(target_order.keys()) == {"B2"}


def test_live_engine_get_open_orders_includes_submitted_string_status(tmp_path):
    """submitted 字符串状态应被视为 live 在途订单。"""

    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    local_order = Order(
        order_id="submitted-1",
        security="000001.XSHE",
        amount=100,
        status="submitted",
        is_buy=True,
    )
    engine._orders[local_order.order_id] = local_order

    open_orders = engine.get_open_orders()

    assert "submitted-1" in open_orders


def test_apply_order_snapshots_prefers_filled_price_over_order_price(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    local_order = Order(
        order_id="local-1",
        security="159915.XSHE",
        amount=100,
        price=3.247,
        status=OrderStatus.open,
        is_buy=True,
    )
    engine._orders[local_order.order_id] = local_order
    engine._broker_order_index["B1"] = local_order.order_id

    engine._apply_order_snapshots(
        [
            {
                "order_id": "B1",
                "security": "159915.XSHE",
                "status": "filled",
                "amount": 100,
                "filled_amount": 100,
                "price": 3.247,
                "avg_cost": 3.231,
                "order_price": 3.247,
            }
        ]
    )

    assert local_order.filled == 100
    assert local_order.price == pytest.approx(3.231)
    assert local_order.extra["order_price"] == pytest.approx(3.247)


def test_apply_order_snapshots_preserves_requested_market_price_when_broker_reports_different_order_price(
    tmp_path,
):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=str(strategy),
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    local_order = Order(
        order_id="local-1",
        security="159967.XSHE",
        amount=1000,
        price=0.0,
        status=OrderStatus.open,
        is_buy=False,
        style=MarketOrderStyle(),
        extra={"order_price": 0.626, "requested_order_price": 0.626},
    )
    engine._orders[local_order.order_id] = local_order
    engine._broker_order_index["B1"] = local_order.order_id

    engine._apply_order_snapshots(
        [
            {
                "order_id": "B1",
                "security": "159967.XSHE",
                "status": "filled",
                "amount": 1000,
                "filled_amount": 1000,
                "price": 0.634,
                "avg_cost": 0.634,
                "order_price": 0.634,
                "style_type": "market",
            }
        ]
    )

    assert local_order.price == pytest.approx(0.634)
    assert local_order.extra["order_price"] == pytest.approx(0.626)
    assert local_order.extra["requested_order_price"] == pytest.approx(0.626)
    assert local_order.extra["broker_order_price"] == pytest.approx(0.634)


def test_apply_order_snapshots_preserves_settlement_state_fields(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    local_order = Order(
        order_id="local-1",
        security="127085.XSHE",
        amount=750,
        price=130.881,
        status=OrderStatus.open,
        is_buy=True,
    )
    engine._orders[local_order.order_id] = local_order
    engine._broker_order_index["B1"] = local_order.order_id

    engine._apply_order_snapshots(
        [
            {
                "order_id": "B1",
                "security": "127085.XSHE",
                "status": "canceled",
                "amount": 750,
                "filled_amount": 0,
                "order_price": 130.881,
                "settlement_state": "pending",
                "settlement_pending_reason": "[pending_settlement] filled_amount=750 缺少可信 traded_price/deal_balance",
            }
        ]
    )

    assert local_order.extra["settlement_state"] == "pending"
    assert "pending_settlement" in local_order.extra["settlement_pending_reason"]


def test_apply_order_snapshots_debug_logs_only_on_signature_change(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("BT_LIVE_ORDER_DEBUG", "1")
    caplog.set_level("INFO", logger="jq_strategy")

    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    local_order = Order(
        order_id="local-1",
        security="159915.XSHE",
        amount=100,
        price=3.247,
        status=OrderStatus.open,
        is_buy=True,
    )
    engine._orders[local_order.order_id] = local_order
    engine._broker_order_index["B1"] = local_order.order_id

    snapshot = {
        "order_id": "B1",
        "security": "159915.XSHE",
        "status": "filled",
        "amount": 100,
        "filled_amount": 100,
        "price": 3.247,
        "avg_cost": 3.231,
        "order_price": 3.247,
    }
    engine._apply_order_snapshots([snapshot])
    engine._apply_order_snapshots([dict(snapshot)])
    engine._apply_order_snapshots([{**snapshot, "order_price": 3.248}])

    lines = [
        record.getMessage()
        for record in caplog.records
        if "[ORDER_DEBUG] live.apply_order_snapshot" in record.getMessage()
    ]
    assert len(lines) == 2
    assert "order_price': 3.247" in lines[0]
    assert "order_price': 3.248" in lines[1]


@pytest.mark.asyncio
async def test_process_orders_reconciles_limit_buy_avg_cost_from_trades(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: FillAwareBroker(fill_price=3.231),
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = FillAwareBroker(fill_price=3.231)
    engine._risk = None
    engine.context.portfolio.available_cash = 100000.0
    engine.context.portfolio.total_value = 100000.0
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 3.231
        high_limit = 3.500
        low_limit = 3.000

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.XSHE": Snap()}
    )

    clear_order_queue()
    await asyncio.to_thread(order, "159915.XSHE", 30700, LimitOrderStyle(3.247))

    position = engine.context.portfolio.positions["159915.XSHE"]
    assert position.total_amount == 30700
    assert position.avg_cost == pytest.approx(3.231)
    assert engine.context.portfolio.available_cash == pytest.approx(100000.0 - 30700 * 3.247)
    set_current_engine(None)


@pytest.mark.asyncio
async def test_process_orders_reconciles_market_buy_avg_cost_from_trades(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "market_buy_price_percent": 0.015,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: FillAwareBroker(fill_price=3.231),
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = FillAwareBroker(fill_price=3.231)
    engine._risk = None
    engine.context.portfolio.available_cash = 100000.0
    engine.context.portfolio.total_value = 100000.0
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 3.231
        high_limit = 3.500
        low_limit = 3.000

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.XSHE": Snap()}
    )
    monkeypatch.setattr(pricing, "compute_market_protect_price", lambda *args, **kwargs: 3.279)

    clear_order_queue()
    await asyncio.to_thread(order, "159915.XSHE", 30700, MarketOrderStyle())

    position = engine.context.portfolio.positions["159915.XSHE"]
    assert position.total_amount == 30700
    assert position.avg_cost == pytest.approx(3.231)
    assert engine.broker.submitted_market is True
    assert engine.broker.submitted_price == pytest.approx(3.279)
    set_current_engine(None)


def test_apply_account_snapshot_preserves_v2_locked_cash_and_stock_subportfolio(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )

    engine._apply_account_snapshot(
        {
            "available_cash": 317.10,
            "transferable_cash": 317.10,
            "locked_cash": 491.20,
            "total_value": 100000.0,
            "positions": [
                {
                    "security": "159915.XSHE",
                    "amount": 30700,
                    "closeable_amount": 0,
                    "avg_cost": 3.231,
                    "current_price": 3.231,
                    "market_value": 99191.7,
                }
            ],
        }
    )

    portfolio = engine.context.portfolio
    assert portfolio.available_cash == pytest.approx(317.10)
    assert portfolio.transferable_cash == pytest.approx(317.10)
    assert portfolio.locked_cash == pytest.approx(491.20)
    assert portfolio.total_value == pytest.approx(100000.0)
    assert portfolio.positions["159915.XSHE"].closeable_amount == 0
    stock_sub = portfolio.subportfolios["stock"]
    assert stock_sub.available_cash == pytest.approx(317.10)
    assert stock_sub.transferable_cash == pytest.approx(317.10)
    assert stock_sub.positions["159915.XSHE"].total_amount == 30700


@pytest.mark.asyncio
async def test_live_engine_v2_rejected_order_releases_locked_cash(monkeypatch, tmp_path):
    client = SequencedV2Client(
        {
            "broker.place_order": [
                {"order_id": "v2-B1"},
            ],
            "broker.orders": [
                [
                    {
                        "order_id": "v2-B1",
                        "security": "159915.SZ",
                        "side": "BUY",
                        "amount": 100,
                        "filled": 0,
                        "status": "open",
                        "style": {"type": "limit", "price": 3.247},
                    }
                ],
                [
                    {
                        "order_id": "v2-B1",
                        "security": "159915.SZ",
                        "side": "BUY",
                        "amount": 100,
                        "filled": 0,
                        "status": "rejected",
                        "raw_status": 57,
                        "style": {"type": "limit", "price": 3.247},
                    }
                ],
            ],
            "broker.trades": [
                [],
            ],
            "broker.account": [
                {
                    "available_cash": 100000.0,
                    "transferable_cash": 100000.0,
                    "frozen_cash": 0.0,
                    "total_asset": 100000.0,
                },
                {
                    "available_cash": 99675.3,
                    "transferable_cash": 99675.3,
                    "frozen_cash": 324.7,
                    "total_asset": 100000.0,
                },
                {
                    "available_cash": 100000.0,
                    "transferable_cash": 100000.0,
                    "frozen_cash": 0.0,
                    "total_asset": 100000.0,
                },
            ],
            "broker.positions": [
                [],
                [],
                [],
            ],
        }
    )
    broker = _build_v2_broker(client)
    engine = _build_v2_live_engine(tmp_path, broker)
    _prime_live_engine(engine, broker)
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 3.231
        high_limit = 3.500
        low_limit = 3.000

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.SZ": Snap()}
    )
    clear_order_queue()
    try:
        order_obj = await asyncio.to_thread(order, "159915.SZ", 100, LimitOrderStyle(3.247))
        assert order_obj is not None
        backing = engine.portfolio_proxy.backing
        assert backing.available_cash == pytest.approx(99675.3)
        assert backing.locked_cash == pytest.approx(324.7)
        assert backing.total_value == pytest.approx(100000.0)

        await engine._order_sync_step()
        await engine._account_sync_step()

        assert order_obj.status == OrderStatus.rejected
        assert backing.available_cash == pytest.approx(100000.0)
        assert backing.locked_cash == pytest.approx(0.0)
        assert backing.total_value == pytest.approx(100000.0)
        assert backing.positions == {}
    finally:
        clear_order_queue()
        set_current_engine(None)


@pytest.mark.asyncio
async def test_live_engine_v2_partial_fill_updates_cash_locked_and_positions(monkeypatch, tmp_path):
    client = SequencedV2Client(
        {
            "broker.place_order": [
                {"order_id": "v2-B2"},
            ],
            "broker.orders": [
                [
                    {
                        "order_id": "v2-B2",
                        "security": "159915.SZ",
                        "side": "BUY",
                        "amount": 30700,
                        "filled": 0,
                        "status": "open",
                        "style": {"type": "limit", "price": 3.247},
                    }
                ],
                [
                    {
                        "order_id": "v2-B2",
                        "security": "159915.SZ",
                        "side": "BUY",
                        "amount": 30700,
                        "filled": 15100,
                        "status": "partial_filled",
                        "avg_cost": 3.231,
                        "deal_balance": 48788.1,
                        "style": {"type": "limit", "price": 3.247},
                    }
                ],
            ],
            "broker.trades": [
                [],
            ],
            "broker.account": [
                {
                    "available_cash": 100000.0,
                    "transferable_cash": 100000.0,
                    "frozen_cash": 0.0,
                    "total_asset": 100000.0,
                },
                {
                    "available_cash": 317.1,
                    "transferable_cash": 317.1,
                    "frozen_cash": 99682.9,
                    "total_asset": 100000.0,
                },
                {
                    "available_cash": 558.7,
                    "transferable_cash": 558.7,
                    "frozen_cash": 50653.2,
                    "total_asset": 100000.0,
                },
            ],
            "broker.positions": [
                [],
                [],
                [
                    {
                        "security": "159915.SZ",
                        "amount": 15100,
                        "available_amount": 0,
                        "closeable_amount": 0,
                        "avg_cost": 3.231,
                        "last_price": 3.231,
                        "position_value": 48788.1,
                    }
                ],
            ],
        }
    )
    broker = _build_v2_broker(client)
    engine = _build_v2_live_engine(tmp_path, broker)
    _prime_live_engine(engine, broker)
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 3.231
        high_limit = 3.500
        low_limit = 3.000

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"159915.SZ": Snap()}
    )
    clear_order_queue()
    try:
        order_obj = await asyncio.to_thread(order, "159915.SZ", 30700, LimitOrderStyle(3.247))
        assert order_obj is not None
        backing = engine.portfolio_proxy.backing
        assert backing.available_cash == pytest.approx(317.1)
        assert backing.locked_cash == pytest.approx(99682.9)
        assert backing.total_value == pytest.approx(100000.0)

        await engine._order_sync_step()
        await engine._account_sync_step()

        assert order_obj.status == OrderStatus.filling
        assert order_obj.filled == 15100
        assert order_obj.price == pytest.approx(3.231)
        assert backing.available_cash == pytest.approx(558.7)
        assert backing.locked_cash == pytest.approx(50653.2)
        assert backing.total_value == pytest.approx(100000.0)
        position = backing.positions["159915.SZ"]
        assert position.total_amount == 15100
        assert position.closeable_amount == 0
        assert position.avg_cost == pytest.approx(3.231)
        stock_sub = backing.subportfolios["stock"]
        assert stock_sub.available_cash == pytest.approx(558.7)
        assert stock_sub.positions["159915.SZ"].total_amount == 15100
    finally:
        clear_order_queue()
        set_current_engine(None)


@pytest.mark.asyncio
async def test_process_orders_runs_once_with_lock(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = DummyBroker()
    engine._risk = None
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()}
    )

    clear_order_queue()
    order("000001.XSHE", 100, wait_timeout=0)

    task1 = asyncio.create_task(engine._process_orders(engine.context.current_dt))
    task2 = asyncio.create_task(engine._process_orders(engine.context.current_dt))
    await asyncio.gather(task1, task2)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert len(engine.broker.orders) == 1
    set_current_engine(None)


@pytest.mark.asyncio
async def test_order_waits_until_processed(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    gate = asyncio.Event()

    class SlowBroker(DummyBroker):
        def __init__(self, signal: asyncio.Event):
            super().__init__()
            self.signal = signal

        async def buy(
            self,
            security: str,
            amount: int,
            price: float | None = None,
            wait_timeout: float | None = None,
            remark: str | None = None,
            *,
            market: bool = False,
        ) -> str:
            await self.signal.wait()
            return await super().buy(
                security,
                amount,
                price,
                wait_timeout=wait_timeout,
                remark=remark,
                market=market,
            )

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=SlowBroker,
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = SlowBroker(gate)
    engine._risk = None
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()}
    )

    clear_order_queue()

    async def _run_order():
        return await asyncio.to_thread(order, "000001.XSHE", 100)

    order_task = asyncio.create_task(_run_order())
    await asyncio.sleep(0)
    assert len(engine.broker.orders) == 0
    gate.set()
    await order_task
    assert len(engine.broker.orders) == 1
    set_current_engine(None)


def test_live_engine_run_returns_nonzero_on_error(tmp_path, monkeypatch, caplog):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )

    async def _boom(self):
        raise RuntimeError("missing xtquant")

    monkeypatch.setattr(LiveEngine, "start", _boom)
    caplog.set_level("ERROR", logger="jq_strategy")
    exit_code = engine.run()
    assert exit_code == 2
    assert "missing xtquant" in caplog.text


def test_dummy_broker_lifecycle_hooks_default_to_noop():
    broker = DummyBroker()

    assert broker.before_open() is None
    assert broker.after_close() is None


@pytest.mark.asyncio
async def test_live_engine_triggers_broker_lifecycle_hooks_at_safe_markers(tmp_path):
    strategy = _write_strategy(tmp_path)
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=LifecycleBroker,
        live_config={"runtime_dir": str(tmp_path / "runtime")},
    )
    broker = LifecycleBroker()
    engine._loop = asyncio.get_running_loop()
    engine.event_bus = EventBus(engine._loop)
    engine.broker = broker
    engine.context.portfolio.total_value = 1_000_000
    current_day = date(2026, 4, 3)
    engine._pre_open_dt = datetime.combine(current_day, Time(9, 0))
    engine._open_dt = datetime.combine(current_day, Time(9, 30))
    engine._close_dt = datetime.combine(current_day, Time(15, 0))
    engine._post_close_dt = datetime.combine(current_day, Time(15, 31))

    await engine._maybe_emit_market_events(datetime.combine(current_day, Time(9, 0)))
    await engine._maybe_emit_market_events(datetime.combine(current_day, Time(15, 31)))

    assert broker.before_open_calls == 1
    assert broker.after_close_calls == 1


def test_live_engine_apply_account_snapshot_sets_position_buy_times(tmp_path):
    engine = _build_v2_live_engine(tmp_path, DummyBroker())
    target = (
        engine.portfolio_proxy.backing
        if isinstance(engine.context.portfolio, LivePortfolioProxy)
        else engine.context.portfolio
    )

    engine._apply_account_snapshot(
        {
            "available_cash": 452.0,
            "total_value": 20097.2,
            "positions": [
                {
                    "security": "159915.XSHE",
                    "amount": 5400,
                    "closeable_amount": 0,
                    "avg_cost": 3.620,
                    "current_price": 3.634,
                    "market_value": 19623.6,
                    "init_time": "2026-04-21T09:40:00",
                    "transact_time": "2026-04-21T10:36:00",
                }
            ],
        }
    )

    position = target.positions["159915.XSHE"]
    assert position.buy_time == datetime(2026, 4, 21, 9, 40, 0)
    assert position.last_buy_time == datetime(2026, 4, 21, 10, 36, 0)


@pytest.mark.asyncio
async def test_live_engine_does_not_backfill_broker_before_open_after_market_open(tmp_path):
    strategy = _write_strategy(tmp_path)
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=LifecycleBroker,
        live_config={"runtime_dir": str(tmp_path / "runtime")},
    )
    broker = LifecycleBroker()
    engine._loop = asyncio.get_running_loop()
    engine.event_bus = EventBus(engine._loop)
    engine.broker = broker
    engine.context.portfolio.total_value = 1_000_000
    current_day = date(2026, 4, 3)
    engine._pre_open_dt = datetime.combine(current_day, Time(9, 0))
    engine._open_dt = datetime.combine(current_day, Time(9, 30))
    engine._close_dt = datetime.combine(current_day, Time(15, 0))
    engine._post_close_dt = datetime.combine(current_day, Time(15, 31))

    await engine._maybe_emit_market_events(datetime.combine(current_day, Time(14, 0)))

    assert broker.before_open_calls == 0
    assert broker.after_close_calls == 0
