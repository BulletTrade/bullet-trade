"""
作者: BruceLee
文件说明:
    远程交易写入持久幂等日志的确定性回归测试。
    输入为内存 fake broker、临时 journal 与同一幂等键；输出为重启后不重发、冲突拒绝和撤单语义证据。
    覆盖 ServerApplication 调用真实 adapter 前的持久占位，不依赖网络或真实券商。
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from types import SimpleNamespace

import pytest

from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.stub import build_stub_bundle
from bullet_trade.server.app import IdempotencyConflictError, ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig
from bullet_trade.server.idempotency_journal import PersistentIdempotencyJournal


class _CountingBroker:
    """记录写调用次数并提供受控订单事实的无网络 broker。"""

    def __init__(
        self,
        *,
        cancel_status: str = "canceled",
        orders=None,
        place_started=None,
        place_release=None,
    ) -> None:
        """初始化 fake broker。

        Args:
            cancel_status: cancel_order 返回的目标订单状态。
            orders: list_orders 返回的预置订单事实。
            place_started: 可选事件；进入 place_order 时置位。
            place_release: 可选事件；place_order 返回前等待其置位。

        Returns:
            None。
        """

        self.place_calls = 0
        self.place_payloads = []
        self.cancel_calls = 0
        self.cancel_status = cancel_status
        self.orders = list(orders or [])
        self.place_started = place_started
        self.place_release = place_release

    async def place_order(self, _account, payload):
        """记录一次下单并返回带完整身份的确定性订单。

        Args:
            _account: 未使用的账户上下文。
            payload: server 下发的订单请求。

        Returns:
            dict: 可用于 client resolve 校验的订单事实。
        """

        self.place_calls += 1
        self.place_payloads.append(dict(payload))
        if self.place_started is not None:
            self.place_started.set()
        if self.place_release is not None:
            await self.place_release.wait()
        return {
            "order_id": f"journal-{self.place_calls}",
            "status": "open",
            "security": payload["security"],
            "side": payload["side"],
            "amount": payload["amount"],
            "idempotency_key": payload["idempotency_key"],
        }

    async def cancel_order(self, _account, order_id):
        """记录一次撤单并返回目标订单快照。

        Args:
            _account: 未使用的账户上下文。
            order_id: 请求撤销的精确订单号。

        Returns:
            dict: 包含 last_snapshot 的兼容撤单响应。
        """

        self.cancel_calls += 1
        return {
            "value": True,
            "last_snapshot": {"order_id": order_id, "status": self.cancel_status},
        }

    async def list_orders(self, _account, _filters=None):
        """返回预置订单，用于 resolve 的反向单红队场景。

        Args:
            _account: 未使用的账户上下文。
            _filters: 未使用的查询过滤条件。

        Returns:
            list: 订单事实列表。
        """

        return [dict(item) for item in self.orders]

    async def get_order_status(self, _account, order_id):
        """按精确订单号返回预置订单。

        Args:
            _account: 未使用的账户上下文。
            order_id: 精确订单号。

        Returns:
            dict: 命中订单或空字典。
        """

        for item in self.orders:
            if item.get("order_id") == order_id:
                return dict(item)
        return {}


def _build_app(tmp_path, broker, *, server_type="qmt"):
    """构造使用临时持久 journal 的独立 server application。

    Args:
        tmp_path: pytest 临时目录。
        broker: fake broker 实例。
        server_type: server 类型，默认模拟交易写路径。

    Returns:
        ServerApplication: 不启动 listener 的可直接 dispatch 实例。
    """

    config = ServerConfig(
        server_type=server_type,
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        idempotency_journal_path=str(tmp_path / "idempotency.sqlite3"),
        idempotency_journal_max_entries=4,
    )
    router = AccountRouter(config.accounts)
    return ServerApplication(
        config, router, AdapterBundle(data_adapter=None, broker_adapter=broker)
    )


def _payload(*, amount=100):
    """生成带执行 binding 冻结证据的规范化下单请求。

    Args:
        amount: 下单数量。

    Returns:
        dict: 供 server dispatch 使用的下单请求。
    """

    return {
        "security": "511880.XSHG",
        "side": "BUY",
        "amount": amount,
        "style": {"type": "limit", "price": 100.0},
        "idempotency_key": "journal-same-key",
        "execution_claim_token": "claim-1",
        "execution_claim_generation": 1,
        "gateway_id_snapshot": 7,
        "sub_account_binding_id_snapshot": 69,
        "backend_provider": "bullet_trade_remote",
        "binding_version": "a" * 64,
    }


@pytest.mark.asyncio
async def test_persistent_journal_prevents_resend_after_restart_and_binding_conflict(tmp_path):
    """重启后同键不重发，变更任何冻结 binding 字段必须稳定冲突。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    first_broker = _CountingBroker()
    first_app = _build_app(tmp_path, first_broker)
    first = await first_app._dispatch_broker(session, "place_order", _payload())

    second_broker = _CountingBroker()
    restarted_app = _build_app(tmp_path, second_broker)
    repeated = await restarted_app._dispatch_broker(session, "place_order", _payload())
    assert first["order_id"] == repeated["order_id"]
    assert first_broker.place_calls == 1
    assert second_broker.place_calls == 0
    assert {
        key: first_broker.place_payloads[0][key]
        for key in (
            "execution_claim_token",
            "execution_claim_generation",
            "gateway_id_snapshot",
            "sub_account_binding_id_snapshot",
            "backend_provider",
            "binding_version",
        )
    } == {
        "execution_claim_token": "claim-1",
        "execution_claim_generation": 1,
        "gateway_id_snapshot": 7,
        "sub_account_binding_id_snapshot": 69,
        "backend_provider": "bullet_trade_remote",
        "binding_version": "a" * 64,
    }
    drift_values = {
        "amount": 200,
        "execution_claim_token": "claim-2",
        "execution_claim_generation": 2,
        "gateway_id_snapshot": 8,
        "sub_account_binding_id_snapshot": 70,
        "backend_provider": "other_backend",
        "binding_version": "b" * 64,
    }
    for field, value in drift_values.items():
        conflicting = _payload()
        conflicting[field] = value
        with pytest.raises(IdempotencyConflictError):
            await restarted_app._dispatch_broker(session, "place_order", conflicting)
    with pytest.raises(IdempotencyConflictError):
        await restarted_app._dispatch_broker(
            session,
            "cancel_order",
            {
                "order_id": first["order_id"],
                "idempotency_key": "journal-same-key",
            },
        )
    assert second_broker.place_calls == 0
    assert second_broker.cancel_calls == 0


@pytest.mark.asyncio
async def test_two_prestarted_apps_share_one_atomic_claim(tmp_path):
    """滚动重启的两个进程共享同一数据库时，同键最多一个 adapter 写调用。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    first_started = asyncio.Event()
    first_release = asyncio.Event()
    first_broker = _CountingBroker(
        place_started=first_started,
        place_release=first_release,
    )
    second_broker = _CountingBroker()
    first_app = _build_app(tmp_path, first_broker)
    second_app = _build_app(tmp_path, second_broker)

    first_task = asyncio.create_task(first_app._dispatch_broker(session, "place_order", _payload()))
    await first_started.wait()
    try:
        pending = await second_app._dispatch_broker(session, "place_order", _payload())
    finally:
        first_release.set()
    first = await first_task
    repeated = await second_app._dispatch_broker(session, "place_order", _payload())

    assert pending["status"] == "submit_unknown"
    assert first["order_id"] == repeated["order_id"]
    assert first_broker.place_calls + second_broker.place_calls == 1
    assert first_app._idempotency_journal.count() == 1


@pytest.mark.asyncio
async def test_sqlite_journal_survives_in_memory_ttl_expiry(tmp_path):
    """进程内 TTL 到期只能清缓存，不能让同键再次调用 adapter。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    broker = _CountingBroker()
    app = _build_app(tmp_path, broker)
    first = await app._dispatch_broker(session, "place_order", _payload())
    cache_key = app._idempotency_cache_key("default", None, "journal-same-key")
    app._idempotency_cache[cache_key].expires_at = 0
    app._purge_expired_idempotency_entries(1)

    repeated = await app._dispatch_broker(session, "place_order", _payload())

    assert first["order_id"] == repeated["order_id"]
    assert broker.place_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "server_type",
    ["qmt", " QMT ", "big_qmt", "big-qmt", " BIG_QMT ", "huaxin", "custom-real"],
)
async def test_adapter_capability_cannot_be_bypassed_by_server_type_alias(tmp_path, server_type):
    """真实 broker 默认要求 journal，名称别名、大小写或第三方类型不能绕过。"""

    broker = _CountingBroker()
    config = ServerConfig(
        server_type=server_type,
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data_adapter=None, broker_adapter=broker),
    )

    with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
        await app._dispatch_broker(
            SimpleNamespace(account_key="default", sub_account_id=None),
            "place_order",
            _payload(),
        )
    assert broker.place_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_capability", [None, 0, "", "test-only"])
async def test_invalid_adapter_capability_values_fail_closed(invalid_capability):
    """除字面量 False 外的异常 capability 值必须继续要求持久 journal。"""

    broker = _CountingBroker()
    config = ServerConfig(
        server_type="unknown-third-party",
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(
            data_adapter=None,
            broker_adapter=broker,
            broker_writes_require_persistent_idempotency=invalid_capability,
        ),
    )

    with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
        await app._dispatch_broker(
            SimpleNamespace(account_key="default", sub_account_id=None),
            "place_order",
            _payload(),
        )
    assert broker.place_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["place_order", "cancel_order"])
@pytest.mark.parametrize(
    "bad_key",
    [None, "", " leading-space", "contains space", "x" * 256, "中文键"],
)
async def test_required_write_rejects_missing_or_invalid_idempotency_key(tmp_path, method, bad_key):
    """真实 adapter 在任何调用前拒绝缺失、超长或非安全格式的幂等键。"""

    broker = _CountingBroker()
    app = _build_app(tmp_path, broker)
    payload = (
        _payload()
        if method == "place_order"
        else {"order_id": "cancel-target", "idempotency_key": "valid-cancel-key"}
    )
    if bad_key is None:
        payload.pop("idempotency_key")
    else:
        payload["idempotency_key"] = bad_key

    with pytest.raises(ValueError, match="idempotency_key"):
        await app._dispatch_broker(
            SimpleNamespace(account_key="default", sub_account_id=None),
            method,
            payload,
        )
    assert broker.place_calls + broker.cancel_calls == 0
    assert app._idempotency_journal.count() == 0


@pytest.mark.asyncio
async def test_stub_explicitly_allows_legacy_writes_without_journal_or_key(tmp_path):
    """显式 test-only stub 豁免持久账本，并保持旧的无 key 测试边界。"""

    config = ServerConfig(
        server_type="stub",
        token="unit-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    router = AccountRouter(config.accounts)
    app = ServerApplication(config, router, build_stub_bundle(config, router))
    payload = _payload()
    payload.pop("idempotency_key")

    result = await app._dispatch_broker(
        SimpleNamespace(account_key="default", sub_account_id=None),
        "place_order",
        payload,
    )

    assert result["order_id"] == "stub-1"
    assert app._idempotency_journal_required is False
    assert app._idempotency_journal is None


@pytest.mark.asyncio
async def test_cancel_filled_is_not_reported_as_cancel_success(tmp_path):
    """已成交目标的撤单响应必须明确 false，而不是兼容层的裸 value=true。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    broker = _CountingBroker(cancel_status="filled")
    app = _build_app(tmp_path, broker)
    result = await app._dispatch_broker(
        session,
        "cancel_order",
        {"order_id": "exact-order", "idempotency_key": "cancel-filled"},
    )

    assert result["value"] is False
    assert result["cancel_outcome"] == "not_canceled_already_terminal"
    assert result["order_id"] == "exact-order"


@pytest.mark.asyncio
async def test_cancel_open_or_timeout_remains_submit_unknown(tmp_path):
    """撤单只返回 open 的等待结果没有取消证据，必须保留 submit_unknown。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    broker = _CountingBroker(cancel_status="open")
    app = _build_app(tmp_path, broker)
    result = await app._dispatch_broker(
        session,
        "cancel_order",
        {"order_id": "still-open", "idempotency_key": "cancel-open"},
    )

    assert result["status"] == "submit_unknown"
    assert result["order_id"] == "still-open"


@pytest.mark.asyncio
async def test_resolve_rejects_reverse_order_even_when_key_matches(tmp_path):
    """同幂等键但反向 SELL 订单不能作为 BUY 下单的只读确认。"""

    payload = _payload()
    broker = _CountingBroker(
        orders=[
            {
                "order_id": "reverse-order",
                "security": payload["security"],
                "side": "SELL",
                "amount": payload["amount"],
                "idempotency_key": payload["idempotency_key"],
                "status": "open",
            }
        ]
    )
    app = _build_app(tmp_path, broker, server_type="stub")
    session = SimpleNamespace(account_key="default", sub_account_id=None)
    result = await app._dispatch_broker(
        session,
        "resolve_submission",
        {
            "idempotency_key": payload["idempotency_key"],
            "write_action": "broker.place_order",
            "request_payload": payload,
        },
    )

    assert result["status"] == "submit_unknown"


@pytest.mark.asyncio
async def test_journal_capacity_and_corruption_fail_closed_before_broker_write(tmp_path):
    """日志满或恢复文件损坏时，新的交易写必须在 adapter 前被拒绝。"""

    session = SimpleNamespace(account_key="default", sub_account_id=None)
    broker = _CountingBroker()
    app = _build_app(tmp_path, broker)
    app._idempotency_journal.max_entries = 1
    await app._dispatch_broker(session, "place_order", _payload())
    second = _payload()
    second["idempotency_key"] = "journal-capacity-key"
    with pytest.raises(RuntimeError, match="容量已满"):
        await app._dispatch_broker(session, "place_order", second)
    assert broker.place_calls == 1

    journal_path = tmp_path / "corrupt-idempotency.sqlite3"
    journal_path.write_text("not-json", encoding="utf-8")
    journal_path.chmod(0o600)
    config = ServerConfig(
        server_type="qmt",
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        idempotency_journal_path=str(journal_path),
    )
    corrupt_broker = _CountingBroker()
    corrupt_app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data_adapter=None, broker_adapter=corrupt_broker),
    )
    with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
        await corrupt_app._dispatch_broker(session, "place_order", _payload())
    assert corrupt_broker.place_calls == 0


@pytest.mark.asyncio
async def test_journal_unsafe_file_permission_fails_closed(tmp_path):
    """POSIX 上已有 journal 权限过宽时，生产写入必须在 adapter 前失败。"""

    journal_path = tmp_path / "unsafe-idempotency.sqlite3"
    journal_path.write_text('{"version":1,"entries":{}}', encoding="utf-8")
    journal_path.chmod(0o644)
    config = ServerConfig(
        server_type="qmt",
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        idempotency_journal_path=str(journal_path),
    )
    broker = _CountingBroker()
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data_adapter=None, broker_adapter=broker),
    )
    with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
        await app._dispatch_broker(
            SimpleNamespace(account_key="default", sub_account_id=None), "place_order", _payload()
        )
    assert broker.place_calls == 0


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode 合同")
@pytest.mark.asyncio
async def test_journal_creates_0600_file_and_rejects_unsafe_parent(tmp_path):
    """新数据库必须为 0600，组或其他用户可写父目录必须 fail-closed。"""

    safe_broker = _CountingBroker()
    safe_app = _build_app(tmp_path, safe_broker)
    assert safe_app._idempotency_journal.path.stat().st_mode & 0o777 == 0o600

    unsafe_parent = tmp_path / "unsafe-parent"
    unsafe_parent.mkdir(mode=0o777)
    unsafe_parent.chmod(0o777)
    config = ServerConfig(
        server_type="qmt",
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        idempotency_journal_path=str(unsafe_parent / "journal.sqlite3"),
    )
    broker = _CountingBroker()
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data_adapter=None, broker_adapter=broker),
    )
    try:
        with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
            await app._dispatch_broker(
                SimpleNamespace(account_key="default", sub_account_id=None),
                "place_order",
                _payload(),
            )
    finally:
        unsafe_parent.chmod(0o700)
    assert broker.place_calls == 0


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink 与 mode 合同")
@pytest.mark.asyncio
async def test_journal_rejects_configured_final_symlink_before_broker_write(tmp_path):
    """最终配置路径为符号链接时必须拒绝，不能解析后改写目标文件。"""

    target = tmp_path / "target.sqlite3"
    target.touch(mode=0o600)
    configured = tmp_path / "configured.sqlite3"
    configured.symlink_to(target)
    config = ServerConfig(
        server_type="qmt",
        token="unit-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
        idempotency_journal_path=str(configured),
    )
    broker = _CountingBroker()
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data_adapter=None, broker_adapter=broker),
    )

    with pytest.raises(RuntimeError, match="持久幂等日志不可用"):
        await app._dispatch_broker(
            SimpleNamespace(account_key="default", sub_account_id=None),
            "place_order",
            _payload(),
        )
    assert broker.place_calls == 0
    assert target.stat().st_size == 0


@pytest.mark.asyncio
async def test_sqlite_lock_timeout_fails_closed_before_broker_write(tmp_path, monkeypatch):
    """数据库被其他进程长期占锁时，新写在有限等待后 fail-closed。"""

    broker = _CountingBroker()
    app = _build_app(tmp_path, broker)
    monkeypatch.setattr(PersistentIdempotencyJournal, "_BUSY_TIMEOUT_SECONDS", 0.01)
    lock = sqlite3.connect(str(app._idempotency_journal.path), isolation_level=None)
    lock.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(RuntimeError, match="幂等持久日志失败"):
            await app._dispatch_broker(
                SimpleNamespace(account_key="default", sub_account_id=None),
                "place_order",
                _payload(),
            )
    finally:
        lock.rollback()
        lock.close()
    assert broker.place_calls == 0
