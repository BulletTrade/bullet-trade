"""
作者: BruceLee
日期: 2026-08-31
文件职责:
    验证远程 Server 的交易写只使用进程内幂等状态，不创建或依赖 SQLite journal。
主要输入:
    带稳定 idempotency_key 的下单/撤单请求、多个虚拟子账户和可控假 Broker。
主要输出:
    native writer 调用次数、重复请求结果、冲突错误和 health 幂等能力断言。
上下游关系:
    上游模拟 V2 Gateway；下游是假 Broker，不访问真实行情、账户或交易柜台。
关键环境约定:
    旧 journal/TTL 环境变量即使残留也必须被忽略，测试不得创建 SQLite 文件。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest

from bullet_trade.server.adapters.base import AccountContext, AccountRouter, AdapterBundle
from bullet_trade.server.app import IdempotencyConflictError, ServerApplication
from bullet_trade.server.config import (
    AccountConfig,
    ServerConfig,
    SubAccountConfig,
    build_server_config,
)


class _Session:
    """提供 Server broker 分发所需的最小会话身份。"""

    account_key = "default"
    sub_account_id = None


class _MemoryBroker:
    """记录 native writer 调用次数并返回确定性结果的假 Broker。"""

    def __init__(self) -> None:
        """初始化调用记录与可选并发阻塞事件。

        Returns:
            None。
        """

        self.place_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.entered: Optional[asyncio.Event] = None
        self.release: Optional[asyncio.Event] = None
        self.raise_after_claim: Optional[Exception] = None

    async def place_order(
        self,
        account: AccountContext,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """记录一次下单并返回稳定柜台订单号。

        Args:
            account: Server 已解析的父账户上下文。
            payload: 原始下单请求。

        Returns:
            Dict[str, Any]: 已受理订单结果。

        Raises:
            Exception: 测试显式配置异常时原样抛出。
        """

        self.place_calls.append(
            {
                "account_key": account.config.key,
                "sub_account_id": payload.get("sub_account_id"),
                "idempotency_key": payload.get("idempotency_key"),
            }
        )
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.raise_after_claim is not None:
            raise self.raise_after_claim
        return {
            "order_id": f"qmt-{len(self.place_calls)}",
            "status": "submitted",
        }

    async def cancel_order_request(
        self,
        account: AccountContext,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """记录一次撤单请求。

        Args:
            account: Server 已解析的父账户上下文。
            payload: 包含订单号和幂等键的撤单请求。

        Returns:
            Dict[str, Any]: 明确受理的撤单结果。
        """

        self.cancel_calls.append(
            {
                "account_key": account.config.key,
                "order_id": payload.get("order_id"),
                "idempotency_key": payload.get("idempotency_key"),
            }
        )
        return {"value": True, "status": "canceled"}

    async def list_orders(
        self,
        account: AccountContext,
        filters: Optional[Dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """返回测试预置的只读柜台订单列表。

        Args:
            account: Server 已解析的父账户上下文。
            filters: Server 提供的只读过滤条件。

        Returns:
            list[dict[str, Any]]: 订单列表副本。
        """

        _ = account, filters
        return [dict(row) for row in self.orders]


def _build_app(
    *,
    broker: Optional[_MemoryBroker] = None,
    sub_accounts: Optional[list[SubAccountConfig]] = None,
) -> tuple[ServerApplication, _MemoryBroker]:
    """构造启用真实写幂等键要求的内存 Server。

    Args:
        broker: 可选的预置假 Broker。
        sub_accounts: 可选虚拟子账户路由。

    Returns:
        tuple[ServerApplication, _MemoryBroker]: Server 与其假 Broker。
    """

    config = ServerConfig(
        server_type="qmt",
        listen="127.0.0.1",
        port=0,
        token="test-token",
        enable_data=False,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="parent")],
        sub_accounts=list(sub_accounts or []),
    )
    router = AccountRouter(config.accounts)
    fake = broker or _MemoryBroker()
    app = ServerApplication(
        config,
        router,
        AdapterBundle(data_adapter=None, broker_adapter=fake),
    )
    return app, fake


def _place_payload(
    key: str,
    *,
    sub_account_id: Optional[str] = None,
    amount: int = 100,
) -> Dict[str, Any]:
    """构造不会触发行情补价的限价卖单。

    Args:
        key: 稳定幂等键。
        sub_account_id: 可选虚拟子账户。
        amount: 委托数量。

    Returns:
        Dict[str, Any]: Server 可直接分发的请求。
    """

    payload: Dict[str, Any] = {
        "account_key": "default",
        "security": "159967.XSHE",
        "side": "SELL",
        "amount": amount,
        "style": {"type": "limit", "price": 0.77},
        "idempotency_key": key,
    }
    if sub_account_id:
        payload["sub_account_id"] = sub_account_id
    return payload


@pytest.mark.asyncio
async def test_server_writes_without_journal_configuration() -> None:
    """没有任何 journal 路径时，下单仍应进入一次 native writer。"""

    app, broker = _build_app()

    first = await app._dispatch_broker(_Session(), "place_order", _place_payload("no-journal"))
    repeated = await app._dispatch_broker(
        _Session(),
        "place_order",
        _place_payload("no-journal"),
    )

    assert first == repeated
    assert first["order_id"] == "qmt-1"
    assert len(broker.place_calls) == 1
    health = app._health_snapshot()["value"]
    assert health["idempotency"] == {
        "mode": "process_memory",
        "key_required": True,
        "entries": 1,
        "cross_restart_exactly_once": False,
        "unknown_auto_resend": False,
    }
    assert "idempotency_journal" not in health


@pytest.mark.asyncio
async def test_legacy_journal_environment_is_ignored(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """残留旧环境变量不得进入配置或创建 SQLite 文件。

    Args:
        monkeypatch: pytest 环境变量替换工具。
        tmp_path: 隔离临时目录。

    Returns:
        None。
    """

    journal_path = tmp_path / "must-not-exist.sqlite3"
    monkeypatch.setenv("QMT_SERVER_IDEMPOTENCY_JOURNAL_PATH", str(journal_path))
    monkeypatch.setenv("QMT_SERVER_IDEMPOTENCY_JOURNAL_MAX_ENTRIES", "1")
    monkeypatch.setenv("QMT_SERVER_IDEMPOTENCY_TTL_SECONDS", "0")

    loaded = build_server_config(SimpleNamespace())
    app, broker = _build_app()
    await app._dispatch_broker(_Session(), "place_order", _place_payload("legacy-env"))

    assert not hasattr(loaded, "idempotency_journal_path")
    assert not hasattr(loaded, "idempotency_journal_max_entries")
    assert not hasattr(loaded, "idempotency_ttl_seconds")
    assert len(broker.place_calls) == 1
    assert journal_path.exists() is False


@pytest.mark.asyncio
async def test_multiple_virtual_accounts_share_one_process_cache() -> None:
    """多个虚拟账户共享同一个 Server，不需要各自持久数据库。"""

    app, broker = _build_app(
        sub_accounts=[
            SubAccountConfig(sub_account_id="b4-a", account_key="default"),
            SubAccountConfig(sub_account_id="b4-b", account_key="default"),
        ]
    )

    await app._dispatch_broker(
        _Session(),
        "place_order",
        _place_payload("virtual-a", sub_account_id="b4-a@default"),
    )
    await app._dispatch_broker(
        _Session(),
        "place_order",
        _place_payload("virtual-b", sub_account_id="b4-b@default"),
    )
    await app._dispatch_broker(
        _Session(),
        "place_order",
        _place_payload("virtual-a", sub_account_id="b4-a@default"),
    )

    assert len(broker.place_calls) == 2
    assert {row["sub_account_id"] for row in broker.place_calls} == {
        "b4-a@default",
        "b4-b@default",
    }
    assert len(app._idempotency_cache) == 2


@pytest.mark.asyncio
async def test_process_memory_entry_never_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """超过旧 300 秒窗口后，同键仍不得再次下单。

    Args:
        monkeypatch: pytest 时间替换工具。

    Returns:
        None。
    """

    app, broker = _build_app()
    clock = {"value": 100.0}
    monkeypatch.setattr("bullet_trade.server.app.time.monotonic", lambda: clock["value"])

    first = await app._dispatch_broker(_Session(), "place_order", _place_payload("no-expiry"))
    clock["value"] += 86400.0
    repeated = await app._dispatch_broker(
        _Session(),
        "place_order",
        _place_payload("no-expiry"),
    )

    assert first == repeated
    assert len(broker.place_calls) == 1


@pytest.mark.asyncio
async def test_concurrent_same_key_enters_native_writer_once() -> None:
    """并发同键请求必须由第一个占位，第二个立即得到 unknown。"""

    broker = _MemoryBroker()
    broker.entered = asyncio.Event()
    broker.release = asyncio.Event()
    app, broker = _build_app(broker=broker)
    payload = _place_payload("concurrent-once")

    first_task = asyncio.create_task(app._dispatch_broker(_Session(), "place_order", dict(payload)))
    await broker.entered.wait()
    repeated = await app._dispatch_broker(_Session(), "place_order", dict(payload))
    broker.release.set()
    first = await first_task

    assert repeated["status"] == "submit_unknown"
    assert first["order_id"] == "qmt-1"
    assert len(broker.place_calls) == 1
    final = await app._dispatch_broker(_Session(), "place_order", dict(payload))
    assert final == first
    assert len(broker.place_calls) == 1


@pytest.mark.asyncio
async def test_same_key_with_changed_payload_is_rejected() -> None:
    """同键不同数量必须冲突且不得增加 native writer 调用。"""

    app, broker = _build_app()
    await app._dispatch_broker(_Session(), "place_order", _place_payload("conflict"))

    with pytest.raises(IdempotencyConflictError, match="冲突"):
        await app._dispatch_broker(
            _Session(),
            "place_order",
            _place_payload("conflict", amount=200),
        )

    assert len(broker.place_calls) == 1


@pytest.mark.asyncio
async def test_unknown_result_is_never_auto_retried() -> None:
    """writer 抛出不确定异常后，同键只返回 submit_unknown。"""

    broker = _MemoryBroker()
    broker.raise_after_claim = TimeoutError("response lost")
    app, broker = _build_app(broker=broker)
    payload = _place_payload("response-lost")

    with pytest.raises(TimeoutError, match="response lost"):
        await app._dispatch_broker(_Session(), "place_order", dict(payload))
    broker.raise_after_claim = None
    repeated = await app._dispatch_broker(_Session(), "place_order", dict(payload))

    assert repeated["status"] == "submit_unknown"
    assert repeated["submission_state"] == "submit_unknown"
    assert len(broker.place_calls) == 1


@pytest.mark.asyncio
async def test_new_process_resolve_without_strong_evidence_stays_unknown() -> None:
    """模拟重启后内存为空且柜台无强证据时，只读解析不得触发补单。"""

    app, broker = _build_app()
    original = _place_payload("restart-unknown")

    resolved = await app._dispatch_broker(
        _Session(),
        "resolve_submission",
        {
            "account_key": "default",
            "idempotency_key": "restart-unknown",
            "write_action": "broker.place_order",
            "request_payload": original,
        },
    )

    assert resolved["status"] == "submit_unknown"
    assert resolved["reason"] == "no_strong_order_evidence"
    assert broker.place_calls == []
