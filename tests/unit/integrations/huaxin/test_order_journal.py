"""
作者: BruceLee

文件职责: 验证华鑫 TORA OrderRef 水位、幂等占位和订单身份的崩溃恢复合同。
主要输入: 临时私有 SQLite 目录、合成登录 MaxOrderRef 与脱敏委托语义。
主要输出: 单调分配、同键不重发、指纹冲突、身份恢复和权限门禁断言。
上游关系: 覆盖 integrations.huaxin.order_journal 的公开合同。
下游关系: 不加载厂商 SDK、不联网、不创建真实交易会话。
关键配置: 所有测试数据库均位于 pytest 私有临时目录。
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from bullet_trade.integrations.huaxin.order_journal import ToraOrderIdentityJournal


def _private_dir(tmp_path: Path) -> Path:
    """把 pytest 临时目录收紧为 journal 所需私有权限。

    Args:
        tmp_path: pytest 创建的测试目录。

    Returns:
        Path: 权限为 0700 的同一目录。

    Side Effects:
        修改临时目录权限。
    """

    os.chmod(tmp_path, 0o700)
    return tmp_path


def _scope() -> str:
    """生成测试使用的稳定脱敏账户作用域。

    Returns:
        str: 64 位 SHA256 作用域。
    """

    return ToraOrderIdentityJournal.account_scope("account", "login", "tcp://front:6500")


def _fingerprint(side: str = "sell") -> str:
    """生成测试使用的委托语义指纹。

    Args:
        side: 委托方向。

    Returns:
        str: 64 位 SHA256 指纹。
    """

    return ToraOrderIdentityJournal.fingerprint(
        {
            "exchange": "SSE",
            "security": "511880",
            "side": side,
            "amount": 100,
            "style": "limit",
            "price": "100.001",
        }
    )


def test_claim_seeds_from_vendor_and_survives_restart(tmp_path: Path) -> None:
    """验证分配严格大于 MaxOrderRef 且重启后同键不会再次报单。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    path = _private_dir(tmp_path) / "orders.sqlite3"
    first = ToraOrderIdentityJournal(path)
    claim = first.claim(_scope(), "key-1", _fingerprint(), "huaxin:one", 48)
    assert claim.is_new is True
    assert claim.order_ref == 49
    assert claim.state == "prepared"
    assert oct(path.stat().st_mode & 0o777) == "0o600"

    restarted = ToraOrderIdentityJournal(path)
    duplicate = restarted.claim(_scope(), "key-1", _fingerprint(), "huaxin:one", 100)
    assert duplicate.is_new is False
    assert duplicate.order_ref == 49
    assert duplicate.state == "prepared"
    second = restarted.claim(_scope(), "key-2", _fingerprint("buy"), "huaxin:two", 100)
    assert second.order_ref == 101


def test_claim_rejects_same_key_with_different_fingerprint(tmp_path: Path) -> None:
    """验证同一幂等键不能绑定另一笔委托。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    journal = ToraOrderIdentityJournal(_private_dir(tmp_path) / "orders.sqlite3")
    journal.claim(_scope(), "same", _fingerprint("sell"), "huaxin:same", 0)
    with pytest.raises(ValueError, match="不同委托指纹"):
        journal.claim(_scope(), "same", _fingerprint("buy"), "huaxin:same", 0)


def test_submit_unknown_and_order_identity_are_recoverable(tmp_path: Path) -> None:
    """验证超时不重发并可在后续私有流回报中补齐撤单身份。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    path = _private_dir(tmp_path) / "orders.sqlite3"
    journal = ToraOrderIdentityJournal(path)
    claim = journal.claim(_scope(), "unknown", _fingerprint(), "huaxin:unknown", 7)
    unknown = journal.mark_result(
        _scope(),
        "unknown",
        _fingerprint(),
        "submit_unknown",
        {"status": "submit_unknown", "idempotency_key": "must-not-persist"},
    )
    assert unknown.state == "submit_unknown"
    assert "idempotency_key" not in unknown.result

    duplicate = ToraOrderIdentityJournal(path).claim(
        _scope(), "unknown", _fingerprint(), "huaxin:unknown", 999
    )
    assert duplicate.is_new is False
    assert duplicate.order_ref == claim.order_ref
    fact = journal.update_order_fact(
        _scope(),
        claim.order_ref,
        state="open",
        front_id=2,
        session_id=3,
        order_sys_id="sys-1",
        order_local_id="local-1",
    )
    assert fact is not None
    assert fact.state == "open"
    resolved = ToraOrderIdentityJournal(path).resolve_identity(_scope(), "huaxin:unknown")
    assert resolved is not None
    assert (resolved.front_id, resolved.session_id, resolved.order_sys_id) == (2, 3, "sys-1")
    by_ref = ToraOrderIdentityJournal(path).resolve_order_ref(_scope(), claim.order_ref)
    assert by_ref is not None
    assert by_ref.stable_local_order_id == "huaxin:unknown"


@pytest.mark.parametrize("session_id", (-1, -(1 << 31), (1 << 31) - 1))
def test_order_fact_persists_signed_int32_session_id(tmp_path: Path, session_id: int) -> None:
    """验证 journal 原样保存有符号 int32 SessionID。

    Args:
        tmp_path: pytest 临时目录。
        session_id: 待落盘的有符号 int32 边界值。

    Returns:
        None。
    """

    path = _private_dir(tmp_path) / "orders.sqlite3"
    journal = ToraOrderIdentityJournal(path)
    claim = journal.claim(_scope(), "signed-session", _fingerprint(), "huaxin:signed", 0)
    journal.update_order_fact(
        _scope(),
        claim.order_ref,
        state="open",
        front_id=7,
        session_id=session_id,
    )
    restored = ToraOrderIdentityJournal(path).resolve_identity(_scope(), "huaxin:signed")
    assert restored is not None
    assert restored.session_id == session_id


def test_concurrent_claims_allocate_unique_monotonic_refs(tmp_path: Path) -> None:
    """验证多连接并发占位不会重复分配 OrderRef。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    path = _private_dir(tmp_path) / "orders.sqlite3"
    ToraOrderIdentityJournal(path).seed(_scope(), 20)

    def claim(index: int) -> int:
        """在独立 journal 实例上占位并返回 OrderRef。

        Args:
            index: 并发任务编号。

        Returns:
            int: 原子分配的 OrderRef。
        """

        journal = ToraOrderIdentityJournal(path)
        item = journal.claim(
            _scope(),
            f"key-{index}",
            ToraOrderIdentityJournal.fingerprint({"index": index}),
            f"huaxin:{index}",
            20,
        )
        return item.order_ref

    with ThreadPoolExecutor(max_workers=4) as pool:
        refs = list(pool.map(claim, range(8)))
    assert sorted(refs) == list(range(21, 29))


def test_order_fact_cannot_regress_or_overwrite_terminal_state(tmp_path: Path) -> None:
    """验证乱序私有流回报不能让订单倒退或改写终态。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    journal = ToraOrderIdentityJournal(_private_dir(tmp_path) / "orders.sqlite3")
    claim = journal.claim(_scope(), "state", _fingerprint(), "huaxin:state", 0)
    journal.update_order_fact(_scope(), claim.order_ref, state="open")
    journal.update_order_fact(_scope(), claim.order_ref, state="filling")
    with pytest.raises(ValueError, match="状态禁止倒退"):
        journal.update_order_fact(_scope(), claim.order_ref, state="open")
    journal.update_order_fact(_scope(), claim.order_ref, state="filled")
    with pytest.raises(ValueError, match="终态禁止"):
        journal.update_order_fact(_scope(), claim.order_ref, state="canceled")


def test_journal_rejects_world_readable_parent(tmp_path: Path) -> None:
    """验证共享目录不能承载生产订单身份数据库。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    os.chmod(tmp_path, 0o755)
    with pytest.raises(ValueError, match="父目录不得授予"):
        ToraOrderIdentityJournal(tmp_path / "orders.sqlite3")
