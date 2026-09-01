"""
作者: BruceLee

文件职责: 验证华鑫节点快照 relay 的完整查询、持久 generation 与幂等安装。
主要输入: 内存 query-only Broker、临时 generation/快照路径和伪造异常载荷。
主要输出: schema v2 快照、0600 文件及明确 replay/conflict 错误。
上游关系: HuaxinBrokerAdapter 私有 relay 动作。
下游关系: external_snapshot consumer；测试不会访问网络或券商写接口。
关键配置: 所有身份均为测试值，文件只写 pytest 临时目录。
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import stat

import pytest

from bullet_trade.integrations.huaxin.snapshot_relay import (
    HuaxinNodeSnapshotRelay,
    HuaxinNodeSnapshotRelayConfig,
    HuaxinNodeSnapshotRelayError,
    _payload_sha256,
    _producer_sha256,
    validate_huaxin_relay_snapshot,
)


_ZONE = timezone(timedelta(hours=8))
_NOW = datetime(2026, 8, 26, 9, 5, tzinfo=_ZONE)


class _QueryOnlyBroker:
    """提供完整 query-end 结果且绝不暴露写方法的测试 Broker。"""

    def __init__(self, *, fail_transfer_query: bool = False) -> None:
        """保存查询失败开关。

        Args:
            fail_transfer_query: 是否模拟资金划拨明细查询失败。

        Returns:
            None。
        """

        self.fail_transfer_query = fail_transfer_query

    def get_system_nodes(self):
        """返回空节点目录以覆盖合规配置 fallback。

        Returns:
            list: 空目录。
        """

        return []

    def get_trading_day(self):
        """返回固定柜台交易日。

        Returns:
            str: 八位交易日。
        """

        return "20260826"

    def get_account_info(self):
        """返回完整资金划拨身份和余额。

        Returns:
            dict: 测试资金行。
        """

        return {
            "department_id": "D",
            "account_id": "A",
            "currency": "CNY",
            "available_cash": 1326.76,
            "transferable_cash": 1326.76,
            "frozen_cash": 0,
        }

    def get_shareholder_accounts(self, *, refresh=True):
        """返回完整股东账户集合。

        Args:
            refresh: 是否要求重新查询；测试只验证为 True。

        Returns:
            list: 单条测试股东账户。
        """

        assert refresh is True
        return [
            {
                "exchange": "SSE",
                "investor_id": "I",
                "business_unit_id": "B",
                "shareholder_id": "S",
            }
        ]

    def get_positions(self):
        """返回含全部划拨分量的持仓。

        Returns:
            list: 银华日利测试持仓。
        """

        return [
            {
                "exchange": "SSE",
                "security": "511880.XSHG",
                "current_position": 100,
                "available_position": 100,
                "history_position": 100,
                "onroad_position": 0,
                "investor_id": "I",
                "business_unit_id": "B",
                "shareholder_id": "S",
                "market_id": 49,
            }
        ]

    def get_fund_transfer_details(self, filters):
        """返回资金划拨明细或模拟 query-end 失败。

        Args:
            filters: 空过滤对象。

        Returns:
            list: 空未决明细。

        Raises:
            RuntimeError: 测试要求模拟失败时抛出。
        """

        assert filters == {}
        if self.fail_transfer_query:
            raise RuntimeError("fund detail query incomplete")
        return []

    def get_position_transfer_details(self, filters):
        """返回空证券划拨明细。

        Args:
            filters: 空过滤对象。

        Returns:
            list: 空未决明细。
        """

        assert filters == {}
        return []


def _capture_config(tmp_path) -> HuaxinNodeSnapshotRelayConfig:
    """构造源端快照配置。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        HuaxinNodeSnapshotRelayConfig: 启用 capture 的隔离配置。
    """

    return HuaxinNodeSnapshotRelayConfig.from_mapping(
        {
            "capture_enabled": True,
            "generation_state_path": tmp_path / "generation.json",
            "node_id": 16,
            "role": "JQ16_SOURCE",
            "host": "source-host",
            "producer_instance": "query-only-test",
            "producer_git_commit": "a" * 40,
        }
    )


def _capture(tmp_path):
    """生成一份可供安装测试使用的完整快照。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        dict: 已严格校验的 generation=1 快照。
    """

    relay = HuaxinNodeSnapshotRelay(
        _capture_config(tmp_path),
        clock=lambda: _NOW,
        hostname=lambda: "source-host",
    )
    return relay.capture(_QueryOnlyBroker())


def _install_config(tmp_path) -> HuaxinNodeSnapshotRelayConfig:
    """构造 consumer 幂等安装配置。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        HuaxinNodeSnapshotRelayConfig: 启用 install 的隔离配置。
    """

    return HuaxinNodeSnapshotRelayConfig.from_mapping(
        {
            "install_enabled": True,
            "install_path": tmp_path / "installed-source.json",
            "expected_source_node_id": 16,
            "expected_source_role": "JQ16_SOURCE",
            "expected_source_host": "source-host",
        }
    )


def test_capture_is_complete_and_generation_persists_across_instances(tmp_path) -> None:
    """验证全部查询完成后才发布，且跨 relay 实例 generation 严格递增。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    config = _capture_config(tmp_path)
    first = HuaxinNodeSnapshotRelay(
        config,
        clock=lambda: _NOW,
        hostname=lambda: "source-host",
    ).capture(_QueryOnlyBroker())
    second = HuaxinNodeSnapshotRelay(
        config,
        clock=lambda: _NOW + timedelta(seconds=5),
        hostname=lambda: "source-host",
    ).capture(_QueryOnlyBroker())

    validate_huaxin_relay_snapshot(first)
    validate_huaxin_relay_snapshot(second)
    assert first["generation"] == first["snapshot_generation"] == 1
    assert second["generation"] == second["snapshot_generation"] == 2
    assert first["query_provenance"]["query_end"]["fund_transfer_details"] is True
    assert first["transfer_details"] == {"fund": [], "position": []}
    assert first["node"]["provenance"] == "configured_session_fallback"
    assert stat.S_IMODE((tmp_path / "generation.json").stat().st_mode) == 0o600


def test_failed_query_does_not_advance_generation(tmp_path) -> None:
    """验证任一 query-end 失败时不发布也不消耗 generation。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    relay = HuaxinNodeSnapshotRelay(
        _capture_config(tmp_path),
        clock=lambda: _NOW,
        hostname=lambda: "source-host",
    )
    with pytest.raises(RuntimeError, match="incomplete"):
        relay.capture(_QueryOnlyBroker(fail_transfer_query=True))
    assert not (tmp_path / "generation.json").exists()

    snapshot = relay.capture(_QueryOnlyBroker())
    assert snapshot["generation"] == 1


def test_install_is_atomic_idempotent_and_rejects_replay_or_conflict(tmp_path) -> None:
    """验证新代次原子安装、同代同摘要 no-op、回放和冲突拒绝。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshot = _capture(tmp_path / "source")
    relay = HuaxinNodeSnapshotRelay(
        _install_config(tmp_path),
        clock=lambda: _NOW,
    )

    first = relay.install({"snapshot": snapshot}, trading_day="20260826")
    repeated = relay.install({"snapshot": snapshot}, trading_day="20260826")

    assert first["installed"] is True and first["noop"] is False
    assert repeated["installed"] is False and repeated["noop"] is True
    installed_path = tmp_path / "installed-source.json"
    assert stat.S_IMODE(installed_path.stat().st_mode) == 0o600
    assert json.loads(installed_path.read_text(encoding="utf-8")) == snapshot
    delayed_retry = HuaxinNodeSnapshotRelay(
        _install_config(tmp_path),
        clock=lambda: _NOW + timedelta(minutes=10),
    )
    assert delayed_retry.install(snapshot, trading_day="20260827")["noop"] is True

    newer_relay = HuaxinNodeSnapshotRelay(
        _capture_config(tmp_path / "source"),
        clock=lambda: _NOW + timedelta(seconds=5),
        hostname=lambda: "source-host",
    )
    newer = newer_relay.capture(_QueryOnlyBroker())
    assert relay.install(newer, trading_day="20260826")["generation"] == 2
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="replayed"):
        relay.install(snapshot, trading_day="20260826")

    conflict = deepcopy(newer)
    conflict["transfer_details"]["fund"] = [{"apply_serial": 1, "transfer_status": "pending"}]
    conflict["payload_digest_sha256"] = _payload_sha256(conflict)
    validate_huaxin_relay_snapshot(conflict)
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="conflict"):
        relay.install(conflict, trading_day="20260826")


def test_install_rejects_disabled_incomplete_and_wrong_identity(tmp_path) -> None:
    """验证安装开关、完整性和私密允许身份均失败关闭。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshot = _capture(tmp_path / "source")
    disabled = HuaxinNodeSnapshotRelay(HuaxinNodeSnapshotRelayConfig())
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="disabled"):
        disabled.install(snapshot, trading_day="20260826")

    relay = HuaxinNodeSnapshotRelay(
        _install_config(tmp_path),
        clock=lambda: _NOW,
    )
    incomplete = deepcopy(snapshot)
    incomplete.pop("transfer_details")
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="transfer_details"):
        relay.install(incomplete, trading_day="20260826")
    wrong_host = deepcopy(snapshot)
    wrong_host["host"] = wrong_host["host_id"] = "unexpected-host"
    wrong_host["payload_digest_sha256"] = _payload_sha256(wrong_host)
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="host_mismatch"):
        relay.install(wrong_host, trading_day="20260826")
    assert not (tmp_path / "installed-source.json").exists()

    with pytest.raises(HuaxinNodeSnapshotRelayError, match="trading_day_mismatch"):
        relay.install(snapshot, trading_day="20260827")

    forbidden = deepcopy(snapshot)
    forbidden["account"]["password"] = "must-not-leave-source"
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="forbidden_field"):
        relay.install(forbidden, trading_day="20260826")

    stale = HuaxinNodeSnapshotRelay(
        _install_config(tmp_path),
        clock=lambda: _NOW + timedelta(seconds=121),
    )
    with pytest.raises(HuaxinNodeSnapshotRelayError, match="stale"):
        stale.install(snapshot, trading_day="20260826")
    assert not (tmp_path / "installed-source.json").exists()


def test_install_accepts_new_producer_version_without_consumer_allowlist(tmp_path) -> None:
    """验证同一固定来源升级 producer 后无需修改 consumer 配置。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshot = _capture(tmp_path / "source")
    snapshot["producer"] = {
        "schema": snapshot["producer"]["schema"],
        "instance_id": "query-only-upgraded",
        "git_commit": "b" * 40,
    }
    snapshot["producer_sha256"] = _producer_sha256(snapshot["producer"])
    snapshot["payload_digest_sha256"] = _payload_sha256(snapshot)

    result = HuaxinNodeSnapshotRelay(
        _install_config(tmp_path),
        clock=lambda: _NOW,
    ).install(snapshot, trading_day="20260826")

    assert result["installed"] is True
