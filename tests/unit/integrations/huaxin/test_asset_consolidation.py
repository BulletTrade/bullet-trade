"""验证华鑫节点资产归集的持久状态、零重试和双端对账。"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bullet_trade.integrations.huaxin.asset_consolidation import (
    HUAXIN_NODE16_READY_SCHEMA,
    HuaxinAssetConsolidationConfig,
    HuaxinAssetConsolidationCoordinator,
    HuaxinAssetConsolidationStateStore,
    build_huaxin_node_asset_snapshot_digest,
)


_ZONE = timezone(timedelta(hours=8))
_NOW = datetime(2026, 8, 25, 9, 10, tzinfo=_ZONE)


def _seal_source_snapshot(snapshot):
    """为测试源快照重算生产者与资产摘要。

    Args:
        snapshot: 待封装的完整源快照。

    Returns:
        dict: 写入摘要后的原对象。
    """

    producer_material = json.dumps(
        snapshot["producer"],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    snapshot["producer_sha256"] = hashlib.sha256(producer_material).hexdigest()
    snapshot["snapshot_id"] = build_huaxin_node_asset_snapshot_digest(snapshot)
    snapshot["snapshot_digest_sha256"] = snapshot["snapshot_id"]
    return snapshot


def _config(tmp_path, mode="full"):
    """构造不含生产身份的隔离归集配置。

    Args:
        tmp_path: pytest 临时目录。
        mode: dry_run/canary/full 模式。

    Returns:
        HuaxinAssetConsolidationConfig: 测试配置。
    """

    return HuaxinAssetConsolidationConfig.from_mapping(
        {
            "mode": mode,
            "source_mode": "external_snapshot",
            "source_snapshot_path": tmp_path / "source.json",
            "state_path": tmp_path / "state.json",
            "source_node_id": 22,
            "target_node_id": 11,
            "source_role": "source-query",
            "target_role": "target-writer",
            "source_host": "source-host",
            "target_host": "target-host",
            "earliest_time": "09:00:00",
            "snapshot_max_age_seconds": 120,
            "stable_samples": 2,
            "stable_interval_seconds": 1,
            "poll_seconds": 0.01,
            "wait_timeout": 0.01,
            "max_position_actions": 10,
            "max_position_volume": 10000,
            "max_fund_amount": 10000,
        }
    )


def _source_snapshot(captured_at, position=100, cash=50.0):
    """构造含完整同行身份的源节点快照。

    Args:
        captured_at: 快照采集时间。
        position: 银华日利可划昨仓。
        cash: 可划资金。

    Returns:
        dict: 外部 source provider 合同快照。
    """

    snapshot = {
        "schema_version": 2,
        "snapshot_schema": "huaxin-node-snapshot/v2",
        "state": "captured",
        "query_complete": True,
        "role": "source-query",
        "host": "source-host",
        "node_id": 22,
        "node": {
            "node_id": 22,
            "current": False,
            "provenance": "configured_session_fallback",
        },
        "nodes": [],
        "trading_day": "20260825",
        "captured_at": captured_at.isoformat(),
        "account": {
            "department_id": "D",
            "account_id": "A",
            "currency": "CNY",
            "available_cash": cash,
            "transferable_cash": cash,
            "frozen_cash": 0,
        },
        "positions": [
            {
                "exchange": "SSE",
                "security": "511880.XSHG",
                "current_position": position,
                "available_position": position,
                "history_position": position,
                "onroad_position": 0,
                "investor_id": "I",
                "business_unit_id": "B",
                "shareholder_id": "S",
                "market_id": 49,
            }
        ],
        "shareholder_accounts": [{"exchange": "SSE", "investor_id": "I", "shareholder_id": "S"}],
        "snapshot_generation": int(captured_at.timestamp() * 1000000),
        "producer": {
            "schema": "huaxin-node-snapshot-producer/v1",
            "instance_id": "fixture-source-producer",
            "git_commit": "a" * 40,
        },
    }
    return _seal_source_snapshot(snapshot)


class _SequenceProvider:
    """按调用次数返回预置源快照，并在耗尽后复用最后一份。"""

    def __init__(self, snapshots):
        """保存快照序列。

        Args:
            snapshots: 按轮次返回的快照列表。

        Returns:
            None。
        """

        self.snapshots = [deepcopy(row) for row in snapshots]
        self.calls = 0

    def __call__(self):
        """返回本轮快照副本。

        Returns:
            dict: 源节点快照。
        """

        index = min(self.calls, len(self.snapshots) - 1)
        self.calls += 1
        return deepcopy(self.snapshots[index])


class _TargetBroker:
    """模拟目标 writer、成功明细和资产到账的内存 Broker。"""

    def __init__(self, outcome="succeeded"):
        """初始化目标资产和划拨调用记录。

        Args:
            outcome: submit 原语返回的归一状态。

        Returns:
            None。
        """

        self.account = {
            "department_id": "TD",
            "account_id": "TA",
            "currency": "CNY",
            "available_cash": 1000.0,
            "transferable_cash": 1000.0,
            "frozen_cash": 0,
        }
        self.positions = []
        self.shareholders = []
        self.outcome = outcome
        self.position_submit_count = 0
        self.fund_submit_count = 0
        self.details = {}

    def get_system_nodes(self):
        """模拟生产空节点目录 fallback。

        Returns:
            list: 空目录。
        """

        return []

    def get_trading_day(self):
        """返回固定交易日。

        Returns:
            str: 八位交易日。
        """

        return "20260825"

    def get_account_info(self):
        """返回目标资金副本。

        Returns:
            dict: 目标资金。
        """

        return dict(self.account)

    def get_positions(self):
        """返回目标持仓副本。

        Returns:
            list: 目标持仓。
        """

        return deepcopy(self.positions)

    def get_shareholder_accounts(self, refresh=True):
        """返回目标股东账户。

        Args:
            refresh: 测试中忽略的刷新开关。

        Returns:
            list: 空股东账户列表。
        """

        del refresh
        return deepcopy(self.shareholders)

    def submit_position_transfer(self, source, **kwargs):
        """记录一次证券调入并在成功时更新目标持仓。

        Args:
            source: 源端同行身份。
            **kwargs: 数量、流水和节点参数。

        Returns:
            dict: 配置的划拨终态。
        """

        self.position_submit_count += 1
        serial = kwargs["apply_serial"]
        if self.outcome == "succeeded":
            self.positions = [
                {
                    "exchange": source["exchange"],
                    "security": source["security"],
                    "current_position": kwargs["volume"],
                    "available_position": kwargs["volume"],
                    "history_position": kwargs["volume"],
                    "onroad_position": 0,
                    "investor_id": "TI",
                    "business_unit_id": "TB",
                    "shareholder_id": "TS",
                    "market_id": 49,
                }
            ]
            self.details[serial] = "success"
        return {"submission_state": self.outcome, "apply_serial": serial}

    def submit_fund_transfer(self, source, **kwargs):
        """记录一次资金调入并在成功时更新目标余额。

        Args:
            source: 源端同行身份。
            **kwargs: 金额、流水和节点参数。

        Returns:
            dict: 配置的划拨终态。
        """

        del source
        self.fund_submit_count += 1
        serial = kwargs["apply_serial"]
        if self.outcome == "succeeded":
            self.account["transferable_cash"] += kwargs["amount"]
            self.account["available_cash"] += kwargs["amount"]
            self.details[serial] = "success"
        return {"submission_state": self.outcome, "apply_serial": serial}

    def get_position_transfer_details(self, filters):
        """按 ApplySerial 返回证券明细。

        Args:
            filters: 含 apply_serial 的过滤映射。

        Returns:
            list: 已知终态明细。
        """

        return self._detail(filters)

    def get_fund_transfer_details(self, filters):
        """按 ApplySerial 返回资金明细。

        Args:
            filters: 含 apply_serial 的过滤映射。

        Returns:
            list: 已知终态明细。
        """

        return self._detail(filters)

    def _detail(self, filters):
        """读取内存划拨终态。

        Args:
            filters: 含 apply_serial 的过滤映射。

        Returns:
            list: 匹配明细或空列表。
        """

        if filters.get("apply_serial") in (None, ""):
            return [
                {"apply_serial": serial, "transfer_status": status}
                for serial, status in sorted(self.details.items())
            ]
        serial = filters["apply_serial"]
        status = self.details.get(serial)
        return [] if status is None else [{"apply_serial": serial, "transfer_status": status}]


def test_off_config_requires_no_private_paths_or_nodes() -> None:
    """验证默认 off 不触发启用字段校验。"""

    config = HuaxinAssetConsolidationConfig.from_mapping({})

    assert config.enabled is False
    assert config.source_snapshot_path is None
    assert config.state_path is None


def test_direct_session_without_injected_provider_stays_explicitly_blocked(tmp_path) -> None:
    """验证尚未接入 direct provider 时不会退回外部文件或尝试划拨。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    config = HuaxinAssetConsolidationConfig.from_mapping(
        {
            "mode": "full",
            "source_mode": "direct_session",
            "state_path": tmp_path / "state.json",
            "source_node_id": 22,
            "target_node_id": 11,
            "source_role": "source-query",
            "target_role": "target-writer",
            "source_host": "target-host",
            "target_host": "target-host",
            "earliest_time": "09:00:00",
        }
    )
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        config,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    result = coordinator.drive_once(broker)

    assert result["state"] == "blocked"
    assert result["reason"] == "direct_session_source_provider_unsupported"
    assert coordinator.order_allowed() is False
    assert broker.position_submit_count == broker.fund_submit_count == 0
    assert not (tmp_path / "state.json").exists()


def test_full_plan_submits_sequentially_and_only_completes_after_two_end_reconciliation(
    tmp_path,
) -> None:
    """验证证券后资金串行执行，逐项明细与双端差额都通过后才放行。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
    ]
    provider = _SequenceProvider(snapshots)
    broker = _TargetBroker()
    config = _config(tmp_path)
    coordinator = HuaxinAssetConsolidationCoordinator(
        config,
        source_snapshot_provider=provider,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    assert coordinator.drive_once(broker)["state"] == "observing"
    assert coordinator.drive_once(broker)["state"] == "reconciling"
    assert broker.position_submit_count == 1
    assert broker.fund_submit_count == 0
    assert coordinator.drive_once(broker)["state"] == "planned"
    assert coordinator.drive_once(broker)["state"] == "reconciling"
    assert broker.fund_submit_count == 1
    assert coordinator.drive_once(broker)["state"] == "reconciling"
    result = coordinator.drive_once(broker)

    assert result["state"] == "complete"
    assert coordinator.order_allowed() is True
    assert broker.position_submit_count == 1
    assert broker.fund_submit_count == 1
    assert (tmp_path / "state.json").stat().st_mode & 0o777 == 0o600
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert [row["state"] for row in state["actions"]] == ["succeeded", "succeeded"]
    ready = state["ready_evidence"]
    assert ready["schema"] == HUAXIN_NODE16_READY_SCHEMA
    assert ready["state"] == "ready"
    assert ready["mode"] == "full"
    assert ready["trading_day"] == "20260825"
    assert ready["source_node_id"] == 22
    assert ready["target_node_id"] == 11
    assert ready["source_role"] == "source-query"
    assert ready["target_role"] == "target-writer"
    assert ready["action_count"] == 2
    assert all(
        len(ready[field]) == 64
        for field in (
            "plan_id_sha256",
            "source_snapshot_sha256",
            "source_snapshot_id",
            "source_producer_sha256",
            "target_snapshot_sha256",
            "target_snapshot_id",
            "target_positions_snapshot_id",
            "actions_sha256",
            "conservation_sha256",
            "source_nontransferable_residual_sha256",
            "generation",
            "fencing_token",
        )
    )
    assert ready["query_complete"] is True
    assert ready["pending_transfer_count"] == 0
    assert ready["source_snapshot_generation"] > 0
    assert ready["completion_cutoff_time"] == "09:25:00"
    assert state["source_nontransferable_residuals"] == {
        "schema": "huaxin-node14-nontransferable-residual/v1",
        "cash": [],
        "positions": [],
    }
    assert "account_id" not in (tmp_path / "state.json").read_text(encoding="utf-8")
    assert "shareholder_id" not in (tmp_path / "state.json").read_text(encoding="utf-8")

    before_restart = deepcopy(state["ready_evidence"])
    restarted = HuaxinAssetConsolidationCoordinator(
        config,
        source_snapshot_provider=provider,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    assert restarted.drive_once(broker)["state"] == "complete"
    reloaded = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert reloaded["ready_evidence"] == before_restart


def test_unknown_restart_only_queries_original_serial_and_never_resubmits(tmp_path) -> None:
    """验证 unknown 跨协调器重建后只查旧流水，native 写调用仍为一次。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1)),
    ]
    provider = _SequenceProvider(snapshots)
    broker = _TargetBroker(outcome="unknown")
    config = _config(tmp_path)
    first = HuaxinAssetConsolidationCoordinator(
        config,
        source_snapshot_provider=provider,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    first.drive_once(broker)
    assert first.drive_once(broker)["state"] == "unknown"
    assert broker.position_submit_count == 1
    restarted = HuaxinAssetConsolidationCoordinator(
        config,
        source_snapshot_provider=provider,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    result = restarted.drive_once(broker)

    assert result["state"] == "unknown"
    assert result["reason"] == "transfer_fact_unknown_query_only"
    assert broker.position_submit_count == 1
    assert broker.fund_submit_count == 0
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert "ready_evidence" not in state


def test_late_node14_transferable_cash_invalidates_existing_ready(tmp_path) -> None:
    """验证 READY 后发现 14 端可划现金会原子撤销 READY。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=5), position=0, cash=12.34),
    ]
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    broker = _TargetBroker()
    for _ in range(6):
        result = coordinator.drive_once(broker)
    assert result["state"] == "complete"

    result = coordinator.drive_once(broker)

    assert result["state"] == "blocked"
    assert result["reason"] == "complete_plan_has_new_source_residual"
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert state["state"] == "blocked"
    assert state["reason"].startswith("ready_invalidated:")
    assert "ready_evidence" not in state
    assert coordinator.order_allowed() is False


def test_dry_run_persists_plan_without_any_transfer_call(tmp_path) -> None:
    """验证 dry-run 只生成脱敏计划，资金和证券写调用均为零。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    provider = _SequenceProvider(
        [
            _source_snapshot(_NOW - timedelta(seconds=10)),
            _source_snapshot(_NOW - timedelta(seconds=5)),
        ]
    )
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path, mode="dry_run"),
        source_snapshot_provider=provider,
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    coordinator.drive_once(broker)
    result = coordinator.drive_once(broker)

    assert result["state"] == "dry_run"
    assert coordinator.order_allowed() is False
    assert broker.position_submit_count == broker.fund_submit_count == 0
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert "ready_evidence" not in state


def test_node_asset_digest_is_stable_and_changes_with_business_assets() -> None:
    """验证采样时间和行顺序不影响摘要，而资金或持仓漂移会改变摘要。

    Returns:
        None。
    """

    first = _source_snapshot(_NOW, position=100, cash=50.0)
    first["positions"].append(
        {
            "exchange": "SZSE",
            "security": "159001.SZ",
            "current_position": 20,
            "available_position": 20,
            "history_position": 20,
            "onroad_position": 0,
            "investor_id": "I2",
            "business_unit_id": "B2",
            "shareholder_id": "S2",
            "market_id": 1,
        }
    )
    reordered = deepcopy(first)
    reordered["captured_at"] = (_NOW + timedelta(seconds=30)).isoformat()
    reordered["positions"] = list(reversed(reordered["positions"]))

    stable_digest = build_huaxin_node_asset_snapshot_digest(first)
    assert build_huaxin_node_asset_snapshot_digest(reordered) == stable_digest

    cash_changed = deepcopy(first)
    cash_changed["account"]["transferable_cash"] = 49.99
    assert build_huaxin_node_asset_snapshot_digest(cash_changed) != stable_digest

    position_changed = deepcopy(first)
    position_changed["positions"][0]["current_position"] = 99
    assert build_huaxin_node_asset_snapshot_digest(position_changed) != stable_digest


def test_canary_complete_never_carries_ready_evidence(tmp_path) -> None:
    """验证 canary 即使双端完成也不会伪装为 full READY。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
    ]
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path, mode="canary"),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    result = {}
    for _ in range(6):
        result = coordinator.drive_once(broker)

    assert result["state"] == "canary_complete"
    assert coordinator.order_allowed() is False
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert "ready_evidence" not in state


def test_incomplete_or_tampered_source_snapshot_blocks_before_plan(tmp_path) -> None:
    """验证 query_complete 缺失和摘要篡改都不能生成计划。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    incomplete = _source_snapshot(_NOW)
    incomplete["query_complete"] = False
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=lambda: deepcopy(incomplete),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    assert coordinator.drive_once(_TargetBroker())["reason"] == "source_snapshot_query_incomplete"
    assert not (tmp_path / "state.json").exists()

    tampered = _source_snapshot(_NOW)
    tampered["account"]["transferable_cash"] = 999
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=lambda: deepcopy(tampered),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    assert coordinator.drive_once(_TargetBroker())["reason"] == "source_snapshot_digest_mismatch"
    assert not (tmp_path / "state.json").exists()


def test_zero_action_source_never_creates_fake_ready(tmp_path) -> None:
    """验证初始源端报零不会绕过真实 14→16 动作证据。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10), position=0, cash=0),
        _source_snapshot(_NOW - timedelta(seconds=5), position=0, cash=0),
    ]
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    assert coordinator.drive_once(_TargetBroker())["state"] == "observing"
    result = coordinator.drive_once(_TargetBroker())

    assert result["state"] == "blocked"
    assert result["reason"] == "zero_action_plan_cannot_prove_14_to_16_consolidation"
    assert not (tmp_path / "state.json").exists()


def test_source_generation_replay_and_conflict_fail_closed(tmp_path) -> None:
    """验证生产者代次回退或同代不同摘要会稳定阻断。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    first = _source_snapshot(_NOW - timedelta(seconds=5))
    replayed = _source_snapshot(_NOW - timedelta(seconds=10))
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider([first, replayed]),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    broker = _TargetBroker()
    assert coordinator.drive_once(broker)["state"] == "observing"
    assert coordinator.drive_once(broker)["reason"] == "source_snapshot_generation_replayed"

    conflict = deepcopy(first)
    conflict["account"]["transferable_cash"] = 49
    conflict["account"]["available_cash"] = 49
    _seal_source_snapshot(conflict)
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider([first, conflict]),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    assert coordinator.drive_once(_TargetBroker())["state"] == "observing"
    assert (
        coordinator.drive_once(_TargetBroker())["reason"] == "source_snapshot_generation_conflict"
    )


def test_completion_cutoff_blocks_without_persisting_plan(tmp_path) -> None:
    """验证超过盘前截止时间后不会新建计划或 READY。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    config = HuaxinAssetConsolidationConfig.from_mapping(
        {
            **_config(tmp_path).__dict__,
            "earliest_time": "09:00:00",
            "completion_cutoff_time": "09:05:00",
        }
    )
    coordinator = HuaxinAssetConsolidationCoordinator(
        config,
        source_snapshot_provider=lambda: _source_snapshot(_NOW),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )

    result = coordinator.drive_once(_TargetBroker())

    assert result["state"] == "blocked"
    assert result["reason"] == "completion_cutoff_exceeded"
    assert not (tmp_path / "state.json").exists()


def test_pending_transfer_or_combined_asset_drift_prevents_ready(tmp_path) -> None:
    """验证无关在途划拨和双端总量漂移都会阻止 READY。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
    ]
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    for _ in range(5):
        coordinator.drive_once(broker)
    broker.details[999999] = "unknown"
    result = coordinator.drive_once(broker)
    assert result["state"] == "blocked"
    assert result["reason"] == "fund_transfer_pending_or_unknown"
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert "ready_evidence" not in state

    second_path = tmp_path / "second"
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(second_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    for _ in range(5):
        coordinator.drive_once(broker)
    broker.account["transferable_cash"] += 1
    broker.account["available_cash"] += 1
    result = coordinator.drive_once(broker)
    assert result["state"] == "blocked"
    assert result["reason"] == "source_target_cash_conservation_mismatch"


def test_missing_original_transfer_detail_prevents_ready(tmp_path) -> None:
    """验证计划流水终态明细缺失时不能仅凭资产变化发布 READY。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
    ]
    broker = _TargetBroker()
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    for _ in range(5):
        coordinator.drive_once(broker)
    broker.details.pop(next(iter(broker.details)))

    result = coordinator.drive_once(broker)

    assert result["state"] == "blocked"
    assert result["reason"] == "ready_transfer_detail_missing_or_not_succeeded"
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert "ready_evidence" not in state


def test_frozen_source_residual_requires_machine_verifiable_evidence(tmp_path) -> None:
    """验证不可划冻结残留逐项留证后才允许完成。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    snapshots = [
        _source_snapshot(_NOW - timedelta(seconds=10)),
        _source_snapshot(_NOW - timedelta(seconds=5)),
        _source_snapshot(_NOW + timedelta(seconds=1), position=0),
        _source_snapshot(_NOW + timedelta(seconds=2), position=0),
        _source_snapshot(_NOW + timedelta(seconds=3), position=0, cash=0),
        _source_snapshot(_NOW + timedelta(seconds=4), position=0, cash=0),
    ]
    for snapshot in snapshots:
        snapshot["account"]["frozen_cash"] = 10
        _seal_source_snapshot(snapshot)
    coordinator = HuaxinAssetConsolidationCoordinator(
        _config(tmp_path),
        source_snapshot_provider=_SequenceProvider(snapshots),
        clock=lambda: _NOW,
        hostname=lambda: "target-host",
    )
    broker = _TargetBroker()
    for _ in range(5):
        coordinator.drive_once(broker)

    assert coordinator.drive_once(broker)["state"] == "complete"
    state = HuaxinAssetConsolidationStateStore(tmp_path / "state.json").load_day("20260825")
    assert state["source_nontransferable_residuals"]["cash"] == [
        {"reason": "frozen_cash", "amount": "10.00"}
    ]
    assert len(state["ready_evidence"]["source_nontransferable_residual_sha256"]) == 64
