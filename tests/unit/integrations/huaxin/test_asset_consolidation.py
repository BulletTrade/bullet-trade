"""验证华鑫节点资产归集的持久状态、零重试和双端对账。"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

from bullet_trade.integrations.huaxin.asset_consolidation import (
    HuaxinAssetConsolidationConfig,
    HuaxinAssetConsolidationCoordinator,
    HuaxinAssetConsolidationStateStore,
)


_ZONE = timezone(timedelta(hours=8))
_NOW = datetime(2026, 8, 25, 9, 10, tzinfo=_ZONE)


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

    return {
        "schema_version": 1,
        "state": "captured",
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
                "investor_id": "I",
                "business_unit_id": "B",
                "shareholder_id": "S",
                "market_id": 49,
            }
        ],
        "shareholder_accounts": [{"exchange": "SSE", "investor_id": "I", "shareholder_id": "S"}],
    }


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

        self.account = {"transferable_cash": 1000.0}
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
    assert "account_id" not in (tmp_path / "state.json").read_text(encoding="utf-8")
    assert "shareholder_id" not in (tmp_path / "state.json").read_text(encoding="utf-8")


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
