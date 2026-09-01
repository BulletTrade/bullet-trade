"""
作者: BruceLee

文件职责: 生成和安装华鑫节点 schema v2 完整资产快照。
主要输入: query-only HuaxinBroker、显式节点/生产者配置和远程快照载荷。
主要输出: 带持久递增 generation 的完整快照，或 0600 原子安装结果。
上游关系: HuaxinBrokerAdapter 的私有 node_asset_snapshot/install_source_snapshot 动作。
下游关系: external_snapshot 归集 consumer 的本地私有快照文件。
关键配置: 功能默认关闭；不保存密码、前置地址、TerminalInfo，也不负责跨机传输。
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import socket
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Mapping, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - 华鑫生产 SDK 只运行在 Linux
    fcntl = None  # type: ignore[assignment]

from bullet_trade.integrations.huaxin.asset_consolidation import (
    HUAXIN_NODE_SNAPSHOT_PRODUCER_SCHEMA,
    HUAXIN_NODE_SNAPSHOT_SCHEMA,
    build_huaxin_node_asset_snapshot_digest,
)
from bullet_trade.utils.env_loader import get_env


_GENERATION_STATE_SCHEMA = "huaxin-node-snapshot-generation/v1"
_MAX_GENERATION = (1 << 64) - 1
_QUERY_NAMES = (
    "trading_day",
    "system_nodes",
    "account",
    "shareholder_accounts",
    "positions",
    "fund_transfer_details",
    "position_transfer_details",
)
_FORBIDDEN_SNAPSHOT_FIELDS = frozenset(
    {
        "dynamic_password",
        "login_account",
        "mac_address",
        "password",
        "secret",
        "terminal_info",
        "token",
        "trade_front",
    }
)


class HuaxinNodeSnapshotRelayError(RuntimeError):
    """表示节点快照生成、校验或安装必须失败关闭。"""


def _local_now() -> datetime:
    """返回本机带时区当前时间。

    Returns:
        datetime: 带本地时区的当前时间。
    """

    return datetime.now(timezone.utc).astimezone()


def _required_text(value: Any, *, field_name: str) -> str:
    """读取必填非空文本。

    Args:
        value: 原始字段值。
        field_name: 非敏感错误字段名。

    Returns:
        str: 去除首尾空白后的文本。

    Raises:
        HuaxinNodeSnapshotRelayError: 字段为空时抛出。
    """

    text = str(value or "").strip()
    if not text:
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_missing")
    return text


def _required_positive_int(value: Any, *, field_name: str) -> int:
    """读取必填正整数。

    Args:
        value: 原始字段值。
        field_name: 非敏感错误字段名。

    Returns:
        int: 已验证正整数。

    Raises:
        HuaxinNodeSnapshotRelayError: 值不是精确正整数时抛出。
    """

    if value in (None, "") or isinstance(value, bool):
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid") from exc
    if parsed <= 0 or str(parsed) != str(value).strip():
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid")
    return parsed


def _required_sha256(value: Any, *, field_name: str) -> str:
    """验证小写 SHA-256 文本。

    Args:
        value: 原始摘要值。
        field_name: 非敏感错误字段名。

    Returns:
        str: 已验证摘要。

    Raises:
        HuaxinNodeSnapshotRelayError: 长度或字符集非法时抛出。
    """

    text = str(value or "").strip()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid")
    return text


def _canonical_json_sha256(value: Any) -> str:
    """计算 JSON 基础对象的确定性摘要。

    Args:
        value: 可被 JSON 编码的基础对象。

    Returns:
        str: SHA-256 十六进制摘要。
    """

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_aware_datetime(value: Any, *, field_name: str) -> datetime:
    """解析必须带时区的 ISO-8601 时间。

    Args:
        value: 原始时间文本。
        field_name: 非敏感错误字段名。

    Returns:
        datetime: 带时区时间。

    Raises:
        HuaxinNodeSnapshotRelayError: 时间为空、非法或无时区时抛出。
    """

    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid") from exc
    if parsed.tzinfo is None:
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_timezone_missing")
    return parsed


def _producer_sha256(producer: Mapping[str, Any]) -> str:
    """验证并摘要快照生产者身份。

    Args:
        producer: schema、实例和 Git 提交对象。

    Returns:
        str: 确定性生产者摘要。

    Raises:
        HuaxinNodeSnapshotRelayError: 生产者字段不完整时抛出。
    """

    if str(producer.get("schema") or "") != HUAXIN_NODE_SNAPSHOT_PRODUCER_SCHEMA:
        raise HuaxinNodeSnapshotRelayError("snapshot_producer_schema_invalid")
    instance_id = _required_text(
        producer.get("instance_id"), field_name="snapshot_producer_instance"
    )
    git_commit = str(producer.get("git_commit") or "").strip().lower()
    if len(git_commit) not in {40, 64} or any(
        char not in "0123456789abcdef" for char in git_commit
    ):
        raise HuaxinNodeSnapshotRelayError("snapshot_producer_git_invalid")
    return _canonical_json_sha256(
        {
            "schema": HUAXIN_NODE_SNAPSHOT_PRODUCER_SCHEMA,
            "instance_id": instance_id,
            "git_commit": git_commit,
        }
    )


def _payload_sha256(snapshot: Mapping[str, Any]) -> str:
    """计算除自身摘要字段外的完整快照 payload 摘要。

    Args:
        snapshot: 完整 schema v2 快照。

    Returns:
        str: 覆盖资金、持仓、股东和划拨明细的 SHA-256。
    """

    material = dict(snapshot)
    material.pop("payload_digest_sha256", None)
    return _canonical_json_sha256(material)


def _reject_forbidden_snapshot_fields(value: Any) -> None:
    """递归拒绝密码、前置和终端身份等敏感字段。

    Args:
        value: 待检查 JSON 基础对象。

    Returns:
        None。

    Raises:
        HuaxinNodeSnapshotRelayError: 任一层出现禁止字段时抛出。
    """

    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in _FORBIDDEN_SNAPSHOT_FIELDS:
                raise HuaxinNodeSnapshotRelayError("source_snapshot_forbidden_field")
            _reject_forbidden_snapshot_fields(item)
    elif isinstance(value, list):
        for item in value:
            _reject_forbidden_snapshot_fields(item)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """以 0600 权限和同目录 rename 原子写入 JSON。

    Args:
        path: 最终文件路径。
        payload: 完整 JSON 对象。

    Returns:
        None。

    Side Effects:
        创建 0700 父目录、0600 临时文件并原子替换最终文件。
    """

    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary), str(target))
        os.chmod(target, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_json_object(path: Path, *, field_name: str) -> Optional[Dict[str, Any]]:
    """读取可选 JSON 对象文件。

    Args:
        path: 待读取文件。
        field_name: 非敏感错误字段名。

    Returns:
        Optional[Dict[str, Any]]: 文件不存在时为 None，否则为对象副本。

    Raises:
        HuaxinNodeSnapshotRelayError: 文件损坏或顶层不是对象时抛出。
    """

    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_corrupt") from exc
    if not isinstance(payload, dict):
        raise HuaxinNodeSnapshotRelayError(f"{field_name}_invalid")
    return payload


@contextmanager
def _exclusive_file_lock(path: Path) -> Iterator[None]:
    """对状态或快照路径取得跨进程排他锁。

    Args:
        path: 被保护的最终文件路径。

    Yields:
        None: 锁持有期间的执行窗口。

    Raises:
        HuaxinNodeSnapshotRelayError: 当前平台没有安全文件锁时抛出。

    Side Effects:
        创建同目录 0600 ``.lock`` 文件；锁文件不含业务内容。
    """

    if fcntl is None:
        raise HuaxinNodeSnapshotRelayError("snapshot_file_lock_unavailable")
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = target.with_name(f".{target.name}.lock")
    descriptor = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@dataclass(frozen=True)
class HuaxinNodeSnapshotRelayConfig:
    """保存节点快照生成与安装配置，并由路径自动确定本机职责。"""

    generation_state_path: Optional[Path] = None
    node_id: Optional[int] = None
    role: str = ""
    host: str = ""
    producer_instance: str = ""
    producer_git_commit: str = ""
    install_path: Optional[Path] = None
    expected_source_node_id: Optional[int] = None
    expected_source_role: str = ""
    expected_source_host: str = ""
    install_max_age_seconds: float = 120.0

    @property
    def capture_enabled(self) -> bool:
        """返回本机是否承担快照生成职责。

        Returns:
            bool: 配置 generation 状态文件时为 True。
        """

        return self.generation_state_path is not None

    @property
    def install_enabled(self) -> bool:
        """返回本机是否承担快照安装职责。

        Returns:
            bool: 配置来源快照文件时为 True。
        """

        return self.install_path is not None

    @classmethod
    def from_env(cls) -> "HuaxinNodeSnapshotRelayConfig":
        """从环境构造按固定路径自动分工的 relay 配置。

        Returns:
            HuaxinNodeSnapshotRelayConfig: 已严格校验的配置。

        Raises:
            HuaxinNodeSnapshotRelayError: 任一已配置职责缺少身份时抛出。

        Side Effects:
            仅读取环境，不访问文件或柜台。
        """

        values = {
            "generation_state_path": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_STATE_FILE"),
            "node_id": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_NODE_ID"),
            "role": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_ROLE"),
            "host": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_HOST"),
            "producer_instance": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_PRODUCER_INSTANCE"),
            "producer_git_commit": get_env("HUAXIN_NODE_ASSET_SNAPSHOT_GIT_COMMIT"),
            "install_path": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_SNAPSHOT"),
            "expected_source_node_id": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_NODE_ID"),
            "expected_source_role": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_ROLE"),
            "expected_source_host": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_HOST"),
            "install_max_age_seconds": get_env(
                "HUAXIN_ASSET_CONSOLIDATION_SNAPSHOT_MAX_AGE_SECONDS"
            ),
        }
        return cls.from_mapping(values)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "HuaxinNodeSnapshotRelayConfig":
        """从测试或部署映射构造 relay 配置。

        Args:
            values: 与配置字段同名的映射。

        Returns:
            HuaxinNodeSnapshotRelayConfig: 已校验配置。

        Raises:
            HuaxinNodeSnapshotRelayError: 启用功能缺少路径或身份时抛出。
        """

        capture_enabled = values.get("generation_state_path") not in (None, "")
        install_enabled = values.get("install_path") not in (None, "")
        kwargs: Dict[str, Any] = {}
        if capture_enabled:
            kwargs.update(
                generation_state_path=Path(
                    _required_text(
                        values.get("generation_state_path"),
                        field_name="snapshot_generation_state_path",
                    )
                ).expanduser(),
                node_id=_required_positive_int(
                    values.get("node_id"), field_name="snapshot_node_id"
                ),
                role=_required_text(values.get("role"), field_name="snapshot_role"),
                host=_required_text(values.get("host"), field_name="snapshot_host"),
                producer_instance=_required_text(
                    values.get("producer_instance"),
                    field_name="snapshot_producer_instance",
                ),
                producer_git_commit=_required_text(
                    values.get("producer_git_commit"),
                    field_name="snapshot_producer_git_commit",
                ).lower(),
            )
            _producer_sha256(
                {
                    "schema": HUAXIN_NODE_SNAPSHOT_PRODUCER_SCHEMA,
                    "instance_id": kwargs["producer_instance"],
                    "git_commit": kwargs["producer_git_commit"],
                }
            )
        if install_enabled:
            raw_max_age = values.get("install_max_age_seconds")
            try:
                max_age = 120.0 if raw_max_age in (None, "") else float(raw_max_age)
            except (TypeError, ValueError, OverflowError) as exc:
                raise HuaxinNodeSnapshotRelayError("snapshot_install_max_age_invalid") from exc
            if not math.isfinite(max_age) or max_age <= 0 or max_age > 120:
                raise HuaxinNodeSnapshotRelayError("snapshot_install_max_age_invalid")
            kwargs.update(
                install_path=Path(
                    _required_text(values.get("install_path"), field_name="snapshot_install_path")
                ).expanduser(),
                expected_source_node_id=_required_positive_int(
                    values.get("expected_source_node_id"),
                    field_name="snapshot_expected_source_node_id",
                ),
                expected_source_role=_required_text(
                    values.get("expected_source_role"),
                    field_name="snapshot_expected_source_role",
                ),
                expected_source_host=_required_text(
                    values.get("expected_source_host"),
                    field_name="snapshot_expected_source_host",
                ),
                install_max_age_seconds=max_age,
            )
        return cls(**kwargs)


class HuaxinNodeSnapshotRelay:
    """串行生成并安装完整节点资产快照。"""

    def __init__(
        self,
        config: HuaxinNodeSnapshotRelayConfig,
        *,
        clock: Optional[Callable[[], datetime]] = None,
        hostname: Optional[Callable[[], str]] = None,
    ) -> None:
        """保存配置和进程内串行锁。

        Args:
            config: 已校验 relay 配置。
            clock: 可注入带时区当前时间函数。
            hostname: 可注入本机主机名函数。

        Returns:
            None。
        """

        self.config = config
        self._clock = clock or _local_now
        self._hostname = hostname or socket.gethostname
        self._lock = threading.RLock()

    def capture(self, broker: Any) -> Dict[str, Any]:
        """完成全部只读 query-end 后生成一份持久递增快照。

        Args:
            broker: 已达到 query ready 的 HuaxinBroker。

        Returns:
            Dict[str, Any]: schema v2 完整节点快照。

        Raises:
            HuaxinNodeSnapshotRelayError: 功能未启用、主机不符或任一查询/合同失败时抛出。

        Side Effects:
            仅持久化 generation 状态；不会调用任何券商写接口。
        """

        if not self.config.capture_enabled:
            raise HuaxinNodeSnapshotRelayError("node_asset_snapshot_disabled")
        assert self.config.generation_state_path is not None
        assert self.config.node_id is not None
        with self._lock, _exclusive_file_lock(self.config.generation_state_path):
            observed_at = self._aware_now()
            actual_host = str(self._hostname() or "").strip()
            if actual_host != self.config.host:
                raise HuaxinNodeSnapshotRelayError("node_asset_snapshot_host_mismatch")
            nodes = list(broker.get_system_nodes() or [])
            node = self._select_node(nodes)
            trading_day = str(broker.get_trading_day() or "").strip()
            if len(trading_day) != 8 or not trading_day.isdigit():
                raise HuaxinNodeSnapshotRelayError("node_asset_snapshot_trading_day_invalid")
            account = dict(broker.get_account_info() or {})
            shareholders = list(broker.get_shareholder_accounts(refresh=True) or [])
            positions = list(broker.get_positions() or [])
            fund_details = list(broker.get_fund_transfer_details({}) or [])
            position_details = list(broker.get_position_transfer_details({}) or [])
            produced_at = self._aware_now()
            generation = self._next_generation_locked()
            producer = {
                "schema": HUAXIN_NODE_SNAPSHOT_PRODUCER_SCHEMA,
                "instance_id": self.config.producer_instance,
                "git_commit": self.config.producer_git_commit,
            }
            snapshot: Dict[str, Any] = {
                "schema_version": 2,
                "snapshot_schema": HUAXIN_NODE_SNAPSHOT_SCHEMA,
                "state": "captured",
                "query_complete": True,
                "source_mode": "external_snapshot",
                "role": self.config.role,
                "host": actual_host,
                "host_id": actual_host,
                "node_id": self.config.node_id,
                "node": node,
                "nodes": nodes,
                "trading_day": trading_day,
                "observed_at": observed_at.isoformat(timespec="microseconds"),
                "produced_at": produced_at.isoformat(timespec="microseconds"),
                "captured_at": produced_at.isoformat(timespec="microseconds"),
                "generation": generation,
                "snapshot_generation": generation,
                "producer": producer,
                "producer_sha256": _producer_sha256(producer),
                "query_provenance": {
                    "query_complete": True,
                    "query_end": {name: True for name in _QUERY_NAMES},
                },
                "account": account,
                "shareholder_accounts": shareholders,
                "positions": positions,
                "transfer_details": {
                    "fund": fund_details,
                    "position": position_details,
                },
            }
            snapshot["snapshot_id"] = build_huaxin_node_asset_snapshot_digest(snapshot)
            snapshot["snapshot_digest_sha256"] = snapshot["snapshot_id"]
            snapshot["payload_digest_sha256"] = _payload_sha256(snapshot)
            validate_huaxin_relay_snapshot(snapshot)
            self._save_generation_locked(generation)
            return json.loads(json.dumps(snapshot, ensure_ascii=False))

    def install(self, payload: Mapping[str, Any], *, trading_day: str) -> Dict[str, Any]:
        """严格校验并幂等安装一份源节点快照。

        Args:
            payload: 含 ``snapshot`` 的远程载荷，或快照对象本身。
            trading_day: consumer 当前柜台八位交易日。

        Returns:
            Dict[str, Any]: installed/noop、generation 和摘要等非敏感结果。

        Raises:
            HuaxinNodeSnapshotRelayError: 功能未启用、合同冲突、回放或交易日不符时抛出。

        Side Effects:
            合格新 generation 以 0600 临时文件和 rename 原子安装。
        """

        if not self.config.install_enabled:
            raise HuaxinNodeSnapshotRelayError("source_snapshot_install_disabled")
        assert self.config.install_path is not None
        raw = payload.get("snapshot") if isinstance(payload.get("snapshot"), Mapping) else payload
        snapshot = json.loads(json.dumps(dict(raw), ensure_ascii=False))
        validate_huaxin_relay_snapshot(snapshot)
        self._validate_install_identity(snapshot)
        generation = int(snapshot["generation"])
        digest = str(snapshot["payload_digest_sha256"])
        with self._lock, _exclusive_file_lock(self.config.install_path):
            existing = _load_json_object(
                self.config.install_path, field_name="installed_source_snapshot"
            )
            if existing is not None:
                validate_huaxin_relay_snapshot(existing)
                existing_generation = int(existing["generation"])
                existing_digest = str(existing["payload_digest_sha256"])
                if generation < existing_generation:
                    raise HuaxinNodeSnapshotRelayError("source_snapshot_generation_replayed")
                if generation == existing_generation:
                    if digest != existing_digest:
                        raise HuaxinNodeSnapshotRelayError("source_snapshot_generation_conflict")
                    return self._install_result(snapshot, installed=False)
            self._validate_install_day_and_freshness(snapshot, trading_day=trading_day)
            _atomic_write_json(self.config.install_path, snapshot)
            return self._install_result(snapshot, installed=True)

    def _aware_now(self) -> datetime:
        """读取并验证当前带时区时间。

        Returns:
            datetime: 带时区当前时间。

        Raises:
            HuaxinNodeSnapshotRelayError: 注入时钟返回无时区时间时抛出。
        """

        now = self._clock()
        if now.tzinfo is None:
            raise HuaxinNodeSnapshotRelayError("snapshot_clock_timezone_missing")
        return now

    def _select_node(self, nodes: list) -> Dict[str, Any]:
        """从柜台节点目录或显式会话身份选择节点证明。

        Args:
            nodes: 完整系统节点查询结果。

        Returns:
            Dict[str, Any]: 带 provenance 的节点记录。

        Raises:
            HuaxinNodeSnapshotRelayError: current 冲突或配置节点不唯一时抛出。
        """

        assert self.config.node_id is not None
        current = [row for row in nodes if isinstance(row, Mapping) and bool(row.get("current"))]
        if len(current) > 1:
            raise HuaxinNodeSnapshotRelayError("snapshot_current_node_duplicated")
        expected = [
            row
            for row in nodes
            if isinstance(row, Mapping) and int(row.get("node_id") or -1) == self.config.node_id
        ]
        if current:
            if int(current[0].get("node_id") or -1) != self.config.node_id:
                raise HuaxinNodeSnapshotRelayError("snapshot_current_node_mismatch")
            return dict(current[0], provenance="vendor_current")
        if nodes:
            if len(expected) != 1:
                raise HuaxinNodeSnapshotRelayError("snapshot_node_catalog_ambiguous")
            return dict(expected[0], provenance="vendor_catalog_expected")
        return {
            "node_id": self.config.node_id,
            "node_info": "",
            "current": False,
            "provenance": "configured_session_fallback",
        }

    def _next_generation_locked(self) -> int:
        """读取持久状态并计算下一个全局 generation。

        Returns:
            int: 尚未落盘的下一正 uint64 generation。

        Raises:
            HuaxinNodeSnapshotRelayError: 状态损坏或 generation 溢出时抛出。
        """

        assert self.config.generation_state_path is not None
        state = _load_json_object(
            self.config.generation_state_path, field_name="snapshot_generation_state"
        )
        if state is None:
            return 1
        if state.get("schema") != _GENERATION_STATE_SCHEMA:
            raise HuaxinNodeSnapshotRelayError("snapshot_generation_state_schema_invalid")
        generation = _required_positive_int(
            state.get("last_generation"), field_name="snapshot_last_generation"
        )
        if generation >= _MAX_GENERATION:
            raise HuaxinNodeSnapshotRelayError("snapshot_generation_exhausted")
        return generation + 1

    def _save_generation_locked(self, generation: int) -> None:
        """原子保存最近已发布 generation。

        Args:
            generation: 已完成校验的正 uint64 generation。

        Returns:
            None。

        Side Effects:
            写入 0600 generation 状态文件。
        """

        assert self.config.generation_state_path is not None
        _atomic_write_json(
            self.config.generation_state_path,
            {
                "schema": _GENERATION_STATE_SCHEMA,
                "last_generation": generation,
            },
        )

    def _validate_install_identity(self, snapshot: Mapping[str, Any]) -> None:
        """校验 consumer 固定的源节点、角色和主机身份。

        Args:
            snapshot: 已通过通用 schema 校验的快照。

        Returns:
            None。

        Raises:
            HuaxinNodeSnapshotRelayError: 节点、主机或角色不符时抛出。
        """

        checks = (
            (int(snapshot["node_id"]), self.config.expected_source_node_id, "node"),
            (str(snapshot["role"]), self.config.expected_source_role, "role"),
            (str(snapshot["host"]), self.config.expected_source_host, "host"),
        )
        for actual, expected, name in checks:
            if actual != expected:
                raise HuaxinNodeSnapshotRelayError(f"source_snapshot_{name}_mismatch")

    def _validate_install_day_and_freshness(
        self, snapshot: Mapping[str, Any], *, trading_day: str
    ) -> None:
        """在首次安装新 generation 前校验交易日和新鲜度。

        Args:
            snapshot: 已通过 schema 和静态身份校验的快照。
            trading_day: consumer 当前柜台八位交易日。

        Returns:
            None。

        Raises:
            HuaxinNodeSnapshotRelayError: 交易日冲突、未来时间或快照过期时抛出。
        """

        expected_day = str(trading_day or "").strip()
        if len(expected_day) != 8 or not expected_day.isdigit():
            raise HuaxinNodeSnapshotRelayError("consumer_trading_day_invalid")
        if str(snapshot.get("trading_day") or "") != expected_day:
            raise HuaxinNodeSnapshotRelayError("source_snapshot_trading_day_mismatch")
        produced_at = _parse_aware_datetime(
            snapshot.get("produced_at"), field_name="source_snapshot_produced_at"
        )
        age = (
            self._aware_now().astimezone(timezone.utc) - produced_at.astimezone(timezone.utc)
        ).total_seconds()
        if age < -5:
            raise HuaxinNodeSnapshotRelayError("source_snapshot_time_in_future")
        if age > self.config.install_max_age_seconds:
            raise HuaxinNodeSnapshotRelayError("source_snapshot_stale")

    @staticmethod
    def _install_result(snapshot: Mapping[str, Any], *, installed: bool) -> Dict[str, Any]:
        """生成不包含账户身份和路径的安装结果。

        Args:
            snapshot: 已验证快照。
            installed: 本次是否发生原子替换。

        Returns:
            Dict[str, Any]: 远程调用的脱敏幂等结果。
        """

        return {
            "installed": installed,
            "noop": not installed,
            "generation": int(snapshot["generation"]),
            "snapshot_id": str(snapshot["snapshot_id"]),
            "payload_digest_sha256": str(snapshot["payload_digest_sha256"]),
            "trading_day": str(snapshot["trading_day"]),
        }


def validate_huaxin_relay_snapshot(snapshot: Mapping[str, Any]) -> None:
    """严格验证 relay schema v2 快照的完整性和所有摘要。

    Args:
        snapshot: 待校验完整节点快照。

    Returns:
        None。

    Raises:
        HuaxinNodeSnapshotRelayError: 任一 schema、查询、身份或摘要字段不合格时抛出。
    """

    _reject_forbidden_snapshot_fields(snapshot)
    if (
        snapshot.get("schema_version") != 2
        or snapshot.get("snapshot_schema") != HUAXIN_NODE_SNAPSHOT_SCHEMA
        or snapshot.get("state") != "captured"
        or snapshot.get("query_complete") is not True
        or snapshot.get("source_mode") != "external_snapshot"
    ):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_schema_invalid")
    host = _required_text(snapshot.get("host"), field_name="source_snapshot_host")
    if str(snapshot.get("host_id") or "") != host:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_host_id_mismatch")
    _required_text(snapshot.get("role"), field_name="source_snapshot_role")
    node_id = _required_positive_int(snapshot.get("node_id"), field_name="source_snapshot_node_id")
    node = snapshot.get("node")
    if not isinstance(node, Mapping) or int(node.get("node_id") or -1) != node_id:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_node_provenance_mismatch")
    if str(node.get("provenance") or "") not in {
        "vendor_current",
        "vendor_catalog_expected",
        "configured_session_fallback",
    }:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_node_provenance_invalid")
    if node.get("provenance") == "configured_session_fallback" and bool(node.get("current")):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_fallback_claims_current")
    trading_day = str(snapshot.get("trading_day") or "")
    if len(trading_day) != 8 or not trading_day.isdigit():
        raise HuaxinNodeSnapshotRelayError("source_snapshot_trading_day_invalid")
    observed_at = _parse_aware_datetime(
        snapshot.get("observed_at"), field_name="source_snapshot_observed_at"
    )
    produced_at = _parse_aware_datetime(
        snapshot.get("produced_at"), field_name="source_snapshot_produced_at"
    )
    captured_at = _parse_aware_datetime(
        snapshot.get("captured_at"), field_name="source_snapshot_captured_at"
    )
    if produced_at < observed_at or captured_at != produced_at:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_time_sequence_invalid")
    generation = _required_positive_int(
        snapshot.get("generation"), field_name="source_snapshot_generation"
    )
    if generation > _MAX_GENERATION or snapshot.get("snapshot_generation") != generation:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_generation_alias_mismatch")
    producer = snapshot.get("producer")
    if not isinstance(producer, Mapping):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_producer_missing")
    if _required_sha256(
        snapshot.get("producer_sha256"), field_name="source_snapshot_producer_sha256"
    ) != _producer_sha256(producer):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_producer_digest_mismatch")
    provenance = snapshot.get("query_provenance")
    if not isinstance(provenance, Mapping) or provenance.get("query_complete") is not True:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_query_provenance_invalid")
    query_end = provenance.get("query_end")
    if not isinstance(query_end, Mapping) or any(
        query_end.get(name) is not True for name in _QUERY_NAMES
    ):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_query_end_incomplete")
    shareholders = snapshot.get("shareholder_accounts")
    if not isinstance(shareholders, list) or any(
        not isinstance(row, Mapping) for row in shareholders
    ):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_shareholders_invalid")
    details = snapshot.get("transfer_details")
    if (
        not isinstance(details, Mapping)
        or not isinstance(details.get("fund"), list)
        or not isinstance(details.get("position"), list)
        or any(not isinstance(row, Mapping) for row in details.get("fund") or [])
        or any(not isinstance(row, Mapping) for row in details.get("position") or [])
    ):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_transfer_details_invalid")
    snapshot_id = _required_sha256(snapshot.get("snapshot_id"), field_name="source_snapshot_id")
    try:
        actual_asset_digest = build_huaxin_node_asset_snapshot_digest(snapshot)
    except Exception as exc:
        raise HuaxinNodeSnapshotRelayError(str(exc)) from exc
    if snapshot_id != actual_asset_digest:
        raise HuaxinNodeSnapshotRelayError("source_snapshot_asset_digest_mismatch")
    if (
        _required_sha256(
            snapshot.get("snapshot_digest_sha256"),
            field_name="source_snapshot_digest_sha256",
        )
        != snapshot_id
    ):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_digest_alias_mismatch")
    if _required_sha256(
        snapshot.get("payload_digest_sha256"),
        field_name="source_snapshot_payload_digest_sha256",
    ) != _payload_sha256(snapshot):
        raise HuaxinNodeSnapshotRelayError("source_snapshot_payload_digest_mismatch")


__all__ = [
    "HuaxinNodeSnapshotRelay",
    "HuaxinNodeSnapshotRelayConfig",
    "HuaxinNodeSnapshotRelayError",
    "validate_huaxin_relay_snapshot",
]
