"""
作者: BruceLee

文件职责: 编排华鑫源节点到目标节点的每日资产归集、持久状态与双端对账。
主要输入: 私密节点角色配置、源端权威资产快照和目标端单 writer HuaxinBroker。
主要输出: 0600 原子 JSON 状态、脱敏健康摘要以及新下单放行门禁。
上游关系: HuaxinBrokerAdapter 在独立 Trader executor 中周期调用本模块。
下游关系: HuaxinBroker 的节点查询、划拨流水查询和一次性 NodeMoveIn 原语。
关键配置: 功能默认 off；公开代码不内置生产节点编号、账号、前置或主机身份。
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import socket
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, time as clock_time, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Set

from bullet_trade.utils.env_loader import get_env


HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED = "huaxin_asset_consolidation_pending"
_ALLOWED_MODES = frozenset({"off", "dry_run", "canary", "full"})
_SUCCESS_TRANSFER_STATES = frozenset({"success", "succeeded", "repeal_success"})
_FAILED_TRANSFER_STATES = frozenset({"failed", "rejected", "repeal_failed"})
_RECOVER_ONLY_ACTION_STATES = frozenset({"submit_started", "unknown"})


class HuaxinAssetConsolidationError(RuntimeError):
    """表示归集合同不满足且必须失败关闭的错误。"""


class HuaxinAssetConsolidationWaiting(RuntimeError):
    """表示只读前置暂未满足、后续可以安全重查的状态。"""


def _as_int(value: Any, default: int = 0) -> int:
    """安全转换整数。

    Args:
        value: 原始字段值。
        default: 转换失败时的默认值。

    Returns:
        int: 转换结果。
    """

    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    """安全转换有限浮点数。

    Args:
        value: 原始字段值。
        default: 转换失败或非有限数时的默认值。

    Returns:
        float: 转换结果。
    """

    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def _parse_positive_int(value: Any, name: str, default: int) -> int:
    """解析正整数配置。

    Args:
        value: 环境或注入配置值。
        name: 用于错误文本的配置名。
        default: 空值时默认值。

    Returns:
        int: 正整数。

    Raises:
        ValueError: 值不是正整数时抛出。
    """

    parsed = default if value in (None, "") else _as_int(value, -1)
    if parsed <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return parsed


def _parse_nonnegative_float(value: Any, name: str, default: float) -> float:
    """解析非负有限浮点配置。

    Args:
        value: 环境或注入配置值。
        name: 用于错误文本的配置名。
        default: 空值时默认值。

    Returns:
        float: 非负有限数。

    Raises:
        ValueError: 值非法时抛出。
    """

    parsed = default if value in (None, "") else _as_float(value, float("nan"))
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{name} 必须为非负有限数")
    return parsed


def _parse_clock(value: str) -> clock_time:
    """解析每日最早执行时刻。

    Args:
        value: ``HH:MM[:SS]`` 文本。

    Returns:
        datetime.time: 无时区墙上时刻。

    Raises:
        ValueError: 格式不合法时抛出。
    """

    text = str(value or "").strip()
    for pattern in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, pattern).time()
        except ValueError:
            continue
    raise ValueError("HUAXIN_ASSET_CONSOLIDATION_EARLIEST_TIME 必须为 HH:MM[:SS]")


def _parse_datetime(value: Any) -> datetime:
    """解析必须带时区的 ISO-8601 时间。

    Args:
        value: 快照或状态中的时间文本。

    Returns:
        datetime: 带时区时间。

    Raises:
        HuaxinAssetConsolidationError: 时间缺失、非法或无时区时抛出。
    """

    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise HuaxinAssetConsolidationError("资产快照缺少有效 captured_at") from exc
    if result.tzinfo is None:
        raise HuaxinAssetConsolidationError("资产快照 captured_at 必须带时区")
    return result


def _now_iso(now: datetime) -> str:
    """生成带时区的秒级 ISO-8601 时间。

    Args:
        now: 当前带时区时间。

    Returns:
        str: ISO-8601 文本。
    """

    return now.isoformat(timespec="seconds")


def _row_fingerprint(row: Mapping[str, Any], kind: str) -> str:
    """对同行技术身份生成不可逆指纹。

    Args:
        row: 资金行或持仓行。
        kind: ``fund`` 或 ``position``。

    Returns:
        str: SHA-256 十六进制摘要。
    """

    fields = (
        ("department_id", "account_id", "currency")
        if kind == "fund"
        else (
            "exchange",
            "investor_id",
            "business_unit_id",
            "shareholder_id",
            "security",
            "market_id",
        )
    )
    material = json.dumps(
        [str(row.get(field) or "") for field in fields],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _position_key(row: Mapping[str, Any]) -> str:
    """生成不含股东账号的证券资产匹配键。

    Args:
        row: 规范化持仓行。

    Returns:
        str: ``exchange|security`` 键。
    """

    security = str(row.get("security") or "").strip().upper().split(".", 1)[0]
    exchange = str(row.get("exchange") or row.get("market") or "").strip().upper()
    return f"{exchange}|{security}"


def _apply_serial(trading_day: str, action_key: str, used: Set[int]) -> int:
    """为计划内动作生成稳定且唯一的正 int32 ApplySerial。

    Args:
        trading_day: 八位交易日。
        action_key: 含 plan_id 和脱敏动作身份的稳定键。
        used: 当前计划已占用的流水集合。

    Returns:
        int: 正 int32 流水。
    """

    salt = 0
    while True:
        digest = hashlib.sha256(f"{trading_day}|{action_key}|{salt}".encode("ascii")).digest()
        serial = int.from_bytes(digest[:4], "big") & 0x7FFFFFFF
        if serial and serial not in used:
            used.add(serial)
            return serial
        salt += 1


@dataclass(frozen=True)
class HuaxinAssetConsolidationConfig:
    """保存华鑫节点归集的显式私密配置与安全边界。"""

    mode: str = "off"
    source_mode: str = "off"
    source_snapshot_path: Optional[Path] = None
    state_path: Optional[Path] = None
    source_node_id: Optional[int] = None
    target_node_id: Optional[int] = None
    source_role: str = ""
    target_role: str = ""
    source_host: str = ""
    target_host: str = ""
    earliest_time: clock_time = clock_time(9, 0)
    snapshot_max_age_seconds: float = 120.0
    stable_samples: int = 2
    stable_interval_seconds: float = 5.0
    poll_seconds: float = 5.0
    wait_timeout: float = 20.0
    max_position_actions: int = 100
    max_position_volume: int = 100000000
    max_fund_amount: float = 1000000000.0
    canary_position_volume: int = 100
    canary_fund_amount: float = 100.0

    @property
    def enabled(self) -> bool:
        """返回功能是否显式启用。

        Returns:
            bool: 非 off 模式为 True。
        """

        return self.mode != "off"

    @classmethod
    def from_env(cls) -> "HuaxinAssetConsolidationConfig":
        """从 ``HUAXIN_ASSET_CONSOLIDATION_*`` 环境变量构造配置。

        Returns:
            HuaxinAssetConsolidationConfig: 已校验配置；off 不要求任何路径和节点。

        Raises:
            ValueError: 启用后缺少身份、路径或数值边界时抛出。

        Side Effects:
            仅读取进程环境，不访问快照或状态文件。
        """

        values: Dict[str, Any] = {
            "mode": get_env("HUAXIN_ASSET_CONSOLIDATION_MODE", "off"),
            "source_mode": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_MODE"),
            "source_snapshot_path": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_SNAPSHOT"),
            "state_path": get_env("HUAXIN_ASSET_CONSOLIDATION_STATE_FILE"),
            "source_node_id": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_NODE_ID"),
            "target_node_id": get_env("HUAXIN_ASSET_CONSOLIDATION_TARGET_NODE_ID"),
            "source_role": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_ROLE"),
            "target_role": get_env("HUAXIN_ASSET_CONSOLIDATION_TARGET_ROLE"),
            "source_host": get_env("HUAXIN_ASSET_CONSOLIDATION_SOURCE_HOST"),
            "target_host": get_env("HUAXIN_ASSET_CONSOLIDATION_TARGET_HOST"),
            "earliest_time": get_env("HUAXIN_ASSET_CONSOLIDATION_EARLIEST_TIME", "09:00:00"),
            "snapshot_max_age_seconds": get_env(
                "HUAXIN_ASSET_CONSOLIDATION_SNAPSHOT_MAX_AGE_SECONDS"
            ),
            "stable_samples": get_env("HUAXIN_ASSET_CONSOLIDATION_STABLE_SAMPLES"),
            "stable_interval_seconds": get_env(
                "HUAXIN_ASSET_CONSOLIDATION_STABLE_INTERVAL_SECONDS"
            ),
            "poll_seconds": get_env("HUAXIN_ASSET_CONSOLIDATION_POLL_SECONDS"),
            "wait_timeout": get_env("HUAXIN_ASSET_CONSOLIDATION_WAIT_TIMEOUT"),
            "max_position_actions": get_env("HUAXIN_ASSET_CONSOLIDATION_MAX_POSITION_ACTIONS"),
            "max_position_volume": get_env("HUAXIN_ASSET_CONSOLIDATION_MAX_POSITION_VOLUME"),
            "max_fund_amount": get_env("HUAXIN_ASSET_CONSOLIDATION_MAX_FUND_AMOUNT"),
            "canary_position_volume": get_env("HUAXIN_ASSET_CONSOLIDATION_CANARY_POSITION_VOLUME"),
            "canary_fund_amount": get_env("HUAXIN_ASSET_CONSOLIDATION_CANARY_FUND_AMOUNT"),
        }
        return cls.from_mapping(values)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "HuaxinAssetConsolidationConfig":
        """从测试或调用方映射构造配置。

        Args:
            values: 与 dataclass 字段同名的配置映射。

        Returns:
            HuaxinAssetConsolidationConfig: 已校验配置。

        Raises:
            ValueError: 模式或启用配置不完整时抛出。
        """

        mode = str(values.get("mode") or "off").strip().lower()
        if mode not in _ALLOWED_MODES:
            raise ValueError("HUAXIN_ASSET_CONSOLIDATION_MODE 必须为 off/dry_run/canary/full")
        if mode == "off":
            return cls()

        def required_text(name: str) -> str:
            """读取启用模式必需的非空文本。

            Args:
                name: 映射字段名。

            Returns:
                str: 去除首尾空白的文本。

            Raises:
                ValueError: 值为空时抛出。
            """

            text = str(values.get(name) or "").strip()
            if not text:
                raise ValueError(f"启用华鑫资产归集时必须配置 {name}")
            return text

        source_node_id = _parse_positive_int(values.get("source_node_id"), "source_node_id", -1)
        target_node_id = _parse_positive_int(values.get("target_node_id"), "target_node_id", -1)
        if source_node_id == target_node_id:
            raise ValueError("source_node_id 与 target_node_id 不能相同")
        stable_samples = _parse_positive_int(values.get("stable_samples"), "stable_samples", 2)
        if stable_samples < 2:
            raise ValueError("stable_samples 至少为 2")
        source_host = required_text("source_host")
        target_host = required_text("target_host")
        source_mode = required_text("source_mode").lower()
        if source_mode not in {"external_snapshot", "direct_session"}:
            raise ValueError("source_mode 必须为 external_snapshot/direct_session")
        if source_mode == "external_snapshot" and source_host == target_host:
            raise ValueError("external_snapshot 必须绑定独立源节点主机")
        source_snapshot_path = values.get("source_snapshot_path")
        if source_mode == "external_snapshot":
            source_snapshot_path = required_text("source_snapshot_path")
        return cls(
            mode=mode,
            source_mode=source_mode,
            source_snapshot_path=(
                Path(str(source_snapshot_path)).expanduser() if source_snapshot_path else None
            ),
            state_path=Path(required_text("state_path")).expanduser(),
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            source_role=required_text("source_role"),
            target_role=required_text("target_role"),
            source_host=source_host,
            target_host=target_host,
            earliest_time=_parse_clock(str(values.get("earliest_time") or "09:00:00")),
            snapshot_max_age_seconds=_parse_nonnegative_float(
                values.get("snapshot_max_age_seconds"), "snapshot_max_age_seconds", 120.0
            ),
            stable_samples=stable_samples,
            stable_interval_seconds=_parse_nonnegative_float(
                values.get("stable_interval_seconds"), "stable_interval_seconds", 5.0
            ),
            poll_seconds=max(
                0.01,
                _parse_nonnegative_float(values.get("poll_seconds"), "poll_seconds", 5.0),
            ),
            wait_timeout=_parse_nonnegative_float(values.get("wait_timeout"), "wait_timeout", 20.0),
            max_position_actions=_parse_positive_int(
                values.get("max_position_actions"), "max_position_actions", 100
            ),
            max_position_volume=_parse_positive_int(
                values.get("max_position_volume"), "max_position_volume", 100000000
            ),
            max_fund_amount=_parse_nonnegative_float(
                values.get("max_fund_amount"), "max_fund_amount", 1000000000.0
            ),
            canary_position_volume=_parse_positive_int(
                values.get("canary_position_volume"), "canary_position_volume", 100
            ),
            canary_fund_amount=_parse_nonnegative_float(
                values.get("canary_fund_amount"), "canary_fund_amount", 100.0
            ),
        )


class HuaxinAssetConsolidationStateStore:
    """用单个 0600 原子 JSON 文档保存逐交易日归集计划。"""

    def __init__(self, path: Path) -> None:
        """保存状态文件路径但不立即访问文件。

        Args:
            path: 私有状态 JSON 路径。

        Returns:
            None。
        """

        self.path = path.expanduser()

    def load_day(self, trading_day: str) -> Optional[Dict[str, Any]]:
        """读取指定交易日计划。

        Args:
            trading_day: 八位柜台交易日。

        Returns:
            Optional[Dict[str, Any]]: 当日计划副本；文件或当日计划不存在时为 None。

        Raises:
            HuaxinAssetConsolidationError: 文件损坏或 schema 不符时抛出。
        """

        document = self._load_document()
        if document is None:
            return None
        plan = (document.get("plans") or {}).get(trading_day)
        if plan is None:
            return None
        if not isinstance(plan, Mapping):
            raise HuaxinAssetConsolidationError("归集状态中的当日计划不是对象")
        return json.loads(json.dumps(plan, ensure_ascii=False))

    def save_day(self, plan: Mapping[str, Any]) -> None:
        """原子保存当日计划并保留其他日期审计状态。

        Args:
            plan: 含 trading_day 的计划对象。

        Returns:
            None。

        Side Effects:
            创建 0700 父目录，并用 0600 临时文件原子替换状态文档。
        """

        trading_day = str(plan.get("trading_day") or "")
        if len(trading_day) != 8 or not trading_day.isdigit():
            raise HuaxinAssetConsolidationError("保存计划缺少有效交易日")
        document = self._load_document() or {"schema_version": 1, "plans": {}}
        plans = document.get("plans")
        if not isinstance(plans, MutableMapping):
            raise HuaxinAssetConsolidationError("归集状态 plans 必须为对象")
        plans[trading_day] = json.loads(json.dumps(plan, ensure_ascii=False))
        document["updated_at"] = plan.get("updated_at")
        self._atomic_write(document)

    def _load_document(self) -> Optional[Dict[str, Any]]:
        """读取并验证完整状态文档。

        Returns:
            Optional[Dict[str, Any]]: 状态文档；不存在时为 None。

        Raises:
            HuaxinAssetConsolidationError: 文件不可读或结构损坏时抛出。
        """

        path = self.path
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HuaxinAssetConsolidationError("归集状态文件损坏，拒绝自动重建") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 1
            or not isinstance(payload.get("plans"), dict)
        ):
            raise HuaxinAssetConsolidationError("归集状态文件 schema 无效")
        return payload

    def _atomic_write(self, payload: Mapping[str, Any]) -> None:
        """以 0600 权限原子写入状态文档。

        Args:
            payload: 可 JSON 序列化的完整状态对象。

        Returns:
            None。

        Side Effects:
            在同目录创建临时文件并调用 os.replace。
        """

        path = self.path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(str(temporary), str(path))
            os.chmod(path, 0o600)
        finally:
            if temporary.exists():
                temporary.unlink()


class ExternalHuaxinAssetSnapshotProvider:
    """从已安全同步到本机的私有 JSON 文件读取源节点权威快照。"""

    def __init__(self, path: Path) -> None:
        """保存快照路径但不立即读取。

        Args:
            path: 外部同步源快照路径。

        Returns:
            None。
        """

        self.path = path.expanduser()

    def get_snapshot(self) -> Dict[str, Any]:
        """读取一次完整源端快照。

        Returns:
            Dict[str, Any]: JSON 快照对象。

        Raises:
            HuaxinAssetConsolidationWaiting: 文件尚未出现时抛出。
            HuaxinAssetConsolidationError: 文件损坏时抛出。
        """

        if not self.path.exists():
            raise HuaxinAssetConsolidationWaiting("source_snapshot_missing")
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HuaxinAssetConsolidationError("源节点资产快照损坏") from exc
        if not isinstance(payload, dict):
            raise HuaxinAssetConsolidationError("源节点资产快照顶层必须为对象")
        return payload


class HuaxinAssetConsolidationCoordinator:
    """在单 writer 上串行推进每日资产归集状态机。"""

    def __init__(
        self,
        config: HuaxinAssetConsolidationConfig,
        *,
        source_snapshot_provider: Optional[Callable[[], Mapping[str, Any]]] = None,
        state_store: Optional[HuaxinAssetConsolidationStateStore] = None,
        clock: Optional[Callable[[], datetime]] = None,
        hostname: Optional[Callable[[], str]] = None,
    ) -> None:
        """创建协调器但不读写文件、不查询柜台。

        Args:
            config: 已校验归集配置。
            source_snapshot_provider: 测试或未来 direct session 注入的源快照函数。
            state_store: 可注入原子状态存储。
            clock: 可注入带时区当前时间函数。
            hostname: 可注入本机主机名函数。

        Returns:
            None。
        """

        if not config.enabled:
            raise ValueError("off 模式不应创建资产归集协调器")
        assert config.state_path is not None
        self.config = config
        if source_snapshot_provider is not None:
            self._source_snapshot_provider = source_snapshot_provider
        elif config.source_mode == "external_snapshot":
            assert config.source_snapshot_path is not None
            external = ExternalHuaxinAssetSnapshotProvider(config.source_snapshot_path)
            self._source_snapshot_provider = external.get_snapshot
        else:
            self._source_snapshot_provider = self._unsupported_direct_snapshot
        self._store = state_store or HuaxinAssetConsolidationStateStore(config.state_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc).astimezone())
        self._hostname = hostname or socket.gethostname
        self._lock = threading.RLock()
        self._health: Dict[str, Any] = {
            "enabled": True,
            "mode": config.mode,
            "source_mode": config.source_mode,
            "state": "observing",
            "reason": "waiting_for_read_only_prerequisites",
            "trading_day": None,
            "source_node_id": config.source_node_id,
            "target_node_id": config.target_node_id,
            "action_count": 0,
            "action_states": {},
            "updated_at": None,
        }
        self._observed_fingerprint: Optional[str] = None
        self._observed_source_time: Optional[datetime] = None
        self._stable_count = 0

    def _unsupported_direct_snapshot(self) -> Mapping[str, Any]:
        """在 direct session provider 尚未注入时保持失败关闭。

        Returns:
            Mapping[str, Any]: 本实现不会返回。

        Raises:
            HuaxinAssetConsolidationError: 始终抛出，阻止未完成路径进入生产写入。
        """

        raise HuaxinAssetConsolidationError("direct_session_source_provider_unsupported")

    @property
    def poll_seconds(self) -> float:
        """返回后台循环的只读轮询间隔。

        Returns:
            float: 秒数。
        """

        return self.config.poll_seconds

    def order_allowed(self) -> bool:
        """返回当前是否已经完成当日全量归集。

        Returns:
            bool: 仅 full 模式当日状态 complete 时为 True。
        """

        with self._lock:
            return self._health.get("state") == "complete"

    def health_snapshot(self) -> Dict[str, Any]:
        """返回不含账户、股东、主机和文件路径的健康摘要。

        Returns:
            Dict[str, Any]: 可公开到 Server health 的隔离副本。
        """

        with self._lock:
            return json.loads(json.dumps(self._health, ensure_ascii=False))

    def blocked_order_result(self) -> Dict[str, Any]:
        """生成归集期间稳定的新下单拒绝响应。

        Returns:
            Dict[str, Any]: 不含账户身份的 rejected 响应。
        """

        health = self.health_snapshot()
        return {
            "value": False,
            "success": False,
            "status": "rejected",
            "submission_state": "rejected",
            "reason": HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED,
            "provider_extension": {
                "huaxin_tora": {
                    "asset_consolidation_state": health.get("state"),
                    "trading_day": health.get("trading_day"),
                }
            },
        }

    def record_runtime_error(self, exc: BaseException) -> None:
        """把后台意外错误投影为脱敏阻断状态。

        Args:
            exc: 后台循环捕获的异常。

        Returns:
            None。

        Side Effects:
            只更新内存健康状态，不写文件、不发起请求。
        """

        self._set_health("blocked", f"runtime_error:{type(exc).__name__}")

    def drive_once(self, broker: Any) -> Dict[str, Any]:
        """执行一轮只读观察、至多一次写入或一次对账推进。

        Args:
            broker: 已达到查询 ready 的目标节点 HuaxinBroker。

        Returns:
            Dict[str, Any]: 本轮结束后的脱敏健康摘要。

        Side Effects:
            full/canary 模式在状态先落盘后至多调用一次节点划拨；其他重查均只读。
        """

        try:
            self._drive_once(broker)
        except HuaxinAssetConsolidationWaiting as exc:
            current = str(self.health_snapshot().get("state") or "")
            state = (
                current if current in {"transferring", "reconciling", "unknown"} else "observing"
            )
            self._set_health(state, str(exc))
        except HuaxinAssetConsolidationError as exc:
            self._set_health("blocked", str(exc))
        return self.health_snapshot()

    def _drive_once(self, broker: Any) -> None:
        """实现单轮归集状态推进。

        Args:
            broker: 目标节点唯一 writer。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationWaiting: 可安全重查的前置暂未满足时抛出。
            HuaxinAssetConsolidationError: 合同冲突或资产不一致时抛出。
        """

        now = self._clock()
        if now.tzinfo is None:
            raise HuaxinAssetConsolidationError("协调器 clock 必须返回带时区时间")
        if now.timetz().replace(tzinfo=None) < self.config.earliest_time:
            raise HuaxinAssetConsolidationWaiting("before_earliest_time")

        source = dict(self._source_snapshot_provider())
        source_time = self._validate_source_snapshot(source, now)
        target = self._capture_target_snapshot(broker, now)
        trading_day = str(source["trading_day"])
        if str(target.get("trading_day") or "") != trading_day:
            raise HuaxinAssetConsolidationError("source_target_trading_day_mismatch")

        plan = self._store.load_day(trading_day)
        if plan is None:
            if not self._observe_stability(source, target, source_time):
                self._set_health("observing", "stable_samples_pending", trading_day=trading_day)
                return
            plan = self._build_plan(source, target, now)
            self._store.save_day(plan)
        else:
            self._validate_existing_plan(plan, trading_day)

        self._publish_plan_health(plan)
        if plan.get("state") == "complete":
            if self._source_has_transferable_assets(source):
                raise HuaxinAssetConsolidationError("complete_plan_has_new_source_residual")
            return
        if plan.get("state") in {"canary_complete", "dry_run"}:
            return
        actions = [row for row in plan.get("actions") or [] if isinstance(row, MutableMapping)]
        if len(actions) != len(plan.get("actions") or []):
            raise HuaxinAssetConsolidationError("计划 actions 元素必须为对象")
        current = next((row for row in actions if row.get("state") != "succeeded"), None)
        if current is None:
            self._finish_plan(plan, source, target, now)
            return

        action_state = str(current.get("state") or "")
        if action_state == "planned":
            if self.config.mode == "dry_run":
                plan["state"] = "dry_run"
                plan["updated_at"] = _now_iso(now)
                self._store.save_day(plan)
                self._publish_plan_health(plan)
                return
            self._submit_action_once(broker, plan, current, source, now)
            return
        if action_state in _RECOVER_ONLY_ACTION_STATES:
            self._recover_unknown_action(broker, plan, current, now)
            return
        if action_state == "reconciling":
            self._reconcile_action(broker, plan, current, source, target, now)
            return
        if action_state == "rejected":
            raise HuaxinAssetConsolidationError("transfer_rejected_requires_manual_review")
        raise HuaxinAssetConsolidationError(f"unsupported_action_state:{action_state or 'empty'}")

    def _validate_source_snapshot(self, snapshot: Mapping[str, Any], now: datetime) -> datetime:
        """验证源快照的 schema、角色、主机、节点和新鲜度。

        Args:
            snapshot: 外部或注入的源端完整快照。
            now: 当前带时区时间。

        Returns:
            datetime: 源快照采集时间。

        Raises:
            HuaxinAssetConsolidationError: 任一来源证明不一致时抛出。
            HuaxinAssetConsolidationWaiting: 快照过期时抛出。
        """

        if snapshot.get("schema_version") != 1 or snapshot.get("state") != "captured":
            raise HuaxinAssetConsolidationError("source_snapshot_schema_invalid")
        if str(snapshot.get("role") or "") != self.config.source_role:
            raise HuaxinAssetConsolidationError("source_snapshot_role_mismatch")
        if str(snapshot.get("host") or "") != self.config.source_host:
            raise HuaxinAssetConsolidationError("source_snapshot_host_mismatch")
        if _as_int(snapshot.get("node_id"), -1) != self.config.source_node_id:
            raise HuaxinAssetConsolidationError("source_snapshot_node_mismatch")
        trading_day = str(snapshot.get("trading_day") or "")
        if len(trading_day) != 8 or not trading_day.isdigit():
            raise HuaxinAssetConsolidationError("source_snapshot_trading_day_invalid")
        captured_at = _parse_datetime(snapshot.get("captured_at"))
        age = (now.astimezone(timezone.utc) - captured_at.astimezone(timezone.utc)).total_seconds()
        if age < -5:
            raise HuaxinAssetConsolidationError("source_snapshot_time_in_future")
        if age > self.config.snapshot_max_age_seconds:
            raise HuaxinAssetConsolidationWaiting("source_snapshot_stale")
        self._validate_snapshot_node_provenance(snapshot, self.config.source_node_id)
        if not isinstance(snapshot.get("account"), Mapping):
            raise HuaxinAssetConsolidationError("source_snapshot_account_missing")
        if not isinstance(snapshot.get("positions"), list):
            raise HuaxinAssetConsolidationError("source_snapshot_positions_invalid")
        if not isinstance(snapshot.get("shareholder_accounts"), list):
            raise HuaxinAssetConsolidationError("source_snapshot_shareholders_invalid")
        return captured_at

    def _validate_snapshot_node_provenance(
        self, snapshot: Mapping[str, Any], expected_node_id: Optional[int]
    ) -> None:
        """验证快照节点证明没有伪造厂商 current。

        Args:
            snapshot: 源或目标资产快照。
            expected_node_id: 私密配置中的预期节点编号。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationError: 节点记录冲突或 provenance 不合法时抛出。
        """

        node = snapshot.get("node")
        if not isinstance(node, Mapping):
            raise HuaxinAssetConsolidationError("snapshot_node_provenance_missing")
        if _as_int(node.get("node_id"), -1) != expected_node_id:
            raise HuaxinAssetConsolidationError("snapshot_node_provenance_mismatch")
        provenance = str(node.get("provenance") or "")
        allowed = {
            "vendor_current",
            "vendor_catalog_expected",
            "configured_session_fallback",
        }
        if provenance not in allowed:
            raise HuaxinAssetConsolidationError("snapshot_node_provenance_invalid")
        if provenance == "configured_session_fallback" and bool(node.get("current")):
            raise HuaxinAssetConsolidationError("configured_fallback_must_not_claim_current")

    def _capture_target_snapshot(self, broker: Any, now: datetime) -> Dict[str, Any]:
        """从目标 writer 同步取得权威只读资产快照。

        Args:
            broker: 目标节点唯一 writer。
            now: 本轮采集时间。

        Returns:
            Dict[str, Any]: 仅在内存中保存的目标端完整快照。

        Raises:
            HuaxinAssetConsolidationError: 主机、节点或交易日无法证明时抛出。
        """

        actual_host = str(self._hostname() or "").strip()
        if actual_host != self.config.target_host:
            raise HuaxinAssetConsolidationError("target_writer_host_mismatch")
        nodes = list(broker.get_system_nodes() or [])
        current = [row for row in nodes if isinstance(row, Mapping) and bool(row.get("current"))]
        if len(current) > 1:
            raise HuaxinAssetConsolidationError("target_current_node_duplicated")
        expected = [
            row
            for row in nodes
            if isinstance(row, Mapping)
            and _as_int(row.get("node_id"), -1) == self.config.target_node_id
        ]
        if current:
            if _as_int(current[0].get("node_id"), -1) != self.config.target_node_id:
                raise HuaxinAssetConsolidationError("target_writer_node_mismatch")
            node = dict(current[0], provenance="vendor_current")
        elif nodes:
            if len(expected) != 1:
                raise HuaxinAssetConsolidationError("target_node_catalog_ambiguous")
            node = dict(expected[0], provenance="vendor_catalog_expected")
        else:
            node = {
                "node_id": self.config.target_node_id,
                "node_info": "",
                "current": False,
                "provenance": "configured_session_fallback",
            }
        if nodes and not any(
            isinstance(row, Mapping)
            and _as_int(row.get("node_id"), -1) == self.config.source_node_id
            for row in nodes
        ):
            raise HuaxinAssetConsolidationError("source_node_missing_from_target_catalog")
        trading_day = str(broker.get_trading_day() or "").strip()
        if len(trading_day) != 8 or not trading_day.isdigit():
            raise HuaxinAssetConsolidationError("target_trading_day_invalid")
        snapshot = {
            "schema_version": 1,
            "state": "captured",
            "role": self.config.target_role,
            "host": actual_host,
            "node_id": self.config.target_node_id,
            "node": node,
            "nodes": nodes,
            "trading_day": trading_day,
            "captured_at": now.isoformat(timespec="microseconds"),
            "account": dict(broker.get_account_info() or {}),
            "positions": list(broker.get_positions() or []),
            "shareholder_accounts": list(broker.get_shareholder_accounts(refresh=True) or []),
        }
        self._validate_snapshot_node_provenance(snapshot, self.config.target_node_id)
        return snapshot

    def _observe_stability(
        self,
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        source_time: datetime,
    ) -> bool:
        """累计内容一致且采集时刻递增的双端稳定采样。

        Args:
            source: 本轮源端快照。
            target: 本轮目标端快照。
            source_time: 源端权威采集时间。

        Returns:
            bool: 已达到配置稳定采样次数时为 True。
        """

        fingerprint = self._observation_fingerprint(source, target)
        if self._observed_fingerprint != fingerprint:
            self._observed_fingerprint = fingerprint
            self._observed_source_time = source_time
            self._stable_count = 1
            return self._stable_count >= self.config.stable_samples
        previous_time = self._observed_source_time
        if previous_time is None or source_time <= previous_time:
            return False
        elapsed = (source_time - previous_time).total_seconds()
        if elapsed < self.config.stable_interval_seconds:
            return False
        self._observed_source_time = source_time
        self._stable_count += 1
        return self._stable_count >= self.config.stable_samples

    def _observation_fingerprint(self, source: Mapping[str, Any], target: Mapping[str, Any]) -> str:
        """计算双端可划资产和身份集合的稳定采样指纹。

        Args:
            source: 源节点快照。
            target: 目标节点快照。

        Returns:
            str: 不可逆 SHA-256 摘要。
        """

        def snapshot_material(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
            """抽取影响计划和对账的稳定字段。

            Args:
                snapshot: 单节点完整资产快照。

            Returns:
                Dict[str, Any]: 不含采集时间和身份明文的摘要材料。
            """

            account = snapshot.get("account") or {}
            positions = [row for row in snapshot.get("positions") or [] if isinstance(row, Mapping)]
            position_material = sorted(
                (
                    _position_key(row),
                    _row_fingerprint(row, "position"),
                    _as_int(row.get("current_position")),
                    _as_int(row.get("available_position")),
                    _as_int(row.get("history_position")),
                )
                for row in positions
            )
            shareholders = [
                row
                for row in snapshot.get("shareholder_accounts") or []
                if isinstance(row, Mapping)
            ]
            shareholder_hashes = sorted(
                hashlib.sha256(
                    json.dumps(
                        [
                            str(row.get("exchange") or ""),
                            str(row.get("investor_id") or ""),
                            str(row.get("shareholder_id") or ""),
                        ],
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                for row in shareholders
            )
            return {
                "trading_day": snapshot.get("trading_day"),
                "node_id": snapshot.get("node_id"),
                "account_fingerprint": _row_fingerprint(account, "fund"),
                "transferable_cash": _as_float(account.get("transferable_cash")),
                "positions": position_material,
                "shareholders": shareholder_hashes,
            }

        material = {
            "source": snapshot_material(source),
            "target": snapshot_material(target),
        }
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _build_plan(
        self,
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        now: datetime,
    ) -> Dict[str, Any]:
        """从已稳定双端快照冻结证券后资金的单向 NodeMoveIn 计划。

        Args:
            source: 源节点权威快照。
            target: 目标 writer 权威快照。
            now: 计划创建时间。

        Returns:
            Dict[str, Any]: 不含完整账户和股东身份的持久计划。

        Raises:
            HuaxinAssetConsolidationError: 身份、仓位类型或上限不满足时抛出。
        """

        trading_day = str(source.get("trading_day"))
        plan_id = uuid.uuid4().hex
        used: Set[int] = set()
        actions: List[Dict[str, Any]] = []
        source_positions = [
            dict(row) for row in source.get("positions") or [] if isinstance(row, Mapping)
        ]
        source_positions.sort(
            key=lambda row: (_position_key(row), _row_fingerprint(row, "position"))
        )
        positive_positions = [
            row for row in source_positions if _as_int(row.get("available_position")) > 0
        ]
        if len(positive_positions) > self.config.max_position_actions:
            raise HuaxinAssetConsolidationError("position_action_count_exceeds_configured_limit")
        target_positions = [
            row for row in target.get("positions") or [] if isinstance(row, Mapping)
        ]
        for row in positive_positions:
            self._validate_position_identity(row, source.get("shareholder_accounts") or [])
            available = _as_int(row.get("available_position"), -1)
            history = _as_int(row.get("history_position"), -1)
            if history < available:
                raise HuaxinAssetConsolidationError("available_position_not_proven_as_history")
            if available > self.config.max_position_volume:
                raise HuaxinAssetConsolidationError("position_volume_exceeds_configured_limit")
            planned_volume = available
            if self.config.mode == "canary":
                planned_volume = min(planned_volume, self.config.canary_position_volume)
            fingerprint = _row_fingerprint(row, "position")
            key = _position_key(row)
            target_before = sum(
                _as_int(item.get("current_position"))
                for item in target_positions
                if _position_key(item) == key
            )
            action_key = f"{plan_id}|position|{key}|{fingerprint}"
            actions.append(
                {
                    "kind": "position",
                    "asset_key": key,
                    "security": str(row.get("security") or ""),
                    "exchange": str(row.get("exchange") or ""),
                    "volume": planned_volume,
                    "transfer_position_type": "history",
                    "source_fingerprint": fingerprint,
                    "source_before": available,
                    "target_before": target_before,
                    "apply_serial": _apply_serial(trading_day, action_key, used),
                    "state": "planned",
                }
            )

        source_account = source.get("account") or {}
        self._validate_fund_identity(source_account)
        transferable = Decimal(str(_as_float(source_account.get("transferable_cash")))).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )
        if transferable > Decimal(str(self.config.max_fund_amount)):
            raise HuaxinAssetConsolidationError("fund_amount_exceeds_configured_limit")
        if transferable > 0:
            planned_amount = transferable
            if self.config.mode == "canary":
                planned_amount = min(
                    planned_amount,
                    Decimal(str(self.config.canary_fund_amount)).quantize(Decimal("0.01")),
                )
            if planned_amount > 0:
                fingerprint = _row_fingerprint(source_account, "fund")
                actions.append(
                    {
                        "kind": "fund",
                        "amount": float(planned_amount),
                        "source_fingerprint": fingerprint,
                        "source_before": float(transferable),
                        "target_before": _as_float(
                            (target.get("account") or {}).get("transferable_cash")
                        ),
                        "apply_serial": _apply_serial(
                            trading_day, f"{plan_id}|fund|{fingerprint}", used
                        ),
                        "state": "planned",
                    }
                )
        frozen_cash = _as_float(source_account.get("frozen_cash"))
        frozen_positions = sum(
            max(0, _as_int(row.get("current_position")) - _as_int(row.get("available_position")))
            for row in source_positions
        )
        return {
            "schema_version": 1,
            "plan_id": plan_id,
            "mode": self.config.mode,
            "state": "planned",
            "source_node_id": self.config.source_node_id,
            "target_node_id": self.config.target_node_id,
            "trading_day": trading_day,
            "created_at": _now_iso(now),
            "updated_at": _now_iso(now),
            "source_snapshot_at": source.get("captured_at"),
            "target_snapshot_at": target.get("captured_at"),
            "actions": actions,
            "residuals": {
                "source_frozen_cash": frozen_cash,
                "source_nontransferable_position": frozen_positions,
            },
        }

    def _validate_fund_identity(self, account: Mapping[str, Any]) -> None:
        """验证资金划拨身份全部来自同一资金行。

        Args:
            account: 源端资金行。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationError: 同行必填字段缺失时抛出。
        """

        missing = [
            field
            for field in ("department_id", "account_id", "currency")
            if not str(account.get(field) or "").strip()
        ]
        if missing:
            raise HuaxinAssetConsolidationError("source_fund_identity_incomplete")

    def _validate_position_identity(
        self, position: Mapping[str, Any], shareholder_rows: Sequence[Any]
    ) -> None:
        """验证持仓同行身份并与股东账户查询交叉核对。

        Args:
            position: 源端单一持仓行。
            shareholder_rows: 同一源会话的股东账户行。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationError: 身份缺失或交叉核对不唯一时抛出。
        """

        text_fields = (
            "exchange",
            "investor_id",
            "business_unit_id",
            "shareholder_id",
            "security",
        )
        if any(not str(position.get(field) or "").strip() for field in text_fields):
            raise HuaxinAssetConsolidationError("source_position_identity_incomplete")
        if _as_int(position.get("market_id"), -1) < 0:
            raise HuaxinAssetConsolidationError("source_position_market_id_invalid")
        matched = []
        for row in shareholder_rows:
            if not isinstance(row, Mapping):
                continue
            if str(row.get("shareholder_id") or "") != str(position.get("shareholder_id") or ""):
                continue
            if str(row.get("exchange") or "") != str(position.get("exchange") or ""):
                continue
            row_investor = str(row.get("investor_id") or "")
            if row_investor and row_investor != str(position.get("investor_id") or ""):
                continue
            matched.append(row)
        if len(matched) != 1:
            raise HuaxinAssetConsolidationError("source_position_shareholder_identity_conflict")

    def _validate_existing_plan(self, plan: Mapping[str, Any], trading_day: str) -> None:
        """验证恢复计划不会跨模式、跨节点或重建 ApplySerial。

        Args:
            plan: 状态文件读取的当日计划。
            trading_day: 本轮权威交易日。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationError: 计划合同冲突时抛出。
        """

        if plan.get("schema_version") != 1 or str(plan.get("trading_day")) != trading_day:
            raise HuaxinAssetConsolidationError("existing_plan_schema_or_day_mismatch")
        if plan.get("mode") != self.config.mode:
            raise HuaxinAssetConsolidationError("existing_plan_mode_mismatch")
        if _as_int(plan.get("source_node_id"), -1) != self.config.source_node_id:
            raise HuaxinAssetConsolidationError("existing_plan_source_node_mismatch")
        if _as_int(plan.get("target_node_id"), -1) != self.config.target_node_id:
            raise HuaxinAssetConsolidationError("existing_plan_target_node_mismatch")
        actions = plan.get("actions")
        if not isinstance(actions, list):
            raise HuaxinAssetConsolidationError("existing_plan_actions_invalid")
        serials = [
            _as_int(row.get("apply_serial"), -1) for row in actions if isinstance(row, Mapping)
        ]
        if len(serials) != len(actions) or any(value <= 0 for value in serials):
            raise HuaxinAssetConsolidationError("existing_plan_apply_serial_invalid")
        if len(set(serials)) != len(serials):
            raise HuaxinAssetConsolidationError("existing_plan_apply_serial_duplicated")

    def _find_source_row(
        self, source: Mapping[str, Any], action: Mapping[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """按持久身份指纹找回当前源端同行记录。

        Args:
            source: 当前源端权威快照。
            action: 冻结动作。

        Returns:
            Optional[Dict[str, Any]]: 唯一匹配行；资产行已消失时为 None。

        Raises:
            HuaxinAssetConsolidationError: 同一指纹出现重复行时抛出。
        """

        kind = str(action.get("kind") or "")
        rows: Sequence[Any]
        if kind == "fund":
            rows = [source.get("account") or {}]
        else:
            rows = source.get("positions") or []
        matched = [
            dict(row)
            for row in rows
            if isinstance(row, Mapping)
            and _row_fingerprint(row, kind) == str(action.get("source_fingerprint") or "")
        ]
        if len(matched) > 1:
            raise HuaxinAssetConsolidationError("source_identity_fingerprint_duplicated")
        return matched[0] if matched else None

    def _submit_action_once(
        self,
        broker: Any,
        plan: MutableMapping[str, Any],
        action: MutableMapping[str, Any],
        source: Mapping[str, Any],
        now: datetime,
    ) -> None:
        """在先落盘后对当前 planned 动作最多提交一次。

        Args:
            broker: 目标节点唯一 writer。
            plan: 可变当日计划。
            action: 当前 planned 动作。
            source: 当前源端权威快照。
            now: 本轮当前时间。

        Returns:
            None。

        Side Effects:
            先持久化 submit_started，再至多调用一次 Broker NodeMoveIn 原语。
        """

        source_row = self._find_source_row(source, action)
        if source_row is None:
            raise HuaxinAssetConsolidationError("planned_source_row_missing_before_submit")
        if action.get("kind") == "fund":
            if _as_float(source_row.get("transferable_cash")) + 0.001 < _as_float(
                action.get("amount")
            ):
                raise HuaxinAssetConsolidationError("source_fund_below_frozen_plan")
        elif _as_int(source_row.get("available_position")) < _as_int(action.get("volume")):
            raise HuaxinAssetConsolidationError("source_position_below_frozen_plan")

        action["state"] = "submit_started"
        action["submit_started_at"] = _now_iso(now)
        plan["state"] = "transferring"
        plan["updated_at"] = _now_iso(now)
        self._store.save_day(plan)
        self._publish_plan_health(plan)
        self._complete_submitted_action(broker, plan, action, source_row)

    def _source_has_transferable_assets(self, source: Mapping[str, Any]) -> bool:
        """判断源端是否仍有正数可划资金或证券。

        Args:
            source: 新鲜源节点快照。

        Returns:
            bool: 任一计划范围资产仍可划时为 True。
        """

        if _as_float((source.get("account") or {}).get("transferable_cash")) > 0.01:
            return True
        return any(
            isinstance(row, Mapping) and _as_int(row.get("available_position")) > 0
            for row in source.get("positions") or []
        )

    def _finish_plan(
        self,
        plan: MutableMapping[str, Any],
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        now: datetime,
    ) -> None:
        """在全部动作已逐项对账后执行最终源端残留复查。

        Args:
            plan: 可变当日计划。
            source: 最新源端快照。
            target: 最新目标端快照。
            now: 本轮当前时间。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationError: full 模式仍有可划残留时抛出。

        Side Effects:
            原子持久化 complete 或 canary_complete 状态。
        """

        del target
        if self.config.mode == "canary":
            plan["state"] = "canary_complete"
            plan["reason"] = "canary_reconciled_full_mode_not_authorized"
        elif self._source_has_transferable_assets(source):
            raise HuaxinAssetConsolidationError("source_transferable_residual_after_full_plan")
        else:
            plan["state"] = "complete"
            plan.pop("reason", None)
        plan["completed_at"] = _now_iso(now)
        plan["updated_at"] = _now_iso(now)
        self._store.save_day(plan)
        self._publish_plan_health(plan)

    def _publish_plan_health(self, plan: Mapping[str, Any]) -> None:
        """把持久计划投影为脱敏内存健康状态。

        Args:
            plan: 当日完整状态计划。

        Returns:
            None。

        Side Effects:
            更新线程安全内存摘要，不写文件。
        """

        actions = [row for row in plan.get("actions") or [] if isinstance(row, Mapping)]
        counts: Dict[str, int] = {}
        for action in actions:
            state = str(action.get("state") or "unknown")
            counts[state] = counts.get(state, 0) + 1
        state = str(plan.get("state") or "blocked")
        reason = plan.get("reason")
        if state == "planned":
            reason = "asset_plan_waiting_for_transfer"
        elif state == "transferring":
            reason = "asset_transfer_in_progress"
        elif state == "reconciling":
            reason = "source_target_reconciliation_pending"
        elif state == "unknown":
            reason = "transfer_fact_unknown_query_only"
        elif state == "dry_run":
            reason = "dry_run_never_submits_transfer"
        elif state == "canary_complete":
            reason = "canary_complete_full_consolidation_still_blocked"
        elif state == "complete":
            reason = None
        with self._lock:
            self._health = {
                "enabled": True,
                "mode": self.config.mode,
                "source_mode": self.config.source_mode,
                "state": state,
                "reason": reason,
                "trading_day": plan.get("trading_day"),
                "source_node_id": self.config.source_node_id,
                "target_node_id": self.config.target_node_id,
                "action_count": len(actions),
                "action_states": counts,
                "updated_at": plan.get("updated_at"),
            }

    def _set_health(
        self,
        state: str,
        reason: Optional[str],
        *,
        trading_day: Optional[str] = None,
    ) -> None:
        """更新脱敏健康状态而不覆盖已有计划统计。

        Args:
            state: observing/blocked 等归集状态。
            reason: 稳定英文原因，不含异常原文或身份。
            trading_day: 可选权威交易日。

        Returns:
            None。

        Side Effects:
            原子替换内存摘要字段。
        """

        with self._lock:
            health = dict(self._health)
            health["state"] = state
            health["reason"] = reason
            if trading_day is not None:
                health["trading_day"] = trading_day
            health["updated_at"] = _now_iso(self._clock())
            self._health = health

    def _complete_submitted_action(
        self,
        broker: Any,
        plan: MutableMapping[str, Any],
        action: MutableMapping[str, Any],
        source_row: Mapping[str, Any],
    ) -> None:
        """完成已经落盘 submit_started 的唯一 native 调用和结果记录。

        Args:
            broker: 目标节点唯一 writer。
            plan: 可变当日计划。
            action: 已持久化 submit_started 的动作。
            source_row: 当前源端同行完整身份。

        Returns:
            None。

        Side Effects:
            至多调用一次划拨原语，并将响应原子保存为 reconciling/rejected/unknown。
        """

        try:
            result = self._call_transfer_primitive(broker, source_row, action)
        except BaseException as exc:
            action["state"] = "unknown"
            action["result_reason"] = f"native_call_exception:{type(exc).__name__}"
            action["finished_at"] = _now_iso(self._clock())
            plan["state"] = "unknown"
            plan["updated_at"] = action["finished_at"]
            self._store.save_day(plan)
            self._publish_plan_health(plan)
            return
        outcome = str(result.get("submission_state") or result.get("status") or "unknown")
        if outcome == "succeeded":
            action["state"] = "reconciling"
            plan["state"] = "reconciling"
        elif outcome == "rejected":
            action["state"] = "rejected"
            plan["state"] = "blocked"
        else:
            action["state"] = "unknown"
            plan["state"] = "unknown"
        action["finished_at"] = _now_iso(self._clock())
        self._record_result(action, result)
        plan["updated_at"] = action["finished_at"]
        self._store.save_day(plan)
        self._publish_plan_health(plan)

    def _call_transfer_primitive(
        self, broker: Any, source_row: Mapping[str, Any], action: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """调用与动作类型对应的单次 NodeMoveIn 原语。

        Args:
            broker: 目标节点唯一 writer。
            source_row: 当前源端同行完整身份。
            action: 已落盘 submit_started 的动作。

        Returns:
            Dict[str, Any]: Broker 的 succeeded/rejected/unknown 响应。

        Side Effects:
            对柜台最多发起一次节点调入请求。
        """

        common = {
            "apply_serial": _as_int(action.get("apply_serial"), -1),
            "external_node_id": self.config.source_node_id,
            "transfer_direction": "node_move_in",
            "wait_timeout": self.config.wait_timeout,
        }
        if action.get("kind") == "fund":
            return dict(
                broker.submit_fund_transfer(
                    source_row,
                    amount=_as_float(action.get("amount")),
                    **common,
                )
                or {}
            )
        return dict(
            broker.submit_position_transfer(
                source_row,
                volume=_as_int(action.get("volume")),
                transfer_position_type="history",
                **common,
            )
            or {}
        )

    def _record_result(self, action: MutableMapping[str, Any], result: Mapping[str, Any]) -> None:
        """保存不含账户身份的最小柜台结果证据。

        Args:
            action: 当前可变动作状态。
            result: Broker 划拨响应。

        Returns:
            None。

        Side Effects:
            仅修改内存动作对象。
        """

        detail = result.get("detail")
        detail_map = dict(detail) if isinstance(detail, Mapping) else {}
        error_id = _as_int(result.get("error_id"))
        if error_id:
            action["result_error_id"] = error_id
        status = str(detail_map.get("transfer_status") or "").strip()
        if status:
            action["result_transfer_status"] = status
        reason = str(
            result.get("reason")
            or result.get("error_message")
            or detail_map.get("status_message")
            or ""
        ).strip()
        if reason:
            action["result_reason"] = reason

    def _query_action_fact(self, broker: Any, action: Mapping[str, Any]) -> str:
        """按原 ApplySerial 查询既有划拨终态。

        Args:
            broker: 目标节点 writer 查询会话。
            action: submit_started/unknown/reconciling 动作。

        Returns:
            str: succeeded、rejected 或 unknown。
        """

        filters = {"apply_serial": _as_int(action.get("apply_serial"), -1)}
        rows = (
            broker.get_fund_transfer_details(filters)
            if action.get("kind") == "fund"
            else broker.get_position_transfer_details(filters)
        )
        states = {
            str(row.get("transfer_status") or "").strip().lower()
            for row in rows or []
            if isinstance(row, Mapping)
        }
        if states.intersection(_SUCCESS_TRANSFER_STATES):
            return "succeeded"
        if states.intersection(_FAILED_TRANSFER_STATES):
            return "rejected"
        return "unknown"

    def _recover_unknown_action(
        self,
        broker: Any,
        plan: MutableMapping[str, Any],
        action: MutableMapping[str, Any],
        now: datetime,
    ) -> None:
        """恢复旧未决动作时只查原流水，绝不再次提交。

        Args:
            broker: 目标节点 writer 查询会话。
            plan: 可变当日计划。
            action: submit_started 或 unknown 动作。
            now: 本轮当前时间。

        Returns:
            None。

        Side Effects:
            只查询划拨明细并原子更新状态，不调用任何划拨写接口。
        """

        fact = self._query_action_fact(broker, action)
        action["detail_checked_at"] = _now_iso(now)
        if fact == "succeeded":
            action["state"] = "reconciling"
            plan["state"] = "reconciling"
        elif fact == "rejected":
            action["state"] = "rejected"
            plan["state"] = "blocked"
        else:
            action["state"] = "unknown"
            plan["state"] = "unknown"
        plan["updated_at"] = _now_iso(now)
        self._store.save_day(plan)
        self._publish_plan_health(plan)

    def _reconcile_action(
        self,
        broker: Any,
        plan: MutableMapping[str, Any],
        action: MutableMapping[str, Any],
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        now: datetime,
    ) -> None:
        """同时核对 ApplySerial 成功明细和双端精确资产变化。

        Args:
            broker: 目标节点 writer 查询会话。
            plan: 可变当日计划。
            action: 当前 reconciling 动作。
            source: 新鲜源端快照。
            target: 新鲜目标端快照。
            now: 本轮当前时间。

        Returns:
            None。

        Raises:
            HuaxinAssetConsolidationWaiting: 明细或快照尚未传播时抛出。
            HuaxinAssetConsolidationError: 明细失败或资产差额不一致时抛出。
        """

        fact = self._query_action_fact(broker, action)
        if fact == "unknown":
            raise HuaxinAssetConsolidationWaiting("matching_transfer_detail_pending")
        if fact == "rejected":
            action["state"] = "rejected"
            plan["state"] = "blocked"
            plan["updated_at"] = _now_iso(now)
            self._store.save_day(plan)
            self._publish_plan_health(plan)
            return
        submitted_at = _parse_datetime(action.get("submit_started_at"))
        source_at = _parse_datetime(source.get("captured_at"))
        if source_at <= submitted_at:
            raise HuaxinAssetConsolidationWaiting("post_transfer_source_snapshot_pending")

        source_row = self._find_source_row(source, action)
        if action.get("kind") == "fund":
            source_after = _as_float((source_row or {}).get("transferable_cash"))
            target_after = _as_float((target.get("account") or {}).get("transferable_cash"))
            amount = _as_float(action.get("amount"))
            expected_source = _as_float(action.get("source_before")) - amount
            expected_target = _as_float(action.get("target_before")) + amount
            unchanged = math.isclose(
                source_after, _as_float(action.get("source_before")), abs_tol=0.01
            ) and math.isclose(target_after, _as_float(action.get("target_before")), abs_tol=0.01)
            matched = math.isclose(source_after, expected_source, abs_tol=0.01) and math.isclose(
                target_after, expected_target, abs_tol=0.01
            )
        else:
            source_after = _as_int((source_row or {}).get("available_position"))
            target_after = sum(
                _as_int(row.get("current_position"))
                for row in target.get("positions") or []
                if isinstance(row, Mapping) and _position_key(row) == action.get("asset_key")
            )
            volume = _as_int(action.get("volume"))
            expected_source = _as_int(action.get("source_before")) - volume
            expected_target = _as_int(action.get("target_before")) + volume
            unchanged = source_after == _as_int(
                action.get("source_before")
            ) and target_after == _as_int(action.get("target_before"))
            matched = source_after == expected_source and target_after == expected_target
        if unchanged:
            raise HuaxinAssetConsolidationWaiting("post_transfer_asset_change_pending")
        if not matched:
            raise HuaxinAssetConsolidationError("source_target_asset_delta_mismatch")
        action["state"] = "succeeded"
        action["reconciled_at"] = _now_iso(now)
        action["source_after"] = source_after
        action["target_after"] = target_after
        remaining = [row for row in plan.get("actions") or [] if row.get("state") != "succeeded"]
        plan["state"] = "planned" if remaining else "reconciling"
        plan["updated_at"] = _now_iso(now)
        self._store.save_day(plan)
        self._publish_plan_health(plan)


__all__ = [
    "ExternalHuaxinAssetSnapshotProvider",
    "HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED",
    "HuaxinAssetConsolidationConfig",
    "HuaxinAssetConsolidationCoordinator",
    "HuaxinAssetConsolidationError",
    "HuaxinAssetConsolidationStateStore",
    "HuaxinAssetConsolidationWaiting",
]
