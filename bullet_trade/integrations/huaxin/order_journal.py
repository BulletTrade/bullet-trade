"""
作者: BruceLee

文件职责: 为华鑫 Trader 委托维护单调 OrderRef 水位和可重启恢复的订单身份。
主要输入: 脱敏账户作用域、服务端幂等键、委托指纹、登录 MaxOrderRef 与柜台回报身份。
主要输出: 原子分配的正 int32 OrderRef、稳定本地订单号和可供撤单/对账的身份快照。
上游关系: HuaxinBroker 在调用 ReqOrderInsert 前 claim，并在私有流回报后更新事实。
下游关系: 仅写调用方指定的私有 SQLite 文件，不访问网络、不保存账号密码或原始幂等键。
关键配置: 数据库必须位于受控私有目录；prepared/submit_unknown 记录重启后禁止自动重发。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


_MAX_ORDER_REF = 2_147_483_647
_SCHEMA_VERSION = 1
_VALID_STATES = frozenset(
    {
        "prepared",
        "submit_unknown",
        "accepted",
        "open",
        "filling",
        "filled",
        "partly_canceled",
        "canceled",
        "rejected",
    }
)
_TERMINAL_STATES = frozenset({"filled", "partly_canceled", "canceled", "rejected"})
_STATE_RANK = {
    "prepared": 0,
    "submit_unknown": 1,
    "accepted": 2,
    "open": 3,
    "filling": 4,
    "filled": 5,
    "partly_canceled": 5,
    "canceled": 5,
    "rejected": 5,
}


@dataclass(frozen=True)
class OrderIdentityClaim:
    """表示一次幂等委托占位或已有占位的只读快照。

    Args:
        is_new: 本次是否新建占位；False 时调用方绝不能再次报单。
        stable_local_order_id: 不回显原幂等键的稳定本地订单号。
        order_ref: 已持久化的正 int32 TORA OrderRef。
        state: prepared、submit_unknown 或已观测到的订单状态。
        result: 已持久化的脱敏结果；没有结果时为空字典。
        front_id: 柜台 FrontID，尚未观测时为 0。
        session_id: 柜台有符号 int32 SessionID，尚未观测时为 0。
        order_sys_id: 柜台 OrderSysID，尚未观测时为空。
        order_local_id: 柜台 OrderLocalID，尚未观测时为空。
    """

    is_new: bool
    stable_local_order_id: str
    order_ref: int
    state: str
    result: Dict[str, Any]
    front_id: int = 0
    session_id: int = 0
    order_sys_id: str = ""
    order_local_id: str = ""


class ToraOrderIdentityJournal:
    """以 SQLite 事务维护 TORA OrderRef 水位和订单身份。

    同一数据库可承载多个脱敏账户作用域；所有分配使用 ``BEGIN IMMEDIATE``，确保多个
    进程不会得到相同 OrderRef。原始账号、原始幂等键和登录凭据均不会落盘。
    """

    def __init__(self, path: Path) -> None:
        """打开或创建私有订单身份数据库。

        Args:
            path: SQLite 文件绝对或可解析路径。

        Returns:
            None。

        Raises:
            ValueError: 路径是符号链接、目录不私有或现有文件权限不安全时抛出。

        Side Effects:
            首次调用会创建 schema，并把数据库权限收紧为 0600。
        """

        self.path = Path(path).expanduser()
        self._validate_path_before_open()
        self._ensure_private_file()
        self._initialize()

    @staticmethod
    def account_scope(account_id: str, login_account: str, trade_front: str) -> str:
        """生成不泄露真实账户或前置地址的稳定作用域。

        Args:
            account_id: 当前资金账户标识。
            login_account: 当前 Trader 登录用户。
            trade_front: 当前 Trader 前置地址。

        Returns:
            str: 64 位小写 SHA256 十六进制作用域。

        Raises:
            ValueError: 任一构成字段为空时抛出，避免多个未知账户共用水位。
        """

        parts = [str(value or "").strip() for value in (account_id, login_account, trade_front)]
        if not all(parts):
            raise ValueError("华鑫 OrderRef 作用域缺少账户、登录用户或交易前置")
        return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()

    @staticmethod
    def fingerprint(payload: Mapping[str, Any]) -> str:
        """计算不依赖字典顺序的委托指纹。

        Args:
            payload: 仅含委托语义字段的映射，不得包含密码等 secret。

        Returns:
            str: 64 位小写 SHA256 十六进制指纹。
        """

        encoded = json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def seed(self, scope: str, vendor_max_order_ref: int) -> int:
        """以登录返回的 MaxOrderRef 单调提升持久水位。

        Args:
            scope: ``account_scope`` 生成的脱敏作用域。
            vendor_max_order_ref: 当前登录回报的最大 OrderRef，可为 0。

        Returns:
            int: 提升后的持久水位。

        Raises:
            ValueError: 作用域或水位越界时抛出。

        Side Effects:
            在一个立即事务中插入或更新水位。
        """

        normalized_scope = self._validate_scope(scope)
        maximum = self._validate_nonnegative_int32(vendor_max_order_ref, "vendor_max_order_ref")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._watermark(connection, normalized_scope)
            updated = max(current, maximum)
            self._set_watermark(connection, normalized_scope, updated)
            connection.commit()
            return updated

    def claim(
        self,
        scope: str,
        idempotency_key: str,
        fingerprint: str,
        stable_local_order_id: str,
        vendor_max_order_ref: int,
    ) -> OrderIdentityClaim:
        """在调用 Native 前持久占位并原子分配 OrderRef。

        Args:
            scope: 脱敏账户作用域。
            idempotency_key: 服务端要求的原始幂等键，仅在内存中用于计算哈希。
            fingerprint: 委托语义指纹。
            stable_local_order_id: 由幂等键派生的脱敏稳定订单号。
            vendor_max_order_ref: 当前登录回报的 MaxOrderRef。

        Returns:
            OrderIdentityClaim: 新占位或已有占位。已有占位时 ``is_new=False``，调用方
            必须返回已有/未知状态，绝不能再次调用 ReqOrderInsert。

        Raises:
            ValueError: 同一幂等键对应不同指纹、参数非法或 int32 水位耗尽时抛出。

        Side Effects:
            新键会在一个立即事务中先更新水位，再写 prepared 占位。
        """

        normalized_scope = self._validate_scope(scope)
        key_hash = self._key_hash(idempotency_key)
        normalized_fingerprint = self._validate_digest(fingerprint, "fingerprint")
        local_id = str(stable_local_order_id or "").strip()
        if not local_id:
            raise ValueError("stable_local_order_id 不能为空")
        maximum = self._validate_nonnegative_int32(vendor_max_order_ref, "vendor_max_order_ref")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._select_by_key(connection, normalized_scope, key_hash)
            if existing is not None:
                if str(existing[1]) != normalized_fingerprint:
                    connection.rollback()
                    raise ValueError("华鑫幂等键已绑定不同委托指纹")
                claim = self._claim_from_row(existing, is_new=False)
                connection.commit()
                return claim
            current = max(self._watermark(connection, normalized_scope), maximum)
            if current >= _MAX_ORDER_REF:
                connection.rollback()
                raise ValueError("华鑫 OrderRef 正 int32 空间已耗尽")
            order_ref = current + 1
            now = time.time()
            self._set_watermark(connection, normalized_scope, order_ref)
            connection.execute(
                "INSERT INTO tora_order_identities "
                "(scope, key_hash, fingerprint, stable_local_order_id, order_ref, state, "
                "result_json, front_id, session_id, order_sys_id, order_local_id, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'prepared', '{}', 0, 0, '', '', ?, ?)",
                (
                    normalized_scope,
                    key_hash,
                    normalized_fingerprint,
                    local_id,
                    order_ref,
                    now,
                    now,
                ),
            )
            connection.commit()
            return OrderIdentityClaim(
                is_new=True,
                stable_local_order_id=local_id,
                order_ref=order_ref,
                state="prepared",
                result={},
            )

    def mark_result(
        self,
        scope: str,
        idempotency_key: str,
        fingerprint: str,
        state: str,
        result: Optional[Mapping[str, Any]] = None,
    ) -> OrderIdentityClaim:
        """持久化明确响应或 submit_unknown，不改变已分配 OrderRef。

        Args:
            scope: 脱敏账户作用域。
            idempotency_key: 原始幂等键，仅计算哈希。
            fingerprint: 必须与 claim 时完全一致的语义指纹。
            state: 受支持的状态；prepared 不能通过本方法倒退写入。
            result: 可选脱敏响应，原始 idempotency_key 会在落盘前移除。

        Returns:
            OrderIdentityClaim: 更新后的只读快照。

        Raises:
            ValueError: 记录不存在、指纹冲突或状态非法时抛出。
        """

        normalized_scope = self._validate_scope(scope)
        key_hash = self._key_hash(idempotency_key)
        normalized_fingerprint = self._validate_digest(fingerprint, "fingerprint")
        normalized_state = str(state or "").strip().lower()
        if normalized_state not in _VALID_STATES or normalized_state == "prepared":
            raise ValueError("华鑫订单身份状态非法")
        safe_result = dict(result or {})
        safe_result.pop("idempotency_key", None)
        payload = json.dumps(
            safe_result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._select_by_key(connection, normalized_scope, key_hash)
            if existing is None:
                connection.rollback()
                raise ValueError("华鑫订单身份占位不存在")
            if str(existing[1]) != normalized_fingerprint:
                connection.rollback()
                raise ValueError("华鑫幂等键已绑定不同委托指纹")
            old_state = str(existing[4])
            if old_state not in {"prepared", "submit_unknown"} and old_state != normalized_state:
                connection.rollback()
                raise ValueError("华鑫订单身份状态禁止倒退或改写")
            connection.execute(
                "UPDATE tora_order_identities SET state = ?, result_json = ?, updated_at = ? "
                "WHERE scope = ? AND key_hash = ?",
                (normalized_state, payload, time.time(), normalized_scope, key_hash),
            )
            updated = self._select_by_key(connection, normalized_scope, key_hash)
            connection.commit()
            assert updated is not None
            return self._claim_from_row(updated, is_new=False)

    def update_order_fact(
        self,
        scope: str,
        order_ref: int,
        *,
        state: str,
        front_id: int = 0,
        session_id: int = 0,
        order_sys_id: str = "",
        order_local_id: str = "",
    ) -> Optional[OrderIdentityClaim]:
        """按 OrderRef 将私有流订单事实并入已有占位。

        Args:
            scope: 脱敏账户作用域。
            order_ref: 已分配的正 int32 OrderRef。
            state: 当前规范化订单状态。
            front_id: 可选 FrontID。
            session_id: 可选 SessionID。
            order_sys_id: 可选 OrderSysID。
            order_local_id: 可选 OrderLocalID。

        Returns:
            Optional[OrderIdentityClaim]: 找到本地占位时返回更新快照；外部历史订单返回 None。

        Raises:
            ValueError: 状态或数值越界时抛出。

        Side Effects:
            只更新已存在占位，不为未知外部订单创建记录。
        """

        normalized_scope = self._validate_scope(scope)
        normalized_order_ref = self._validate_positive_int32(order_ref, "order_ref")
        normalized_state = str(state or "").strip().lower()
        if normalized_state not in _VALID_STATES or normalized_state == "prepared":
            raise ValueError("华鑫订单事实状态非法")
        normalized_front = self._validate_nonnegative_int32(front_id, "front_id")
        normalized_session = self._validate_signed_int32(session_id, "session_id")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT key_hash, state FROM tora_order_identities "
                "WHERE scope = ? AND order_ref = ?",
                (normalized_scope, normalized_order_ref),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            old_state = str(row[1] or "")
            if old_state in _TERMINAL_STATES and old_state != normalized_state:
                connection.rollback()
                raise ValueError("华鑫订单终态禁止被后续回报改写")
            if _STATE_RANK.get(normalized_state, -1) < _STATE_RANK.get(old_state, -1):
                connection.rollback()
                raise ValueError("华鑫订单事实状态禁止倒退")
            connection.execute(
                "UPDATE tora_order_identities SET state = ?, front_id = ?, session_id = ?, "
                "order_sys_id = ?, order_local_id = ?, updated_at = ? "
                "WHERE scope = ? AND order_ref = ?",
                (
                    normalized_state,
                    normalized_front,
                    normalized_session,
                    str(order_sys_id or "").strip(),
                    str(order_local_id or "").strip(),
                    time.time(),
                    normalized_scope,
                    normalized_order_ref,
                ),
            )
            updated = self._select_by_key(connection, normalized_scope, str(row[0]))
            connection.commit()
            assert updated is not None
            return self._claim_from_row(updated, is_new=False)

    def resolve_identity(self, scope: str, order_id: str) -> Optional[OrderIdentityClaim]:
        """按稳定本地号、OrderSysID 或 OrderLocalID 查找撤单身份。

        Args:
            scope: 脱敏账户作用域。
            order_id: 调用方持有的订单标识。

        Returns:
            Optional[OrderIdentityClaim]: 唯一匹配时返回；找不到时返回 None。
        """

        normalized_scope = self._validate_scope(scope)
        wanted = str(order_id or "").strip()
        if not wanted:
            return None
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT key_hash, fingerprint, stable_local_order_id, order_ref, state, "
                "result_json, front_id, session_id, order_sys_id, order_local_id "
                "FROM tora_order_identities WHERE scope = ? AND "
                "(stable_local_order_id = ? OR order_sys_id = ? OR order_local_id = ?)",
                (normalized_scope, wanted, wanted, wanted),
            ).fetchall()
        if len(rows) != 1:
            return None
        return self._claim_from_row(rows[0], is_new=False)

    def resolve_order_ref(self, scope: str, order_ref: int) -> Optional[OrderIdentityClaim]:
        """按正 int32 OrderRef 恢复稳定本地订单号和柜台身份。

        Args:
            scope: 脱敏账户作用域。
            order_ref: TORA 私有流或查询返回的 OrderRef。

        Returns:
            Optional[OrderIdentityClaim]: 本地 journal 有唯一占位时返回；否则返回 None。
        """

        normalized_scope = self._validate_scope(scope)
        normalized_order_ref = self._validate_positive_int32(order_ref, "order_ref")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT key_hash, fingerprint, stable_local_order_id, order_ref, state, "
                "result_json, front_id, session_id, order_sys_id, order_local_id "
                "FROM tora_order_identities WHERE scope = ? AND order_ref = ?",
                (normalized_scope, normalized_order_ref),
            ).fetchone()
        if row is None:
            return None
        return self._claim_from_row(row, is_new=False)

    def _validate_path_before_open(self) -> None:
        """验证数据库路径及父目录不通过符号链接逃逸。

        Returns:
            None。

        Raises:
            ValueError: 父目录缺失、路径是符号链接或权限不安全时抛出。
        """

        if not self.path.is_absolute():
            raise ValueError("华鑫订单身份数据库必须使用绝对路径")
        parent = self.path.parent
        if not parent.exists() or not parent.is_dir() or parent.is_symlink():
            raise ValueError("华鑫订单身份数据库父目录必须是已存在的真实目录")
        parent_mode = stat.S_IMODE(parent.stat().st_mode)
        if parent_mode & 0o077:
            raise ValueError("华鑫订单身份数据库父目录不得授予 group/other 权限")
        if parent.stat().st_uid != os.geteuid():
            raise ValueError("华鑫订单身份数据库父目录必须属于当前用户")
        if self.path.exists():
            if self.path.is_symlink() or not self.path.is_file():
                raise ValueError("华鑫订单身份数据库必须是普通文件且不得为符号链接")
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode & 0o077:
                raise ValueError("华鑫订单身份数据库不得授予 group/other 权限")
            if self.path.stat().st_uid != os.geteuid():
                raise ValueError("华鑫订单身份数据库必须属于当前用户")

    def _ensure_private_file(self) -> None:
        """以 0600 原子创建尚不存在的数据库文件。

        Returns:
            None。

        Raises:
            ValueError: 并发创建出的目标不是当前用户私有普通文件时抛出。

        Side Effects:
            仅在文件不存在时创建一个空的 0600 文件。
        """

        try:
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
                0o600,
            )
        except FileExistsError:
            self._validate_path_before_open()
            return
        else:
            os.close(descriptor)

    def _initialize(self) -> None:
        """创建并校验 schema。

        Returns:
            None。

        Side Effects:
            创建数据库、索引与 schema version，并将文件权限设为 0600。
        """

        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, _SCHEMA_VERSION}:
                raise ValueError("华鑫订单身份数据库 schema 版本不受支持")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS tora_order_ref_watermarks ("
                "scope TEXT PRIMARY KEY, last_order_ref INTEGER NOT NULL, updated_at REAL NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS tora_order_identities ("
                "scope TEXT NOT NULL, key_hash TEXT NOT NULL, fingerprint TEXT NOT NULL, "
                "stable_local_order_id TEXT NOT NULL, order_ref INTEGER NOT NULL, "
                "state TEXT NOT NULL, result_json TEXT NOT NULL, front_id INTEGER NOT NULL, "
                "session_id INTEGER NOT NULL, order_sys_id TEXT NOT NULL, "
                "order_local_id TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL, "
                "PRIMARY KEY(scope, key_hash), UNIQUE(scope, order_ref))"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_tora_identity_stable "
                "ON tora_order_identities(scope, stable_local_order_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_tora_identity_sys "
                "ON tora_order_identities(scope, order_sys_id)"
            )
            connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            connection.commit()
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        """创建启用外键和强同步的短生命周期 SQLite 连接。

        Returns:
            sqlite3.Connection: 已配置连接；由调用方上下文关闭。
        """

        connection = sqlite3.connect(str(self.path), timeout=10.0, isolation_level=None)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    @staticmethod
    def _watermark(connection: sqlite3.Connection, scope: str) -> int:
        """读取一个作用域的当前水位。

        Args:
            connection: 已位于事务中的 SQLite 连接。
            scope: 脱敏账户作用域。

        Returns:
            int: 不存在时为 0。
        """

        row = connection.execute(
            "SELECT last_order_ref FROM tora_order_ref_watermarks WHERE scope = ?", (scope,)
        ).fetchone()
        return int(row[0]) if row is not None else 0

    @staticmethod
    def _set_watermark(connection: sqlite3.Connection, scope: str, value: int) -> None:
        """在当前事务中插入或提升一个作用域的水位。

        Args:
            connection: 已位于立即事务中的 SQLite 连接。
            scope: 脱敏账户作用域。
            value: 新水位。

        Returns:
            None。
        """

        connection.execute(
            "INSERT INTO tora_order_ref_watermarks(scope, last_order_ref, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT(scope) DO UPDATE SET "
            "last_order_ref = CASE WHEN excluded.last_order_ref > last_order_ref "
            "THEN excluded.last_order_ref ELSE last_order_ref END, updated_at = excluded.updated_at",
            (scope, int(value), time.time()),
        )

    @staticmethod
    def _select_by_key(
        connection: sqlite3.Connection, scope: str, key_hash: str
    ) -> Optional[sqlite3.Row]:
        """按作用域和幂等键哈希读取身份行。

        Args:
            connection: SQLite 连接。
            scope: 脱敏账户作用域。
            key_hash: 原幂等键的 SHA256。

        Returns:
            Optional[sqlite3.Row]: 固定顺序的行或 None。
        """

        return connection.execute(
            "SELECT key_hash, fingerprint, stable_local_order_id, order_ref, state, "
            "result_json, front_id, session_id, order_sys_id, order_local_id "
            "FROM tora_order_identities WHERE scope = ? AND key_hash = ?",
            (scope, key_hash),
        ).fetchone()

    @staticmethod
    def _claim_from_row(row: sqlite3.Row, *, is_new: bool) -> OrderIdentityClaim:
        """把固定顺序 SQLite 行转换为不可变 claim。

        Args:
            row: ``_select_by_key`` 或等价查询返回的固定顺序行。
            is_new: 是否为本次新占位。

        Returns:
            OrderIdentityClaim: 类型化快照。
        """

        try:
            result = json.loads(str(row[5] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            result = {}
        if not isinstance(result, dict):
            result = {}
        return OrderIdentityClaim(
            is_new=bool(is_new),
            stable_local_order_id=str(row[2]),
            order_ref=int(row[3]),
            state=str(row[4]),
            result=result,
            front_id=int(row[6]),
            session_id=int(row[7]),
            order_sys_id=str(row[8] or ""),
            order_local_id=str(row[9] or ""),
        )

    @staticmethod
    def _key_hash(idempotency_key: str) -> str:
        """把原幂等键转换为落盘哈希。

        Args:
            idempotency_key: 非空原始幂等键。

        Returns:
            str: SHA256 十六进制。

        Raises:
            ValueError: 幂等键为空时抛出。
        """

        text = str(idempotency_key or "").strip()
        if not text:
            raise ValueError("idempotency_key 不能为空")
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _validate_scope(scope: str) -> str:
        """校验脱敏账户作用域格式。

        Args:
            scope: 期望的 64 位小写十六进制值。

        Returns:
            str: 规范化作用域。

        Raises:
            ValueError: 格式非法时抛出。
        """

        return ToraOrderIdentityJournal._validate_digest(scope, "scope")

    @staticmethod
    def _validate_digest(value: str, name: str) -> str:
        """校验 SHA256 十六进制字段。

        Args:
            value: 待校验文本。
            name: 错误信息使用的字段名。

        Returns:
            str: 小写值。

        Raises:
            ValueError: 长度或字符集非法时抛出。
        """

        text = str(value or "").strip().lower()
        if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
            raise ValueError(f"{name} 必须为 64 位 SHA256 十六进制")
        return text

    @staticmethod
    def _validate_nonnegative_int32(value: Any, name: str) -> int:
        """校验非负 int32 数值且拒绝 bool。

        Args:
            value: 待校验值。
            name: 错误字段名。

        Returns:
            int: 原值。

        Raises:
            ValueError: 不是整数或越界时抛出。
        """

        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} 必须为整数")
        if value < 0 or value > _MAX_ORDER_REF:
            raise ValueError(f"{name} 必须位于 0..{_MAX_ORDER_REF}")
        return int(value)

    @staticmethod
    def _validate_positive_int32(value: Any, name: str) -> int:
        """校验正 int32 数值且拒绝 bool。

        Args:
            value: 待校验值。
            name: 错误字段名。

        Returns:
            int: 原值。

        Raises:
            ValueError: 不是正整数或越界时抛出。
        """

        normalized = ToraOrderIdentityJournal._validate_nonnegative_int32(value, name)
        if normalized < 1:
            raise ValueError(f"{name} 必须为正整数")
        return normalized

    @staticmethod
    def _validate_signed_int32(value: Any, name: str) -> int:
        """校验有符号 int32 数值且拒绝 bool。

        Args:
            value: 待校验值。
            name: 错误字段名。

        Returns:
            int: 原值。

        Raises:
            ValueError: 不是整数或超出有符号 int32 时抛出。
        """

        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} 必须为整数")
        if value < -(1 << 31) or value > _MAX_ORDER_REF:
            raise ValueError(f"{name} 必须位于有符号 int32 范围")
        return int(value)


__all__ = ["OrderIdentityClaim", "ToraOrderIdentityJournal"]
