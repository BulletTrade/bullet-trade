"""
作者: BruceLee
文件说明:
    远程交易写请求的 SQLite 持久化幂等账本。
    输入为已脱敏的账户作用域、幂等键哈希、action、请求指纹和最小结果；输出为可恢复的占位或最终事实。
    上游由 ServerApplication 在调用真实券商前写入，下游用于重启、TTL 后的重复请求和只读结果解析。
    使用 SQLite UNIQUE 约束与 BEGIN IMMEDIATE 保证多进程原子占位，不保存 token、密码或完整请求载荷。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class IdempotencyJournalError(RuntimeError):
    """表示幂等账本无法安全读取、锁定或写入。"""


class IdempotencyJournalConflictError(IdempotencyJournalError):
    """表示已持久化幂等键与当前 action 或指纹不一致。"""


class PersistentIdempotencyJournal:
    """管理远程写入的 SQLite 持久化幂等状态。

    每次读取或写入都使用新的数据库连接，避免旧进程持有过期内存快照。
    新键占位通过 ``BEGIN IMMEDIATE``、主键唯一约束和同事务容量检查完成，
    因而多个 server 进程共享同一数据库时最多一个进程获得真实写入资格。
    """

    _SCHEMA_VERSION = 1
    _BUSY_TIMEOUT_SECONDS = 2.0

    def __init__(self, path: str, max_entries: int) -> None:
        """打开并校验 SQLite 幂等账本。

        Args:
            path: 数据库文件路径；父目录必须已存在且不可被组或其他用户写入。
            max_entries: 最大条目数，达到后新写入 fail-closed。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 路径、权限、数据库结构或锁状态不安全时抛出。
        """

        self.path = self._configured_path(path)
        self.max_entries = max(1, int(max_entries))
        self._validate_parent()
        self._prepare_database_file()
        self._initialize_schema()

    def claim(
        self,
        scope: Tuple[str, str, str],
        action: str,
        fingerprint: str,
        pending_result: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """跨进程原子占用幂等键，已有条目只返回已有结果。

        Args:
            scope: 父账户、子账户和原幂等键组成的隔离作用域。
            action: 具体写 action。
            fingerprint: 不含 secret 的规范化请求指纹。
            pending_result: 新条目在未知态使用的最小结果。

        Returns:
            Optional[Dict[str, Any]]: 新占位返回 None；已有条目返回其结果副本。

        Raises:
            IdempotencyJournalError: 同键冲突、容量、锁或持久化失败时抛出。
        """

        entry_key = self._entry_key(scope)
        result_json = self._encode_result(pending_result)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT action, fingerprint, result_json FROM idempotency_entries "
                "WHERE entry_key = ?",
                (entry_key,),
            ).fetchone()
            if row is not None:
                self._ensure_match(row[0], row[1], action, fingerprint)
                result = self._decode_result(row[2])
                connection.commit()
                return result
            count = int(
                connection.execute("SELECT COUNT(*) FROM idempotency_entries").fetchone()[0]
            )
            if count >= self.max_entries:
                raise IdempotencyJournalError("幂等持久账本容量已满，拒绝新的交易写请求")
            now = time.time()
            connection.execute(
                "INSERT INTO idempotency_entries "
                "(entry_key, action, fingerprint, result_json, finalized, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 0, ?, ?)",
                (entry_key, str(action), str(fingerprint), result_json, now, now),
            )
            connection.commit()
            return None
        except IdempotencyJournalError:
            self._rollback_quietly(connection)
            raise
        except (sqlite3.Error, OSError) as exc:
            self._rollback_quietly(connection)
            raise IdempotencyJournalError(f"幂等账本原子占位失败: {exc}") from exc
        finally:
            connection.close()

    def finalize(
        self,
        scope: Tuple[str, str, str],
        action: str,
        fingerprint: str,
        result: Dict[str, Any],
        *,
        finalized: bool,
    ) -> None:
        """跨进程持久化写请求的明确或仍未知结果。

        Args:
            scope: 父账户、子账户和原幂等键组成的隔离作用域。
            action: 具体写 action。
            fingerprint: 不含 secret 的规范化请求指纹。
            result: 需要保存的最小结果。
            finalized: 是否已得到可重复返回的终态事实。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 条目缺失、冲突、锁或持久化失败时抛出。
        """

        entry_key = self._entry_key(scope)
        result_json = self._encode_result(result)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT action, fingerprint FROM idempotency_entries WHERE entry_key = ?",
                (entry_key,),
            ).fetchone()
            if row is None:
                raise IdempotencyJournalError("幂等账本缺少已占用写请求，拒绝伪造最终结果")
            self._ensure_match(row[0], row[1], action, fingerprint)
            connection.execute(
                "UPDATE idempotency_entries "
                "SET result_json = ?, finalized = ?, updated_at = ? WHERE entry_key = ?",
                (result_json, 1 if finalized else 0, time.time(), entry_key),
            )
            connection.commit()
        except IdempotencyJournalError:
            self._rollback_quietly(connection)
            raise
        except (sqlite3.Error, OSError) as exc:
            self._rollback_quietly(connection)
            raise IdempotencyJournalError(f"幂等账本最终结果写入失败: {exc}") from exc
        finally:
            connection.close()

    def get(
        self,
        scope: Tuple[str, str, str],
        action: str,
        fingerprint: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """读取并校验指定幂等键的最小条目。

        Args:
            scope: 父账户、子账户和原幂等键组成的隔离作用域。
            action: 解析请求声明的原写 action。
            fingerprint: 原请求指纹；为 None 时只允许 action 比较。

        Returns:
            Optional[Dict[str, Any]]: 条目副本；不存在返回 None。

        Raises:
            IdempotencyJournalError: action、指纹、权限、锁或内容异常时抛出。
        """

        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT action, fingerprint, result_json, finalized, created_at, updated_at "
                "FROM idempotency_entries WHERE entry_key = ?",
                (self._entry_key(scope),),
            ).fetchone()
            if row is None:
                return None
            if fingerprint is None:
                if row[0] != action:
                    raise IdempotencyJournalConflictError("幂等键 action 冲突")
            else:
                self._ensure_match(row[0], row[1], action, fingerprint)
            return {
                "action": str(row[0]),
                "fingerprint": str(row[1]),
                "result": self._decode_result(row[2]),
                "finalized": bool(row[3]),
                "created_at": float(row[4]),
                "updated_at": float(row[5]),
            }
        except IdempotencyJournalError:
            raise
        except (sqlite3.Error, OSError) as exc:
            raise IdempotencyJournalError(f"幂等账本读取失败: {exc}") from exc
        finally:
            connection.close()

    def count(self) -> int:
        """返回当前持久条目数，供 readiness 和确定性测试使用。

        Args:
            None。

        Returns:
            int: 当前数据库中的幂等条目数。

        Raises:
            IdempotencyJournalError: 数据库不可安全读取时抛出。
        """

        connection = self._connect()
        try:
            return int(connection.execute("SELECT COUNT(*) FROM idempotency_entries").fetchone()[0])
        except (sqlite3.Error, OSError) as exc:
            raise IdempotencyJournalError(f"幂等账本计数失败: {exc}") from exc
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        """初始化或校验 SQLite schema 和完整性。

        Args:
            None。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 数据库损坏、版本不兼容或锁超时时抛出。
        """

        connection = self._connect(validate_schema=False)
        try:
            connection.execute("BEGIN IMMEDIATE")
            current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current_version not in {0, self._SCHEMA_VERSION}:
                raise IdempotencyJournalError(f"幂等账本 schema 版本不受支持: {current_version}")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS idempotency_entries ("
                "entry_key TEXT PRIMARY KEY NOT NULL, "
                "action TEXT NOT NULL, "
                "fingerprint TEXT NOT NULL, "
                "result_json TEXT NOT NULL, "
                "finalized INTEGER NOT NULL CHECK (finalized IN (0, 1)), "
                "created_at REAL NOT NULL, "
                "updated_at REAL NOT NULL"
                ") WITHOUT ROWID"
            )
            connection.execute(f"PRAGMA user_version = {self._SCHEMA_VERSION}")
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
            if quick_check is None or str(quick_check[0]).lower() != "ok":
                raise IdempotencyJournalError("幂等账本完整性检查失败")
            connection.commit()
        except IdempotencyJournalError:
            self._rollback_quietly(connection)
            raise
        except (sqlite3.Error, OSError) as exc:
            self._rollback_quietly(connection)
            raise IdempotencyJournalError(f"幂等账本无法安全初始化: {exc}") from exc
        finally:
            connection.close()
        self._validate_file(self.path)

    def _connect(self, *, validate_schema: bool = True) -> sqlite3.Connection:
        """打开配置了 FULL 同步和有限锁等待的 SQLite 连接。

        Args:
            validate_schema: 是否要求数据库已经是当前 schema 版本。

        Returns:
            sqlite3.Connection: 使用显式事务的短连接。

        Raises:
            IdempotencyJournalError: 文件权限、schema、损坏或连接失败时抛出。
        """

        self._validate_file(self.path)
        connection: Optional[sqlite3.Connection] = None
        try:
            connection = sqlite3.connect(
                f"{self.path.as_uri()}?mode=rw",
                timeout=self._BUSY_TIMEOUT_SECONDS,
                isolation_level=None,
                uri=True,
            )
            connection.execute(f"PRAGMA busy_timeout = {int(self._BUSY_TIMEOUT_SECONDS * 1000)}")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA journal_mode = DELETE")
            if validate_schema:
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if version != self._SCHEMA_VERSION:
                    raise IdempotencyJournalError(f"幂等账本 schema 版本不受支持: {version}")
            return connection
        except IdempotencyJournalError:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            raise
        except (sqlite3.Error, OSError) as exc:
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    pass
            raise IdempotencyJournalError(f"幂等账本连接失败: {exc}") from exc

    def _prepare_database_file(self) -> None:
        """拒绝最终路径链接，并以 0600 原子创建缺失数据库文件。

        Args:
            None。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 路径存在但不安全，或文件无法安全创建时抛出。
        """

        try:
            info = self.path.lstat()
        except FileNotFoundError:
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(str(self.path), flags, 0o600)
            except FileExistsError:
                self._validate_file(self.path)
                return
            except OSError as exc:
                raise IdempotencyJournalError(f"幂等账本文件无法安全创建: {exc}") from exc
            else:
                os.close(fd)
                if os.name != "nt":
                    os.chmod(self.path, 0o600)
                return
        except OSError as exc:
            raise IdempotencyJournalError(f"幂等账本路径无法检查: {exc}") from exc
        self._validate_file_info(info)

    def _validate_parent(self) -> None:
        """校验数据库父目录存在且不允许组或其他用户写入。

        Args:
            None。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 父目录不存在、不是目录或权限不安全时抛出。
        """

        try:
            info = self.path.parent.stat()
        except OSError as exc:
            raise IdempotencyJournalError(f"幂等账本父目录无法检查: {exc}") from exc
        if not stat.S_ISDIR(info.st_mode):
            raise IdempotencyJournalError("幂等账本父目录不存在")
        if os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o022:
            raise IdempotencyJournalError("幂等账本父目录可被组或其他用户写入")

    @classmethod
    def _validate_file(cls, path: Path) -> None:
        """重新校验数据库最终路径不是链接、类型正确且权限为私有。

        Args:
            path: 数据库文件路径。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 文件为链接、非普通文件或权限不安全时抛出。
        """

        try:
            info = path.lstat()
        except OSError as exc:
            raise IdempotencyJournalError(f"幂等账本文件无法检查: {exc}") from exc
        cls._validate_file_info(info)

    @staticmethod
    def _validate_file_info(info: os.stat_result) -> None:
        """校验一次 lstat 结果符合数据库文件安全合同。

        Args:
            info: 最终配置路径的 lstat 结果。

        Returns:
            None。

        Raises:
            IdempotencyJournalError: 路径不是私有普通文件时抛出。
        """

        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise IdempotencyJournalError("幂等账本必须是普通文件且不能为符号链接")
        if os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o077:
            raise IdempotencyJournalError("幂等账本权限过宽")

    @staticmethod
    def _configured_path(path: str) -> Path:
        """生成不解析最终符号链接的绝对配置路径。

        Args:
            path: 用户配置的原始数据库路径。

        Returns:
            Path: 保留最终路径组件的绝对路径。

        Raises:
            IdempotencyJournalError: 路径为空时抛出。
        """

        raw = str(path or "").strip()
        if not raw:
            raise IdempotencyJournalError("幂等账本路径不能为空")
        expanded = Path(raw).expanduser()
        if not expanded.is_absolute():
            expanded = Path.cwd() / expanded
        return expanded.absolute()

    @staticmethod
    def _entry_key(scope: Tuple[str, str, str]) -> str:
        """生成不暴露原始作用域的稳定数据库主键。

        Args:
            scope: 父账户、子账户和原幂等键。

        Returns:
            str: SHA-256 十六进制摘要。
        """

        return hashlib.sha256(
            "\x1f".join(str(value) for value in scope).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _ensure_match(
        stored_action: str,
        stored_fingerprint: str,
        action: str,
        fingerprint: str,
    ) -> None:
        """验证已有条目与当前请求是同一写操作。

        Args:
            stored_action: 数据库中的 action。
            stored_fingerprint: 数据库中的请求指纹。
            action: 当前具体写 action。
            fingerprint: 当前规范化请求指纹。

        Returns:
            None。

        Raises:
            IdempotencyJournalConflictError: action 或指纹不一致时抛出。
        """

        if stored_action != action or stored_fingerprint != fingerprint:
            raise IdempotencyJournalConflictError("幂等键 action 或请求指纹冲突")

    @staticmethod
    def _encode_result(result: Dict[str, Any]) -> str:
        """编码最小非敏感结果供 SQLite 保存。

        Args:
            result: 原始 adapter 或服务端结果。

        Returns:
            str: 规范化 JSON 文本。

        Raises:
            IdempotencyJournalError: 结果包含不可序列化内容时抛出。
        """

        try:
            return json.dumps(
                _minimal_result(result),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as exc:
            raise IdempotencyJournalError(f"幂等账本结果无法编码: {exc}") from exc

    @staticmethod
    def _decode_result(raw: str) -> Dict[str, Any]:
        """解码并校验 SQLite 中的最小结果。

        Args:
            raw: 数据库中的 JSON 文本。

        Returns:
            Dict[str, Any]: 最小结果字典。

        Raises:
            IdempotencyJournalError: JSON 损坏或结构错误时抛出。
        """

        try:
            value = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError) as exc:
            raise IdempotencyJournalError(f"幂等账本结果损坏: {exc}") from exc
        if not isinstance(value, dict):
            raise IdempotencyJournalError("幂等账本结果结构非法")
        return dict(value)

    @staticmethod
    def _rollback_quietly(connection: sqlite3.Connection) -> None:
        """尽力回滚失败事务，不覆盖原始安全异常。

        Args:
            connection: 当前 SQLite 连接。

        Returns:
            None。
        """

        try:
            connection.rollback()
        except sqlite3.Error:
            pass


def _minimal_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """仅保留重复写与只读解析所需的非敏感字段。

    Args:
        result: 原始 adapter 或服务端结果。

    Returns:
        Dict[str, Any]: 去除了 token、密码、账户配置和原始载荷的最小结果。
    """

    allowed = {
        "status",
        "submission_state",
        "order_status",
        "order_id",
        "stable_local_order_id",
        "value",
        "success",
        "timed_out",
        "submit_unknown",
        "async_tracking",
        "reason",
        "warning",
        "cancel_outcome",
        "idempotency_key",
        "security",
        "side",
        "direction",
        "amount",
        "volume",
    }
    cleaned = {key: value for key, value in dict(result or {}).items() if key in allowed}
    snapshot = result.get("last_snapshot") if isinstance(result, dict) else None
    if isinstance(snapshot, dict):
        cleaned["last_snapshot"] = {
            key: snapshot.get(key)
            for key in (
                "order_id",
                "status",
                "order_status",
                "idempotency_key",
                "security",
                "side",
                "amount",
            )
            if snapshot.get(key) not in (None, "")
        }
    return cleaned
