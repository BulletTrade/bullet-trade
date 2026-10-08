"""离线资金卡号绑定契约。调用方负责取得真实 GUI 观察及用户确认。

配置仅保存卡号的 HMAC-SHA256 摘要；密钥须由独立的受保护渠道提供。
本模块不连接客户端，也不把一次核验当作后续动作的永久授权。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
from uuid import uuid4


class BindingError(RuntimeError):
    """账户绑定失败；异常文本仅包含固定错误码。"""


def _text(value: str, code: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise BindingError(code)
    return value


def _card(value: str) -> str:
    # 不修剪、不补零、不接受掩码或 Unicode 数字：比较的是 GUI 完整原文。
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{8,32}", value) is None:
        raise BindingError("full_card_number_required")
    return value


def _secret(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) < 32:
        raise BindingError("binding_secret_invalid")
    return value


def _digest(card_number: str, secret: bytes) -> str:
    return hmac.new(_secret(secret), _card(card_number).encode("ascii"), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class BindingRecord:
    service_account_id: str
    profile_id: str
    card_digest: str
    confirmation_id: str
    confirmed: bool

    @classmethod
    def from_confirmation(
        cls, *, card_number: str, service_account_id: str, profile_id: str,
        confirmation_id: str, secret: bytes,
    ) -> "BindingRecord":
        """仅由外部在用户确认完整卡号和服务映射后调用。

        本函数不取得用户确认，也不验证 confirmation_id 的来源；调用方须
        独立保留确认依据，并从独立受保护渠道提供 HMAC 密钥。
        """
        return cls(_text(service_account_id, "service_account_id_invalid"),
                   _text(profile_id, "profile_id_invalid"),
                   _digest(card_number, secret),
                   _text(confirmation_id, "confirmation_id_missing"), True)

    def validate(self) -> None:
        _text(self.service_account_id, "service_account_id_invalid")
        _text(self.profile_id, "profile_id_invalid")
        _text(self.confirmation_id, "confirmation_id_missing")
        if self.confirmed is not True:
            raise BindingError("binding_unconfirmed")
        if not isinstance(self.card_digest, str) or re.fullmatch(r"[0-9a-f]{64}", self.card_digest) is None:
            raise BindingError("card_digest_invalid")


def save_protected_record(path: str | Path, record: BindingRecord) -> None:
    """仅在 POSIX 新建 0600 文件；已存在时拒绝覆盖。"""
    record.validate()
    if os.name != "posix":
        raise BindingError("protected_file_unavailable")
    payload = json.dumps({"version": 1, "service_account_id": record.service_account_id,
                          "profile_id": record.profile_id, "card_digest": record.card_digest,
                          "confirmation_id": record.confirmation_id, "confirmed": True},
                         ensure_ascii=False, sort_keys=True).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(fd)
    finally:
        os.close(fd)


def load_protected_record(path: str | Path) -> BindingRecord:
    """只读取属主为当前用户且权限恰为 0600 的普通文件。"""
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise BindingError("protected_file_unavailable")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise BindingError("protected_file_invalid")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                payload = stream.read(16_385)
        finally:
            os.close(fd)
    except OSError as exc:
        raise BindingError("protected_file_unavailable") from exc
    if len(payload) > 16_384:
        raise BindingError("protected_file_invalid")
    try:
        data = json.loads(payload)
        if not isinstance(data, dict) or set(data) != {
                "version", "service_account_id", "profile_id", "card_digest",
                "confirmation_id", "confirmed"} or data["version"] != 1:
            raise ValueError
        record = BindingRecord(data["service_account_id"], data["profile_id"],
                               data["card_digest"], data["confirmation_id"],
                               data["confirmed"])
        record.validate()
        return record
    except (UnicodeError, ValueError, TypeError, KeyError) as exc:
        raise BindingError("protected_file_invalid") from exc


@dataclass(frozen=True)
class BindingObservation:
    card_number: str = field(repr=False)
    session_id: str
    client_pid: int
    client_created_ns: int
    hwnd: int
    refresh_generation: str

    def context(self) -> tuple[str, int, int, int, str]:
        _text(self.session_id, "session_id_invalid")
        for name in ("client_pid", "client_created_ns", "hwnd"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise BindingError("client_context_invalid")
        value = self.refresh_generation
        if not isinstance(value, str) or not value or value != value.strip():
            raise BindingError("client_context_invalid")
        return (self.session_id, self.client_pid, self.client_created_ns,
                self.hwnd, self.refresh_generation)


@dataclass(frozen=True)
class BindingProof:
    service_account_id: str
    profile_id: str
    card_digest: str
    context: tuple[str, int, int, int, str]
    proof_token: str


class AccountBindingContract:
    """一实例只处理一条确认记录；任何失败撤销现有 proof。"""

    def __init__(self, record: BindingRecord, *, secret: bytes):
        record.validate()
        self._record = record
        self._secret = _secret(secret)
        self._context: tuple[str, int, int, int, str] | None = None
        self._proof: BindingProof | None = None

    def begin_session(self, observation: BindingObservation) -> None:
        """登录或刷新后显式开始新观察周期，并撤销旧 proof。"""
        self._proof = None
        self._context = None
        self._context = observation.context()

    def invalidate(self) -> None:
        self._proof = None
        self._context = None

    def verify(self, observation: BindingObservation, *, service_account_id: str,
               profile_id: str) -> BindingProof:
        self._proof = None
        try:
            context = observation.context()
            if self._context is None or context != self._context:
                raise BindingError("session_context_changed")
            if service_account_id != self._record.service_account_id or profile_id != self._record.profile_id:
                raise BindingError("service_mapping_mismatch")
            digest = _digest(observation.card_number, self._secret)
            if not hmac.compare_digest(digest, self._record.card_digest):
                raise BindingError("card_number_mismatch")
            proof = BindingProof(self._record.service_account_id, self._record.profile_id,
                                 digest, context, uuid4().hex)
            self._proof = proof
            return proof
        except BindingError:
            self.invalidate()
            raise

    def assert_current(self, proof: BindingProof, observation: BindingObservation, *,
                       service_account_id: str, profile_id: str) -> None:
        """每次使用 proof 前以当前 GUI 原文和上下文复核。"""
        if proof is not self._proof:
            raise BindingError("binding_proof_stale_or_missing")
        try:
            if observation.context() != self._context or proof.context != self._context:
                raise BindingError("session_context_changed")
            if service_account_id != self._record.service_account_id or profile_id != self._record.profile_id:
                raise BindingError("service_mapping_mismatch")
            if not hmac.compare_digest(_digest(observation.card_number, self._secret),
                                       self._record.card_digest):
                raise BindingError("card_number_mismatch")
        except BindingError:
            self.invalidate()
            raise
