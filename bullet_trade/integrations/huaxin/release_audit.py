"""
作者: BruceLee
文件职责: 对 BulletTrade 华鑫相关 Git tree、wheel、sdist 与 native bundle 做纯离线发布审计。
主要输入: 受跟踪源码目录、Python 发布归档或显式构建 bundle，以及可覆盖的体积策略。
主要输出: 不含绝对本地路径和原始敏感值的确定性 SBOM、制品元数据与 fail-closed 发现项。
上游关系: 发布前离线脚本、打包测试和未来 CI 发布门禁显式调用本模块。
下游关系: 仅使用标准库读取归档，并在真实 native 制品存在时调用本机只读依赖检查工具。
关键环境或配置: 不联网、不加载 native、不读取 SDK 配置；依赖/RPATH 工具缺失时仅对真实 native 制品失败。
"""

from __future__ import annotations

import ast
import base64
import csv
import email.policy
import hashlib
import importlib
import io
import json
import os
import platform
import re
import shutil
import stat
import struct
import subprocess
import tarfile
import tempfile
import unicodedata
import zipfile
import zlib
from dataclasses import dataclass, field
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

AUDIT_SCHEMA_VERSION = 1
MEBIBYTE = 1024 * 1024
_NATIVE_TOOL_OUTPUT_MAX_BYTES = MEBIBYTE
_BUNDLE_NAME_PATTERN = re.compile(r"[0-9a-f]{64}")
_SELF_BRIDGE_STEMS = ("libbullet_trade_huaxin", "bullet_trade_huaxin")
_NATIVE_SUFFIXES = {".so", ".dylib", ".dll", ".pyd"}
_PUBLIC_FORBIDDEN_SUFFIXES = {
    ".a",
    ".doc",
    ".docx",
    ".gz",
    ".jar",
    ".lib",
    ".o",
    ".obj",
    ".pdf",
    ".pyd",
    ".rar",
    ".so",
    ".tar",
    ".tgz",
    ".war",
    ".whl",
    ".xls",
    ".xlsx",
    ".zip",
    ".7z",
    ".dll",
    ".dylib",
}
_BUNDLE_FORBIDDEN_SUFFIXES = {
    ".a",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".doc",
    ".docx",
    ".h",
    ".hpp",
    ".lib",
    ".pdf",
    ".zip",
}
_REQUIRED_HUAXIN_PATHS = {
    "bullet_trade/integrations/huaxin/__init__.py",
    "bullet_trade/integrations/huaxin/build.py",
    "bullet_trade/integrations/huaxin/native.py",
    "bullet_trade/integrations/huaxin/native_src/CMakeLists.txt",
    "bullet_trade/integrations/huaxin/native_src/include/bt_huaxin_bridge.h",
    "bullet_trade/integrations/huaxin/native_src/src/bt_huaxin_bridge.cpp",
}
_EXPECTED_BRIDGE_SYMBOLS = (
    b"bt_huaxin_abi_version",
    b"bt_huaxin_create",
    b"bt_huaxin_destroy",
    b"bt_huaxin_submit_request",
    b"bt_huaxin_drain_event_batch",
    b"bt_huaxin_free_event_batch",
)
_BUNDLE_MANIFEST_KEYS = {
    "schema_version",
    "mode",
    "distribution",
    "source",
    "bridge",
    "target",
    "toolchain",
    "runtime",
    "integrity_scope",
    "vendor_sdk",
    "fingerprint",
}
_PUBLIC_WHEEL_TAGS = {
    "py3-none-any",
    "py3-none-linux_x86_64",
    "py3-none-win_amd64",
}
_PUBLIC_EXTRAS = {
    "all",
    "dev",
    "qmt",
    "qmtserver",
    "report",
    "rqdata",
    "tdx",
    "tushare",
}


def _optional_module(name: str) -> Optional[Any]:
    """
    按名称加载审计所需的可选解析器，并把缺失留给调用点 fail closed。

    参数:
        name: 待加载模块的完整导入名。
    返回:
        成功时返回模块对象；模块不存在时返回 None。
    """

    try:
        return importlib.import_module(name)
    except ImportError:
        return None


_TOML_PARSER = _optional_module("tomllib") or _optional_module("tomli")
_PACKAGING_UTILS = _optional_module("packaging.utils")
_PACKAGING_VERSION = _optional_module("packaging.version")
_PACKAGING_REQUIREMENTS = _optional_module("packaging.requirements")


@dataclass(frozen=True)
class ReleaseAuditPolicy:
    """定义发布审计体积、数量和单文件读取硬门槛。"""

    universal_wheel_max_bytes: int = 2 * MEBIBYTE
    universal_wheel_unpacked_max_bytes: int = 8 * MEBIBYTE
    platform_wheel_max_bytes: int = 5 * MEBIBYTE
    platform_wheel_unpacked_max_bytes: int = 20 * MEBIBYTE
    sdist_max_bytes: int = 2 * MEBIBYTE
    sdist_unpacked_max_bytes: int = 8 * MEBIBYTE
    bundle_unpacked_max_bytes: int = 10 * MEBIBYTE
    max_file_bytes: int = 32 * MEBIBYTE
    max_file_count: int = 10_000
    max_archive_scan_bytes: int = 16 * MEBIBYTE
    max_unpacked_scan_bytes: int = 64 * MEBIBYTE

    def __post_init__(self) -> None:
        """
        校验所有发布门槛均为正整数。

        参数:
            无。
        返回:
            无；数据类构造完成即表示策略有效。
        异常:
            ValueError: 任一门槛不是正整数。
        """

        for name, value in self.__dict__.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError("发布审计门槛必须为正整数: {}".format(name))


@dataclass(frozen=True)
class AuditFinding:
    """表示一个不携带原始敏感值或绝对路径的审计发现。"""

    code: str
    message: str
    path: Optional[str] = None
    severity: str = "error"
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        将发现项转换为稳定、可 JSON 序列化的脱敏字典。

        参数:
            无。
        返回:
            仅含规则、相对路径、级别和结构化摘要的字典。
        """

        payload: Dict[str, Any] = {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "details": dict(self.details),
        }
        if self.path is not None:
            payload["path"] = self.path
        return payload


@dataclass(frozen=True)
class ReleaseAuditReport:
    """封装单个 Git tree 或发布制品的离线审计结果与脱敏 SBOM。"""

    artifact_kind: str
    artifact_name: str
    artifact_sha256: Optional[str]
    archive_size: Optional[int]
    unpacked_size: int
    file_count: int
    metadata: Mapping[str, Any]
    sbom: Tuple[Mapping[str, Any], ...]
    native_inspection: Tuple[Mapping[str, Any], ...]
    findings: Tuple[AuditFinding, ...]

    @property
    def passed(self) -> bool:
        """
        判断当前制品是否没有 error 级别发现。

        参数:
            无。
        返回:
            不存在 error 发现时返回 True。
        """

        return not any(item.severity == "error" for item in self.findings)

    def to_dict(self) -> Dict[str, Any]:
        """
        将报告转换为不暴露审计源绝对路径的稳定字典。

        参数:
            无。
        返回:
            可直接写入 JSON 的报告字典。
        """

        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "artifact_kind": self.artifact_kind,
            "artifact_name": self.artifact_name,
            "artifact_sha256": self.artifact_sha256,
            "passed": self.passed,
            "archive_size": self.archive_size,
            "unpacked_size": self.unpacked_size,
            "file_count": self.file_count,
            "metadata": dict(self.metadata),
            "sbom": [dict(item) for item in self.sbom],
            "native_inspection": [dict(item) for item in self.native_inspection],
            "findings": [item.to_dict() for item in self.findings],
        }


@dataclass(frozen=True)
class CleanImportReport:
    """表示当前解释器上从离线 wheel 临时安装并导入的尽力验证结果。"""

    passed: bool
    python_version: str
    installed_from_wheel: bool
    huaxin_import_side_effect_guard: bool
    reason_code: str

    def to_dict(self) -> Dict[str, Any]:
        """
        将 clean-import 结果转换为不含临时目录的 JSON 字典。

        参数:
            无。
        返回:
            当前解释器、验证边界与稳定原因码。
        """

        return {
            "passed": self.passed,
            "python_version": self.python_version,
            "installed_from_wheel": self.installed_from_wheel,
            "huaxin_import_side_effect_guard": self.huaxin_import_side_effect_guard,
            "reason_code": self.reason_code,
            "scope": "current_interpreter_offline_target_best_effort",
        }


@dataclass
class _ArtifactFile:
    """保存审计期间的相对路径、字节、哈希与可选真实临时路径。"""

    path: str
    size: int
    sha256: Optional[str]
    data: Optional[bytes]
    source_path: Optional[Path] = None


@dataclass(frozen=True)
class _BundleManifestResult:
    """保存严格 manifest 审计后的脱敏摘要与内部受信路由信息。"""

    metadata: Mapping[str, Any]
    manifest: Optional[Mapping[str, Any]]
    artifact_path: Optional[str]
    mode_is_offline_fake: bool
    structurally_valid: bool


class _StrictJsonError(ValueError):
    """表示 JSON 重复键、非有限数字或其他禁止的非确定性结构。"""


class _AuditBuilder:
    """在单个制品审计期间集中收集发现项，避免抛出不稳定底层异常。"""

    def __init__(self) -> None:
        """
        初始化空发现列表。

        参数:
            无。
        返回:
            无。
        """

        self.findings: List[AuditFinding] = []

    def add(
        self,
        code: str,
        message: str,
        path: Optional[str] = None,
        details: Optional[Mapping[str, Any]] = None,
        severity: str = "error",
    ) -> None:
        """
        增加一个已脱敏的结构化发现项。

        参数:
            code: 稳定规则码。
            message: 不含原始值的中文说明。
            path: 可选的制品内相对路径。
            details: 可选的计数、阈值或分类摘要。
            severity: error 或 warning。
        返回:
            无。
        """

        self.findings.append(
            AuditFinding(
                code=code,
                message=message,
                path=_redact_report_path(path) if path is not None else None,
                details=dict(details or {}),
                severity=severity,
            )
        )


def _sha256_bytes(data: bytes) -> str:
    """
    计算内存字节的 SHA-256。

    参数:
        data: 待计算的原始字节。
    返回:
        小写十六进制摘要。
    """

    return hashlib.sha256(data).hexdigest()


def _strict_json_object(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
    """
    将 JSON 对象键值对转为字典，并拒绝任意层级的重复键。

    参数:
        pairs: JSON 解码器按原始顺序提供的键值对。
    返回:
        无重复键的普通字典。
    异常:
        _StrictJsonError: 同一对象中出现重复键。
    """

    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _StrictJsonError("duplicate_json_key")
        result[key] = value
    return result


def _reject_nonfinite_json_number(value: str) -> None:
    """
    拒绝 JSON 标准之外的 NaN、Infinity 和负 Infinity 常量。

    参数:
        value: JSON 解码器遇到的非有限数字字面量；不会写入报告。
    返回:
        永不返回。
    异常:
        _StrictJsonError: 每次调用均抛出，确保 manifest fail closed。
    """

    del value
    raise _StrictJsonError("nonfinite_json_number")


def _parse_bounded_json_integer(value: str) -> int:
    """
    解析 manifest 的有界十进制整数，避免旧版 Python 处理超长整数造成 CPU 放大。

    参数:
        value: JSON 解码器提供的十进制整数字面量。
    返回:
        最多 20 个字符的 Python 整数。
    异常:
        _StrictJsonError: 字面量超过 manifest schema 所需的安全长度。
    """

    if len(value) > 20:
        raise _StrictJsonError("json_integer_too_long")
    return int(value, 10)


def _reject_json_float(value: str) -> None:
    """
    拒绝当前 manifest schema 不使用的浮点数字面量。

    参数:
        value: JSON 解码器提供的浮点字面量；不会写入报告。
    返回:
        永不返回。
    异常:
        _StrictJsonError: 每次调用均抛出，保持数字类型合同唯一。
    """

    del value
    raise _StrictJsonError("json_float_forbidden")


def _json_contains_unicode_surrogate(value: object) -> bool:
    """
    迭代检查 JSON 值中的未配对 UTF-16 surrogate，避免后续 UTF-8 编码异常。

    参数:
        value: 已由严格 JSON 解码器产生的基础类型、列表或字典。
    返回:
        任一键或字符串值含 U+D800-U+DFFF 时返回 True。
    """

    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            if any(0xD800 <= ord(character) <= 0xDFFF for character in current):
                return True
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    return False


def _load_strict_json_object(data: bytes) -> Mapping[str, Any]:
    """
    从严格 UTF-8 字节解析唯一 JSON 对象，拒绝 NUL、重复键和非有限数字。

    参数:
        data: manifest 的单次文件描述符快照字节。
    返回:
        顶层为对象的 JSON 映射。
    异常:
        UnicodeError: 字节不是严格 UTF-8。
        ValueError: JSON 语法、顶层类型或禁止结构无效。
        RecursionError: JSON 嵌套超过解释器安全深度。
    """

    if b"\x00" in data:
        raise _StrictJsonError("json_nul_forbidden")
    text = data.decode("utf-8", errors="strict")
    payload = json.loads(
        text,
        object_pairs_hook=_strict_json_object,
        parse_constant=_reject_nonfinite_json_number,
        parse_int=_parse_bounded_json_integer,
        parse_float=_reject_json_float,
    )
    if not isinstance(payload, dict):
        raise _StrictJsonError("json_root_not_object")
    if _json_contains_unicode_surrogate(payload):
        raise _StrictJsonError("json_unicode_surrogate_forbidden")
    return payload


def _bundle_manifest_schema_is_exact(manifest: Mapping[str, Any]) -> bool:
    """
    验证 offline_fake manifest 的每层键集合、基础类型和固定边界值。

    参数:
        manifest: 严格 JSON 解码后、仍不受信任的 manifest 对象。
    返回:
        所有对象均无未知/缺失字段且基础值满足当前 schema v1 时返回 True。
    """

    if set(manifest) != _BUNDLE_MANIFEST_KEYS:
        return False
    distribution = manifest.get("distribution")
    source = manifest.get("source")
    bridge = manifest.get("bridge")
    target = manifest.get("target")
    toolchain = manifest.get("toolchain")
    runtime = manifest.get("runtime")
    vendor_sdk = manifest.get("vendor_sdk")
    fingerprint = manifest.get("fingerprint")
    objects_and_keys = (
        (distribution, {"name", "version"}),
        (source, {"sha256", "files"}),
        (
            bridge,
            {
                "abi_version",
                "vendor_schema_id",
                "field_set_version",
                "artifact",
                "sha256",
                "vendor_sdk_linked",
            },
        ),
        (target, {"system", "machine", "python_abi_independent"}),
        (toolchain, {"cmake", "compiler", "build_type"}),
        (runtime, {"inspection_status", "dynamic_dependencies", "rpath"}),
        (vendor_sdk, {"included", "status"}),
        (fingerprint, {"algorithm", "value"}),
    )
    if any(not isinstance(value, dict) or set(value) != keys for value, keys in objects_and_keys):
        return False
    assert isinstance(distribution, dict)
    assert isinstance(source, dict)
    assert isinstance(bridge, dict)
    assert isinstance(target, dict)
    assert isinstance(toolchain, dict)
    assert isinstance(runtime, dict)
    assert isinstance(vendor_sdk, dict)
    assert isinstance(fingerprint, dict)
    version = distribution.get("version")
    if (
        distribution.get("name") != "bullet-trade"
        or not isinstance(version, str)
        or not version
        or len(version) > 128
    ):
        return False
    source_hash = source.get("sha256")
    source_files = source.get("files")
    if (
        not isinstance(source_hash, str)
        or _BUNDLE_NAME_PATTERN.fullmatch(source_hash) is None
        or not isinstance(source_files, list)
        or not 1 <= len(source_files) <= 10_000
    ):
        return False
    source_paths: Set[str] = set()
    for entry in source_files:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            return False
        source_path = entry.get("path")
        entry_hash = entry.get("sha256")
        if (
            not isinstance(source_path, str)
            or not source_path
            or len(source_path) > 512
            or not _safe_relative_path(source_path)
            or source_path in source_paths
            or not isinstance(entry_hash, str)
            or _BUNDLE_NAME_PATTERN.fullmatch(entry_hash) is None
        ):
            return False
        source_paths.add(source_path)
    try:
        calculated_source_hash = _sha256_bytes(
            json.dumps(
                {"files": source_files},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return False
    if source_hash != calculated_source_hash:
        return False
    artifact = bridge.get("artifact")
    bridge_hash = bridge.get("sha256")
    if (
        type(bridge.get("abi_version")) is not int
        or bridge.get("abi_version") != 2
        or bridge.get("vendor_schema_id") != "bullet_trade.huaxin.offline_fake.v1"
        or bridge.get("field_set_version") != "1"
        or not isinstance(artifact, str)
        or not isinstance(bridge_hash, str)
        or _BUNDLE_NAME_PATTERN.fullmatch(bridge_hash) is None
        or bridge.get("vendor_sdk_linked") is not False
    ):
        return False
    system = target.get("system")
    machine = target.get("machine")
    if (
        not isinstance(system, str)
        or system not in {"darwin", "linux", "windows"}
        or not isinstance(machine, str)
        or _normalize_machine(machine) not in {"x86_64", "aarch64"}
        or target.get("python_abi_independent") is not True
    ):
        return False
    if any(
        not isinstance(toolchain.get(key), str)
        or not toolchain.get(key)
        or len(toolchain.get(key, "")) > 4096
        for key in ("cmake", "compiler")
    ) or toolchain.get("build_type") not in {"Release", "RelWithDebInfo", "Debug"}:
        return False
    if runtime != {
        "inspection_status": "not_inspected",
        "dynamic_dependencies": None,
        "rpath": None,
    }:
        return False
    if manifest.get("integrity_scope") != "self_consistency_not_provenance":
        return False
    if vendor_sdk != {"included": False, "status": "not_used"}:
        return False
    expected_fingerprint = fingerprint.get("value")
    return (
        type(manifest.get("schema_version")) is int
        and manifest.get("schema_version") == 1
        and manifest.get("mode") == "offline_fake"
        and fingerprint.get("algorithm") == "sha256"
        and isinstance(expected_fingerprint, str)
        and _BUNDLE_NAME_PATTERN.fullmatch(expected_fingerprint) is not None
    )


def _sha256_path(path: Path) -> str:
    """
    流式计算普通文件的 SHA-256。

    参数:
        path: 待读取的普通文件。
    返回:
        小写十六进制摘要。
    副作用:
        只读打开文件，不修改制品。
    """

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(MEBIBYTE), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_outer_artifact_snapshot(
    path: Path,
    max_bytes: int,
    artifact_label: str,
    builder: _AuditBuilder,
) -> Tuple[Optional[bytes], Optional[int], Optional[str]]:
    """
    通过同一只读文件描述符获取外层制品大小、字节和哈希，避免 hash/解析跨代 TOCTOU。

    参数:
        path: 已完成词法符号链接检查的外层 artifact 路径。
        max_bytes: 防 DoS 最大读取字节数。
        artifact_label: 脱敏错误文案中的 wheel 或 sdist 名称。
        builder: 接收打开、类型、硬上限或并发修改发现。
    返回:
        ``(完整字节, 大小, SHA-256)``；失败/超限时字节和哈希为 None。
    """

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        builder.add(
            "ARTIFACT_READ_FAILED",
            "{} 外层普通文件无法安全打开".format(artifact_label),
            details={"error_type": type(exc).__name__},
        )
        return None, None, None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            builder.add("ARTIFACT_NOT_REGULAR", "外层制品不是普通文件")
            return None, before.st_size, None
        if before.st_size > max_bytes:
            builder.add(
                "ARCHIVE_HARD_LIMIT",
                "{} 外层大小超过审计防 DoS 硬上限，拒绝读取".format(artifact_label),
                details={"size": before.st_size, "max_bytes": max_bytes},
            )
            return None, before.st_size, None
        chunks: List[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            block = os.read(descriptor, min(MEBIBYTE, remaining))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        stable = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            getattr(before, "st_mtime_ns", None),
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            getattr(after, "st_mtime_ns", None),
        )
        if not stable or len(data) != before.st_size:
            builder.add(
                "ARTIFACT_CHANGED_DURING_AUDIT",
                "外层制品在同一快照读取期间发生变化",
            )
            return None, before.st_size, None
        return data, before.st_size, _sha256_bytes(data)
    except OSError as exc:
        builder.add(
            "ARTIFACT_READ_FAILED",
            "{} 外层普通文件读取失败".format(artifact_label),
            details={"error_type": type(exc).__name__},
        )
        return None, None, None
    finally:
        os.close(descriptor)


def _canonical_distribution_name(
    value: object,
    builder: _AuditBuilder,
    path: Optional[str] = None,
) -> Optional[str]:
    """
    使用 packaging 的 PEP 503 规范化器验证 distribution 名称。

    参数:
        value: 元数据或文件名提供的 distribution 名称。
        builder: 接收解析器缺失或名称非法发现。
        path: 可选的归档内元数据路径。
    返回:
        合法名称的 PEP 503 规范形式；解析器缺失或输入非法时返回 None。
    """

    if _PACKAGING_UTILS is None:
        builder.add(
            "PACKAGING_PARSER_UNAVAILABLE",
            "缺少 packaging，无法按 PEP 503 验证 distribution 身份",
            path,
        )
        return None
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9]+(?:[-_.]+[A-Za-z0-9]+)*", value
    ):
        return None
    try:
        normalized = _PACKAGING_UTILS.canonicalize_name(value)
    except (AttributeError, TypeError, ValueError):
        return None
    return str(normalized)


def _contains_forbidden_huaxin_requirement(
    values: Sequence[object],
    builder: _AuditBuilder,
    path: Optional[str] = None,
) -> bool:
    """
    结构化解析依赖声明并识别 PEP 503 等价的独立 Huaxin distribution。

    参数:
        values: METADATA、PKG-INFO 或 pyproject 提供的依赖声明序列。
        builder: 接收解析器缺失或依赖声明非法发现。
        path: 可选的归档内元数据路径。
    返回:
        任一合法依赖名规范化为 ``bullet-trade-huaxin`` 时返回 True。
    """

    if _PACKAGING_REQUIREMENTS is None or _PACKAGING_UTILS is None:
        builder.add(
            "PACKAGING_PARSER_UNAVAILABLE",
            "缺少 packaging，无法结构化验证 dependency 身份",
            path,
        )
        return False
    invalid_count = 0
    forbidden = False
    for value in values:
        if not isinstance(value, str):
            invalid_count += 1
            continue
        try:
            requirement = _PACKAGING_REQUIREMENTS.Requirement(value)
            normalized = _PACKAGING_UTILS.canonicalize_name(requirement.name)
        except (AttributeError, TypeError, ValueError):
            invalid_count += 1
            continue
        if str(normalized) == "bullet-trade-huaxin":
            forbidden = True
    if invalid_count:
        builder.add(
            "REQUIREMENT_METADATA_INVALID",
            "依赖元数据包含无法由 packaging 唯一解析的声明",
            path,
            {"invalid_count": invalid_count},
        )
    return forbidden


def _canonical_version(
    value: object,
    builder: _AuditBuilder,
    path: Optional[str] = None,
) -> Optional[str]:
    """
    使用 packaging 的 PEP 440 解析器验证版本并返回规范形式。

    参数:
        value: 元数据或文件名提供的版本值。
        builder: 接收解析器缺失发现。
        path: 可选的归档内元数据路径。
    返回:
        合法 PEP 440 版本的规范字符串；解析器缺失或输入非法时返回 None。
    """

    if _PACKAGING_VERSION is None:
        builder.add(
            "PACKAGING_PARSER_UNAVAILABLE",
            "缺少 packaging，无法按 PEP 440 验证版本身份",
            path,
        )
        return None
    if not isinstance(value, str):
        return None
    try:
        return str(_PACKAGING_VERSION.Version(value))
    except (AttributeError, TypeError, ValueError):
        return None


def _safe_wheel_report_tags(filename_tags: Mapping[str, Any]) -> Mapping[str, Any]:
    """
    把内部 wheel 文件名解析结果投影为不回显任意标签的公开摘要。

    参数:
        filename_tags: `_parse_wheel_filename` 返回的内部精确标签。
    返回:
        仅含 V1 白名单标签、版本有效性和布尔分类的报告映射。
    """

    python_tag = filename_tags.get("python_tag")
    abi_tag = filename_tags.get("abi_tag")
    platform_tag = filename_tags.get("platform_tag")
    tag = (
        "{}-{}-{}".format(python_tag, abi_tag, platform_tag)
        if all(isinstance(item, str) for item in (python_tag, abi_tag, platform_tag))
        else None
    )
    tag_supported = tag in _PUBLIC_WHEEL_TAGS
    return {
        "filename_distribution": filename_tags.get("filename_distribution"),
        "filename_version_valid": filename_tags.get("filename_version") is not None,
        "python_tag": python_tag if tag_supported else "<invalid-tag>",
        "abi_tag": abi_tag if tag_supported else "<invalid-tag>",
        "platform_tag": platform_tag if tag_supported else "<invalid-tag>",
        "universal": bool(filename_tags.get("universal")),
        "tag_supported": tag_supported,
    }


def _basic_relative_path(value: str) -> bool:
    """
    判断路径是否为不含转义、绝对前缀和点段的 POSIX 相对路径。

    参数:
        value: 制品内部路径字符串。
    返回:
        基础结构安全时返回 True。
    """

    if not value or "\x00" in value or "\\" in value:
        return False
    if value.startswith("/") or re.match(r"^[A-Za-z]:", value):
        return False
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        return False
    return path.as_posix() == value


def _portable_relative_path(value: str) -> bool:
    """
    校验归档路径在 Windows、Linux 和 macOS 上均无设备名、ADS 或归一化歧义。

    参数:
        value: 已通过基础结构校验的 POSIX 相对路径。
    返回:
        所有组件满足跨平台可移植约束时返回 True。
    """

    if not _basic_relative_path(value):
        return False
    reserved = {"con", "prn", "aux", "nul"}
    reserved.update("com{}".format(index) for index in range(1, 10))
    reserved.update("lpt{}".format(index) for index in range(1, 10))
    for part in PurePosixPath(value).parts:
        if any(ord(character) < 32 or ord(character) == 127 for character in part):
            return False
        if any(character in part for character in '<>:"|?*') or part.endswith((" ", ".")):
            return False
        if unicodedata.normalize("NFC", part) != part:
            return False
        device_stem = part.split(".", 1)[0].casefold()
        if device_stem in reserved:
            return False
    return True


def _safe_relative_path(value: str) -> bool:
    """
    判断归档或清单路径是否是无歧义的 POSIX 相对路径。

    参数:
        value: 制品内部路径字符串。
    返回:
        不含绝对前缀、反斜线、NUL、空段、点段或父目录时返回 True。
    """

    return _portable_relative_path(value)


def _redact_report_path(value: str) -> str:
    """
    将不安全成员名替换为稳定摘要，避免报告回显绝对路径或穿越载荷。

    参数:
        value: Git、归档、RECORD 或 bundle 提供的原始路径。
    返回:
        安全相对路径保持不变；其他值返回不可逆的摘要占位符。
    """

    sensitive_component = re.search(
        r"(?i)(?:password|passwd|secret|credential|account(?:[-_.]?id)?|"
        r"investor(?:[-_.]?id)?|terminal(?:[-_.]?info)?|access(?:[-_.]?token)|"
        r"api(?:[-_.]?key))",
        value,
    )
    if _safe_relative_path(value) and sensitive_component is None:
        return value
    digest = hashlib.sha256(value.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]
    return "<unsafe-path-sha256:{}>".format(digest)


def _path_has_symlink_component(value: Path) -> bool:
    """
    按用户提供的词法路径检查最终 artifact 自身是否为符号链接。

    参数:
        value: 尚未调用 resolve 的输入路径。
    返回:
        最终 artifact 是符号链接时返回 True；系统级父目录别名由后续内容快照约束。
    """

    try:
        return value.is_symlink()
    except OSError:
        return True


def _validate_inventory_paths(files: Sequence[_ArtifactFile], builder: _AuditBuilder) -> None:
    """
    检查路径穿越、完全重复和大小写折叠冲突。

    参数:
        files: 已收集的制品文件序列。
        builder: 接收结构化发现项的构建器。
    返回:
        无。
    """

    exact: Dict[str, int] = {}
    folded: Dict[str, str] = {}
    for item in files:
        if not _basic_relative_path(item.path):
            builder.add("PATH_TRAVERSAL", "制品包含不安全或不规范的内部路径", item.path)
        elif not _portable_relative_path(item.path):
            builder.add(
                "NONPORTABLE_ARCHIVE_PATH",
                "制品路径包含控制字符、Windows 保留字符/设备名、尾随点空格或非 NFC 组件",
                item.path,
            )
        exact[item.path] = exact.get(item.path, 0) + 1
        canonical = unicodedata.normalize("NFC", item.path).casefold()
        previous = folded.get(canonical)
        if previous is not None and previous != item.path:
            builder.add(
                "PATH_NORMALIZATION_COLLISION",
                "制品包含跨平台可能冲突的 Unicode/大小写路径",
                item.path,
                {"other_path": _redact_report_path(previous)},
            )
        folded[canonical] = item.path
    for path, count in sorted(exact.items()):
        if count > 1:
            builder.add(
                "DUPLICATE_ARCHIVE_PATH",
                "制品包含重复内部路径",
                path,
                {"count": count},
            )


def _native_format(data: bytes) -> Optional[str]:
    """
    依据文件魔数识别 ELF、Mach-O 或 PE 原生制品。

    参数:
        data: 至少包含文件头的原始字节。
    返回:
        elf、mach_o、pe，无法识别时返回 None。
    """

    if data.startswith(b"\x7fELF"):
        return "elf"
    if data.startswith(b"MZ"):
        return "pe"
    if data[:4] in {
        b"\xfe\xed\xfa\xce",
        b"\xce\xfa\xed\xfe",
        b"\xfe\xed\xfa\xcf",
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"\xbe\xba\xfe\xca",
        b"\xca\xfe\xba\xbf",
        b"\xbf\xba\xfe\xca",
    }:
        return "mach_o"
    return None


def _native_architecture(data: bytes, native_format: str) -> Optional[str]:
    """
    从 ELF 或 PE 固定头部读取受支持的 CPU 架构，不执行原生文件。

    参数:
        data: 原生文件完整字节。
        native_format: 已由魔数识别的 ``elf``、``pe`` 或 ``mach_o``。
    返回:
        ``x86_64``、``x86``、``aarch64``；头部不完整、组合非法或未知时返回 None。
    """

    if native_format == "elf":
        if len(data) < 20 or data[4] != 2 or data[5] != 1:
            return None
        if int.from_bytes(data[16:18], byteorder="little") != 3:
            return None
        machine = int.from_bytes(data[18:20], byteorder="little")
        return {0x3E: "x86_64", 0x03: "x86", 0xB7: "aarch64"}.get(machine)
    if native_format == "pe":
        if len(data) < 64:
            return None
        pe_offset = int.from_bytes(data[60:64], byteorder="little")
        if pe_offset < 64 or pe_offset + 26 > len(data):
            return None
        if data[pe_offset : pe_offset + 4] != b"PE\x00\x00":
            return None
        machine = int.from_bytes(data[pe_offset + 4 : pe_offset + 6], byteorder="little")
        optional_magic = int.from_bytes(data[pe_offset + 24 : pe_offset + 26], byteorder="little")
        if machine == 0x8664 and optional_magic == 0x20B:
            return "x86_64"
        if machine == 0x014C and optional_magic == 0x10B:
            return "x86"
        if machine == 0xAA64 and optional_magic == 0x20B:
            return "aarch64"
        return None
    if native_format == "mach_o":
        magic = data[:4]
        thin_big_endian = {b"\xfe\xed\xfa\xce", b"\xfe\xed\xfa\xcf"}
        thin_little_endian = {b"\xce\xfa\xed\xfe", b"\xcf\xfa\xed\xfe"}
        if magic in thin_big_endian and len(data) >= 8:
            cpu_type = int.from_bytes(data[4:8], byteorder="big")
            return {0x01000007: "x86_64", 0x00000007: "x86", 0x0100000C: "aarch64"}.get(cpu_type)
        if magic in thin_little_endian and len(data) >= 8:
            cpu_type = int.from_bytes(data[4:8], byteorder="little")
            return {0x01000007: "x86_64", 0x00000007: "x86", 0x0100000C: "aarch64"}.get(cpu_type)
        fat_big_endian = {b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf"}
        fat_little_endian = {b"\xbe\xba\xfe\xca", b"\xbf\xba\xfe\xca"}
        if magic in fat_big_endian and len(data) >= 12:
            cpu_type = int.from_bytes(data[8:12], byteorder="big")
            architecture = {
                0x01000007: "x86_64",
                0x00000007: "x86",
                0x0100000C: "aarch64",
            }.get(cpu_type)
            return "multi" if architecture is not None else None
        if magic in fat_little_endian and len(data) >= 12:
            cpu_type = int.from_bytes(data[8:12], byteorder="little")
            architecture = {
                0x01000007: "x86_64",
                0x00000007: "x86",
                0x0100000C: "aarch64",
            }.get(cpu_type)
            return "multi" if architecture is not None else None
    return None


def _audit_elf_shared_image(
    data: bytes,
    relative_path: str,
    builder: _AuditBuilder,
) -> bool:
    """
    验证 ELF64 小端文件是无解释器、非 PIE 的 ET_DYN 共享库映像。

    参数:
        data: 原生文件的同代快照字节。
        relative_path: 报告中的制品相对路径。
        builder: 接收布局、解释器或 PIE 发现。
    返回:
        完整满足共享库映像合同时返回 True。
    """

    if (
        len(data) < 64
        or data[:4] != b"\x7fELF"
        or data[4:7] != b"\x02\x01\x01"
        or int.from_bytes(data[16:18], "little") != 3
        or int.from_bytes(data[52:54], "little") < 64
    ):
        builder.add(
            "NATIVE_IMAGE_TYPE_INVALID",
            "ELF bridge 必须是 ELF64 小端 ET_DYN 共享库",
            relative_path,
        )
        return False
    program_offset = int.from_bytes(data[32:40], "little")
    program_entry_size = int.from_bytes(data[54:56], "little")
    program_count = int.from_bytes(data[56:58], "little")
    if (
        program_count in {0, 0xFFFF}
        or program_entry_size < 56
        or program_offset < 64
        or program_offset + program_entry_size * program_count > len(data)
    ):
        builder.add(
            "NATIVE_IMAGE_LAYOUT_INVALID",
            "ELF program header 布局被截断或使用未支持的扩展计数",
            relative_path,
        )
        return False
    dynamic_ranges: List[Tuple[int, int]] = []
    has_interpreter = False
    for index in range(program_count):
        offset = program_offset + index * program_entry_size
        program_type = int.from_bytes(data[offset : offset + 4], "little")
        file_offset = int.from_bytes(data[offset + 8 : offset + 16], "little")
        file_size = int.from_bytes(data[offset + 32 : offset + 40], "little")
        if file_offset > len(data) or file_size > len(data) - file_offset:
            builder.add(
                "NATIVE_IMAGE_LAYOUT_INVALID",
                "ELF program segment 越过原生快照边界",
                relative_path,
            )
            return False
        if program_type == 3:
            has_interpreter = True
        elif program_type == 2:
            dynamic_ranges.append((file_offset, file_size))
    if has_interpreter:
        builder.add(
            "NATIVE_ELF_INTERPRETER_FORBIDDEN",
            "ELF bridge 共享库不得包含 PT_INTERP 可执行入口",
            relative_path,
        )
        return False
    if not dynamic_ranges:
        builder.add(
            "NATIVE_IMAGE_LAYOUT_INVALID",
            "ELF bridge 缺少可验证的 PT_DYNAMIC 段",
            relative_path,
        )
        return False
    pie = False
    for dynamic_offset, dynamic_size in dynamic_ranges:
        if dynamic_size == 0 or dynamic_size % 16:
            builder.add(
                "NATIVE_IMAGE_LAYOUT_INVALID",
                "ELF PT_DYNAMIC 长度不是完整 Elf64_Dyn 序列",
                relative_path,
            )
            return False
        saw_terminator = False
        for offset in range(dynamic_offset, dynamic_offset + dynamic_size, 16):
            tag = int.from_bytes(data[offset : offset + 8], "little")
            value = int.from_bytes(data[offset + 8 : offset + 16], "little")
            if tag == 0:
                saw_terminator = True
                break
            if tag == 0x6FFFFFFB and value & 0x08000000:
                pie = True
        if not saw_terminator:
            builder.add(
                "NATIVE_IMAGE_LAYOUT_INVALID",
                "ELF PT_DYNAMIC 缺少 DT_NULL 终止项",
                relative_path,
            )
            return False
    if pie:
        builder.add(
            "NATIVE_ELF_PIE_FORBIDDEN",
            "ELF bridge 不得设置 DF_1_PIE 可执行标记",
            relative_path,
        )
        return False
    return True


def _audit_macho_shared_image(
    data: bytes,
    relative_path: str,
    builder: _AuditBuilder,
) -> bool:
    """
    验证 Mach-O 为单架构 MH_DYLIB，而不是 executable、bundle 或 fat 包装。

    参数:
        data: 原生文件的同代快照字节。
        relative_path: 报告中的制品相对路径。
        builder: 接收映像类型发现。
    返回:
        单架构 Mach-O filetype 等于 MH_DYLIB 时返回 True。
    """

    magic = data[:4]
    little_magics = {b"\xce\xfa\xed\xfe", b"\xcf\xfa\xed\xfe"}
    big_magics = {b"\xfe\xed\xfa\xce", b"\xfe\xed\xfa\xcf"}
    if len(data) < 16 or magic not in little_magics | big_magics:
        builder.add(
            "NATIVE_IMAGE_TYPE_INVALID",
            "Mach-O bridge 必须是单架构 MH_DYLIB 映像",
            relative_path,
        )
        return False
    file_type = (
        int.from_bytes(data[12:16], "little")
        if magic in little_magics
        else int.from_bytes(data[12:16], "big")
    )
    if file_type != 6:
        builder.add(
            "NATIVE_IMAGE_TYPE_INVALID",
            "Mach-O bridge filetype 必须唯一等于 MH_DYLIB",
            relative_path,
        )
        return False
    return True


def _audit_pe_shared_image(
    data: bytes,
    relative_path: str,
    builder: _AuditBuilder,
) -> bool:
    """
    验证 PE bridge 的 COFF Characteristics 声明 IMAGE_FILE_DLL。

    参数:
        data: 原生文件的同代快照字节。
        relative_path: 报告中的制品相对路径。
        builder: 接收布局或类型发现。
    返回:
        PE 头完整且 DLL 位已设置时返回 True。
    """

    if len(data) < 64 or data[:2] != b"MZ":
        builder.add("NATIVE_IMAGE_TYPE_INVALID", "PE bridge 缺少有效 DOS/PE 头", relative_path)
        return False
    pe_offset = int.from_bytes(data[60:64], "little")
    if pe_offset < 64 or pe_offset + 24 > len(data) or data[pe_offset : pe_offset + 4] != b"PE\0\0":
        builder.add("NATIVE_IMAGE_LAYOUT_INVALID", "PE bridge 头部被截断", relative_path)
        return False
    characteristics = int.from_bytes(data[pe_offset + 22 : pe_offset + 24], "little")
    if characteristics & 0x2000 == 0:
        builder.add(
            "NATIVE_IMAGE_TYPE_INVALID",
            "PE bridge 必须设置 IMAGE_FILE_DLL 特征位",
            relative_path,
        )
        return False
    return True


def _audit_dynamic_library_image(
    data: bytes,
    native_format: str,
    relative_path: str,
    builder: _AuditBuilder,
) -> bool:
    """
    按原生格式路由真实动态库映像类型合同，拒绝改名可执行文件。

    参数:
        data: 原生文件的同代快照字节。
        native_format: elf、mach_o 或 pe。
        relative_path: 报告中的制品相对路径。
        builder: 接收格式特定发现。
    返回:
        对应平台动态库合同完整通过时返回 True。
    """

    if native_format == "elf":
        return _audit_elf_shared_image(data, relative_path, builder)
    if native_format == "mach_o":
        return _audit_macho_shared_image(data, relative_path, builder)
    if native_format == "pe":
        return _audit_pe_shared_image(data, relative_path, builder)
    builder.add(
        "NATIVE_FORMAT_UNSUPPORTED",
        "native 魔数格式没有动态库映像合同",
        relative_path,
    )
    return False


def _normalize_machine(value: object) -> Optional[str]:
    """
    把平台或 manifest 的常见 CPU 名称归一为审计架构标识。

    参数:
        value: 机器架构字符串。
    返回:
        ``x86_64``、``x86`` 或 ``aarch64``；未知值返回 None。
    """

    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace("-", "_")
    return {
        "amd64": "x86_64",
        "x86_64": "x86_64",
        "i386": "x86",
        "i686": "x86",
        "x86": "x86",
        "arm64": "aarch64",
        "aarch64": "aarch64",
    }.get(normalized)


def _platform_wheel_contract(platform_tag: object) -> Optional[Mapping[str, str]]:
    """
    把 V1 条件 platform wheel 标签映射为唯一原生格式、架构和后缀合同。

    参数:
        platform_tag: wheel 文件名解析得到的平台标签。
    返回:
        Linux x86_64 或 Windows x64 合同；未知、多架构或非 V1 标签返回 None。
    """

    if not isinstance(platform_tag, str) or not platform_tag:
        return None
    if platform_tag == "linux_x86_64":
        return {
            "format": "elf",
            "architecture": "x86_64",
            "suffix": ".so",
            "path": "bullet_trade/integrations/huaxin/libbullet_trade_huaxin.so",
        }
    if platform_tag == "win_amd64":
        return {
            "format": "pe",
            "architecture": "x86_64",
            "suffix": ".dll",
            "path": "bullet_trade/integrations/huaxin/bullet_trade_huaxin.dll",
        }
    return None


def _audit_platform_wheel_native_contract(
    native_files: Sequence[Tuple[_ArtifactFile, str]],
    filename_tags: Mapping[str, Any],
    builder: _AuditBuilder,
) -> None:
    """
    校验条件 platform wheel 的标签、路径、格式、架构、后缀与 flat C ABI 标记一致。

    参数:
        native_files: 已按魔数识别的原生文件。
        filename_tags: wheel 文件名解析结果。
        builder: 接收 fail-closed 发现项。
    返回:
        无；所有不一致均追加稳定发现项。
    """

    contract = _platform_wheel_contract(filename_tags.get("platform_tag"))
    if contract is None:
        builder.add(
            "PLATFORM_WHEEL_TAG_UNSUPPORTED",
            "条件 platform wheel 仅允许 Linux x86_64 或 Windows x64 V1 标签",
        )
        return
    if (filename_tags.get("python_tag"), filename_tags.get("abi_tag")) != ("py3", "none"):
        builder.add(
            "PLATFORM_WHEEL_ABI_INVALID",
            "ctypes flat C ABI platform wheel 必须使用 py3-none Python/ABI 标签",
        )
    if len(native_files) != 1:
        builder.add(
            "PLATFORM_WHEEL_NATIVE_COUNT_INVALID",
            "条件 platform wheel 必须且只能包含一个第一方 Huaxin bridge native",
            details={"count": len(native_files)},
        )
    for item, native_format in native_files:
        lowered_path = item.path.lower()
        suffix = PurePosixPath(lowered_path).suffix
        if item.data is None:
            builder.add(
                "NATIVE_IMAGE_LAYOUT_INVALID",
                "platform wheel bridge 字节不可读，无法验证动态库映像",
                item.path,
            )
        else:
            _audit_dynamic_library_image(item.data, native_format, item.path, builder)
        if lowered_path != contract["path"]:
            builder.add(
                "PLATFORM_BRIDGE_PATH_INVALID",
                "platform wheel bridge 必须使用目标平台唯一 canonical 包内路径",
                item.path,
            )
        if native_format != contract["format"]:
            builder.add(
                "PLATFORM_NATIVE_FORMAT_MISMATCH",
                "wheel platform tag 与 bridge 原生格式不一致",
                item.path,
                {"expected_format": contract["format"], "actual_format": native_format},
            )
        if suffix != contract["suffix"]:
            builder.add(
                "PLATFORM_NATIVE_SUFFIX_MISMATCH",
                "wheel platform tag 与 bridge 文件后缀不一致",
                item.path,
                {"expected_suffix": contract["suffix"], "actual_suffix": suffix},
            )
        architecture = (
            _native_architecture(item.data, native_format) if item.data is not None else None
        )
        if architecture != contract["architecture"]:
            builder.add(
                "PLATFORM_NATIVE_ARCH_MISMATCH",
                "wheel platform tag 与 bridge CPU 架构或原生头部不一致",
                item.path,
                {
                    "expected_architecture": contract["architecture"],
                    "actual_architecture": architecture,
                },
            )
        missing_symbol_count = sum(
            1 for symbol in _EXPECTED_BRIDGE_SYMBOLS if item.data is None or symbol not in item.data
        )
        if missing_symbol_count:
            builder.add(
                "PLATFORM_BRIDGE_SYMBOLS_MISSING",
                "platform wheel bridge 缺少当前 flat C ABI 的必要导出符号标记",
                item.path,
                {"missing_count": missing_symbol_count},
            )


def _forbidden_magic(data: bytes) -> Optional[str]:
    """
    识别不能进入公开源码/universal 制品的二进制或文档魔数。

    参数:
        data: 文件原始字节。
    返回:
        稳定魔数分类；普通文本/图片等允许内容返回 None。
    """

    native = _native_format(data)
    if native is not None:
        return native
    if data.startswith(b"%PDF-"):
        return "pdf"
    if data.startswith(b"!<arch>\n"):
        return "static_archive"
    if data.startswith(b"PK\x03\x04") or data.startswith(b"PK\x05\x06"):
        return "zip_archive"
    if data.startswith(b"\x1f\x8b"):
        return "gzip_archive"
    return None


def _is_self_bridge_path(path: str) -> bool:
    """
    判断 native 路径是否位于第一方华鑫包且文件名属于自研 bridge。

    参数:
        path: 制品内 POSIX 相对路径。
    返回:
        路径和文件名都满足第一方边界时返回 True。
    """

    lowered = path.lower()
    name = PurePosixPath(lowered).name
    return (
        lowered.startswith("bullet_trade/integrations/huaxin/")
        and any(name.startswith(stem) for stem in _SELF_BRIDGE_STEMS)
        and PurePosixPath(lowered).suffix in _NATIVE_SUFFIXES
    ) or (
        lowered.startswith("lib/")
        and any(name.startswith(stem) for stem in _SELF_BRIDGE_STEMS)
        and PurePosixPath(lowered).suffix in _NATIVE_SUFFIXES
    )


def _is_placeholder_secret(value: str) -> bool:
    """
    判断配置字面量是否明显是公开模板或测试占位值。

    参数:
        value: 已从配置行提取但不会写入报告的候选值。
    返回:
        值为空或精确匹配受控环境引用/公开占位字面量时返回 True。
    """

    normalized = value.strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in "'\"":
        normalized = normalized[1:-1].strip()
    normalized = normalized.lower()
    if not normalized:
        return True
    if len(normalized) >= 3 and set(normalized) <= {"x", "*"}:
        return True
    if re.fullmatch(r"\$\{[a-z_][a-z0-9_]*\}", normalized):
        return True
    if re.fullmatch(r"<[a-z0-9_. -]{1,64}>", normalized):
        return True
    exact_placeholders = {
        "***",
        "changeme",
        "change_me",
        "change-me",
        "change_me_gateway_password",
        "change_me_hmac_secret",
        "dummy",
        "example",
        "fake",
        "invalid",
        "mock",
        "placeholder",
        "redacted",
        "replace-me",
        "replace_me",
        "sample",
        "secret",
        "test",
        "true",
        "false",
        "none",
        "null",
        "wrong",
        "xxxxx",
        "your-password",
        "your_password",
        "your-secret",
        "your_secret",
        "your-token",
        "your_token",
    }
    return normalized in exact_placeholders


def _sensitive_key_class(key: object, vendor_scoped: bool) -> Optional[str]:
    """
    将配置键映射为固定敏感类别，避免把攻击者控制的键名写入报告。

    参数:
        key: JSON、AST 或文本配置提供的键。
        vendor_scoped: 当前值是否位于 Huaxin/TORA 配置上下文。
    返回:
        固定类别名；非敏感键返回 None。
    """

    normalized = str(key).strip().lower().replace("-", "_")
    prefixed = normalized.startswith(("huaxin_", "tora_"))
    if prefixed:
        normalized = normalized.split("_", 1)[1]
    categories = {
        "password": "credential",
        "passwd": "credential",
        "secret": "credential",
        "api_key": "credential",
        "access_token": "credential",
        "dynamic_password": "credential",
        "dynamic_token": "credential",
        "dynamic_code": "credential",
        "terminal_info": "terminal_identity",
        "terminalinfo": "terminal_identity",
        "account": "account_identity",
        "account_id": "account_identity",
        "investor_id": "account_identity",
        "user": "account_identity",
        "user_id": "account_identity",
        "broker": "account_identity",
        "broker_id": "account_identity",
        "trade_front": "service_endpoint",
        "md_front": "service_endpoint",
        "front": "service_endpoint",
        "endpoint": "service_endpoint",
        "address": "service_endpoint",
        "host": "service_endpoint",
        "server": "service_endpoint",
    }
    category = categories.get(normalized)
    globally_sensitive = category in {"credential", "terminal_identity"}
    if category is not None and (globally_sensitive or vendor_scoped or prefixed):
        return category
    if prefixed:
        return "vendor_config"
    return None


def _looks_like_structured_sensitive_text(text: str) -> bool:
    """
    判断候选解码结果是否像配置、源码、密钥或 SDK 路径文本。

    参数:
        text: UTF-16 候选解码结果。
    返回:
        文本以高比例可打印字符组成且含受支持结构标记时返回 True。
    """

    if not text or "\x00" in text:
        return False
    printable_ratio = sum(
        character.isprintable() or character in "\r\n\t" for character in text
    ) / len(text)
    if printable_ratio < 0.85:
        return False
    structured_pattern = re.compile(
        r"(?im)(?:^|[\r\n{,])\s*[\"']?[A-Za-z_]"
        r"[A-Za-z0-9_.-]*[\"']?\s*[:=]|"
        r"-----BEGIN\s+(?:[A-Z0-9]+\s+)*PRIVATE\s+KEY-----|"
        r"(?:^|[\s\"'])[A-Za-z]:[\\/]|"
        r"(?:^|[\s\"'])/(?:Users|home|root|srv|private|tmp|var|opt|usr/local|mnt|data)/"
    )
    return structured_pattern.search(text) is not None


def _decode_sensitive_text(data: bytes) -> Tuple[str, bool]:
    """
    解码敏感扫描文本，支持 BOM 及高置信度无 BOM UTF-16，并保留兼容回退。

    参数:
        data: 单文件受大小约束的原始字节。
    返回:
        ``(可扫描文本, 是否发生截断或替代解码)``；第二项用于配置文件 fail closed。
    """

    if data.startswith(b"\xef\xbb\xbf"):
        try:
            return data.decode("utf-8-sig", errors="strict"), False
        except UnicodeError:
            return data.decode("utf-8-sig", errors="replace"), True
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16", errors="strict"), False
        except UnicodeError:
            return data.decode("utf-16", errors="replace"), True
    try:
        utf8_text: Optional[str] = data.decode("utf-8", errors="strict")
    except UnicodeError:
        utf8_text = None
    if len(data) >= 8 and (utf8_text is None or "\x00" in utf8_text):
        even_data = data[: (len(data) // 2) * 2]
        utf16_candidates: List[str] = []
        candidate_decode_failed = len(even_data) != len(data)
        for encoding in ("utf-16-le", "utf-16-be"):
            try:
                candidate = even_data.decode(encoding, errors="strict")
            except UnicodeError:
                candidate = even_data.decode(encoding, errors="replace")
                candidate_decode_failed = True
            if _looks_like_structured_sensitive_text(candidate):
                utf16_candidates.append(candidate)
        if utf16_candidates:
            return "\n".join(utf16_candidates), candidate_decode_failed
    if utf8_text is not None:
        return utf8_text, "\x00" in utf8_text
    return data.decode("latin-1", errors="ignore"), True


def _json_sensitive_key_classes(
    text: str,
    huaxin_scoped: bool,
) -> Tuple[Tuple[str, ...], bool]:
    """
    递归解析 JSON 配置并返回非占位 Huaxin/凭据键类别，不回传原始值。

    参数:
        text: 待解析 JSON 文本。
        huaxin_scoped: 文件路径是否位于 Huaxin/TORA 专属边界。
    返回:
        ``(固定敏感类别, 解析失败)``；深度或语法异常时第二项为 True。
    """

    try:
        payload = json.loads(text, object_pairs_hook=_strict_json_object)
    except (TypeError, ValueError, UnicodeError, RecursionError, _StrictJsonError):
        return tuple(), True
    found: List[str] = []
    stack: List[Tuple[object, bool, Optional[str]]] = [(payload, huaxin_scoped, None)]
    while stack:
        value, scoped, identity_container = stack.pop()
        if isinstance(value, dict):
            locally_scoped = scoped or any(
                str(raw_key).strip().lower().replace("-", "_") in {"vendor", "provider"}
                and isinstance(child, str)
                and str(child).strip().lower().replace("-", "_") in {"huaxin", "tora"}
                for raw_key, child in value.items()
            )
            for raw_key, child in value.items():
                key = str(raw_key).strip().lower().replace("-", "_")
                child_scoped = (
                    locally_scoped
                    or key in {"huaxin", "tora"}
                    or key.startswith(("huaxin_", "tora_"))
                )
                category: Optional[str]
                if (
                    locally_scoped
                    and key == "id"
                    and identity_container in {"account", "broker", "investor", "user"}
                ):
                    category = "account_identity"
                else:
                    category = _sensitive_key_class(key, locally_scoped)
                if category is not None:
                    if isinstance(child, (dict, list)):
                        if child:
                            found.append(category)
                    else:
                        scalar = "" if child is None else str(child)
                        if not _is_placeholder_secret(scalar):
                            found.append(category)
                child_identity_container = identity_container
                if locally_scoped and key in {"account", "broker", "investor", "user"}:
                    child_identity_container = key
                stack.append((child, child_scoped, child_identity_container))
        elif isinstance(value, list):
            for child in value:
                stack.append((child, scoped, identity_container))
    return tuple(sorted(set(found))), False


def _python_sensitive_key_classes(text: str) -> Tuple[Tuple[str, ...], bool]:
    """
    用 AST 扫描 Huaxin Python 配置中的常量赋值和字典敏感值。

    参数:
        text: 已按 BOM 解码的 Python 源码。
    返回:
        ``(固定敏感类别, 解析失败)``；不执行源码且不回传原值。
    """

    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError, RecursionError):
        return tuple(), True
    found: List[str] = []

    def record(key: object, value_node: ast.AST) -> None:
        """
        检查单个 AST 键和值是否形成非占位敏感常量。

        参数:
            key: 赋值目标名或字典键。
            value_node: 不会求值、仅接受 Constant 的 AST 值节点。
        返回:
            无；命中时向外层固定类别集合追加类别。
        """

        category = _sensitive_key_class(key, True)
        if category is None or not isinstance(value_node, ast.Constant):
            return
        raw_value = value_node.value
        if isinstance(raw_value, (dict, list, tuple, set)):
            return
        scalar = "" if raw_value is None else str(raw_value)
        if not _is_placeholder_secret(scalar):
            found.append(category)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    record(target.id, node.value)
                elif isinstance(target, ast.Attribute):
                    record(target.attr, node.value)
        elif isinstance(node, ast.AnnAssign):
            if node.value is not None and isinstance(node.target, ast.Name):
                record(node.target.id, node.value)
        elif isinstance(node, ast.Dict):
            for key_node, value_node in zip(node.keys, node.values):
                if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
                    record(key_node.value, value_node)
    return tuple(sorted(set(found))), False


def _scan_sensitive_content(item: _ArtifactFile, builder: _AuditBuilder) -> None:
    """
    扫描私钥、真实凭据字面量、TerminalInfo 值及华鑫 SDK/build 绝对路径。

    参数:
        item: 已收集且大小受控的单个文件。
        builder: 接收脱敏发现项的构建器。
    返回:
        无；发现项从不记录匹配到的原始值。
    """

    if item.data is None:
        return
    data = item.data
    lowered_path = item.path.lower()
    file_name = PurePosixPath(lowered_path).name
    suffix = PurePosixPath(lowered_path).suffix
    config_like = suffix in {
        ".cfg",
        ".env",
        ".example",
        ".ini",
        ".json",
        ".toml",
        ".yaml",
        ".yml",
    }
    configuration_candidate = config_like or file_name.startswith(".env")
    huaxin_scoped = (
        "bullet_trade/integrations/huaxin/" in lowered_path
        or "huaxin" in file_name
        or "tora" in file_name
    )
    begin = b"-----BEGIN "
    end = b" KEY-----"
    private_key_pattern = re.compile(begin + rb"(?:[A-Z0-9]+ )*PRIVATE" + end)
    private_key_found = bool(private_key_pattern.search(data))
    if private_key_found:
        builder.add("PRIVATE_KEY_MATERIAL", "文件包含私钥材料标记", item.path)

    text, decode_failed = _decode_sensitive_text(data)
    if decode_failed and configuration_candidate:
        builder.add(
            "SENSITIVE_TEXT_DECODE_FAILED",
            "敏感配置文本需要截断或替代解码，无法证明可安全发布",
            item.path,
        )
    if not private_key_found and re.search(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----", text):
        builder.add("PRIVATE_KEY_MATERIAL", "文件包含私钥材料标记", item.path)
    literal_pattern = re.compile(
        r"(?i)(?P<key>password|passwd|secret|api[_-]?key|access[_-]?token|"
        r"dynamic[_-]?(?:password|token|code)|terminal[_-]?info|terminalinfo|"
        r"(?:huaxin|tora)[_-]?(?:account(?:[_-]?id)?|user(?:[_-]?id)?|"
        r"broker(?:[_-]?id)?|trade[_-]?front|md[_-]?front|front|address|host|"
        r"server|endpoint))"
        r"[\"']?\s*[:=]\s*(?P<quote>[\"'])(?P<value>[^\r\n\"']{1,512})(?P=quote)"
    )
    config_pattern = re.compile(
        r"(?im)^\s*(?:export\s+)?(?P<key>(?:PASSWORD|PASSWD|SECRET|API_KEY|"
        r"ACCESS_TOKEN|DYNAMIC_PASSWORD|DYNAMIC_TOKEN|DYNAMIC_CODE|TERMINAL_INFO|"
        r"TERMINALINFO)|(?:(?:HUAXIN|TORA)[_-]?(?:PASSWORD|PASSWD|SECRET|"
        r"API_KEY|ACCESS_TOKEN|DYNAMIC_PASSWORD|DYNAMIC_TOKEN|DYNAMIC_CODE|"
        r"TERMINAL_INFO|TERMINALINFO|ACCOUNT|ACCOUNT_ID|USER|USER_ID|BROKER|"
        r"BROKER_ID|TRADE_FRONT|MD_FRONT|FRONT|ADDRESS|HOST|SERVER|ENDPOINT)))"
        r"\s*[:=]\s*(?P<value>[^\r\n#]{1,512})\s*(?:#.*)?$"
    )
    generic_huaxin_config_pattern = re.compile(
        r"(?im)^\s*[\"']?(?P<key>account(?:[_-]?id)?|investor[_-]?id|user[_-]?id|"
        r"broker[_-]?id|trade[_-]?front|md[_-]?front|front|endpoint|address|host|"
        r"server|terminal[_-]?info|terminalinfo)[\"']?\s*[:=]\s*"
        r"(?P<value>[^\r\n#]{1,512})\s*(?:#.*)?$"
    )
    vendor_scope_pattern = re.compile(
        r"(?im)^\s*[\"']?(?:vendor|provider)[\"']?\s*[:=]\s*"
        r"[\"']?(?:huaxin|tora)[\"']?\s*(?:[,#].*)?$"
    )
    credential_classes: List[str] = []
    patterns = []
    if configuration_candidate:
        patterns.append(literal_pattern)
        patterns.append(config_pattern)
    if config_like and vendor_scope_pattern.search(text):
        huaxin_scoped = True
    if config_like and huaxin_scoped:
        patterns.append(generic_huaxin_config_pattern)
    if suffix == ".json":
        json_classes, json_failed = _json_sensitive_key_classes(text, huaxin_scoped)
        credential_classes.extend(json_classes)
        if json_failed:
            builder.add(
                "SENSITIVE_SCAN_PARSE_FAILED",
                "JSON 敏感配置无法在受控深度内结构化解析",
                item.path,
            )
    if suffix == ".py" and huaxin_scoped and PurePosixPath(lowered_path).name == "config.py":
        python_classes, python_failed = _python_sensitive_key_classes(text)
        credential_classes.extend(python_classes)
        if python_failed:
            builder.add(
                "SENSITIVE_SCAN_PARSE_FAILED",
                "Huaxin Python 配置无法由 AST 安全解析",
                item.path,
            )
    for pattern in patterns:
        for match in pattern.finditer(text):
            value = match.group("value")
            if not _is_placeholder_secret(value):
                category = _sensitive_key_class(match.group("key"), huaxin_scoped)
                if category is not None:
                    credential_classes.append(category)
    if credential_classes:
        builder.add(
            "SENSITIVE_LITERAL",
            "文件包含非占位的敏感配置或 TerminalInfo 字面量",
            item.path,
            {"key_classes": sorted(set(credential_classes))},
        )

    token_patterns = (
        re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        re.compile(r"\bgh[oprsu]_[A-Za-z0-9]{30,}\b"),
        re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
        re.compile(r"\bsk-[A-Za-z0-9]{24,}\b"),
    )
    if any(pattern.search(text) for pattern in token_patterns):
        builder.add("KNOWN_SECRET_FORMAT", "文件包含高置信度访问令牌格式", item.path)

    unix_path_pattern = re.compile(
        r"(?<![A-Za-z0-9_])/(?:Users|home|root|srv|private|tmp|var|opt|usr/local|mnt|data)/"
        r"[^\s\"'<>\x00]{2,512}"
    )
    windows_path_pattern = re.compile(r"(?i)\b[A-Z]:[\\/][^\r\n\"'<>\x00]{2,512}")
    unc_path_pattern = re.compile(r"(?i)(?<!\\)\\\\[^\\/\s\"'<>\x00]+[\\/][^\r\n\"'<>\x00]{2,512}")
    path_markers = (
        "tora",
        "huaxin-sdk",
        "huaxin_sdk",
        "huaxinsdk",
        "huaxin/sdk",
        "huaxin\\sdk",
        "huaxin-build",
        "huaxin_build",
        "cmake-build",
        "tora/sdk",
        "tora\\sdk",
    )
    leaked_path = False
    for pattern in (unix_path_pattern, windows_path_pattern, unc_path_pattern):
        for match in pattern.finditer(text):
            normalized = match.group(0).lower()
            if any(marker in normalized for marker in path_markers):
                leaked_path = True
                break
        if leaked_path:
            break
    if leaked_path:
        builder.add(
            "ABSOLUTE_SDK_BUILD_PATH",
            "文件包含华鑫/TORA SDK 或构建目录的绝对路径",
            item.path,
        )


def _scan_vendor_header(item: _ArtifactFile, builder: _AuditBuilder) -> None:
    """
    识别误收录的典型 TORA 厂商头文件签名，同时允许自研 flat C ABI 头文件。

    参数:
        item: 待扫描文件。
        builder: 接收脱敏发现项的构建器。
    返回:
        无。
    """

    if item.data is None or PurePosixPath(item.path.lower()).suffix not in {".h", ".hpp"}:
        return
    signatures = (
        b"C" + b"TORATstp",
        b"TORA" + b"LEV1API",
        b"TORA" + b"LEV2API",
        b"TORATstp" + b"TraderApi",
    )
    if any(signature in item.data for signature in signatures):
        builder.add(
            "PROPRIETARY_HEADER_SUSPECTED",
            "头文件包含典型厂商 API 声明签名",
            item.path,
        )


def _scan_files(
    files: Sequence[_ArtifactFile],
    builder: _AuditBuilder,
    public_distribution: bool,
    platform_wheel: bool = False,
    bundle: bool = False,
) -> List[Tuple[_ArtifactFile, str]]:
    """
    对文件清单执行扩展名、魔数、敏感内容和第一方 native 边界检查。

    参数:
        files: 已收集的文件序列。
        builder: 接收发现项的构建器。
        public_distribution: 是否按 Git/universal/sdist 的无二进制规则检查。
        platform_wheel: 是否允许正确路径下的自研 bridge native。
        bundle: 是否按显式 build bundle 的严格文件类型检查。
    返回:
        实际识别出的 ``(文件, native 格式)`` 序列。
    """

    native_files: List[Tuple[_ArtifactFile, str]] = []
    for item in files:
        suffix = PurePosixPath(item.path.lower()).suffix
        if public_distribution and suffix in _PUBLIC_FORBIDDEN_SUFFIXES:
            builder.add(
                "FORBIDDEN_FILE_EXTENSION",
                "公开源码或 universal/sdist 包含禁止扩展名",
                item.path,
                {"extension": suffix},
            )
        if bundle and suffix in _BUNDLE_FORBIDDEN_SUFFIXES:
            builder.add(
                "FORBIDDEN_BUNDLE_ASSET",
                "显式 build bundle 包含源码、头文件、文档或静态库",
                item.path,
                {"extension": suffix},
            )
        if item.data is not None:
            magic = _forbidden_magic(item.data)
            native = _native_format(item.data)
            if native is not None:
                native_files.append((item, native))
                if public_distribution:
                    builder.add(
                        "FORBIDDEN_NATIVE_MAGIC",
                        "公开源码或 universal/sdist 包含原生二进制魔数",
                        item.path,
                        {"format": native},
                    )
                elif platform_wheel or bundle:
                    if not _is_self_bridge_path(item.path):
                        builder.add(
                            "NON_SELF_BRIDGE_NATIVE",
                            "platform wheel/bundle 只允许第一方 Huaxin bridge native",
                            item.path,
                            {"format": native},
                        )
            elif suffix in _NATIVE_SUFFIXES:
                builder.add(
                    "NATIVE_EXTENSION_MAGIC_MISMATCH",
                    "原生扩展名文件没有受支持的原生魔数",
                    item.path,
                    {"extension": suffix},
                )
            elif magic is not None:
                builder.add(
                    "FORBIDDEN_FILE_MAGIC",
                    "文件内容魔数属于禁止的归档、文档或静态库",
                    item.path,
                    {"format": magic},
                )
        _scan_sensitive_content(item, builder)
        _scan_vendor_header(item, builder)
    return native_files


def _logical_report_path(
    value: str,
    private_prefix: Optional[str] = None,
    logical_prefix: Optional[str] = None,
) -> str:
    """
    将归档身份根替换为固定逻辑根，再执行通用敏感路径摘要。

    参数:
        value: 归档内原始相对路径。
        private_prefix: 可选的版本化 dist-info 或 sdist 根目录。
        logical_prefix: 对应的不含版本逻辑根。
    返回:
        不含原始版本身份且经过敏感语义检查的报告路径。
    """

    candidate = value
    if private_prefix and logical_prefix:
        if value == private_prefix:
            candidate = logical_prefix
        elif value.startswith(private_prefix + "/"):
            candidate = logical_prefix + value[len(private_prefix) :]
    return _redact_report_path(candidate)


def _logical_findings(
    findings: Sequence[AuditFinding],
    private_prefix: Optional[str] = None,
    logical_prefix: Optional[str] = None,
) -> Tuple[AuditFinding, ...]:
    """
    将发现项中的版本化身份根替换为固定逻辑根而不改变规则证据。

    参数:
        findings: 构建器已经脱敏的发现项。
        private_prefix: 可选的版本化 dist-info 或 sdist 根目录。
        logical_prefix: 对应的不含版本逻辑根。
    返回:
        路径经固定根映射后的不可变发现项序列。
    """

    normalized: List[AuditFinding] = []
    for finding in findings:
        path = finding.path
        if path is not None:
            path = _logical_report_path(path, private_prefix, logical_prefix)
        normalized.append(
            AuditFinding(
                code=finding.code,
                message=finding.message,
                path=path,
                severity=finding.severity,
                details=finding.details,
            )
        )
    return tuple(normalized)


def _sbom(
    files: Sequence[_ArtifactFile],
    private_prefix: Optional[str] = None,
    logical_prefix: Optional[str] = None,
) -> Tuple[Mapping[str, Any], ...]:
    """
    生成只含相对路径、大小、哈希和文件分类的脱敏 SBOM。

    参数:
        files: 已收集的制品文件。
        private_prefix: 可选的版本化 dist-info 或 sdist 根目录。
        logical_prefix: 对应的不含版本逻辑根。
    返回:
        按路径排序的不可变 SBOM 条目序列。
    """

    entries: List[Mapping[str, Any]] = []
    for item in sorted(files, key=lambda value: value.path):
        suffix = PurePosixPath(item.path.lower()).suffix
        native = _native_format(item.data or b"")
        if native is not None:
            classification = "native"
        elif suffix in {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp"}:
            classification = "first_party_native_source"
        elif ".dist-info/" in item.path or ".egg-info/" in item.path:
            classification = "distribution_metadata"
        elif suffix == ".py":
            classification = "python_source"
        else:
            classification = "data"
        entries.append(
            {
                "path": _logical_report_path(item.path, private_prefix, logical_prefix),
                "size": item.size,
                "sha256": item.sha256,
                "classification": classification,
            }
        )
    return tuple(entries)


def _inventory_digest(files: Sequence[_ArtifactFile]) -> str:
    """
    计算目录/Git tree 清单的确定性组合 SHA-256。

    参数:
        files: 已收集的文件序列。
    返回:
        基于相对路径、大小和单文件哈希的组合摘要。
    """

    inventory = [
        {"path": item.path, "size": item.size, "sha256": item.sha256}
        for item in sorted(files, key=lambda value: value.path)
    ]
    encoded = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(encoded)


def _read_filesystem_file(
    path: Path,
    relative_path: str,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
    strict_bundle: bool = False,
    retain_source_path: bool = True,
) -> _ArtifactFile:
    """
    用单个只读文件描述符快照普通文件，并在前后 fstat 间验证稳定性。

    参数:
        path: 文件系统中的真实路径。
        relative_path: 报告使用的相对路径。
        policy: 单文件读取上限。
        builder: 接收读取或大小失败。
        strict_bundle: 是否额外要求当前用户所有、单硬链接且组/其他不可写。
        retain_source_path: 是否保留原路径供非 bundle 调用方使用。
    返回:
        可能因超限而不含 data 的文件记录。
    """

    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    source_path = path if retain_source_path else None
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        builder.add(
            "FILE_READ_FAILED",
            "制品文件无法通过不跟随链接的只读描述符打开",
            relative_path,
            {"error_type": type(exc).__name__},
        )
        return _ArtifactFile(relative_path, 0, None, None, source_path)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            builder.add("SPECIAL_BUNDLE_NODE", "制品路径不是普通文件", relative_path)
            return _ArtifactFile(relative_path, before.st_size, None, None, source_path)
        if strict_bundle:
            if os.name == "posix" and hasattr(os, "geteuid") and before.st_uid != os.geteuid():
                builder.add(
                    "BUNDLE_FILE_OWNER_INVALID",
                    "bundle 普通文件必须由当前审计用户拥有",
                    relative_path,
                )
            if before.st_nlink != 1:
                builder.add(
                    "BUNDLE_FILE_LINK_COUNT_INVALID",
                    "bundle 普通文件必须只有一个硬链接",
                    relative_path,
                    {"link_count": before.st_nlink},
                )
            if os.name == "posix" and stat.S_IMODE(before.st_mode) & 0o022:
                builder.add(
                    "BUNDLE_FILE_PERMISSIONS_UNSAFE",
                    "bundle 普通文件不得允许组或其他用户写入",
                    relative_path,
                )
        if before.st_size > policy.max_file_bytes:
            builder.add(
                "FILE_SIZE_LIMIT",
                "单文件超过发布审计读取硬上限",
                relative_path,
                {"size": before.st_size, "max_bytes": policy.max_file_bytes},
            )
            return _ArtifactFile(relative_path, before.st_size, None, None, source_path)
        chunks: List[bytes] = []
        remaining = policy.max_file_bytes + 1
        while remaining > 0:
            block = os.read(descriptor, min(MEBIBYTE, remaining))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            getattr(before, "st_mtime_ns", int(before.st_mtime * 1_000_000_000)),
            getattr(before, "st_ctime_ns", int(before.st_ctime * 1_000_000_000)),
            before.st_mode,
            before.st_uid,
            before.st_nlink,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            getattr(after, "st_mtime_ns", int(after.st_mtime * 1_000_000_000)),
            getattr(after, "st_ctime_ns", int(after.st_ctime * 1_000_000_000)),
            after.st_mode,
            after.st_uid,
            after.st_nlink,
        )
        if before_identity != after_identity or len(data) != before.st_size:
            builder.add(
                "FILE_CHANGED_DURING_AUDIT",
                "制品文件在同一描述符快照期间发生变化",
                relative_path,
            )
            return _ArtifactFile(relative_path, before.st_size, None, None, source_path)
        return _ArtifactFile(
            relative_path,
            len(data),
            _sha256_bytes(data),
            data,
            source_path,
        )
    except OSError as exc:
        builder.add(
            "FILE_READ_FAILED",
            "制品文件无法完成单描述符稳定读取",
            relative_path,
            {"error_type": type(exc).__name__},
        )
        return _ArtifactFile(relative_path, 0, None, None, source_path)
    finally:
        os.close(descriptor)


def _collect_git_files(
    project_root: Path,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> Tuple[List[_ArtifactFile], Optional[str]]:
    """
    使用 Git 索引列出受跟踪路径，再读取当前工作树内容且不跟随符号链接。

    参数:
        project_root: 含 `.git` 的项目根目录。
        policy: 文件数量和大小策略。
        builder: 接收 Git、路径或文件读取发现。
    返回:
        受跟踪文件记录与可得的 HEAD revision。
    """

    try:
        completed = subprocess.run(
            ["git", "-C", str(project_root), "ls-files", "--stage", "-z"],
            check=False,
            capture_output=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        builder.add(
            "GIT_INVENTORY_FAILED",
            "无法读取 Git 受跟踪文件清单",
            details={"error_type": type(exc).__name__},
        )
        return [], None
    if completed.returncode != 0:
        builder.add(
            "GIT_INVENTORY_FAILED",
            "git ls-files 返回非零状态",
            details={"returncode": completed.returncode},
        )
        return [], None

    files: List[_ArtifactFile] = []
    records = [record for record in completed.stdout.split(b"\x00") if record]
    if len(records) > policy.max_file_count:
        builder.add(
            "FILE_COUNT_LIMIT",
            "Git tree 文件数超过审计硬上限",
            details={"count": len(records), "max_count": policy.max_file_count},
        )
    scanned_bytes = 0
    for record in records[: policy.max_file_count]:
        try:
            prefix, raw_path = record.split(b"\t", 1)
            mode = prefix.split(b" ", 1)[0]
            relative = raw_path.decode("utf-8", errors="strict")
        except (ValueError, UnicodeError) as exc:
            builder.add(
                "GIT_PATH_INVALID",
                "Git 索引包含无法解析的路径记录",
                details={"error_type": type(exc).__name__},
            )
            continue
        if not _safe_relative_path(relative):
            builder.add("PATH_TRAVERSAL", "Git 索引包含不安全路径", relative)
            continue
        path = project_root / Path(*PurePosixPath(relative).parts)
        if mode == b"120000" or path.is_symlink():
            builder.add("SYMLINK_NOT_ALLOWED", "发布 Git tree 不允许受跟踪符号链接", relative)
            continue
        if not path.is_file():
            builder.add("TRACKED_FILE_MISSING", "Git 受跟踪路径不是当前普通文件", relative)
            continue
        try:
            file_size = path.stat().st_size
        except OSError as exc:
            builder.add(
                "FILE_READ_FAILED",
                "Git 受跟踪文件无法读取大小",
                relative,
                {"error_type": type(exc).__name__},
            )
            continue
        if scanned_bytes + file_size > policy.max_unpacked_scan_bytes:
            builder.add(
                "INVENTORY_HARD_LIMIT",
                "Git tree 累计读取量超过审计防 DoS 硬上限",
                details={"max_bytes": policy.max_unpacked_scan_bytes},
            )
            break
        scanned_bytes += file_size
        files.append(_read_filesystem_file(path, relative, policy, builder))

    revision: Optional[str] = None
    revision_result = subprocess.run(
        ["git", "-C", str(project_root), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    candidate = revision_result.stdout.strip()
    if revision_result.returncode == 0 and re.fullmatch(r"[0-9a-fA-F]{40,64}", candidate):
        revision = candidate.lower()
    return files, revision


def _required_huaxin_paths(
    paths: Iterable[str],
    builder: _AuditBuilder,
    root_prefix: Optional[str] = None,
) -> None:
    """
    验证同一 distribution 包含第一方 wrapper 与自研 bridge 源码边界。

    参数:
        paths: 制品内部路径。
        builder: 接收缺失路径发现。
        root_prefix: sdist 的单一顶层目录；wheel/Git tree 为 None。
    返回:
        无。
    """

    normalized = set(paths)
    if root_prefix is not None:
        prefix = root_prefix.rstrip("/") + "/"
        normalized = {
            path[len(prefix) :]
            for path in normalized
            if path.startswith(prefix) and len(path) > len(prefix)
        }
    for required in sorted(_REQUIRED_HUAXIN_PATHS - normalized):
        builder.add(
            "HUAXIN_FIRST_PARTY_SOURCE_MISSING",
            "同一 bullet-trade 制品缺少第一方 Huaxin 源码边界文件",
            required,
        )


def audit_git_tree(
    project_root: Path,
    policy: Optional[ReleaseAuditPolicy] = None,
) -> ReleaseAuditReport:
    """
    审计当前工作树中所有 Git 受跟踪文件，不纳入未跟踪临时文件。

    参数:
        project_root: bullet-trade Git 根目录。
        policy: 可选审计门槛；缺省使用发布安全默认值。
    返回:
        Git tree 的脱敏审计报告。
    副作用:
        只执行只读 Git 命令并读取受跟踪文件。
    """

    effective_policy = policy or ReleaseAuditPolicy()
    builder = _AuditBuilder()
    root = Path(project_root).expanduser().resolve()
    files, revision = _collect_git_files(root, effective_policy, builder)
    _validate_inventory_paths(files, builder)
    _scan_files(files, builder, public_distribution=True)
    _required_huaxin_paths((item.path for item in files), builder)

    pyproject = next((item for item in files if item.path == "pyproject.toml"), None)
    if pyproject is not None and pyproject.data is not None:
        text = pyproject.data.decode("utf-8", errors="ignore")
        if not re.search(r"(?m)^name\s*=\s*[\"']bullet-trade[\"']\s*$", text):
            builder.add(
                "DISTRIBUTION_NAME_INVALID",
                "pyproject 未声明唯一 bullet-trade distribution",
                "pyproject.toml",
            )
        if re.search(r"(?m)^huaxin\s*=\s*\[", text):
            builder.add(
                "INDEPENDENT_HUAXIN_EXTRA_FORBIDDEN",
                "项目不得用不存在的 huaxin extra 表示 native 已可用",
                "pyproject.toml",
            )

    unpacked = sum(item.size for item in files)
    metadata: Dict[str, Any] = {"tracked_only": True, "revision": revision}
    return ReleaseAuditReport(
        artifact_kind="git_tree",
        artifact_name="bullet-trade-git-tree",
        artifact_sha256=_inventory_digest(files),
        archive_size=None,
        unpacked_size=unpacked,
        file_count=len(files),
        metadata=metadata,
        sbom=_sbom(files),
        native_inspection=tuple(),
        findings=tuple(builder.findings),
    )


def _audit_zip_envelope(
    data: bytes,
    archive: zipfile.ZipFile,
    builder: _AuditBuilder,
) -> None:
    """
    校验 wheel ZIP 的原始包络，拒绝前后缀、注释、extra 与隐藏目录数据。

    参数:
        data: 与归档哈希相同的外层 wheel 完整字节快照。
        archive: 已从该快照打开的 ZipFile。
        builder: 接收固定规则码和计数，不记录原始注释或 extra 内容。
    返回:
        无；任何结构歧义均追加 fail-closed 发现。
    """

    signature = b"PK\x05\x06"
    minimum_offset = max(0, len(data) - (22 + 65_535))
    eocd_offset = data.rfind(signature, minimum_offset)
    if eocd_offset < 0 or eocd_offset + 22 > len(data):
        builder.add("ZIP_ENVELOPE_INVALID", "wheel ZIP 缺少唯一可验证的 EOCD 包络")
        return
    try:
        (
            _marker,
            disk_number,
            central_disk,
            disk_entries,
            total_entries,
            central_size,
            central_offset,
            comment_length,
        ) = struct.unpack_from("<4s4H2LH", data, eocd_offset)
    except struct.error:
        builder.add("ZIP_ENVELOPE_INVALID", "wheel ZIP EOCD 长度无效")
        return
    expected_end = eocd_offset + 22 + comment_length
    if comment_length:
        builder.add("ZIP_ARCHIVE_COMMENT_FORBIDDEN", "wheel ZIP 不允许 archive comment")
    if expected_end != len(data):
        builder.add("ZIP_TRAILING_DATA_FORBIDDEN", "wheel ZIP EOCD 后存在未签名尾随数据")
    if any((disk_number, central_disk)) or disk_entries != total_entries:
        builder.add("ZIP_MULTIDISK_FORBIDDEN", "wheel ZIP 不允许多磁盘或分片结构")
    if 0xFFFF in (disk_entries, total_entries) or 0xFFFFFFFF in (
        central_size,
        central_offset,
    ):
        builder.add("ZIP64_ENVELOPE_FORBIDDEN", "wheel ZIP64 包络未纳入当前发布合同")
        return
    physical_central_offset = eocd_offset - central_size
    if physical_central_offset < 0 or central_offset != physical_central_offset:
        builder.add("ZIP_PREFIX_OR_GAP_FORBIDDEN", "wheel ZIP 不允许前缀或中央目录前隐藏间隙")

    infos = sorted(archive.infolist(), key=lambda item: item.header_offset)
    entry_comment_count = 0
    central_extra_count = 0
    local_extra_count = 0
    directory_data_count = 0
    descriptor_count = 0
    layout_error_count = 0
    cursor = 0
    for info in infos:
        if info.comment:
            entry_comment_count += 1
        if info.extra:
            central_extra_count += 1
        unix_mode = (info.external_attr >> 16) & 0xFFFF
        directory_entry = info.is_dir() or stat.S_IFMT(unix_mode) == stat.S_IFDIR
        if directory_entry and info.file_size != 0:
            directory_data_count += 1
        offset = info.header_offset
        if offset != cursor or offset < 0 or offset + 30 > len(data):
            layout_error_count += 1
            continue
        try:
            (
                local_signature,
                _version_needed,
                flags,
                _compression,
                _modification_time,
                _modification_date,
                _crc32,
                compressed_size,
                uncompressed_size,
                filename_length,
                extra_length,
            ) = struct.unpack_from("<4s5H3L2H", data, offset)
        except struct.error:
            layout_error_count += 1
            continue
        if local_signature != b"PK\x03\x04":
            layout_error_count += 1
            continue
        if extra_length:
            local_extra_count += 1
        if flags & 0x08:
            descriptor_count += 1
        payload_offset = offset + 30 + filename_length + extra_length
        payload_size = info.compress_size
        if flags & 0x08 or compressed_size == 0xFFFFFFFF or uncompressed_size == 0xFFFFFFFF:
            layout_error_count += 1
            continue
        if compressed_size != info.compress_size or uncompressed_size != info.file_size:
            layout_error_count += 1
        cursor = payload_offset + payload_size
        if cursor > len(data):
            layout_error_count += 1
    if infos and infos[0].header_offset != 0:
        layout_error_count += 1
    if cursor != physical_central_offset:
        layout_error_count += 1
    if entry_comment_count:
        builder.add(
            "ZIP_ENTRY_COMMENT_FORBIDDEN",
            "wheel ZIP 条目不允许 comment",
            details={"count": entry_comment_count},
        )
    if central_extra_count or local_extra_count:
        builder.add(
            "ZIP_ENTRY_EXTRA_FORBIDDEN",
            "wheel ZIP 条目不允许 central/local extra 字段",
            details={
                "central_count": central_extra_count,
                "local_count": local_extra_count,
            },
        )
    if directory_data_count:
        builder.add(
            "ARCHIVE_DIRECTORY_DATA_FORBIDDEN",
            "wheel ZIP 目录条目不允许携带非零数据",
            details={"count": directory_data_count},
        )
    if descriptor_count:
        builder.add(
            "ZIP_DATA_DESCRIPTOR_FORBIDDEN",
            "wheel ZIP 不允许产生边界歧义的数据描述符",
            details={"count": descriptor_count},
        )
    if layout_error_count:
        builder.add(
            "ZIP_LOCAL_LAYOUT_INVALID",
            "wheel ZIP 本地条目必须从字节零开始连续覆盖到中央目录",
            details={"count": layout_error_count},
        )


def _collect_zip_files(
    archive: zipfile.ZipFile,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> List[_ArtifactFile]:
    """
    安全读取 wheel ZIP 条目，不解包到文件系统且拒绝链接和特殊路径。

    参数:
        archive: 已打开的 wheel ZIP。
        policy: 文件数量和大小硬上限。
        builder: 接收归档结构发现。
    返回:
        普通文件记录序列。
    """

    infos = archive.infolist()
    if len(infos) > policy.max_file_count:
        builder.add(
            "FILE_COUNT_LIMIT",
            "wheel 文件数超过审计硬上限",
            details={"count": len(infos), "max_count": policy.max_file_count},
        )
    declared_total = sum(info.file_size for info in infos if not info.is_dir())
    if declared_total > policy.max_unpacked_scan_bytes:
        builder.add(
            "ARCHIVE_UNPACKED_HARD_LIMIT",
            "wheel 声明解包总量超过审计防 DoS 硬上限，拒绝读取条目",
            details={"size": declared_total, "max_bytes": policy.max_unpacked_scan_bytes},
        )
        return []
    files: List[_ArtifactFile] = []
    for info in infos[: policy.max_file_count]:
        path = info.filename.rstrip("/") if info.is_dir() else info.filename
        unix_mode = (info.external_attr >> 16) & 0xFFFF
        file_type = stat.S_IFMT(unix_mode)
        if file_type == stat.S_IFLNK:
            builder.add("SYMLINK_NOT_ALLOWED", "wheel 不允许符号链接条目", path)
            continue
        if file_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
            builder.add("SPECIAL_ARCHIVE_MEMBER", "wheel 不允许设备或其他特殊条目", path)
            continue
        if info.is_dir() or file_type == stat.S_IFDIR:
            if not path or not _safe_relative_path(path):
                builder.add("PATH_TRAVERSAL", "wheel 目录包含不安全内部路径", path)
            continue
        if not _safe_relative_path(path):
            builder.add("PATH_TRAVERSAL", "wheel 包含不安全内部路径", path)
        if info.file_size > policy.max_file_bytes:
            builder.add(
                "FILE_SIZE_LIMIT",
                "wheel 单文件超过读取硬上限",
                path,
                {"size": info.file_size, "max_bytes": policy.max_file_bytes},
            )
            files.append(_ArtifactFile(path, info.file_size, None, None))
            continue
        try:
            data = archive.read(info)
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            builder.add(
                "ARCHIVE_READ_FAILED",
                "wheel 条目无法读取或校验",
                path,
                {"error_type": type(exc).__name__},
            )
            files.append(_ArtifactFile(path, info.file_size, None, None))
            continue
        if len(data) != info.file_size:
            builder.add("ARCHIVE_SIZE_MISMATCH", "wheel 条目实际大小与目录记录不一致", path)
        files.append(_ArtifactFile(path, len(data), _sha256_bytes(data), data))
    return files


def _parse_wheel_filename(path: Path, builder: _AuditBuilder) -> Mapping[str, Any]:
    """
    从 wheel 文件名右侧解析 Python、ABI 和平台标签。

    参数:
        path: wheel 文件路径。
        builder: 接收文件名格式发现。
    返回:
        标签与 universal/platform 分类字典。
    """

    name = path.name
    if not name.endswith(".whl"):
        builder.add("WHEEL_FILENAME_INVALID", "wheel 文件扩展名不正确")
        return {"python_tag": None, "abi_tag": None, "platform_tag": None, "universal": False}
    match = re.fullmatch(
        r"(?P<distribution>[A-Za-z0-9_]+)-(?P<version>[0-9][A-Za-z0-9_.!+]*)"
        r"(?:-(?P<build>[0-9][A-Za-z0-9_.]*))?-(?P<python>[A-Za-z0-9_.]+)-"
        r"(?P<abi>[A-Za-z0-9_.]+)-(?P<platform>[A-Za-z0-9_.]+)\.whl",
        name,
    )
    if match is None:
        builder.add("WHEEL_FILENAME_INVALID", "wheel 文件名无法解析三元标签")
        return {"python_tag": None, "abi_tag": None, "platform_tag": None, "universal": False}
    distribution = match.group("distribution")
    version = match.group("version")
    python_tag = match.group("python")
    abi_tag = match.group("abi")
    platform_tag = match.group("platform")
    normalized_distribution = _canonical_distribution_name(distribution, builder)
    normalized_version = _canonical_version(version, builder)
    parsed_by_packaging = False
    if _PACKAGING_UTILS is not None:
        try:
            parsed_name, parsed_version, _build, _tags = _PACKAGING_UTILS.parse_wheel_filename(name)
            parsed_by_packaging = (
                str(parsed_name) == normalized_distribution
                and str(parsed_version) == normalized_version
            )
        except (AttributeError, TypeError, ValueError):
            parsed_by_packaging = False
    if not parsed_by_packaging:
        builder.add("WHEEL_FILENAME_INVALID", "wheel 文件名不满足 PEP 427/PEP 440 解析合同")
    if normalized_distribution != "bullet-trade":
        builder.add("WHEEL_DISTRIBUTION_INVALID", "wheel 文件名必须属于 bullet_trade")
    if normalized_version is None:
        builder.add("WHEEL_VERSION_INVALID", "wheel 文件名版本不是有效 PEP 440 版本")
    trusted_filename_identity = (
        parsed_by_packaging
        and normalized_distribution == "bullet-trade"
        and normalized_version is not None
    )
    return {
        "filename_distribution": (
            "bullet-trade" if normalized_distribution == "bullet-trade" else None
        ),
        "filename_version": normalized_version,
        "dist_info_directory": (
            "{}-{}.dist-info".format(distribution, version) if trusted_filename_identity else None
        ),
        "python_tag": python_tag,
        "abi_tag": abi_tag,
        "platform_tag": platform_tag,
        "universal": (python_tag, abi_tag, platform_tag) == ("py3", "none", "any"),
    }


def _parse_metadata_headers(data: bytes) -> Mapping[str, Tuple[str, ...]]:
    """
    解析 wheel METADATA/WHEEL 的简单 RFC822 多值头字段。

    参数:
        data: UTF-8 兼容元数据字节。
    返回:
        小写字段名到值元组的映射。
    """

    try:
        message = BytesParser(policy=email.policy.compat32).parsebytes(data, headersonly=True)
    except (TypeError, ValueError):
        return {}
    values: Dict[str, List[str]] = {}
    for key in message.keys():
        normalized_key = str(key).strip().lower()
        items = message.get_all(key, [])
        values[normalized_key] = [" ".join(str(item).split()) for item in items]
    return {key: tuple(items) for key, items in values.items()}


def _expanded_wheel_filename_tags(filename_tags: Mapping[str, Any]) -> Tuple[str, ...]:
    """
    按 wheel 压缩标签规则展开文件名中的 Python、ABI 和平台笛卡尔积。

    参数:
        filename_tags: `_parse_wheel_filename` 的标签映射。
    返回:
        排序后的完整 Tag 集；任一标签缺失或非法时返回空元组。
    """

    values = (
        filename_tags.get("python_tag"),
        filename_tags.get("abi_tag"),
        filename_tags.get("platform_tag"),
    )
    if not all(isinstance(value, str) and value for value in values):
        return tuple()
    python_value = filename_tags.get("python_tag")
    abi_value = filename_tags.get("abi_tag")
    platform_value = filename_tags.get("platform_tag")
    if (
        not isinstance(python_value, str)
        or not isinstance(abi_value, str)
        or not isinstance(platform_value, str)
    ):
        return tuple()
    python_tags = python_value.split(".")
    abi_tags = abi_value.split(".")
    platform_tags = platform_value.split(".")
    return tuple(
        sorted(
            "{}-{}-{}".format(python_tag, abi_tag, platform_tag)
            for python_tag in python_tags
            for abi_tag in abi_tags
            for platform_tag in platform_tags
        )
    )


def _audit_wheel_metadata(
    files: Sequence[_ArtifactFile],
    filename_tags: Mapping[str, Any],
    builder: _AuditBuilder,
) -> Mapping[str, Any]:
    """
    校验 WHEEL/METADATA 标签、distribution 名称和禁止的独立 extra。

    参数:
        files: wheel 普通文件。
        filename_tags: 从文件名解析的标签。
        builder: 接收元数据发现。
    返回:
        可安全写入报告的 wheel 元数据摘要。
    """

    wheel_files = [item for item in files if item.path.endswith(".dist-info/WHEEL")]
    metadata_files = [item for item in files if item.path.endswith(".dist-info/METADATA")]
    expected_directory = filename_tags.get("dist_info_directory")
    expected_wheel = "{}/WHEEL".format(expected_directory)
    expected_metadata = "{}/METADATA".format(expected_directory)
    dist_info_roots = set()
    invalid_dist_info_paths = 0
    for item in files:
        parts = PurePosixPath(item.path).parts
        roots = [part for part in parts if part.casefold().endswith(".dist-info")]
        if roots:
            if len(roots) != 1 or not parts or parts[0] != roots[0]:
                invalid_dist_info_paths += 1
            dist_info_roots.add(roots[0])
    expected_root_set = {expected_directory} if isinstance(expected_directory, str) else set()
    if dist_info_roots != expected_root_set or invalid_dist_info_paths:
        builder.add(
            "WHEEL_DIST_INFO_BOUNDARY_INVALID",
            "wheel 必须只包含文件名绑定的唯一顶层 dist-info 身份边界",
            details={
                "root_count": len(dist_info_roots),
                "invalid_path_count": invalid_dist_info_paths,
            },
        )
    if (
        len(wheel_files) != 1
        or wheel_files[0].path != expected_wheel
        or wheel_files[0].data is None
    ):
        builder.add(
            "WHEEL_METADATA_INVALID",
            "wheel 必须包含唯一可读的 .dist-info/WHEEL",
            details={"count": len(wheel_files)},
        )
        wheel_headers: Mapping[str, Tuple[str, ...]] = {}
    else:
        wheel_headers = _parse_metadata_headers(wheel_files[0].data)
    if (
        len(metadata_files) != 1
        or metadata_files[0].path != expected_metadata
        or metadata_files[0].data is None
    ):
        builder.add(
            "WHEEL_METADATA_INVALID",
            "wheel 必须包含唯一可读的 .dist-info/METADATA",
            details={"count": len(metadata_files)},
        )
        metadata_headers: Mapping[str, Tuple[str, ...]] = {}
    else:
        metadata_headers = _parse_metadata_headers(metadata_files[0].data)

    tags = wheel_headers.get("tag", tuple())
    expected_tags = _expanded_wheel_filename_tags(filename_tags)
    safe_tags = tuple(value if value in _PUBLIC_WHEEL_TAGS else "<invalid-tag>" for value in tags)
    if tuple(sorted(tags)) != expected_tags or len(tags) != len(set(tags)):
        builder.add(
            "WHEEL_TAG_SET_MISMATCH",
            "WHEEL Tag 集必须与文件名压缩标签展开结果完全一致且无重复",
            details={"expected_count": len(expected_tags), "metadata_tags": list(safe_tags)},
        )
    wheel_versions = wheel_headers.get("wheel-version", tuple())
    if wheel_versions != ("1.0",):
        builder.add(
            "WHEEL_SPEC_VERSION_INVALID",
            "WHEEL Wheel-Version 必须唯一等于受支持的 1.0",
            details={"count": len(wheel_versions)},
        )
    names = metadata_headers.get("name", tuple())
    canonical_name = (
        _canonical_distribution_name(names[0], builder, expected_metadata)
        if len(names) == 1
        else None
    )
    if canonical_name != "bullet-trade":
        builder.add(
            "DISTRIBUTION_NAME_INVALID",
            "wheel 必须属于唯一 bullet-trade distribution",
            details={"name_count": len(names)},
        )
    versions = metadata_headers.get("version", tuple())
    canonical_metadata_version = (
        _canonical_version(versions[0], builder, expected_metadata) if len(versions) == 1 else None
    )
    if canonical_metadata_version != filename_tags.get("filename_version"):
        builder.add(
            "WHEEL_VERSION_MISMATCH",
            "wheel METADATA Version 必须与文件名版本一致",
            details={"version_count": len(versions)},
        )
    extras = tuple(
        value.lower().replace("_", "-") for value in metadata_headers.get("provides-extra", tuple())
    )
    safe_extras = tuple(
        value if value in _PUBLIC_EXTRAS else "<unsupported-extra>" for value in extras
    )
    if "huaxin" in extras:
        builder.add(
            "INDEPENDENT_HUAXIN_EXTRA_FORBIDDEN",
            "wheel 不得声明把 native readiness 混同安装入口的 huaxin extra",
        )
    requirements = metadata_headers.get("requires-dist", tuple())
    if _contains_forbidden_huaxin_requirement(requirements, builder, expected_metadata):
        builder.add(
            "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN",
            "wheel 不得依赖独立 bullet-trade-huaxin distribution",
        )
    pure_values = tuple(value.lower() for value in wheel_headers.get("root-is-purelib", tuple()))
    safe_pure_value = (
        pure_values[0] if len(pure_values) == 1 and pure_values[0] in {"true", "false"} else None
    )
    return {
        "metadata_tags": list(safe_tags),
        "root_is_purelib": safe_pure_value,
        "distribution_name": "bullet-trade" if canonical_name == "bullet-trade" else None,
        "distribution_version_matches_filename": (
            canonical_metadata_version is not None
            and canonical_metadata_version == filename_tags.get("filename_version")
        ),
        "provides_extra": list(safe_extras),
    }


def _audit_wheel_record(
    files: Sequence[_ArtifactFile],
    builder: _AuditBuilder,
    expected_directory: Optional[str],
) -> None:
    """
    验证 wheel RECORD 覆盖所有文件且每项 SHA-256/size 自洽。

    参数:
        files: wheel 普通文件记录。
        builder: 接收 RECORD 发现。
        expected_directory: 文件名绑定的唯一 dist-info 目录。
    返回:
        无。
    """

    records = [item for item in files if item.path.endswith(".dist-info/RECORD")]
    if len(records) != 1:
        builder.add(
            "WHEEL_RECORD_INVALID",
            "wheel 必须包含唯一可读 RECORD",
            details={"count": len(records)},
        )
        return
    record_file = records[0]
    expected_record = "{}/RECORD".format(expected_directory)
    signature_paths = {
        expected_record + ".jws",
        expected_record + ".p7s",
    }
    signature_items = [item for item in files if item.path in signature_paths]
    unexpected_signatures = [
        item
        for item in files
        if item.path.endswith((".dist-info/RECORD.jws", ".dist-info/RECORD.p7s"))
        and item.path not in signature_paths
    ]
    if unexpected_signatures:
        builder.add(
            "WHEEL_SIGNATURE_BOUNDARY_INVALID",
            "wheel 签名只能位于文件名绑定的唯一 RECORD 签名边界",
            details={"count": len(unexpected_signatures)},
        )
    if len(signature_items) != len({item.path for item in signature_items}):
        builder.add(
            "WHEEL_SIGNATURE_BOUNDARY_INVALID",
            "wheel 同一 RECORD 签名类型只能出现一次",
        )
    if record_file.path != expected_record:
        builder.add(
            "WHEEL_RECORD_INVALID",
            "RECORD 必须位于文件名绑定的唯一 dist-info 目录",
            record_file.path,
        )
    record_data = record_file.data
    if record_data is None:
        builder.add(
            "WHEEL_RECORD_INVALID",
            "wheel 必须包含唯一可读 RECORD",
            record_file.path,
            {"count": 1},
        )
        return
    actual = {item.path: item for item in files}
    seen: Dict[str, int] = {}
    try:
        rows = list(csv.reader(io.StringIO(record_data.decode("utf-8"))))
    except (UnicodeError, csv.Error) as exc:
        builder.add(
            "WHEEL_RECORD_INVALID",
            "RECORD 不是有效 UTF-8 CSV",
            record_file.path,
            {"error_type": type(exc).__name__},
        )
        return
    for row in rows:
        if len(row) != 3:
            builder.add("WHEEL_RECORD_INVALID", "RECORD 行必须恰含三列", record_file.path)
            continue
        path, digest_field, size_field = row
        if not _safe_relative_path(path):
            builder.add("PATH_TRAVERSAL", "RECORD 包含不安全内部路径", path)
            continue
        seen[path] = seen.get(path, 0) + 1
        item = actual.get(path)
        if item is None:
            builder.add("WHEEL_RECORD_UNKNOWN_PATH", "RECORD 引用了 wheel 中不存在的路径", path)
            continue
        if path in signature_paths:
            builder.add(
                "WHEEL_SIGNATURE_RECORD_INVALID",
                "RECORD.jws/RECORD.p7s 必须位于受信边界且不得列入 RECORD",
                path,
            )
            continue
        if path == record_file.path:
            if digest_field or size_field:
                builder.add("WHEEL_RECORD_INVALID", "RECORD 自身哈希与大小必须为空", path)
            continue
        if item.data is None or item.sha256 is None:
            builder.add("WHEEL_RECORD_UNVERIFIED", "RECORD 条目因文件不可读而无法验证", path)
            continue
        expected_digest = "sha256=" + base64.urlsafe_b64encode(
            hashlib.sha256(item.data).digest()
        ).rstrip(b"=").decode("ascii")
        if digest_field != expected_digest:
            builder.add("WHEEL_RECORD_HASH_MISMATCH", "RECORD SHA-256 与文件内容不一致", path)
        if size_field != str(item.size):
            builder.add("WHEEL_RECORD_SIZE_MISMATCH", "RECORD size 与文件内容不一致", path)
    for path, count in seen.items():
        if count > 1:
            builder.add(
                "WHEEL_RECORD_DUPLICATE_PATH",
                "RECORD 包含重复路径",
                path,
                {"count": count},
            )
    missing = sorted(set(actual) - set(seen) - signature_paths)
    for path in missing:
        builder.add("WHEEL_RECORD_MISSING_PATH", "wheel 文件未被 RECORD 覆盖", path)


def _run_native_command(command: Sequence[str]) -> Tuple[bool, str]:
    """
    运行只读 native 元数据工具并返回受控文本，不使用 shell。

    参数:
        command: 完整参数序列。
    返回:
        ``(是否成功, 合并输出)``；调用方不得直接写入公开报告。
    """

    try:
        with tempfile.TemporaryFile(mode="w+b") as output:
            completed = subprocess.run(
                list(command),
                check=False,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=30,
            )
            output.flush()
            size = output.tell()
            if size > _NATIVE_TOOL_OUTPUT_MAX_BYTES:
                return False, ""
            output.seek(0)
            raw_output = output.read(_NATIVE_TOOL_OUTPUT_MAX_BYTES + 1)
    except (OSError, subprocess.SubprocessError):
        return False, ""
    if completed.returncode != 0 or len(raw_output) > _NATIVE_TOOL_OUTPUT_MAX_BYTES:
        return False, ""
    if b"\x00" in raw_output:
        return False, ""
    try:
        text_output = raw_output.decode("utf-8", errors="strict")
    except UnicodeError:
        return False, ""
    return True, text_output


def _pe_export_layout(
    data: bytes,
) -> Optional[Tuple[int, int, Tuple[Tuple[int, int, int, int, int], ...]]]:
    """
    解析 PE export data-directory 与 section 表，不信任外部工具的符号分类。

    参数:
        data: PE 文件的同代快照字节。
    返回:
        ``(export_rva, export_size, sections)``；头部越界或组合非法时返回 None。
        每个 section 为 ``(virtual_address, virtual_size, raw_offset, raw_size, flags)``。
    """

    if len(data) < 64 or data[:2] != b"MZ":
        return None
    pe_offset = int.from_bytes(data[60:64], "little")
    if pe_offset < 64 or pe_offset + 24 > len(data) or data[pe_offset : pe_offset + 4] != b"PE\0\0":
        return None
    section_count = int.from_bytes(data[pe_offset + 6 : pe_offset + 8], "little")
    optional_size = int.from_bytes(data[pe_offset + 20 : pe_offset + 22], "little")
    optional_offset = pe_offset + 24
    section_offset = optional_offset + optional_size
    if section_count == 0 or section_count > 96 or section_offset + section_count * 40 > len(data):
        return None
    if optional_offset + 2 > len(data):
        return None
    optional_magic = int.from_bytes(data[optional_offset : optional_offset + 2], "little")
    if optional_magic == 0x20B:
        directory_count_offset = optional_offset + 108
        export_directory_offset = optional_offset + 112
    elif optional_magic == 0x10B:
        directory_count_offset = optional_offset + 92
        export_directory_offset = optional_offset + 96
    else:
        return None
    if export_directory_offset + 8 > section_offset:
        return None
    directory_count = int.from_bytes(
        data[directory_count_offset : directory_count_offset + 4], "little"
    )
    export_rva = 0
    export_size = 0
    if directory_count:
        export_rva = int.from_bytes(
            data[export_directory_offset : export_directory_offset + 4], "little"
        )
        export_size = int.from_bytes(
            data[export_directory_offset + 4 : export_directory_offset + 8], "little"
        )
    sections: List[Tuple[int, int, int, int, int]] = []
    virtual_ranges: List[Tuple[int, int]] = []
    raw_ranges: List[Tuple[int, int]] = []
    for index in range(section_count):
        offset = section_offset + index * 40
        virtual_size = int.from_bytes(data[offset + 8 : offset + 12], "little")
        virtual_address = int.from_bytes(data[offset + 12 : offset + 16], "little")
        raw_size = int.from_bytes(data[offset + 16 : offset + 20], "little")
        raw_offset = int.from_bytes(data[offset + 20 : offset + 24], "little")
        flags = int.from_bytes(data[offset + 36 : offset + 40], "little")
        if raw_offset > len(data) or raw_size > len(data) - raw_offset:
            return None
        virtual_span = max(virtual_size, raw_size)
        if virtual_span == 0 or virtual_address + virtual_span > 0x1_0000_0000:
            return None
        virtual_ranges.append((virtual_address, virtual_address + virtual_span))
        if raw_size:
            raw_ranges.append((raw_offset, raw_offset + raw_size))
        sections.append((virtual_address, virtual_size, raw_offset, raw_size, flags))
    for ranges in (virtual_ranges, raw_ranges):
        ordered = sorted(ranges)
        if any(previous[1] > current[0] for previous, current in zip(ordered, ordered[1:])):
            return None
    return export_rva, export_size, tuple(sections)


def _pe_rva_offset(
    rva: int,
    size: int,
    sections: Sequence[Tuple[int, int, int, int, int]],
    data_size: int,
    executable: bool = False,
) -> Optional[int]:
    """
    将 PE RVA 映射到同一文件快照偏移，并可强制目标 section 可执行。

    参数:
        rva: 待映射的相对虚拟地址。
        size: 需要读取的连续字节数。
        sections: `_pe_export_layout` 返回的 section 序列。
        data_size: 完整 PE 快照长度。
        executable: 是否要求 section 设置 IMAGE_SCN_MEM_EXECUTE。
    返回:
        可安全读取的文件偏移；映射越界或 section 类型不符时返回 None。
    """

    if rva < 0 or size < 0:
        return None
    matches: List[int] = []
    for virtual_address, virtual_size, raw_offset, raw_size, flags in sections:
        span = max(virtual_size, raw_size)
        if rva < virtual_address or rva - virtual_address > span:
            continue
        delta = rva - virtual_address
        if size > raw_size or delta > raw_size - size:
            continue
        if executable and flags & 0x20000000 == 0:
            continue
        offset = raw_offset + delta
        if offset > data_size or size > data_size - offset:
            continue
        matches.append(offset)
    return matches[0] if len(matches) == 1 else None


def _read_pe_export_name(
    data: bytes,
    rva: int,
    sections: Sequence[Tuple[int, int, int, int, int]],
) -> Optional[str]:
    """
    从 PE section 映射中读取有界 ASCII NUL 终止导出名。

    参数:
        data: PE 文件的同代快照字节。
        rva: 名称字符串的 RVA。
        sections: 已验证 section 序列。
    返回:
        合法 ASCII 符号名；越界、无 NUL 或编码非法时返回 None。
    """

    offset = _pe_rva_offset(rva, 1, sections, len(data))
    if offset is None:
        return None
    limit = min(len(data), offset + 4096)
    end = data.find(b"\x00", offset, limit)
    if end < 0:
        return None
    try:
        value = data[offset:end].decode("ascii", errors="strict")
    except UnicodeError:
        return None
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        return None
    return value


def _pe_bridge_exports(data: bytes) -> Tuple[Set[str], Set[str], bool]:
    """
    直接从 PE export table 验证目标名称指向非 forwarder 的可执行 section RVA。

    参数:
        data: PE 文件的同代快照字节。
    返回:
        ``(强函数导出, 同名非法导出, 结构是否可验证)``。
    """

    layout = _pe_export_layout(data)
    if layout is None:
        return set(), set(), False
    export_rva, export_size, sections = layout
    if export_rva == 0 or export_size < 40:
        return set(), set(), True
    directory_offset = _pe_rva_offset(export_rva, 40, sections, len(data))
    if directory_offset is None:
        return set(), set(), False
    function_count = int.from_bytes(data[directory_offset + 20 : directory_offset + 24], "little")
    name_count = int.from_bytes(data[directory_offset + 24 : directory_offset + 28], "little")
    functions_rva = int.from_bytes(data[directory_offset + 28 : directory_offset + 32], "little")
    names_rva = int.from_bytes(data[directory_offset + 32 : directory_offset + 36], "little")
    ordinals_rva = int.from_bytes(data[directory_offset + 36 : directory_offset + 40], "little")
    if function_count > 100_000 or name_count > 100_000 or name_count > function_count:
        return set(), set(), False
    functions_offset = _pe_rva_offset(functions_rva, function_count * 4, sections, len(data))
    names_offset = _pe_rva_offset(names_rva, name_count * 4, sections, len(data))
    ordinals_offset = _pe_rva_offset(ordinals_rva, name_count * 2, sections, len(data))
    if None in {functions_offset, names_offset, ordinals_offset}:
        return set(), set(), False
    assert functions_offset is not None
    assert names_offset is not None
    assert ordinals_offset is not None
    expected = {symbol.decode("ascii") for symbol in _EXPECTED_BRIDGE_SYMBOLS}
    exported: Set[str] = set()
    invalid: Set[str] = set()
    for index in range(name_count):
        name_rva = int.from_bytes(
            data[names_offset + index * 4 : names_offset + index * 4 + 4], "little"
        )
        name = _read_pe_export_name(data, name_rva, sections)
        if name is None:
            return set(), set(), False
        ordinal = int.from_bytes(
            data[ordinals_offset + index * 2 : ordinals_offset + index * 2 + 2], "little"
        )
        if ordinal >= function_count:
            return set(), set(), False
        function_rva = int.from_bytes(
            data[functions_offset + ordinal * 4 : functions_offset + ordinal * 4 + 4],
            "little",
        )
        if name not in expected:
            continue
        is_forwarder = export_rva <= function_rva < export_rva + export_size
        executable_offset = _pe_rva_offset(
            function_rva,
            1,
            sections,
            len(data),
            executable=True,
        )
        if function_rva == 0 or is_forwarder or executable_offset is None:
            invalid.add(name)
        else:
            exported.add(name)
    return exported, invalid, True


def _inspect_bridge_exports(
    path: Path,
    data: bytes,
    relative_path: str,
    native_format: str,
    builder: _AuditBuilder,
) -> Mapping[str, Any]:
    """
    从平台动态导出表校验 flat C ABI 六个符号，拒绝仅出现在普通字符串区的伪标记。

    参数:
        path: 由同代字节写出的私有 0600 原生副本。
        data: 与私有副本完全相同的内存快照字节。
        relative_path: 报告中的脱敏制品内路径。
        native_format: elf、mach_o 或 pe。
        builder: 接收工具缺失、执行失败或导出缺失发现。
    返回:
        只含检查工具、成功状态和已确认符号数量的脱敏摘要。
    """

    expected = {symbol.decode("ascii") for symbol in _EXPECTED_BRIDGE_SYMBOLS}
    exported_functions: Set[str] = set()
    invalid_kind: Set[str] = set()
    tool: Optional[str] = None
    parser: Optional[str] = None
    if native_format == "elf":
        tool = shutil.which("readelf")
        command = [tool, "--wide", "--dyn-syms", str(path)] if tool else []
        parser = "readelf" if tool else None
    elif native_format == "mach_o":
        tool = shutil.which("nm")
        command = [tool, "-m", "-gU", str(path)] if tool else []
        parser = "nm" if tool else None
    elif native_format == "pe":
        exported_functions, invalid_kind, parsed = _pe_bridge_exports(data)
        if not parsed:
            builder.add(
                "NATIVE_EXPORT_INSPECTION_FAILED",
                "PE export table 无法由原始字节安全验证",
                relative_path,
                {"format": native_format},
            )
            return {
                "export_tool": "raw_pe",
                "exports_inspected": False,
                "bridge_export_count": 0,
            }
        parser = "raw_pe"
        command = []
    else:
        command = []
    if native_format != "pe" and not tool:
        builder.add(
            "NATIVE_EXPORT_INSPECTOR_UNAVAILABLE",
            "真实 bridge 存在但动态导出表检查工具不可用",
            relative_path,
            {"format": native_format},
        )
        return {"export_tool": None, "exports_inspected": False, "bridge_export_count": 0}
    output = ""
    if native_format != "pe":
        ok, output = _run_native_command(command)
        if not ok:
            assert tool is not None
            builder.add(
                "NATIVE_EXPORT_INSPECTION_FAILED",
                "动态导出表检查工具输出非零、过大、含 NUL 或不是严格 UTF-8",
                relative_path,
                {"format": native_format, "tool": Path(tool).name},
            )
            return {
                "export_tool": Path(tool).name,
                "exports_inspected": False,
                "bridge_export_count": 0,
            }
    if parser == "readelf":
        for line in output.splitlines():
            parts = line.split()
            if len(parts) < 8 or not parts[0].endswith(":"):
                continue
            raw_symbol = parts[7]
            default_version = True
            if "@@" in raw_symbol:
                version_parts = raw_symbol.split("@@")
                symbol = version_parts[0]
                default_version = (
                    len(version_parts) == 2
                    and bool(symbol)
                    and bool(version_parts[1])
                    and "@" not in version_parts[1]
                )
            elif "@" in raw_symbol:
                symbol = raw_symbol.split("@", 1)[0]
                default_version = False
            else:
                symbol = raw_symbol
            if symbol not in expected:
                continue
            symbol_type, binding, visibility, section = parts[3], parts[4], parts[5], parts[6]
            if (
                default_version
                and symbol_type == "FUNC"
                and binding == "GLOBAL"
                and visibility in {"DEFAULT", "PROTECTED"}
                and section != "UND"
            ):
                exported_functions.add(symbol)
            else:
                invalid_kind.add(symbol)
    elif parser == "nm":
        for line in output.splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            symbol = parts[-1]
            if native_format == "mach_o" and symbol.startswith("_"):
                symbol = symbol[1:]
            if symbol not in expected:
                continue
            strong_text_export = (
                len(parts) == 4
                and re.fullmatch(r"[0-9A-Fa-f]+", parts[0]) is not None
                and parts[1] == "(__TEXT,__text)"
                and parts[2] == "external"
            )
            if strong_text_export:
                exported_functions.add(symbol)
            else:
                invalid_kind.add(symbol)
    if invalid_kind:
        builder.add(
            "BRIDGE_EXPORTS_INVALID_KIND",
            "flat C ABI 名称存在但不是已定义的动态函数导出",
            relative_path,
            {"invalid_count": len(invalid_kind)},
        )
    missing_count = len(expected - exported_functions)
    if missing_count:
        builder.add(
            "BRIDGE_EXPORTS_MISSING",
            "bridge 动态导出表缺少当前 flat C ABI 必要符号",
            relative_path,
            {"missing_count": missing_count},
        )
    return {
        "export_tool": "raw_pe" if native_format == "pe" else Path(str(tool)).name,
        "exports_inspected": True,
        "bridge_export_count": len(exported_functions),
    }


def _parse_macho_load_commands(output: str) -> Tuple[List[str], List[str], bool]:
    """
    从严格 UTF-8 `otool -l` 输出提取 LC_LOAD_* 依赖与 LC_RPATH，跳过 LC_ID_DYLIB。

    参数:
        output: `_run_native_command` 已验证的有界文本。
    返回:
        ``(加载依赖, RPATH, 结构可验证)``；所需 name/path 行缺失时失败。
    """

    dependency_commands = {
        "LC_LOAD_DYLIB",
        "LC_LOAD_WEAK_DYLIB",
        "LC_LAZY_LOAD_DYLIB",
        "LC_REEXPORT_DYLIB",
        "LC_LOAD_UPWARD_DYLIB",
    }
    forbidden_loader_commands = {
        "LC_DYLD_ENVIRONMENT",
        "LC_LOAD_DYLINKER",
        "LC_ID_DYLINKER",
        "LC_LOADFVMLIB",
        "LC_IDFVMLIB",
        "LC_PREBOUND_DYLIB",
        "LC_SUB_FRAMEWORK",
        "LC_SUB_UMBRELLA",
        "LC_SUB_CLIENT",
        "LC_SUB_LIBRARY",
        "LC_ROUTINES",
        "LC_ROUTINES_64",
    }
    dependencies: List[str] = []
    rpaths: List[str] = []
    pending: Optional[str] = None
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Load command "):
            if pending is not None:
                return [], [], False
            continue
        if stripped.startswith("cmd "):
            if pending is not None:
                return [], [], False
            command = stripped[4:].strip()
            if command in forbidden_loader_commands:
                return [], [], False
            if command in dependency_commands:
                pending = "dependency"
            elif command == "LC_RPATH":
                pending = "rpath"
            else:
                pending = None
            continue
        if pending == "dependency" and stripped.startswith("name "):
            match = re.fullmatch(r"name\s+(.*?)\s+\(offset\s+\d+\)", stripped)
            if match is None:
                return [], [], False
            dependencies.append(match.group(1))
            pending = None
        elif pending == "rpath" and stripped.startswith("path"):
            match = re.fullmatch(r"path\s*(.*?)\s+\(offset\s+\d+\)", stripped)
            if match is None:
                return [], [], False
            rpaths.append(match.group(1))
            pending = None
    return dependencies, rpaths, pending is None


def _inspect_native_path(
    path: Path,
    relative_path: str,
    native_format: str,
    builder: _AuditBuilder,
    dependency_profile: Optional[str] = None,
    snapshot_data: Optional[bytes] = None,
) -> Mapping[str, Any]:
    """
    使用平台对应工具读取依赖表和 RPATH，并对缺失工具或绝对构建路径 fail closed。

    参数:
        path: 临时或 bundle 内 native 普通文件。
        relative_path: 报告中的制品内相对路径。
        native_format: elf、mach_o 或 pe。
        builder: 接收工具、依赖或 RPATH 发现。
        dependency_profile: 可选的 offline_fake 精确依赖白名单合同。
        snapshot_data: 与私有工具副本相同的唯一内存快照；缺失时 fail closed。
    返回:
        仅含工具名、依赖文件名和脱敏 RPATH 分类的摘要。
    """

    if snapshot_data is None:
        builder.add(
            "NATIVE_SNAPSHOT_MISSING",
            "native 工具检查缺少与私有副本绑定的内存快照",
            relative_path,
        )
        return {
            "path": _redact_report_path(relative_path),
            "format": native_format,
            "tool": None,
            "inspected": False,
        }
    if not _audit_dynamic_library_image(snapshot_data, native_format, relative_path, builder):
        return {
            "path": _redact_report_path(relative_path),
            "format": native_format,
            "tool": None,
            "inspected": False,
        }
    export_summary = _inspect_bridge_exports(
        path,
        snapshot_data,
        relative_path,
        native_format,
        builder,
    )
    dependencies: List[str] = []
    rpaths: List[str] = []
    forbidden_loader_tag_count = 0
    forbidden_loader_command_count = 0
    tool_name: Optional[str] = None
    if native_format == "elf":
        tool = shutil.which("readelf")
        if tool is None:
            tool = shutil.which("objdump")
            command = [tool, "-p", str(path)] if tool else []
            parser = "objdump"
        else:
            command = [tool, "-d", str(path)]
            parser = "readelf"
        if not tool:
            builder.add(
                "NATIVE_INSPECTOR_UNAVAILABLE",
                "真实 ELF 制品存在但 readelf/objdump 均不可用",
                relative_path,
                {"format": native_format},
            )
            return {
                "path": _redact_report_path(relative_path),
                "format": native_format,
                "tool": None,
                "inspected": False,
                **export_summary,
            }
        tool_name = Path(tool).name
        ok, output = _run_native_command(command)
        if ok and parser == "readelf":
            dependencies = re.findall(r"\(NEEDED\).*?\[([^\]]+)\]", output)
            rpaths = re.findall(r"\((?:RPATH|RUNPATH)\).*?\[([^\]]*)\]", output)
            forbidden_loader_tag_count = len(
                re.findall(r"\((?:FILTER|AUXILIARY|AUDIT|DEPAUDIT)\)", output)
            )
        elif ok:
            dependencies = re.findall(r"(?m)^\s*NEEDED\s+([^\s]+)", output)
            rpaths = re.findall(r"(?m)^\s*(?:RPATH|RUNPATH)\s+(.+?)\s*$", output)
            forbidden_loader_tag_count = len(
                re.findall(r"(?im)^\s*(?:FILTER|AUXILIARY|AUDIT|DEPAUDIT)\s+", output)
            )
    elif native_format == "mach_o":
        tool = shutil.which("otool")
        if not tool:
            builder.add(
                "NATIVE_INSPECTOR_UNAVAILABLE",
                "真实 Mach-O 制品存在但 otool 不可用",
                relative_path,
                {"format": native_format},
            )
            return {
                "path": _redact_report_path(relative_path),
                "format": native_format,
                "tool": None,
                "inspected": False,
                **export_summary,
            }
        tool_name = Path(tool).name
        ok, load_output = _run_native_command([tool, "-l", str(path)])
        if ok:
            forbidden_loader_command_count = len(
                re.findall(
                    r"(?m)^\s*cmd\s+(?:LC_LOAD_WEAK_DYLIB|LC_LAZY_LOAD_DYLIB|"
                    r"LC_REEXPORT_DYLIB|LC_LOAD_UPWARD_DYLIB)\s*$",
                    load_output,
                )
            )
            dependencies, rpaths, parsed = _parse_macho_load_commands(load_output)
            ok = parsed
    elif native_format == "pe":
        tool = shutil.which("llvm-objdump") or shutil.which("objdump")
        if not tool:
            builder.add(
                "NATIVE_INSPECTOR_UNAVAILABLE",
                "真实 PE 制品存在但 llvm-objdump/objdump 不可用",
                relative_path,
                {"format": native_format},
            )
            return {
                "path": _redact_report_path(relative_path),
                "format": native_format,
                "tool": None,
                "inspected": False,
                **export_summary,
            }
        tool_name = Path(tool).name
        ok, output = _run_native_command([tool, "-p", str(path)])
        if ok:
            dependencies = re.findall(r"(?im)^\s*DLL Name:\s*(.+?)\s*$", output)
    else:
        builder.add(
            "NATIVE_FORMAT_UNSUPPORTED",
            "native 魔数格式没有依赖检查实现",
            relative_path,
            {"format": native_format},
        )
        return {
            "path": _redact_report_path(relative_path),
            "format": native_format,
            "tool": None,
            "inspected": False,
            **export_summary,
        }

    if not ok:
        builder.add(
            "NATIVE_INSPECTION_FAILED",
            "native 依赖/RPATH 工具失败或输出过大、含 NUL、非严格 UTF-8/结构无效",
            relative_path,
            {"format": native_format, "tool": tool_name},
        )
        return {
            "path": _redact_report_path(relative_path),
            "format": native_format,
            "tool": tool_name,
            "inspected": False,
            **export_summary,
        }

    allowed_absolute_dependencies = ("/usr/lib/", "/System/Library/")
    elf_loader_tokens = ("$ORIGIN", "${ORIGIN}")
    macho_loader_tokens = ("@loader_path", "@rpath", "@executable_path")
    empty_dependency_count = 0
    for dependency in dependencies:
        dependency = dependency.strip()
        if not dependency:
            empty_dependency_count += 1
            continue
        is_windows_absolute = bool(re.match(r"^[A-Za-z]:[\\/]", dependency))
        parts = PurePosixPath(dependency.replace("\\", "/")).parts
        has_parent = ".." in parts
        if native_format == "elf" and dependency.startswith("@"):
            builder.add(
                "NATIVE_LOADER_TOKEN_FORMAT_MISMATCH",
                "ELF DT_NEEDED 不得使用 Mach-O loader token",
                relative_path,
            )
        if native_format == "mach_o" and dependency.startswith(("$ORIGIN", "${ORIGIN}")):
            builder.add(
                "NATIVE_LOADER_TOKEN_FORMAT_MISMATCH",
                "Mach-O dependency 不得使用 ELF loader token",
                relative_path,
            )
        if dependency.startswith("/"):
            allowed_system = dependency.startswith(allowed_absolute_dependencies) and not has_parent
            if allowed_system:
                continue
            builder.add(
                "NATIVE_ABSOLUTE_DEPENDENCY",
                "native 依赖表包含非系统绝对路径",
                relative_path,
            )
        elif is_windows_absolute:
            builder.add(
                "NATIVE_ABSOLUTE_DEPENDENCY",
                "native 依赖表包含非系统绝对路径",
                relative_path,
            )
        elif "/" in dependency or "\\" in dependency:
            allowed_tokens = macho_loader_tokens if native_format == "mach_o" else tuple()
            loader_relative = dependency.startswith(tuple(token + "/" for token in allowed_tokens))
            if not loader_relative or has_parent or "." in parts:
                builder.add(
                    "NATIVE_UNSAFE_DEPENDENCY",
                    "native 依赖路径不是安全 basename 或 loader-relative 路径",
                    relative_path,
                )
    if empty_dependency_count:
        builder.add(
            "NATIVE_EMPTY_DEPENDENCY",
            "native 依赖表包含空名称",
            relative_path,
            {"count": empty_dependency_count},
        )
    if dependency_profile == "offline_fake":
        allowed_dependencies = {
            "elf": {
                "libstdc++.so.6",
                "libgcc_s.so.1",
                "libc.so.6",
                "libm.so.6",
            },
            "mach_o": {
                "/usr/lib/libc++.1.dylib",
                "/usr/lib/libSystem.B.dylib",
            },
        }
        if forbidden_loader_tag_count:
            builder.add(
                "OFFLINE_FAKE_LOADER_TAG_FORBIDDEN",
                "offline_fake native 包含未授权的动态加载器控制标签",
                relative_path,
                {"count": forbidden_loader_tag_count},
            )
        if forbidden_loader_command_count:
            builder.add(
                "OFFLINE_FAKE_LOADER_COMMAND_FORBIDDEN",
                "offline_fake Mach-O 仅允许普通 LC_LOAD_DYLIB 动态依赖命令",
                relative_path,
                {"count": forbidden_loader_command_count},
            )
        if native_format == "pe":
            builder.add(
                "OFFLINE_FAKE_DEPENDENCY_BASELINE_UNAVAILABLE",
                "Windows offline_fake 尚无受验证的精确运行库依赖基线",
                relative_path,
            )
        else:
            normalized_dependencies = {value.strip() for value in dependencies if value.strip()}
            allowed = allowed_dependencies.get(native_format, set())
            unexpected = normalized_dependencies - allowed
            duplicate_count = len([value for value in dependencies if value.strip()]) - len(
                normalized_dependencies
            )
            if unexpected or duplicate_count:
                builder.add(
                    "OFFLINE_FAKE_DEPENDENCY_NOT_ALLOWED",
                    "offline_fake native 包含精确白名单之外或重复的动态依赖",
                    relative_path,
                    {
                        "unexpected_count": len(unexpected),
                        "duplicate_count": duplicate_count,
                    },
                )
    flattened_rpaths: List[str] = []
    empty_rpath_count = 0
    for value in rpaths:
        components = value.split(":") if native_format == "elf" else [value]
        for component in components:
            normalized_component = component.strip()
            if not normalized_component:
                empty_rpath_count += 1
            else:
                flattened_rpaths.append(normalized_component)
    if empty_rpath_count:
        builder.add(
            "NATIVE_EMPTY_RPATH_ENTRY",
            "native RPATH/RUNPATH 包含会隐式搜索当前工作目录的空项",
            relative_path,
            {"count": empty_rpath_count},
        )
    if dependency_profile == "offline_fake" and rpaths:
        builder.add(
            "OFFLINE_FAKE_RPATH_FORBIDDEN",
            "offline_fake native 不得声明任何 RPATH/RUNPATH",
            relative_path,
            {"entry_count": len(rpaths)},
        )
    relative_rpath_count = 0
    for value in flattened_rpaths:
        value = value.strip()
        parts = PurePosixPath(value.replace("\\", "/")).parts
        allowed_rpath_tokens = (
            elf_loader_tokens
            if native_format == "elf"
            else macho_loader_tokens
            if native_format == "mach_o"
            else tuple()
        )
        wrong_token = (native_format == "elf" and value.startswith("@")) or (
            native_format == "mach_o" and value.startswith(("$ORIGIN", "${ORIGIN}"))
        )
        if wrong_token:
            builder.add(
                "NATIVE_LOADER_TOKEN_FORMAT_MISMATCH",
                "RPATH/RUNPATH 使用了其他原生格式的 loader token",
                relative_path,
            )
        if value.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", value):
            builder.add(
                "NATIVE_ABSOLUTE_RPATH",
                "native RPATH/RUNPATH 包含绝对构建或 SDK 路径",
                relative_path,
            )
        elif value in allowed_rpath_tokens or value.startswith(
            tuple(token + "/" for token in allowed_rpath_tokens)
        ):
            if ".." in parts or "." in parts:
                builder.add(
                    "NATIVE_UNSAFE_RPATH",
                    "native RPATH/RUNPATH 的 loader-relative 路径包含点段",
                    relative_path,
                )
            else:
                relative_rpath_count += 1
        else:
            builder.add(
                "NATIVE_UNSAFE_RPATH",
                "native RPATH/RUNPATH 必须使用受支持的 loader-relative token",
                relative_path,
            )
    dependency_count = len({value.strip() for value in dependencies if value.strip()})
    return {
        "path": _redact_report_path(relative_path),
        "format": native_format,
        "tool": tool_name,
        "inspected": True,
        "dependency_count": dependency_count,
        "rpath_count": len(flattened_rpaths),
        "relative_rpath_count": relative_rpath_count,
        **export_summary,
    }


def _inspect_native_files(
    native_files: Sequence[Tuple[_ArtifactFile, str]],
    builder: _AuditBuilder,
    dependency_profile: Optional[str] = None,
) -> Tuple[Mapping[str, Any], ...]:
    """
    为归档或目录中的真实 native 文件准备只读检查路径并汇总结果。

    参数:
        native_files: 文件记录及已识别格式。
        builder: 接收依赖检查发现。
        dependency_profile: 可选的 offline_fake 精确依赖白名单合同。
    返回:
        按路径排序的脱敏 native 检查摘要。
    """

    results: List[Mapping[str, Any]] = []
    for item, native_format in sorted(native_files, key=lambda value: value[0].path):
        if item.data is None:
            builder.add("NATIVE_INSPECTION_FAILED", "native 文件不可读，无法检查依赖", item.path)
            continue
        with tempfile.TemporaryDirectory(prefix="bt-native-audit-") as temporary:
            temporary_root = Path(temporary)
            suffix = PurePosixPath(item.path).suffix or ".bin"
            staged = temporary_root / ("artifact" + suffix)
            try:
                temporary_root.chmod(0o700)
                _write_private_snapshot_file(staged, item.data)
            except OSError as exc:
                builder.add(
                    "NATIVE_SNAPSHOT_FAILED",
                    "无法为 native 工具建立 0700/0600 私有字节副本",
                    item.path,
                    {"error_type": type(exc).__name__},
                )
                continue
            results.append(
                _inspect_native_path(
                    staged,
                    item.path,
                    native_format,
                    builder,
                    dependency_profile=dependency_profile,
                    snapshot_data=item.data,
                )
            )
    return tuple(results)


def audit_wheel(
    wheel_path: Path,
    policy: Optional[ReleaseAuditPolicy] = None,
) -> ReleaseAuditReport:
    """
    离线审计 wheel 的标签、内容、RECORD、SBOM、敏感资产与可选 native 依赖。

    参数:
        wheel_path: 本地 wheel 文件。
        policy: 可选审计门槛。
    返回:
        wheel 的脱敏审计报告。
    副作用:
        只读归档；仅 platform wheel 含 native 时调用依赖检查工具。
    """

    effective_policy = policy or ReleaseAuditPolicy()
    builder = _AuditBuilder()
    requested_path = Path(wheel_path).expanduser()
    path = requested_path.absolute()
    archive_size: Optional[int] = None
    archive_hash: Optional[str] = None
    files: List[_ArtifactFile] = []
    tags = _parse_wheel_filename(path, builder)
    if _path_has_symlink_component(requested_path):
        builder.add("ARTIFACT_SYMLINK_NOT_ALLOWED", "wheel 外层路径不允许符号链接")
    elif not path.is_file():
        builder.add("ARTIFACT_MISSING", "wheel 不是可读普通文件")
    else:
        archive_bytes, archive_size, archive_hash = _read_outer_artifact_snapshot(
            path,
            effective_policy.max_archive_scan_bytes,
            "wheel",
            builder,
        )
        if archive_bytes is not None:
            try:
                with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
                    _audit_zip_envelope(archive_bytes, archive, builder)
                    files = _collect_zip_files(archive, effective_policy, builder)
            except (OSError, zipfile.BadZipFile) as exc:
                builder.add(
                    "ARCHIVE_INVALID",
                    "wheel 不是有效 ZIP 归档",
                    details={"error_type": type(exc).__name__},
                )

    _validate_inventory_paths(files, builder)
    metadata = dict(_audit_wheel_metadata(files, tags, builder))
    metadata.update(_safe_wheel_report_tags(tags))
    universal = bool(tags.get("universal"))
    if tags.get("platform_tag") == "any" and not universal:
        builder.add(
            "UNIVERSAL_WHEEL_TAG_INVALID",
            "V1 any wheel 必须严格使用 py3-none-any 标签",
            details={
                "python_tag_matches": tags.get("python_tag") == "py3",
                "abi_tag_matches": tags.get("abi_tag") == "none",
                "platform_tag_matches": tags.get("platform_tag") == "any",
            },
        )
    root_is_purelib = metadata.get("root_is_purelib")
    if universal and root_is_purelib != "true":
        builder.add(
            "UNIVERSAL_WHEEL_METADATA_INVALID", "py3-none-any wheel 必须 Root-Is-Purelib: true"
        )
    if (
        not universal
        and tags.get("platform_tag") not in {None, "any"}
        and root_is_purelib != "false"
    ):
        builder.add("PLATFORM_WHEEL_METADATA_INVALID", "platform wheel 必须 Root-Is-Purelib: false")

    native_files = _scan_files(
        files,
        builder,
        public_distribution=universal or tags.get("platform_tag") == "any",
        platform_wheel=not universal and tags.get("platform_tag") not in {None, "any"},
    )
    if not universal and tags.get("platform_tag") not in {None, "any"}:
        builder.add(
            "PLATFORM_WHEEL_RELEASE_NOT_ENABLED",
            "条件 platform wheel 尚未完成可信 build manifest/SBOM/license 与运行库标签门禁",
        )
        _audit_platform_wheel_native_contract(native_files, tags, builder)
    _audit_wheel_record(files, builder, tags.get("dist_info_directory"))
    _required_huaxin_paths((item.path for item in files), builder)

    unpacked_size = sum(item.size for item in files)
    if archive_size is not None:
        max_archive = (
            effective_policy.universal_wheel_max_bytes
            if universal
            else effective_policy.platform_wheel_max_bytes
        )
        if archive_size > max_archive:
            builder.add(
                "ARTIFACT_SIZE_REVIEW_REQUIRED",
                "wheel archive 体积超过发布门槛，必须人工审查后才能发布",
                details={"size": archive_size, "max_bytes": max_archive},
            )
    max_unpacked = (
        effective_policy.universal_wheel_unpacked_max_bytes
        if universal
        else effective_policy.platform_wheel_unpacked_max_bytes
    )
    if unpacked_size > max_unpacked:
        builder.add(
            "UNPACKED_SIZE_REVIEW_REQUIRED",
            "wheel 解包体积超过发布门槛",
            details={"size": unpacked_size, "max_bytes": max_unpacked},
        )
    native_inspection = _inspect_native_files(native_files, builder) if native_files else tuple()
    return ReleaseAuditReport(
        artifact_kind="universal_wheel" if universal else "platform_wheel",
        artifact_name="bullet-trade-wheel",
        artifact_sha256=archive_hash,
        archive_size=archive_size,
        unpacked_size=unpacked_size,
        file_count=len(files),
        metadata=metadata,
        sbom=_sbom(
            files,
            tags.get("dist_info_directory"),
            "bullet_trade-release.dist-info",
        ),
        native_inspection=native_inspection,
        findings=_logical_findings(
            builder.findings,
            tags.get("dist_info_directory"),
            "bullet_trade-release.dist-info",
        ),
    )


def _decompress_gzip_envelope(
    data: bytes,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> Optional[bytes]:
    """
    在解压硬上限内读取唯一 gzip member，并拒绝前后缀、拼接和可变头元数据。

    参数:
        data: 与 sdist 外层哈希相同的完整 gzip 字节快照。
        policy: 提供最大解压扫描字节数的审计策略。
        builder: 接收固定规则码，不记录 gzip 文件名或注释原值。
    返回:
        唯一 gzip member 的 tar 字节；包络无效或解压超限时返回 None。
    """

    if len(data) < 10 or not data.startswith(b"\x1f\x8b\x08"):
        builder.add("GZIP_PREFIX_FORBIDDEN", "sdist 必须从字节零开始于唯一 gzip member")
        return None
    flags = data[3]
    if flags & 0xE0:
        builder.add("GZIP_HEADER_INVALID", "sdist gzip 头包含保留 flag")
    if flags & 0x08:
        builder.add("GZIP_FNAME_FORBIDDEN", "sdist gzip 不允许携带原始文件名 FNAME")
    if flags & (0x04 | 0x10 | 0x02):
        builder.add(
            "GZIP_OPTIONAL_METADATA_FORBIDDEN",
            "sdist gzip 不允许 extra/comment/header-CRC 可变元数据",
        )
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        output = decompressor.decompress(data, policy.max_unpacked_scan_bytes + 1)
    except zlib.error:
        builder.add("GZIP_MEMBER_INVALID", "sdist gzip member 校验或解压失败")
        return None
    while decompressor.unconsumed_tail and len(output) <= policy.max_unpacked_scan_bytes:
        pending = decompressor.unconsumed_tail
        before = len(pending)
        try:
            output += decompressor.decompress(
                pending,
                policy.max_unpacked_scan_bytes + 1 - len(output),
            )
        except zlib.error:
            builder.add("GZIP_MEMBER_INVALID", "sdist gzip member 校验或解压失败")
            return None
        if len(decompressor.unconsumed_tail) >= before:
            builder.add("GZIP_MEMBER_INVALID", "sdist gzip member 无法确定性完成解压")
            return None
    if len(output) > policy.max_unpacked_scan_bytes:
        builder.add(
            "ARCHIVE_UNPACKED_HARD_LIMIT",
            "sdist gzip 解压结果超过审计防 DoS 硬上限",
            details={"max_bytes": policy.max_unpacked_scan_bytes},
        )
        return None
    if not decompressor.eof:
        builder.add("GZIP_MEMBER_INVALID", "sdist gzip member 被截断或超过解压边界")
        return None
    trailing = decompressor.unused_data
    if trailing:
        if trailing.startswith(b"\x1f\x8b"):
            builder.add("GZIP_CONCATENATED_MEMBER_FORBIDDEN", "sdist gzip 不允许拼接多个 member")
        else:
            builder.add("GZIP_TRAILING_DATA_FORBIDDEN", "sdist gzip member 后存在尾随数据")
    return output


def _parse_tar_number(field: bytes) -> Optional[int]:
    """
    解析传统 tar 八进制数字字段，并拒绝 base-256 等未纳入合同的表示。

    参数:
        field: tar header 中定长数字字段的原始字节。
    返回:
        合法非负整数；字段非法或使用未支持编码时返回 None。
    """

    if not field or field[0] & 0x80:
        return None
    normalized = field.rstrip(b"\x00 ").lstrip(b" \x00")
    if not normalized:
        return 0
    if not re.fullmatch(rb"[0-7]+", normalized):
        return None
    try:
        return int(normalized, 8)
    except ValueError:
        return None


def _tar_header_path(block: bytes) -> str:
    """
    从受限 ustar header 提取仅用于安全路径摘要的成员名。

    参数:
        block: 单个 512 字节 tar header。
    返回:
        以 UTF-8 surrogateescape 解码并合并 prefix/name 的路径。
    """

    name = block[0:100].split(b"\x00", 1)[0]
    prefix = block[345:500].split(b"\x00", 1)[0]
    raw_path = prefix + (b"/" if prefix and name else b"") + name
    return raw_path.decode("utf-8", errors="surrogateescape")


def _audit_tar_envelope(data: bytes, builder: _AuditBuilder) -> None:
    """
    逐块校验 tar 原始边界，拒绝非零尾部、PAX/长名头和身份元数据。

    参数:
        data: 已通过 gzip 单 member 上限解压得到的 tar 字节。
        builder: 接收固定规则码和计数，不记录 uname/gname/PAX 原值。
    返回:
        无；原始头部或结束边界存在歧义时追加发现。
    """

    block_size = 512
    if not data or len(data) % block_size:
        builder.add("TAR_ENVELOPE_INVALID", "sdist tar 长度不是完整 512 字节块")
        return
    offset = 0
    saw_end = False
    checksum_errors = 0
    extended_headers = 0
    identity_metadata = 0
    directory_data = 0
    regular_trailing_slash = 0
    while offset + block_size <= len(data):
        block = data[offset : offset + block_size]
        if block == b"\x00" * block_size:
            second = data[offset + block_size : offset + 2 * block_size]
            if second != b"\x00" * block_size:
                builder.add("TAR_END_MARKER_INVALID", "sdist tar 必须以两个连续零块结束")
                break
            saw_end = True
            if any(data[offset + 2 * block_size :]):
                builder.add("TAR_TRAILING_DATA_FORBIDDEN", "sdist tar 结束块后存在非零尾随数据")
            break
        stored_checksum = _parse_tar_number(block[148:156])
        calculated_checksum = sum(block[:148]) + (8 * 32) + sum(block[156:])
        if stored_checksum is None or stored_checksum != calculated_checksum:
            checksum_errors += 1
        size = _parse_tar_number(block[124:136])
        if size is None:
            builder.add("TAR_HEADER_INVALID", "sdist tar 使用未支持或非法的大小字段")
            break
        member_type = block[156:157] or b"\x00"
        if member_type in {
            tarfile.XHDTYPE,
            tarfile.XGLTYPE,
            tarfile.GNUTYPE_LONGNAME,
            tarfile.GNUTYPE_LONGLINK,
        }:
            extended_headers += 1
        if block[265:297].rstrip(b"\x00 ") or block[297:329].rstrip(b"\x00 "):
            identity_metadata += 1
        if member_type == tarfile.DIRTYPE and size:
            directory_data += 1
        if member_type in {tarfile.REGTYPE, tarfile.AREGTYPE} and _tar_header_path(block).endswith(
            "/"
        ):
            regular_trailing_slash += 1
        padded_size = ((size + block_size - 1) // block_size) * block_size
        offset += block_size + padded_size
        if offset > len(data):
            builder.add("TAR_ENVELOPE_INVALID", "sdist tar 成员越过外层解压快照边界")
            break
    if not saw_end:
        builder.add("TAR_END_MARKER_INVALID", "sdist tar 缺少两个连续零结束块")
    if checksum_errors:
        builder.add(
            "TAR_HEADER_CHECKSUM_INVALID",
            "sdist tar header checksum 无效",
            details={"count": checksum_errors},
        )
    if extended_headers:
        builder.add(
            "TAR_EXTENDED_HEADER_FORBIDDEN",
            "sdist tar 不允许 PAX/GNU 扩展头",
            details={"count": extended_headers},
        )
    if identity_metadata:
        builder.add(
            "TAR_IDENTITY_METADATA_FORBIDDEN",
            "sdist tar 不允许 uname/gname 身份元数据",
            details={"count": identity_metadata},
        )
    if directory_data:
        builder.add(
            "ARCHIVE_DIRECTORY_DATA_FORBIDDEN",
            "sdist tar 目录条目不允许携带非零数据",
            details={"count": directory_data},
        )
    if regular_trailing_slash:
        builder.add(
            "TAR_REGULAR_TRAILING_SLASH_FORBIDDEN",
            "sdist tar 普通文件名不允许以斜线结尾",
            details={"count": regular_trailing_slash},
        )


def _collect_tar_files(
    archive: tarfile.TarFile,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> List[_ArtifactFile]:
    """
    安全读取 sdist tar 条目，不落盘并拒绝链接、设备和其他特殊类型。

    参数:
        archive: 已打开的 tar 归档。
        policy: 文件数量和大小硬上限。
        builder: 接收结构发现。
    返回:
        普通文件记录序列。
    """

    files: List[_ArtifactFile] = []
    member_count = 0
    declared_total = 0
    count_limit_reported = False
    size_limit_reported = False
    pax_count = 0
    identity_count = 0
    while True:
        try:
            member = archive.next()
        except (OSError, tarfile.TarError):
            builder.add("ARCHIVE_READ_FAILED", "sdist tar 成员流无法继续解析")
            break
        if member is None:
            break
        member_count += 1
        if member.pax_headers:
            pax_count += 1
        if member.uname or member.gname:
            identity_count += 1
        if member_count > policy.max_file_count:
            if not count_limit_reported:
                builder.add(
                    "FILE_COUNT_LIMIT",
                    "sdist 文件数超过审计硬上限",
                    details={"max_count": policy.max_file_count},
                )
                count_limit_reported = True
            break
        raw_path = member.name
        path = raw_path.rstrip("/") if member.isdir() else raw_path
        if member.isdir():
            if member.size:
                builder.add(
                    "ARCHIVE_DIRECTORY_DATA_FORBIDDEN",
                    "sdist tar 目录条目不允许携带非零数据",
                    details={"count": 1},
                )
            if not path or not _safe_relative_path(path):
                builder.add("PATH_TRAVERSAL", "sdist 目录包含不安全路径", path)
            continue
        if not member.isfile():
            builder.add(
                "SPECIAL_ARCHIVE_MEMBER",
                "sdist 不允许符号链接、硬链接、设备或其他特殊条目",
                path,
                {"member_type": repr(member.type)},
            )
            continue
        if raw_path.endswith("/"):
            builder.add(
                "TAR_REGULAR_TRAILING_SLASH_FORBIDDEN",
                "sdist tar 普通文件名不允许以斜线结尾",
                path,
            )
        if not _safe_relative_path(path):
            builder.add("PATH_TRAVERSAL", "sdist 包含不安全内部路径", path)
        declared_total += member.size
        if declared_total > policy.max_unpacked_scan_bytes:
            if not size_limit_reported:
                builder.add(
                    "ARCHIVE_UNPACKED_HARD_LIMIT",
                    "sdist 声明解包总量超过审计防 DoS 硬上限，拒绝继续读取条目",
                    details={"max_bytes": policy.max_unpacked_scan_bytes},
                )
                size_limit_reported = True
            break
        if member.size > policy.max_file_bytes:
            builder.add(
                "FILE_SIZE_LIMIT",
                "sdist 单文件超过读取硬上限",
                path,
                {"size": member.size, "max_bytes": policy.max_file_bytes},
            )
            files.append(_ArtifactFile(path, member.size, None, None))
            continue
        extracted = archive.extractfile(member)
        if extracted is None:
            builder.add("ARCHIVE_READ_FAILED", "sdist 普通文件无法读取", path)
            files.append(_ArtifactFile(path, member.size, None, None))
            continue
        try:
            data = extracted.read(policy.max_file_bytes + 1)
        finally:
            extracted.close()
        if len(data) != member.size:
            builder.add("ARCHIVE_SIZE_MISMATCH", "sdist 条目实际大小与 tar 记录不一致", path)
        files.append(_ArtifactFile(path, len(data), _sha256_bytes(data), data))
    if pax_count:
        builder.add(
            "TAR_EXTENDED_HEADER_FORBIDDEN",
            "sdist tar 不允许 PAX 扩展元数据",
            details={"count": pax_count},
        )
    if identity_count:
        builder.add(
            "TAR_IDENTITY_METADATA_FORBIDDEN",
            "sdist tar 不允许 uname/gname 身份元数据",
            details={"count": identity_count},
        )
    return files


def _sdist_root(files: Sequence[_ArtifactFile], builder: _AuditBuilder) -> Optional[str]:
    """
    验证 sdist 只有一个非空顶层目录并返回该目录名。

    参数:
        files: sdist 普通文件记录。
        builder: 接收根目录布局发现。
    返回:
        唯一顶层目录，无法确定时返回 None。
    """

    roots = {PurePosixPath(item.path).parts[0] for item in files if PurePosixPath(item.path).parts}
    if len(roots) != 1:
        builder.add(
            "SDIST_ROOT_INVALID",
            "sdist 必须把全部文件放在唯一顶层目录",
            details={"root_count": len(roots)},
        )
        return None
    return next(iter(roots))


def _parse_sdist_filename(
    path: Path,
    builder: _AuditBuilder,
) -> Tuple[Optional[str], Optional[str]]:
    """
    使用 packaging 解析并约束 sdist 文件名，返回规范版本和字面根目录。

    参数:
        path: 用户提供的源码归档词法路径。
        builder: 接收文件名发现项。
    返回:
        ``(PEP 440 规范版本, 文件名字面根目录)``；非法时两项均为 None。
    """

    if _PACKAGING_UTILS is None:
        builder.add(
            "PACKAGING_PARSER_UNAVAILABLE",
            "缺少 packaging，无法按 PEP 503/PEP 440 验证 sdist 文件名",
        )
        return None, None
    try:
        distribution, version = _PACKAGING_UTILS.parse_sdist_filename(path.name)
    except (AttributeError, TypeError, ValueError):
        builder.add("SDIST_FILENAME_INVALID", "sdist 文件名必须绑定 bullet_trade distribution 和版本")
        return None, None
    if str(distribution) != "bullet-trade":
        builder.add("SDIST_FILENAME_INVALID", "sdist 文件名必须绑定 bullet_trade distribution 和版本")
        return None, None
    normalized_version = _canonical_version(str(version), builder)
    if normalized_version is None:
        builder.add("SDIST_FILENAME_INVALID", "sdist 文件名版本不是有效 PEP 440 版本")
        return None, None
    return normalized_version, path.name[: -len(".tar.gz")]


def _audit_sdist_identity(
    files: Sequence[_ArtifactFile],
    root: Optional[str],
    filename_version: Optional[str],
    filename_root: Optional[str],
    builder: _AuditBuilder,
) -> Mapping[str, Any]:
    """
    交叉校验 sdist 文件名、唯一根目录、PKG-INFO 与 pyproject 的 distribution 身份。

    参数:
        files: sdist 普通文件记录。
        root: 已验证的唯一顶层目录。
        filename_version: 文件名解析得到的版本。
        filename_root: 文件名去掉 `.tar.gz` 的字面根目录合同。
        builder: 接收身份不一致发现项。
    返回:
        仅含已验证 distribution 名称和版本的脱敏元数据。
    """

    if root is None or filename_version is None or filename_root is None:
        return {
            "distribution_name": None,
            "distribution_version_matches_filename": False,
        }
    if root != filename_root:
        builder.add(
            "SDIST_ROOT_MISMATCH",
            "sdist 唯一根目录必须与文件名 distribution/version 完全一致",
            root,
        )
    pkg_info_path = root + "/PKG-INFO"
    pkg_info_items = [item for item in files if item.path == pkg_info_path]
    if len(pkg_info_items) != 1 or pkg_info_items[0].data is None:
        builder.add(
            "SDIST_METADATA_INVALID",
            "sdist 根目录必须包含唯一可读 PKG-INFO",
            pkg_info_path,
            {"count": len(pkg_info_items)},
        )
        return {
            "distribution_name": None,
            "distribution_version_matches_filename": False,
        }
    headers = _parse_metadata_headers(pkg_info_items[0].data)
    names = headers.get("name", tuple())
    versions = headers.get("version", tuple())
    canonical_name = (
        _canonical_distribution_name(names[0], builder, pkg_info_path) if len(names) == 1 else None
    )
    canonical_pkg_version = (
        _canonical_version(versions[0], builder, pkg_info_path) if len(versions) == 1 else None
    )
    if canonical_name != "bullet-trade":
        builder.add("SDIST_DISTRIBUTION_INVALID", "PKG-INFO Name 必须唯一等于 bullet-trade")
    if canonical_pkg_version != filename_version:
        builder.add("SDIST_VERSION_MISMATCH", "PKG-INFO Version 必须与 sdist 文件名一致")
    extras = tuple(
        value.lower().replace("_", "-") for value in headers.get("provides-extra", tuple())
    )
    requirements = headers.get("requires-dist", tuple())
    if "huaxin" in extras:
        builder.add("INDEPENDENT_HUAXIN_EXTRA_FORBIDDEN", "sdist PKG-INFO 不得声明 huaxin extra")
    if _contains_forbidden_huaxin_requirement(requirements, builder, pkg_info_path):
        builder.add(
            "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN",
            "sdist PKG-INFO 不得依赖独立 bullet-trade-huaxin distribution",
        )
    return {
        "distribution_name": "bullet-trade" if canonical_name == "bullet-trade" else None,
        "distribution_version_matches_filename": (
            canonical_pkg_version is not None and canonical_pkg_version == filename_version
        ),
    }


def _audit_pyproject_identity(
    data: bytes,
    filename_version: Optional[str],
    path: str,
    builder: _AuditBuilder,
) -> None:
    """
    以严格、最小的 canonical `[project]` TOML 子集校验 sdist 构建身份，拒绝 decoy section。

    参数:
        data: pyproject.toml 原始字节。
        filename_version: sdist 文件名绑定的版本。
        path: 报告中的 pyproject 相对路径。
        builder: 接收项目名称、版本或可选依赖发现。
    返回:
        无；非 canonical 或身份不一致均 fail closed。
    """

    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject.toml 不是有效 UTF-8",
            path,
            {"error_type": type(exc).__name__},
        )
        return
    if _TOML_PARSER is None:
        builder.add(
            "PYPROJECT_PARSER_UNAVAILABLE",
            "缺少 tomllib/tomli，无法结构化验证 pyproject.toml",
            path,
        )
        return
    try:
        document = _TOML_PARSER.loads(text)
    except (TypeError, ValueError, RecursionError) as exc:
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject.toml 不能由真实 TOML 解析器唯一解析",
            path,
            {"error_type": type(exc).__name__},
        )
        return
    if not isinstance(document, dict) or not isinstance(document.get("project"), dict):
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject 必须包含唯一结构化 project table",
            path,
        )
        return
    project = document["project"]
    canonical_name = _canonical_distribution_name(project.get("name"), builder, path)
    if canonical_name != "bullet-trade":
        builder.add(
            "PYPROJECT_DISTRIBUTION_MISMATCH",
            "pyproject [project].name 必须唯一等于 bullet-trade",
            path,
        )
    explicit_present = "version" in project
    explicit_version = (
        _canonical_version(project.get("version"), builder, path) if explicit_present else None
    )
    dynamic = project.get("dynamic", [])
    dynamic_valid = isinstance(dynamic, list) and all(isinstance(value, str) for value in dynamic)
    dynamic_version = dynamic_valid and dynamic.count("version") == 1
    if not dynamic_valid or explicit_present == dynamic_version:
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject version 必须在有效显式 PEP 440 值与 dynamic version 中恰选一种",
            path,
        )
    elif explicit_present and explicit_version is None:
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject 显式 version 不是有效 PEP 440 字符串",
            path,
        )
    elif explicit_present and explicit_version != filename_version:
        builder.add(
            "PYPROJECT_VERSION_MISMATCH",
            "pyproject 显式版本必须与 sdist 文件名/PKG-INFO 一致",
            path,
        )
    dependencies = project.get("dependencies", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(value, str) for value in dependencies
    ):
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject dependencies 必须是字符串数组",
            path,
        )
    elif _contains_forbidden_huaxin_requirement(dependencies, builder, path):
        builder.add(
            "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN",
            "pyproject 不得依赖独立 bullet-trade-huaxin distribution",
            path,
        )
    optional_dependencies = project.get("optional-dependencies", {})
    if not isinstance(optional_dependencies, dict):
        builder.add(
            "PYPROJECT_METADATA_INVALID",
            "pyproject optional-dependencies 必须是结构化 table",
            path,
        )
        return
    for raw_key, raw_requirements in optional_dependencies.items():
        if _canonical_distribution_name(raw_key, builder, path) == "huaxin":
            builder.add(
                "INDEPENDENT_HUAXIN_EXTRA_FORBIDDEN",
                "pyproject 不得声明 huaxin optional dependency extra",
                path,
            )
        if not isinstance(raw_requirements, list) or not all(
            isinstance(value, str) for value in raw_requirements
        ):
            builder.add(
                "PYPROJECT_METADATA_INVALID",
                "pyproject optional dependency 必须是字符串数组",
                path,
            )
        elif _contains_forbidden_huaxin_requirement(raw_requirements, builder, path):
            builder.add(
                "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN",
                "pyproject optional dependency 不得依赖独立 bullet-trade-huaxin distribution",
                path,
            )


def audit_sdist(
    sdist_path: Path,
    policy: Optional[ReleaseAuditPolicy] = None,
) -> ReleaseAuditReport:
    """
    离线审计 sdist 的 tar 安全、第一方源码、禁止资产、敏感值、SBOM 和体积。

    参数:
        sdist_path: 本地 `.tar.gz` 源码分发包。
        policy: 可选审计门槛。
    返回:
        sdist 的脱敏审计报告。
    """

    effective_policy = policy or ReleaseAuditPolicy()
    builder = _AuditBuilder()
    requested_path = Path(sdist_path).expanduser()
    path = requested_path.absolute()
    filename_version, filename_root = _parse_sdist_filename(path, builder)
    archive_size: Optional[int] = None
    archive_hash: Optional[str] = None
    files: List[_ArtifactFile] = []
    if _path_has_symlink_component(requested_path):
        builder.add("ARTIFACT_SYMLINK_NOT_ALLOWED", "sdist 外层路径不允许符号链接")
    elif not path.is_file():
        builder.add("ARTIFACT_MISSING", "sdist 不是可读普通文件")
    else:
        archive_bytes, archive_size, archive_hash = _read_outer_artifact_snapshot(
            path,
            effective_policy.max_archive_scan_bytes,
            "sdist",
            builder,
        )
        if archive_bytes is not None:
            tar_bytes = _decompress_gzip_envelope(archive_bytes, effective_policy, builder)
            if tar_bytes is not None:
                _audit_tar_envelope(tar_bytes, builder)
                try:
                    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
                        files = _collect_tar_files(archive, effective_policy, builder)
                except (OSError, tarfile.TarError) as exc:
                    builder.add(
                        "ARCHIVE_INVALID",
                        "sdist 不是有效 tar 归档",
                        details={"error_type": type(exc).__name__},
                    )
    _validate_inventory_paths(files, builder)
    _scan_files(files, builder, public_distribution=True)
    root = _sdist_root(files, builder)
    identity_metadata = _audit_sdist_identity(
        files,
        root,
        filename_version,
        filename_root,
        builder,
    )
    if root is not None:
        _required_huaxin_paths((item.path for item in files), builder, root_prefix=root)
        pyproject_path = root + "/pyproject.toml"
        pyproject = next((item for item in files if item.path == pyproject_path), None)
        if pyproject is None or pyproject.data is None:
            builder.add("SDIST_METADATA_INVALID", "sdist 缺少可读 pyproject.toml", pyproject_path)
        else:
            _audit_pyproject_identity(
                pyproject.data,
                filename_version,
                pyproject_path,
                builder,
            )
    unpacked_size = sum(item.size for item in files)
    if archive_size is not None and archive_size > effective_policy.sdist_max_bytes:
        builder.add(
            "ARTIFACT_SIZE_REVIEW_REQUIRED",
            "sdist archive 体积超过发布门槛，必须人工审查",
            details={"size": archive_size, "max_bytes": effective_policy.sdist_max_bytes},
        )
    if unpacked_size > effective_policy.sdist_unpacked_max_bytes:
        builder.add(
            "UNPACKED_SIZE_REVIEW_REQUIRED",
            "sdist 解包体积超过发布门槛",
            details={"size": unpacked_size, "max_bytes": effective_policy.sdist_unpacked_max_bytes},
        )
    return ReleaseAuditReport(
        artifact_kind="sdist",
        artifact_name="bullet-trade-sdist",
        artifact_sha256=archive_hash,
        archive_size=archive_size,
        unpacked_size=unpacked_size,
        file_count=len(files),
        metadata={
            "root_directory": "bullet_trade-release" if root is not None else None,
            "format": "tar.gz",
            **identity_metadata,
        },
        sbom=_sbom(files, root, "bullet_trade-release"),
        native_inspection=tuple(),
        findings=_logical_findings(builder.findings, root, "bullet_trade-release"),
    )


def _enumerate_bundle_nodes(
    bundle_root: Path,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> List[Tuple[Path, os.stat_result]]:
    """
    用显式 scandir 栈枚举 bundle 节点，且永不递归进入符号链接目录。

    参数:
        bundle_root: 已由 O_DIRECTORY/O_NOFOLLOW 打开的 bundle 根目录。
        policy: 提供最大节点计数硬上限。
        builder: 接收枚举失败或数量超限发现。
    返回:
        按相对路径排序的 ``(路径, lstat)`` 序列；所有目录均按不跟随链接方式判断。
    """

    nodes: List[Tuple[Path, os.stat_result]] = []
    pending = [bundle_root]
    limit_reported = False
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(str(directory)) as entries:
                current_entries = sorted(entries, key=lambda entry: entry.name)
        except OSError as exc:
            builder.add(
                "BUNDLE_WALK_FAILED",
                "bundle 子目录无法以不跟随链接方式枚举",
                details={"error_type": type(exc).__name__},
            )
            continue
        child_directories: List[Path] = []
        for entry in current_entries:
            path = directory / entry.name
            try:
                status = entry.stat(follow_symlinks=False)
            except OSError as exc:
                builder.add(
                    "FILE_READ_FAILED",
                    "bundle 节点无法读取不跟随链接的元数据",
                    path.relative_to(bundle_root).as_posix(),
                    {"error_type": type(exc).__name__},
                )
                continue
            if len(nodes) >= policy.max_file_count:
                if not limit_reported:
                    builder.add(
                        "FILE_COUNT_LIMIT",
                        "bundle 条目数超过审计硬上限",
                        details={"max_count": policy.max_file_count},
                    )
                    limit_reported = True
                continue
            nodes.append((path, status))
            if stat.S_ISDIR(status.st_mode):
                child_directories.append(path)
        pending.extend(reversed(child_directories))
    return sorted(nodes, key=lambda item: item[0].relative_to(bundle_root).as_posix())


def _collect_bundle_files(
    bundle_root: Path,
    policy: ReleaseAuditPolicy,
    builder: _AuditBuilder,
) -> List[_ArtifactFile]:
    """
    递归读取 bundle 普通文件，不跟随符号链接并拒绝越界节点。

    参数:
        bundle_root: 显式内容寻址 bundle 根目录。
        policy: 数量和大小硬上限。
        builder: 接收目录结构发现。
    返回:
        bundle 文件记录序列。
    """

    files: List[_ArtifactFile] = []
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    root_descriptor: Optional[int] = None
    try:
        if os.name == "posix":
            root_descriptor = os.open(str(bundle_root), directory_flags)
            root_before = os.fstat(root_descriptor)
        else:
            root_before = bundle_root.lstat()
    except OSError as exc:
        builder.add(
            "BUNDLE_WALK_FAILED",
            "无法以当前平台最强不跟随链接方式打开 bundle 根目录",
            details={"error_type": type(exc).__name__},
        )
        return files
    try:
        if not stat.S_ISDIR(root_before.st_mode):
            builder.add("ARTIFACT_MISSING", "bundle 根路径不是普通目录")
            return files
        if os.name == "posix" and hasattr(os, "geteuid") and root_before.st_uid != os.geteuid():
            builder.add(
                "BUNDLE_DIRECTORY_OWNER_INVALID",
                "bundle 根目录必须由当前审计用户拥有",
            )
        if os.name == "posix" and stat.S_IMODE(root_before.st_mode) & 0o022:
            builder.add(
                "BUNDLE_DIRECTORY_PERMISSIONS_UNSAFE",
                "bundle 根目录不得允许组或其他用户写入",
            )
        candidates = _enumerate_bundle_nodes(bundle_root, policy, builder)
        scanned_bytes = 0
        for path, path_status in candidates:
            relative = path.relative_to(bundle_root).as_posix()
            if stat.S_ISLNK(path_status.st_mode):
                builder.add("SYMLINK_NOT_ALLOWED", "bundle 不允许符号链接", relative)
                continue
            if stat.S_ISDIR(path_status.st_mode):
                if relative != "lib":
                    builder.add(
                        "BUNDLE_DIRECTORY_INVENTORY_INVALID",
                        "bundle 目录清单只允许 canonical artifact 的 lib 父目录",
                        details={"unexpected_count": 1},
                    )
                if (
                    os.name == "posix"
                    and hasattr(os, "geteuid")
                    and path_status.st_uid != os.geteuid()
                ):
                    builder.add(
                        "BUNDLE_DIRECTORY_OWNER_INVALID",
                        "bundle 子目录必须由当前审计用户拥有",
                        relative,
                    )
                if os.name == "posix" and stat.S_IMODE(path_status.st_mode) & 0o022:
                    builder.add(
                        "BUNDLE_DIRECTORY_PERMISSIONS_UNSAFE",
                        "bundle 子目录不得允许组或其他用户写入",
                        relative,
                    )
                continue
            if not stat.S_ISREG(path_status.st_mode):
                builder.add("SPECIAL_BUNDLE_NODE", "bundle 包含非普通文件节点", relative)
                continue
            if scanned_bytes + path_status.st_size > policy.max_unpacked_scan_bytes:
                builder.add(
                    "INVENTORY_HARD_LIMIT",
                    "bundle 累计读取量超过审计防 DoS 硬上限",
                    details={"max_bytes": policy.max_unpacked_scan_bytes},
                )
                break
            item = _read_filesystem_file(
                path,
                relative,
                policy,
                builder,
                strict_bundle=True,
                retain_source_path=False,
            )
            scanned_bytes += item.size
            files.append(item)
        root_after = (
            os.fstat(root_descriptor) if root_descriptor is not None else bundle_root.lstat()
        )
        root_before_identity = (
            root_before.st_dev,
            root_before.st_ino,
            getattr(root_before, "st_mtime_ns", int(root_before.st_mtime * 1_000_000_000)),
            getattr(root_before, "st_ctime_ns", int(root_before.st_ctime * 1_000_000_000)),
            root_before.st_mode,
            root_before.st_uid,
        )
        root_after_identity = (
            root_after.st_dev,
            root_after.st_ino,
            getattr(root_after, "st_mtime_ns", int(root_after.st_mtime * 1_000_000_000)),
            getattr(root_after, "st_ctime_ns", int(root_after.st_ctime * 1_000_000_000)),
            root_after.st_mode,
            root_after.st_uid,
        )
        if root_before_identity != root_after_identity:
            builder.add(
                "BUNDLE_CHANGED_DURING_AUDIT",
                "bundle 根目录在清单快照期间发生变化",
            )
        return files
    except OSError as exc:
        builder.add(
            "BUNDLE_WALK_FAILED",
            "无法稳定枚举 bundle 内容",
            details={"error_type": type(exc).__name__},
        )
        return files
    finally:
        if root_descriptor is not None:
            os.close(root_descriptor)


def _canonical_bundle_artifact_path(target: object) -> Optional[str]:
    """
    根据受支持的 target 身份返回 offline_fake bridge 的唯一 bundle 相对路径。

    参数:
        target: manifest 中未经信任的 target 对象。
    返回:
        Darwin、Linux 或 Windows 的 canonical artifact 路径；身份非法时返回 None。
    """

    if not isinstance(target, dict):
        return None
    system = target.get("system")
    machine = _normalize_machine(target.get("machine"))
    if not isinstance(system, str) or system not in {"darwin", "linux", "windows"}:
        return None
    if machine not in {"x86_64", "aarch64"}:
        return None
    filename = {
        "darwin": "libbullet_trade_huaxin.dylib",
        "linux": "libbullet_trade_huaxin.so",
        "windows": "bullet_trade_huaxin.dll",
    }[system]
    return "lib/" + filename


def _safe_bundle_name(value: str) -> str:
    """
    将 bundle 目录名限制为小写 64 位十六进制指纹或固定占位符。

    参数:
        value: 用户提供路径的 basename；不会原样写入非法报告。
    返回:
        合法内容指纹或固定 ``<invalid-bundle-name>``。
    """

    return value if _BUNDLE_NAME_PATTERN.fullmatch(value) else "<invalid-bundle-name>"


def _audit_bundle_manifest(
    files: Sequence[_ArtifactFile],
    builder: _AuditBuilder,
    bundle_directory_name: str,
) -> _BundleManifestResult:
    """
    校验 build bundle manifest 的相对 artifact、哈希和厂商资产外置声明。

    参数:
        files: bundle 普通文件记录。
        builder: 接收 manifest 发现。
        bundle_directory_name: 内容寻址 bundle 的目录 basename。
    返回:
        不含路径或厂商版本原值的摘要，以及仅供私有快照路由的受控信息。
    """

    manifests = [item for item in files if item.path == "manifest.json"]
    if len(manifests) != 1 or manifests[0].data is None:
        builder.add(
            "BUNDLE_MANIFEST_INVALID",
            "bundle 必须包含唯一可读 manifest.json",
            details={"count": len(manifests)},
        )
        return _BundleManifestResult(
            metadata={"manifest_present": False, "manifest_valid": False},
            manifest=None,
            artifact_path=None,
            mode_is_offline_fake=False,
            structurally_valid=False,
        )
    try:
        manifest = _load_strict_json_object(manifests[0].data)
    except (UnicodeError, ValueError, RecursionError):
        builder.add(
            "BUNDLE_MANIFEST_INVALID",
            "bundle manifest 不是唯一、有限且无 NUL 的严格 UTF-8 JSON 对象",
            "manifest.json",
        )
        return _BundleManifestResult(
            metadata={"manifest_present": True, "manifest_valid": False},
            manifest=None,
            artifact_path=None,
            mode_is_offline_fake=False,
            structurally_valid=False,
        )
    valid = True
    if not _bundle_manifest_schema_is_exact(manifest):
        builder.add(
            "BUNDLE_MANIFEST_SCHEMA_INVALID",
            "bundle manifest 必须精确符合当前 schema，且不得包含未知字段",
            "manifest.json",
        )
        valid = False
    schema_version_is_one = (
        type(manifest.get("schema_version")) is int and manifest.get("schema_version") == 1
    )
    if not schema_version_is_one:
        builder.add("BUNDLE_MANIFEST_INVALID", "bundle manifest schema_version 必须为 1")
        valid = False
    mode_is_offline_fake = manifest.get("mode") == "offline_fake"
    if not mode_is_offline_fake:
        builder.add(
            "BUNDLE_MODE_BOUNDARY_INVALID",
            "当前审计仅接受不链接厂商 SDK 的 offline_fake bundle",
            "manifest.json",
        )
        valid = False
    fingerprint = manifest.get("fingerprint")
    expected_fingerprint = fingerprint.get("value") if isinstance(fingerprint, dict) else None
    unsigned = dict(manifest)
    unsigned.pop("fingerprint", None)
    try:
        actual_fingerprint = _sha256_bytes(
            json.dumps(
                unsigned,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        actual_fingerprint = None
    if (
        not isinstance(fingerprint, dict)
        or fingerprint.get("algorithm") != "sha256"
        or not isinstance(expected_fingerprint, str)
        or _BUNDLE_NAME_PATTERN.fullmatch(expected_fingerprint) is None
        or expected_fingerprint != actual_fingerprint
        or bundle_directory_name != actual_fingerprint
    ):
        builder.add(
            "BUNDLE_FINGERPRINT_MISMATCH",
            "bundle manifest 指纹或内容寻址目录名不自洽",
            "manifest.json",
        )
        valid = False
    bridge = manifest.get("bridge")
    if not isinstance(bridge, dict):
        builder.add("BUNDLE_MANIFEST_INVALID", "bundle manifest 缺少 bridge 对象", "manifest.json")
        return _BundleManifestResult(
            metadata={
                "manifest_present": True,
                "manifest_valid": False,
                "schema_version": 1 if schema_version_is_one else None,
                "mode_is_offline_fake": mode_is_offline_fake,
            },
            manifest=manifest,
            artifact_path=None,
            mode_is_offline_fake=mode_is_offline_fake,
            structurally_valid=False,
        )
    bridge_identity_valid = (
        bridge.get("abi_version") != 2
        or bridge.get("vendor_schema_id") != "bullet_trade.huaxin.offline_fake.v1"
        or bridge.get("field_set_version") != "1"
    )
    if bridge_identity_valid:
        builder.add(
            "BUNDLE_BRIDGE_IDENTITY_INVALID",
            "bundle bridge ABI/schema 身份不符合当前第一方合同",
            "manifest.json",
        )
        valid = False
    artifact = bridge.get("artifact")
    canonical_artifact = _canonical_bundle_artifact_path(manifest.get("target"))
    file_map = {item.path: item for item in files}
    if canonical_artifact is None:
        builder.add(
            "BUNDLE_TARGET_INVALID",
            "bundle target 不能映射到受支持平台的 canonical artifact",
            "manifest.json",
        )
        valid = False
        artifact_path = None
    elif artifact != canonical_artifact:
        builder.add(
            "BUNDLE_ARTIFACT_PATH_INVALID",
            "bundle manifest artifact 必须等于 target 对应的唯一 canonical 路径",
            "manifest.json",
        )
        valid = False
        artifact_path = None
    else:
        artifact_path = canonical_artifact
        item = file_map.get(canonical_artifact)
        if item is None:
            builder.add(
                "BUNDLE_ARTIFACT_MISSING",
                "manifest 指向的 canonical bridge artifact 不存在",
                "manifest.json",
            )
            valid = False
        elif (
            item.data is None
            or _native_format(item.data) is None
            or not _is_self_bridge_path(canonical_artifact)
        ):
            builder.add(
                "BUNDLE_ARTIFACT_INVALID",
                "manifest artifact 必须指向第一方 Huaxin bridge native",
                "manifest.json",
            )
            valid = False
        elif item.sha256 != bridge.get("sha256"):
            builder.add(
                "BUNDLE_ARTIFACT_HASH_MISMATCH",
                "manifest bridge SHA-256 不自洽",
                "manifest.json",
            )
            valid = False
    expected_inventory = {"manifest.json"}
    if canonical_artifact is not None:
        expected_inventory.add(canonical_artifact)
    actual_inventory = {item.path for item in files}
    if actual_inventory != expected_inventory or len(files) != len(expected_inventory):
        builder.add(
            "BUNDLE_INVENTORY_INVALID",
            "bundle 文件清单必须精确等于 manifest 与 target canonical artifact",
            details={
                "actual_count": len(files),
                "expected_count": len(expected_inventory),
                "missing_count": len(expected_inventory - actual_inventory),
                "unexpected_count": len(actual_inventory - expected_inventory),
            },
        )
        valid = False
    vendor_sdk = manifest.get("vendor_sdk")
    if not isinstance(vendor_sdk, dict) or vendor_sdk.get("included") is not False:
        builder.add(
            "VENDOR_SDK_INCLUDED",
            "bundle manifest 未明确声明厂商 SDK 资产外置",
            "manifest.json",
        )
        valid = False
    if bridge.get("vendor_sdk_linked") not in {False, True}:
        builder.add(
            "BUNDLE_MANIFEST_INVALID",
            "bundle manifest 缺少 vendor_sdk_linked 布尔值",
            "manifest.json",
        )
        valid = False
    elif bridge.get("vendor_sdk_linked") is not False:
        builder.add(
            "BUNDLE_MODE_BOUNDARY_INVALID",
            "offline_fake bundle 不得链接厂商 SDK",
            "manifest.json",
        )
        valid = False
    source = manifest.get("source")
    source_hash = source.get("sha256") if isinstance(source, dict) else None
    if (
        not isinstance(source, dict)
        or not isinstance(source_hash, str)
        or _BUNDLE_NAME_PATTERN.fullmatch(source_hash) is None
    ):
        builder.add(
            "BUNDLE_SOURCE_FINGERPRINT_INVALID",
            "bundle manifest 缺少第一方源码快照 SHA-256",
            "manifest.json",
        )
        valid = False
    distribution = manifest.get("distribution")
    if not isinstance(distribution, dict) or distribution.get("name") != "bullet-trade":
        builder.add(
            "BUNDLE_DISTRIBUTION_INVALID",
            "bundle manifest distribution 必须属于 bullet-trade",
            "manifest.json",
        )
        valid = False
    metadata = {
        "manifest_present": True,
        "manifest_valid": valid,
        "schema_version": 1 if schema_version_is_one else None,
        "mode_is_offline_fake": mode_is_offline_fake,
        "vendor_sdk_included_is_false": (
            isinstance(vendor_sdk, dict) and vendor_sdk.get("included") is False
        ),
        "bridge_vendor_sdk_linked_is_false": bridge.get("vendor_sdk_linked") is False,
        "bridge_abi_version": 2 if bridge.get("abi_version") == 2 else None,
        "canonical_artifact_present": (
            artifact_path is not None
            and artifact_path in file_map
            and file_map[artifact_path].data is not None
        ),
    }
    return _BundleManifestResult(
        metadata=metadata,
        manifest=manifest,
        artifact_path=artifact_path,
        mode_is_offline_fake=mode_is_offline_fake,
        structurally_valid=(
            valid
            and artifact_path is not None
            and artifact_path in file_map
            and file_map[artifact_path].data is not None
        ),
    )


def _write_private_snapshot_file(path: Path, data: bytes) -> None:
    """
    在私有快照目录中以排他方式写入 0600 普通文件。

    参数:
        path: 已验证位于私有 0700 根目录下的目标路径。
        data: 来自单文件描述符快照的唯一字节串。
    返回:
        无。
    副作用:
        创建父目录和目标文件；不会读取原始 bundle 路径。
    异常:
        OSError: 创建、写入或权限收紧失败。
    """

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(str(path), flags, 0o600)
    descriptor_chmod = getattr(os, "fchmod", None)
    try:
        view = memoryview(data)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise OSError("private_snapshot_write_failed")
            written += count
        os.fsync(descriptor)
        if descriptor_chmod is not None:
            descriptor_chmod(descriptor, 0o600)
    finally:
        os.close(descriptor)
    if descriptor_chmod is None:
        path.chmod(0o600)


def _materialize_bundle_snapshot(
    files: Sequence[_ArtifactFile],
    manifest_result: _BundleManifestResult,
    temporary_parent: Path,
    original_directory_name: str,
    builder: _AuditBuilder,
) -> Tuple[Optional[Path], List[_ArtifactFile]]:
    """
    将 manifest 与 canonical artifact 的同代字节物化为精确私有 bundle 快照。

    参数:
        files: 已通过单描述符读取的原 bundle 文件记录。
        manifest_result: 严格 manifest 审计结果。
        temporary_parent: 由 TemporaryDirectory 创建的私有父目录。
        original_directory_name: 原内容寻址目录 basename，仅合法 64hex 时复用。
        builder: 接收物化失败发现。
    返回:
        ``(0700 内容寻址根, 两个私有文件记录)``；前提不足时返回 ``(None, [])``。
    副作用:
        仅在临时父目录写入 manifest 和 canonical artifact 的快照字节。
    """

    if (
        not manifest_result.structurally_valid
        or manifest_result.artifact_path is None
        or manifest_result.manifest is None
    ):
        return None, []
    file_map = {item.path: item for item in files}
    selected_paths = ("manifest.json", manifest_result.artifact_path)
    selected = [file_map.get(path) for path in selected_paths]
    if any(item is None or item.data is None for item in selected):
        return None, []
    snapshot_name = (
        original_directory_name
        if _BUNDLE_NAME_PATTERN.fullmatch(original_directory_name)
        else "0" * 64
    )
    try:
        temporary_parent.chmod(0o700)
        snapshot_root = temporary_parent / snapshot_name
        snapshot_root.mkdir(mode=0o700)
        snapshot_root.chmod(0o700)
        copied: List[_ArtifactFile] = []
        for original in selected:
            assert original is not None
            assert original.data is not None
            destination = snapshot_root / Path(*PurePosixPath(original.path).parts)
            _write_private_snapshot_file(destination, original.data)
            copied.append(
                _ArtifactFile(
                    path=original.path,
                    size=len(original.data),
                    sha256=_sha256_bytes(original.data),
                    data=original.data,
                    source_path=destination,
                )
            )
        return snapshot_root, copied
    except OSError as exc:
        builder.add(
            "BUNDLE_SNAPSHOT_FAILED",
            "无法建立只含 manifest 与 canonical artifact 的私有快照",
            details={"error_type": type(exc).__name__},
        )
        return None, []


def _audit_managed_bundle_trust(
    root: Path,
    native_files: Sequence[Tuple[_ArtifactFile, str]],
    builder: _AuditBuilder,
) -> None:
    """
    把 bundle 自述 manifest 绑定到当前 distribution、第一方源码、平台和真实 artifact。

    参数:
        root: 未经符号链接跟随的 bundle 绝对路径。
        native_files: 审计清单中按魔数识别的原生文件。
        builder: 接收受信基线或目标身份不一致发现。
    返回:
        无；当前 `verify_bundle` 或平台身份任一不一致均 fail closed。
    """

    try:
        from bullet_trade.integrations.huaxin.build import verify_bundle
        from bullet_trade.integrations.huaxin.errors import HuaxinBundleError
    except ImportError as exc:
        builder.add(
            "BUNDLE_TRUSTED_BASELINE_UNAVAILABLE",
            "无法加载当前第一方 bundle 验证器",
            details={"error_type": type(exc).__name__},
        )
        return
    try:
        manifest, verified_artifact = verify_bundle(root)
    except (HuaxinBundleError, OSError, UnicodeError, ValueError, RecursionError) as exc:
        builder.add(
            "BUNDLE_TRUSTED_BASELINE_MISMATCH",
            "bundle 未通过当前 distribution/source/ABI 内容寻址验证",
            details={"error_type": type(exc).__name__},
        )
        return
    distribution = manifest.get("distribution")
    bridge = manifest.get("bridge")
    vendor_sdk = manifest.get("vendor_sdk")
    if not isinstance(distribution, dict) or distribution.get("name") != "bullet-trade":
        builder.add("BUNDLE_DISTRIBUTION_INVALID", "bundle distribution.name 必须等于 bullet-trade")
    if (
        manifest.get("mode") != "offline_fake"
        or not isinstance(bridge, dict)
        or bridge.get("vendor_sdk_linked") is not False
        or not isinstance(vendor_sdk, dict)
        or vendor_sdk.get("included") is not False
    ):
        builder.add(
            "BUNDLE_MODE_BOUNDARY_INVALID",
            "当前审计仅信任不链接且不包含厂商 SDK 的 offline_fake bundle",
        )
    target = manifest.get("target")
    if not isinstance(target, dict):
        builder.add("BUNDLE_TARGET_INVALID", "bundle manifest 缺少结构化 target 身份")
        return
    target_system = target.get("system")
    target_machine = _normalize_machine(target.get("machine"))
    current_system = platform.system().lower()
    current_machine = _normalize_machine(platform.machine())
    if target_system != current_system or target_machine != current_machine:
        builder.add(
            "BUNDLE_TARGET_MISMATCH",
            "bundle target 与当前受信构建/审计主机不一致",
            details={
                "system_match": target_system == current_system,
                "machine_match": target_machine == current_machine,
            },
        )
    if len(native_files) != 1:
        return
    item, native_format = native_files[0]
    expected_format = {"darwin": "mach_o", "linux": "elf", "windows": "pe"}.get(current_system)
    actual_architecture = (
        _native_architecture(item.data, native_format) if item.data is not None else None
    )
    if native_format != expected_format or actual_architecture != current_machine:
        builder.add(
            "BUNDLE_NATIVE_TARGET_MISMATCH",
            "bundle 原生格式/架构与 manifest target 和当前主机不一致",
            item.path,
            {
                "format_match": native_format == expected_format,
                "architecture_match": actual_architecture == current_machine,
            },
        )
    source_path = item.source_path.resolve() if item.source_path is not None else None
    if source_path != verified_artifact:
        builder.add(
            "BUNDLE_ARTIFACT_IDENTITY_MISMATCH",
            "manifest 绑定 artifact 与审计到的唯一 native 不是同一普通文件",
            item.path,
        )


def audit_bundle(
    bundle_path: Path,
    policy: Optional[ReleaseAuditPolicy] = None,
) -> ReleaseAuditReport:
    """
    离线审计显式构建 bundle 的路径、manifest、资产、native 依赖/RPATH 与 SBOM。

    参数:
        bundle_path: 内容寻址 bundle 目录。
        policy: 可选审计门槛。
    返回:
        bundle 的脱敏审计报告。
    副作用:
        只读 bundle；真实 native 存在时调用只读依赖检查工具，不执行 dlopen。
    """

    effective_policy = policy or ReleaseAuditPolicy()
    builder = _AuditBuilder()
    requested_root = Path(bundle_path).expanduser()
    root = requested_root.absolute()
    files: List[_ArtifactFile] = []
    if _path_has_symlink_component(requested_root):
        builder.add("ARTIFACT_SYMLINK_NOT_ALLOWED", "bundle 外层路径不允许符号链接")
    elif not root.is_dir():
        builder.add("ARTIFACT_MISSING", "bundle 不是可读普通目录")
    else:
        files = _collect_bundle_files(root, effective_policy, builder)
    _validate_inventory_paths(files, builder)
    native_files = _scan_files(files, builder, public_distribution=False, bundle=True)
    manifest_result = _audit_bundle_manifest(files, builder, root.name)
    if len(native_files) != 1:
        builder.add(
            "BUNDLE_NATIVE_COUNT_INVALID",
            "build bundle 必须且只能包含一个第一方 bridge native",
            details={"count": len(native_files)},
        )
    valid_native_paths: Set[str] = set()
    for item, native_kind in native_files:
        if item.data is not None and _audit_dynamic_library_image(
            item.data,
            native_kind,
            item.path,
            builder,
        ):
            valid_native_paths.add(item.path)
        missing_symbol_count = sum(
            1 for symbol in _EXPECTED_BRIDGE_SYMBOLS if item.data is None or symbol not in item.data
        )
        if missing_symbol_count:
            builder.add(
                "BUNDLE_BRIDGE_SYMBOLS_MISSING",
                "bridge native 缺少当前 flat C ABI 的必要导出符号标记",
                item.path,
                {"missing_count": missing_symbol_count},
            )
    unpacked_size = sum(item.size for item in files)
    if unpacked_size > effective_policy.bundle_unpacked_max_bytes:
        builder.add(
            "UNPACKED_SIZE_REVIEW_REQUIRED",
            "bundle 总体积超过发布审计门槛",
            details={
                "size": unpacked_size,
                "max_bytes": effective_policy.bundle_unpacked_max_bytes,
            },
        )
    native_inspection: Tuple[Mapping[str, Any], ...] = tuple()
    if files and manifest_result.structurally_valid:
        with tempfile.TemporaryDirectory(prefix="bt-bundle-snapshot-") as temporary:
            snapshot_root, snapshot_files = _materialize_bundle_snapshot(
                files,
                manifest_result,
                Path(temporary),
                root.name,
                builder,
            )
            snapshot_native_files = [
                (item, native_format)
                for item in snapshot_files
                if item.data is not None
                for native_format in (_native_format(item.data),)
                if native_format is not None and item.path in valid_native_paths
            ]
            if snapshot_root is not None and snapshot_native_files:
                _audit_managed_bundle_trust(snapshot_root, snapshot_native_files, builder)
                native_inspection = _inspect_native_files(
                    snapshot_native_files,
                    builder,
                    dependency_profile=(
                        "offline_fake" if manifest_result.mode_is_offline_fake else None
                    ),
                )
    return ReleaseAuditReport(
        artifact_kind="native_bundle",
        artifact_name=_safe_bundle_name(root.name),
        artifact_sha256=_inventory_digest(files),
        archive_size=None,
        unpacked_size=unpacked_size,
        file_count=len(files),
        metadata=manifest_result.metadata,
        sbom=_sbom(files),
        native_inspection=native_inspection,
        findings=tuple(builder.findings),
    )


def clean_import_wheel(
    wheel_path: Path, python_executable: Optional[Path] = None
) -> CleanImportReport:
    """
    在临时 target 中离线安装 wheel，并验证主包与 Huaxin 模块不触发编译/dlopen。

    参数:
        wheel_path: 本地 wheel 文件。
        python_executable: 可选当前解释器路径；缺省使用运行本模块的解释器。
    返回:
        当前解释器范围的尽力 clean-import 证据，不替代 Python 3.8-3.12 CI 矩阵。
    副作用:
        在临时目录执行 ``pip --no-index --no-deps --target`` 并启动隔离子进程。
    """

    import sys

    requested_wheel = Path(wheel_path).expanduser()
    wheel = requested_wheel.absolute()
    executable = Path(python_executable or sys.executable).expanduser().resolve()
    python_version = "unknown"
    if _path_has_symlink_component(requested_wheel):
        return CleanImportReport(False, python_version, False, False, "INPUT_SYMLINK_NOT_ALLOWED")
    if not wheel.is_file() or not executable.is_file():
        return CleanImportReport(False, python_version, False, False, "INPUT_MISSING")
    static_report = audit_wheel(wheel)
    if not static_report.passed:
        return CleanImportReport(False, python_version, False, False, "STATIC_AUDIT_FAILED")
    snapshot_builder = _AuditBuilder()
    wheel_bytes, _wheel_size, wheel_hash = _read_outer_artifact_snapshot(
        wheel,
        ReleaseAuditPolicy().max_archive_scan_bytes,
        "wheel",
        snapshot_builder,
    )
    if snapshot_builder.findings or wheel_bytes is None:
        return CleanImportReport(False, python_version, False, False, "ARTIFACT_CHANGED")
    if static_report.artifact_sha256 != wheel_hash:
        return CleanImportReport(False, python_version, False, False, "ARTIFACT_CHANGED")
    try:
        version_probe = subprocess.run(
            [
                str(executable),
                "-I",
                "-c",
                "import sys; print('.'.join(str(v) for v in sys.version_info[:3]))",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return CleanImportReport(False, python_version, False, False, "PYTHON_PROBE_FAILED")
    version_candidate = version_probe.stdout.strip()
    if version_probe.returncode != 0 or not re.fullmatch(r"\d+\.\d+\.\d+", version_candidate):
        return CleanImportReport(False, python_version, False, False, "PYTHON_PROBE_FAILED")
    python_version = version_candidate
    with tempfile.TemporaryDirectory(prefix="bt-wheel-import-") as temporary:
        temporary_root = Path(temporary)
        target = temporary_root / "site"
        work = temporary_root / "work"
        audited_wheel = temporary_root / wheel.name
        target.mkdir()
        work.mkdir()
        audited_wheel.write_bytes(wheel_bytes)
        try:
            install = subprocess.run(
                [
                    str(executable),
                    "-m",
                    "pip",
                    "install",
                    "--no-index",
                    "--no-deps",
                    "--no-compile",
                    "--target",
                    str(target),
                    str(audited_wheel),
                ],
                cwd=str(work),
                check=False,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except (OSError, subprocess.SubprocessError):
            return CleanImportReport(False, python_version, False, False, "OFFLINE_INSTALL_FAILED")
        if install.returncode != 0:
            return CleanImportReport(False, python_version, False, False, "OFFLINE_INSTALL_FAILED")
        script = """
import ctypes
import os
import pathlib
import socket
import subprocess
import sys

def forbidden(*args, **kwargs):
    '''拒绝 clean-import 期间的进程启动或动态库加载。

    参数:
        args: 被拦截调用的位置参数，不读取。
        kwargs: 被拦截调用的关键字参数，不读取。
    返回:
        永不返回。
    异常:
        AssertionError: 每次调用均抛出，证明导入存在副作用。
    '''
    raise AssertionError("Huaxin import 不得编译或 dlopen")

class GuardedSocket(socket.socket):
    '''保留 socket 类型兼容性，同时拒绝 clean-import 发起网络连接。

    参数:
        继承标准库 socket.socket 的构造参数。
    返回:
        仅允许创建但不能 connect/connect_ex 的 socket。
    '''

    def connect(self, *args, **kwargs):
        '''拒绝网络 connect。

        参数:
            args: connect 位置参数，不读取。
            kwargs: connect 关键字参数，不读取。
        返回:
            永不返回。
        '''
        return forbidden(*args, **kwargs)

    def connect_ex(self, *args, **kwargs):
        '''拒绝网络 connect_ex。

        参数:
            args: connect_ex 位置参数，不读取。
            kwargs: connect_ex 关键字参数，不读取。
        返回:
            永不返回。
        '''
        return forbidden(*args, **kwargs)

ctypes.CDLL = forbidden
ctypes.PyDLL = forbidden
ctypes.cdll._dlltype = forbidden
ctypes.pydll._dlltype = forbidden
ctypes.cdll.LoadLibrary = forbidden
ctypes.pydll.LoadLibrary = forbidden
if hasattr(ctypes, "windll"):
    ctypes.windll._dlltype = forbidden
    ctypes.windll.LoadLibrary = forbidden
if hasattr(ctypes, "oledll"):
    ctypes.oledll._dlltype = forbidden
    ctypes.oledll.LoadLibrary = forbidden
subprocess.run = forbidden
subprocess.call = forbidden
subprocess.check_call = forbidden
subprocess.check_output = forbidden
subprocess.Popen = forbidden
os.system = forbidden
os.popen = forbidden
socket.socket = GuardedSocket
socket.create_connection = forbidden
target = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(target))
import bullet_trade
package_file = pathlib.Path(bullet_trade.__file__).resolve()
if target not in package_file.parents:
    raise AssertionError("未从临时 wheel target 导入主包")
import bullet_trade.integrations.huaxin
print("clean-import-ok")
"""
        isolated_home = temporary_root / "home"
        isolated_tmp = temporary_root / "tmp"
        isolated_mpl = temporary_root / "matplotlib"
        isolated_home.mkdir()
        isolated_tmp.mkdir()
        isolated_mpl.mkdir()
        environment = {
            key: os.environ[key]
            for key in ("SYSTEMROOT", "WINDIR", "LANG", "LC_ALL", "LC_CTYPE")
            if key in os.environ
        }
        environment.update(
            {
                "HOME": str(isolated_home),
                "TMPDIR": str(isolated_tmp),
                "MPLCONFIGDIR": str(isolated_mpl),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
            }
        )
        try:
            imported = subprocess.run(
                [str(executable), "-I", "-c", script, str(target)],
                cwd=str(work),
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
                env=environment,
            )
        except (OSError, subprocess.SubprocessError):
            return CleanImportReport(False, python_version, True, False, "CLEAN_IMPORT_FAILED")
        if imported.returncode != 0 or imported.stdout.strip() != "clean-import-ok":
            return CleanImportReport(False, python_version, True, False, "CLEAN_IMPORT_FAILED")
    return CleanImportReport(True, python_version, True, True, "OK")


def aggregate_reports(
    reports: Sequence[ReleaseAuditReport],
    clean_imports: Sequence[CleanImportReport] = (),
) -> Mapping[str, Any]:
    """
    汇总多个制品报告和可选 clean-import 证据，形成发布脚本顶层 JSON。

    参数:
        reports: Git/wheel/sdist/bundle 审计报告。
        clean_imports: 可选的当前解释器离线导入结果。
    返回:
        不含源绝对路径的顶层发布审计对象。
    """

    passed = (
        bool(reports)
        and all(report.passed for report in reports)
        and all(item.passed for item in clean_imports)
    )
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "passed": passed,
        "reports": [report.to_dict() for report in reports],
        "clean_imports": [item.to_dict() for item in clean_imports],
        "limitations": [
            "当前解释器 clean-import 不替代 Python 3.8-3.12/Linux/macOS/Windows CI 矩阵",
            "clean-import 仅对静态通过制品做清空敏感环境的尽力 smoke，不替代 OS 级网络/文件沙箱",
            "无真实 SDK/native 时不会宣称厂商动态依赖或运行 readiness 已验收",
            "bundle 的当前源码/hash/导出/target 检查只证明本地合同自洽，不是可复现构建、签名或供应链 attestation",
            "sdist 只接受单 gzip member、严格 ustar 风格元数据和流式 tar 枚举；默认 setuptools PAX/FNAME 产物会明确失败",
        ],
    }


__all__ = [
    "AUDIT_SCHEMA_VERSION",
    "AuditFinding",
    "CleanImportReport",
    "ReleaseAuditPolicy",
    "ReleaseAuditReport",
    "aggregate_reports",
    "audit_bundle",
    "audit_git_tree",
    "audit_sdist",
    "audit_wheel",
    "clean_import_wheel",
]
