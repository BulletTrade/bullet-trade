"""
作者: BruceLee
文件职责: 显式构建、原子发布、校验和诊断自研华鑫 native bundle。
主要输入: 操作员指定的站点包外 prefix、构建模式和可选 bundle 路径。
主要输出: 内容寻址 bundle、脱敏 manifest、doctor 报告和稳定失败原因。
上游关系: 华鑫 CLI、离线测试和未来本地 Huaxin preflight 显式调用。
下游关系: 包内 native_src/CMakeLists.txt 与 native.py 的显式 loader。
关键环境或配置: 当前仅支持 offline_fake；不读取、复制或加载任何厂商 SDK。
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sysconfig
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ...__version__ import __version__
from .errors import (
    BRIDGE_ARTIFACT_HASH_MISMATCH,
    BRIDGE_BUNDLE_INVALID,
    BRIDGE_BUNDLE_MISSING,
    BUILD_FAILED,
    BUILD_FINGERPRINT_MISMATCH,
    BUILD_PREFIX_UNSAFE,
    BUILD_TOOL_MISSING,
    OFFLINE_FAKE_ONLY,
    SDK_BUILD_NOT_IMPLEMENTED,
    HuaxinBuildError,
    HuaxinBundleError,
    HuaxinError,
)
from .native import ABI_VERSION, FIELD_SET_VERSION, VENDOR_SCHEMA_ID

MANIFEST_SCHEMA_VERSION = 1
SUPPORTED_BUILD_MODE = "offline_fake"
MAX_MANIFEST_BYTES = 1024 * 1024


@dataclass(frozen=True)
class BuildResult:
    """表示一次显式构建得到的内容寻址 bundle。"""

    bundle_path: Path
    manifest_path: Path
    artifact_path: Path
    fingerprint: str
    reused: bool

    def to_dict(self) -> Dict[str, Any]:
        """
        将构建结果转换为 CLI 可序列化字典。

        参数:
            无。
        返回:
            包含路径、指纹和是否复用现有 bundle 的字典。
        """

        return {
            "bundle_path": str(self.bundle_path),
            "manifest_path": str(self.manifest_path),
            "artifact_path": str(self.artifact_path),
            "fingerprint": self.fingerprint,
            "reused": self.reused,
        }


@dataclass(frozen=True)
class DoctorReport:
    """表示不包含敏感配置的华鑫 native 离线诊断结果。"""

    native_ready: bool
    offline_bridge_ready: bool
    bridge_loadable: Optional[bool]
    platform_supported: bool
    source_present: bool
    toolchain_ready: bool
    mode: str
    reason_code: str
    checks: Tuple[Mapping[str, Any], ...]
    bundle_fingerprint: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        将 doctor 结果转换为稳定 JSON 字典。

        参数:
            无。
        返回:
            包含 readiness 分层、原因和脱敏检查项的字典。
        """

        return {
            "native_ready": self.native_ready,
            "offline_bridge_ready": self.offline_bridge_ready,
            "bridge_loadable": self.bridge_loadable,
            "platform_supported": self.platform_supported,
            "source_present": self.source_present,
            "toolchain_ready": self.toolchain_ready,
            "mode": self.mode,
            "reason_code": self.reason_code,
            "checks": [dict(item) for item in self.checks],
            "bundle_fingerprint": self.bundle_fingerprint,
        }


def _native_source_root() -> Path:
    """
    返回包内自研 native 源码根目录。

    参数:
        无。
    返回:
        native_src 的绝对路径。
    """

    return Path(__file__).resolve().with_name("native_src")


def _sha256_file(path: Path) -> str:
    """
    流式计算单个文件的 SHA-256。

    参数:
        path: 需要校验的普通文件路径。
    返回:
        小写十六进制 SHA-256。
    副作用:
        只读打开文件，不修改内容。
    """

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    """
    将字典编码为确定性的 UTF-8 JSON 字节。

    参数:
        value: 仅含 JSON 基础类型的映射。
    返回:
        排序键、无多余空白的 UTF-8 字节。
    """

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _source_snapshot(source_root: Path) -> Dict[str, Any]:
    """
    计算自研 native 源码清单与组合指纹。

    参数:
        source_root: 包含 CMakeLists、include 和 src 的目录。
    返回:
        仅含相对路径、文件哈希和组合 SHA-256 的字典。
    异常:
        HuaxinBuildError: 源码目录不完整或没有允许的源文件。
    """

    allowed_names = {"CMakeLists.txt"}
    allowed_suffixes = {".h", ".hpp", ".c", ".cc", ".cpp", ".cxx"}
    if not source_root.is_dir():
        raise HuaxinBuildError(
            BUILD_FAILED,
            "包内华鑫自研 native 源码目录不存在",
            {"component": "native_src"},
        )
    entries: List[Dict[str, str]] = []
    for path in sorted(source_root.rglob("*")):
        if not path.is_file():
            continue
        if path.name not in allowed_names and path.suffix.lower() not in allowed_suffixes:
            continue
        entries.append(
            {
                "path": path.relative_to(source_root).as_posix(),
                "sha256": _sha256_file(path),
            }
        )
    if not entries:
        raise HuaxinBuildError(
            BUILD_FAILED,
            "包内华鑫自研 native 源码清单为空",
            {"component": "native_src"},
        )
    combined = hashlib.sha256(_canonical_json_bytes({"files": entries})).hexdigest()
    return {"sha256": combined, "files": entries}


def _command_version(command: Sequence[str]) -> str:
    """
    获取构建工具首行版本信息。

    参数:
        command: 不含 shell 展开的命令参数序列。
    返回:
        标准输出或标准错误的首个非空行，失败时返回 unknown。
    副作用:
        启动一次无网络的本地工具版本查询进程。
    """

    try:
        completed = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    output = "\n".join([completed.stdout or "", completed.stderr or ""])
    for line in output.splitlines():
        if line.strip():
            return line.strip()
    return "unknown"


def _find_tool(name: str, alternatives: Sequence[str]) -> Optional[str]:
    """
    按固定候选顺序寻找本地构建工具。

    参数:
        name: 诊断用工具角色名。
        alternatives: 允许的可执行文件名序列。
    返回:
        首个可执行文件的绝对路径，未找到时返回 None。
    """

    del name
    for candidate in alternatives:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def _validate_prefix(prefix: Path) -> Path:
    """
    校验 build prefix 不位于站点包、源码包或危险宽目录。

    参数:
        prefix: 操作员显式指定的构建根目录。
    返回:
        规范化后的绝对路径。
    异常:
        HuaxinBuildError: prefix 指向根、用户目录、site-packages、当前源码包或源码仓。
    """

    resolved = prefix.expanduser().resolve()
    package_root = Path(__file__).resolve().parents[2]
    project_root = package_root.parent
    source_checkout = (project_root / "pyproject.toml").is_file() or (
        project_root / ".git"
    ).exists()
    dangerous = {Path(resolved.anchor), Path.home().resolve(), package_root}
    if resolved in dangerous or package_root in resolved.parents:
        raise HuaxinBuildError(
            BUILD_PREFIX_UNSAFE,
            "build prefix 不能位于根目录、用户目录或已安装源码包内",
            {"path_class": "unsafe_root_or_package"},
        )
    if source_checkout and (resolved == project_root or project_root in resolved.parents):
        raise HuaxinBuildError(
            BUILD_PREFIX_UNSAFE,
            "build prefix 必须位于当前源码仓之外",
            {"path_class": "source_checkout"},
        )
    for key in ("purelib", "platlib"):
        raw_path = sysconfig.get_paths().get(key)
        if not raw_path:
            continue
        site_path = Path(raw_path).expanduser().resolve()
        if resolved == site_path or site_path in resolved.parents:
            raise HuaxinBuildError(
                BUILD_PREFIX_UNSAFE,
                "build prefix 必须位于 site-packages 之外",
                {"path_class": "site_packages"},
            )
    return resolved


def _find_installed_artifact(stage_root: Path) -> Path:
    """
    在 CMake 安装树中寻找唯一自研动态库。

    参数:
        stage_root: 临时安装前缀。
    返回:
        唯一 `.so`、`.dylib` 或 `.dll` 文件。
    异常:
        HuaxinBuildError: 没有或发现多个运行时动态库。
    """

    candidates = sorted(
        path
        for path in stage_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".so", ".dylib", ".dll"}
    )
    if len(candidates) != 1:
        raise HuaxinBuildError(
            BUILD_FAILED,
            "CMake 安装树中的自研动态库数量不符合预期",
            {"artifact_count": len(candidates)},
        )
    return candidates[0]


def _manifest_fingerprint(manifest: Mapping[str, Any]) -> str:
    """
    计算不含 fingerprint 字段的 manifest 内容指纹。

    参数:
        manifest: 待签名或待验证的 manifest。
    返回:
        小写十六进制 SHA-256。
    """

    unsigned = dict(manifest)
    unsigned.pop("fingerprint", None)
    return hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()


def _write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    """
    以确定性格式写入临时 bundle manifest。

    参数:
        path: 临时 bundle 内的 manifest.json 路径。
        manifest: 已包含 fingerprint 的 manifest。
    返回:
        无返回值。
    副作用:
        创建或覆盖临时构建目录内的 JSON 文件。
    """

    path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def build_native_bridge(
    prefix: Path,
    mode: str = SUPPORTED_BUILD_MODE,
    build_type: str = "Release",
    timeout_seconds: int = 300,
) -> BuildResult:
    """
    显式构建并原子发布自研 fake/offline native bridge。

    参数:
        prefix: 站点包外的显式构建根目录。
        mode: 当前必须为 offline_fake；真实 SDK 模式尚未实现。
        build_type: CMake 构建类型，允许 Release、RelWithDebInfo 或 Debug。
        timeout_seconds: 每个 CMake 步骤的正整数超时秒数。
    返回:
        内容寻址 bundle 的 BuildResult。
    副作用:
        仅在 prefix 下创建临时构建目录和最终 bundle；调用本地 CMake/C++ 编译器。
    异常:
        HuaxinBuildError: 模式、工具链、prefix、构建或制品校验失败。
    """

    if mode != SUPPORTED_BUILD_MODE:
        raise HuaxinBuildError(
            SDK_BUILD_NOT_IMPLEMENTED,
            "当前基础切片只实现 offline_fake，不会编译或链接厂商 SDK",
            {"requested_mode": mode},
        )
    if build_type not in {"Release", "RelWithDebInfo", "Debug"}:
        raise ValueError("build_type 仅允许 Release、RelWithDebInfo 或 Debug")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds 必须为正整数")

    resolved_prefix = _validate_prefix(Path(prefix))
    cmake = _find_tool("cmake", ("cmake",))
    compiler = _find_tool("cxx", ("c++", "g++", "clang++", "cl"))
    missing = [name for name, value in (("cmake", cmake), ("cxx", compiler)) if not value]
    if missing:
        raise HuaxinBuildError(
            BUILD_TOOL_MISSING,
            "显式构建缺少本地工具链",
            {"missing": missing},
        )
    assert cmake is not None
    assert compiler is not None

    source_root = _native_source_root()
    source_snapshot = _source_snapshot(source_root)
    resolved_prefix.mkdir(parents=True, exist_ok=True)
    bundles_root = resolved_prefix / "bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(tempfile.mkdtemp(prefix=".huaxin-build-", dir=str(resolved_prefix)))
    build_root = temporary_root / "cmake-build"
    install_root = temporary_root / "cmake-install"
    stage_bundle = temporary_root / "bundle"

    configure_command = [
        cmake,
        "-S",
        str(source_root),
        "-B",
        str(build_root),
        f"-DCMAKE_BUILD_TYPE={build_type}",
        f"-DCMAKE_CXX_COMPILER={compiler}",
    ]
    build_command = [cmake, "--build", str(build_root), "--config", build_type]
    install_command = [
        cmake,
        "--install",
        str(build_root),
        "--config",
        build_type,
        "--prefix",
        str(install_root),
    ]

    try:
        for command in (configure_command, build_command, install_command):
            subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        built_artifact = _find_installed_artifact(install_root)
        library_dir = stage_bundle / "lib"
        library_dir.mkdir(parents=True, exist_ok=True)
        staged_artifact = library_dir / built_artifact.name
        shutil.copy2(built_artifact, staged_artifact)
        artifact_hash = _sha256_file(staged_artifact)

        manifest: Dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": SUPPORTED_BUILD_MODE,
            "distribution": {"name": "bullet-trade", "version": __version__},
            "source": source_snapshot,
            "bridge": {
                "abi_version": ABI_VERSION,
                "vendor_schema_id": VENDOR_SCHEMA_ID,
                "field_set_version": FIELD_SET_VERSION,
                "artifact": staged_artifact.relative_to(stage_bundle).as_posix(),
                "sha256": artifact_hash,
                "vendor_sdk_linked": False,
            },
            "target": {
                "system": platform.system().lower(),
                "machine": platform.machine().lower(),
                "python_abi_independent": True,
            },
            "toolchain": {
                "cmake": _command_version((cmake, "--version")),
                "compiler": _command_version((compiler, "--version")),
                "build_type": build_type,
            },
            "runtime": {
                "inspection_status": "not_inspected",
                "dynamic_dependencies": None,
                "rpath": None,
            },
            "integrity_scope": "self_consistency_not_provenance",
            "vendor_sdk": {"included": False, "status": "not_used"},
        }
        fingerprint = _manifest_fingerprint(manifest)
        manifest["fingerprint"] = {"algorithm": "sha256", "value": fingerprint}
        _write_manifest(stage_bundle / "manifest.json", manifest)

        final_bundle = bundles_root / fingerprint
        reused = final_bundle.exists()
        if reused:
            verify_bundle(final_bundle)
        else:
            os.replace(stage_bundle, final_bundle)
        verified_manifest, verified_artifact = verify_bundle(final_bundle)
        return BuildResult(
            bundle_path=final_bundle,
            manifest_path=final_bundle / "manifest.json",
            artifact_path=verified_artifact,
            fingerprint=str(verified_manifest["fingerprint"]["value"]),
            reused=reused,
        )
    except subprocess.CalledProcessError as exc:
        raise HuaxinBuildError(
            BUILD_FAILED,
            "CMake 显式构建失败",
            {
                "returncode": exc.returncode,
                "stderr_available": bool(exc.stderr),
            },
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise HuaxinBuildError(
            BUILD_FAILED,
            "CMake 显式构建超过受控超时",
            {"timeout_seconds": timeout_seconds},
        ) from exc
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)


def verify_bundle(bundle_path: Path) -> Tuple[Dict[str, Any], Path]:
    """
    在 dlopen 前校验 bundle schema、内容指纹、当前源码和 artifact hash。

    参数:
        bundle_path: 内容寻址 bundle 目录。
    返回:
        已验证 manifest 字典与 artifact 绝对路径。
    副作用:
        只读 manifest、当前自研源码和动态库，不执行 native 代码。
    异常:
        HuaxinBundleError: bundle 缺失、路径越界、指纹/源码/制品不匹配。
    """

    root = Path(bundle_path).expanduser().resolve()
    manifest_path = root / "manifest.json"
    if not root.is_dir() or not manifest_path.is_file() or manifest_path.is_symlink():
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_MISSING,
            "华鑫 native bundle 或 manifest 不存在",
            {"component": "bundle"},
        )
    if manifest_path.stat().st_size > MAX_MANIFEST_BYTES:
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native manifest 超过允许大小",
            {"max_bytes": MAX_MANIFEST_BYTES},
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native manifest 不是有效 UTF-8 JSON",
            {"error_type": type(exc).__name__},
        ) from exc
    if not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native manifest schema 不兼容",
            {"expected_schema": MANIFEST_SCHEMA_VERSION},
        )

    fingerprint = manifest.get("fingerprint")
    expected_fingerprint = fingerprint.get("value") if isinstance(fingerprint, dict) else None
    actual_fingerprint = _manifest_fingerprint(manifest)
    if expected_fingerprint != actual_fingerprint or root.name != actual_fingerprint:
        raise HuaxinBundleError(
            BUILD_FINGERPRINT_MISMATCH,
            "华鑫 native bundle 内容指纹不匹配",
            {"component": "manifest"},
        )

    bridge = manifest.get("bridge")
    if (
        not isinstance(bridge, dict)
        or bridge.get("abi_version") != ABI_VERSION
        or bridge.get("vendor_schema_id") != VENDOR_SCHEMA_ID
        or bridge.get("field_set_version") != FIELD_SET_VERSION
    ):
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native manifest ABI 或 schema 身份不兼容",
            {
                "expected_abi": ABI_VERSION,
                "expected_vendor_schema_id": VENDOR_SCHEMA_ID,
                "expected_field_set_version": FIELD_SET_VERSION,
            },
        )
    artifact_relative = bridge.get("artifact")
    if not isinstance(artifact_relative, str) or not artifact_relative:
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native manifest 缺少 artifact 相对路径",
            {"component": "bridge.artifact"},
        )
    relative_path = Path(artifact_relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native artifact 路径越出 bundle",
            {"component": "bridge.artifact"},
        )
    artifact_candidate = root / relative_path
    if artifact_candidate.is_symlink():
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native artifact 不允许使用符号链接",
            {"component": "bridge.artifact"},
        )
    artifact_path = artifact_candidate.resolve()
    if root not in artifact_path.parents or not artifact_path.is_file():
        raise HuaxinBundleError(
            BRIDGE_BUNDLE_INVALID,
            "华鑫 native artifact 不是 bundle 内普通文件",
            {"component": "bridge.artifact"},
        )
    expected_artifact_hash = bridge.get("sha256")
    if expected_artifact_hash != _sha256_file(artifact_path):
        raise HuaxinBundleError(
            BRIDGE_ARTIFACT_HASH_MISMATCH,
            "华鑫 native artifact SHA-256 不匹配",
            {"component": "bridge.artifact"},
        )

    current_source = _source_snapshot(_native_source_root())
    manifest_source = manifest.get("source")
    if (
        not isinstance(manifest_source, dict)
        or manifest_source.get("sha256") != current_source["sha256"]
    ):
        raise HuaxinBundleError(
            BUILD_FINGERPRINT_MISMATCH,
            "当前自研 bridge 源码与 bundle 构建指纹不一致",
            {"component": "source"},
        )
    distribution = manifest.get("distribution")
    if not isinstance(distribution, dict) or distribution.get("version") != __version__:
        raise HuaxinBundleError(
            BUILD_FINGERPRINT_MISMATCH,
            "当前 BulletTrade 版本与 bundle 构建版本不一致",
            {"component": "distribution"},
        )
    return manifest, artifact_path


def _production_platform_supported() -> bool:
    """
    判断当前平台是否属于未来真实 TORA bridge 的 Linux x86_64 基线。

    参数:
        无。
    返回:
        Linux 且机器架构为 x86_64/amd64 时返回 True。
    """

    machine = platform.machine().lower()
    return platform.system().lower() == "linux" and machine in {"x86_64", "amd64"}


def doctor(bundle_path: Optional[Path] = None, load: bool = False) -> DoctorReport:
    """
    执行不连接柜台、不调用 Create 的华鑫 native 离线诊断。

    参数:
        bundle_path: 可选的内容寻址 bundle；缺省仅检查源码和工具链。
        load: 是否在完整性校验后显式 dlopen 并读取 ABI/version；仍不创建 runtime。
    返回:
        分离 production native_ready 与 offline_bridge_ready 的 DoctorReport。
    副作用:
        默认只读文件和查找工具；load=True 时显式 dlopen 自研 fake bridge。
    """

    source_present = _native_source_root().is_dir()
    cmake = _find_tool("cmake", ("cmake",))
    compiler = _find_tool("cxx", ("c++", "g++", "clang++", "cl"))
    toolchain_ready = bool(cmake and compiler)
    platform_supported = _production_platform_supported()
    checks: List[Mapping[str, Any]] = [
        {"name": "source_present", "ok": source_present},
        {"name": "cmake_available", "ok": bool(cmake)},
        {"name": "compiler_available", "ok": bool(compiler)},
        {"name": "production_platform", "ok": platform_supported},
        {"name": "vendor_sdk", "ok": False, "status": "not_checked_by_offline_slice"},
    ]
    if bundle_path is None:
        checks.append({"name": "bundle_present", "ok": False})
        return DoctorReport(
            native_ready=False,
            offline_bridge_ready=False,
            bridge_loadable=None,
            platform_supported=platform_supported,
            source_present=source_present,
            toolchain_ready=toolchain_ready,
            mode=SUPPORTED_BUILD_MODE,
            reason_code=BRIDGE_BUNDLE_MISSING,
            checks=tuple(checks),
        )

    try:
        manifest, _artifact_path = verify_bundle(Path(bundle_path))
        checks.append({"name": "bundle_integrity", "ok": True})
        bridge_loadable: Optional[bool] = None
        if load:
            from .native import NativeBridge

            bridge = NativeBridge.load(Path(bundle_path))
            checks.append(
                {
                    "name": "bridge_load",
                    "ok": bridge.abi_version() == ABI_VERSION,
                    "version": bridge.bridge_version(),
                }
            )
            bridge_loadable = True
        return DoctorReport(
            native_ready=False,
            offline_bridge_ready=True,
            bridge_loadable=bridge_loadable,
            platform_supported=platform_supported,
            source_present=source_present,
            toolchain_ready=toolchain_ready,
            mode=str(manifest.get("mode", SUPPORTED_BUILD_MODE)),
            reason_code=OFFLINE_FAKE_ONLY,
            checks=tuple(checks),
            bundle_fingerprint=str(manifest["fingerprint"]["value"]),
        )
    except HuaxinError as exc:
        checks.append({"name": "bundle_integrity", "ok": False, "code": exc.code})
        return DoctorReport(
            native_ready=False,
            offline_bridge_ready=False,
            bridge_loadable=False if load else None,
            platform_supported=platform_supported,
            source_present=source_present,
            toolchain_ready=toolchain_ready,
            mode=SUPPORTED_BUILD_MODE,
            reason_code=exc.code,
            checks=tuple(checks),
        )
