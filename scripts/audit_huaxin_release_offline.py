#!/usr/bin/env python3
"""
作者: BruceLee
文件职责: 构建或接收本地 BulletTrade 发布制品，规范化 sdist，并执行华鑫发布边界的纯离线审计。
主要输入: Git 根目录、wheel/sdist/native bundle 路径及可选 `--build --no-isolation` 动作。
主要输出: 不含绝对本地路径和原始敏感值的 JSON 审计报告与稳定退出码。
上游关系: 开发者、发布人员和未来 CI 在发布前显式运行本脚本。
下游关系: bullet_trade.integrations.huaxin.release_audit 的 Git/归档/bundle 审计函数。
关键环境或配置: 不联网、不加载 SDK、不交易；构建只使用已安装的本地 build backend，并会原子替换本次构建的 sdist。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any, List, Mapping, Optional, Sequence, Tuple

from bullet_trade.integrations.huaxin.release_audit import (
    CleanImportReport,
    ReleaseAuditReport,
    aggregate_reports,
    audit_bundle,
    audit_git_tree,
    audit_sdist,
    audit_wheel,
    canonicalize_sdist,
    clean_import_wheel,
)


def _default_project_root() -> Path:
    """
    返回脚本所在 bullet-trade 源码仓根目录。

    参数:
        无。
    返回:
        包含 pyproject.toml 的绝对路径。
    """

    return Path(__file__).resolve().parents[1]


def _parser() -> argparse.ArgumentParser:
    """
    创建离线发布审计命令行解析器。

    参数:
        无。
    返回:
        已声明构建、制品、输出和 clean-import 参数的解析器。
    """

    parser = argparse.ArgumentParser(
        description="纯离线审计 BulletTrade Git tree、wheel、sdist 与华鑫 native bundle",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=_default_project_root(),
        help="bullet-trade Git 根目录，默认自动定位",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="先执行 python -m build --no-isolation；不会联网安装构建依赖",
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        help="--build 的输出目录；省略时使用并自动清理临时目录",
    )
    parser.add_argument("--wheel", action="append", type=Path, default=[], help="审计本地 wheel")
    parser.add_argument("--sdist", action="append", type=Path, default=[], help="审计本地 sdist")
    parser.add_argument("--bundle", action="append", type=Path, default=[], help="审计 native bundle")
    parser.add_argument("--skip-git", action="store_true", help="不审计受跟踪 Git tree")
    parser.add_argument(
        "--clean-import",
        action="store_true",
        help="对每个 wheel 做当前解释器离线 target 安装/import 尽力验证",
    )
    parser.add_argument("--output", type=Path, help="可选 JSON 输出文件；报告不含制品绝对路径")
    parser.add_argument("--compact", action="store_true", help="输出紧凑 JSON")
    return parser


def _build_distributions(project_root: Path, dist_dir: Path) -> Tuple[bool, int]:
    """
    使用当前解释器和已安装 backend 构建 wheel/sdist，不创建隔离下载环境。

    参数:
        project_root: 含 pyproject.toml 的源码仓。
        dist_dir: 本地构建输出目录。
    返回:
        ``(是否成功, 子进程返回码)``。
    副作用:
        在 dist_dir 写入本项目 wheel/sdist，并可能刷新 setuptools 的本地构建缓存。
    """

    dist_dir.mkdir(parents=True, exist_ok=True)
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--no-isolation",
                "--outdir",
                str(dist_dir),
            ],
            cwd=str(project_root),
            check=False,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (OSError, subprocess.SubprocessError):
        return False, -1
    if completed.returncode != 0:
        return False, completed.returncode
    try:
        for sdist in sorted(dist_dir.glob("*.tar.gz")):
            canonicalize_sdist(sdist)
    except (OSError, ValueError, tarfile.TarError):
        return False, -1
    return True, 0


def _discover_built_artifacts(dist_dir: Path) -> Tuple[List[Path], List[Path]]:
    """
    从显式构建目录中按扩展名发现 wheel 和 gzip sdist。

    参数:
        dist_dir: 仅包含本轮构建输出的目录。
    返回:
        按文件名排序的 wheel、sdist 路径列表。
    """

    wheels = sorted(path for path in dist_dir.glob("*.whl") if path.is_file())
    sdists = sorted(path for path in dist_dir.glob("*.tar.gz") if path.is_file())
    return wheels, sdists


def _write_report(payload: object, output: Optional[Path], compact: bool) -> str:
    """
    将汇总对象序列化为 UTF-8 JSON，并可显式写入用户指定文件。

    参数:
        payload: aggregate_reports 或受控构建失败对象。
        output: 可选输出文件。
        compact: 是否省略缩进和多余空白。
    返回:
        完整 JSON 文本。
    副作用:
        output 存在时创建父目录并覆盖该单个显式目标文件。
    """

    if compact:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    else:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    serialized += "\n"
    if output is not None:
        resolved = output.expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(serialized, encoding="utf-8")
    return serialized


def _audit_requested(
    project_root: Path,
    wheels: Sequence[Path],
    sdists: Sequence[Path],
    bundles: Sequence[Path],
    skip_git: bool,
    run_clean_import: bool,
) -> Mapping[str, Any]:
    """
    执行请求范围内的 Git、归档、bundle 和可选 clean-import 审计。

    参数:
        project_root: bullet-trade Git 根目录。
        wheels: 本地 wheel 路径。
        sdists: 本地 sdist 路径。
        bundles: 本地 native bundle 目录。
        skip_git: 是否跳过 Git tree。
        run_clean_import: 是否执行当前解释器离线导入。
    返回:
        aggregate_reports 生成的顶层脱敏对象。
    """

    reports: List[ReleaseAuditReport] = []
    clean_imports: List[CleanImportReport] = []
    if run_clean_import and not wheels:
        clean_imports.append(
            CleanImportReport(False, "unknown", False, False, "CLEAN_IMPORT_SCOPE_EMPTY")
        )
    if not skip_git:
        reports.append(audit_git_tree(project_root))
    for wheel in wheels:
        wheel_report = audit_wheel(wheel)
        reports.append(wheel_report)
        if run_clean_import and wheel_report.passed:
            clean_imports.append(clean_import_wheel(wheel))
        elif run_clean_import:
            clean_imports.append(
                CleanImportReport(False, "unknown", False, False, "STATIC_AUDIT_FAILED")
            )
    for sdist in sdists:
        reports.append(audit_sdist(sdist))
    for bundle in bundles:
        reports.append(audit_bundle(bundle))
    return aggregate_reports(reports, clean_imports)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    解析参数、可选离线构建、执行审计并按通过状态返回退出码。

    参数:
        argv: 可选参数序列；缺省读取进程命令行。
    返回:
        全部通过为 0，审计失败为 2，离线构建失败为 3。
    """

    arguments = _parser().parse_args(argv)
    project_root = arguments.project_root.expanduser().resolve()
    payload: Mapping[str, Any]
    if not (project_root / "pyproject.toml").is_file():
        payload = {
            "schema_version": 1,
            "passed": False,
            "error": {"code": "PROJECT_ROOT_INVALID"},
        }
        sys.stdout.write(_write_report(payload, arguments.output, arguments.compact))
        return 3

    temporary: Optional[tempfile.TemporaryDirectory[str]] = None
    try:
        wheels = list(arguments.wheel)
        sdists = list(arguments.sdist)
        if arguments.build:
            if arguments.dist_dir is None:
                temporary = tempfile.TemporaryDirectory(prefix="bt-release-dist-")
                dist_dir = Path(temporary.name)
            else:
                dist_dir = arguments.dist_dir.expanduser().resolve()
                existing_wheels, existing_sdists = _discover_built_artifacts(dist_dir)
                if existing_wheels or existing_sdists:
                    payload = {
                        "schema_version": 1,
                        "passed": False,
                        "error": {"code": "BUILD_OUTPUT_NOT_EMPTY"},
                    }
                    sys.stdout.write(_write_report(payload, arguments.output, arguments.compact))
                    return 3
            build_ok, returncode = _build_distributions(project_root, dist_dir)
            if not build_ok:
                payload = {
                    "schema_version": 1,
                    "passed": False,
                    "error": {"code": "OFFLINE_BUILD_FAILED", "returncode": returncode},
                }
                sys.stdout.write(_write_report(payload, arguments.output, arguments.compact))
                return 3
            built_wheels, built_sdists = _discover_built_artifacts(dist_dir)
            if len(built_wheels) != 1 or len(built_sdists) != 1:
                payload = {
                    "schema_version": 1,
                    "passed": False,
                    "error": {
                        "code": "BUILD_ARTIFACT_SET_INVALID",
                        "wheel_count": len(built_wheels),
                        "sdist_count": len(built_sdists),
                    },
                }
                sys.stdout.write(_write_report(payload, arguments.output, arguments.compact))
                return 3
            wheels.extend(built_wheels)
            sdists.extend(built_sdists)

        payload = _audit_requested(
            project_root=project_root,
            wheels=wheels,
            sdists=sdists,
            bundles=arguments.bundle,
            skip_git=arguments.skip_git,
            run_clean_import=arguments.clean_import,
        )
        sys.stdout.write(_write_report(payload, arguments.output, arguments.compact))
        return 0 if bool(payload.get("passed")) else 2
    finally:
        if temporary is not None:
            temporary.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
