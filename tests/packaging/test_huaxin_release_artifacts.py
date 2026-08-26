"""
作者: BruceLee
文件职责: 用当前工作树真实构建 universal wheel/sdist，并验证 wheel 可用及含身份元数据的 sdist 被拒绝。
主要输入: 本地 pyproject/build backend、pytest 临时输出目录和当前源码工作树。
主要输出: 真实归档文件名、标签、大小、RECORD/SBOM 审计和无 SDK import 断言。
上游关系: `python -m build --no-isolation` 与 release_audit.py 的公开函数。
下游关系: 不联网、不构建/加载华鑫 native、不连接柜台、不触发交易。
关键环境或配置: 只验证当前 Python；Python 3.8-3.12 与跨 OS 矩阵仍由后续 CI 完成。
"""

from __future__ import annotations

import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from bullet_trade.__version__ import __version__
from bullet_trade.integrations.huaxin.release_audit import (
    audit_sdist,
    audit_wheel,
    clean_import_wheel,
)


@pytest.mark.integration
def test_current_wheel_passes_and_noncanonical_sdist_fails_closed(tmp_path: Path) -> None:
    """
    构建真实 wheel/sdist，验证 wheel 可用且 setuptools 非规范 sdist 元数据被精确拒绝。

    参数:
        tmp_path: pytest 提供的隔离构建输出目录。
    返回:
        无；wheel/clean-import 通过且 sdist 只因不透明身份元数据 fail closed 即通过。
    副作用:
        调用本地 build backend 并在 tmp_path 写入两个发布归档及临时安装内容。
    """

    project_root = Path(__file__).resolve().parents[2]
    dist_dir = tmp_path / "dist"
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
    assert completed.returncode == 0, "本地 --no-isolation 构建失败"

    wheels = sorted(dist_dir.glob("*.whl"))
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1

    wheel_report = audit_wheel(wheels[0])
    sdist_report = audit_sdist(sdists[0])
    clean_import = clean_import_wheel(wheels[0])

    assert wheels[0].name.endswith("-py3-none-any.whl")
    assert wheel_report.passed is True, [finding.to_dict() for finding in wheel_report.findings]
    assert sdist_report.passed is False
    assert {finding.code for finding in sdist_report.findings} == {
        "GZIP_FNAME_FORBIDDEN",
        "TAR_EXTENDED_HEADER_FORBIDDEN",
        "TAR_IDENTITY_METADATA_FORBIDDEN",
    }
    assert clean_import.passed is True, clean_import.to_dict()
    assert wheel_report.archive_size == wheels[0].stat().st_size
    assert sdist_report.archive_size == sdists[0].stat().st_size
    assert wheel_report.metadata["metadata_tags"] == ["py3-none-any"]
    assert any(
        item["path"] == "bullet_trade/integrations/huaxin/release_audit.py"
        for item in wheel_report.sbom
    )
    with tarfile.open(sdists[0], "r:gz") as archive:
        sdist_names = set(archive.getnames())
    sdist_root = "bullet_trade-" + __version__
    assert sdist_root + "/bullet_trade/integrations/huaxin/release_audit.py" in sdist_names
    assert sdist_root + "/scripts/audit_huaxin_release_offline.py" not in sdist_names
    assert sdist_root + "/tests/unit/integrations/huaxin/test_release_audit.py" not in sdist_names
    assert sdist_root + "/tests/packaging/test_huaxin_release_artifacts.py" not in sdist_names
    serialized = json.dumps(
        {
            "wheel": wheel_report.to_dict(),
            "sdist": sdist_report.to_dict(),
            "clean_import": clean_import.to_dict(),
        },
        ensure_ascii=False,
    )
    assert str(tmp_path) not in serialized
