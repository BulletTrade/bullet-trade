"""
作者: BruceLee
文件职责: 验证华鑫发布审计对归档穿越、RECORD、native、敏感值和 bundle 工具门禁 fail closed。
主要输入: pytest 临时目录中生成的最小 wheel、sdist、bundle 与当前离线 fake bundle。
主要输出: pytest 断言，证明审计报告稳定、脱敏且纯 Python 制品不依赖 native 工具。
上游关系: release_audit.py 的公开审计函数和现有 offline_bundle fixture。
下游关系: 不联网、不加载厂商 SDK、不连接柜台、不执行任何交易写入。
关键环境或配置: 合成 native 只含最小魔数；除现有 fake bundle 用例外不执行真实二进制。
"""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import types
import zipfile
from pathlib import Path
from typing import Dict, Mapping, Optional

import pytest

from bullet_trade.integrations.huaxin import BuildResult, release_audit
from bullet_trade.integrations.huaxin.release_audit import (
    ReleaseAuditPolicy,
    audit_bundle,
    audit_sdist,
    audit_wheel,
    canonicalize_sdist,
)


def _required_distribution_files() -> Dict[str, bytes]:
    """
    返回能证明同 distribution 第一方边界的最小文件映射。

    参数:
        无。
    返回:
        wheel 根相对路径到无敏感测试字节的映射。
    """

    return {
        "bullet_trade/integrations/huaxin/__init__.py": b"",
        "bullet_trade/integrations/huaxin/build.py": b"# first party\n",
        "bullet_trade/integrations/huaxin/native.py": b"# wrapper\n",
        "bullet_trade/integrations/huaxin/native_src/CMakeLists.txt": b"project(test)\n",
        "bullet_trade/integrations/huaxin/native_src/include/bt_huaxin_bridge.h": (
            b"#define BT_HUAXIN_ABI_VERSION 2\n"
        ),
        "bullet_trade/integrations/huaxin/native_src/src/bt_huaxin_bridge.cpp": (
            b'extern "C" int bt_test(void) { return 0; }\n'
        ),
    }


def _record_bytes(files: Mapping[str, bytes], record_path: str) -> bytes:
    """
    为最小 wheel 生成符合规范的 SHA-256 RECORD。

    参数:
        files: RECORD 之外的 wheel 文件。
        record_path: `.dist-info/RECORD` 相对路径。
    返回:
        UTF-8 CSV 字节。
    """

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    for path, data in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow((path, "sha256=" + digest, str(len(data))))
    writer.writerow((record_path, "", ""))
    return output.getvalue().encode("utf-8")


def _write_wheel(
    directory: Path,
    tag: str = "py3-none-any",
    extra_files: Optional[Mapping[str, bytes]] = None,
    tamper_after_record: Optional[str] = None,
    metadata: Optional[bytes] = None,
    extra_wheel_tags: tuple = (),
    wheel_version: bytes = b"1.0",
) -> Path:
    """
    生成带完整 METADATA/WHEEL/RECORD 的最小 bullet-trade wheel。

    参数:
        directory: 输出临时目录。
        tag: 文件名与 WHEEL 使用的三元标签。
        extra_files: 可选额外文件。
        tamper_after_record: 可选在 RECORD 生成后篡改的路径。
        metadata: 可选覆盖的完整 METADATA 字节。
        extra_wheel_tags: 可选加入 WHEEL 的额外 Tag，用于验证标签集合拒绝。
        wheel_version: WHEEL 元数据中的 Wheel-Version 测试值。
    返回:
        合成 wheel 路径。
    """

    dist_info = "bullet_trade-0.0.0.dist-info"
    files = _required_distribution_files()
    files.update(extra_files or {})
    files[dist_info + "/METADATA"] = metadata or (
        b"Metadata-Version: 2.1\nName: bullet-trade\nVersion: 0.0.0\n"
    )
    pure = b"true" if tag.endswith("-any") else b"false"
    python_tag, abi_tag, platform_tag = tag.split("-", 2)
    expanded_tags = tuple(
        "{}-{}-{}".format(python_value, abi_value, platform_value)
        for python_value in python_tag.split(".")
        for abi_value in abi_tag.split(".")
        for platform_value in platform_tag.split(".")
    ) + tuple(extra_wheel_tags)
    tag_lines = b"".join(b"Tag: " + value.encode() + b"\n" for value in expanded_tags)
    files[dist_info + "/WHEEL"] = (
        b"Wheel-Version: " + wheel_version + b"\nRoot-Is-Purelib: " + pure + b"\n" + tag_lines
    )
    record_path = dist_info + "/RECORD"
    record = _record_bytes(files, record_path)
    if tamper_after_record is not None:
        files[tamper_after_record] = files[tamper_after_record] + b"tampered"
    files[record_path] = record
    wheel = directory / ("bullet_trade-0.0.0-" + tag + ".whl")
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, data in files.items():
            archive.writestr(path, data)
    return wheel


def _fake_elf_x86_64(include_bridge_symbols: bool = True) -> bytes:
    """
    构造只用于静态平台合同测试的最小 ELF64 x86_64 头部与可选 ABI 标记。

    参数:
        include_bridge_symbols: 是否附加当前 flat C ABI 必要符号字节。
    返回:
        不可执行的确定性测试字节。
    """

    header = bytearray(136)
    header[:4] = b"\x7fELF"
    header[4] = 2
    header[5] = 1
    header[6] = 1
    header[16:18] = (3).to_bytes(2, byteorder="little")
    header[18:20] = (0x3E).to_bytes(2, byteorder="little")
    header[20:24] = (1).to_bytes(4, byteorder="little")
    header[32:40] = (64).to_bytes(8, byteorder="little")
    header[52:54] = (64).to_bytes(2, byteorder="little")
    header[54:56] = (56).to_bytes(2, byteorder="little")
    header[56:58] = (1).to_bytes(2, byteorder="little")
    header[64:68] = (2).to_bytes(4, byteorder="little")
    header[72:80] = (120).to_bytes(8, byteorder="little")
    header[96:104] = (16).to_bytes(8, byteorder="little")
    header[104:112] = (16).to_bytes(8, byteorder="little")
    symbols = b"\x00".join(release_audit._EXPECTED_BRIDGE_SYMBOLS)
    return bytes(header) + (symbols if include_bridge_symbols else b"")


def _fake_pe_x86_64(include_bridge_symbols: bool = True) -> bytes:
    """
    构造只用于静态平台合同测试的最小 PE32+ x86_64 头部与可选 ABI 标记。

    参数:
        include_bridge_symbols: 是否附加当前 flat C ABI 必要符号字节。
    返回:
        不可执行的确定性测试字节。
    """

    header = bytearray(128)
    header[:2] = b"MZ"
    header[60:64] = (64).to_bytes(4, byteorder="little")
    header[64:68] = b"PE\x00\x00"
    header[68:70] = (0x8664).to_bytes(2, byteorder="little")
    header[86:88] = (0x2000).to_bytes(2, byteorder="little")
    header[88:90] = (0x20B).to_bytes(2, byteorder="little")
    symbols = b"\x00".join(release_audit._EXPECTED_BRIDGE_SYMBOLS)
    return bytes(header) + (symbols if include_bridge_symbols else b"")


def _fake_macho_x86_64(file_type: int = 6) -> bytes:
    """
    构造用于 Mach-O 映像类型合同的最小 64 位小端头部。

    参数:
        file_type: Mach-O filetype；6 表示 MH_DYLIB。
    返回:
        含 x86_64 CPU 与指定 filetype 的 32 字节头部。
    """

    header = bytearray(32)
    header[:4] = b"\xcf\xfa\xed\xfe"
    header[4:8] = (0x01000007).to_bytes(4, "little")
    header[12:16] = file_type.to_bytes(4, "little")
    return bytes(header)


def _fake_pe_bridge_exports(
    executable: bool = True,
    forwarder: bool = False,
) -> bytes:
    """
    构造带六个命名导出的最小 PE32+ DLL，目标可切换到代码、数据或 forwarder。

    参数:
        executable: 非 forwarder 时导出 RVA 是否落在可执行 `.text` section。
        forwarder: 是否把函数 RVA 指向 export directory 内的 forwarder 字符串。
    返回:
        可由原始 PE export table 解析器验证的确定性字节。
    """

    pe_offset = 0x80
    optional_size = 0xF0
    section_offset = pe_offset + 24 + optional_size
    data = bytearray(0xA00)
    data[:2] = b"MZ"
    data[60:64] = pe_offset.to_bytes(4, "little")
    data[pe_offset : pe_offset + 4] = b"PE\x00\x00"
    data[pe_offset + 4 : pe_offset + 6] = (0x8664).to_bytes(2, "little")
    data[pe_offset + 6 : pe_offset + 8] = (3).to_bytes(2, "little")
    data[pe_offset + 20 : pe_offset + 22] = optional_size.to_bytes(2, "little")
    data[pe_offset + 22 : pe_offset + 24] = (0x2000).to_bytes(2, "little")
    optional_offset = pe_offset + 24
    data[optional_offset : optional_offset + 2] = (0x20B).to_bytes(2, "little")
    data[optional_offset + 108 : optional_offset + 112] = (16).to_bytes(4, "little")
    data[optional_offset + 112 : optional_offset + 116] = (0x1000).to_bytes(4, "little")
    data[optional_offset + 116 : optional_offset + 120] = (0x200).to_bytes(4, "little")
    sections = (
        (b".edata\x00\x00", 0x400, 0x1000, 0x400, 0x200, 0x40000040),
        (b".text\x00\x00\x00", 0x200, 0x2000, 0x200, 0x600, 0x60000020),
        (b".data\x00\x00\x00", 0x200, 0x3000, 0x200, 0x800, 0xC0000040),
    )
    for index, (name, virtual_size, virtual_address, raw_size, raw_offset, flags) in enumerate(
        sections
    ):
        offset = section_offset + index * 40
        data[offset : offset + 8] = name
        data[offset + 8 : offset + 12] = virtual_size.to_bytes(4, "little")
        data[offset + 12 : offset + 16] = virtual_address.to_bytes(4, "little")
        data[offset + 16 : offset + 20] = raw_size.to_bytes(4, "little")
        data[offset + 20 : offset + 24] = raw_offset.to_bytes(4, "little")
        data[offset + 36 : offset + 40] = flags.to_bytes(4, "little")
    export_offset = 0x200
    symbol_count = len(release_audit._EXPECTED_BRIDGE_SYMBOLS)
    data[export_offset + 20 : export_offset + 24] = symbol_count.to_bytes(4, "little")
    data[export_offset + 24 : export_offset + 28] = symbol_count.to_bytes(4, "little")
    data[export_offset + 28 : export_offset + 32] = (0x1040).to_bytes(4, "little")
    data[export_offset + 32 : export_offset + 36] = (0x1060).to_bytes(4, "little")
    data[export_offset + 36 : export_offset + 40] = (0x1080).to_bytes(4, "little")
    string_rva = 0x1100
    for index, raw_symbol in enumerate(release_audit._EXPECTED_BRIDGE_SYMBOLS):
        function_rva = 0x2010 + index
        if forwarder:
            function_rva = 0x11F0
        elif not executable:
            function_rva = 0x3010 + index
        data[0x240 + index * 4 : 0x244 + index * 4] = function_rva.to_bytes(4, "little")
        data[0x260 + index * 4 : 0x264 + index * 4] = string_rva.to_bytes(4, "little")
        data[0x280 + index * 2 : 0x282 + index * 2] = index.to_bytes(2, "little")
        string_offset = 0x200 + (string_rva - 0x1000)
        data[string_offset : string_offset + len(raw_symbol) + 1] = raw_symbol + b"\x00"
        string_rva += len(raw_symbol) + 1
    data[0x3F0:0x3FA] = b"other.func\x00"
    return bytes(data)


def _write_sdist(
    directory: Path,
    extra_files: Optional[Mapping[str, bytes]] = None,
    symlink: bool = False,
    tar_variant: Optional[str] = None,
    gzip_filename: str = "",
) -> Path:
    """
    生成包含第一方 Huaxin 源码边界的最小 gzip sdist。

    参数:
        directory: 输出临时目录。
        extra_files: 可选额外文件。
        symlink: 是否加入一个必须被拒绝的符号链接条目。
        tar_variant: 可选 pax、identity 或 regular-slash 结构反例。
        gzip_filename: gzip header 中的可选 FNAME 反例；默认严格为空。
    返回:
        合成 sdist 路径。
    """

    root = "bullet_trade-0.0.0"
    files = {root + "/" + path: data for path, data in _required_distribution_files().items()}
    files[root + "/pyproject.toml"] = b'[project]\nname = "bullet-trade"\nversion = "0.0.0"\n'
    files[root + "/PKG-INFO"] = b"Metadata-Version: 2.1\nName: bullet-trade\nVersion: 0.0.0\n"
    for path, data in (extra_files or {}).items():
        files[root + "/" + path] = data
    sdist = directory / "bullet_trade-0.0.0.tar.gz"
    tar_payload = io.BytesIO()
    tar_format = tarfile.PAX_FORMAT if tar_variant == "pax" else tarfile.USTAR_FORMAT
    with tarfile.open(fileobj=tar_payload, mode="w:", format=tar_format) as archive:
        for index, (path, data) in enumerate(files.items()):
            info = tarfile.TarInfo(path)
            info.size = len(data)
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            if tar_variant == "pax" and index == 0:
                info.pax_headers = {"comment": "synthetic-metadata"}
            if tar_variant == "identity":
                info.uname = "synthetic-user"
                info.gname = "synthetic-group"
            archive.addfile(info, io.BytesIO(data))
        if tar_variant == "regular-slash":
            info = tarfile.TarInfo(root + "/ambiguous-regular/")
            info.type = tarfile.REGTYPE
            info.size = 1
            info.mtime = 0
            archive.addfile(info, io.BytesIO(b"x"))
        if symlink:
            info = tarfile.TarInfo(root + "/unsafe-link")
            info.type = tarfile.SYMTYPE
            info.linkname = "../../outside"
            info.mtime = 0
            archive.addfile(info)
    gzip_payload = io.BytesIO()
    with gzip.GzipFile(
        fileobj=gzip_payload,
        mode="wb",
        filename=gzip_filename,
        mtime=0,
    ) as stream:
        stream.write(tar_payload.getvalue())
    sdist.write_bytes(gzip_payload.getvalue())
    return sdist


def _finding_codes(report: release_audit.ReleaseAuditReport) -> set:
    """
    提取单个报告的稳定规则码集合。

    参数:
        report: 待检查审计报告。
    返回:
        发现项 code 集合。
    """

    return {finding.code for finding in report.findings}


def _copy_offline_bundle(
    offline_bundle: BuildResult,
    directory: Path,
    name: Optional[str] = None,
) -> Path:
    """
    将真实 offline_fake bundle 复制到单测隔离目录且保持内容寻址 basename。

    参数:
        offline_bundle: 会话级本机 CMake 构建结果。
        directory: pytest 临时目标父目录。
        name: 可选覆盖的目标 basename，用于验证名称脱敏。
    返回:
        复制后的 bundle 根目录。
    副作用:
        只在 pytest 临时目录复制现有 fake bundle。
    """

    target = directory / (name or offline_bundle.fingerprint)
    shutil.copytree(offline_bundle.bundle_path, target)
    return target


def _resign_bundle_manifest(bundle: Path, manifest: Dict[str, object]) -> Path:
    """
    仅为负向审计测试重算自洽指纹并同步内容寻址目录名。

    参数:
        bundle: pytest 临时目录中的 offline_fake bundle 副本。
        manifest: 已完成合成篡改的 manifest 对象。
    返回:
        重命名后的 64 位十六进制内容寻址 bundle 根目录。
    副作用:
        覆盖临时 manifest 并重命名临时 bundle；不修改真实构建结果。
    """

    unsigned = dict(manifest)
    unsigned.pop("fingerprint", None)
    fingerprint = hashlib.sha256(
        json.dumps(
            unsigned,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    manifest["fingerprint"] = {"algorithm": "sha256", "value": fingerprint}
    manifest_path = bundle / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    destination = bundle.with_name(fingerprint)
    bundle.rename(destination)
    return destination


def _load_release_audit_cli() -> types.ModuleType:
    """
    从源码仓脚本路径加载离线发布审计 CLI，供无子进程单测注入构建结果。

    参数:
        无。
    返回:
        已执行模块顶层定义、但未触发 main 的临时模块对象。
    异常:
        脚本路径不可加载时抛出 AssertionError，避免测试静默跳过 CLI 合同。
    """

    project_root = Path(__file__).resolve().parents[4]
    script_path = project_root / "scripts" / "audit_huaxin_release_offline.py"
    spec = importlib.util.spec_from_file_location("_bt_huaxin_release_audit_cli_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
def test_universal_wheel_passes_without_native_inspection_tool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    验证纯 Python py3-none-any wheel 无需 readelf/otool/objdump 也能通过。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 用于模拟所有 native 工具缺失。
    返回:
        无；标签、RECORD、SBOM 与工具调用边界正确即通过。
    """

    wheel = _write_wheel(tmp_path)
    monkeypatch.setattr(release_audit.shutil, "which", lambda _name: None)

    report = audit_wheel(wheel)

    assert report.passed is True
    assert report.artifact_kind == "universal_wheel"
    assert report.metadata["universal"] is True
    assert report.native_inspection == ()
    assert str(tmp_path) not in json.dumps(report.to_dict(), ensure_ascii=False)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variant", "expected_code"),
    (
        ("prefix", "ZIP_PREFIX_OR_GAP_FORBIDDEN"),
        ("tail", "ZIP_TRAILING_DATA_FORBIDDEN"),
        ("archive-comment", "ZIP_ARCHIVE_COMMENT_FORBIDDEN"),
        ("entry-comment", "ZIP_ENTRY_COMMENT_FORBIDDEN"),
        ("entry-extra", "ZIP_ENTRY_EXTRA_FORBIDDEN"),
        ("directory-data", "ARCHIVE_DIRECTORY_DATA_FORBIDDEN"),
    ),
)
def test_wheel_zip_envelope_rejects_hidden_unsigned_regions(
    tmp_path: Path,
    variant: str,
    expected_code: str,
) -> None:
    """
    验证 ZIP 前后缀、注释、extra 和目录数据均不能绕过 wheel 内容清单。

    参数:
        tmp_path: pytest 临时目录。
        variant: 待构造的 ZIP 包络攻击类别。
        expected_code: 对应的固定拒绝规则码。
    返回:
        无；每个变体均 fail closed 且报告不回显注释或尾随载荷。
    """

    case_directory = tmp_path / variant
    case_directory.mkdir()
    wheel = _write_wheel(case_directory)
    if variant == "prefix":
        wheel.write_bytes(b"hidden-prefix" + wheel.read_bytes())
    elif variant == "tail":
        wheel.write_bytes(wheel.read_bytes() + b"hidden-tail")
    elif variant == "archive-comment":
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.comment = b"hidden-archive-comment"
    else:
        info = zipfile.ZipInfo("bullet_trade/unsigned-entry")
        payload = b""
        if variant == "entry-comment":
            info.comment = b"hidden-entry-comment"
        elif variant == "entry-extra":
            info.extra = b"\xfe\xca\x01\x00x"
        elif variant == "directory-data":
            info = zipfile.ZipInfo("bullet_trade/unsigned-directory/")
            info.create_system = 3
            info.external_attr = (stat.S_IFDIR | 0o755) << 16
            payload = b"x"
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr(info, payload)

    report = audit_wheel(wheel)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert expected_code in _finding_codes(report)
    assert "hidden-archive-comment" not in serialized
    assert "hidden-entry-comment" not in serialized
    assert "hidden-prefix" not in serialized
    assert "hidden-tail" not in serialized


@pytest.mark.unit
def test_wheel_metadata_requires_single_dist_info_signature_boundary_and_version(
    tmp_path: Path,
) -> None:
    """
    验证 Wheel-Version、唯一 dist-info 和 RECORD 签名边界均与文件名身份绑定。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；decoy dist-info/签名及不支持的 Wheel-Version 均产生固定拒绝码。
    """

    wheel = _write_wheel(
        tmp_path,
        wheel_version=b"1.1",
        extra_files={
            "decoy-9.9.dist-info/RECORD.jws": b"synthetic-signature",
            "DECOY-9.9.DIST-INFO/payload": b"synthetic-metadata",
            "bullet_trade-0.0.0.dist-info/RECORD.p7s": b"synthetic-signature",
        },
    )

    codes = _finding_codes(audit_wheel(wheel))

    assert "WHEEL_SPEC_VERSION_INVALID" in codes
    assert "WHEEL_DIST_INFO_BOUNDARY_INVALID" in codes
    assert "WHEEL_SIGNATURE_BOUNDARY_INVALID" in codes
    assert "WHEEL_SIGNATURE_RECORD_INVALID" in codes


@pytest.mark.unit
def test_wheel_record_tamper_and_path_traversal_fail_closed(tmp_path: Path) -> None:
    """
    验证 RECORD 内容篡改和 ZIP 父目录条目均产生稳定失败。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；两类供应链攻击均被拒绝即通过。
    """

    synthetic_absolute_member = (
        "/" + "Users" + "/SYNTHETIC_ACCOUNT/HuaXin-SDK/SECRET_MEMBER_92831.py"
    )
    wheel = _write_wheel(
        tmp_path,
        extra_files={
            "../escape.py": b"pass\n",
            synthetic_absolute_member: b"pass\n",
        },
        tamper_after_record="bullet_trade/integrations/huaxin/native.py",
    )
    link = zipfile.ZipInfo("bullet_trade/unsafe-link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(link, "target")

    report = audit_wheel(wheel)
    codes = _finding_codes(report)

    assert report.passed is False
    assert "PATH_TRAVERSAL" in codes
    assert "SYMLINK_NOT_ALLOWED" in codes
    assert "WHEEL_RECORD_HASH_MISMATCH" in codes
    assert "SECRET_MEMBER_92831" not in json.dumps(report.to_dict(), ensure_ascii=False)


@pytest.mark.unit
def test_wheel_identity_and_folded_metadata_are_fail_closed(tmp_path: Path) -> None:
    """
    验证文件名身份、dist-info 根和 RFC822 折行字段不能绕过 distribution 策略。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；折行 huaxin extra/依赖与错误文件名均被拒绝即通过。
    """

    metadata = (
        b"Metadata-Version: 2.1\nName: bullet-trade\nVersion: 0.0.0\n"
        b"Provides-Extra:\n huaxin\nRequires-Dist:\n bullet-trade-huaxin\n"
    )
    wheel = _write_wheel(tmp_path, metadata=metadata)
    folded_codes = _finding_codes(audit_wheel(wheel))
    renamed = wheel.with_name("evil_distribution-999.0-py3-none-any.whl")
    wheel.rename(renamed)
    renamed_codes = _finding_codes(audit_wheel(renamed))

    assert "INDEPENDENT_HUAXIN_EXTRA_FORBIDDEN" in folded_codes
    assert "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN" in folded_codes
    assert "WHEEL_DISTRIBUTION_INVALID" in renamed_codes


@pytest.mark.unit
def test_wheel_rejects_pep503_equivalent_huaxin_dependency(tmp_path: Path) -> None:
    """
    验证独立华鑫 distribution 的点号变体经 PEP 503 规范化后仍被拒绝。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；结构化 Requirement 解析命中固定禁止规则即通过。
    """

    metadata = (
        b"Metadata-Version: 2.1\nName: bullet-trade\nVersion: 0.0.0\n"
        b"Requires-Dist: bullet.trade.huaxin>=1\n"
    )
    wheel = _write_wheel(tmp_path, metadata=metadata)

    codes = _finding_codes(audit_wheel(wheel))

    assert "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN" in codes


@pytest.mark.unit
def test_universal_wheel_rejects_native_magic_even_with_data_suffix(tmp_path: Path) -> None:
    """
    验证 universal wheel 依据 ELF 魔数而不只依赖扩展名拒绝 native。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；伪装成 `.bin` 的 native 被拒绝即通过。
    """

    wheel = _write_wheel(tmp_path, extra_files={"bullet_trade/vendor_payload.bin": b"\x7fELFfake"})

    report = audit_wheel(wheel)

    assert "FORBIDDEN_NATIVE_MAGIC" in _finding_codes(report)


@pytest.mark.unit
def test_universal_wheel_rejects_macho_fat64_magic_with_data_suffix(tmp_path: Path) -> None:
    """
    验证 Mach-O fat64 魔数即使伪装为普通数据后缀也不能进入 universal wheel。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；fat64 与反字节序 fat64 均产生原生魔数拒绝即通过。
    """

    for index, magic in enumerate((b"\xca\xfe\xba\xbf", b"\xbf\xba\xfe\xca")):
        wheel = _write_wheel(
            tmp_path,
            extra_files={"bullet_trade/fat64-{}.bin".format(index): magic + b"synthetic"},
        )

        report = audit_wheel(wheel)

        assert "FORBIDDEN_NATIVE_MAGIC" in _finding_codes(report)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("tag", "path", "payload"),
    (
        (
            "py3-none-linux_x86_64",
            "bullet_trade/integrations/huaxin/libbullet_trade_huaxin.so",
            _fake_elf_x86_64(),
        ),
        (
            "py3-none-win_amd64",
            "bullet_trade/integrations/huaxin/bullet_trade_huaxin.dll",
            _fake_pe_x86_64(),
        ),
    ),
)
def test_platform_wheel_contract_is_valid_but_release_remains_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tag: str,
    path: str,
    payload: bytes,
) -> None:
    """
    验证 Linux/Windows x64 静态合同正确时仍因发布 manifest/SBOM/license 未完成而关闭。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 隔离外部依赖工具，只验证静态平台合同。
        tag: 待验证 wheel 三元标签。
        path: 第一方 bridge 制品内路径。
        payload: 与平台合同一致的测试原生头部和符号字节。
    返回:
        无；只出现明确的条件发布未启用门禁，不出现格式/架构/符号误报即通过。
    """

    wheel = _write_wheel(tmp_path, tag=tag, extra_files={path: payload})
    monkeypatch.setattr(release_audit, "_inspect_native_files", lambda *_args: tuple())

    report = audit_wheel(wheel)

    codes = _finding_codes(report)
    assert "PLATFORM_WHEEL_RELEASE_NOT_ENABLED" in codes
    assert "PLATFORM_NATIVE_FORMAT_MISMATCH" not in codes
    assert "PLATFORM_NATIVE_ARCH_MISMATCH" not in codes
    assert "PLATFORM_BRIDGE_SYMBOLS_MISSING" not in codes


@pytest.mark.unit
def test_platform_wheel_rejects_format_mismatch_and_missing_bridge_symbols(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    验证 manylinux 标签不能包装 Mach-O，且真实 ELF 头也不能缺少必要 flat C ABI 标记。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 隔离外部依赖工具，只验证静态平台合同。
    返回:
        无；格式/架构和符号缺失分别产生稳定发现项即通过。
    """

    path = "bullet_trade/integrations/huaxin/libbullet_trade_huaxin.so"
    tag = "py3-none-linux_x86_64"
    macho = b"\xca\xfe\xba\xbf" + b"\x00".join(release_audit._EXPECTED_BRIDGE_SYMBOLS)
    mismatched = _write_wheel(tmp_path, tag=tag, extra_files={path: macho})
    monkeypatch.setattr(release_audit, "_inspect_native_files", lambda *_args: tuple())

    mismatch_codes = _finding_codes(audit_wheel(mismatched))
    mismatched.unlink()
    missing_symbols = _write_wheel(
        tmp_path,
        tag=tag,
        extra_files={path: _fake_elf_x86_64(include_bridge_symbols=False)},
    )
    missing_codes = _finding_codes(audit_wheel(missing_symbols))

    assert "PLATFORM_NATIVE_FORMAT_MISMATCH" in mismatch_codes
    assert "PLATFORM_NATIVE_ARCH_MISMATCH" in mismatch_codes
    assert "PLATFORM_BRIDGE_SYMBOLS_MISSING" in missing_codes


@pytest.mark.unit
def test_platform_wheel_rejects_inventory_only_windows_x86_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    验证当前仅盘点的 Windows x86 制品不能被误报为 BulletTrade V1 platform wheel。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 隔离外部依赖工具，只验证标签门禁。
    返回:
        无；win32 标签产生不支持发现项即通过。
    """

    wheel = _write_wheel(
        tmp_path,
        tag="py3-none-win32",
        extra_files={"bullet_trade/integrations/huaxin/bullet_trade_huaxin.dll": _fake_pe_x86_64()},
    )
    monkeypatch.setattr(release_audit, "_inspect_native_files", lambda *_args: tuple())

    report = audit_wheel(wheel)

    assert "PLATFORM_WHEEL_TAG_UNSUPPORTED" in _finding_codes(report)


@pytest.mark.unit
def test_wheel_rejects_extra_metadata_tag_and_nonportable_paths(tmp_path: Path) -> None:
    """
    验证 WHEEL 额外标签、NTFS ADS/设备名/控制字符和 Unicode 归一化冲突均被拒绝。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；标签集合与跨平台路径分别产生稳定发现项即通过。
    """

    composed = "bullet_trade/caf\u00e9.py"
    decomposed = "bullet_trade/cafe\u0301.py"
    wheel = _write_wheel(
        tmp_path,
        extra_files={
            "bullet_trade/payload.py:secret": b"pass\n",
            "bullet_trade/CON.py": b"pass\n",
            "bullet_trade/tab\tname.py": b"pass\n",
            "bullet_trade/question?.py": b"pass\n",
            "bullet_trade/less<than.py": b"pass\n",
            "bullet_trade/pipe|name.py": b"pass\n",
            "bullet_trade/star*name.py": b"pass\n",
            composed: b"pass\n",
            decomposed: b"pass\n",
        },
        extra_wheel_tags=("cp39-cp39-any",),
    )

    codes = _finding_codes(audit_wheel(wheel))

    assert "WHEEL_TAG_SET_MISMATCH" in codes
    assert "NONPORTABLE_ARCHIVE_PATH" in codes
    assert "PATH_NORMALIZATION_COLLISION" in codes


@pytest.mark.unit
def test_huaxin_json_and_yaml_generic_account_fields_are_sensitive(tmp_path: Path) -> None:
    """
    验证 Huaxin 目录 JSON 数值账户和通用 YAML 柜台字段不能绕过前缀/引号规则。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；两种独立配置均命中脱敏敏感字面量规则。
    """

    account_key = "HUAXIN_" + "ACCOUNT_ID"
    json_payload = json.dumps({account_key: int("386" + "000099999")}).encode()
    yaml_payload = b"account_" + b"id: 386" + b"000088888\n"
    wheel = _write_wheel(
        tmp_path,
        extra_files={
            "bullet_trade/integrations/huaxin/config.json": json_payload,
            "bullet_trade/integrations/huaxin/account.yaml": yaml_payload,
        },
    )

    report = audit_wheel(wheel)

    assert "SENSITIVE_LITERAL" in _finding_codes(report)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)
    assert "386" + "000099999" not in serialized
    assert "386" + "000088888" not in serialized


@pytest.mark.unit
def test_sensitive_scan_handles_bom_python_ast_nested_json_and_absolute_sdk_paths(
    tmp_path: Path,
) -> None:
    """
    验证 BOM、Huaxin Python AST、嵌套 vendor JSON 与多平台 SDK 绝对路径均被拒绝。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；固定规则码存在且任何合成原值都不出现在报告中。
    """

    account_value = "386" + "000077777"
    credential_value = "example-" + "production-credential-73921"
    utf16_python = (
        "ACCOUNT_"
        + 'ID = "{}"\nPASS'.format(account_value)
        + 'WORD = "{}"\n'.format(credential_value)
    ).encode("utf-16")
    nested_json = json.dumps({"services": {"huaxin": {"account_" + "id": account_value}}}).encode(
        "utf-8-sig"
    )
    path_payload = "\n".join(
        (
            "/" + "root/vendor/huaxin-" + "sdk/private",
            "/" + "srv/build/tora/" + "sdk/private",
            "C:" + "/vendor/HuaXin-" + "SDK/private",
            "\\\\" + "synthetic-host/share/TORA/" + "sdk/private",
        )
    ).encode()
    wheel = _write_wheel(
        tmp_path,
        extra_files={
            "bullet_trade/integrations/huaxin/config.py": utf16_python,
            "bullet_trade/settings.json": nested_json,
            "bullet_trade/build-paths.txt": path_payload,
        },
    )

    report = audit_wheel(wheel)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert "SENSITIVE_LITERAL" in _finding_codes(report)
    assert "ABSOLUTE_SDK_BUILD_PATH" in _finding_codes(report)
    assert account_value not in serialized
    assert credential_value not in serialized
    assert "synthetic-host" not in serialized


@pytest.mark.unit
@pytest.mark.parametrize("encoding", ("utf-16-le", "utf-16-be"))
def test_sensitive_scan_handles_bomless_utf16_and_sibling_vendor_context(
    tmp_path: Path,
    encoding: str,
) -> None:
    """
    验证无 BOM UTF-16 配置与同级 vendor=huaxin 的账户字段均无法绕过扫描。

    参数:
        tmp_path: pytest 临时目录。
        encoding: 无 BOM UTF-16 大小端编码。
    返回:
        无；两个文件均产生脱敏敏感字面量发现即通过。
    """

    password_key = "PASS" + "WORD"
    password_value = "real-" + "utf16-credential-92831"
    account_value = "386" + "000066666"
    nested_account_value = "386" + "000055555"
    list_account_value = "386" + "000044444"
    duplicate_account_value = "386" + "000033333"
    utf16_payload = ("#" + ("密" * 100) + '\n{}="{}"\n'.format(password_key, password_value)).encode(
        encoding
    )
    sibling_payload = json.dumps(
        {"service": {"vendor": "huaxin", "account_" + "id": account_value}}
    ).encode("utf-8")
    nested_payload = json.dumps(
        {
            "service": {
                "provider": "tora",
                "credentials": {"account": {"id": nested_account_value}},
            }
        }
    ).encode("utf-8")
    list_payload = json.dumps({"vendor": "huaxin", "account": [list_account_value]}).encode("utf-8")
    duplicate_payload = (
        '{"vendor":"huaxin","vendor":"other","account_' + 'id":"' + duplicate_account_value + '"}'
    ).encode("utf-8")
    yaml_payload = ("vendor: huaxin\naccount_" + "id: " + account_value + "\n").encode("utf-8")
    toml_payload = (
        'provider = "tora"\naccount_' + 'id = "{}"\n'.format(nested_account_value)
    ).encode("utf-8")
    wheel = _write_wheel(
        tmp_path,
        extra_files={
            "bullet_trade/integrations/huaxin/.env.runtime": utf16_payload,
            "bullet_trade/settings.json": sibling_payload,
            "bullet_trade/nested-settings.json": nested_payload,
            "bullet_trade/list-settings.json": list_payload,
            "bullet_trade/duplicate-settings.json": duplicate_payload,
            "bullet_trade/settings.yaml": yaml_payload,
            "bullet_trade/settings.toml": toml_payload,
        },
    )

    report = audit_wheel(wheel)
    sensitive_paths = {
        finding.path for finding in report.findings if finding.code == "SENSITIVE_LITERAL"
    }
    parse_failure_paths = {
        finding.path for finding in report.findings if finding.code == "SENSITIVE_SCAN_PARSE_FAILED"
    }
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert "bullet_trade/integrations/huaxin/.env.runtime" in sensitive_paths
    assert "bullet_trade/settings.json" in sensitive_paths
    assert "bullet_trade/nested-settings.json" in sensitive_paths
    assert "bullet_trade/list-settings.json" in sensitive_paths
    assert "bullet_trade/settings.yaml" in sensitive_paths
    assert "bullet_trade/settings.toml" in sensitive_paths
    assert "bullet_trade/duplicate-settings.json" in parse_failure_paths
    assert password_value not in serialized
    assert account_value not in serialized
    assert nested_account_value not in serialized
    assert list_account_value not in serialized
    assert duplicate_account_value not in serialized


@pytest.mark.unit
@pytest.mark.parametrize(
    ("encoding", "bom"),
    (("utf-16-le", b"\xff\xfe"), ("utf-16-be", b"\xfe\xff")),
)
def test_malformed_bom_utf16_config_is_scanned_and_fails_closed(
    tmp_path: Path,
    encoding: str,
    bom: bytes,
) -> None:
    """
    验证明示 UTF-16 BOM 的奇数字节配置仍扫描秘密并报告替代解码。

    参数:
        tmp_path: pytest 临时目录。
        encoding: 与 BOM 对应的 UTF-16 大小端编码。
        bom: 显式 UTF-16 BOM。
    返回:
        无；敏感字面量与解码失败规则同时出现且报告不回显原值。
    """

    password_key = "PASS" + "WORD"
    password_value = "redteam-" + "credential-92831"
    payload = bom + '{}="{}"\n'.format(password_key, password_value).encode(encoding) + b"X"
    wheel = _write_wheel(
        tmp_path,
        extra_files={"bullet_trade/integrations/huaxin/.env.broken": payload},
    )

    report = audit_wheel(wheel)
    codes = _finding_codes(report)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert "SENSITIVE_LITERAL" in codes
    assert "SENSITIVE_TEXT_DECODE_FAILED" in codes
    assert password_value not in serialized


@pytest.mark.unit
def test_sensitive_json_depth_and_placeholder_prefix_fail_closed(tmp_path: Path) -> None:
    """
    验证深层 JSON 解析异常会失败，且仅前缀像测试值的真实字面量不被白名单放过。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；深度异常产生解析失败码，非精确占位值返回 False。
    """

    deep_json = ("[" * 1_500 + "0" + "]" * 1_500).encode()
    wheel = _write_wheel(
        tmp_path,
        extra_files={"bullet_trade/integrations/huaxin/deep.json": deep_json},
    )

    report = audit_wheel(wheel)

    assert "SENSITIVE_SCAN_PARSE_FAILED" in _finding_codes(report)
    assert release_audit._is_placeholder_secret("${SYNTHETIC_ENV}") is True
    assert release_audit._is_placeholder_secret("<replace-me>") is True
    assert release_audit._is_placeholder_secret("test-production-password") is False
    assert release_audit._is_placeholder_secret("example-real-token") is False


@pytest.mark.unit
def test_sdist_identity_parsers_are_required_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 packaging 与 tomllib/tomli 缺失时 sdist 身份检查不能退回正则猜测。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 临时模拟结构化解析器缺失。
    返回:
        无；两个缺失解析器都产生稳定失败规则码。
    """

    sdist = _write_sdist(tmp_path)
    monkeypatch.setattr(release_audit, "_PACKAGING_UTILS", None)
    monkeypatch.setattr(release_audit, "_PACKAGING_VERSION", None)
    monkeypatch.setattr(release_audit, "_TOML_PARSER", None)

    report = audit_sdist(sdist)
    codes = _finding_codes(report)

    assert "PACKAGING_PARSER_UNAVAILABLE" in codes
    assert "PYPROJECT_PARSER_UNAVAILABLE" in codes
    assert report.passed is False


@pytest.mark.unit
def test_canonicalize_sdist_removes_opaque_archive_metadata(tmp_path: Path) -> None:
    """验证规范化步骤移除 gzip FNAME 与 PAX 元数据并产出可审计 sdist。

    参数:
        tmp_path: pytest 提供的隔离归档目录。
    返回:
        无；断言原路径被安全替换且严格审计通过。
    副作用:
        在临时目录原子替换一份合成 sdist。
    """

    sdist = _write_sdist(
        tmp_path,
        tar_variant="pax",
        gzip_filename="opaque-builder-path.tar",
    )
    before = audit_sdist(sdist)

    canonicalized = canonicalize_sdist(sdist)
    after = audit_sdist(sdist)

    assert {
        "GZIP_FNAME_FORBIDDEN",
        "TAR_EXTENDED_HEADER_FORBIDDEN",
    }.issubset(_finding_codes(before))
    assert canonicalized == sdist.resolve()
    assert after.passed is True, [finding.to_dict() for finding in after.findings]


@pytest.mark.unit
def test_canonicalize_sdist_rejects_links_without_rewriting_input(tmp_path: Path) -> None:
    """验证外层或归档内符号链接失败关闭，且不会改写原始输入。

    参数:
        tmp_path: pytest 提供的隔离归档目录。
    返回:
        无；断言两种链接均被拒绝且恶意归档字节保持不变。
    副作用:
        在临时目录创建合成归档及指向它的符号链接。
    """

    sdist = _write_sdist(tmp_path, symlink=True)
    before = sdist.read_bytes()

    with pytest.raises(ValueError, match="普通文件和目录"):
        canonicalize_sdist(sdist)

    link = tmp_path / "linked.tar.gz"
    link.symlink_to(sdist.name)
    with pytest.raises(ValueError, match="符号链接"):
        canonicalize_sdist(link)

    assert sdist.read_bytes() == before


@pytest.mark.unit
def test_outer_artifact_symlink_and_unpacked_hard_limit_fail_closed(tmp_path: Path) -> None:
    """
    验证外层 wheel 符号链接不被 resolve 掩盖，累计声明解包量在读取前触发硬门禁。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；symlink 与累计容量分别产生稳定失败。
    """

    wheel = _write_wheel(tmp_path, extra_files={"bullet_trade/large.bin": b"x" * 256})
    link = tmp_path / "linked.whl"
    link.symlink_to(wheel.name)

    link_codes = _finding_codes(audit_wheel(link))
    limit_codes = _finding_codes(
        audit_wheel(wheel, ReleaseAuditPolicy(max_unpacked_scan_bytes=128))
    )

    assert "ARTIFACT_SYMLINK_NOT_ALLOWED" in link_codes
    assert "ARCHIVE_UNPACKED_HARD_LIMIT" in limit_codes


@pytest.mark.unit
def test_invalid_or_missing_archive_names_never_enter_reports(tmp_path: Path) -> None:
    """
    验证非法或缺失 wheel/sdist 的外层文件名不会原样进入脱敏报告。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；合成私有标记不在任一序列化报告内即通过。
    """

    private_marker = "SYNTHETIC_" + "PRIVATE_ACCOUNT_88421"
    tag_marker = "synthetic" + "privateaccount88421"
    version_marker = "synthetic" + "privateaccount77312"
    extra_marker = "synthetic" + "privateaccount66203"
    numeric_version_marker = "172.31.254.199"
    wheel_path = tmp_path / (private_marker + "-0.0.0-py3-none-any.whl")
    sdist_path = tmp_path / (private_marker + "-0.0.0.tar.gz")
    tag_path = tmp_path / ("bullet_trade-0.0.0-" + tag_marker + "-none-any.whl")
    version_path = tmp_path / ("bullet_trade-1.0+" + version_marker + "-py3-none-any.whl")
    numeric_version_path = tmp_path / (
        "bullet_trade-" + numeric_version_marker + "-py3-none-any.whl"
    )
    metadata = (
        b"Metadata-Version: 2.1\nName: bullet-trade\nVersion: 0.0.0\nProvides-Extra: "
        + extra_marker.encode("ascii")
        + b"\n"
    )
    extra_wheel = _write_wheel(tmp_path, metadata=metadata)

    wheel_report = audit_wheel(wheel_path)
    sdist_report = audit_sdist(sdist_path)
    tag_report = audit_wheel(tag_path)
    version_report = audit_wheel(version_path)
    numeric_version_report = audit_wheel(numeric_version_path)
    extra_report = audit_wheel(extra_wheel)
    serialized = json.dumps(
        {
            "wheel": wheel_report.to_dict(),
            "sdist": sdist_report.to_dict(),
            "tag": tag_report.to_dict(),
            "version": version_report.to_dict(),
            "numeric_version": numeric_version_report.to_dict(),
            "extra": extra_report.to_dict(),
        },
        ensure_ascii=False,
    )

    assert wheel_report.artifact_name == "bullet-trade-wheel"
    assert sdist_report.artifact_name == "bullet-trade-sdist"
    assert private_marker not in serialized
    assert tag_marker not in serialized
    assert version_marker not in serialized
    assert numeric_version_marker not in serialized
    assert extra_marker not in serialized


@pytest.mark.unit
def test_sdist_identity_rejects_decoy_pyproject_and_renamed_archive(tmp_path: Path) -> None:
    """
    验证 sdist 文件名/根/PKG-INFO 与 canonical `[project]` 身份不能被 tool decoy 欺骗。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；真实 project.name/version 与外层身份不一致均被拒绝。
    """

    decoy_pyproject = (
        b'[tool.decoy]\nname = "bullet-trade"\n' b'[project]\nname = "evil-dist"\nversion = "999"\n'
    )
    sdist = _write_sdist(tmp_path, extra_files={"pyproject.toml": decoy_pyproject})
    decoy_codes = _finding_codes(audit_sdist(sdist))
    renamed = sdist.with_name("renamed-0.0.0.tar.gz")
    sdist.rename(renamed)
    renamed_codes = _finding_codes(audit_sdist(renamed))

    assert "PYPROJECT_DISTRIBUTION_MISMATCH" in decoy_codes
    assert "PYPROJECT_VERSION_MISMATCH" in decoy_codes
    assert "SDIST_FILENAME_INVALID" in renamed_codes


@pytest.mark.unit
def test_sdist_rejects_pep503_equivalent_dependency_in_pyproject(tmp_path: Path) -> None:
    """
    验证 pyproject 主依赖中的独立华鑫 distribution 点号变体被结构化拒绝。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；即使 PKG-INFO 省略该依赖，pyproject 仍触发固定禁止规则。
    """

    pyproject = (
        b'[project]\nname = "bullet-trade"\nversion = "0.0.0"\n'
        b'dependencies = ["bullet.trade.huaxin>=1"]\n'
    )
    sdist = _write_sdist(tmp_path, extra_files={"pyproject.toml": pyproject})

    codes = _finding_codes(audit_sdist(sdist))

    assert "INDEPENDENT_HUAXIN_DISTRIBUTION_FORBIDDEN" in codes


@pytest.mark.unit
def test_strict_synthetic_sdist_passes_archive_and_identity_audit(tmp_path: Path) -> None:
    """
    验证无 FNAME/PAX/身份元数据的严格 sdist 可通过完整离线审计。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；严格 gzip/tar 包络、PEP 身份和 TOML 均无发现即通过。
    """

    sdist = _write_sdist(tmp_path)

    report = audit_sdist(sdist)

    assert report.passed is True, [finding.to_dict() for finding in report.findings]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variant", "expected_code"),
    (
        ("gzip-prefix", "GZIP_PREFIX_FORBIDDEN"),
        ("gzip-tail", "GZIP_TRAILING_DATA_FORBIDDEN"),
        ("gzip-concat", "GZIP_CONCATENATED_MEMBER_FORBIDDEN"),
        ("gzip-fname", "GZIP_FNAME_FORBIDDEN"),
        ("tar-prefix", "TAR_HEADER_CHECKSUM_INVALID"),
        ("tar-tail", "TAR_TRAILING_DATA_FORBIDDEN"),
    ),
)
def test_sdist_gzip_and_tar_envelopes_reject_hidden_regions(
    tmp_path: Path,
    variant: str,
    expected_code: str,
) -> None:
    """
    验证 gzip/tar 的前后缀、拼接 member 与 FNAME 都不能形成未审计区域。

    参数:
        tmp_path: pytest 临时目录。
        variant: 待构造的 gzip 或 tar 包络反例。
        expected_code: 对应固定拒绝规则码。
    返回:
        无；每个包络反例均由完整 audit_sdist 路径 fail closed。
    """

    case_directory = tmp_path / variant
    case_directory.mkdir()
    filename = "synthetic-name" if variant == "gzip-fname" else ""
    sdist = _write_sdist(case_directory, gzip_filename=filename)
    original = sdist.read_bytes()
    if variant == "gzip-prefix":
        sdist.write_bytes(b"prefix" + original)
    elif variant == "gzip-tail":
        sdist.write_bytes(original + b"tail")
    elif variant == "gzip-concat":
        sdist.write_bytes(original + original)
    elif variant in {"tar-prefix", "tar-tail"}:
        tar_payload = bytearray(gzip.decompress(original))
        if variant == "tar-prefix":
            tar_payload = bytearray(b"x" * 512) + tar_payload
        else:
            tar_payload[-1] = 1
        gzip_payload = io.BytesIO()
        with gzip.GzipFile(fileobj=gzip_payload, mode="wb", filename="", mtime=0) as stream:
            stream.write(tar_payload)
        sdist.write_bytes(gzip_payload.getvalue())

    report = audit_sdist(sdist)

    assert expected_code in _finding_codes(report)
    assert report.passed is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variant", "expected_code"),
    (
        ("pax", "TAR_EXTENDED_HEADER_FORBIDDEN"),
        ("identity", "TAR_IDENTITY_METADATA_FORBIDDEN"),
        ("regular-slash", "TAR_REGULAR_TRAILING_SLASH_FORBIDDEN"),
    ),
)
def test_sdist_tar_metadata_is_strict_and_portable(
    tmp_path: Path,
    variant: str,
    expected_code: str,
) -> None:
    """
    验证 PAX、uname/gname 与普通文件尾斜线均被严格 tar 合同拒绝。

    参数:
        tmp_path: pytest 临时目录。
        variant: tar 元数据反例类别。
        expected_code: 对应固定拒绝规则码。
    返回:
        无；完整 sdist 审计命中对应规则。
    """

    sdist = _write_sdist(tmp_path, tar_variant=variant)

    report = audit_sdist(sdist)

    assert expected_code in _finding_codes(report)
    assert report.passed is False


@pytest.mark.unit
def test_tar_directory_with_nonzero_payload_is_rejected() -> None:
    """
    验证 tar 目录 header 声明非零数据时即使不落盘也被原始块审计拒绝。

    参数:
        无。
    返回:
        无；原始 tar envelope 产生目录数据固定拒绝码。
    """

    info = tarfile.TarInfo("bullet_trade-0.0.0/nonzero-directory/")
    info.type = tarfile.DIRTYPE
    info.size = 1
    info.mtime = 0
    header = info.tobuf(format=tarfile.USTAR_FORMAT)
    raw_tar = header + b"x" + (b"\x00" * 511) + (b"\x00" * 1024)
    builder = release_audit._AuditBuilder()

    release_audit._audit_tar_envelope(raw_tar, builder)

    codes = {finding.code for finding in builder.findings}
    assert "ARCHIVE_DIRECTORY_DATA_FORBIDDEN" in codes


@pytest.mark.unit
def test_sdist_rejects_special_member_and_redacts_sensitive_values(tmp_path: Path) -> None:
    """
    验证 sdist 拒绝链接、TerminalInfo/密码和华鑫 SDK 绝对路径且报告不回显原值。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；规则码存在且序列化报告不含候选秘密即通过。
    """

    secret_value = "protestCredential92731"
    terminal_value = "LIP=private,MAC=private,HD=private"
    password_key = "HUAXIN_" + "PASSWORD"
    terminal_key = "HUAXIN_" + "TERMINAL_INFO"
    account_key = "HUAXIN_" + "ACCOUNT_ID"
    front_key = "HUAXIN_" + "TRADE_FRONT"
    account_value = "386" + "000099999"
    front_value = "tcp://10." + "20.30.40:9001"
    sdk_path = "/opt/" + "huaxin-" + "sdk/private"
    environment_payload = '{}="{}"\n{}="{}"\n{}={}\n{}={}\nSDK_DIR={}\n'.format(
        password_key,
        secret_value,
        terminal_key,
        terminal_value,
        account_key,
        account_value,
        front_key,
        front_value,
        sdk_path,
    ).encode()
    yaml_payload = (
        b"pass" + b"word: RealYamlPassword92731\nterminal_" + b"info: RealTerminal92731\n"
    )
    build_path = b"/" + b"Users/SYNTHETIC/HuaXin/sdk/include\n"
    sdist = _write_sdist(
        tmp_path,
        extra_files={
            ".env": environment_payload,
            "config.yaml": yaml_payload,
            "private.pem": b"-----BEGIN ENCRYPTED " + b"PRIVATE KEY-----\nsynthetic\n",
            "build.txt": build_path,
        },
        symlink=True,
    )

    report = audit_sdist(sdist)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)
    codes = _finding_codes(report)

    assert "SPECIAL_ARCHIVE_MEMBER" in codes
    assert "SENSITIVE_LITERAL" in codes
    assert "PRIVATE_KEY_MATERIAL" in codes
    assert "ABSOLUTE_SDK_BUILD_PATH" in codes
    assert "PERSONAL_HOME_PATH" in codes
    assert secret_value not in serialized
    assert terminal_value not in serialized
    assert account_value not in serialized
    assert "RealYamlPassword92731" not in serialized
    assert str(tmp_path) not in serialized


@pytest.mark.unit
def test_native_bundle_requires_available_dependency_inspector(
    offline_bundle: BuildResult,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证真实 native 魔数存在时工具缺失稳定 fail closed。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
        monkeypatch: 用于模拟所有 native 工具缺失。
    返回:
        无；报告 NATIVE_INSPECTOR_UNAVAILABLE 即通过。
    """

    bundle = _copy_offline_bundle(offline_bundle, tmp_path)
    monkeypatch.setattr(release_audit.shutil, "which", lambda _name: None)

    report = audit_bundle(bundle)

    assert "NATIVE_INSPECTOR_UNAVAILABLE" in _finding_codes(report)
    assert "NATIVE_EXPORT_INSPECTOR_UNAVAILABLE" in _finding_codes(report)
    assert report.passed is False


@pytest.mark.unit
def test_bundle_snapshot_rejects_extra_hardlink_and_writable_nodes(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证 bundle 文件清单、硬链接、根目录和普通文件写权限均 fail closed。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
    返回:
        无；三份互不干扰的副本分别命中精确规则码。
    """

    extra_root = _copy_offline_bundle(offline_bundle, tmp_path / "extra")
    (extra_root / "unexpected.dat").write_bytes(b"synthetic")
    assert "BUNDLE_INVENTORY_INVALID" in _finding_codes(audit_bundle(extra_root))

    empty_directory_root = _copy_offline_bundle(offline_bundle, tmp_path / "empty-directory")
    (empty_directory_root / "unexpected-empty-directory").mkdir()
    assert "BUNDLE_DIRECTORY_INVENTORY_INVALID" in _finding_codes(
        audit_bundle(empty_directory_root)
    )

    if os.name == "posix":
        symlink_root = _copy_offline_bundle(offline_bundle, tmp_path / "symlink")
        external_directory = tmp_path / "external-directory"
        external_directory.mkdir()
        external_marker = "SYNTHETIC_EXTERNAL_PAYLOAD_59123"
        (external_directory / external_marker).write_bytes(b"not-read")
        (symlink_root / "linked-directory").symlink_to(
            external_directory,
            target_is_directory=True,
        )
        symlink_report = audit_bundle(symlink_root)
        assert "SYMLINK_NOT_ALLOWED" in _finding_codes(symlink_report)
        assert external_marker not in json.dumps(symlink_report.to_dict(), ensure_ascii=False)

    hardlink_root = _copy_offline_bundle(offline_bundle, tmp_path / "hardlink")
    hardlink_manifest = json.loads((hardlink_root / "manifest.json").read_text(encoding="utf-8"))
    hardlink_artifact = hardlink_root / hardlink_manifest["bridge"]["artifact"]
    external = tmp_path / "hardlink-source"
    external.write_bytes(hardlink_artifact.read_bytes())
    hardlink_artifact.unlink()
    os.link(str(external), str(hardlink_artifact))
    assert "BUNDLE_FILE_LINK_COUNT_INVALID" in _finding_codes(audit_bundle(hardlink_root))

    if os.name == "posix":
        writable_root = _copy_offline_bundle(offline_bundle, tmp_path / "writable")
        writable_manifest = json.loads(
            (writable_root / "manifest.json").read_text(encoding="utf-8")
        )
        writable_artifact = writable_root / writable_manifest["bridge"]["artifact"]
        writable_artifact.chmod(0o666)
        writable_root.chmod(0o777)
        writable_codes = _finding_codes(audit_bundle(writable_root))
        assert "BUNDLE_FILE_PERMISSIONS_UNSAFE" in writable_codes
        assert "BUNDLE_DIRECTORY_PERMISSIONS_UNSAFE" in writable_codes


@pytest.mark.unit
@pytest.mark.parametrize(
    "variant", ("duplicate", "nan", "nul", "deep", "surrogate", "huge_integer")
)
def test_bundle_manifest_parser_rejects_ambiguous_json_without_value_echo(
    offline_bundle: BuildResult,
    tmp_path: Path,
    variant: str,
) -> None:
    """
    验证重复键、NaN、NUL 与递归深度异常均稳定失败且不回显原值。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
        variant: 待构造的严格 JSON 反例。
    返回:
        无；完整 audit_bundle 不抛异常且只报告固定错误语义。
    """

    bundle = _copy_offline_bundle(offline_bundle, tmp_path / variant)
    manifest_path = bundle / "manifest.json"
    original = manifest_path.read_text(encoding="utf-8")
    marker = "SYNTHETIC_PRIVATE_MODE_73921"
    if variant == "duplicate":
        payload = '{"mode":"' + marker + '",' + original.lstrip()[1:]
    elif variant == "nan":
        payload = original.replace('"schema_version": 1', '"schema_version": NaN', 1)
    elif variant == "nul":
        payload = original + "\x00" + marker
    elif variant == "deep":
        payload = '{"nested":' + ("[" * 1_500) + "0" + ("]" * 1_500) + "}"
    elif variant == "surrogate":
        payload = '{"nested":"\\ud800"}'
    else:
        payload = original.replace('"schema_version": 1', '"schema_version": ' + ("9" * 128), 1)
    manifest_path.write_bytes(payload.encode("utf-8"))

    report = audit_bundle(bundle)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert "BUNDLE_MANIFEST_INVALID" in _finding_codes(report)
    assert marker not in serialized


@pytest.mark.unit
@pytest.mark.parametrize("location", ("top", "target", "source_entry"))
def test_bundle_manifest_exact_schema_rejects_unknown_sensitive_fields(
    offline_bundle: BuildResult,
    tmp_path: Path,
    location: str,
) -> None:
    """
    验证 manifest 任意层级未知字段即使自签也失败，且名称和值均不回显。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
        location: 合成未知字段所在的顶层、target 或 source.files 项。
    返回:
        无；精确 schema 规则码存在且敏感标记不进入报告即通过。
    """

    marker_key = "synthetic_account_front_41729"
    marker_value = "SYNTHETIC_PRIVATE_ENDPOINT_41729"
    bundle = _copy_offline_bundle(offline_bundle, tmp_path / location)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    if location == "top":
        manifest[marker_key] = marker_value
    elif location == "target":
        manifest["target"][marker_key] = marker_value
    else:
        manifest["source"]["files"][0][marker_key] = marker_value
    resigned = _resign_bundle_manifest(bundle, manifest)

    report = audit_bundle(resigned)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert "BUNDLE_MANIFEST_SCHEMA_INVALID" in _finding_codes(report)
    assert marker_key not in serialized
    assert marker_value not in serialized


@pytest.mark.unit
def test_bundle_manifest_schema_version_rejects_boolean(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证 Python 中与整数一相等的布尔值不能冒充 schema_version。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
    返回:
        无；自签后的布尔 schema 同时命中精确 schema 与版本规则。
    """

    bundle = _copy_offline_bundle(offline_bundle, tmp_path / "boolean-schema")
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    manifest["schema_version"] = True
    resigned = _resign_bundle_manifest(bundle, manifest)

    codes = _finding_codes(audit_bundle(resigned))

    assert "BUNDLE_MANIFEST_SCHEMA_INVALID" in codes
    assert "BUNDLE_MANIFEST_INVALID" in codes


@pytest.mark.unit
def test_bundle_report_redacts_invalid_name_mode_and_artifact_value(
    offline_bundle: BuildResult,
    tmp_path: Path,
) -> None:
    """
    验证非法目录名、mode 与 artifact 不会进入 artifact_name、metadata 或 details。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
    返回:
        无；攻击者控制的三个原始字符串均不出现在序列化报告。
    """

    directory_marker = "SYNTHETIC_PRIVATE_BUNDLE_61234"
    mode_marker = "SYNTHETIC_PRIVATE_MODE_61234"
    artifact_marker = "lib/SYNTHETIC_PRIVATE_ACCOUNT_61234.dylib"
    bundle = _copy_offline_bundle(offline_bundle, tmp_path, name=directory_marker)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["mode"] = mode_marker
    manifest["bridge"]["artifact"] = artifact_marker
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = audit_bundle(bundle)
    serialized = json.dumps(report.to_dict(), ensure_ascii=False)

    assert report.artifact_name == "<invalid-bundle-name>"
    assert directory_marker not in serialized
    assert mode_marker not in serialized
    assert artifact_marker not in serialized
    assert "mode" not in report.metadata
    assert report.metadata["mode_is_offline_fake"] is False


@pytest.mark.unit
def test_bundle_verify_and_native_inspection_receive_only_private_snapshot(
    offline_bundle: BuildResult,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 verify 与 native 检查仅接收 0700/0600 私有快照且清单恰含两个文件。

    参数:
        offline_bundle: 会话级真实 offline_fake bundle。
        tmp_path: pytest 临时目录。
        monkeypatch: 捕获两个下游检查器的输入，不执行外部工具。
    返回:
        无；两个检查器都观察到相同私有内容寻址根即通过。
    """

    bundle = _copy_offline_bundle(offline_bundle, tmp_path / "input")
    observed: Dict[str, Path] = {}

    def _capture_trust(
        root: Path,
        native_files: object,
        builder: object,
    ) -> None:
        """
        捕获私有 verify 根并验证精确清单与权限。

        参数:
            root: audit_bundle 提供的临时内容寻址根。
            native_files: 私有 native 文件序列。
            builder: 审计构建器，本桩不修改。
        返回:
            无。
        """

        del native_files, builder
        observed["root"] = root
        assert root != bundle
        assert root.name == offline_bundle.fingerprint
        if os.name == "posix":
            assert stat.S_IMODE(root.stat().st_mode) == 0o700
        relative_files = sorted(
            path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()
        )
        assert relative_files == [
            "lib/" + offline_bundle.artifact_path.name,
            "manifest.json",
        ]
        if os.name == "posix":
            assert all(
                stat.S_IMODE(path.stat().st_mode) == 0o600
                for path in root.rglob("*")
                if path.is_file()
            )

    def _capture_native(
        native_files: object,
        builder: object,
        dependency_profile: Optional[str] = None,
    ) -> tuple:
        """
        捕获 native 私有路径并验证 offline_fake profile。

        参数:
            native_files: audit_bundle 提供的私有文件序列。
            builder: 审计构建器，本桩不修改。
            dependency_profile: 应为 offline_fake。
        返回:
            空检查摘要。
        """

        del builder
        assert dependency_profile == "offline_fake"
        assert isinstance(native_files, list)
        item = native_files[0][0]
        assert item.source_path is not None
        assert observed["root"] in item.source_path.parents
        return tuple()

    monkeypatch.setattr(release_audit, "_audit_managed_bundle_trust", _capture_trust)
    monkeypatch.setattr(release_audit, "_inspect_native_files", _capture_native)

    report = audit_bundle(bundle)

    assert report.passed is True, [finding.to_dict() for finding in report.findings]


@pytest.mark.unit
def test_empty_bundle_never_calls_verify_or_native_tools(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证外层目录没有任何文件时不会调用 verify_bundle 路径或 native 工具。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 把两个下游入口替换为一旦调用就失败的桩。
    返回:
        无；audit_bundle 返回受控失败报告且桩未触发。
    """

    bundle = tmp_path / ("0" * 64)
    bundle.mkdir()

    def _unexpected_call(*_args: object, **_kwargs: object) -> object:
        """
        在不应触发的下游入口被调用时立即使测试失败。

        参数:
            _args: 不应存在的位置参数。
            _kwargs: 不应存在的关键字参数。
        返回:
            永不返回。
        异常:
            AssertionError: 每次调用均抛出。
        """

        raise AssertionError("空 bundle 不得触发 verify/native")

    monkeypatch.setattr(release_audit, "_audit_managed_bundle_trust", _unexpected_call)
    monkeypatch.setattr(release_audit, "_inspect_native_files", _unexpected_call)

    report = audit_bundle(bundle)

    assert report.passed is False
    assert "BUNDLE_MANIFEST_INVALID" in _finding_codes(report)


@pytest.mark.unit
def test_bundle_file_snapshot_detects_fstat_generation_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证同一文件描述符前后 fstat 身份变化时不返回 data 或 hash。

    参数:
        tmp_path: pytest 临时普通文件目录。
        monkeypatch: 仅在第二次 fstat 中模拟 size 代际变化。
    返回:
        无；快照记录 FILE_CHANGED_DURING_AUDIT 且内容不可授权。
    """

    path = tmp_path / "manifest.json"
    path.write_bytes(b"{}")
    real_fstat = os.fstat
    calls = {"count": 0}

    def _changing_fstat(descriptor: int) -> os.stat_result:
        """
        第二次调用返回 size 增一的合成 stat_result。

        参数:
            descriptor: `_read_filesystem_file` 持有的只读描述符。
        返回:
            首次为真实状态，第二次起为变更后的状态。
        """

        current = real_fstat(descriptor)
        calls["count"] += 1
        if calls["count"] == 1:
            return current
        values = list(current)
        values[6] = current.st_size + 1
        return os.stat_result(values)

    monkeypatch.setattr(release_audit.os, "fstat", _changing_fstat)
    builder = release_audit._AuditBuilder()

    item = release_audit._read_filesystem_file(
        path,
        "manifest.json",
        ReleaseAuditPolicy(),
        builder,
        strict_bundle=True,
        retain_source_path=False,
    )

    assert item.data is None
    assert item.sha256 is None
    assert "FILE_CHANGED_DURING_AUDIT" in {finding.code for finding in builder.findings}


@pytest.mark.unit
def test_native_dependency_and_rpath_traversal_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    验证 native 工具输出中的父目录依赖和裸相对 RPATH 不能伪装成安全路径。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 提供确定性的合成 readelf 输出。
    返回:
        无；依赖与 RPATH 均产生稳定规则码即通过。
    """

    native = tmp_path / "libbullet_trade_huaxin.so"
    native_bytes = _fake_elf_x86_64()
    native.write_bytes(native_bytes)
    builder = release_audit._AuditBuilder()
    output = (
        "(NEEDED) Shared library: [../attacker/libvendor.so]\n"
        "(NEEDED) Shared library: [/usr/lib/../../tmp/libevil.so]\n"
        "(NEEDED) Shared library: [@rpath/libwrong.so]\n"
        "(NEEDED) Shared library: [libTORAProprietary.so]\n"
        "(FILTER) Filter library: [/tmp/evil-filter.so]\n"
        "(RUNPATH) Library runpath: [:$ORIGIN::../../attacker:.:@loader_path/vendor]\n"
    )
    monkeypatch.setattr(release_audit.shutil, "which", lambda _name: "/usr/bin/readelf")
    monkeypatch.setattr(release_audit, "_run_native_command", lambda _command: (True, output))

    result = release_audit._inspect_native_path(
        native,
        "lib/libbullet_trade_huaxin.so",
        "elf",
        builder,
        dependency_profile="offline_fake",
        snapshot_data=native_bytes,
    )
    codes = {finding.code for finding in builder.findings}

    assert result["inspected"] is True
    assert "NATIVE_UNSAFE_DEPENDENCY" in codes
    assert "NATIVE_ABSOLUTE_DEPENDENCY" in codes
    assert "NATIVE_UNSAFE_RPATH" in codes
    assert "NATIVE_EMPTY_RPATH_ENTRY" in codes
    assert "NATIVE_LOADER_TOKEN_FORMAT_MISMATCH" in codes
    assert "OFFLINE_FAKE_DEPENDENCY_NOT_ALLOWED" in codes
    assert "OFFLINE_FAKE_LOADER_TAG_FORBIDDEN" in codes
    assert "OFFLINE_FAKE_RPATH_FORBIDDEN" in codes
    assert "dependencies" not in result


@pytest.mark.unit
def test_dynamic_library_image_contract_rejects_executables_pie_and_extended_elf() -> None:
    """
    验证 ELF/Mach-O/PE 改名可执行文件、PIE、PT_INTERP 与 PN_XNUM 均被拒绝。

    参数:
        无。
    返回:
        无；三平台有效头通过，所有反例产生对应稳定规则码。
    """

    valid_cases = (
        (_fake_elf_x86_64(False), "elf"),
        (_fake_macho_x86_64(), "mach_o"),
        (_fake_pe_x86_64(False), "pe"),
    )
    for payload, native_format in valid_cases:
        builder = release_audit._AuditBuilder()
        assert release_audit._audit_dynamic_library_image(
            payload,
            native_format,
            "lib/synthetic",
            builder,
        )
        assert builder.findings == []

    elf_executable = bytearray(_fake_elf_x86_64(False))
    elf_executable[16:18] = (2).to_bytes(2, "little")
    executable_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        bytes(elf_executable), "elf", "lib/synthetic.so", executable_builder
    )
    assert "NATIVE_IMAGE_TYPE_INVALID" in {finding.code for finding in executable_builder.findings}

    elf_interpreter = bytearray(_fake_elf_x86_64(False))
    elf_interpreter[64:68] = (3).to_bytes(4, "little")
    interpreter_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        bytes(elf_interpreter), "elf", "lib/synthetic.so", interpreter_builder
    )
    assert "NATIVE_ELF_INTERPRETER_FORBIDDEN" in {
        finding.code for finding in interpreter_builder.findings
    }

    elf_pie = bytearray(_fake_elf_x86_64(False))
    elf_pie.extend(b"\x00" * 16)
    elf_pie[96:104] = (32).to_bytes(8, "little")
    elf_pie[104:112] = (32).to_bytes(8, "little")
    elf_pie[120:128] = (0x6FFFFFFB).to_bytes(8, "little")
    elf_pie[128:136] = (0x08000000).to_bytes(8, "little")
    pie_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        bytes(elf_pie), "elf", "lib/synthetic.so", pie_builder
    )
    assert "NATIVE_ELF_PIE_FORBIDDEN" in {finding.code for finding in pie_builder.findings}

    elf_extended = bytearray(_fake_elf_x86_64(False))
    elf_extended[56:58] = (0xFFFF).to_bytes(2, "little")
    extended_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        bytes(elf_extended), "elf", "lib/synthetic.so", extended_builder
    )
    assert "NATIVE_IMAGE_LAYOUT_INVALID" in {finding.code for finding in extended_builder.findings}

    macho_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        _fake_macho_x86_64(file_type=2),
        "mach_o",
        "lib/synthetic.dylib",
        macho_builder,
    )
    assert "NATIVE_IMAGE_TYPE_INVALID" in {finding.code for finding in macho_builder.findings}

    pe_executable = bytearray(_fake_pe_x86_64(False))
    pe_executable[86:88] = (0).to_bytes(2, "little")
    pe_builder = release_audit._AuditBuilder()
    assert not release_audit._audit_dynamic_library_image(
        bytes(pe_executable), "pe", "lib/synthetic.dll", pe_builder
    )
    assert "NATIVE_IMAGE_TYPE_INVALID" in {finding.code for finding in pe_builder.findings}


@pytest.mark.unit
def test_elf_export_parser_rejects_weak_ifunc_local_and_hidden(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 ELF 导出仅接受 GLOBAL FUNC DEFAULT/PROTECTED 且必须 defined。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 注入确定性的 readelf dynsym 输出。
    返回:
        无；WEAK、IFUNC、LOCAL 与 HIDDEN 同名符号不能授权 flat C ABI。
    """

    artifact = tmp_path / "libsynthetic.so"
    payload = _fake_elf_x86_64()
    artifact.write_bytes(payload)
    lines = []
    variants = (
        ("FUNC", "WEAK", "DEFAULT", "11"),
        ("IFUNC", "GLOBAL", "DEFAULT", "11"),
        ("FUNC", "LOCAL", "DEFAULT", "11"),
        ("FUNC", "GLOBAL", "HIDDEN", "11"),
        ("FUNC", "GLOBAL", "DEFAULT", "UND"),
        ("OBJECT", "GLOBAL", "DEFAULT", "11"),
    )
    for index, (symbol, variant) in enumerate(
        zip(release_audit._EXPECTED_BRIDGE_SYMBOLS, variants), 1
    ):
        symbol_type, binding, visibility, section = variant
        lines.append(
            "{}: 0000000000000000 1 {} {} {} {} {}".format(
                index,
                symbol_type,
                binding,
                visibility,
                section,
                symbol.decode("ascii"),
            )
        )
    monkeypatch.setattr(
        release_audit.shutil,
        "which",
        lambda name: "/usr/bin/readelf" if name == "readelf" else None,
    )
    monkeypatch.setattr(
        release_audit, "_run_native_command", lambda _command: (True, "\n".join(lines))
    )
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        payload,
        "lib/libbullet_trade_huaxin.so",
        "elf",
        builder,
    )
    codes = {finding.code for finding in builder.findings}

    assert "BRIDGE_EXPORTS_INVALID_KIND" in codes
    assert "BRIDGE_EXPORTS_MISSING" in codes
    assert summary["bridge_export_count"] == 0


@pytest.mark.unit
@pytest.mark.parametrize("version_separator", ("@PRIVATE", "@@DEFAULT@EXTRA"))
def test_elf_export_parser_rejects_nondefault_or_malformed_symbol_versions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    version_separator: str,
) -> None:
    """
    验证 ELF 单 @ 非默认版本和畸形多版本后缀不能授权 flat C ABI。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 注入确定性的 readelf dynsym 输出。
        version_separator: 待附加到六个 ABI 名称后的版本片段。
    返回:
        无；所有同名版本符号均标为非法且必要导出保持缺失。
    """

    artifact = tmp_path / "libsynthetic.so"
    payload = _fake_elf_x86_64()
    artifact.write_bytes(payload)
    lines = [
        "{}: 0000000000001000 16 FUNC GLOBAL DEFAULT 11 {}{}".format(
            index,
            symbol.decode("ascii"),
            version_separator,
        )
        for index, symbol in enumerate(release_audit._EXPECTED_BRIDGE_SYMBOLS, 1)
    ]
    monkeypatch.setattr(
        release_audit.shutil,
        "which",
        lambda name: "/usr/bin/readelf" if name == "readelf" else None,
    )
    monkeypatch.setattr(
        release_audit,
        "_run_native_command",
        lambda _command: (True, "\n".join(lines)),
    )
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        payload,
        "lib/libbullet_trade_huaxin.so",
        "elf",
        builder,
    )
    codes = {finding.code for finding in builder.findings}

    assert "BRIDGE_EXPORTS_INVALID_KIND" in codes
    assert "BRIDGE_EXPORTS_MISSING" in codes
    assert summary["bridge_export_count"] == 0


@pytest.mark.unit
def test_macho_export_parser_rejects_weak_external_functions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 Mach-O weak external 即使位于 __TEXT/__text 也不能授权 flat C ABI。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 注入确定性的 ``nm -m -gU`` weak external 输出。
    返回:
        无；六个 weak 同名函数均为非法 kind 且强函数计数为零。
    """

    artifact = tmp_path / "libsynthetic.dylib"
    payload = _fake_macho_x86_64()
    artifact.write_bytes(payload)
    output = "\n".join(
        "0000000000001000 (__TEXT,__text) weak external _{}".format(symbol.decode("ascii"))
        for symbol in release_audit._EXPECTED_BRIDGE_SYMBOLS
    )
    monkeypatch.setattr(release_audit.shutil, "which", lambda _name: "/usr/bin/nm")
    monkeypatch.setattr(
        release_audit,
        "_run_native_command",
        lambda _command: (True, output),
    )
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        payload,
        "lib/libbullet_trade_huaxin.dylib",
        "mach_o",
        builder,
    )
    codes = {finding.code for finding in builder.findings}

    assert "BRIDGE_EXPORTS_INVALID_KIND" in codes
    assert "BRIDGE_EXPORTS_MISSING" in codes
    assert summary["bridge_export_count"] == 0


@pytest.mark.unit
@pytest.mark.parametrize(("executable", "forwarder"), ((False, False), (True, True)))
def test_pe_export_parser_rejects_data_and_forwarder_exports(
    tmp_path: Path,
    executable: bool,
    forwarder: bool,
) -> None:
    """
    验证 PE 同名导出必须指向可执行 section，且 export forwarder 不能冒充函数。

    参数:
        tmp_path: pytest 临时目录。
        executable: 测试导出是否位于可执行 section。
        forwarder: 测试导出是否为 forwarder。
    返回:
        无；数据和 forwarder 两类反例均被原始 export table 拒绝。
    """

    payload = _fake_pe_bridge_exports(executable=executable, forwarder=forwarder)
    artifact = tmp_path / "synthetic.dll"
    artifact.write_bytes(payload)
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        payload,
        "lib/bullet_trade_huaxin.dll",
        "pe",
        builder,
    )
    codes = {finding.code for finding in builder.findings}

    assert "BRIDGE_EXPORTS_INVALID_KIND" in codes
    assert "BRIDGE_EXPORTS_MISSING" in codes
    assert summary["bridge_export_count"] == 0
    assert summary["export_tool"] == "raw_pe"


@pytest.mark.unit
def test_pe_export_parser_accepts_only_executable_raw_export_rvas(tmp_path: Path) -> None:
    """
    验证 PE 原始 export table 六个名称全部落在可执行 section 时通过导出合同。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；不调用任何 PE 工具也能确认六个强函数 RVA。
    """

    payload = _fake_pe_bridge_exports(executable=True, forwarder=False)
    artifact = tmp_path / "synthetic.dll"
    artifact.write_bytes(payload)
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        payload,
        "lib/bullet_trade_huaxin.dll",
        "pe",
        builder,
    )

    assert builder.findings == []
    assert summary["bridge_export_count"] == len(release_audit._EXPECTED_BRIDGE_SYMBOLS)


@pytest.mark.unit
def test_macho_load_parser_skips_id_and_preserves_empty_rpath() -> None:
    """
    验证 Mach-O LC_ID_DYLIB 不计入依赖，LC_LOAD 与空 LC_RPATH 均精确保留。

    参数:
        无。
    返回:
        无；解析结果只含真实 load dependency，RPATH 空值不被过滤。
    """

    output = """
Load command 0
          cmd LC_ID_DYLIB
      cmdsize 64
         name @rpath/libbullet_trade_huaxin.dylib (offset 24)
Load command 1
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name /usr/lib/libSystem.B.dylib (offset 24)
Load command 2
          cmd LC_RPATH
      cmdsize 24
         path  (offset 12)
"""

    dependencies, rpaths, valid = release_audit._parse_macho_load_commands(output)

    assert valid is True
    assert dependencies == ["/usr/lib/libSystem.B.dylib"]
    assert rpaths == [""]


@pytest.mark.unit
def test_macho_load_parser_captures_lazy_dependency_and_rejects_loader_controls() -> None:
    """
    验证 Mach-O lazy dependency 不被漏审，且 dyld 环境/加载器命令 fail closed。

    参数:
        无。
    返回:
        无；LC_LAZY_LOAD_DYLIB 被提取，两个未授权 loader 命令均解析失败。
    """

    lazy_output = """
Load command 0
          cmd LC_LAZY_LOAD_DYLIB
      cmdsize 56
         name /tmp/liblazy-evil.dylib (offset 24)
"""
    dependencies, rpaths, valid = release_audit._parse_macho_load_commands(lazy_output)
    assert valid is True
    assert dependencies == ["/tmp/liblazy-evil.dylib"]
    assert rpaths == []

    for command in ("LC_DYLD_ENVIRONMENT", "LC_LOAD_DYLINKER"):
        forbidden_output = "Load command 0\n          cmd {}\n".format(command)
        assert release_audit._parse_macho_load_commands(forbidden_output) == ([], [], False)


@pytest.mark.unit
def test_macho_offline_fake_rejects_self_dependency_and_safe_rpath(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 offline_fake 的 Mach-O 自依赖和看似安全的 loader-relative RPATH 均被拒绝。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 注入强导出与包含自依赖/RPATH 的确定性工具输出。
    返回:
        无；精确依赖基线与零 RPATH 合同分别产生稳定规则码。
    """

    payload = _fake_macho_x86_64()
    artifact = tmp_path / "libsynthetic.dylib"
    artifact.write_bytes(payload)
    strong_exports = "\n".join(
        "0000000000001000 (__TEXT,__text) external _{}".format(symbol.decode("ascii"))
        for symbol in release_audit._EXPECTED_BRIDGE_SYMBOLS
    )
    load_commands = """
Load command 0
          cmd LC_ID_DYLIB
      cmdsize 64
         name @rpath/libbullet_trade_huaxin.dylib (offset 24)
Load command 1
          cmd LC_LAZY_LOAD_DYLIB
      cmdsize 64
         name @rpath/libbullet_trade_huaxin.dylib (offset 24)
Load command 2
          cmd LC_LOAD_DYLIB
      cmdsize 64
         name /usr/lib/libc++.1.dylib (offset 24)
Load command 3
          cmd LC_LOAD_DYLIB
      cmdsize 64
         name /usr/lib/libSystem.B.dylib (offset 24)
Load command 4
          cmd LC_RPATH
      cmdsize 48
         path @loader_path/vendor (offset 12)
"""
    monkeypatch.setattr(
        release_audit.shutil,
        "which",
        lambda name: "/usr/bin/" + name,
    )
    monkeypatch.setattr(
        release_audit,
        "_run_native_command",
        lambda command: (True, strong_exports if "-m" in command else load_commands),
    )
    builder = release_audit._AuditBuilder()

    result = release_audit._inspect_native_path(
        artifact,
        "lib/libbullet_trade_huaxin.dylib",
        "mach_o",
        builder,
        dependency_profile="offline_fake",
        snapshot_data=payload,
    )
    codes = {finding.code for finding in builder.findings}

    assert result["inspected"] is True
    assert "OFFLINE_FAKE_DEPENDENCY_NOT_ALLOWED" in codes
    assert "OFFLINE_FAKE_LOADER_COMMAND_FORBIDDEN" in codes
    assert "OFFLINE_FAKE_RPATH_FORBIDDEN" in codes


@pytest.mark.unit
def test_windows_offline_fake_dependency_profile_fails_closed_without_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证 Windows offline_fake 尚无精确依赖基线时即使 PE 结构正确也拒绝放行。

    参数:
        tmp_path: pytest 临时目录。
        monkeypatch: 注入 objdump 路径和受控依赖输出。
    返回:
        无；报告固定 baseline unavailable 规则码。
    """

    payload = _fake_pe_bridge_exports()
    artifact = tmp_path / "synthetic.dll"
    artifact.write_bytes(payload)
    monkeypatch.setattr(release_audit.shutil, "which", lambda _name: "/usr/bin/objdump")
    monkeypatch.setattr(
        release_audit,
        "_run_native_command",
        lambda _command: (True, "DLL Name: KERNEL32.dll\n"),
    )
    builder = release_audit._AuditBuilder()

    release_audit._inspect_native_path(
        artifact,
        "lib/bullet_trade_huaxin.dll",
        "pe",
        builder,
        dependency_profile="offline_fake",
        snapshot_data=payload,
    )

    assert "OFFLINE_FAKE_DEPENDENCY_BASELINE_UNAVAILABLE" in {
        finding.code for finding in builder.findings
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "script",
    (
        "import sys; sys.stdout.buffer.write(b'valid\\x00tail')",
        "import sys; sys.stdout.buffer.write(b'\\xff')",
        "import sys; sys.stdout.buffer.write(b'x' * (1024 * 1024 + 1))",
    ),
)
def test_native_command_output_is_bounded_strict_utf8_without_nul(script: str) -> None:
    """
    验证 native 工具输出含 NUL、非法 UTF-8 或超过硬上限时统一 fail closed。

    参数:
        script: 由当前 Python 离线生成确定性 stdout 的短脚本。
    返回:
        无；三类不受信输出均返回 ``(False, "")``。
    """

    assert release_audit._run_native_command([sys.executable, "-c", script]) == (False, "")


@pytest.mark.unit
def test_dynamic_export_inspector_rejects_same_named_data_globals(tmp_path: Path) -> None:
    """
    验证六个同名全局数据导出或普通字符串不能冒充 flat C ABI 函数导出。

    参数:
        tmp_path: pytest 临时目录。
    返回:
        无；真实动态导出表把同名 data 标为非法 kind 并保持函数缺失。
    副作用:
        在临时目录调用现有本机 C++ 编译器；工具链缺失时跳过。
    """

    system = platform.system().lower()
    if system not in {"darwin", "linux"}:
        pytest.skip("当前回归只在 Mach-O/ELF 工具链执行")
    compiler = shutil.which("c++") or shutil.which("clang++") or shutil.which("g++")
    if compiler is None:
        pytest.skip("本机缺少 C++ 编译器")
    compiler_path = str(compiler)
    if system == "darwin" and shutil.which("nm") is None:
        pytest.skip("本机缺少 Mach-O nm")
    if system == "linux" and not (shutil.which("readelf") or shutil.which("nm")):
        pytest.skip("本机缺少 ELF export inspector")
    source = tmp_path / "decoy.cpp"
    declarations = "\n".join(
        'extern "C" __attribute__((visibility("default"))) int {} = 1;'.format(
            symbol.decode("ascii")
        )
        for symbol in release_audit._EXPECTED_BRIDGE_SYMBOLS
    )
    source.write_text(declarations + "\n", encoding="utf-8")
    artifact = tmp_path / ("libdecoy.dylib" if system == "darwin" else "libdecoy.so")
    command = [compiler_path, "-dynamiclib", str(source), "-o", str(artifact)]
    native_format = "mach_o"
    if system == "linux":
        command = [compiler_path, "-shared", "-fPIC", str(source), "-o", str(artifact)]
        native_format = "elf"
    subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
    builder = release_audit._AuditBuilder()

    summary = release_audit._inspect_bridge_exports(
        artifact,
        artifact.read_bytes(),
        "lib/libbullet_trade_huaxin" + artifact.suffix,
        native_format,
        builder,
    )
    codes = {finding.code for finding in builder.findings}

    assert "BRIDGE_EXPORTS_INVALID_KIND" in codes
    assert "BRIDGE_EXPORTS_MISSING" in codes
    assert summary["bridge_export_count"] == 0


@pytest.mark.unit
def test_existing_offline_bundle_passes_dependency_and_rpath_audit(
    offline_bundle: BuildResult,
) -> None:
    """
    验证现有第一方 fake bundle 通过 manifest、magic、依赖与 RPATH 审计。

    参数:
        offline_bundle: 会话级本地 CMake 构建的自研 fake bundle。
    返回:
        无；报告通过且只检查一个 native 即通过。
    """

    report = audit_bundle(offline_bundle.bundle_path)

    assert report.passed is True
    assert report.artifact_kind == "native_bundle"
    assert len(report.native_inspection) == 1
    assert report.native_inspection[0]["inspected"] is True


@pytest.mark.unit
def test_size_policy_is_fail_closed_and_rejects_boolean_threshold() -> None:
    """
    验证体积策略拒绝布尔/非正门槛，避免配置关闭门禁。

    参数:
        无。
    返回:
        无；非法门槛均抛 ValueError 即通过。
    """

    with pytest.raises(ValueError):
        ReleaseAuditPolicy(universal_wheel_max_bytes=0)
    with pytest.raises(ValueError):
        ReleaseAuditPolicy(sdist_max_bytes=True)


@pytest.mark.unit
def test_empty_aggregate_is_not_a_successful_release_gate() -> None:
    """
    验证零 Git tree、零制品的空范围不能利用 all(empty) 返回成功。

    参数:
        无。
    返回:
        无；aggregate passed 为 False 即通过。
    """

    assert release_audit.aggregate_reports([])["passed"] is False


@pytest.mark.unit
def test_release_cli_rejects_invalid_project_and_empty_scope(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    验证 CLI 对非法项目根和跳过 Git 后的零制品范围返回稳定非零退出码。

    参数:
        tmp_path: pytest 提供的隔离目录。
        capsys: pytest 标准输出捕获器。
    返回:
        无；非法根返回 3、空范围返回 2 且报告 fail closed 即通过。
    """

    cli = _load_release_audit_cli()
    invalid_root = tmp_path / "missing-project"
    assert cli.main(["--project-root", str(invalid_root), "--skip-git", "--compact"]) == 3
    invalid_payload = json.loads(capsys.readouterr().out)
    assert invalid_payload["error"]["code"] == "PROJECT_ROOT_INVALID"

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pyproject.toml").write_text(
        '[project]\nname = "bullet-trade"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    assert cli.main(["--project-root", str(project_root), "--skip-git", "--compact"]) == 2
    empty_payload = json.loads(capsys.readouterr().out)
    assert empty_payload["passed"] is False
    assert empty_payload["reports"] == []


@pytest.mark.unit
def test_release_cli_requires_wheel_for_clean_import(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    验证显式 clean-import 没有 wheel 时形成失败证据，而不是静默跳过。

    参数:
        tmp_path: pytest 提供的隔离项目目录。
        capsys: pytest 标准输出捕获器。
    返回:
        无；退出码为 2 且 reason 为 CLEAN_IMPORT_SCOPE_EMPTY 即通过。
    """

    cli = _load_release_audit_cli()
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pyproject.toml").write_text(
        '[project]\nname = "bullet-trade"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )

    result = cli.main(
        [
            "--project-root",
            str(project_root),
            "--skip-git",
            "--clean-import",
            "--compact",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 2
    assert payload["passed"] is False
    assert payload["clean_imports"][0]["reason_code"] == "CLEAN_IMPORT_SCOPE_EMPTY"


@pytest.mark.unit
def test_release_cli_build_scope_rejects_old_or_incomplete_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    验证显式构建目录不能复用旧制品，且返回成功但产物集合不完整时受控失败。

    参数:
        tmp_path: pytest 提供的隔离项目和构建目录。
        capsys: pytest 标准输出捕获器。
        monkeypatch: pytest 属性替换器，用于模拟 backend 成功但不产出归档。
    返回:
        无；两种错误均返回 3 并给出稳定规则码即通过。
    """

    cli = _load_release_audit_cli()
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pyproject.toml").write_text(
        '[project]\nname = "bullet-trade"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    old_dist = tmp_path / "old-dist"
    old_dist.mkdir()
    (old_dist / "old.whl").write_bytes(b"old")

    old_result = cli.main(
        [
            "--project-root",
            str(project_root),
            "--build",
            "--dist-dir",
            str(old_dist),
            "--skip-git",
            "--compact",
        ]
    )
    old_payload = json.loads(capsys.readouterr().out)
    assert old_result == 3
    assert old_payload["error"]["code"] == "BUILD_OUTPUT_NOT_EMPTY"

    def _fake_empty_build(project: Path, output: Path) -> tuple:
        """
        模拟 backend 返回成功但没有生成 wheel/sdist。

        参数:
            project: 被测 CLI 传入的项目根。
            output: 被测 CLI 传入的空构建目录。
        返回:
            固定 ``(True, 0)``，让 CLI 自行验证产物集合。
        """

        assert project == project_root.resolve()
        output.mkdir(parents=True, exist_ok=True)
        return True, 0

    monkeypatch.setattr(cli, "_build_distributions", _fake_empty_build)
    empty_dist = tmp_path / "empty-dist"
    empty_result = cli.main(
        [
            "--project-root",
            str(project_root),
            "--build",
            "--dist-dir",
            str(empty_dist),
            "--skip-git",
            "--compact",
        ]
    )
    empty_payload = json.loads(capsys.readouterr().out)
    assert empty_result == 3
    assert empty_payload["error"]["code"] == "BUILD_ARTIFACT_SET_INVALID"
    assert empty_payload["error"]["wheel_count"] == 0
    assert empty_payload["error"]["sdist_count"] == 0
