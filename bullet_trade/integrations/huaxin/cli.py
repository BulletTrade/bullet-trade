"""
作者: BruceLee
文件职责: 提供华鑫 native 基础切片的显式 doctor 与离线构建命令。
主要输入: 命令行中的 bundle、构建 prefix、构建类型和受控超时。
主要输出: 可机器读取的脱敏 JSON 诊断或内容寻址 bundle 信息。
上游关系: ``bullet-trade huaxin`` 主命令或模块入口按需调用。
下游关系: build.py 的 doctor/build_native_bridge；不直接调用 native 或厂商 API。
关键环境或配置: 只接受 offline_fake 构建，不读取凭据、不联网、不连接柜台或交易。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .build import build_native_bridge, doctor
from .errors import HuaxinError


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """向已有 parser 注入华鑫 doctor/build 子命令。

    参数:
        parser: 主 CLI 创建或模块入口创建的华鑫命令解析器。
    返回:
        已配置完成的同一 ``ArgumentParser`` 对象。
    副作用:
        只注册 argparse 参数，不执行 doctor、构建、dlopen、网络或 SDK 操作。
    """

    subparsers = parser.add_subparsers(dest="huaxin_command", required=True)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="检查源码、工具链和可选 bundle；不会连接柜台",
    )
    doctor_parser.add_argument(
        "--bundle",
        type=Path,
        help="待校验的内容寻址 native bundle 目录",
    )
    doctor_parser.add_argument(
        "--load",
        action="store_true",
        help="完整性校验后显式 dlopen 自研 bridge，但不创建 runtime",
    )

    build_parser = subparsers.add_parser(
        "build",
        help="从包内自研源码显式构建离线 fake bridge",
    )
    build_parser.add_argument(
        "--prefix",
        type=Path,
        required=True,
        help="站点包和源码包之外的构建输出根目录",
    )
    build_parser.add_argument(
        "--offline-fake",
        action="store_true",
        required=True,
        help="确认仅构建不连接厂商 SDK 的离线 fake bridge",
    )
    build_parser.add_argument(
        "--build-type",
        choices=("Release", "RelWithDebInfo", "Debug"),
        default="Release",
        help="CMake 构建类型，默认 Release",
    )
    build_parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="每个 CMake 步骤的超时秒数，默认 300",
    )
    return parser


def create_parser() -> argparse.ArgumentParser:
    """
    创建华鑫 native 子命令解析器。

    参数:
        无。
    返回:
        含 doctor 和 build 子命令的 ArgumentParser。
    """

    parser = argparse.ArgumentParser(
        prog="bullet-trade huaxin",
        description="华鑫第一方 native bridge 的离线诊断与显式构建工具",
    )
    return configure_parser(parser)


def _print_json(value: Mapping[str, Any], stream: Any = None) -> None:
    """
    以稳定格式输出一条 JSON 文档。

    参数:
        value: 仅含可 JSON 序列化内容的映射。
        stream: 可选文本输出流，默认标准输出。
    返回:
        无返回值。
    副作用:
        向指定文本流写入 UTF-8 友好的 JSON 和换行。
    """

    target = sys.stdout if stream is None else stream
    print(json.dumps(dict(value), ensure_ascii=False, sort_keys=True, indent=2), file=target)


def _run_doctor(arguments: argparse.Namespace) -> int:
    """
    执行 doctor 并按离线 bridge 完整性返回退出码。

    参数:
        arguments: 已解析的 bundle 与 load 参数。
    返回:
        bundle 可用于离线验证时返回 0，否则返回 2。
    副作用:
        输出脱敏 doctor JSON；load=True 时可能显式 dlopen 自研动态库。
    """

    report = doctor(bundle_path=arguments.bundle, load=arguments.load)
    _print_json(report.to_dict())
    return 0 if report.offline_bridge_ready else 2


def _run_build(arguments: argparse.Namespace) -> int:
    """
    执行显式 offline_fake 构建并输出内容寻址结果。

    参数:
        arguments: 已解析的 prefix、build_type 与 timeout 参数。
    返回:
        构建并校验成功时返回 0。
    副作用:
        在显式 prefix 下调用 CMake/C++ 编译器并发布 bundle。
    """

    result = build_native_bridge(
        prefix=arguments.prefix,
        mode="offline_fake",
        build_type=arguments.build_type,
        timeout_seconds=arguments.timeout,
    )
    _print_json(result.to_dict())
    return 0


def run_arguments(arguments: argparse.Namespace) -> int:
    """执行主 CLI 或模块入口已经解析的华鑫参数。

    参数:
        arguments: 含 ``huaxin_command`` 及对应子命令参数的 namespace。
    返回:
        成功为 0，诊断未就绪或结构化失败为 2。
    副作用:
        doctor 输出 JSON；只有 build 或 doctor --load 才显式执行本地 native 动作。
    """

    try:
        if arguments.huaxin_command == "doctor":
            return _run_doctor(arguments)
        if arguments.huaxin_command == "build":
            return _run_build(arguments)
    except HuaxinError as exc:
        _print_json({"ok": False, "error": exc.to_dict()}, stream=sys.stderr)
        return 2
    raise ValueError(f"未知华鑫命令: {arguments.huaxin_command}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    解析并执行华鑫 native 离线子命令。

    参数:
        argv: 可选参数序列；缺省读取当前进程命令行。
    返回:
        成功为 0，诊断未就绪或结构化失败为 2。
    副作用:
        输出 JSON；只有 build 或 doctor --load 才会显式触发本地 native 动作。
    """

    parser = create_parser()
    arguments = parser.parse_args(argv)
    try:
        return run_arguments(arguments)
    except ValueError as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
