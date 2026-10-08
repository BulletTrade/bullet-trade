"""
作者: BruceLee
文件职责: 提供 bullet-trade gm doctor 和 probe 的环境与只读连接诊断。
主要输入: 子命令、SDK 加载开关、查询范围和超时。
主要输出: 脱敏 JSON 及退出码；0 为环境检查通过，2 为环境缺失或导入失败。
上下游关系: 主 CLI 或模块入口调用本模块，本模块调用 environment.doctor 或 probe。
关键配置: doctor 默认离线；probe 显式查询行情和可选账户，不提供交易动作。
"""

from __future__ import annotations

import argparse
import json
from typing import Optional, Sequence

from .environment import doctor
from .probe import probe


def configure_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """为已有命令解析器注册掘金环境诊断参数。

    参数:
        parser: 主 CLI 创建的解析器。
    返回:
        已配置的同一个解析器，解析过程不加载 SDK。
    """

    subparsers = parser.add_subparsers(dest="gm_command", required=True)
    diagnostic = subparsers.add_parser("doctor", help="离线检查掘金 SDK 安装和配置是否存在")
    diagnostic.add_argument(
        "--load-sdk", action="store_true", help="显式在子进程导入 gm.api，不调用业务 API"
    )
    diagnostic.add_argument("--timeout", type=float, default=10.0, help="SDK 导入超时秒数")
    readonly = subparsers.add_parser("probe", help="显式联网查询行情及可选账户，不下单或撤单")
    readonly.add_argument("--symbol", default="SHSE.510300", help="SHSE/SZSE 六位证券代码")
    readonly.add_argument("--start", required=True, help="日线开始日期 YYYY-MM-DD")
    readonly.add_argument("--end", required=True, help="日线结束日期 YYYY-MM-DD")
    readonly.add_argument(
        "--account", action="store_true", help="显式查询 GM_ACCOUNT_ID 的资金与持仓"
    )
    readonly.add_argument(
        "--timeout", type=float, default=30.0, help="整体查询超时秒数，最多 120 秒"
    )
    return parser


def run_arguments(args: argparse.Namespace) -> int:
    """执行已解析的环境诊断或只读探针，并输出脱敏结果。

    参数:
        args: 含子命令、查询参数和 timeout 的命令参数。
    返回:
        0 表示环境可用且显式导入检查通过；2 表示缺失或失败。
        doctor 未要求导入时，0 仅说明平台和安装元信息通过。
        probe 的 0 只表示有限查询通过，不表示交易能力通过。
    副作用:
        输出脱敏 JSON；仅在显式要求时启动 SDK 导入子进程。
    """

    try:
        if args.gm_command == "probe":
            result = probe(
                args.symbol, args.start, args.end, account=args.account, timeout=args.timeout
            )
        else:
            result = doctor(load_sdk=args.load_sdk, timeout=args.timeout)
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.gm_command == "probe":
        return 0 if result["ok"] else 2
    import_ok = not args.load_sdk or result["sdk_import"]["status"] == "ok"
    return 0 if result["environment_available"] and import_ok else 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    """运行独立的掘金诊断模块入口。

    参数:
        argv: 可选参数序列，默认读取命令行。
    返回:
        诊断退出码。
    副作用:
        解析参数并按显式请求执行诊断。
    """

    parser = configure_parser(argparse.ArgumentParser(prog="bullet-trade gm"))
    return run_arguments(parser.parse_args(argv))


__all__ = ["configure_parser", "run_arguments", "main"]
