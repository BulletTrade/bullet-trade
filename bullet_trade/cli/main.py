#!/usr/bin/env python
"""
BulletTrade 命令行主入口

用法:
    bullet-trade --help
    bullet-trade backtest strategy.py --start 2023-01-01 --end 2023-12-31
    bullet-trade optimize strategy.py --params params.json
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bullet_trade.core.globals import Logger


def apply_cli_overrides(args, logger: Optional["Logger"] = None) -> Dict[str, str]:
    """
    应用 CLI 覆写逻辑，返回可传入 LiveConfig 的额外配置。
    """
    overrides: Dict[str, str] = {}

    log_dir = getattr(args, 'log_dir', None)
    if log_dir:
        resolved = str(Path(log_dir).expanduser().resolve())
        os.environ['LOG_DIR'] = resolved
        if logger is None:
            from bullet_trade.core.globals import log as global_log
            logger = global_log
        logger.configure_file_logging(log_dir=resolved)

    runtime_dir = getattr(args, 'runtime_dir', None)
    if runtime_dir:
        resolved = str(Path(runtime_dir).expanduser().resolve())
        os.environ['RUNTIME_DIR'] = resolved
        overrides['runtime_dir'] = resolved

    return overrides


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        prog='bullet-trade',
        description='BulletTrade - 专业的量化交易系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 运行回测
  bullet-trade backtest strategy.py --start 2023-01-01 --end 2023-12-31

  # 参数优化
  bullet-trade optimize strategy.py --params params.json
  
  # 查看版本
  bullet-trade --version
  
  更多信息请访问: https://github.com/BulletTrade/bullet-trade
               https://bullettrade.cn/
        """
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.1.0'
    )

    # 全局 env 文件参数（对所有子命令生效）
    parser.add_argument(
        '--env-file',
        dest='env_file',
        default=None,
        help='指定要加载的 .env 文件（优先于默认搜索）'
    )
    
    subparsers = parser.add_subparsers(
        title='commands',
        description='可用命令',
        dest='command',
        help='命令帮助'
    )
    
    # backtest 命令
    backtest_parser = subparsers.add_parser(
        'backtest',
        help='运行策略回测'
    )
    backtest_parser.add_argument(
        'strategy_file',
        type=str,
        help='策略文件路径'
    )
    backtest_parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='回测开始日期 (YYYY-MM-DD)'
    )
    backtest_parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='回测结束日期 (YYYY-MM-DD)'
    )
    backtest_parser.add_argument(
        '--cash',
        type=float,
        default=100000,
        help='初始资金 (默认: 100000)'
    )
    backtest_parser.add_argument(
        '--frequency',
        type=str,
        choices=['day', 'minute'],
        default='day',
        help='回测频率: day=日线回测, minute=分钟回测 (默认: day)'
    )
    backtest_parser.add_argument(
        '--benchmark',
        type=str,
        default=None,
        help='基准指数代码 (如: 000300.XSHG)'
    )
    backtest_parser.add_argument(
        '--output',
        type=str,
        default='./backtest_results',
        help='输出目录 (默认: ./backtest_results)'
    )
    backtest_parser.add_argument(
        '--log',
        type=str,
        default=None,
        help='日志文件路径'
    )
    backtest_parser.add_argument(
        '--images',
        dest='generate_images',
        action='store_true',
        help='生成PNG图表输出 (默认关闭，以提升速度)'
    )
    backtest_parser.add_argument(
        '--no-csv',
        dest='generate_csv',
        action='store_false',
        help='不导出CSV明细 (默认导出)'
    )
    backtest_parser.add_argument(
        '--no-html',
        dest='generate_html',
        action='store_false',
        help='不生成交互式HTML报告 (默认生成)'
    )
    backtest_parser.add_argument(
        '--no-logs',
        dest='generate_logs',
        action='store_false',
        help='仅在终端打印日志，不写日志文件 (默认写入输出目录)'
    )
    backtest_parser.set_defaults(
        generate_images=False,
        generate_csv=True,
        generate_html=True,
        generate_logs=True,
        auto_report=False,
        report_format='html',
        report_template=None,
        report_metrics=None,
        report_title=None,
    )
    backtest_parser.add_argument(
        '--auto-report',
        action='store_true',
        help='回测完成后自动生成标准化报告'
    )
    backtest_parser.add_argument(
        '--report-format',
        choices=['html', 'pdf'],
        default='html',
        help='自动报告格式 (默认: html)'
    )
    backtest_parser.add_argument(
        '--report-template',
        type=str,
        default=None,
        help='自定义报告模板路径'
    )
    backtest_parser.add_argument(
        '--report-metrics',
        type=str,
        default=None,
        help='报告中展示的指标名称列表（逗号分隔）'
    )
    backtest_parser.add_argument(
        '--report-title',
        type=str,
        default=None,
        help='报告标题（默认使用输出目录名称）'
    )
    
    # optimize 命令
    optimize_parser = subparsers.add_parser(
        'optimize',
        help='参数优化'
    )
    optimize_parser.add_argument(
        'strategy_file',
        type=str,
        help='策略文件路径'
    )
    optimize_parser.add_argument(
        '--params',
        type=str,
        required=True,
        help='参数配置文件 (JSON)'
    )
    optimize_parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='回测开始日期'
    )
    optimize_parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='回测结束日期'
    )
    optimize_parser.add_argument(
        '--processes',
        type=int,
        default=None,
        help='并行进程数 (默认: CPU核心数)'
    )
    optimize_parser.add_argument(
        '--output',
        type=str,
        default='./optimization_results.csv',
        help='输出CSV文件路径'
    )
    
    # live 命令（待实现）
    live_parser = subparsers.add_parser(
        'live',
        help='实盘交易 (开发中)'
    )
    live_parser.add_argument(
        'strategy_file',
        type=str,
        help='策略文件路径'
    )
    live_parser.add_argument(
        '--broker',
        type=str,
        choices=['qmt', 'qmt-remote', 'simulator'],
        default=None,
        help='券商类型（默认读取 DEFAULT_BROKER）'
    )
    live_parser.add_argument(
        '--log-dir',
        type=str,
        default=None,
        help='覆盖 LOG_DIR，优先于 .env'
    )
    live_parser.add_argument(
        '--runtime-dir',
        type=str,
        default=None,
        help='覆盖 RUNTIME_DIR，优先于 .env'
    )

    # report 命令
    report_parser = subparsers.add_parser(
        'report',
        help='根据回测结果目录生成标准化报告'
    )
    report_parser.add_argument(
        '--input',
        '-i',
        required=True,
        help='回测结果目录'
    )
    report_parser.add_argument(
        '--output',
        '-o',
        type=str,
        default=None,
        help='输出文件路径（默认写入回测目录）'
    )
    report_parser.add_argument(
        '--format',
        '-f',
        choices=['html', 'pdf'],
        default='html',
        help='报告格式 (默认: html)'
    )
    report_parser.add_argument(
        '--template',
        type=str,
        default=None,
        help='自定义HTML模板文件路径'
    )
    report_parser.add_argument(
        '--metrics',
        type=str,
        default=None,
        help='展示的指标名称，使用逗号分隔'
    )
    report_parser.add_argument(
        '--title',
        type=str,
        default=None,
        help='报告标题'
    )
    

    # server 命令
    server_parser = subparsers.add_parser(
        'server',
        help='启动远程数据/券商服务'
    )
    server_parser.add_argument(
        '--server-type',
        dest='server_type',
        default='qmt',
        help='服务类型（默认 qmt，可扩展）'
    )
    server_parser.add_argument(
        '--listen',
        dest='listen',
        default=None,
        help='监听地址（覆盖 QMT_SERVER_LISTEN）'
    )
    server_parser.add_argument(
        '--port',
        dest='port',
        type=int,
        default=None,
        help='监听端口（覆盖 QMT_SERVER_PORT）'
    )
    server_parser.add_argument(
        '--token',
        dest='token',
        default=None,
        help='访问 token（覆盖 QMT_SERVER_TOKEN）'
    )
    server_parser.add_argument(
        '--tls-cert',
        dest='tls_cert',
        default=None,
        help='TLS 证书路径（可选）'
    )
    server_parser.add_argument(
        '--tls-key',
        dest='tls_key',
        default=None,
        help='TLS 私钥路径（可选）'
    )
    server_parser.add_argument(
        '--enable-data',
        dest='enable_data',
        action='store_true',
        default=None,
        help='强制启用数据服务'
    )
    server_parser.add_argument(
        '--disable-data',
        dest='enable_data',
        action='store_false',
        help='强制禁用数据服务'
    )
    server_parser.add_argument(
        '--enable-broker',
        dest='enable_broker',
        action='store_true',
        default=None,
        help='强制启用券商服务'
    )
    server_parser.add_argument(
        '--disable-broker',
        dest='enable_broker',
        action='store_false',
        help='强制禁用券商服务'
    )
    server_parser.add_argument(
        '--allowlist',
        dest='allowlist',
        default=None,
        help='允许访问的 IP 列表（逗号分隔）'
    )
    server_parser.add_argument(
        '--max-connections',
        dest='max_connections',
        type=int,
        default=None,
        help='最大并发连接数'
    )
    server_parser.add_argument(
        '--max-subscriptions',
        dest='max_subscriptions',
        type=int,
        default=None,
        help='单会话 tick 订阅上限'
    )
    server_parser.add_argument(
        '--accounts',
        dest='accounts',
        default=None,
        help='多账户配置（例: main=123456:stock,hedge=654321:future）'
    )
    server_parser.add_argument(
        '--sub-accounts',
        dest='sub_accounts',
        default=None,
        help='虚拟子账户配置（例: demo@main:limit=1000000,test@hedge）'
    )
    server_parser.add_argument(
        '--log-file',
        dest='log_file',
        default=None,
        help='将服务器日志写入指定文件（默认仅输出到控制台）'
    )
    server_parser.add_argument(
        '--log-account-overview',
        dest='log_account_snapshot',
        action='store_true',
        default=None,
        help='允许 admin.print_account 将账户快照打印到日志'
    )
    server_parser.add_argument(
        '--no-log-account-overview',
        dest='log_account_snapshot',
        action='store_false',
        help='禁止账户快照打印到日志'
    )
    server_parser.add_argument(
        '--access-log',
        dest='access_log',
        action='store_true',
        default=None,
        help='显式开启请求访问日志'
    )
    server_parser.add_argument(
        '--no-access-log',
        dest='access_log',
        action='store_false',
        help='关闭请求访问日志'
    )
    return parser


def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()
    # 若提供 env 文件，覆盖加载一次，便于区分客户端/服务端环境
    try:
        if getattr(args, 'env_file', None):
            from bullet_trade.utils.env_loader import load_env  # 延迟导入，避免循环
            load_env(env_file=args.env_file, override=True)
    except Exception:
        pass
    overrides = apply_cli_overrides(args) if getattr(args, 'command', None) else {}

    if not args.command:
        parser.print_help()
        return 0
    
    # 导入命令处理模块
    if args.command == 'backtest':
        from .backtest import run_backtest
        return run_backtest(args)
    elif args.command == 'optimize':
        from .optimize import run_optimize
        return run_optimize(args)
    elif args.command == 'report':
        from .report import run_report
        return run_report(args)
    elif args.command == 'live':
        from .live import run_live
        return run_live(args, live_config_override=(overrides or None))
    elif args.command == 'server':
        from bullet_trade.server.cli import run_server_command
        return run_server_command(args)
    else:
        print(f"未知命令: {args.command}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
