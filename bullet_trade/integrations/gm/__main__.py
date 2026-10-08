"""
作者: BruceLee
文件职责: 提供 python -m bullet_trade.integrations.gm 入口。
主要输入: 命令行参数；主要输出: 诊断 JSON 与退出码。
上下游关系: Python 模块入口调用 cli.main。
关键配置: 环境诊断按显式参数执行，不自动连接终端。
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
