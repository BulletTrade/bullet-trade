#!/usr/bin/python
# coding=utf-8

"""
作者: BruceLee
文件职责:
    验证 LiveEngine 面向 Windows 控制台的日志模板可由 GBK 编码。
主要输入:
    bullet_trade.core.live_engine 源码中的 log/print 首参数字符串。
主要输出:
    非 GBK 日志装饰字符的失败断言。
上下游关系:
    上游为 LiveEngine 运行日志；下游为 Windows GBK 控制台和文件日志 handler。
关键环境约定:
    仅静态解析源码，不启动引擎、不连接 provider/broker，也不写运行状态。
"""

from __future__ import annotations

import ast
from pathlib import Path


def _literal_fragments(node: ast.AST) -> list[str]:
    """提取日志首参数中的静态字符串片段。

    Args:
        node: log/print 调用的首参数 AST 节点。

    Returns:
        list[str]: 常量字符串及 f-string 静态片段；动态表达式不在检查范围内。
    """

    return [
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    ]


def _is_runtime_output_call(node: ast.Call) -> bool:
    """判断调用是否为 LiveEngine 的日志或控制台输出。

    Args:
        node: 待判定的函数调用 AST 节点。

    Returns:
        bool: log 各级别方法或 print 调用返回 True。
    """

    if isinstance(node.func, ast.Name):
        return node.func.id == "print"
    return (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "log"
        and node.func.attr in {"debug", "info", "warning", "error", "exception", "critical"}
    )


def test_live_engine_output_templates_are_gbk_encodable() -> None:
    """LiveEngine 的静态输出模板不得再触发 Windows GBK 编码异常。

    Returns:
        None: 所有 log/print 静态字符串片段均能 GBK 编码。
    """

    source_path = Path(__file__).resolve().parents[2] / "bullet_trade" / "core" / "live_engine.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    failures: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args or not _is_runtime_output_call(node):
            continue
        for fragment in _literal_fragments(node.args[0]):
            try:
                fragment.encode("gbk")
            except UnicodeEncodeError:
                failures.append(f"line={node.lineno} text={fragment!r}")
    assert failures == []
