"""
作者: BruceLee
文件职责: 提供掘金集成的命名空间，当前包含环境诊断入口。
主要输入: BulletTrade CLI 或用户的显式模块导入。
主要输出: 掘金集成模块；本模块导入不加载厂商 SDK。
上下游关系: CLI 调用 environment 与 cli；后续数据和交易适配复用此命名空间。
关键配置: gm 是 Windows/Linux 可选依赖，Mac 可运行开发与离线测试。
"""

__all__ = []
