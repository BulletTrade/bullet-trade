"""
作者: BruceLee
文件职责: 为华鑫第一方 native 单元测试构建一次共享的离线 fake bundle。
主要输入: pytest 临时目录与本机 CMake/C++ 工具链。
主要输出: 已通过内容指纹校验的 BuildResult 测试夹具。
上游关系: pytest 收集 tests/unit/integrations/huaxin 下的测试时按需调用。
下游关系: build_native_bridge，仅编译包内自研 C++ 源码。
关键环境或配置: 缺少工具链时跳过 native 构建测试；不需要厂商 SDK、网络或凭据。
"""

from __future__ import annotations

import shutil

import pytest

from bullet_trade.integrations.huaxin import BuildResult, build_native_bridge


@pytest.fixture(scope="session")
def offline_bundle(tmp_path_factory: pytest.TempPathFactory) -> BuildResult:
    """
    构建并返回本次测试会话共享的离线 fake bundle。

    参数:
        tmp_path_factory: pytest 提供的会话级临时目录工厂。
    返回:
        已构建并完成完整性校验的 BuildResult。
    副作用:
        在 pytest 临时目录调用本地 CMake/C++ 编译器；缺少工具时跳过。
    """

    if shutil.which("cmake") is None or not any(
        shutil.which(candidate) for candidate in ("c++", "g++", "clang++", "cl")
    ):
        pytest.skip("本机缺少 CMake 或 C++ 编译器")
    prefix = tmp_path_factory.mktemp("huaxin-native") / "explicit-prefix"
    return build_native_bridge(prefix=prefix, mode="offline_fake")
