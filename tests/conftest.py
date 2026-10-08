"""
Pytest 全局配置与命令行参数（tests 作用域）。

新增参数：
- --live-providers=jqdata,tushare,qmt 用于在线用例的 Provider 指定（默认 jqdata）。
- 兼容旧用法：--requires-network / --requires-jqdata（等价于 -m 过滤），便于脚本沿用。

示例：
- 只跑联网用例并指定 jqdata：
  pytest -q -m requires_network bullet-trade/tests/unit/test_exec_and_dividends_reference.py --live-providers=jqdata
- 旧用法等价：
  pytest -q --requires-network bullet-trade/tests/unit/test_exec_and_dividends_reference.py --live-providers=jqdata
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--live-providers",
        action="store",
        default="jqdata",
        help="Comma separated provider list for online tests (e.g. jqdata,tushare,miniqmt)",
    )
    parser.addoption(
        "--gm-parity-report",
        action="store",
        default=None,
        help="Explicit live GM versus JoinQuant RPC report produced by probe_gm_parity.py",
    )
    parser.addoption(
        "--gm-adapter-report",
        action="store",
        default=None,
        help="Explicit GM capture/review report from probe_gm_adapter.py or review_gm_acceptance.py",
    )
    parser.addoption(
        "--gm-acceptance-profile",
        choices=["practical", "strict"],
        default="practical",
        help="GM acceptance: practical tolerances/common post basis, or original strict diagnostic",
    )
    # 兼容历史脚本：将选项映射为标记过滤
    parser.addoption(
        "--requires-network",
        action="store_true",
        default=False,
        help="Compatibility alias: filter tests to requires_network",
    )
    parser.addoption(
        "--requires-jqdata",
        action="store_true",
        default=False,
        help="Compatibility alias: filter tests to requires_jqdata",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    # 动态参数化在线提供者
    if "provider_name" in metafunc.fixturenames:
        opt = metafunc.config.getoption("--live-providers") or "jqdata"
        providers = [p.strip() for p in str(opt).split(",") if p.strip()]
        metafunc.parametrize("provider_name", providers, scope="session")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """默认不运行联网用例；仅在用户显式请求时才纳入执行。

    显式请求的两种方式：
    - 标准：-m requires_network（或 and requires_jqdata 组合）
    - 兼容：--requires-network / --requires-jqdata
    """
    # 是否显式要求运行联网用例
    marker_expr = str(config.getoption("-m") or "")
    want_network = ("requires_network" in marker_expr) or bool(
        config.getoption("--requires-network")
    )
    want_jq = ("requires_jqdata" in marker_expr) or bool(config.getoption("--requires-jqdata"))

    deselected: list[pytest.Item] = []
    kept: list[pytest.Item] = []

    for item in items:
        marks = {m.name for m in item.iter_markers()}
        is_net = "requires_network" in marks
        is_jq = "requires_jqdata" in marks

        # 默认丢弃所有联网用例（含 requires_jqdata），除非显式请求
        if is_net or is_jq:
            # 若用户表达了网络需求，再根据更细粒度筛选
            if want_network or want_jq:
                if want_jq and is_jq:
                    kept.append(item)
                elif want_network and not want_jq:
                    kept.append(item)
                else:
                    deselected.append(item)
            else:
                deselected.append(item)
        else:
            kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = kept


@pytest.fixture(autouse=True)
def _reset_core_state():
    """
    为每个用例重置核心全局状态，避免相互污染。
    - 清空 g、策略设置、订单队列
    - 解除当前引擎和数据上下文
    """
    from bullet_trade.core.globals import reset_globals
    from bullet_trade.core.orders import clear_order_queue
    from bullet_trade.core.runtime import set_current_engine
    from bullet_trade.core.settings import reset_settings
    from bullet_trade.data import api as data_api

    reset_globals()
    reset_settings()
    clear_order_queue()
    set_current_engine(None)
    try:
        data_api.set_current_context(None)
    except Exception:
        pass

    yield

    clear_order_queue()
    set_current_engine(None)
    try:
        data_api.set_current_context(None)
    except Exception:
        pass
