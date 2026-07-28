"""
作者：BruceLee

文件职责：验证回测当前行情、保护价与撮合行情共享同一有效价格口径。
主要输入：离线可控行情提供者、回测业务时刻、策略价格设置与订单。
主要输出：订单状态、成交价格、费用以及 provider 调用口径断言。
上下游关系：上游模拟 DataProvider 行情合同，下游覆盖 BacktestCurrentData 与 BacktestEngine。
关键约定：测试不访问网络、不读取真实账号配置，也不使用展示报告反推撮合事实。
"""

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.models import Context, OrderStatus, Portfolio
from bullet_trade.core.orders import MarketOrderStyle, clear_order_queue, order
from bullet_trade.core.price_basis import (
    EffectivePriceBasis,
    PriceBasisDataError,
    PriceBasisUnsupportedError,
    resolve_effective_price_basis,
)
from bullet_trade.core.settings import reset_settings, set_option
from bullet_trade.data import api as data_api
from bullet_trade.data.providers.base import DataProvider


class CorporateActionPriceProvider(DataProvider):
    """提供除权前后价差的离线行情，并记录每次行情口径请求。"""

    name = "corporate-action-fixture"

    def __init__(
        self,
        *,
        current_price: float = 100.0,
        has_corporate_action: bool = True,
        category: str = "stock",
    ) -> None:
        """初始化行情场景和调用记录；不建立网络连接。

        Args:
            current_price: 参考日未复权真实价格。
            has_corporate_action: 是否让无参考日的前复权价格与真实价格产生二倍差异。
            category: get_security_info 返回的证券分类。

        Returns:
            None。

        Side Effects:
            仅初始化进程内字段。
        """

        self.calls: List[Dict[str, Any]] = []
        self.current_price = float(current_price)
        self.has_corporate_action = bool(has_corporate_action)
        self.category = str(category)

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """完成离线认证占位；输入被忽略，返回值为 None。"""

        _ = user, pwd, host, port

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        """按请求复权口径返回单行行情；前复权价 50，未复权价 100。"""

        self.calls.append(
            {
                "security": security,
                "frequency": frequency,
                "fq": fq,
                "pre_factor_ref_date": pre_factor_ref_date,
            }
        )
        _ = start_date, end_date, fields, skip_paused, count, panel, fill_paused
        _ = prefer_engine, force_no_engine
        try:
            reference_date = pd.to_datetime(pre_factor_ref_date).date()
        except Exception:
            reference_date = None
        is_current_reference = reference_date == date(2024, 6, 12)
        if fq == "pre" and self.has_corporate_action and not is_current_reference:
            price = self.current_price / 2.0
        else:
            price = self.current_price
        return pd.DataFrame(
            {
                "open": [price],
                "close": [price],
                "high_limit": [price * 1.1],
                "low_limit": [price * 0.9],
                "paused": [False],
            },
            index=[pd.Timestamp("2024-06-12 15:00:00")],
        )

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        """返回固定交易日；输入仅用于满足 DataProvider 合同。"""

        _ = start_date, end_date, count
        return [datetime(2024, 6, 12)]

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """返回空证券表；本测试不依赖证券全集。"""

        _ = types, date
        return pd.DataFrame()

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        """返回空成分列表；本测试不依赖指数成分。"""

        _ = index_symbol, date
        return []

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """返回一个历史公司行动，用于说明前复权与未复权价格存在差异。"""

        _ = start_date, end_date
        return [
            {
                "security": security,
                "date": datetime(2024, 6, 12).date(),
                "security_type": "stock",
                "scale_factor": 2.0,
                "bonus_pre_tax": 0.0,
                "per_base": 10,
            }
        ]

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """返回股票分类；输入日期不影响固定测试结果。"""

        _ = security, date
        if self.category == "money_market_fund":
            return {"type": "fund", "subtype": "money_market_fund"}
        return {"type": self.category}


class DynamicPriceProvider(CorporateActionPriceProvider):
    """明确承诺支持动态前复权且在参考日返回 raw 等价值的离线 provider。"""

    supports_dynamic_pre_price_basis = True


class BrokenDynamicPriceProvider(DynamicPriceProvider):
    """虚构动态能力但返回错误参考点，用于验证运行时等价检查失败关闭。"""

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        """在动态前复权请求中故意返回 50，在未复权请求中返回 100。

        Args:
            security: 证券代码。
            start_date: 请求起始时间。
            end_date: 请求结束时间。
            frequency: 行情频率。
            fields: 请求字段。
            skip_paused: 是否跳过停牌。
            fq: 复权口径。
            count: 请求条数。
            panel: 是否返回宽表。
            fill_paused: 是否填充停牌数据。
            pre_factor_ref_date: 动态前复权参考日。
            prefer_engine: 是否优先 provider 引擎。
            force_no_engine: 是否禁用 provider 引擎。

        Returns:
            pd.DataFrame: 故意不满足参考点等价性的单行行情。

        Side Effects:
            记录调用参数，不访问网络。
        """

        _ = start_date, end_date, fields, skip_paused, count, panel, fill_paused
        _ = prefer_engine, force_no_engine
        self.calls.append(
            {
                "security": security,
                "frequency": frequency,
                "fq": fq,
                "pre_factor_ref_date": pre_factor_ref_date,
            }
        )
        price = 50.0 if fq == "pre" else 100.0
        return pd.DataFrame(
            {
                "open": [price],
                "close": [price],
                "high_limit": [price * 1.1],
                "low_limit": [price * 0.9],
                "paused": [False],
            },
            index=[pd.Timestamp("2024-06-12 15:00:00")],
        )


@pytest.fixture(autouse=True)
def reset_price_basis_state() -> Any:
    """隔离全局 settings、provider、context 与订单队列，并在测试后完整恢复。"""

    original_provider = data_api._provider
    original_auth_attempted = data_api._auth_attempted
    original_context = data_api._current_context
    original_provider_cache = dict(data_api._provider_cache)
    original_provider_auth = dict(data_api._provider_auth_attempted)
    original_security_cache = dict(data_api._security_info_cache)
    original_pending_provider = data_api._pending_default_provider_name
    reset_settings()
    clear_order_queue()
    yield
    clear_order_queue()
    reset_settings()
    data_api._provider = original_provider
    data_api._auth_attempted = original_auth_attempted
    data_api._provider_cache.clear()
    data_api._provider_cache.update(original_provider_cache)
    data_api._provider_auth_attempted.clear()
    data_api._provider_auth_attempted.update(original_provider_auth)
    data_api._security_info_cache.clear()
    data_api._security_info_cache.update(original_security_cache)
    data_api._pending_default_provider_name = original_pending_provider
    data_api.set_current_context(original_context)


def _do_nothing(*args: Any, **kwargs: Any) -> None:
    """提供空策略回调；输入被忽略且不产生副作用。"""

    _ = args, kwargs


def _build_engine() -> BacktestEngine:
    """构造固定业务时刻、十万元现金的最小回测引擎。

    Returns:
        BacktestEngine: 已绑定 Context 且已同步到数据 API 的引擎。

    Side Effects:
        更新数据 API 当前上下文；不触发策略、行情或订单处理。
    """

    engine = BacktestEngine(initialize=_do_nothing, handle_data=_do_nothing)
    engine.context = Context(
        portfolio=Portfolio(
            total_value=100_000.0,
            available_cash=100_000.0,
            transferable_cash=100_000.0,
            starting_cash=100_000.0,
        ),
        current_dt=datetime(2024, 6, 12, 15, 0, 0),
    )
    engine.start_total_value = 100_000.0
    data_api.set_current_context(engine.context)
    return engine


def test_effective_price_basis_is_frozen_and_clamps_future_reference() -> None:
    """价格口径不可变，显式未来参考日必须被限制到当前回测业务日。"""

    basis = EffectivePriceBasis.create(
        use_real_price=True,
        provider="fixture",
        business_time=datetime(2024, 6, 12, 10, 0, 0),
        pre_factor_ref_date=date(2099, 1, 1),
    )

    assert basis.fq == "pre"
    assert basis.pre_factor_ref_date == date(2024, 6, 12)
    assert basis.business_time.isoformat() == "2024-06-12T10:00:00+08:00"
    assert basis.as_dict() == {
        "use_real_price": True,
        "fq": "pre",
        "pre_factor_ref_date": "2024-06-12",
        "provider": "fixture",
        "business_time": basis.business_time,
    }
    with pytest.raises(FrozenInstanceError):
        basis.fq = "none"  # type: ignore[misc]


def test_effective_price_basis_normalizes_supported_business_time_inputs() -> None:
    """日期、ISO 字符串和 aware datetime 都必须规范到上海业务时区。"""

    from_date = EffectivePriceBasis.create(
        use_real_price=False,
        provider="fixture",
        business_time=date(2024, 6, 12),
    )
    from_text = EffectivePriceBasis.create(
        use_real_price=True,
        provider="fixture",
        business_time="2024-06-12T02:00:00+00:00",
        pre_factor_ref_date="2024-06-12T01:00:00+00:00",
    )
    from_aware = EffectivePriceBasis.create(
        use_real_price=False,
        provider="fixture",
        business_time=datetime(2024, 6, 12, 2, 0, tzinfo=timezone.utc),
    )

    assert from_date.business_time.isoformat() == "2024-06-12T00:00:00+08:00"
    assert from_text.business_time.isoformat() == "2024-06-12T10:00:00+08:00"
    assert from_text.pre_factor_ref_date == date(2024, 6, 12)
    assert from_aware.business_time.isoformat() == "2024-06-12T10:00:00+08:00"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {
                "use_real_price": False,
                "fq": "none",
                "pre_factor_ref_date": None,
                "provider": "",
                "business_time": datetime(2024, 6, 12),
            },
            "provider",
        ),
        (
            {
                "use_real_price": False,
                "fq": "post",
                "pre_factor_ref_date": None,
                "provider": "fixture",
                "business_time": datetime(2024, 6, 12),
            },
            "复权口径",
        ),
        (
            {
                "use_real_price": True,
                "fq": "pre",
                "pre_factor_ref_date": None,
                "provider": "fixture",
                "business_time": datetime(2024, 6, 12),
            },
            "参考日",
        ),
        (
            {
                "use_real_price": True,
                "fq": "pre",
                "pre_factor_ref_date": date(2024, 6, 13),
                "provider": "fixture",
                "business_time": datetime(2024, 6, 12),
            },
            "不得晚于",
        ),
        (
            {
                "use_real_price": False,
                "fq": "none",
                "pre_factor_ref_date": date(2024, 6, 12),
                "provider": "fixture",
                "business_time": datetime(2024, 6, 12),
            },
            "不得携带",
        ),
        (
            {
                "use_real_price": False,
                "fq": "pre",
                "pre_factor_ref_date": date(2024, 6, 12),
                "provider": "fixture",
                "business_time": datetime(2024, 6, 12),
            },
            "不一致",
        ),
    ],
)
def test_effective_price_basis_rejects_invalid_direct_construction(
    kwargs: Dict[str, Any],
    message: str,
) -> None:
    """绕过 create 直接构造时，所有不一致字段组合仍必须被拒绝。"""

    with pytest.raises(ValueError, match=message):
        EffectivePriceBasis(**kwargs)


@pytest.mark.parametrize("bad_time", ["", "not-a-time", 123])
def test_effective_price_basis_rejects_invalid_business_time(bad_time: Any) -> None:
    """空值、非法文本和不支持类型都不能成为回测业务时刻。"""

    with pytest.raises(ValueError):
        EffectivePriceBasis.create(
            use_real_price=False,
            provider="fixture",
            business_time=bad_time,
        )


@pytest.mark.parametrize("bad_reference", ["", "not-a-date", 123])
def test_effective_price_basis_rejects_invalid_reference_date(bad_reference: Any) -> None:
    """动态前复权参考日格式非法时必须失败，不能读取宿主机日期兜底。"""

    with pytest.raises(ValueError):
        EffectivePriceBasis.create(
            use_real_price=True,
            provider="fixture",
            business_time=datetime(2024, 6, 12),
            pre_factor_ref_date=bad_reference,
        )


def test_legacy_provider_capability_is_strict_for_dynamic_pre() -> None:
    """旧 provider 仅可按可证明属性使用；动态前复权不能因有 get_price 就放行。"""

    class LegacyRawProvider:
        """只提供未复权 get_price 能力的旧 provider。"""

        name = "legacy-raw"

        def get_price(self) -> pd.DataFrame:
            """返回旧 provider 的空行情占位。

            Returns:
                pd.DataFrame: 空行情表。

            Side Effects:
                无；能力解析不会实际调用本方法。
            """

            return pd.DataFrame()

    class LegacyDynamicProvider(LegacyRawProvider):
        """通过显式属性承诺动态前复权的旧 provider。"""

        supports_dynamic_pre_price_basis = True

    raw_basis = resolve_effective_price_basis(
        provider=LegacyRawProvider(),
        business_time=datetime(2024, 6, 12),
        use_real_price=False,
    )
    dynamic_basis = resolve_effective_price_basis(
        provider=LegacyDynamicProvider(),
        business_time=datetime(2024, 6, 12),
        use_real_price=True,
    )

    assert raw_basis.fq == "none"
    assert dynamic_basis.fq == "pre"
    with pytest.raises(PriceBasisUnsupportedError) as exc_info:
        resolve_effective_price_basis(
            provider=object(),
            business_time=datetime(2024, 6, 12),
            use_real_price=False,
        )
    assert exc_info.value.code == "PRICE_BASIS_CAPABILITY_MISSING"


def test_use_real_price_true_uses_current_business_day_and_same_provider() -> None:
    """动态前复权必须锚定当前业务日，并只向同一 provider 获取 pre/raw 证明点。"""

    provider = DynamicPriceProvider()
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    set_option("pre_factor_ref_date", date(2099, 1, 1))
    context = _build_engine().context

    snapshot = data_api.get_current_data()["000001.XSHE"]

    assert snapshot.last_price == pytest.approx(100.0)
    basis = context.effective_price_basis
    assert isinstance(basis, EffectivePriceBasis)
    assert basis.fq == "pre"
    assert basis.pre_factor_ref_date == date(2024, 6, 12)
    assert {call["fq"] for call in provider.calls} == {"pre", "none"}
    pre_calls = [call for call in provider.calls if call["fq"] == "pre"]
    assert pre_calls
    assert all(call["pre_factor_ref_date"] == date(2024, 6, 12) for call in pre_calls)


def test_get_price_pre_defaults_to_current_business_day_in_real_price_mode() -> None:
    """回测 get_price 前复权请求未显式给参考日时必须锚定当前业务日。"""

    provider = DynamicPriceProvider()
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    _build_engine()

    frame = data_api.get_price(
        "000001.XSHE",
        end_date=datetime(2024, 6, 12, 15, 0, 0),
        frequency="daily",
        fields=["close"],
        count=1,
        fq="pre",
        panel=False,
    )

    assert not frame.empty
    assert provider.calls[-1]["fq"] == "pre"
    assert provider.calls[-1]["pre_factor_ref_date"] == date(2024, 6, 12)


def test_unsupported_dynamic_provider_fails_closed_before_fetch() -> None:
    """provider 未声明动态前复权能力时必须明确拒单，且不得试探或切换数据源。"""

    provider = CorporateActionPriceProvider()
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    set_option("order_match_mode", "bar_end")
    engine = _build_engine()
    local_order = order("000001.XSHE", 100)
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.rejected
    assert provider.calls == []
    assert engine.price_basis_failures[-1]["failure_code"] == "PRICE_BASIS_UNSUPPORTED"
    assert engine.context.price_basis_failure == engine.price_basis_failures[-1]


def test_dynamic_provider_reference_mismatch_fails_closed() -> None:
    """provider 虚报动态能力但参考点不等价时，当前行情必须失败且不返回混合价格。"""

    provider = BrokenDynamicPriceProvider()
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    engine = _build_engine()

    with pytest.raises(PriceBasisDataError) as exc_info:
        _ = data_api.get_current_data()["000001.XSHE"]

    assert exc_info.value.code == "PRICE_BASIS_REFERENCE_MISMATCH"
    assert {call["fq"] for call in provider.calls} == {"pre", "none"}
    assert engine.context.price_basis_failure["failure_code"] == "PRICE_BASIS_REFERENCE_MISMATCH"


@pytest.mark.parametrize("use_real_price", [False, True])
def test_no_corporate_action_keeps_existing_fill_baseline(
    monkeypatch: pytest.MonkeyPatch,
    use_real_price: bool,
) -> None:
    """无公司行动窗口在真实价开关两种模式下都保持 100 元正确成交基线。"""

    provider = DynamicPriceProvider(has_corporate_action=False)
    data_api.set_data_provider(provider)
    set_option("use_real_price", use_real_price)
    set_option("order_match_mode", "bar_end")
    engine = _build_engine()
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *_args: price)
    snapshot = data_api.get_current_data()["000001.XSHE"]
    local_order = order(
        "000001.XSHE",
        100,
        style=MarketOrderStyle(limit_price=snapshot.last_price * 1.1),
    )
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert engine.trades[-1].price == pytest.approx(100.0)
    expected_fq = "pre" if use_real_price else "none"
    assert engine.effective_price_basis.fq == expected_fq


def test_money_market_fund_keeps_protect_price_fill_and_zero_cost() -> None:
    """511880 动态前复权回放仍按三位价格成交，且佣金、税费与滑点保持零。"""

    provider = DynamicPriceProvider(
        current_price=100.071,
        has_corporate_action=False,
        category="money_market_fund",
    )
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    set_option("order_match_mode", "bar_end")
    engine = _build_engine()
    local_order = order(
        "511880.XSHG",
        100,
        style=MarketOrderStyle(limit_price=100.200),
    )
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert engine.trades[-1].price == pytest.approx(100.071)
    assert engine.trades[-1].commission == pytest.approx(0.0)
    assert engine.trades[-1].tax == pytest.approx(0.0)
    assert local_order.extra["requested_order_price"] == pytest.approx(100.200)
    assert local_order.extra["effective_price_basis"]["fq"] == "pre"


def test_corporate_action_price_basis_does_not_cancel_comparable_protect_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """除权窗口中保护价与撮合价必须同口径，本可成交订单不得因 50/100 混用而撤单。"""

    provider = CorporateActionPriceProvider()
    data_api.set_data_provider(provider)
    set_option("use_real_price", False)
    set_option("order_match_mode", "bar_end")

    engine = BacktestEngine(initialize=_do_nothing, handle_data=_do_nothing)
    engine.context = Context(
        portfolio=Portfolio(
            total_value=100_000.0,
            available_cash=100_000.0,
            transferable_cash=100_000.0,
            starting_cash=100_000.0,
        ),
        current_dt=datetime(2024, 6, 12, 15, 0, 0),
    )
    engine.start_total_value = 100_000.0
    data_api.set_current_context(engine.context)
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *_args: price)

    strategy_last_price = data_api.get_current_data()["000001.XSHE"].last_price
    local_order = order(
        "000001.XSHE",
        100,
        style=MarketOrderStyle(limit_price=strategy_last_price * 1.1),
    )
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert len(engine.trades) == 1
    assert engine.trades[0].price == pytest.approx(100.0)
    assert {call["fq"] for call in provider.calls} == {"none"}
    assert engine.effective_price_basis is engine.context.effective_price_basis
    assert engine.effective_price_basis.as_dict()["pre_factor_ref_date"] is None


def test_corporate_action_dynamic_pre_basis_keeps_current_point_fillable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """除权窗口启用真实价时，动态前复权当前点与撮合价等价且订单正常成交。"""

    provider = DynamicPriceProvider(has_corporate_action=True)
    data_api.set_data_provider(provider)
    set_option("use_real_price", True)
    set_option("order_match_mode", "bar_end")
    engine = _build_engine()
    monkeypatch.setattr(engine, "_apply_slippage_price", lambda price, *_args: price)
    strategy_last_price = data_api.get_current_data()["000001.XSHE"].last_price
    local_order = order(
        "000001.XSHE",
        100,
        style=MarketOrderStyle(limit_price=strategy_last_price * 1.1),
    )
    assert local_order is not None

    engine._process_orders(engine.context.current_dt)

    assert local_order.status == OrderStatus.filled
    assert engine.trades[-1].price == pytest.approx(100.0)
    assert engine.effective_price_basis.fq == "pre"
    assert engine.effective_price_basis.pre_factor_ref_date == date(2024, 6, 12)
    assert {call["fq"] for call in provider.calls} == {"pre", "none"}
