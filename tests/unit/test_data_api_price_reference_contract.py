"""公开价格接口重试时的复权基准契约回归。

作者：BruceLee
职责：验证真实价格分支失败时不丢弃参考日，也不改变普通请求的参数及错误语义。
输入：固定回测时间、内存价格表和可控制失败的假数据源，不读取账号或远程行情。
输出：pytest 断言及内存中的调用记录，不写策略、订单、配置或通知。
上下游：通过公开 data.api.get_price 调用真实参数适配包装，仅替换最末端数据源。
环境约定：本地 pytest/pandas；不启动回测引擎，不运行策略，不联网。
"""

from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.settings import set_option
from bullet_trade.data import api as data_api

pytestmark = pytest.mark.unit


class _ReferenceProvider:
    """记录价格请求并模拟失败；维护调用列表及失败次数，不持有外部连接。"""

    def __init__(self, failures=0, failure_type=RuntimeError):
        """初始化假数据源；输入失败次数与异常类，无返回值，仅建立实例状态。"""
        self.calls = []
        self.failures = failures
        self.failure_type = failure_type

    def get_price(self, **kwargs):
        """记录完整请求；输入价格关键字，返回固定价格表，按配置抛异常且只修改调用列表。"""
        self.calls.append(kwargs.copy())
        if len(self.calls) <= self.failures:
            raise self.failure_type("测试行情读取失败")
        value = 6.216 if kwargs.get("pre_factor_ref_date") == date(2026, 9, 7) else 9.999
        return pd.DataFrame({"close": [value]}, index=pd.to_datetime(["2026-09-04"]))


class _LegacyProvider(_ReferenceProvider):
    """模拟不声明复权基准的旧签名，复用调用记录，仅用于参数兼容性回归。"""

    def get_price(self, security, fields=None, count=None):
        """接收旧版价格参数；输入证券、字段和数量，返回测试表或配置异常，不支持锚定。"""
        return super().get_price(security=security, fields=fields, count=count)


class _ExplicitReferenceProvider(_ReferenceProvider):
    """模拟支持基准但没有引擎选项的显式签名，以调用记录验证参数过滤。"""

    def get_price(self, security, fields=None, count=None, pre_factor_ref_date=None):
        """接收价格及基准参数；返回测试表或配置异常，仅追加内存记录，不声明引擎选项。"""
        return super().get_price(
            security=security,
            fields=fields,
            count=count,
            pre_factor_ref_date=pre_factor_ref_date,
        )


class _LongTableProvider(_ReferenceProvider):
    """模拟返回time/code长表的数据源，复用调用记录，不承担复权计算。"""

    def get_price(self, **kwargs):
        """接收价格参数；返回带时间和证券列的固定长表，仅追加调用记录，无外部副作用。"""
        result = super().get_price(**kwargs).rename_axis("time").reset_index()
        result["code"] = kwargs["security"]
        return result


@pytest.fixture
def install_provider(monkeypatch):
    """安装无网络数据源工厂；输入pytest替换器，返回安装函数，测试结束自动恢复全局状态。"""

    def install(provider, *, context=True):
        """替换认证及默认数据源；输入假对象和上下文开关，无返回值，仅修改测试内全局引用。"""
        monkeypatch.setattr(data_api, "_ensure_auth", lambda: provider)
        monkeypatch.setattr(data_api, "_get_default_provider", lambda: provider)
        monkeypatch.setattr(
            data_api,
            "_current_context",
            SimpleNamespace(current_dt=datetime(2026, 9, 7, 14, 50)) if context else None,
        )
        set_option("use_real_price", True)
        set_option("avoid_future_data", False)

    return install


@pytest.mark.parametrize("force_no_engine", [False, True])
def test_real_price_retry_keeps_reference_and_all_price_parameters(
    install_provider, force_no_engine
):
    """首次失败后验证原基准与参数不变；输入安装器和引擎选项，无返回值，断言重试价格正确。"""
    provider = _ReferenceProvider(failures=1)
    install_provider(provider)
    set_option("force_no_engine", force_no_engine)

    result = data_api.get_price(
        "510500.XSHG",
        end_date=datetime(2026, 9, 4, 15),
        frequency="daily",
        fields=["close"],
        skip_paused=True,
        fq="pre",
        count=1,
        panel=True,
        fill_paused=False,
    )

    assert len(provider.calls) == 2
    first, retry = provider.calls
    assert first["pre_factor_ref_date"] == retry["pre_factor_ref_date"] == date(2026, 9, 7)
    assert first["prefer_engine"] is (not force_no_engine)
    assert "prefer_engine" not in retry
    assert {key: value for key, value in first.items() if key != "prefer_engine"} == retry
    assert retry["force_no_engine"] is force_no_engine
    assert result.iloc[0]["close"] == 6.216


def test_real_price_both_attempts_fail_without_unanchored_third_request(install_provider):
    """两次失败时保持空表约定；输入安装器，无返回值，断言不再尝试无基准成功请求。"""
    provider = _ReferenceProvider(failures=2)
    install_provider(provider)

    result = data_api.get_price("510500.XSHG", count=1, fields=["close"])

    assert result.empty
    assert list(result.columns) == ["close"]
    assert len(provider.calls) == 2
    assert all(call["pre_factor_ref_date"] == date(2026, 9, 7) for call in provider.calls)


def test_real_price_success_does_not_retry(install_provider):
    """正常真实价格只请求一次；输入安装器，无返回值，断言基准及固定返回值。"""
    provider = _ReferenceProvider()
    install_provider(provider)

    result = data_api.get_price("510500.XSHG", count=1, fields=["close"])

    assert len(provider.calls) == 1
    assert provider.calls[0]["pre_factor_ref_date"] == date(2026, 9, 7)
    assert result.iloc[0]["close"] == 6.216


def test_unsupported_real_price_remains_explicit_error(install_provider):
    """保留不支持异常直接传播；输入安装器，无返回值，断言不启动第二次请求。"""
    provider = _ReferenceProvider(failures=1, failure_type=NotImplementedError)
    install_provider(provider)

    with pytest.raises(NotImplementedError, match="测试行情读取失败"):
        data_api.get_price("510500.XSHG", count=1, fields=["close"])

    assert len(provider.calls) == 1


@pytest.mark.parametrize("fq", [None, "none", "post"])
def test_other_adjustment_modes_do_not_gain_reference_parameter(install_provider, fq):
    """保留原价和后复权调用；输入安装器和复权方式，无返回值，断言不附加前复权参数。"""
    provider = _ReferenceProvider()
    install_provider(provider)

    data_api.get_price("510500.XSHG", count=1, fields=["close"], fq=fq)

    assert len(provider.calls) == 1
    assert provider.calls[0]["fq"] == fq
    assert "pre_factor_ref_date" not in provider.calls[0]
    assert "prefer_engine" not in provider.calls[0]


@pytest.mark.parametrize("context", [False, True])
def test_standard_price_request_stays_unchanged(install_provider, context):
    """保留无上下文及非真实价格模式；输入安装器和上下文开关，无返回值，断言请求不变。"""
    provider = _ReferenceProvider()
    install_provider(provider, context=context)
    set_option("use_real_price", False)

    data_api.get_price("510500.XSHG", count=1, fields=["close"], fq="pre")

    assert len(provider.calls) == 1
    assert "pre_factor_ref_date" not in provider.calls[0]
    assert "prefer_engine" not in provider.calls[0]


def test_panel_false_keeps_existing_long_table_path(install_provider):
    """仅验证既有长表兼容而非锚定；输入安装器，无返回值，断言格式及标准请求保持不变。"""
    provider = _LongTableProvider()
    install_provider(provider)

    result = data_api.get_price("510500.XSHG", count=1, fields=["close"], panel=False)

    assert len(provider.calls) == 1
    assert provider.calls[0]["panel"] is False
    assert "pre_factor_ref_date" not in provider.calls[0]
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {"time": pd.to_datetime(["2026-09-04"]), "close": [9.999], "code": ["510500.XSHG"]}
        ),
    )


def test_legacy_signature_retry_stays_compatible_without_claiming_anchor(install_provider):
    """旧签名重试仍可用但不声称锚定；输入安装器，无返回值，断言新增基准参数被兼容过滤。"""
    provider = _LegacyProvider(failures=1)
    install_provider(provider)

    result = data_api.get_price("510500.XSHG", count=1, fields=["close"])

    assert len(provider.calls) == 2
    assert provider.calls[0] == provider.calls[1]
    assert "pre_factor_ref_date" not in provider.calls[0]
    assert not result.empty


def test_explicit_signature_preserves_reference_without_engine_options(install_provider):
    """显式签名两次均获得基准；输入安装器，无返回值，断言未声明的引擎选项不会造成失败。"""
    provider = _ExplicitReferenceProvider(failures=1)
    install_provider(provider)

    result = data_api.get_price("510500.XSHG", count=1, fields=["close"])

    assert len(provider.calls) == 2
    assert provider.calls[0] == provider.calls[1]
    assert all(call["pre_factor_ref_date"] == date(2026, 9, 7) for call in provider.calls)
    assert result.iloc[0]["close"] == 6.216
