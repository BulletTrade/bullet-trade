"""大QMT实际历史窗口的原始数值校验边界。

作者：BruceLee
职责：验证无关预取坏值不阻断完整最新窗口，所需原价、竞价、前bar及事件依赖仍严格。
输入：既有内存Gateway和局部坏值，不读取真实服务、账号或参考库。
输出：远程wire经过实际adapter后的行情/异常断言；不修改磁盘行情或外部状态。
上下游：RemoteQmtProvider -> BigQmtDataAdapter -> 白名单测试Gateway。
环境约定：本地pytest/pandas，无服务、策略、交易或网络调用。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from test_big_qmt_history_standardization import _FakeGateway, _client, _price_request

pytestmark = pytest.mark.unit
_SECURITY = "000001.XSHE"


def _bad_value(gateway, frequency, stamp, field, value):
    """放入指定原始坏值；输入假网关、周期、时间和字段值，返回None，仅修改测试内存副本。"""
    frame = gateway.frames[(_SECURITY, frequency)]
    frame[field] = frame[field].astype(object)
    frame.loc[pd.Timestamp(stamp), field] = value


@pytest.mark.parametrize("field", ["close", "high", "volume", "suspendFlag"])
@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0, "bad", None, True])
@pytest.mark.parametrize("skip_paused", [False, True])
def test_unneeded_earlier_values_do_not_block_complete_latest_window(field, invalid, skip_paused):
    """完整最近窗口不受更早坏值影响；输入字段、坏值及停牌参数，无返回，逐格检查正常结果。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1m", "2026-09-03 09:45", field, invalid)
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        _SECURITY,
        frequency="5m",
        fq=None,
        end_date="2026-09-03 11:30",
        count=1,
        skip_paused=skip_paused,
    )
    assert result.index.tolist() == [pd.Timestamp("2026-09-03 11:30")]
    assert result.iloc[0].tolist() == [8.0, 8.1, 7.9, 8.0, 4000.0, 40000.0]


@pytest.mark.parametrize("field", ["close", "high", "volume"])
@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0])
def test_required_raw_values_still_fail(field, invalid):
    """真实参与聚合的坏值必须失败；输入字段与坏值，无返回，不因校验窗口收紧而放行。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1m", "2026-09-03 11:29", field, invalid)
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数"):
        _price_request(
            provider, _SECURITY, frequency="5m", fq=None, end_date="2026-09-03 11:30", count=1
        )


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0, 2.0, None, "0", True])
@pytest.mark.parametrize("skip_paused", [False, True])
def test_unknown_required_pause_flag_is_never_inferred(invalid, skip_paused):
    """所需未知停牌标识不能猜测；输入坏flag和过滤参数，无返回，禁止误作交易或停牌。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1m", "2026-09-03 11:30", "suspendFlag", invalid)
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数|停牌"):
        _price_request(
            provider,
            _SECURITY,
            frequency="1m",
            fq=None,
            end_date="2026-09-03 11:30",
            count=1,
            skip_paused=skip_paused,
        )


@pytest.mark.parametrize("stamp", ["2026-09-03 09:30", "2026-09-03 09:31"])
@pytest.mark.parametrize("field", ["high", "volume"])
@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0])
def test_auction_inputs_are_checked_before_price_or_volume_merge(stamp, field, invalid):
    """竞价及目标分钟须先验原值；输入位置字段坏值，无返回，防止max或求和掩盖异常。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1m", stamp, field, invalid)
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数"):
        _price_request(
            provider, _SECURITY, frequency="1m", fq=None, end_date="2026-09-03 09:31", count=1
        )


@pytest.mark.parametrize("dependency", ["pre_close", "paused_seed"])
@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0])
def test_required_previous_bar_or_suspension_seed_is_checked(dependency, invalid):
    """前bar及暂停种子仍是严格依赖；输入依赖类型和坏收盘，无返回，不能使用未校验值填充。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1m", "2026-09-03 11:29", "close", invalid)
    fields = ["close", "pre_close"] if dependency == "pre_close" else ["close"]
    if dependency == "paused_seed":
        gateway.frames[(_SECURITY, "1m")].loc["2026-09-03 11:30", "suspendFlag"] = 1.0
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数"):
        _price_request(
            provider,
            _SECURITY,
            frequency="1m",
            fq=None,
            end_date="2026-09-03 11:30",
            fields=fields,
            count=1,
        )


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0])
def test_event_previous_daily_close_keeps_full_validation(invalid):
    """事件前一日单日查询仍严格校验；输入坏收盘，无返回，不能把辅助C当无关历史跳过。"""
    gateway = _FakeGateway()
    _bad_value(gateway, "1d", "2026-09-02", "close", invalid)
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数"):
        _price_request(
            provider, _SECURITY, frequency="1m", fq="post", end_date="2026-09-04 11:30", count=1
        )


class _BadPauseFactGateway(_FakeGateway):
    """仅污染辅助停牌响应的假网关；协作对象为内存父类，状态只有本例坏值。"""

    def __init__(self, invalid):
        """记录辅助坏值；输入数值，返回None，仅初始化测试内存状态。"""
        super().__init__()
        self.invalid = invalid

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """重放响应后替换辅助close；输入只读请求，返回新wire，仅修改本次测试响应。"""
        response = await super().post(path, payload, timeout_seconds=timeout_seconds)
        if path == "/data/history" and (payload or {}).get("fill_data"):
            position = response["columns"].index("close")
            for row in response["records"]:
                row[position] = self.invalid
        return response


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -1.0])
def test_pause_fact_response_remains_strictly_validated(invalid):
    """暂停事实辅助响应不豁免坏值；输入坏close，无返回，确保严格校验不是只检查flag。"""
    gateway = _BadPauseFactGateway(invalid)
    day = pd.Timestamp("2026-09-02")
    gateway.frames[(_SECURITY, "1d")] = gateway.frames[(_SECURITY, "1d")].drop(day)
    gateway.suspension_facts[(_SECURITY, day)] = 1.0
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="数值|非有限|负数"):
        _price_request(
            provider, _SECURITY, frequency="1m", fq="post", end_date="2026-09-04 11:30", count=1
        )
