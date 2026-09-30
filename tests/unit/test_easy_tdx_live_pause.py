"""easy_tdx 实时停复牌回归测试。

作者: BruceLee
日期: 2026-09-30
文件职责: 用真实 Mac 标签及独立 QMT 停牌事实验证实时适配。
主要输入: 2026-09-30 513100 临停、688496/002731 全日停牌及正常交易样本。
主要输出: 停牌、最后价和行情时间合同断言。
上下游关系: 注入 SDK 报价至 EasyTdxProvider，不连接账户或外部服务器。
关键约定: 零价本身不证明停牌；价格补齐不能更新时间或放宽健康判断。
"""

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider

pytestmark = pytest.mark.unit


class CapturedQuotes:
    """承载单证券固定 SDK 报价，由 provider 读取；无连接和交易状态。"""

    def __init__(self, row):
        """输入单行报价字典，保存供后续查询；无返回或外部副作用。"""
        self.row = dict(row)

    def get_stock_quotes(self, symbols, **kwargs):
        """输入证券和字段参数，返回固定报价 DataFrame；不访问网络。"""
        return pd.DataFrame([self.row])


@pytest.mark.parametrize(
    "security,flags,previous_close,expected",
    [
        ("513100.SH", 528386, 2.3239998817, 2.324),
        ("688496.SH", 1052672, 0.5799999833, 0.58),
        ("002731.SZ", 1052672, 0.7699999809, 0.77),
    ],
)
def test_verified_mac_pause_uses_previous_close(security, flags, previous_close, expected):
    """输入独立 QMT 已确认的真实停牌样本，断言零价补昨收及源时间保留；无网络。"""
    provider = EasyTdxProvider(
        {
            "client": CapturedQuotes(
                {
                    "close": 0.0,
                    "pre_close": previous_close,
                    "open": 0.0,
                    "high": 0.0,
                    "low": 0.0,
                    "vol": 0,
                    "amount": 0.0,
                    "stock_tag_flags": flags,
                    "server_update_date": 20260930,
                    "server_update_time": 93001,
                }
            )
        }
    )
    snapshot = provider.get_live_current(security)
    assert snapshot["paused"] is True
    assert snapshot["last_price"] == pytest.approx(expected, abs=1e-6)
    assert snapshot["source_time"] == "2026-09-30T09:30:01"
    assert snapshot["last_price_source"] == "pre_close"
    assert snapshot["raw_last_price"] == 0.0
    assert snapshot["stock_tag_flags"] == flags
    assert "feed_health" not in snapshot


@pytest.mark.parametrize("flags", [524290, 1081344, 1048576, 11, 6, 0])
def test_trading_tag_samples_are_not_paused(flags):
    """输入复牌 QDII、交易中 ST 和普通证券真实标签，验证其他标签位不误判停牌。"""
    provider = EasyTdxProvider(
        {"client": CapturedQuotes({"close": 2.331, "pre_close": 2.324, "stock_tag_flags": flags})}
    )
    snapshot = provider.get_live_current("513100.SH")
    assert snapshot["paused"] is False
    assert snapshot["last_price"] == 2.331
    assert snapshot["last_price_source"] == "quote"


@pytest.mark.parametrize("flags", [None, 524290, 0])
def test_zero_quote_without_pause_evidence_is_not_filled(flags):
    """输入无停牌标记的零价故障，断言不拿昨收伪装有效实时价；无外部依赖。"""
    row = {"close": 0.0, "pre_close": 2.324}
    if flags is not None:
        row["stock_tag_flags"] = flags
    snapshot = EasyTdxProvider({"client": CapturedQuotes(row)}).get_live_current("513100.SH")
    assert snapshot["paused"] is False
    assert snapshot["last_price"] == 0.0


@pytest.mark.parametrize("previous_close", [0.0, -1.0, float("nan"), float("inf")])
def test_pause_with_invalid_previous_close_stays_invalid(previous_close):
    """输入停牌标记和无效昨收，断言不产生正有限价；无网络及状态写入。"""
    row = {"close": 0.0, "pre_close": previous_close, "stock_tag_flags": 528386}
    snapshot = EasyTdxProvider({"client": CapturedQuotes(row)}).get_live_current("513100.SH")
    assert snapshot["paused"] is True
    assert snapshot["last_price"] == 0.0


def test_intraday_pause_preserves_last_trade_and_resume_reloads_quote():
    """输入有最后成交的临停与复牌快照，验证临停保留成交价且复牌读取实时价。"""
    client = CapturedQuotes({"close": 2.33, "pre_close": 2.324, "stock_tag_flags": 528386})
    provider = EasyTdxProvider({"client": client})
    paused = provider.get_live_current("513100.SH")
    assert paused["paused"] is True and paused["last_price"] == 2.33
    client.row.update(close=2.331, stock_tag_flags=524290)
    resumed = provider.get_live_current("513100.SH")
    assert resumed["paused"] is False and resumed["last_price"] == 2.331


@pytest.mark.parametrize(
    "status,paused", [(0x8000, False), (0x8020, True), (0x20, True), (0, False)]
)
def test_standard_protocol_uses_actual_pause_bit(status, paused):
    """输入标准报价状态位，验证 0x8000 本身不构成停牌；返回独立状态断言。"""
    row = {"price": 10.0, "pre_close": 9.0, "trading_status": status}
    snapshot = EasyTdxProvider({"client": CapturedQuotes(row)}).get_live_current("000001.SZ")
    assert snapshot["paused"] is paused
