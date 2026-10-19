"""本地标准数据 API 的离线金样。

作者: BruceLee
职责: 验证真实 Parquet 发布、固定版本、标准形状、缺口和未来时点边界。
输入: 临时目录和明确的合成日/分钟量价；输出: 值、版本和无回退断言。
上下游: DatasetPublisher -> LocalBulletDataProvider -> BulletTrade 公共 API。
约定: 全程禁止网络，测试数据不代表供应商或生产数据验收。
"""

import socket
from datetime import datetime

import pandas as pd
import pytest
from bullet_data.provider import LocalCoverageError
from bullet_data.publisher import DatasetPublisher, PublishRequest

from bullet_trade.data.providers.local import LocalBulletDataProvider

CODES = ["000001.XSHE", "510300.XSHG"]
DATES = ["2025-01-02", "2025-01-03", "2025-01-06"]


def publish(publisher, object_name, frame, run_id=None, **kwargs):
    """输入对象/事实表/发布参数，返回真实 manifest；仅在临时目录发布。"""
    return publisher.publish(
        PublishRequest(
            object_name=object_name,
            source_name="fixture",
            sync_run_id=run_id or object_name,
            **kwargs
        ),
        frame,
    )


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    """输入临时目录与补丁器，发布金样并禁网，返回发布器及固定 provider。"""

    def deny(*args, **kwargs):
        """输入任何联网参数即失败；无返回，阻止意外 SDK 或客户端访问。"""
        raise AssertionError("离线金样禁止联网")

    monkeypatch.setattr(socket.socket, "connect", deny)
    publisher = DatasetPublisher(tmp_path)
    publish(
        publisher,
        "trade_calendar",
        pd.DataFrame([{"market": "XSHG", "trade_date": date, "is_open": True} for date in DATES]),
    )
    publish(
        publisher,
        "instrument_master",
        pd.DataFrame(
            [
                {
                    "canonical_code": code,
                    "display_name": "测试" + code,
                    "name": code,
                    "security_type": "stock" if index == 0 else "fund",
                    "exchange": code.split(".")[1],
                    "list_date": "2020-01-01",
                    "delist_date": None,
                }
                for index, code in enumerate(CODES)
            ]
        ),
    )
    publish(
        publisher,
        "price_bars_1d",
        pd.DataFrame(
            [
                {
                    "canonical_code": code,
                    "trade_date": date,
                    "open": 10.0 + index,
                    "high": 12.0 + index,
                    "low": 9.0 + index,
                    "close": 10.0 + index,
                    "volume": 100.0,
                    "money": 1000.0,
                    "paused": index == 1,
                    "high_limit": 15.0,
                    "low_limit": 5.0,
                }
                for code in CODES
                for index, date in enumerate(DATES)
            ]
        ),
    )
    publish(
        publisher,
        "adjustment_factor",
        pd.DataFrame(
            [
                {
                    "canonical_code": code,
                    "trade_date": date,
                    "factor": factor,
                    "source_name": "fixture",
                }
                for code in CODES
                for date, factor in zip(DATES, [1.0, 2.0, 2.0])
            ]
        ),
    )
    publish(
        publisher,
        "price_bars_1m",
        pd.DataFrame(
            [
                {
                    "canonical_code": CODES[0],
                    "trade_time": "2025-01-02 " + time,
                    "trade_date": DATES[0],
                    "open": price,
                    "close": price,
                    "high": price,
                    "low": price,
                    "volume": 100.0,
                    "money": price * 100,
                }
                for time, price in [("09:31:00", 10.0), ("09:32:00", 10.5)]
            ]
        ),
    )
    provider = LocalBulletDataProvider({"workspace_root": str(tmp_path)})
    return publisher, provider


def test_raw_pre_adjustment_and_fixed_reference(dataset):
    """输入固定数据，验证原始/前复权及因子值，参考日不读取未来因子。"""
    _, provider = dataset
    raw = provider.get_price(
        CODES[0], start_date=DATES[0], end_date=DATES[-1], fields=["close"], fq=None
    )
    adjusted = provider.get_price(
        CODES[0], start_date=DATES[0], end_date=DATES[-1], fields=["close", "factor"]
    )
    earlier = provider.get_price(
        CODES[0], start_date=DATES[0], end_date=DATES[0], fields=["close", "factor"]
    )
    assert raw.close.tolist() == [10.0, 11.0, 12.0]
    assert adjusted.close.tolist() == [5.0, 11.0, 12.0]
    assert adjusted.factor.tolist() == [0.5, 1.0, 1.0]
    assert earlier.close.tolist() == [10.0]
    assert earlier.factor.tolist() == [1.0]
    assert isinstance(raw.index, pd.DatetimeIndex)
    assert raw.attrs["data_source"]["manifest_id"] == provider.manifest_id


def test_multiple_security_shapes_and_count(dataset):
    """输入证券列表，验证标准宽表/长表、字段顺序和倒推条数。"""
    _, provider = dataset
    wide = provider.get_price(
        CODES, end_date=DATES[-1], count=2, fields=["close", "volume"], panel=True, fq=None
    )
    long = provider.get_price(
        CODES, end_date=DATES[-1], count=2, fields=["close"], panel=False, fq=None
    )
    assert wide.columns.names == ["field", "code"]
    assert wide["close"][CODES[0]].tolist() == [11, 12]
    assert long.columns.tolist() == ["time", "code", "close"]
    assert long.code.tolist() == CODES + CODES
    assert long.close.tolist() == [11, 11, 12, 12]


def test_paused_skip_and_nan_fill(dataset):
    """输入明确停牌行，验证跳过与不填充，count 仍取得指定非停牌条数。"""
    _, provider = dataset
    skipped = provider.get_price(
        CODES[0], end_date=DATES[-1], count=2, skip_paused=True, fields=["close", "paused"], fq=None
    )
    unfilled = provider.get_price(
        CODES[0],
        start_date=DATES[0],
        end_date=DATES[-1],
        fill_paused=False,
        fields=["close", "paused"],
        fq=None,
    )
    assert skipped.index.strftime("%Y-%m-%d").tolist() == [DATES[0], DATES[-1]]
    assert skipped.close.tolist() == [10, 12]
    assert pd.isna(unfilled.iloc[1].close)
    assert bool(unfilled.iloc[1].paused)


def test_minute_end_is_exact_and_missing_status_is_not_normal(dataset):
    """输入分钟时间，验证 UTC 换算、截止不越界，缺停牌状态明确报缺。"""
    _, provider = dataset
    frame = provider.get_price(
        CODES[0],
        start_date="2025-01-02 09:31",
        end_date="2025-01-02T01:31:00Z",
        frequency="minute",
        fields=["close"],
        fq=None,
    )
    assert frame.close.tolist() == [10.0]
    with pytest.raises(LocalCoverageError) as error:
        provider.get_price(
            CODES[0],
            start_date="2025-01-02 09:31",
            end_date="2025-01-02 09:32",
            frequency="1m",
            fields=["paused"],
            fq=None,
        )
    assert error.value.diagnostic["gaps"][0]["reason"] == "UNKNOWN_PAUSED_STATE"


def test_calendar_and_instrument_history(dataset):
    """输入日期与类型，验证开市日及证券元数据，不推测历史未知字段。"""
    _, provider = dataset
    assert provider.get_trade_days(end_date=DATES[-1], count=2) == [
        datetime(2025, 1, 3),
        datetime(2025, 1, 6),
    ]
    assert provider.get_all_securities("fund", date=DATES[0]).index.tolist() == [CODES[1]]
    assert (
        provider.get_security_info(CODES[0], date=DATES[0])["start_date"].isoformat()
        == "2020-01-01"
    )
    with pytest.raises(LocalCoverageError):
        provider.get_security_info("UNKNOWN.XSHG", date=DATES[0])
    with pytest.raises(LocalCoverageError):
        provider.get_trade_days(start_date=DATES[0], end_date="2025-01-07")


def test_old_manifest_does_not_follow_new_publication(dataset):
    """发布替换事实后，验证旧实例和指定旧 manifest 查询仍得到原数据。"""
    publisher, provider = dataset
    replacement = provider.local.query(
        object_name="price_bars_1d", codes=[CODES[0]], start=DATES[0], end=DATES[0]
    )
    replacement["close"] = 999.0
    replacement["high"] = 1000.0
    publish(
        publisher,
        "price_bars_1d",
        replacement,
        run_id="replace",
        merge_mode="replace_range",
        scope={"codes": [CODES[0]], "start": DATES[0], "end": DATES[0]},
        confirm_replace_range=True,
    )
    new = LocalBulletDataProvider(
        {"workspace_root": str(publisher.manifest_store.layout.workspace_root)}
    )
    assert new.manifest_id != provider.manifest_id
    assert provider.get_price(
        CODES[0], start_date=DATES[0], end_date=DATES[0], fields=["close"], fq=None
    ).close.tolist() == [10]
    assert new.get_price(
        CODES[0], start_date=DATES[0], end_date=DATES[0], fields=["close"], fq=None
    ).close.tolist() == [999]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"count": True},
        {"count": 0},
        {"start_date": DATES[0], "count": 1},
        {"frequency": "tick"},
        {"fq": "post"},
        {"skip_paused": True, "security": CODES},
        {"pre_factor_ref_date": "2025-01-07"},
    ],
)
def test_invalid_or_unsupported_parameters_fail_explicitly(dataset, kwargs):
    """输入不支持或冲突参数，验证明确失败而不是空表或假结果。"""
    _, provider = dataset
    arguments = {"security": CODES[0], "end_date": DATES[-1], "fields": ["close"]}
    arguments.update(kwargs)
    with pytest.raises((ValueError, NotImplementedError)):
        provider.get_price(**arguments)


def test_missing_field_count_and_future_calendar_fail(dataset):
    """输入缺字段、超量条数和超出日历请求，验证结构化缺口且不访问远程。"""
    _, provider = dataset
    for options in [{"fields": ["missing"]}, {"count": 4}, {"end_date": "2025-01-07"}]:
        arguments = {"security": CODES[0], "end_date": DATES[-1], "fq": None}
        arguments.update(options)
        with pytest.raises(LocalCoverageError) as error:
            provider.get_price(**arguments)
        assert error.value.diagnostic["fallback_attempted"] is False


def test_public_api_propagates_local_errors_and_factory(dataset, monkeypatch):
    """装配公共 API，验证本地缺口传播，不吞成空表，不导入外部 SDK。"""
    from bullet_trade.data import api

    publisher, provider = dataset
    previous = api._provider
    context = api._current_context
    monkeypatch.setattr(api, "_current_context", None)
    try:
        api.set_data_provider(provider)
        result = api.get_price(
            CODES[0], start_date=DATES[0], end_date=DATES[0], fields=["close"], fq=None
        )
        assert result.close.tolist() == [10]
        with pytest.raises(LocalCoverageError):
            api.get_price(CODES[0], end_date="2025-01-07", fields=["close"], fq=None)
        with pytest.raises(LocalCoverageError):
            api.get_security_info("UNKNOWN.XSHG")
        created = api._create_provider(
            "local",
            {
                "workspace_root": str(publisher.manifest_store.layout.workspace_root),
                "manifest_id": provider.manifest_id,
            },
        )
        assert created.manifest_id == provider.manifest_id
    finally:
        api._provider = previous
        api._current_context = context
        api._security_info_cache.clear()
