"""合约规格分层来源测试：全部使用合成来源与内置配置，不依赖外部数据源。

覆盖四件事：规格按合约代码解析而非按品种解析、伪合约被拒、
来源链的批量预热与缓存、保证金率的四级优先级与按日分段。
"""

import datetime as dt
import json

import pytest

from bullet_trade.core.contract_specs import (
    ChainedSpecSource,
    FuturesSpecConfig,
    JsonSpecSource,
    MarginRateRule,
    RemoteSpecSource,
    build_spec_source,
    default_config_path,
    is_pseudo_contract,
    load_futures_spec_config,
)
from bullet_trade.core.futures_account import ContractSpec, ContractSpecError, ContractSpecTable

AU_EARLY = "AU1910.XSGE"
AU_LATE = "AU1912.XSGE"
TREASURY = "T2109.CCFX"
UNKNOWN = "ZZ2109.XDCE"


class RecordingSpecSource:
    """合成远端来源：记录调用批次，只对登记过的合约给出规格。"""

    def __init__(self, specs=None):
        self.specs = dict(specs or {})
        self.calls = []

    def query_contract(self, security, day=None):
        self.calls.append(([str(security).upper()], day))
        return self.specs.get(str(security).upper())

    def prefetch(self, securities, day=None):
        codes = sorted({str(code).upper() for code in securities})
        self.calls.append((codes, day))
        return None


def test_pseudo_contract_detection():
    assert is_pseudo_contract("AU8888.XSGE") is True
    assert is_pseudo_contract("IF9999.CCFX") is True
    assert is_pseudo_contract("IF00.CCFX") is True
    assert is_pseudo_contract("AU1910.XSGE") is False
    assert is_pseudo_contract("002714.XSHE") is False
    assert is_pseudo_contract("not-a-code") is False


def test_contract_level_resolution_beats_product_level():
    # 同品种相邻月份合约的报价单位可以不同，品种级规格表无法表达
    source = RecordingSpecSource({AU_EARLY: ContractSpec("AU", 1000.0, 0.05)})
    table = ContractSpecTable(spec_source=source, load_config=False)
    assert table.tick_size(AU_EARLY) == 0.05
    assert table.tick_size(AU_LATE) == 0.02  # 远端未覆盖，落回内置品种兜底
    assert table.multiplier(AU_EARLY) == 1000.0


def test_explicit_contract_registration_wins_over_source():
    source = RecordingSpecSource({AU_EARLY: ContractSpec("AU", 1000.0, 0.05)})
    table = ContractSpecTable(spec_source=source, load_config=False)
    table.register_contract(AU_EARLY, 1000.0, tick_size=0.01)
    assert table.tick_size(AU_EARLY) == 0.01
    assert source.calls == []  # 显式注册后不再询问来源


def test_pseudo_contract_rejected_on_trading_path():
    table = ContractSpecTable(load_config=False)
    with pytest.raises(ContractSpecError):
        table.spec("AU8888.XSGE")
    with pytest.raises(ContractSpecError):
        table.multiplier("IF9999.CCFX")
    with pytest.raises(ContractSpecError):
        table.register_contract("AU8888.XSGE", 1000.0)


def test_unregistered_product_does_not_degrade():
    table = ContractSpecTable(load_config=False)
    with pytest.raises(ContractSpecError):
        table.multiplier(UNKNOWN)


def test_shipped_config_supplies_treasury_fallback():
    table = ContractSpecTable()
    spec = table.spec(TREASURY)
    assert spec.multiplier == 10000.0
    assert spec.tick_size == 0.005


def test_margin_rate_precedence():
    table = ContractSpecTable(
        specs=[ContractSpec("LH", 16.0, 5.0, 0.10)],
        margin_rate=0.15,
        margin_rate_by_product={"LH": 0.03},
        load_config=False,
    )
    # 品种覆盖 > 全局覆盖 > 规格挂牌费率
    assert table.margin_rate("LH2109.XDCE") == 0.03
    table.set_margin_rate_by_product(None)
    assert table.margin_rate("LH2109.XDCE") == 0.15
    table.set_margin_rate(None)
    assert table.margin_rate("LH2109.XDCE") == 0.10
    with pytest.raises(ContractSpecError):
        table.margin_rate(UNKNOWN)


def test_margin_rate_by_product_filters_invalid_entries():
    table = ContractSpecTable(load_config=False)
    table.set_margin_rate_by_product({"t": 0.03, "IF": 0, "CU": None, "RB": -1})
    assert table._margin_rate_by_product == {"T": 0.03}


def test_margin_rate_rule_effective_segments():
    rule = MarginRateRule(
        product="T",
        rate=0.05,
        effective=(
            (dt.date(2015, 3, 20), 0.02),
            (dt.date(2020, 1, 1), 0.03),
        ),
    )
    assert rule.rate_on(None) == 0.05
    assert rule.rate_on(dt.date(2015, 3, 19)) == 0.05  # 首段生效前回落缺省
    assert rule.rate_on(dt.date(2016, 1, 4)) == 0.02
    assert rule.rate_on(dt.date(2021, 6, 1)) == 0.03


def test_config_margin_layer_sits_below_option_overrides(tmp_path):
    path = tmp_path / "specs.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "margin_rate": {
                    "default": 0.08,
                    "by_product": {
                        "T": {
                            "rate": 0.05,
                            "effective": [{"start": "2020-01-01", "rate": 0.03}],
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    config = load_futures_spec_config(str(path))
    table = ContractSpecTable(config=config, load_config=False)
    assert table.margin_rate(TREASURY) == 0.05
    assert table.margin_rate(TREASURY, dt.date(2021, 6, 1)) == 0.03
    assert table.margin_rate("IF2106.CCFX") == 0.08  # 配置层缺省
    table.set_margin_rate(0.15)
    assert table.margin_rate("IF2106.CCFX") == 0.15  # 全局覆盖压过配置
    assert table.margin_rate(TREASURY) == 0.15
    table.set_margin_rate_by_product({"T": 0.02})
    assert table.margin_rate(TREASURY) == 0.02  # 品种覆盖压过全局


def test_json_contract_overrides(tmp_path):
    path = tmp_path / "specs.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "contract_overrides": {AU_EARLY: {"multiplier": 1000.0, "tick_size": 0.05}},
                "offline_fallback_specs": {"ZZ": {"multiplier": 7.0, "tick_size": None}},
            }
        ),
        encoding="utf-8",
    )
    config = load_futures_spec_config(str(path))
    table = ContractSpecTable(config=config, load_config=False)
    assert table.tick_size(AU_EARLY) == 0.05
    # 兜底规格的 tick_size 显式为 null，离线时不猜测报价单位
    assert table.multiplier(UNKNOWN) == 7.0
    assert table.tick_size(UNKNOWN) is None


def test_invalid_config_entries_are_dropped(tmp_path):
    path = tmp_path / "specs.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "contract_overrides": {AU_EARLY: {"multiplier": 0}},
                "margin_rate": {"by_product": {"T": {"effective": [{"start": "bad", "rate": 0.03}]}}},
            }
        ),
        encoding="utf-8",
    )
    config = load_futures_spec_config(str(path))
    assert config.spec_source.query_contract(AU_EARLY) is None
    assert config.margin_rate("T") is None


def test_missing_config_file_yields_empty_config(tmp_path):
    config = load_futures_spec_config(str(tmp_path / "absent.json"))
    assert config.margin_default is None
    assert config.margin_rules == {}
    assert config.fallback_specs == {}


def test_shipped_config_file_exists_and_parses():
    config = load_futures_spec_config()
    assert config.path == default_config_path()
    assert config.version == 1


def test_remote_source_batches_and_caches():
    fetched = []

    def fetcher(codes, day):
        fetched.append((tuple(codes), day))
        return {
            code: {"contract_multiplier": 1000.0, "tick_size": 0.05}
            for code in codes
            if code.startswith("AU19")
        }

    source = RemoteSpecSource(fetcher=fetcher, batch_size=2)
    source.prefetch([AU_EARLY, AU_LATE, "AU8888.XSGE", "AU1906.XSGE"])
    # 伪合约被剔除，剩余 3 个按批大小 2 分两批
    assert [codes for codes, _ in fetched] == [("AU1906.XSGE", "AU1910.XSGE"), ("AU1912.XSGE",)]
    assert source.query_contract(AU_EARLY) is not None
    assert len(fetched) == 2  # 已缓存，不再发起请求
    assert source.query_contract("AU2112.XSGE") is None
    assert len(fetched) == 3  # 未命中的合约单独补一次


def test_remote_source_swallows_fetcher_errors():
    def fetcher(codes, day):
        raise RuntimeError("上游不可达")

    source = RemoteSpecSource(fetcher=fetcher)
    assert source.query_contract(AU_EARLY) is None
    assert source.query_contract(AU_EARLY) is None  # 失败结果同样缓存，不反复重试


def test_chained_source_first_hit_wins():
    override = JsonSpecSource(contract_overrides={AU_EARLY: {"multiplier": 1000.0, "tick_size": 0.01}})
    remote = RecordingSpecSource({AU_EARLY: ContractSpec("AU", 1000.0, 0.05)})
    chain = ChainedSpecSource([override, remote])
    assert chain.query_contract(AU_EARLY).tick_size == 0.01
    assert remote.calls == []
    assert chain.query_contract(AU_LATE) is None
    assert len(remote.calls) == 1
    assert chain.query_contract("AU8888.XSGE") is None


def test_build_spec_source_can_disable_remote():
    chain, config = build_spec_source(enable_remote=False)
    assert [source.name for source in chain.sources] == ["json"]
    assert isinstance(config, FuturesSpecConfig)
    chain, _ = build_spec_source(enable_remote=True)
    assert [source.name for source in chain.sources] == ["json", "remote"]
