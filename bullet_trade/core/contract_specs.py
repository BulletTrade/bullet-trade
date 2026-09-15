"""
期货合约规格来源

作者: BruceLee
文件职责: 提供合约规格的分层来源（显式覆盖、远端解析、离线兜底）与保证金率的按日配置解析。
主要输入: 合约代码、回测交易日、外部规格配置文件、远端合约规格接口。
主要输出: ContractSpec 对象、按品种与日期解析出的保证金率。
上下游关系: 上游是回测引擎的规格查询与保证金计算，下游是数据接口层的合约规格批量查询。
关键约定: 规格按合约代码解析而非按品种解析，因为同一品种不同月份合约的最小变动价位可以不同；
         主力/连续/指数等伪合约不参与撮合，规格查询必须拒绝，避免用指数口径的乘数下单；
         远端不可达且离线兜底缺失时显式报错，不得退化为乘数 1 或最小变动价位 0.01。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date as Date
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .futures_account import ContractSpec

log = logging.getLogger(__name__)

_PSEUDO_DIGITS = frozenset({"8888", "9999", "88", "99", "0000", "00"})
_FUTURES_EXCHANGE_SUFFIXES = ("XSGE", "CCFX", "XDCE", "XZCE", "XINE", "GFEX", "XSHF", "CZCE", "DCE", "CFFEX")
_REMOTE_BATCH_SIZE = 50
_DEFAULT_CONFIG_FILENAME = "futures_contract_specs.json"


def is_pseudo_contract(security: str) -> bool:
    """判断代码是否为主力/连续/指数等不可撮合的伪合约。

    Args:
        security: 带交易所后缀的合约代码，例如 AU8888.XSGE。

    Returns:
        bool: 属于伪合约返回 True。
    """

    code, _, exchange = str(security).partition(".")
    if not exchange or exchange.upper() not in _FUTURES_EXCHANGE_SUFFIXES:
        return False
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits in _PSEUDO_DIGITS


def _to_date(value: Any) -> Optional[Date]:
    """把字符串或 datetime 归一化为 date。

    Args:
        value: 日期输入，None 原样返回。

    Returns:
        Optional[Date]: 解析出的日期，无法解析时返回 None。
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, Date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _to_positive_float(value: Any) -> Optional[float]:
    """把配置里的数值字段转成正 float。

    Args:
        value: 原始配置值。

    Returns:
        Optional[float]: 大于 0 的浮点数，缺失或非法时为 None。
    """

    if value is None or isinstance(value, bool) or isinstance(value, str):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not number or number <= 0:
        return None
    return number


@dataclass(frozen=True)
class MarginRateRule:
    """单个品种的保证金率规则，支持按生效日期分段。

    Attributes:
        product: 品种代码。
        rate: 缺省保证金率，分段未命中时使用。
        effective: 按起始日升序排列的 (起始日, 保证金率) 分段。
    """

    product: str
    rate: Optional[float] = None
    effective: Tuple[Tuple[Date, float], ...] = ()

    def rate_on(self, day: Optional[Date] = None) -> Optional[float]:
        """取得某交易日的保证金率。

        Args:
            day: 交易日，None 表示使用缺省费率。

        Returns:
            Optional[float]: 命中的保证金率，全部未命中时返回缺省费率。
        """

        if day is None or not self.effective:
            return self.rate
        matched: Optional[float] = None
        for start, rate in self.effective:
            if day >= start:
                matched = rate
            else:
                break
        return self.rate if matched is None else matched


class ContractSpecSource:
    """合约规格来源接口。"""

    name: str = "base"

    def query_contract(self, security: str, day: Optional[Date] = None) -> Optional[ContractSpec]:
        """查询单个合约的规格。

        Args:
            security: 合约代码。
            day: 回测交易日，用于按日解析或防止未来数据。

        Returns:
            Optional[ContractSpec]: 本来源无法给出规格时返回 None。
        """

        return None

    def prefetch(self, securities: Iterable[str], day: Optional[Date] = None) -> None:
        """预热一批合约的规格缓存。

        Args:
            securities: 合约代码集合。
            day: 回测交易日。

        Returns:
            None: 结果写入来源内部缓存。
        """

        return None


class JsonSpecSource(ContractSpecSource):
    """从配置文件读取的合约规格来源，含合约级覆盖与离线兜底规格。"""

    name = "json"

    def __init__(
        self,
        contract_overrides: Optional[Mapping[str, Mapping[str, Any]]] = None,
        fallback_specs: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> None:
        """构建 JSON 规格来源。

        Args:
            contract_overrides: 合约代码到规格字段的映射，优先级高于兜底。
            fallback_specs: 品种代码到规格字段的映射，仅在合约级全部落空时使用。

        Returns:
            None: 仅初始化内部映射。
        """

        self._overrides: Dict[str, ContractSpec] = {}
        for code, payload in (contract_overrides or {}).items():
            spec = _spec_from_payload(code, payload)
            if spec is not None:
                self._overrides[code.upper()] = spec
        self._fallback: Dict[str, ContractSpec] = {}
        for product, payload in (fallback_specs or {}).items():
            spec = _spec_from_payload(product, payload)
            if spec is not None:
                self._fallback[product.upper()] = spec

    @property
    def fallback_specs(self) -> Dict[str, ContractSpec]:
        """离线兜底规格，供规格表合并到内置兜底层。"""

        return dict(self._fallback)

    def query_contract(self, security: str, day: Optional[Date] = None) -> Optional[ContractSpec]:
        """查询合约级覆盖规格。

        Args:
            security: 合约代码。
            day: 未使用，保留以统一接口。

        Returns:
            Optional[ContractSpec]: 未配置覆盖时返回 None。
        """

        if is_pseudo_contract(security):
            return None
        return self._overrides.get(str(security).upper())


def _spec_from_payload(key: str, payload: Any) -> Optional[ContractSpec]:
    """把配置片段转成 ContractSpec。

    Args:
        key: 合约代码或品种代码，用于推导品种字段。
        payload: 配置片段，需至少含正的合约乘数。

    Returns:
        Optional[ContractSpec]: 乘数缺失或非法时返回 None。
    """

    if not isinstance(payload, Mapping):
        return None
    multiplier = _to_positive_float(payload.get("multiplier", payload.get("contract_multiplier")))
    if multiplier is None:
        return None
    letters = "".join(ch for ch in str(key) if ch.isalpha())
    return ContractSpec(
        product=(payload.get("product") or letters or str(key)).upper(),
        multiplier=multiplier,
        tick_size=_to_positive_float(payload.get("tick_size")),
        margin_rate=_to_positive_float(payload.get("margin_rate")),
    )


class RemoteSpecSource(ContractSpecSource):
    """通过数据接口批量解析合约规格，带进程内缓存与伪合约拦截。"""

    name = "remote"

    def __init__(self, fetcher: Optional[Any] = None, batch_size: int = _REMOTE_BATCH_SIZE) -> None:
        """构建远端规格来源。

        Args:
            fetcher: 可调用对象，签名 (codes, day) -> Mapping[str, Mapping[str, Any]]；
                缺省惰性使用数据接口层的 get_futures_info。
            batch_size: 单次远端请求的合约数量上限。

        Returns:
            None: 仅初始化缓存。
        """

        self._fetcher = fetcher
        self._batch_size = max(1, int(batch_size))
        self._cache: Dict[str, Optional[ContractSpec]] = {}

    def _resolve_fetcher(self) -> Optional[Any]:
        if self._fetcher is not None:
            return self._fetcher
        try:
            from ..data import api as data_api
        except Exception as exc:  # pragma: no cover - 数据层不可用时降级为无远端
            log.debug("合约规格远端来源不可用: %s", exc)
            self._fetcher = lambda codes, day: {}
            return self._fetcher
        self._fetcher = lambda codes, day: data_api.get_futures_info(list(codes), date=day) or {}
        return self._fetcher

    def query_contract(self, security: str, day: Optional[Date] = None) -> Optional[ContractSpec]:
        """查询单个合约规格，未命中时单独发起一次远端请求。

        Args:
            security: 合约代码。
            day: 回测交易日。

        Returns:
            Optional[ContractSpec]: 远端无该合约或乘数非法时返回 None。
        """

        code = str(security).upper()
        if is_pseudo_contract(code):
            return None
        if code in self._cache:
            return self._cache[code]
        self._load([code], day)
        return self._cache.get(code)

    def prefetch(self, securities: Iterable[str], day: Optional[Date] = None) -> None:
        """批量预热合约规格。

        Args:
            securities: 合约代码集合。
            day: 回测交易日。

        Returns:
            None: 结果写入进程内缓存。
        """

        pending = sorted(
            {
                str(code).upper()
                for code in securities
                if code and not is_pseudo_contract(str(code)) and str(code).upper() not in self._cache
            }
        )
        for start in range(0, len(pending), self._batch_size):
            self._load(pending[start : start + self._batch_size], day)

    def _load(self, codes: Sequence[str], day: Optional[Date]) -> None:
        """执行一次远端批量请求并写入缓存。

        Args:
            codes: 本批合约代码。
            day: 回测交易日。

        Returns:
            None: 结果写入 self._cache。
        """

        if not codes:
            return
        fetcher = self._resolve_fetcher()
        try:
            raw = fetcher(list(codes), day) or {}
        except Exception as exc:
            log.warning("远端合约规格查询失败，跳过 %d 个合约: %s", len(codes), exc)
            raw = {}
        if not isinstance(raw, Mapping):
            raw = {}
        for code in codes:
            payload = raw.get(code) or raw.get(code.lower())
            self._cache[code] = _spec_from_payload(code, payload) if isinstance(payload, Mapping) else None


class ChainedSpecSource(ContractSpecSource):
    """按顺序串联多个规格来源，取第一个给出结果的来源。"""

    name = "chained"

    def __init__(self, sources: Sequence[ContractSpecSource]) -> None:
        """构建串联来源。

        Args:
            sources: 按优先级从高到低排列的来源。

        Returns:
            None: 仅保存来源列表。
        """

        self._sources: Tuple[ContractSpecSource, ...] = tuple(sources)

    @property
    def sources(self) -> Tuple[ContractSpecSource, ...]:
        """当前来源链。"""

        return self._sources

    def query_contract(self, security: str, day: Optional[Date] = None) -> Optional[ContractSpec]:
        """依次询问各来源。

        Args:
            security: 合约代码。
            day: 回测交易日。

        Returns:
            Optional[ContractSpec]: 所有来源都落空时返回 None。
        """

        if is_pseudo_contract(security):
            return None
        for source in self._sources:
            spec = source.query_contract(security, day)
            if spec is not None:
                return spec
        return None

    def prefetch(self, securities: Iterable[str], day: Optional[Date] = None) -> None:
        """把预热请求转发给所有来源。

        Args:
            securities: 合约代码集合。
            day: 回测交易日。

        Returns:
            None: 各来源自行缓存。
        """

        codes = list(securities)
        for source in self._sources:
            source.prefetch(codes, day)


@dataclass
class FuturesSpecConfig:
    """合约规格配置文件解析结果。

    Attributes:
        version: 配置文件版本号。
        margin_default: 配置层的全局缺省保证金率，None 表示不覆盖。
        margin_rules: 品种到保证金率规则的映射。
        spec_source: 由 contract_overrides 构建的 JSON 规格来源。
        fallback_specs: 品种到离线兜底规格的映射。
        path: 配置文件路径，未加载文件时为 None。
    """

    version: int = 1
    margin_default: Optional[float] = None
    margin_rules: Dict[str, MarginRateRule] = field(default_factory=dict)
    spec_source: JsonSpecSource = field(default_factory=JsonSpecSource)
    fallback_specs: Dict[str, ContractSpec] = field(default_factory=dict)
    path: Optional[str] = None

    def margin_rate(self, product: str, day: Optional[Date] = None) -> Optional[float]:
        """取得配置层保证金率。

        Args:
            product: 品种代码。
            day: 交易日，用于命中生效日期分段。

        Returns:
            Optional[float]: 未配置时返回 None。
        """

        rule = self.margin_rules.get(str(product).upper())
        if rule is not None:
            rate = rule.rate_on(day)
            if rate is not None:
                return rate
        return self.margin_default


def default_config_path() -> str:
    """返回随包发布的合约规格配置文件路径。

    Returns:
        str: bullet_trade/config/futures_contract_specs.json 的绝对路径。
    """

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "config", _DEFAULT_CONFIG_FILENAME)


def load_futures_spec_config(path: Optional[str] = None) -> FuturesSpecConfig:
    """加载并解析合约规格配置文件。

    Args:
        path: 配置文件路径，None 表示使用随包发布的默认配置。

    Returns:
        FuturesSpecConfig: 解析结果；文件缺失或损坏时返回空的缺省配置。
    """

    resolved = path or default_config_path()
    payload: Mapping[str, Any] = {}
    if os.path.exists(resolved):
        try:
            with open(resolved, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, Mapping):
                payload = loaded
        except Exception as exc:
            log.warning("合约规格配置读取失败 %s: %s", resolved, exc)
    else:
        log.debug("合约规格配置不存在: %s", resolved)

    margin_payload = payload.get("margin_rate") or {}
    if not isinstance(margin_payload, Mapping):
        margin_payload = {}
    rules: Dict[str, MarginRateRule] = {}
    for product, entry in (margin_payload.get("by_product") or {}).items():
        rule = _margin_rule_from_payload(str(product), entry)
        if rule is not None:
            rules[rule.product] = rule

    source = JsonSpecSource(
        contract_overrides=payload.get("contract_overrides") or {},
        fallback_specs=payload.get("offline_fallback_specs") or {},
    )
    return FuturesSpecConfig(
        version=int(payload.get("version") or 1),
        margin_default=_to_positive_float(margin_payload.get("default")),
        margin_rules=rules,
        spec_source=source,
        fallback_specs=source.fallback_specs,
        path=resolved,
    )


def _margin_rule_from_payload(product: str, entry: Any) -> Optional[MarginRateRule]:
    """把保证金率配置片段转成 MarginRateRule。

    Args:
        product: 品种代码。
        entry: 浮点数、或含 rate 与 effective 分段的映射。

    Returns:
        Optional[MarginRateRule]: 无任何有效费率时返回 None。
    """

    if not isinstance(entry, Mapping):
        rate = _to_positive_float(entry)
        return None if rate is None else MarginRateRule(product=product.upper(), rate=rate)

    segments: List[Tuple[Date, float]] = []
    for segment in entry.get("effective") or ():
        if not isinstance(segment, Mapping):
            continue
        start = _to_date(segment.get("start"))
        rate = _to_positive_float(segment.get("rate"))
        if start is None or rate is None:
            continue
        segments.append((start, rate))
    segments.sort(key=lambda item: item[0])
    base = _to_positive_float(entry.get("rate"))
    if base is None and segments:
        base = segments[0][1]
    if base is None and not segments:
        return None
    return MarginRateRule(product=product.upper(), rate=base, effective=tuple(segments))


def build_spec_source(
    config: Optional[FuturesSpecConfig] = None,
    *,
    remote_fetcher: Optional[Any] = None,
    enable_remote: bool = True,
) -> Tuple[ChainedSpecSource, FuturesSpecConfig]:
    """构建默认的合约规格来源链。

    Args:
        config: 已加载的配置，None 表示加载随包默认配置。
        remote_fetcher: 远端查询函数，主要供测试注入。
        enable_remote: 关闭后只保留 JSON 覆盖层。

    Returns:
        Tuple[ChainedSpecSource, FuturesSpecConfig]: 来源链与所用配置。
    """

    resolved = config if config is not None else load_futures_spec_config()
    sources: List[ContractSpecSource] = [resolved.spec_source]
    if enable_remote:
        sources.append(RemoteSpecSource(fetcher=remote_fetcher))
    return ChainedSpecSource(sources), resolved
