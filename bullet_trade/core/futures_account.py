"""
期货多空账本

作者: BruceLee
文件职责: 提供期货合约规格解析、多空持仓、保证金冻结与释放、盯市结算和到期了结。
主要输入: 合约代码、成交方向与手数、成交价、手续费、每日结算价。
主要输出: 持仓对象、现金变动、已实现盈亏、保证金占用与账户权益。
上下游关系: 上游是回测引擎的撮合分派与策略下单接口，下游是组合权益统计与逐日对账。
关键约定: 保证金按 价×手数×合约乘数×保证金率 计算；现金在开仓时扣保证金、平仓时释放保证金并计入盯市盈亏；
         保证金以实际占用额入账，开仓按成交价计收、日终按结算价结转，差额由现金吸收，故结算不改变权益；
         权益 = 现金 + 占用保证金 + 相对上一结算价的盯市盈亏，相对开仓均价的浮动盈亏仅用于绩效展示；
         合约乘数或保证金率缺失时显式报错，不得退化为乘数 1 或保证金率 0。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as Date
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .security_id import SecurityId, SecurityIdValidationError, parse_security_id

LONG = "long"
SHORT = "short"

_FUTURES_EXCHANGE_VALUES = frozenset({"XSGE", "CCFX", "XDCE", "XZCE", "XINE", "GFEX"})


@dataclass(frozen=True)
class ContractSpec:
    """单个期货品种的合约规格。

    Attributes:
        product: 品种代码，例如 LH。
        multiplier: 合约乘数（每手对应的标的数量）。
        tick_size: 最小变动价位，未知时为 None。
        margin_rate: 交易所挂牌保证金率，未知时为 None。
    """

    product: str
    multiplier: float
    tick_size: Optional[float] = None
    margin_rate: Optional[float] = None


# 品种规格来自各交易所公开合约文本；新增品种前须逐条核对，错误乘数会直接放大盈亏与保证金。
_BUILTIN_SPECS: Tuple[ContractSpec, ...] = (
    ContractSpec("LH", 16.0, 5.0),
    ContractSpec("IF", 300.0, 0.2),
    ContractSpec("IH", 300.0, 0.2),
    ContractSpec("IC", 200.0, 0.2),
    ContractSpec("RB", 10.0, 1.0),
    ContractSpec("HC", 10.0, 1.0),
    ContractSpec("CU", 5.0, 10.0),
    ContractSpec("AL", 5.0, 5.0),
    ContractSpec("AU", 1000.0, 0.02),
    ContractSpec("AG", 15.0, 1.0),
    ContractSpec("I", 100.0, 0.5),
    ContractSpec("J", 100.0, 0.5),
    ContractSpec("JM", 60.0, 0.5),
    ContractSpec("M", 10.0, 1.0),
    ContractSpec("Y", 10.0, 2.0),
    ContractSpec("P", 10.0, 2.0),
    ContractSpec("C", 10.0, 1.0),
    ContractSpec("MA", 10.0, 1.0),
    ContractSpec("TA", 5.0, 2.0),
    ContractSpec("SR", 10.0, 1.0),
    ContractSpec("CF", 5.0, 5.0),
    ContractSpec("SC", 1000.0, 0.1),
)


class ContractSpecError(RuntimeError):
    """合约规格缺失或不可用。"""


class InsufficientMarginError(RuntimeError):
    """可用资金不足以支付开仓保证金与手续费。"""


class OverCloseError(RuntimeError):
    """平仓手数超过对应方向的持仓。"""


def is_futures_security(security: str) -> bool:
    """判断证券代码是否属于期货交易所。

    Args:
        security: 带交易所后缀的证券代码。

    Returns:
        bool: 属于期货交易所返回 True，无法解析或属股票/基金交易所返回 False。
    """

    try:
        identity = parse_security_id(security, "legacy_alias")
    except (SecurityIdValidationError, AttributeError, TypeError):
        return False
    return identity.exchange.value in _FUTURES_EXCHANGE_VALUES


def futures_product(security: str) -> str:
    """提取期货合约的品种代码。

    Args:
        security: 带交易所后缀的期货合约代码，例如 LH2109.XDCE。

    Returns:
        str: 大写品种字母，例如 LH。

    Raises:
        ContractSpecError: 代码不可解析或不含品种字母前缀。
    """

    try:
        identity: SecurityId = parse_security_id(security, "legacy_alias")
    except (SecurityIdValidationError, AttributeError, TypeError) as exc:
        raise ContractSpecError("无法解析期货合约代码: {0}".format(security)) from exc
    letters = ""
    for char in identity.symbol:
        if char.isalpha():
            letters += char
        else:
            break
    if not letters:
        raise ContractSpecError("期货合约代码缺少品种字母: {0}".format(security))
    return letters.upper()


class ContractSpecTable:
    """合约规格表，按合约代码分层解析规格并按四级优先级解析保证金率。"""

    def __init__(
        self,
        specs: Optional[Iterable[ContractSpec]] = None,
        margin_rate: Optional[float] = None,
        margin_rate_by_product: Optional[Mapping[str, float]] = None,
        spec_source: Optional[Any] = None,
        config: Optional[Any] = None,
        load_config: bool = True,
        enable_remote: bool = False,
    ) -> None:
        """构建规格表。

        Args:
            specs: 追加或覆盖内置规格的品种级合约规格集合。
            margin_rate: 全局保证金率覆盖，低于品种级覆盖、高于配置文件。
            margin_rate_by_product: 按品种的保证金率覆盖，优先级最高。
            spec_source: 注入的合约级规格来源，需实现 query_contract(security, day)。
            config: 注入的规格配置对象，None 且 load_config 为真时惰性加载随包配置。
            load_config: 是否加载随包发布的合约规格配置文件。
            enable_remote: 是否允许通过数据接口解析合约规格。

        Returns:
            None: 仅初始化内部映射。
        """

        self._fallback_products: Dict[str, ContractSpec] = {spec.product: spec for spec in _BUILTIN_SPECS}
        self._registered_products: Dict[str, ContractSpec] = {}
        for spec in specs or ():
            self._registered_products[spec.product] = spec
        self._contract_specs: Dict[str, ContractSpec] = {}
        self._resolved_cache: Dict[Tuple[str, Optional[Date]], Optional[ContractSpec]] = {}
        self._margin_rate = margin_rate
        self._margin_rate_by_product = dict(margin_rate_by_product or {})
        self._spec_source = spec_source
        self._config = config
        self._config_loaded = config is not None
        self._fallback_merged = False
        self._load_config = load_config
        self._enable_remote = enable_remote

    def _ensure_config(self) -> Optional[Any]:
        """惰性加载规格配置，避免在无期货需求的进程里读盘。

        Returns:
            Optional[Any]: 配置对象，加载失败或未启用时返回 None。
        """

        if not self._config_loaded:
            self._config_loaded = True
            if self._load_config:
                from .contract_specs import load_futures_spec_config

                self._config = load_futures_spec_config()
        if self._config is not None and not self._fallback_merged:
            self._fallback_merged = True
            for product, spec in self._config.fallback_specs.items():
                self._fallback_products.setdefault(product, spec)
        return self._config

    def _source(self) -> Optional[Any]:
        """取得合约级规格来源，必要时按当前配置构建。

        Returns:
            Optional[Any]: 来源对象，未配置时返回 None。
        """

        config = self._ensure_config()
        if self._spec_source is None and (config is not None or self._enable_remote):
            from .contract_specs import FuturesSpecConfig, build_spec_source

            base = config if config is not None else FuturesSpecConfig()
            self._spec_source, _ = build_spec_source(base, enable_remote=self._enable_remote)
        return self._spec_source

    def register(
        self,
        product: str,
        multiplier: float,
        tick_size: Optional[float] = None,
        margin_rate: Optional[float] = None,
    ) -> None:
        """注册或更新一个品种的规格。

        Args:
            product: 品种代码。
            multiplier: 合约乘数，必须为正。
            tick_size: 最小变动价位。
            margin_rate: 该品种的挂牌保证金率。

        Returns:
            None: 结果写入内部映射。

        Raises:
            ContractSpecError: 乘数不是正数。
        """

        if not multiplier or float(multiplier) <= 0:
            raise ContractSpecError("合约乘数必须为正数: {0}".format(product))
        self._registered_products[product.upper()] = ContractSpec(
            product=product.upper(),
            multiplier=float(multiplier),
            tick_size=None if tick_size is None else float(tick_size),
            margin_rate=None if margin_rate is None else float(margin_rate),
        )
        self._resolved_cache.clear()

    def register_contract(
        self,
        security: str,
        multiplier: float,
        tick_size: Optional[float] = None,
        margin_rate: Optional[float] = None,
    ) -> None:
        """注册或更新单个合约的规格，优先级高于品种级注册。

        Args:
            security: 带交易所后缀的合约代码。
            multiplier: 合约乘数，必须为正。
            tick_size: 最小变动价位，None 表示该合约不归档价格。
            margin_rate: 该合约的挂牌保证金率。

        Returns:
            None: 结果写入内部映射。

        Raises:
            ContractSpecError: 乘数不是正数，或代码是伪合约。
        """

        from .contract_specs import is_pseudo_contract

        code = str(security).upper()
        if is_pseudo_contract(code):
            raise ContractSpecError("主力/连续/指数合约不可参与撮合，无法登记交易规格: {0}".format(code))
        if not multiplier or float(multiplier) <= 0:
            raise ContractSpecError("合约乘数必须为正数: {0}".format(code))
        self._contract_specs[code] = ContractSpec(
            product=futures_product(code),
            multiplier=float(multiplier),
            tick_size=None if tick_size is None else float(tick_size),
            margin_rate=None if margin_rate is None else float(margin_rate),
        )
        self._resolved_cache.pop((code, None), None)

    def set_margin_rate(self, margin_rate: Optional[float]) -> None:
        """设置全局保证金率覆盖。

        Args:
            margin_rate: 保证金率，None 表示取消覆盖。

        Returns:
            None: 结果写入内部字段。
        """

        self._margin_rate = None if margin_rate is None else float(margin_rate)

    def set_margin_rate_by_product(self, margin_rate_by_product: Optional[Mapping[str, float]]) -> None:
        """整体替换按品种的保证金率覆盖。

        Args:
            margin_rate_by_product: 品种到保证金率的映射，None 表示清空。

        Returns:
            None: 结果写入内部映射。
        """

        self._margin_rate_by_product = {
            str(product).upper(): float(rate)
            for product, rate in (margin_rate_by_product or {}).items()
            if rate is not None and float(rate) > 0
        }

    def set_spec_source(self, spec_source: Optional[Any]) -> None:
        """替换合约级规格来源。

        Args:
            spec_source: 实现 query_contract(security, day) 的对象，None 表示清空。

        Returns:
            None: 结果写入内部字段并清空解析缓存。
        """

        self._spec_source = spec_source
        self._resolved_cache.clear()

    def prefetch(self, securities: Iterable[str], day: Optional[Date] = None) -> None:
        """预热一批合约的规格，减少回测循环内的远端往返。

        Args:
            securities: 合约代码集合。
            day: 回测交易日。

        Returns:
            None: 结果由来源自行缓存。
        """

        source = self._source()
        if source is not None:
            source.prefetch(securities, day)

    def spec(self, security: str, day: Optional[Date] = None) -> ContractSpec:
        """取得合约规格，按 合约覆盖 > 品种注册 > 来源链 > 内置兜底 解析。

        Args:
            security: 期货合约代码。
            day: 回测交易日，传递给来源链用于按日解析。

        Returns:
            ContractSpec: 该合约的规格。

        Raises:
            ContractSpecError: 代码是伪合约，或四层来源都未取得规格。
        """

        from .contract_specs import is_pseudo_contract

        code = str(security).upper()
        if is_pseudo_contract(code):
            raise ContractSpecError(
                "主力/连续/指数合约（{0}）没有可撮合的合约规格，只能用于行情信号".format(code)
            )
        product = futures_product(code)
        for spec in (
            self._contract_specs.get(code),
            self._registered_products.get(product),
        ):
            if spec is not None:
                return spec

        cache_key = (code, day)
        if cache_key in self._resolved_cache:
            cached = self._resolved_cache[cache_key]
        else:
            source = self._source()
            cached = None if source is None else source.query_contract(code, day)
            self._resolved_cache[cache_key] = cached
        if cached is not None:
            return cached

        fallback = self._fallback_products.get(product)
        if fallback is None:
            self._ensure_config()
            fallback = self._fallback_products.get(product)
        if fallback is None:
            raise ContractSpecError(
                "缺少品种 {0} 的合约规格（合约乘数未知），"
                "请通过 ContractSpecTable.register 或合约规格配置补充".format(product)
            )
        return fallback

    def multiplier(self, security: str, day: Optional[Date] = None) -> float:
        """取得合约乘数。

        Args:
            security: 期货合约代码。
            day: 回测交易日。

        Returns:
            float: 每手对应的标的数量。

        Raises:
            ContractSpecError: 品种未登记规格。
        """

        return float(self.spec(security, day).multiplier)

    def tick_size(self, security: str, day: Optional[Date] = None) -> Optional[float]:
        """取得最小变动价位。

        Args:
            security: 期货合约代码。
            day: 回测交易日。

        Returns:
            Optional[float]: 未登记时返回 None。

        Raises:
            ContractSpecError: 品种未登记规格。
        """

        return self.spec(security, day).tick_size

    def margin_rate(self, security: str, day: Optional[Date] = None) -> float:
        """取得保证金率，优先级为 品种覆盖 > 全局覆盖 > 配置分段 > 规格挂牌费率。

        Args:
            security: 期货合约代码。
            day: 回测交易日，用于命中配置的生效日期分段。

        Returns:
            float: 大于 0 的保证金率。

        Raises:
            ContractSpecError: 四个来源都未取得有效保证金率。
        """

        product = futures_product(security)
        candidates: List[Optional[float]] = [
            self._margin_rate_by_product.get(product),
            self._margin_rate,
        ]
        config = self._ensure_config()
        if config is not None:
            candidates.append(config.margin_rate(product, day))
        try:
            candidates.append(self.spec(security, day).margin_rate)
        except ContractSpecError:
            # 规格缺失属于另一类错误，此处只关心保证金率是否可得
            candidates.append(None)
        for candidate in candidates:
            if candidate is not None and float(candidate) > 0:
                return float(candidate)
        raise ContractSpecError(
            "缺少品种 {0} 的保证金率，请通过 set_option('futures_margin_rate', x) "
            "或合约规格配置提供".format(product)
        )


@dataclass
class FuturesPosition:
    """单向期货持仓。

    Attributes:
        security: 合约代码。
        side: 方向，'long' 或 'short'。
        amount: 持仓手数。
        today_amount: 当日开仓手数，日终结算后归零。
        open_price: 持仓开仓均价。
        last_price: 最新价。
        prev_settlement: 上一交易日结算价；当日开仓时为开仓均价。
        multiplier: 合约乘数。
        margin_rate: 保证金率。
        margin_held: 账户当前为该持仓实际占用的保证金；开仓按成交价计入，日终结算按结算价重新计收。
        end_date: 合约最后交易日，未知时为 None。
        open_time: 首次建仓时间。
        last_trade_time: 最近一次成交时间。
    """

    security: str
    side: str
    amount: int = 0
    today_amount: int = 0
    open_price: float = 0.0
    last_price: float = 0.0
    prev_settlement: float = 0.0
    multiplier: float = 1.0
    margin_rate: float = 0.0
    margin_held: float = 0.0
    end_date: Optional[Date] = None
    open_time: Optional[datetime] = None
    last_trade_time: Optional[datetime] = None

    @property
    def direction(self) -> int:
        """返回方向符号。

        Returns:
            int: 多头 +1，空头 -1。
        """

        return 1 if self.side == LONG else -1

    @property
    def closeable_amount(self) -> int:
        """返回可平手数；期货无 T+1，当日开仓即可平。

        Returns:
            int: 当前持仓手数。
        """

        return self.amount

    @property
    def yesterday_amount(self) -> int:
        """返回昨仓手数。

        Returns:
            int: 持仓手数减去当日开仓手数。
        """

        return max(0, self.amount - self.today_amount)

    @property
    def margin(self) -> float:
        """返回当前实际占用的保证金。

        Returns:
            float: 开仓时按成交价计入、日终结算后按结算价重新计收的保证金。
        """

        return self.margin_held

    def margin_at(self, price: float, amount: Optional[int] = None) -> float:
        """按指定价格计算保证金。

        Args:
            price: 计收保证金的价格。
            amount: 手数，缺省为当前持仓手数。

        Returns:
            float: 手数×价格×合约乘数×保证金率。
        """

        lots = self.amount if amount is None else amount
        return lots * float(price) * self.multiplier * self.margin_rate

    @property
    def market_value(self) -> float:
        """返回合约名义市值。

        Returns:
            float: 手数×最新价×合约乘数。
        """

        return self.amount * self.last_price * self.multiplier

    @property
    def floating_pnl(self) -> float:
        """返回相对开仓均价的浮动盈亏，仅用于绩效展示。

        Returns:
            float: (最新价-开仓均价)×手数×合约乘数×方向符号。
        """

        return (self.last_price - self.open_price) * self.amount * self.multiplier * self.direction

    @property
    def daily_pnl(self) -> float:
        """返回相对上一结算价的盯市盈亏；账户权益中的浮动项以此为准。

        Returns:
            float: (最新价-上一结算价)×手数×合约乘数×方向符号。
        """

        return (
            (self.last_price - self.prev_settlement)
            * self.amount
            * self.multiplier
            * self.direction
        )

    def update_price(self, price: float) -> None:
        """更新最新价。

        Args:
            price: 最新价。

        Returns:
            None: 结果写入 last_price。
        """

        if price is None:
            return
        self.last_price = float(price)

    def apply_open(
        self, amount: int, price: float, trade_time: Optional[datetime]
    ) -> float:
        """按开仓成交更新持仓、开仓均价与实际占用保证金。

        Args:
            amount: 开仓手数，正数。
            price: 成交价。
            trade_time: 成交时间。

        Returns:
            float: 本次开仓新增占用的保证金。
        """

        old_amount = self.amount
        total_cost = self.open_price * old_amount + price * amount
        self.amount += amount
        self.today_amount += amount
        self.open_price = total_cost / self.amount if self.amount else 0.0
        if self.prev_settlement <= 0:
            self.prev_settlement = price
        else:
            # 新增手数的盯市起点是成交价，不是上一结算价；不合并会让当日结算
            # 把 (成交价 - 上一结算价) 当成浮动盈亏计入权益。
            self.prev_settlement = (
                self.prev_settlement * old_amount + price * amount
            ) / self.amount
        added_margin = amount * float(price) * self.multiplier * self.margin_rate
        self.margin_held += added_margin
        self.update_price(price)
        if self.open_time is None:
            self.open_time = trade_time
        self.last_trade_time = trade_time
        return added_margin

    def apply_close(
        self, amount: int, price: float, trade_time: Optional[datetime]
    ) -> Tuple[float, float, float]:
        """按平仓成交更新持仓。

        Args:
            amount: 平仓手数，正数。
            price: 成交价。
            trade_time: 成交时间。

        Returns:
            Tuple[float, float, float]: 相对开仓均价的盈亏、释放的保证金、相对上一结算价的盯市盈亏。

        Raises:
            OverCloseError: 平仓手数超过持仓。
        """

        if amount > self.amount:
            raise OverCloseError(
                "{0} {1} 平仓 {2} 手超过持仓 {3} 手".format(
                    self.security, self.side, amount, self.amount
                )
            )
        pnl = (price - self.open_price) * amount * self.multiplier * self.direction
        variation = (
            (float(price) - self.prev_settlement) * amount * self.multiplier * self.direction
        )
        released = self.margin_held * amount / self.amount
        closed_today = min(amount, self.today_amount)
        self.today_amount -= closed_today
        self.amount -= amount
        self.margin_held -= released
        if self.amount == 0:
            self.open_price = 0.0
            self.today_amount = 0
            self.margin_held = 0.0
        self.update_price(price)
        self.last_trade_time = trade_time
        return pnl, released, variation

    def settle(self, settlement_price: float) -> Tuple[float, float]:
        """按结算价盯市，并把保证金计收基准结转到结算价。

        Args:
            settlement_price: 当日结算价。

        Returns:
            Tuple[float, float]: 盯市变动盈亏、保证金计收基准变化（正数表示需追加，负数表示释放）。
        """

        variation = (
            (settlement_price - self.prev_settlement)
            * self.amount
            * self.multiplier
            * self.direction
        )
        required = self.amount * float(settlement_price) * self.multiplier * self.margin_rate
        margin_delta = required - self.margin_held
        self.margin_held = required
        self.prev_settlement = float(settlement_price)
        self.update_price(settlement_price)
        self.today_amount = 0
        return variation, margin_delta


@dataclass
class CloseResult:
    """一次平仓的账本结果。

    Attributes:
        security: 合约代码。
        side: 被平持仓的方向。
        amount: 平仓手数。
        price: 成交价。
        released_margin: 释放的保证金。
        variation_pnl: 相对上一结算价的盯市盈亏，构成现金变动。
        realized_pnl: 相对开仓均价的已实现盈亏，用于绩效统计。
        commission: 手续费。
        cash_delta: 现金净变动。
        close_today: 是否按平今计费。
    """

    security: str
    side: str
    amount: int
    price: float
    released_margin: float
    variation_pnl: float
    realized_pnl: float
    commission: float
    cash_delta: float
    close_today: bool = False


@dataclass
class SettlementResult:
    """一次日终结算的结果。

    Attributes:
        day: 结算日。
        variation_margin: 全账户当日盯市变动合计。
        margin: 结算后的保证金占用。
        delivered: 到期了结记录，元素为 (合约, 方向, 手数, 结算价, 盯市盈亏)。
        missing_settlement: 缺结算价的持仓合约。
    """

    day: Optional[Date] = None
    variation_margin: float = 0.0
    margin: float = 0.0
    delivered: Tuple[Tuple[str, str, int, float, float], ...] = ()
    missing_settlement: Tuple[str, ...] = ()


@dataclass
class FuturesAccount:
    """期货子账户账本，按合约与方向维护持仓并用盯市口径结算现金。

    Attributes:
        cash: 可用资金；开仓扣保证金与手续费，平仓释放保证金并计入盯市盈亏。
        spec_table: 合约规格表。
        starting_cash: 初始资金。
        realized_pnl: 累计已实现盈亏（相对开仓均价）。
        commission_paid: 累计手续费。
        today_realized_pnl: 当日已实现盈亏。
        today_commission: 当日手续费。
        today_variation: 当日结算产生的盯市变动。
        positions: 以 (合约, 方向) 为键的持仓映射。
    """

    cash: float = 0.0
    spec_table: ContractSpecTable = field(default_factory=ContractSpecTable)
    starting_cash: float = 0.0
    realized_pnl: float = 0.0
    commission_paid: float = 0.0
    today_realized_pnl: float = 0.0
    today_commission: float = 0.0
    today_variation: float = 0.0
    positions: Dict[Tuple[str, str], FuturesPosition] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """初始化起始资金默认值。

        Returns:
            None: 仅在未显式给出起始资金时回填。
        """

        if not self.starting_cash:
            self.starting_cash = self.cash

    def get_position(self, security: str, side: Optional[str] = None) -> Optional[FuturesPosition]:
        """取得持仓。

        Args:
            security: 合约代码。
            side: 方向；None 时返回该合约任意方向的第一个持仓。

        Returns:
            Optional[FuturesPosition]: 无持仓时返回 None。
        """

        if side is not None:
            return self.positions.get((security, side))
        for key in ((security, LONG), (security, SHORT)):
            position = self.positions.get(key)
            if position is not None:
                return position
        return None

    def iter_positions(self) -> Iterable[FuturesPosition]:
        """遍历全部持仓。

        Returns:
            Iterable[FuturesPosition]: 按 (合约, 方向) 键序稳定遍历。
        """

        for key in sorted(self.positions):
            yield self.positions[key]

    @property
    def margin(self) -> float:
        """返回保证金占用合计。

        Returns:
            float: 各持仓保证金之和。
        """

        return sum(position.margin for position in self.positions.values())

    @property
    def floating_pnl(self) -> float:
        """返回浮动盈亏合计（相对开仓均价），仅用于绩效展示。

        Returns:
            float: 各持仓浮动盈亏之和。
        """

        return sum(position.floating_pnl for position in self.positions.values())

    @property
    def mark_to_market_pnl(self) -> float:
        """返回盯市浮动盈亏合计（相对上一结算价）。

        Returns:
            float: 各持仓盯市盈亏之和；这是权益中的浮动项。
        """

        return sum(position.daily_pnl for position in self.positions.values())

    @property
    def total_value(self) -> float:
        """返回账户权益。

        Returns:
            float: 可用资金 + 实际占用保证金 + 盯市浮动盈亏。
        """

        return self.cash + self.margin + self.mark_to_market_pnl

    @property
    def available_cash(self) -> float:
        """返回可用资金；保证金已在开仓时从现金扣除。

        Returns:
            float: 当前现金。
        """

        return self.cash

    @property
    def positions_value(self) -> float:
        """返回保证金占用，供组合层以期货口径展示持仓价值。

        Returns:
            float: 保证金占用合计。
        """

        return self.margin

    def required_margin(
        self, security: str, amount: int, price: float, day: Optional[Date] = None
    ) -> float:
        """计算开仓所需保证金。

        Args:
            security: 合约代码。
            amount: 开仓手数，正数。
            price: 用于锁资的价格。
            day: 交易日，用于命中保证金率的生效日期分段。

        Returns:
            float: 手数×价格×合约乘数×保证金率。

        Raises:
            ContractSpecError: 合约乘数或保证金率缺失。
        """

        spec = self.spec_table.spec(security, day)
        margin_rate = self.spec_table.margin_rate(security, day)
        return abs(amount) * float(price) * float(spec.multiplier) * margin_rate

    def open(
        self,
        security: str,
        side: str,
        amount: int,
        price: float,
        commission: float = 0.0,
        trade_time: Optional[datetime] = None,
        end_date: Optional[Date] = None,
    ) -> FuturesPosition:
        """开仓并冻结保证金。

        Args:
            security: 合约代码。
            side: 'long' 或 'short'。
            amount: 开仓手数，正数。
            price: 成交价。
            commission: 手续费。
            trade_time: 成交时间。
            end_date: 合约最后交易日。

        Returns:
            FuturesPosition: 更新后的持仓。

        Raises:
            ContractSpecError: 合约规格或保证金率缺失。
            InsufficientMarginError: 现金不足以支付保证金与手续费。
            ValueError: 方向或手数非法。
        """

        if side not in (LONG, SHORT):
            raise ValueError("期货方向必须是 'long' 或 'short'，收到 {0}".format(side))
        if amount <= 0:
            raise ValueError("开仓手数必须为正数，收到 {0}".format(amount))
        trade_day = trade_time.date() if isinstance(trade_time, datetime) else None
        spec = self.spec_table.spec(security, trade_day)
        margin_rate = self.spec_table.margin_rate(security, trade_day)
        margin = amount * float(price) * float(spec.multiplier) * margin_rate
        required = margin + float(commission)
        if required > self.cash:
            raise InsufficientMarginError(
                "{0} {1} 开仓 {2} 手需要 {3:.2f}（保证金 {4:.2f} + 手续费 {5:.2f}），"
                "可用 {6:.2f}".format(security, side, amount, required, margin,
                                      float(commission), self.cash)
            )
        position = self.positions.get((security, side))
        if position is None:
            position = FuturesPosition(
                security=security,
                side=side,
                multiplier=float(spec.multiplier),
                margin_rate=margin_rate,
                end_date=end_date,
            )
            self.positions[(security, side)] = position
        position.multiplier = float(spec.multiplier)
        position.margin_rate = margin_rate
        if end_date is not None:
            position.end_date = end_date
        added_margin = position.apply_open(int(amount), float(price), trade_time)
        self.cash -= added_margin + float(commission)
        self.commission_paid += float(commission)
        self.today_commission += float(commission)
        return position

    def close(
        self,
        security: str,
        side: str,
        amount: int,
        price: float,
        commission: float = 0.0,
        trade_time: Optional[datetime] = None,
        close_today: bool = False,
    ) -> CloseResult:
        """平仓并释放保证金，现金按盯市口径变动。

        Args:
            security: 合约代码。
            side: 被平持仓的方向，'long' 或 'short'。
            amount: 平仓手数，正数。
            price: 成交价。
            commission: 手续费。
            trade_time: 成交时间。
            close_today: 是否按平今计费，仅影响记录与费用口径。

        Returns:
            CloseResult: 释放保证金、盯市盈亏、已实现盈亏与现金变动。

        Raises:
            OverCloseError: 无持仓或平仓手数超过持仓。
            ValueError: 方向或手数非法。
        """

        if side not in (LONG, SHORT):
            raise ValueError("期货方向必须是 'long' 或 'short'，收到 {0}".format(side))
        if amount <= 0:
            raise ValueError("平仓手数必须为正数，收到 {0}".format(amount))
        position = self.positions.get((security, side))
        if position is None or position.amount <= 0:
            raise OverCloseError("{0} 无 {1} 持仓，不能平仓 {2} 手".format(security, side, amount))
        realized, released, variation = position.apply_close(
            int(amount), float(price), trade_time
        )
        cash_delta = released + variation - float(commission)
        self.cash += cash_delta
        self.realized_pnl += realized
        self.today_realized_pnl += realized
        self.commission_paid += float(commission)
        self.today_commission += float(commission)
        if position.amount == 0:
            del self.positions[(security, side)]
        return CloseResult(
            security=security,
            side=side,
            amount=int(amount),
            price=float(price),
            released_margin=released,
            variation_pnl=variation,
            realized_pnl=realized,
            commission=float(commission),
            cash_delta=cash_delta,
            close_today=bool(close_today),
        )

    def mark_prices(self, prices: Mapping[str, float]) -> None:
        """按最新价刷新持仓估值，不产生现金变动。

        Args:
            prices: 合约到最新价的映射。

        Returns:
            None: 结果写入各持仓的 last_price。
        """

        for position in self.positions.values():
            price = prices.get(position.security)
            if price is not None:
                position.update_price(float(price))

    def settle_day(
        self,
        settlement_prices: Mapping[str, float],
        day: Optional[Date] = None,
    ) -> SettlementResult:
        """日终结算：盯市变动入现金、到期合约了结、当日开仓结转为昨仓。

        Args:
            settlement_prices: 合约到当日结算价的映射。
            day: 结算日，用于判定最后交易日。

        Returns:
            SettlementResult: 盯市合计、结算后保证金、到期了结记录与缺结算价合约。
            缺结算价的持仓不做任何替代估值，保持原基准并出现在 missing_settlement 中。
        """

        variation_total = 0.0
        delivered = []
        delivered_keys = []
        missing = []
        for key in sorted(self.positions):
            position = self.positions[key]
            settlement = settlement_prices.get(position.security)
            if settlement is None:
                missing.append(position.security)
                continue
            variation, margin_delta = position.settle(float(settlement))
            variation_total += variation
            # 现金吸收盯市变动与保证金重算差额，保证结算本身不改变权益
            self.cash += variation - margin_delta
            if day is not None and position.end_date is not None and position.end_date <= day:
                delivered.append(
                    (position.security, position.side, position.amount,
                     float(settlement), variation)
                )
                delivered_keys.append(key)
        for key in delivered_keys:
            position = self.positions.pop(key, None)
            if position is not None:
                self.cash += position.margin_held
        result = SettlementResult(
            day=day,
            variation_margin=variation_total,
            margin=self.margin,
            delivered=tuple(delivered),
            missing_settlement=tuple(dict.fromkeys(missing)),
        )
        self.today_variation = variation_total
        return result

    def on_day_start(self) -> None:
        """日初复位当日累计口径。

        Returns:
            None: 结果写入当日累计字段。
        """

        self.today_realized_pnl = 0.0
        self.today_commission = 0.0
        self.today_variation = 0.0

    def summary(self) -> Dict[str, float]:
        """返回账本快照，用于逐日对账。

        Returns:
            Dict[str, float]: 现金、保证金、浮动盈亏、权益与累计盈亏费用。
        """

        return {
            "cash": self.cash,
            "margin": self.margin,
            "floating_pnl": self.floating_pnl,
            "total_value": self.total_value,
            "realized_pnl": self.realized_pnl,
            "commission_paid": self.commission_paid,
            "today_realized_pnl": self.today_realized_pnl,
            "today_commission": self.today_commission,
            "today_variation": self.today_variation,
            "position_count": float(len(self.positions)),
        }


__all__ = [
    "LONG",
    "SHORT",
    "CloseResult",
    "ContractSpec",
    "ContractSpecError",
    "ContractSpecTable",
    "FuturesAccount",
    "FuturesPosition",
    "InsufficientMarginError",
    "OverCloseError",
    "SettlementResult",
    "futures_product",
    "is_futures_security",
]
