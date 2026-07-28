"""
作者：BruceLee

文件职责：定义并解析回测订单链使用的不可变有效价格口径。
主要输入：回测业务时刻、use_real_price 设置、复权参考日与当前 DataProvider。
主要输出：可供当前行情、保护价、撮合及机器 manifest 共同使用的 EffectivePriceBasis。
上下游关系：上游由数据 API 根据上下文解析，下游由回测引擎和 execution-facts manifest 消费。
关键约定：业务时区固定 Asia/Shanghai；参考日不得晚于业务日；provider 未明确证明能力时失败关闭。
"""

from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Union

BUSINESS_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")


def _normalize_business_time(value: Union[datetime, Date, str]) -> datetime:
    """把业务时刻规范为 Asia/Shanghai aware datetime。

    Args:
        value: datetime、date 或 ISO-8601 字符串；naive datetime 按上海时区解释。

    Returns:
        datetime: 转换到 Asia/Shanghai 的 aware datetime。

    Raises:
        ValueError: 输入为空、格式非法或不能转换为业务时刻。

    Side Effects:
        无；不会读取宿主机当前时间。
    """

    candidate: Union[datetime, Date]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("业务时刻不能为空")
        try:
            candidate = datetime.fromisoformat(text)
        except ValueError:
            try:
                candidate = Date.fromisoformat(text)
            except ValueError as exc:
                raise ValueError("业务时刻必须是 ISO-8601 日期或时间") from exc
    else:
        candidate = value

    if isinstance(candidate, datetime):
        if candidate.tzinfo is None:
            return candidate.replace(tzinfo=BUSINESS_TIMEZONE)
        return candidate.astimezone(BUSINESS_TIMEZONE)
    if isinstance(candidate, Date):
        return datetime.combine(candidate, time.min, tzinfo=BUSINESS_TIMEZONE)
    raise ValueError("业务时刻类型不受支持")


def _normalize_reference_date(value: Union[datetime, Date, str]) -> Date:
    """把显式复权参考日规范为 date。

    Args:
        value: datetime、date 或 ISO-8601 日期/时间字符串。

    Returns:
        date: 不带时区的业务参考日。

    Raises:
        ValueError: 输入为空或无法转换为日期。

    Side Effects:
        无。
    """

    if isinstance(value, datetime):
        normalized = _normalize_business_time(value)
        return normalized.date()
    if isinstance(value, Date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("复权参考日不能为空")
        try:
            return Date.fromisoformat(text)
        except ValueError:
            try:
                return _normalize_business_time(text).date()
            except ValueError as exc:
                raise ValueError("复权参考日必须是 ISO-8601 日期或时间") from exc
    raise ValueError("复权参考日类型不受支持")


@dataclass(frozen=True)
class EffectivePriceBasis:
    """回测订单链共享的不可变价格口径。

    核心协作对象是 DataProvider、BacktestCurrentData、保护价计算和 BacktestEngine。
    实例一经创建不能修改；动态前复权固定锚定业务日，未复权口径不携带参考日。
    """

    use_real_price: bool
    fq: str
    pre_factor_ref_date: Optional[Date]
    provider: str
    business_time: datetime

    def __post_init__(self) -> None:
        """规范并校验冻结字段，保证任何构造路径都不能产生未来参考日。

        Args:
            self: 当前价格口径实例。

        Returns:
            None。

        Raises:
            ValueError: provider、fq、业务时刻或参考日不满足有效口径约束。

        Side Effects:
            仅通过 object.__setattr__ 写入当前冻结实例的规范化初始值。
        """

        provider = str(self.provider or "").strip()
        if not provider:
            raise ValueError("价格口径必须记录 provider")
        fq = str(self.fq or "none").strip().lower()
        if fq not in {"none", "pre"}:
            raise ValueError(f"不支持的有效复权口径: {fq}")
        business_time = _normalize_business_time(self.business_time)
        reference = self.pre_factor_ref_date
        if fq == "pre":
            if reference is None:
                raise ValueError("动态前复权必须记录参考日")
            reference = _normalize_reference_date(reference)
            if reference > business_time.date():
                raise ValueError("复权参考日不得晚于回测业务日")
        elif reference is not None:
            raise ValueError("未复权口径不得携带复权参考日")
        if bool(self.use_real_price) != (fq == "pre"):
            raise ValueError("use_real_price 与 fq 口径不一致")

        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "fq", fq)
        object.__setattr__(self, "business_time", business_time)
        object.__setattr__(self, "pre_factor_ref_date", reference)

    @classmethod
    def create(
        cls,
        *,
        use_real_price: bool,
        provider: str,
        business_time: Union[datetime, Date, str],
        pre_factor_ref_date: Optional[Union[datetime, Date, str]] = None,
    ) -> "EffectivePriceBasis":
        """按回测设置创建口径，并把显式未来参考日限制到当前业务日。

        Args:
            use_real_price: 是否启用以当前业务日为锚的动态前复权。
            provider: 实际提供行情的 provider 名称。
            business_time: 当前回测业务时刻。
            pre_factor_ref_date: 可选显式参考日；晚于业务日时会被限制到业务日。

        Returns:
            EffectivePriceBasis: 已规范化且冻结的有效价格口径。

        Raises:
            ValueError: 业务时刻、provider 或参考日格式非法。

        Side Effects:
            无。
        """

        normalized_time = _normalize_business_time(business_time)
        if not use_real_price:
            return cls(
                use_real_price=False,
                fq="none",
                pre_factor_ref_date=None,
                provider=provider,
                business_time=normalized_time,
            )

        requested_reference = (
            _normalize_reference_date(pre_factor_ref_date)
            if pre_factor_ref_date is not None
            else normalized_time.date()
        )
        effective_reference = min(requested_reference, normalized_time.date())
        return cls(
            use_real_price=True,
            fq="pre",
            pre_factor_ref_date=effective_reference,
            provider=provider,
            business_time=normalized_time,
        )

    def provider_kwargs(self, *, force_no_engine: bool = False) -> Dict[str, Any]:
        """生成同一 provider 的 get_price 口径参数。

        Args:
            force_no_engine: 是否要求 provider 不使用其内部价格引擎；不会改变 provider 身份。

        Returns:
            dict: 至少含 fq；动态前复权时额外含参考日和引擎选择参数。

        Raises:
            不抛出异常。

        Side Effects:
            无；返回新字典。
        """

        kwargs: Dict[str, Any] = {"fq": self.fq}
        if self.fq == "pre":
            kwargs.update(
                pre_factor_ref_date=self.pre_factor_ref_date,
                prefer_engine=not force_no_engine,
                force_no_engine=force_no_engine,
            )
        return kwargs

    def as_dict(self) -> Dict[str, Any]:
        """返回可写入 machine manifest 的脱敏价格口径。

        Args:
            self: 当前有效价格口径。

        Returns:
            dict: 精确匹配 execution-facts/v1 writer 输入的五个价格口径字段。

        Raises:
            不抛出异常。

        Side Effects:
            无；不暴露 provider 配置、地址、token 或账号。
        """

        return {
            "use_real_price": self.use_real_price,
            "fq": self.fq,
            "pre_factor_ref_date": (
                self.pre_factor_ref_date.isoformat() if self.pre_factor_ref_date else None
            ),
            "provider": self.provider,
            "business_time": self.business_time,
        }


class PriceBasisError(RuntimeError):
    """价格口径无法证明或执行时的失败关闭异常，携带脱敏诊断。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        basis: Optional[EffectivePriceBasis] = None,
        provider: Optional[str] = None,
    ) -> None:
        """初始化价格口径异常。

        Args:
            code: 稳定诊断代码。
            message: 不含 secret、地址或账号的用户可见原因。
            basis: 已解析的有效价格口径；能力解析前失败时可以为空。
            provider: basis 为空时使用的脱敏 provider 名称。

        Returns:
            None。

        Raises:
            不主动抛出额外异常。

        Side Effects:
            初始化 RuntimeError 文本和只读诊断字段。
        """

        super().__init__(message)
        self.code = str(code)
        self.safe_message = str(message)
        self.basis = basis
        self.provider = str(provider or (basis.provider if basis else "unknown"))

    def as_dict(self) -> Dict[str, Any]:
        """返回可写入日志、订单诊断或 machine manifest 的脱敏失败信息。

        Args:
            self: 当前价格口径异常。

        Returns:
            dict: 稳定错误代码、安全原因和可用的口径元数据。

        Raises:
            不抛出异常。

        Side Effects:
            无。
        """

        diagnostic = self.basis.as_dict() if self.basis is not None else {"provider": self.provider}
        business_time = diagnostic.get("business_time")
        if isinstance(business_time, datetime):
            diagnostic["business_time"] = business_time.isoformat()
        diagnostic.update(
            {
                "status": "FAILED",
                "failure_code": self.code,
                "failure_reason": self.safe_message,
            }
        )
        return diagnostic


class PriceBasisUnsupportedError(PriceBasisError):
    """provider 未明确声明支持所需有效价格口径。"""


class PriceBasisDataError(PriceBasisError):
    """provider 已声明能力但未能返回可证明一致的行情。"""


def resolve_effective_price_basis(
    *,
    provider: Any,
    business_time: Union[datetime, Date, str],
    use_real_price: bool,
    pre_factor_ref_date: Optional[Union[datetime, Date, str]] = None,
) -> EffectivePriceBasis:
    """解析并验证 provider 明确支持的有效价格口径。

    Args:
        provider: 当前 DataProvider 实例；必须提供能力证明方法。
        business_time: 当前回测业务时刻。
        use_real_price: 是否启用动态前复权。
        pre_factor_ref_date: 可选显式参考日，晚于业务日时会被限制。

    Returns:
        EffectivePriceBasis: provider 明确支持的冻结价格口径。

    Raises:
        PriceBasisUnsupportedError: provider 未提供能力证明或明确不支持。
        ValueError: 时间、provider 名称或参考日格式非法。

    Side Effects:
        无；不会认证、联网或切换 provider。
    """

    provider_name = str(getattr(provider, "name", "") or provider.__class__.__name__).strip()
    basis = EffectivePriceBasis.create(
        use_real_price=bool(use_real_price),
        provider=provider_name,
        business_time=business_time,
        pre_factor_ref_date=pre_factor_ref_date,
    )
    capability = getattr(provider, "supports_effective_price_basis", None)
    if not callable(capability):
        if basis.fq == "none" and callable(getattr(provider, "get_price", None)):
            return basis
        if basis.fq == "pre" and bool(getattr(provider, "supports_dynamic_pre_price_basis", False)):
            return basis
        raise PriceBasisUnsupportedError(
            "PRICE_BASIS_CAPABILITY_MISSING",
            f"数据源 {provider_name} 未声明有效价格口径能力",
            basis=basis,
        )
    if not bool(
        capability(
            fq=basis.fq,
            pre_factor_ref_date=basis.pre_factor_ref_date,
        )
    ):
        raise PriceBasisUnsupportedError(
            "PRICE_BASIS_UNSUPPORTED",
            f"数据源 {provider_name} 无法证明支持 fq={basis.fq} 的可比行情",
            basis=basis,
        )
    return basis


__all__ = [
    "BUSINESS_TIMEZONE",
    "EffectivePriceBasis",
    "PriceBasisDataError",
    "PriceBasisError",
    "PriceBasisUnsupportedError",
    "resolve_effective_price_basis",
]
