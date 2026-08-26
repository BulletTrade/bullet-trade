"""
作者: BruceLee

文件职责: 为华鑫节点完整资产快照提供纯 Python、确定性的摘要合同。
主要输入: 交易日、节点证明、资金对象和完整持仓对象。
主要输出: 持仓快照 ID 与节点资产 SHA-256 摘要。
上游关系: 华鑫归集 writer 和外部只读父账户同步门禁。
下游关系: READY 证据互验；不依赖 Broker、DataProvider、Remote wire 或厂商 SDK。
关键环境约定: 本模块没有网络、文件、数据库或交易副作用，不包含生产节点与账户身份。
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Sequence


HUAXIN_NODE_ASSET_DIGEST_VERSION = "huaxin-node-assets/v2"
HUAXIN_NODE16_READY_SCHEMA = "huaxin-node16-ready/v2"


class HuaxinNodeAssetDigestError(ValueError):
    """表示华鑫完整节点资产不满足确定性摘要合同。"""


def _required_int(value: Any, *, field_name: str) -> int:
    """严格转换必填整数。

    Args:
        value: 原始字段值。
        field_name: 用于错误定位的非敏感字段名。

    Returns:
        int: 已验证整数。

    Raises:
        HuaxinNodeAssetDigestError: 字段缺失、布尔或不是整数时抛出。
    """

    if value in (None, "") or isinstance(value, bool):
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid")
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid") from exc
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid")
    return int(parsed)


def _row_fingerprint(row: Mapping[str, Any], kind: str) -> str:
    """对同行技术身份生成不可逆指纹。

    Args:
        row: 资金行或持仓行。
        kind: ``fund`` 或 ``position``。

    Returns:
        str: SHA-256 十六进制摘要。
    """

    fields = (
        ("department_id", "account_id", "currency")
        if kind == "fund"
        else (
            "exchange",
            "investor_id",
            "business_unit_id",
            "shareholder_id",
            "security",
            "market_id",
        )
    )
    values = [str(row.get(field) or "").strip() for field in fields]
    if any(not value for value in values):
        raise HuaxinNodeAssetDigestError(f"node_asset_{kind}_identity_incomplete")
    material = json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _canonical_decimal_text(value: Any, *, field_name: str) -> str:
    """把资产数值收敛为无指数的确定性十进制文本。

    Args:
        value: 原始必填数值。
        field_name: 用于非敏感错误定位的字段名。

    Returns:
        str: 去除无意义尾零后的十进制文本。

    Raises:
        HuaxinNodeAssetDigestError: 数值非法、非有限或为布尔值时抛出。
    """

    if value in (None, "") or isinstance(value, bool):
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid")
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid") from exc
    if not parsed.is_finite():
        raise HuaxinNodeAssetDigestError(f"{field_name}_invalid")
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _required_snapshot_value(*values: Any, field_name: str) -> Any:
    """从兼容别名中选择第一个显式必填值。

    Args:
        *values: 按优先级排列的候选值。
        field_name: 用于错误定位的非敏感字段名。

    Returns:
        Any: 第一个非空候选。

    Raises:
        HuaxinNodeAssetDigestError: 所有别名均缺失时抛出。
    """

    for value in values:
        if value not in (None, ""):
            return value
    raise HuaxinNodeAssetDigestError(f"{field_name}_missing")


def _canonical_trading_day(value: Any) -> str:
    """验证并返回八位华鑫交易日。

    Args:
        value: 快照中的交易日。

    Returns:
        str: ``YYYYMMDD`` 文本。

    Raises:
        HuaxinNodeAssetDigestError: 日期不是八位数字时抛出。
    """

    trading_day = str(value or "").strip()
    if len(trading_day) != 8 or not trading_day.isdigit():
        raise HuaxinNodeAssetDigestError("node_asset_trading_day_invalid")
    return trading_day


def _canonical_security_for_digest(row: Mapping[str, Any]) -> str:
    """把华鑫持仓代码收敛为跨公共/原生字段一致的证券标识。

    Args:
        row: 公共持仓行或华鑫原生持仓行。

    Returns:
        str: 规范化证券代码。

    Raises:
        HuaxinNodeAssetDigestError: 证券代码缺失时抛出。
    """

    raw = str(row.get("security") or row.get("code") or "").strip().upper()
    exchange = str(row.get("exchange") or row.get("market") or "").strip().upper()
    if not raw:
        raise HuaxinNodeAssetDigestError("node_asset_security_missing")
    base, dot, suffix = raw.partition(".")
    if not base:
        raise HuaxinNodeAssetDigestError("node_asset_security_missing")
    suffixes = {
        "SH": "XSHG",
        "XSHG": "XSHG",
        "SSE": "XSHG",
        "SZ": "XSHE",
        "XSHE": "XSHE",
        "SZSE": "XSHE",
    }
    normalized_suffix = suffixes.get(suffix if dot else "")
    if normalized_suffix is None:
        normalized_suffix = suffixes.get(exchange)
    return f"{base}.{normalized_suffix}" if normalized_suffix else raw


def _canonical_node_position_material(row: Mapping[str, Any]) -> Dict[str, Any]:
    """抽取单条华鑫持仓的确定性、脱敏资产字段。

    Args:
        row: 公共持仓行；允许从 provider extension 回退原生字段。

    Returns:
        Dict[str, Any]: 可排序并参与 SHA-256 的持仓材料。
    """

    extra = row.get("extra") if isinstance(row.get("extra"), Mapping) else {}
    provider_extension = (
        extra.get("provider_extension")
        if isinstance(extra.get("provider_extension"), Mapping)
        else {}
    )
    native = (
        provider_extension.get("huaxin_tora")
        if isinstance(provider_extension.get("huaxin_tora"), Mapping)
        else {}
    )
    combined = dict(native)
    combined.update(dict(row))
    current = _required_snapshot_value(
        combined.get("current_position"),
        combined.get("amount"),
        combined.get("volume"),
        field_name="node_asset_position_current",
    )
    available = _required_snapshot_value(
        combined.get("available_position"),
        combined.get("closeable_amount"),
        combined.get("available_amount"),
        combined.get("available"),
        field_name="node_asset_position_available",
    )
    history = _required_snapshot_value(
        combined.get("history_position"),
        combined.get("yesterday_volume"),
        field_name="node_asset_position_history",
    )
    onroad = _required_snapshot_value(
        combined.get("onroad_position"),
        combined.get("on_road_position"),
        combined.get("on_road_volume"),
        combined.get("in_transit_position"),
        field_name="node_asset_position_onroad",
    )
    return {
        "security": _canonical_security_for_digest(combined),
        "identity_sha256": _row_fingerprint(combined, "position"),
        "current": _canonical_decimal_text(current, field_name="node_asset_position_current"),
        "available": _canonical_decimal_text(
            available,
            field_name="node_asset_position_available",
        ),
        "history": _canonical_decimal_text(
            history,
            field_name="node_asset_position_history",
        ),
        "onroad": _canonical_decimal_text(
            onroad,
            field_name="node_asset_position_onroad",
        ),
    }


def _canonical_node_positions(snapshot: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """验证并规范化一个节点的完整持仓集合。

    Args:
        snapshot: 含 positions 列表的节点快照。

    Returns:
        List[Dict[str, Any]]: 按证券和身份摘要稳定排序的持仓材料。

    Raises:
        HuaxinNodeAssetDigestError: positions 不是完整对象列表时抛出。
    """

    rows = snapshot.get("positions")
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise HuaxinNodeAssetDigestError("node_asset_positions_invalid")
    material = [_canonical_node_position_material(row) for row in rows]
    material.sort(key=lambda row: (row["security"], row["identity_sha256"]))
    identities = [(row["security"], row["identity_sha256"]) for row in material]
    if len(identities) != len(set(identities)):
        raise HuaxinNodeAssetDigestError("node_asset_position_duplicated")
    return material


def build_huaxin_positions_snapshot_id(
    trading_day: Any,
    positions: Sequence[Mapping[str, Any]],
) -> str:
    """为完整华鑫持仓集合生成与采样时间无关的快照 ID。

    Args:
        trading_day: 八位交易日。
        positions: 完整公共或原生持仓对象序列。

    Returns:
        str: SHA-256 十六进制摘要。

    Raises:
        HuaxinNodeAssetDigestError: 日期或持仓合同非法时抛出。
    """

    material = {
        "schema": HUAXIN_NODE_ASSET_DIGEST_VERSION,
        "trading_day": _canonical_trading_day(trading_day),
        "positions": _canonical_node_positions({"positions": list(positions)}),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_huaxin_node_asset_snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    """为华鑫单节点完整资金和持仓快照生成确定性摘要。

    Args:
        snapshot: 含交易日、节点证明、资金和完整持仓的快照。

    Returns:
        str: 不含采样时间和身份明文的 SHA-256 摘要。

    Raises:
        HuaxinNodeAssetDigestError: 必填快照字段不完整或非法时抛出。
    """

    account = snapshot.get("account")
    node = snapshot.get("node")
    if not isinstance(account, Mapping):
        raise HuaxinNodeAssetDigestError("node_asset_account_missing")
    if not isinstance(node, Mapping):
        raise HuaxinNodeAssetDigestError("node_asset_provenance_missing")
    node_id = _required_int(snapshot.get("node_id"), field_name="node_asset_node_id")
    node_proven_node_id = _required_int(
        node.get("node_id"),
        field_name="node_asset_provenance_node_id",
    )
    if node_id < 0 or node_proven_node_id != node_id:
        raise HuaxinNodeAssetDigestError("node_asset_node_mismatch")
    provenance = str(node.get("provenance") or "").strip()
    if not provenance:
        raise HuaxinNodeAssetDigestError("node_asset_provenance_missing")
    positions = _canonical_node_positions(snapshot)
    material = {
        "schema": HUAXIN_NODE_ASSET_DIGEST_VERSION,
        "trading_day": _canonical_trading_day(snapshot.get("trading_day")),
        "node_id": node_id,
        "node_provenance": provenance,
        "account": {
            "identity_sha256": _row_fingerprint(account, "fund"),
            "available_cash": _canonical_decimal_text(
                _required_snapshot_value(
                    account.get("available_cash"),
                    account.get("cash"),
                    field_name="node_asset_available_cash",
                ),
                field_name="node_asset_available_cash",
            ),
            "transferable_cash": _canonical_decimal_text(
                _required_snapshot_value(
                    account.get("transferable_cash"),
                    field_name="node_asset_transferable_cash",
                ),
                field_name="node_asset_transferable_cash",
            ),
            "frozen_cash": _canonical_decimal_text(
                _required_snapshot_value(
                    account.get("frozen_cash"),
                    account.get("locked_cash"),
                    field_name="node_asset_frozen_cash",
                ),
                field_name="node_asset_frozen_cash",
            ),
        },
        "positions_snapshot_id": build_huaxin_positions_snapshot_id(
            snapshot.get("trading_day"),
            [dict(row) for row in snapshot.get("positions") or []],
        ),
        "positions": positions,
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "HUAXIN_NODE16_READY_SCHEMA",
    "HUAXIN_NODE_ASSET_DIGEST_VERSION",
    "HuaxinNodeAssetDigestError",
    "build_huaxin_node_asset_snapshot_digest",
    "build_huaxin_positions_snapshot_id",
]
