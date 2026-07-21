"""
security-id/v1 权威实现与跨仓 golden corpus 的单元测试。

作者: BruceLee
文件职责: 验证 canonical 身份、JQ/QMT/THS 边界、稳定错误、序列化、哈希和幂等身份。
主要输入: 随包发布的 bullet_trade/contracts/security-id/v1/golden.json。
主要输出: 对所有有效、非法和序列化冲突样例的离线断言。
上下游关系: 上游是 OpenSpec security-identity-contract，下游是 AIStocks/strategies conformance 消费方。
关键约定: 测试不连接数据库、行情或券商；fixture 结果变化必须升级契约版本而非静默改值。
"""

import json
from pathlib import Path
from typing import Any, Dict

import pytest

import bullet_trade.core.security_id as security_id_module
from bullet_trade.core.security_id import (
    SECURITY_ID_SCHEME_VERSION,
    SecurityExchange,
    SecurityId,
    SecurityIdErrorCode,
    SecurityIdValidationError,
    format_security_id,
    parse_security_id,
)

pytestmark = pytest.mark.unit

CORPUS_PATH = (
    Path(security_id_module.__file__).resolve().parents[1]
    / "contracts"
    / "security-id"
    / "v1"
    / "golden.json"
)


def _load_corpus() -> Dict[str, Any]:
    """读取版本化 security-id golden corpus。

    Returns:
        Dict[str, Any]: JSON fixture 的完整结构。
    """

    with CORPUS_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


CORPUS = _load_corpus()
VALID_CASES = CORPUS["valid_cases"]
INVALID_CASES = CORPUS["invalid_cases"]
INVALID_SERIALIZATIONS = CORPUS["invalid_serializations"]


def _corpus_case_id(case: Dict[str, Any]) -> str:
    """为 pytest 参数化样例提供稳定可读名称。

    Args:
        case: golden corpus 中的单个样例。

    Returns:
        str: fixture 维护的样例 ID。
    """

    return str(case["id"])


def _assert_format_result(identity: SecurityId, target: str, expected: Dict[str, Any]) -> None:
    """断言单个出站 adapter 的成功值或稳定错误码。

    Args:
        identity: 已解析的领域 SecurityId。
        target: canonical、jq、qmt 或 ths。
        expected: fixture 中的预期结果结构。

    Returns:
        None: 断言通过时无返回值。
    """

    if expected["ok"]:
        assert format_security_id(identity, target) == expected["value"]
        return
    with pytest.raises(SecurityIdValidationError) as captured:
        format_security_id(identity, target)
    assert captured.value.code.value == expected["error"]


def test_corpus_metadata_is_versioned_and_complete() -> None:
    """golden corpus 必须声明固定 schema、身份版本和签名算法。"""

    assert CORPUS_PATH.is_file()
    assert CORPUS["corpus_schema_version"] == "security-id-conformance/v1"
    assert CORPUS["security_id_scheme_version"] == SECURITY_ID_SCHEME_VERSION
    assert CORPUS["semantic_signature"]["algorithm"] == "sha256"
    assert len(VALID_CASES) >= 10
    assert len(INVALID_CASES) >= 10


@pytest.mark.parametrize("case", VALID_CASES, ids=_corpus_case_id)
def test_valid_golden_cases_match_all_boundaries(case: Dict[str, Any]) -> None:
    """所有有效 corpus 输入都必须得到冻结的身份、格式、哈希和幂等值。"""

    identity = parse_security_id(
        case["input"],
        case["source"],
        exchange=case.get("exchange"),
    )
    expected = case["expected"]

    assert identity.to_versioned_dict() == expected["serialized"]
    assert identity.idempotency_identity == expected["idempotency_identity"]
    assert identity.semantic_signature == expected["semantic_signature"]
    assert str(identity) == expected["serialized"]["canonical"]
    assert SecurityId.from_versioned_dict(expected["serialized"]) == identity

    for target, format_expected in expected["formats"].items():
        _assert_format_result(identity, target, format_expected)


@pytest.mark.parametrize("case", INVALID_CASES, ids=_corpus_case_id)
def test_invalid_golden_cases_fail_with_stable_codes(case: Dict[str, Any]) -> None:
    """所有非法 corpus 输入都必须 fail closed 并返回跨仓稳定错误码。"""

    with pytest.raises(SecurityIdValidationError) as captured:
        parse_security_id(
            case["input"],
            case["source"],
            exchange=case.get("exchange"),
        )
    assert captured.value.code.value == case["expected_error"]


@pytest.mark.parametrize("case", INVALID_SERIALIZATIONS, ids=_corpus_case_id)
def test_invalid_serializations_fail_with_stable_codes(case: Dict[str, Any]) -> None:
    """损坏或自相矛盾的结构化身份不得被反序列化。"""

    with pytest.raises(SecurityIdValidationError) as captured:
        SecurityId.from_versioned_dict(case["payload"])
    assert captured.value.code.value == case["expected_error"]


def test_qmt_and_canonical_aliases_are_one_hashable_identity() -> None:
    """仿真事故中的 SH/XSHG 别名必须相等且集合去重。"""

    qmt_identity = parse_security_id("510180.SH", "qmt")
    canonical_identity = parse_security_id("510180.XSHG", "canonical")

    assert qmt_identity == canonical_identity
    assert len({qmt_identity, canonical_identity}) == 1
    assert qmt_identity.idempotency_identity == canonical_identity.idempotency_identity


def test_domain_constructor_rejects_vendor_short_exchange() -> None:
    """领域对象直接构造时不得把 QMT 短后缀称为 canonical。"""

    with pytest.raises(SecurityIdValidationError) as captured:
        SecurityId(symbol="510180", exchange="SH")
    assert captured.value.code == SecurityIdErrorCode.EXCHANGE_NOT_ALLOWED


def test_ths_plain_code_requires_trusted_exchange_metadata() -> None:
    """同花顺纯代码没有 exchange 元数据时不得按数字前缀猜市场。"""

    with pytest.raises(SecurityIdValidationError) as captured:
        parse_security_id("600519", "ths")
    assert captured.value.code == SecurityIdErrorCode.MISSING_EXCHANGE


def test_outbound_adapter_rejects_raw_string() -> None:
    """供应商出站 adapter 只接受 SecurityId，阻止领域层直接传字符串。"""

    with pytest.raises(SecurityIdValidationError) as captured:
        format_security_id("510180.XSHG", "qmt")
    assert captured.value.code == SecurityIdErrorCode.INVALID_TYPE


def test_security_id_orders_by_canonical_components() -> None:
    """结构化身份应可作为稳定排序、映射和集合键。"""

    identities = [
        SecurityId("159915", SecurityExchange.XSHE),
        SecurityId("510180", SecurityExchange.XSHG),
    ]
    assert [item.canonical for item in sorted(identities)] == [
        "159915.XSHE",
        "510180.XSHG",
    ]


def test_core_package_exports_authoritative_contract() -> None:
    """公共 core 包必须导出同一个 SecurityId 类型和版本常量。"""

    from bullet_trade.core import SecurityId as ExportedSecurityId
    from bullet_trade.core import SECURITY_ID_SCHEME_VERSION as exported_version

    assert ExportedSecurityId is SecurityId
    assert exported_version == SECURITY_ID_SCHEME_VERSION
