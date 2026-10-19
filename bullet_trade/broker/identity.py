"""作者：BruceLee。

职责：规范只读券商实体身份结果，区分真实查询和配置回显，不处理资产或交易。
输入：适配器明确查询的资金账号、券商标识及依据，或已认证接口的结果。
输出：脱密、版本化的已核验/待核对结果；不返回完整账户快照。
上下游：Broker和Server适配器生成，工作台及远程客户端读取；不导入厂商SDK。
约定：账号保留前导零；IP、端口、账号类型和配置名称不是券商身份。
"""

from __future__ import annotations

import re

__all__ = ["identity_pending", "identity_from_query", "validate_identity_result", "normalize_broker_id", "identity_with_server_declaration"]

REASONS = frozenset(
    {"UNSUPPORTED", "ACCOUNT_UNAVAILABLE", "BROKER_UNAVAILABLE", "QUERY_FAILED", "ACCOUNT_MISMATCH", "BROKER_MISMATCH"}
)
BROKER_SOURCES = frozenset({"broker_query", "authenticated_server"})


def _identifier(value, *, broker=False):
    """输入原查询标识和券商要求，返回规范字符串；无I/O，拒绝数值及不透明对象转换。"""
    if not isinstance(value, str):
        raise ValueError("身份字段必须是字符串")
    result = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", result):
        raise ValueError("身份字段为空或格式无效")
    return result.lower() if broker else result


def identity_pending(reason="UNSUPPORTED", *, observed_account_id=None):
    """输入固定原因及可选查询账号，返回待核对结果；不把部分账号观察当完整实体核验。"""
    if not isinstance(reason, str) or reason not in REASONS:
        raise ValueError("身份核验原因无效")
    observed = None if observed_account_id is None else _identifier(observed_account_id)
    return {
        "schema_version": 1,
        "state": "unverified",
        "reason_code": reason,
        "identity": None,
        "observed_account_id": observed,
        "evidence": None,
    }


def normalize_broker_id(value):
    """输入显式券商标识，返回规范小写字符串；无查询或状态修改，非法类型/格式抛ValueError。"""
    return _identifier(value, broker=True)


def identity_from_query(broker_id, account_id, *, broker_source="broker_query"):
    """输入独立查询账号/券商及券商依据，返回规范实体；调用者不得将配置账号传作查询值。

    本函数只核对数据结构，不证明调用方诚实或网络已认证。服务端声明必须来自
    已认证Server自己的配置，不能取客户端选择；工作台另与所选目标身份比较。
    无I/O、不下单、不改变账户/进程。缺字段返回待核对，坏类型/依据抛ValueError。
    """
    if not isinstance(broker_source, str) or broker_source not in BROKER_SOURCES:
        raise ValueError("券商身份依据无效")
    if account_id is None or account_id == "":
        return identity_pending("ACCOUNT_UNAVAILABLE")
    account = _identifier(account_id)
    if broker_id is None or broker_id == "":
        return identity_pending("BROKER_UNAVAILABLE", observed_account_id=account)
    return {
        "schema_version": 1,
        "state": "verified",
        "reason_code": None,
        "identity": {"broker_id": _identifier(broker_id, broker=True), "account_id": account},
        "observed_account_id": account,
        "evidence": {"account_source": "broker_query", "broker_source": broker_source},
    }


def validate_identity_result(value):
    """输入只读接口结果，返回规范副本；拒绝旧账户回显、隐式真值和身份依据缺失，无写入。"""
    if (
        not isinstance(value, dict)
        or set(value)
        != {"schema_version", "state", "reason_code", "identity", "observed_account_id", "evidence"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
    ):
        raise ValueError("身份接口结果版本或结构无效")
    if value["state"] == "unverified":
        if value["identity"] is not None or value["evidence"] is not None:
            raise ValueError("待核对身份不能附带已认证实体")
        result = identity_pending(
            value["reason_code"], observed_account_id=value["observed_account_id"]
        )
    elif value["state"] == "verified":
        entity, evidence = value["identity"], value["evidence"]
        if (
            value["reason_code"] is not None
            or not isinstance(entity, dict)
            or set(entity) != {"broker_id", "account_id"}
            or not isinstance(evidence, dict)
            or set(evidence) != {"account_source", "broker_source"}
            or evidence["account_source"] != "broker_query"
        ):
            raise ValueError("身份接口未提供实际资金账号查询依据")
        result = identity_from_query(
            entity["broker_id"], entity["account_id"], broker_source=evidence["broker_source"]
        )
        if (
            result["state"] != "verified"
            or result["observed_account_id"] != value["observed_account_id"]
        ):
            raise ValueError("身份字段不一致")
    else:
        raise ValueError("身份核验状态无效")
    if result != value:
        raise ValueError("身份接口字段未规范化")
    return result


def identity_with_server_declaration(value, broker_id, expected_account_id):
    """输入原查询结果、服务端券商及目标账号，返回核对结果；仅供原已认证Server调用。

    只为缺券商但实际账号已查到的结果补依据；配置账号不充当查询值。
    原查询或券商声明冲突返回待核对，查询失败/缺能力不能借声明升级。
    本函数不认证网络、不查询资产、不下单，不改变原结果；坏结构或标识抛ValueError。
    """
    result = validate_identity_result(value)
    observed = result["observed_account_id"]
    if observed is not None and observed != _identifier(expected_account_id):
        return identity_pending("ACCOUNT_MISMATCH", observed_account_id=observed)
    if broker_id is None:
        return result
    broker = normalize_broker_id(broker_id)
    if result["state"] == "verified":
        if result["identity"]["broker_id"] != broker:
            return identity_pending("BROKER_MISMATCH", observed_account_id=observed)
        return result
    if result["reason_code"] != "BROKER_UNAVAILABLE" or observed is None:
        return result
    return identity_from_query(broker, observed, broker_source="authenticated_server")
