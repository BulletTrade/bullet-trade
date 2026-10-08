"""Attach server-side ownership to already normalized broker facts.

Never infer ownership from symbol/price/quantity or overwrite a broker field.
Unmapped manual orders stay unassigned. The caller must independently establish
the source account and trading day of the broker query.
"""
from copy import deepcopy


def attribute_order(store, *, account, trade_day, order):
    result = deepcopy(order)
    if "ths_attribution" in result:
        raise ValueError("reserved attribution field supplied by source")
    contract = result.get("order_id")
    if not isinstance(contract, str) or not contract:
        result["ths_attribution"] = {"status": "unassigned"}
        return result
    owner = store.get_by_contract(account, trade_day, contract)
    if owner is None:
        result["ths_attribution"] = {"status": "unassigned"}
    else:
        result["ths_attribution"] = {
            "status": "mapped", "source": "durable_request_journal",
            "account": owner.account, "trade_day": owner.trade_day,
            "request_id": owner.request_id, "idempotency_key": owner.idempotency_key,
            "origin": deepcopy(owner.origin), "evidence_ref": owner.evidence_ref,
        }
    return result
