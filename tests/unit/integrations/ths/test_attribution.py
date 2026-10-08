import time

from bullet_trade.integrations.ths.attribution import attribute_order
from bullet_trade.integrations.ths.request_store import RequestStore


def test_contract_mapping_is_parent_day_scoped_and_manual_order_unassigned(tmp_path):
    store = RequestStore(tmp_path / "requests.db")
    request = store.enqueue("paper", "2026-09-30", "v1:test", "limit_buy",
        {"security": "518880", "quantity": 100, "price": "8.500"}, time.time() + 60,
        origin={"virtual_account_id": "v1", "client_order_id": "original-client-id"})
    store.mark_preparing(request.request_id)
    store.mark_submit_unknown(request.request_id)
    store.mark_accepted(request.request_id, "00001234", "fixture:receipt")
    order = {"order_id": "00001234", "status": "filled"}
    mapped = attribute_order(store, account="paper", trade_day="2026-09-30", order=order)
    assert mapped["ths_attribution"]["origin"]["virtual_account_id"] == "v1"
    assert mapped["order_id"] == "00001234" and "ths_attribution" not in order
    for account, day, contract in (("other", "2026-09-30", "00001234"),
                                  ("paper", "2026-10-01", "00001234"),
                                  ("paper", "2026-09-30", "manual-order")):
        result = attribute_order(store, account=account, trade_day=day, order={"order_id": contract})
        assert result["ths_attribution"]["status"] == "unassigned"
