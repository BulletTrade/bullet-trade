"""已授权仿真账户的银华日利 100 股往返测试；配置仅从 stdin 传入。"""

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bullet_trade.integrations.gm.broker import GmBroker


def safe(value):
    if isinstance(value, dict):
        return {
            k: safe(v)
            for k, v in value.items()
            if k not in ("account_id", "account_name", "strategy_id", "token")
        }
    if isinstance(value, list):
        return [safe(x) for x in value]
    return value


async def run(broker, report, save, run_id):
    code = "511880.XSHG"
    report["before"] = broker.get_account_info()
    if broker.get_positions() or broker.get_orders(from_broker=True):
        raise RuntimeError("本验收要求专用空仿真账户且当日无旧委托")
    for side in ("buy", "sell"):
        quote = broker.get_quote(code)
        price = (
            round(quote["quotes"][0]["ask_p"] + 0.002, 3)
            if side == "buy"
            else round(quote["quotes"][0]["bid_p"] - 0.002, 3)
        )
        if price <= 0:
            raise RuntimeError("盘口无有效价格")
        report[side] = dict(quote=quote, limit_price=price, requested_volume=100)
        save()
        oid = await getattr(broker, side)(
            code, 100, price, extra={"idempotency_key": run_id + ":" + side}
        )
        report[side]["order_id"] = oid
        save()
        for _ in range(60):
            status = await broker.get_order_status(oid)
            report[side]["order"] = status
            save()
            if status.get("submission_state") == "submit_unknown":
                raise RuntimeError("提交结果未知，停止后续写入并保留对账记录")
            if status["status"] == "filled":
                if status["filled"] != 100:
                    raise RuntimeError("成交数量不符合授权")
                break
            if status["status"] in ("rejected", "canceled", "partly_canceled"):
                raise RuntimeError("委托未完全成交")
            await asyncio.sleep(0.5)
        else:
            report[side]["cancel_confirmed"] = await broker.cancel_order(oid)
            save()
            raise RuntimeError("委托未及时成交，已尝试定向撤销原单，未重发")
        report[side]["trades"] = broker.get_trades(order_id=oid)
        report[side]["account"] = broker.get_account_info()
        save()
        if side == "buy":
            for _ in range(20):
                positions = broker.get_positions()
                if sum(x["enable_amount"] for x in positions if x["security"] == code) == 100:
                    break
                await asyncio.sleep(0.5)
            else:
                raise RuntimeError("已买入但尚无 100 股可卖持仓，停止卖出")
    for _ in range(20):
        report["after"] = broker.get_account_info()
        if not any(x["amount"] for x in report["after"]["positions"]):
            break
        await asyncio.sleep(0.5)
    trades = broker.get_trades()
    report["trades"] = trades
    assert sum(x["amount"] for x in trades) == 200, "成交回报总量必须为 200"
    assert len({x["trade_id"] for x in trades}) == len(trades)
    assert not any(x["amount"] for x in report["after"]["positions"]), "最终持仓未回到零"
    assert not broker.get_open_orders(), "仍有未结委托"
    buy_amount = sum(
        x["gross_amount"] for x in trades if x["order_id"] == report["buy"]["order_id"]
    )
    sell_amount = sum(
        x["gross_amount"] for x in trades if x["order_id"] == report["sell"]["order_id"]
    )
    commissions = sum(x["commission"] for x in trades)
    expected = report["before"]["cash"] - buy_amount + sell_amount - commissions
    report["cash_residual"] = report["after"]["cash"] - expected
    assert abs(report["cash_residual"]) <= 0.01, "资金与实际成交/手续费不守恒"
    report["ok"] = True
    save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", required=True)
    parser.add_argument("--account-fingerprint", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    config = json.load(sys.stdin)
    fingerprint = hashlib.sha256(config["account_id"].encode()).hexdigest()[:12]
    if fingerprint != args.account_fingerprint:
        raise RuntimeError("账户身份不匹配")
    config["enable_trading"] = True
    b = GmBroker(config["account_id"], config=config)
    report = dict(ok=False, account_fingerprint=fingerprint, symbol="511880.XSHG", volume=100)

    def save():
        Path(args.output).write_text(
            json.dumps(safe(report), default=str, ensure_ascii=False, indent=2), encoding="utf8"
        )

    try:
        b.connect()
        asyncio.run(run(b, report, save, args.run_id))
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        save()
        print("RESULT=" + json.dumps(safe(report), default=str), flush=True)
        raise
    finally:
        b.disconnect()
    print("RESULT=" + json.dumps(safe(report), default=str), flush=True)


if __name__ == "__main__":
    main()
