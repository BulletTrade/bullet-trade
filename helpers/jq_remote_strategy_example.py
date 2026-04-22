"""
可直接拷贝到聚宽研究环境的示例策略。

准备：
- 将 `bullet_trade_jq_remote_helper.py` 放到聚宽研究根目录。
- 将本文件拷贝到同目录，在聚宽里打开后直接运行。
- 请先在下方填写你的服务器参数。

能力：
- 启动时自动调用 bt.configure，适配聚宽频繁重启。
- 查看账户与持仓。
- 市价单（自动补价转限价）/限价单，下单时可指定 wait_timeout>0 同步等待，否则异步。
- 撤单、订单状态查询。
"""

import datetime
import os

from jqdata import *  # 聚宽内置

import bullet_trade_jq_remote_helper as bt

# ===== 配置区域 =====
BT_REMOTE_HOST = '111.111.111.111'  #远程qmt服务器
BT_REMOTE_PORT = 58620              #远程qmt服务器端口
BT_REMOTE_TOKEN = 'my_remote_token_879237283'  #修改为你自己的服务器token秘钥
ACCOUNT_KEY = None  # 可选
SUB_ACCOUNT = None  # 可选


def _ensure_configured():
    if not BT_REMOTE_TOKEN:
        raise RuntimeError("请先在 BT_REMOTE_HOST/BT_REMOTE_PORT/BT_REMOTE_TOKEN/ACCOUNT_KEY/SUB_ACCOUNT 填写远程服务器配置")
    bt.configure(
        host=BT_REMOTE_HOST,
        port=BT_REMOTE_PORT,
        token=BT_REMOTE_TOKEN,
        account_key=ACCOUNT_KEY,
        sub_account_id=SUB_ACCOUNT,
    )
    # 让券商端可用数据补价
    bt.get_broker_client().bind_data_client(bt.get_data_client())


def process_initialize(context):
    """
    聚宽重启/代码刷新时调用，此处完成所有初始化与任务注册。
    """
    log.info(f"process_initialize 重建配置 {datetime.datetime.now()}")
    _ensure_configured()




def initialize(context):
    """
    占位：聚宽重启后由 process_initialize 完成实际初始化。
    """
    set_benchmark("000001.XSHE")
    set_option("use_real_price", True)

    run_daily(show_account_and_positions, time="09:35")
    run_daily(place_limit_buy_and_cancel, time="09:36")
    run_daily(place_market_buy_async, time="09:37")
    run_daily(check_open_orders, time="09:38")
    run_daily(trade_gold_roundtrip, time="09:39")


def show_account_and_positions(context):
    """
    打印账户与持仓。
    """
    try:
        acct = bt.get_account()
        positions = bt.get_positions()
        log.info(
            f"[账号] 现金={acct.available_cash:.2f} 总资产={acct.total_value:.2f} 持仓数量={len(positions)}"
        )
        for pos in positions:
            log.info(f"[持仓] {pos.security} 数量={pos.amount} 成本={pos.avg_cost:.4f} 市值={pos.market_value:.2f}")
    except Exception as exc:
        log.error(f"获取账户/持仓失败: {exc}")


def place_limit_buy_and_cancel(context):
    """
    取当前价打 99 折挂单买入 100 手，wait_timeout=10s 同步等待，然后尝试撤单。
    """
    symbol = "000001.XSHE"
    try:
        last = bt.get_data_client().get_last_price(symbol)
        if not last:
            log.warning(f"[限价单] 无法获取 {symbol} 价格，跳过")
            return
        limit_price = round(last * 0.99, 2)
        log.info(f"[限价单] {symbol} 99折买入尝试，限价={limit_price}")
        oid = bt.order(symbol, 100, price=limit_price, wait_timeout=10)
        log.info(f"[限价单] 下单返回 order_id={oid}，准备撤单")
        if oid:
            try:
                bt.cancel_order(oid)
                log.info(f"[撤单] 已提交撤单 order_id={oid}")
            except Exception as exc:
                log.error(f"[撤单] 撤单失败 order_id={oid}, err={exc}")
    except Exception as exc:
        log.error(f"[限价单] 下单流程异常: {exc}")


def place_market_buy_async(context):
    """
    市价单（自动补价转限价），不等待回报。
    """
    symbol = "000002.XSHE"
    try:
        log.info(f"[市价单] {symbol} 买入 100，异步模式")
        oid = bt.order(symbol, 100, price=None, wait_timeout=0)
        log.info(f"[市价单] 已提交，order_id={oid}")
    except Exception as exc:
        log.error(f"[市价单] 下单失败: {exc}")


def trade_gold_roundtrip(context):
    """
    同步买入黄金 100 份（市价自动补限价），待回报后再卖出同等数量。
    """
    symbol = "518880.XSHG"  # 黄金 ETF
    try:
        before_acct = bt.get_account()
        positions = bt.get_positions()
        pos_map = {p.security: p for p in positions}
        before_pos = pos_map.get(symbol)
        log.info(
            f"[黄金回合][前] 现金={before_acct.available_cash:.2f} "
            f"持仓={before_pos.amount if before_pos else 0} 可用={before_pos.available if before_pos else 0}"
        )
        log.info(f"[黄金回合] 市价买入 {symbol} 100 份，同步等待成交（保护价自动计算）")
        buy_oid = bt.order(symbol, 100, price=None, wait_timeout=10)
        log.info(f"[黄金回合] 买单 order_id={buy_oid}")
        if buy_oid:
            mid_acct = bt.get_account()
            positions = bt.get_positions()
            pos_map = {p.security: p for p in positions}
            mid_pos = pos_map.get(symbol)
            log.info(
                f"[黄金回合][买后] 现金={mid_acct.available_cash:.2f} "
                f"持仓={mid_pos.amount if mid_pos else 0} 可用={mid_pos.available if mid_pos else 0}"
            )
            log.info(f"[黄金回合] 市价卖出 {symbol} 100 份，同步等待成交（保护价自动计算）")
            sell_oid = bt.order(symbol, -100, price=None, wait_timeout=10)
            log.info(f"[黄金回合] 卖单 order_id={sell_oid}")
            after_acct = bt.get_account()
            positions = bt.get_positions()
            pos_map = {p.security: p for p in positions}
            after_pos = pos_map.get(symbol)
            log.info(
                f"[黄金回合][卖后] 现金={after_acct.available_cash:.2f} "
                f"持仓={after_pos.amount if after_pos else 0} 可用={after_pos.available if after_pos else 0}"
            )
    except Exception as exc:
        log.error(f"[黄金回合] 交易失败: {exc}")


def check_open_orders(context):
    """
    查询开放订单、状态，并演示可卖数量。
    """
    try:
        opens = bt.get_open_orders()
        log.info(f"[订单查询] 当前未完结订单数={len(opens)}")
        for it in opens:
            oid = it.get("order_id")
            status = bt.get_order_status(oid) if oid else {}
            log.info(f"[订单查询] order_id={oid}, status={status.get('status')}, raw={status}")
        # 展示可卖数量
            positions = bt.get_positions()
            for pos in positions:
                log.info(f"[持仓核对] {pos.security} 总量={pos.amount} 可用={pos.available} 冻结={pos.frozen}")
    except Exception as exc:
        log.error(f"[订单查询] 失败: {exc}")
