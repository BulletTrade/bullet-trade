# 导入函数库
from jqdata import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
      
    #run_daily(every_bar, 'every_bar')
    run_daily(every_bar, 'every_minute')

def process_initialize(context):
  log.info('系统每次开始运行process_initialize')

#def every_minute(context):
    # 输出运行时间
#    log.info(f'============函数运行时间(every_minute)：{context.current_dt.time()}============')


## 开盘前运行函数
def every_bar(context):
    # 输出运行时间
    log.info(f'============函数运行时间(every_bar)：{context.current_dt.time()}============')

    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('美好的一天~')

    # 要操作的股票：平安银行（g.为全局变量）
    g.security = '000001.XSHE'
    log.info(f"cash:{context.portfolio.available_cash}")
    log.info(f"positions:{context.portfolio.positions}")
    log.info(f"total_value:{context.portfolio.total_value}")


def after_code_changed(context):
    log.info(f'============代码变更后运行after_code_changed：{context.current_dt.time()}============')
    g.info ='after code chage 1'


