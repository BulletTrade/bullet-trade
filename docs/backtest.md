# 回测引擎

如何在本地快速跑通与聚宽对齐的回测，落盘报告并校验指标。

## 工作流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  bullet-trade backtest strategy.py --start 2024-01-01 --end 2024-06-30      │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. 初始化阶段                                                               │
│     ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │
│     │ 加载策略文件  │ → │ 重置全局状态  │ → │ 调用initialize│                │
│     │ strategy.py  │    │ g, context   │    │ 注册定时任务  │                │
│     └──────────────┘    └──────────────┘    └──────────────┘                │
├─────────────────────────────────────────────────────────────────────────────┤
│  2. 回测循环（逐交易日）                                                      │
│                                                                              │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │  每个交易日                                                       │    │
│     │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐              │    │
│     │  │盘前回调     │ → │执行定时任务  │ → │订单撮合     │              │    │
│     │  │before_      │   │run_daily等  │   │真实价格成交  │              │    │
│     │  │trading_start│   │handle_data  │   │分红送股处理  │              │    │
│     │  └─────────────┘   └─────────────┘   └─────────────┘              │    │
│     │         ↓                                   ↓                     │    │
│     │  ┌─────────────┐                    ┌─────────────┐               │    │
│     │  │盘后回调     │ ←───────────────── │记录每日数据  │               │    │
│     │  │after_       │                    │净值/持仓/交易│               │    │
│     │  │trading_end  │                    └─────────────┘               │    │
│     │  └─────────────┘                                                  │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                    ↓ 循环至回测结束                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  3. 结果生成                                                                 │
│     ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │
│     │ 计算风险指标  │ → │ 生成报告文件  │ → │ 输出到目录    │                │
│     │ 夏普/回撤/胜率│    │ HTML/CSV/PNG │    │ backtest_    │                │
│     │              │    │              │    │ results/     │                │
│     └──────────────┘    └──────────────┘    └──────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 最短路径（3 步）

1) 安装与模板：
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e "bullet-trade[dev]"
cp env.backtest.example .env
```

2) 只填本页用到的变量（其余见 [配置总览](config.md)）：
```bash
# 数据源类型 (jqdata, tushare, qmt)
DEFAULT_DATA_PROVIDER=jqdata         # 必填，行情源
JQDATA_USERNAME=your_username       # 选填，按数据源需要
JQDATA_PASSWORD=your_password
```

3) 运行回测：
```bash
bullet-trade backtest strategies/demo_strategy.py \
  --start 2024-01-01 --end 2024-06-30 \
  --benchmark 000300.XSHG \
  --cash 100000 \
  --output backtest_results/demo
```

> 聚宽策略可直接复用：`from jqdata import *`、`order_target_value` 等 API 已兼容，无需改代码。

## 命令行参数

```bash
bullet-trade backtest [-h] strategy_file --start START --end END
                      [--cash CASH] [--frequency {day,minute}]
                      [--benchmark BENCHMARK] [--output OUTPUT]
                      [--log LOG] [--images] [--no-csv] [--no-html] [--no-logs]
                      [--auto-report] [--report-format {html,pdf}]
                      [--report-template TEMPLATE] [--report-metrics METRICS]
                      [--report-title TITLE]
```

### 必填参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `strategy_file` | 策略文件路径 | `strategies/demo.py` |
| `--start` | 回测开始日期 | `2024-01-01` |
| `--end` | 回测结束日期 | `2024-06-30` |

### 可选参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--cash` | `100000` | 初始资金 |
| `--frequency` | `day` | 回测频率：`day`（日线）或 `minute`（分钟线） |
| `--benchmark` | 无 | 基准标的，如 `000300.XSHG` |
| `--output` | `./backtest_results` | 输出目录 |
| `--log` | 自动 | 日志文件路径，默认写入 `<output>/backtest.log` |

### 输出控制

| 参数 | 默认 | 说明 |
|------|------|------|
| `--images` | 关闭 | 生成 PNG 图表（净值曲线、持仓分布） |
| `--no-csv` | 开启 | 禁用 CSV 导出 |
| `--no-html` | 开启 | 禁用 HTML 报告 |
| `--no-logs` | 开启 | 禁用日志落盘 |

### 报告选项

| 参数 | 默认 | 说明 |
|------|------|------|
| `--auto-report` | 关闭 | 回测完成后自动生成标准化报告 |
| `--report-format` | `html` | 报告格式：`html` 或 `pdf` |
| `--report-template` | 无 | 自定义报告模板路径 |
| `--report-metrics` | 无 | 报告中展示的指标（逗号分隔） |
| `--report-title` | 无 | 报告标题（默认使用输出目录名） |

## 推荐设置：真实价格 + 分红送股

在策略初始化中开启真实价格撮合，更贴近实盘：

```python
set_option('use_real_price', True)
```

选择真实价格成交后，系统会自动处理分红、配股等现金/股份变动。

![分红配股](assets/分红配股.png)

## 兼容聚宽的策略示例

```python
from jqdata import *

def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    g.target = ['000001.XSHE', '600000.XSHG']
    run_daily(market_open, time='10:00')

def market_open(context):
    for stock in g.target:
        df = get_price(stock, count=5, fields=['close'])
        if df['close'][-1] > df['close'].mean():
            order_target_value(stock, 10000)
```

## 回测输出说明

运行结束后，输出目录包含以下文件：

```
backtest_results/
├── backtest.log          # 回测日志
├── report.html           # HTML 交互式报告
├── metrics.json          # 风险指标 JSON
├── daily_records.csv     # 每日净值记录
├── daily_positions.csv   # 每日持仓快照
├── trades.csv            # 交易明细
├── annual_returns.csv    # 年度收益
├── monthly_returns.csv   # 月度收益
├── risk_metrics.csv      # 风险指标表格
├── open_counts.csv       # 开仓次数统计
├── instrument_pnl.csv    # 分标的盈亏
└── dividend_split_events.csv  # 分红送股事件
```

### 核心输出文件说明

| 文件 | 内容 |
|------|------|
| `daily_records.csv` | 每日净值、现金、持仓市值、累计收益率 |
| `trades.csv` | 每笔交易的时间、标的、方向、数量、价格、费用 |
| `metrics.json` | 年化收益、最大回撤、夏普比率、胜率等指标 |
| `report.html` | 可视化报告，包含净值曲线、回撤图、交易统计 |

### 风险指标说明

| 指标 | 说明 |
|------|------|
| 策略收益 | 回测期间总收益率 |
| 策略年化收益 | 年化收益率 |
| 策略波动率 | 日收益率标准差的年化值 |
| 最大回撤 | 回测期间最大净值回撤幅度 |
| 夏普比率 | 风险调整收益（年化收益 / 波动率） |
| 索提诺比率 | 仅考虑下行风险的夏普比率 |
| Calmar 比率 | 年化收益 / 最大回撤 |
| 日胜率 | 盈利天数占比 |
| 交易胜率 | 盈利交易笔数占比 |
| 盈亏比 | 平均盈利 / 平均亏损 |

![backtest_result](assets/backtest_result.png)

## 单独生成报告

如需对已有回测结果重新生成报告：

```bash
bullet-trade report --input backtest_results/demo --format html
```

报告文件（HTML/PNG）可直接复用到站点或 MR 截图。

## 常见问题

### 中文字体
首次生成图片会自动配置中文字体，确保系统存在任意中文字体即可。

### 数据认证失败
检查 `.env` 中的账号/Token 或环境变量覆盖，参考 [配置总览](config.md)。

### 分钟线回测
确认数据源支持分钟线，且策略设置 `use_real_price=True`。

```bash
bullet-trade backtest strategy.py --start 2024-01-01 --end 2024-01-31 --frequency minute
```

### 日志为空
若未指定 `--log` 且又使用了 `--no-logs`，不会写入文件。

### 回测速度慢
- 减少回测时间范围先验证逻辑
- 分钟级回测比日线慢很多，建议先用日线调试
- 检查策略中是否有不必要的数据请求
