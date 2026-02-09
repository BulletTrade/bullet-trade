# 快速上手：三步跑通（聚宽兼容）

目标：用原有聚宽策略，最少改动跑通回测与实盘（本地或远程）。如需细节配置，参考 [配置总览](config.md)。

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BulletTrade 架构概览                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                           策略层                                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │ from jqdata │  │ initialize  │  │ run_daily   │  │ order_*     │  │   │
│  │  │ import *    │  │ (context)   │  │ handle_data │  │ 交易函数     │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  │                       ↑ 聚宽兼容 API，无需改代码                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                     │                                       │
│                                     ▼                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                          引擎层                                       │   │
│  │  ┌─────────────────┐          ┌─────────────────┐                    │   │
│  │  │   回测引擎       │          │   实盘引擎       │                     │   │
│  │  │  BacktestEngine │          │   LiveEngine    │                    │   │
│  │  │  - 历史数据回放   │          │  - 实时行情驱动  │                     │   │
│  │  │  - 模拟撮合      │          │  - 真实下单      │                     │   │
│  │  │  - 分红送股处理   │          │  - 状态持久化    │                     │   │
│  │  └─────────────────┘          └─────────────────┘                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                         │                     │                             │
│            ┌────────────┴─────────┐    ┌──────┴───────┐                     │
│            ▼                      ▼    ▼              ▼                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │
│  │    数据层        │    │    券商层        │    │    工具层        │          │
│  │  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │          │
│  │  │ JQData    │  │    │  │ QMT本地    │  │    │  │ 参数优化   │  │          │
│  │  │ MiniQMT   │  │    │  │ QMT远程    │  │    │  │ 报告生成   │  │          │
│  │  │ TuShare   │  │    │  │ Simulator │  │    │  │ 研究环境   │  │          │
│  │  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │          │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 工作流程

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  1. 安装     │  →   │  2. 配置    │  →   │  3. 运行     │  →   │  4. 分析     │
│  pip install│      │  .env 文件  │       │  CLI 命令   │      │  查看报告     │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  bullet-trade         数据源账号           backtest/live       HTML/CSV 报告
  一行安装完成          券商配置              一键启动            风险指标分析
```

## 1. 安装与准备环境

```bash
python -m venv .venv && source .venv/bin/activate
pip install bullet-trade
```

开发模式：
```bash
pip install -e "bullet-trade[dev]"
```

> 提示：使用 `pip install bullet-trade` 时，实盘 CLI 读取的是已安装包；本地改源码后若想立即生效，请改用 `pip install -e "bullet-trade[dev]"` 并重启进程/Kernel。

复制模板：
```bash
cp env.backtest.example .env    # 回测
cp env.live.example .env        # 实盘/远程
```

## 2. 兼容聚宽的最小策略

```python
from jqdata import *

def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    g.stocks = ['000001.XSHE', '600000.XSHG']
    run_daily(trade, time='10:00')

def trade(context):
    order(g.stocks[0], 100)
    order(g.stocks[1], 200)
```

> 聚宽 API 直接可用，无需改接口；仅通过 `.env` 切换数据源/券商。

## 3. 直接运行

### 回测
```bash
bullet-trade backtest strategies/demo_strategy.py \
    --start 2024-01-01 --end 2024-06-01 \
    --output backtest_results/demo
```

### 参数优化
```bash
bullet-trade optimize strategies/demo_strategy.py \
    --params params.json \
    --start 2020-01-01 --end 2023-12-31 \
    --output optimization.csv
```

### 本地实盘（QMT/模拟）
```bash
bullet-trade live strategies/demo_strategy.py --broker qmt
```

### 远程实盘（qmt-remote）
```bash
bullet-trade live strategies/demo_strategy.py --broker qmt-remote
```

## 常用命令速查

| 场景 | 命令 |
|------|------|
| 回测 | `bullet-trade backtest strategy.py --start 2024-01-01 --end 2024-06-01` |
| 参数优化 | `bullet-trade optimize strategy.py --params params.json --start 2020-01-01 --end 2023-12-31` |
| 本地实盘 | `bullet-trade live strategy.py --broker qmt` |
| 远程实盘 | `bullet-trade live strategy.py --broker qmt-remote` |
| 启动服务 | `bullet-trade server --listen 0.0.0.0 --port 58620 --token secret` |
| 研究环境 | `bullet-trade lab` |
| 生成报告 | `bullet-trade report --input backtest_results --format html` |

## 我应该看哪篇文档？

```
┌─────────────────────────────────────────────────────────────────┐
│                        你想做什么？                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│   历史回测     │   │   实盘交易     │   │   研究分析     │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        ▼                   ▼                   ▼
 [回测引擎](backtest.md)    │            [研究环境](research.md)
 [参数优化](optimize.md)    │
                            │
            ┌───────────────┼───────────────┐
            │               │               │
            ▼               ▼               ▼
     ┌───────────┐   ┌───────────┐   ┌───────────┐
     │  本地QMT   │   │  远程QMT   │   │  模拟券商  │
     └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
           │               │               │
           ▼               ▼               ▼
    [实盘引擎](live.md)  [QMT服务](qmt-server.md)  [实盘引擎](live.md)
```

### 文档索引

| 需求 | 文档 | 简介 |
|------|------|------|
| 环境变量配置 | [配置总览](config.md) | 数据源、券商、风控参数一览 |
| 回测细节/FAQ | [回测引擎](backtest.md) | 流程、参数、输出说明 |
| 参数寻优 | [参数优化](optimize.md) | 多进程并行优化 |
| 本地/远程实盘 | [实盘引擎](live.md) | 生命周期、状态持久化 |
| 远程 QMT 部署 | [QMT server](qmt-server.md) | 服务端配置详解 |
| 聚宽模拟盘接入 | [trade-support](trade-support.md) | 远程下单支持 |
| 数据源配置 | [数据源指南](data/DATA_PROVIDER_GUIDE.md) | JQData/MiniQMT/TuShare |
| Tick 行情 | [Tick 指南](tick.md) | 订阅、字段、常见问题 |
| API 参考 | [API 文档](api.md) | 策略可用 API 一览 |
