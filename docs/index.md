# BulletTrade 帮助文档

BulletTrade 是一套兼容聚宽 API 的量化研究与交易框架，支持多数据源、多券商接入，覆盖回测、仿真与本地/远程实盘。本页是文档主入口，与 bullettrade.cn 首页的“文档/帮助”按钮保持一致。

<p>
  <img src="assets/logo.png" alt="BulletTrade Logo" width="100">
</p>

## index
- [快速上手](quickstart.md)：三步跑通回测/实盘，聚宽策略无改直接复用。
- [研究环境（JupyterLab）](research.md)：`bullet-trade lab` 一键启动 Notebook，默认根目录、设置文件与示例说明。
- [配置总览](config.md)：回测/本地实盘/远程实盘/聚宽接入的环境变量一览。
- [回测引擎](backtest.md)：真实价格成交、分红送股处理、聚宽代码示例与 CLI 回测。
- [实盘引擎](live.md)：本地 QMT 独立实盘与远程实盘流程。
- [交易支撑](trade-support.md)：聚宽模拟盘接入、远程 QMT 服务与 helper 用法。
- [QMT 服务配置](qmt-server.md)：bullet-trade server 的完整说明。
- [Tick 订阅指南](tick.md)：本地 xtdata 与远程 qmt-remote 的订阅、字段与常见问题。
- [数据源指南](data/DATA_PROVIDER_GUIDE.md)：聚宽、MiniQMT、Tushare 以及自定义 Provider 配置。
- [API 文档](api.md)：策略可用 API、类模型与工具函数。

**链接**：
- GitHub 仓库 https://github.com/BulletTrade/bullet-trade 
- 官方站点 https://bullettrade.cn/

## 风险与声明
- 量化及实盘有市场与系统风险，任何策略/软件均不保证收益,软件不可避免有BUG,请先小额或模拟验证，自行承担交易风险。
- TuShare 数据源受测试账号权限限制，覆盖不完全，欢迎补充测试与提交 PR 完善。
- 目前示例策略以量价数据为主，若需要财务/基本面等扩展，建议先在聚宽模拟环境调用，再通过 qmt server 完成下单。


## 一键安装与环境准备

- 推荐使用 Python 3.10+ 并创建虚拟环境：
  ```bash
  python -m venv .venv
  # macos/linux
  source .venv/bin/activate
  # windows
  .venv\Scripts\activate.bat
  ```
- 一键安装：
  ```bash
  pip install bullet-trade
  ```
- 开发/贡献模式：
  ```bash
  pip install -e "bullet-trade[dev]"
  cp bullet-trade/env.example bullet-trade/.env
  ```
- 安装后可用 `python -m bullet_trade.cli --help` 或 `bullet-trade --version` 检查。

## BulletTrade 有哪些优势
- 兼容聚宽策略：`from jqdata import *` / `from bullet_trade.compat.api import *` 即可平滑迁移。
- 数据自由切换：JQData、MiniQMT、TuShare、本地缓存、远程 QMT server 均可用。
- 券商多入口：本地 QMT、远程 QMT server 与模拟券商可按场景切换。
- CLI 简单双击：回测、报告生成、实盘/服务启动都用同一套命令。

## 常用 CLI 速览
- 回测：  
  `bullet-trade backtest strategies/demo_strategy.py --start 2024-01-01 --end 2024-03-01 --frequency minute --benchmark 000300.XSHG`
- 实盘（本地/远程 QMT，未配置时可先用模拟券商）：  
  `bullet-trade live strategies/demo_strategy.py --broker qmt`  
  `bullet-trade live strategies/demo_strategy.py --broker qmt-remote  # 需要 .env 配好 QMT_SERVER_*`
- 远程服务（MiniQMT+QMT）：  
  `bullet-trade server --listen 0.0.0.0 --port 58620 --token secret --enable-data --enable-broker`
- 报告：  
  `bullet-trade report --input backtest_results --format html`

## 联系与支持

如需交流或反馈，低佣开通QMT等，可扫码添加微信，并在 Issue/PR 中提出建议：

<img src="assets/wechat-contact.png" alt="微信二维码" width="180">
