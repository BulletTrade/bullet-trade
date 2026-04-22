# BulletTrade 

<p>
  <img src="docs/assets/logo.png" alt="BulletTrade Logo" width="100">
</p>

[![PyPI version](https://badge.fury.io/py/bullet-trade.svg)](https://badge.fury.io/py/bullet-trade)
[![Python version](https://img.shields.io/pypi/pyversions/bullet-trade.svg)](https://pypi.org/project/bullet-trade/)
[![License](https://img.shields.io/github/license/BulletTrade/bullet-trade.svg)](https://github.com/BulletTrade/bullet-trade/blob/main/LICENSE)

**BulletTrade** 是一个专业的量化交易系统 Python 包，提供完整的回测和实盘交易解决方案。

简体中文 | [完整文档](docs/index.md)

## ✨ 核心特性

- **🔄 聚宽兼容**：`from jqdata import *`
- **📊 多数据源**：JQData、MiniQMT、TuShare、本地缓存与远程 QMT server 均可切换。
- **⚡ 回测 & 报告**：分钟/日线回测、真实价格撮合、HTML/PDF 报告一键生成。
- **💼 实盘接入**：本地 QMT、远程 QMT server、模拟券商按需选择。
- **🧩 可扩展**：数据/券商接口基于抽象基类，便于自定义实现。


## 📖 文档

- [文档首页](docs/index.md):  站点 <https://bullettrade.cn/docs/>
- [环境准备：安装 Python](docs/python-setup.md)：两个方案共用的前置步骤，先装 Python，再创建虚拟环境。
- [新手入门总览](docs/beginner-guide.md)：先看 BulletTrade 目前支持的两种方案、结构图和选型方法，再进入对应文档。
- [方案 A：独立运行](docs/beginner-route-a.md)：策略在 BulletTrade 独立运行，连接本地 QMT。
- [方案 B：聚宽侧模拟盘运行](docs/beginner-route-b.md)：策略在聚宽侧模拟盘运行，BulletTrade 负责接收信号并在本地 QMT 执行。
- [快速上手](docs/quickstart.md)：三步跑通回测/实盘，聚宽策略无改直接复用。
- [配置总览](docs/config.md)：回测/本地实盘/远程实盘/聚宽接入的环境变量一览。
- [回测引擎](docs/backtest.md)：真实价格成交、分红送股处理、聚宽代码示例与 CLI 回测。
- [实盘引擎](docs/live.md)：本地 QMT 独立实盘与远程实盘流程。
- [交易支撑](docs/trade-support.md)：聚宽模拟盘接入、远程 QMT 服务与 helper 用法。
- [QMT 服务配置](docs/qmt-server.md)：bullet-trade server 的完整说明。
- [数据源指南](docs/data/DATA_PROVIDER_GUIDE.md)：聚宽、MiniQMT、Tushare 以及自定义 Provider 配置。
- [API 文档](docs/api.md)：策略可用 API、类模型与工具函数。
- [邀请贡献](docs/contributing.md): 贡献与联系方式。 

## 🔗 链接

- GitHub 仓库：https://github.com/BulletTrade/bullet-trade
- 官方站点：https://bullettrade.cn/


## 📄 许可证

[MIT License](LICENSE)

## 联系与支持

如需交流或反馈，低佣开通QMT等，可扫码添加微信，并在 Issue/PR 中提出建议：

<img src="docs/assets/wechat-contact.png" alt="微信二维码" width="180">

---

**⚠️ 风险提示：** 量化交易存在高风险，因策略、配置或软件缺陷/网络异常等导致的任何损失由使用者自行承担，请先在仿真/小仓位充分验证。
