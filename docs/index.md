# BulletTrade 文档

BulletTrade 是一套兼容聚宽 API 的开源量化研究与交易框架。新用户不需要先读完整配置表：先选一种运行方式，只填写当前步骤真正需要的参数。

## 第一次来，从这里开始

1. [安装 Python 与 BulletTrade](python-setup.md)
2. [新手选型](beginner-guide.md)
3. [大 QMT：两种接入方式](big-qmt-server.md)

当前主要推荐大 QMT。MiniQMT 继续作为兼容方案保留；华鑫 TORA 面向自备 SDK 的专业用户，不作为新手主线。

## 大 QMT 的两条主路线

### 聚宽运行策略

策略继续在聚宽运行，通过互联网连接自己的 BulletTrade 服务，再由大 QMT 完成账户查询和下单。

[查看聚宽路线的完整步骤](big-qmt-server.md#joinquant-route)

### 本地运行策略

策略由 `bullet-trade live` 在本地运行，通过 `qmt-remote` 连接同一台机器上的大 QMT。

[查看本地路线的完整步骤](big-qmt-server.md#local-route)

## 按任务查文档

- [快速上手](quickstart.md)：先跑通最小示例。
- [研究环境](research.md)：启动 JupyterLab。
- [回测引擎](backtest.md)：运行和检查回测。
- [实盘引擎](live.md)：了解本地实盘运行方式。
- [配置总览](config.md)：只有需要扩展默认行为时再查。
- [API 文档](api.md)：查策略函数和对象。
- [数据源指南](data/DATA_PROVIDER_GUIDE.md)：选择 JQData、QMT、Tushare 等数据源。

## 安装

```bash
pip install bullet-trade
```

安装后检查：

```bash
bullet-trade --version
```

开发者可参考 [邀请贡献](contributing.md)，问题和建议请提交到 [GitHub Issues](https://github.com/BulletTrade/bullet-trade/issues)。

## 风险提示

量化与实盘存在市场、网络和软件风险。策略与软件不保证收益；请先在仿真环境验证，再以小额资金逐步上线。服务启动、请求成功或返回订单号都不等于已经成交，最终应核对券商委托、成交和持仓。
