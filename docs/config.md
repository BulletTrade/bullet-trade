# 配置总览

这页只保留最常用、最少量的配置。  
原则只有一个：**有默认值的先不要写，先把必须项配通。**

## 1. 本地 QMT 最小配置

适用场景：策略和 QMT 在同一台 Windows 机器上运行。

```env
DEFAULT_DATA_PROVIDER=qmt
DEFAULT_BROKER=qmt
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
```

运行：

```bash
bullet-trade live strategies/demo_strategy.py --broker qmt
```

补充：

- 股票账户默认就是 `stock`，所以 `QMT_ACCOUNT_TYPE` 不用写
- 只有期货账户才需要写 `QMT_ACCOUNT_TYPE=future`

## 2. 远程 server 最小配置

适用场景：Windows 机器只负责连 QMT，对外提供远程行情和交易服务。

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

运行：

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

补充：

- 当前版本不支持 `--data-path`
- `QMT_DATA_PATH` 必须写在 `.env`

## 3. 远程 qmt-remote 客户端最小配置

适用场景：策略跑在另一台机器上，通过 TCP 连远程 server。

```env
DEFAULT_DATA_PROVIDER=qmt-remote
DEFAULT_BROKER=qmt-remote
QMT_SERVER_HOST=10.0.0.8
QMT_SERVER_PORT=58620
QMT_SERVER_TOKEN=secret
```

运行：

```bash
bullet-trade live strategies/demo_strategy.py --broker qmt-remote
```

单账户时，不用写 `QMT_SERVER_ACCOUNT_KEY`。

## 4. 多账户时才需要的参数

### server 端

如果你在一个 server 上挂多个账户，才需要 `--accounts`。  
股票账户可以这样写：

```bash
--accounts main=123456
```

期货账户才需要显式写类型：

```bash
--accounts hedge=654321:future
```

### 客户端

多账户时，客户端才需要指定：

```env
QMT_SERVER_ACCOUNT_KEY=main
```

## 5. 这些参数一开始通常不用配

下面这些先不用急着写，等跑通以后再加：

- `LOG_DIR`
- `RUNTIME_DIR`
- `QMT_SESSION_ID`
- `QMT_AUTO_SUBSCRIBE`
- `MAX_ORDER_VALUE`
- `MIN_BUY_ORDER_VALUE`
- `MAX_DAILY_TRADES`
- `RISK_CHECK_ENABLED`
- `QMT_SERVER_TLS_CERT`

`MIN_BUY_ORDER_VALUE` 用于过滤实盘买入小单，默认 `0` 表示不限制。它只在风控开启后生效：本地 LiveEngine 需 `RISK_CHECK_ENABLED=true`，远程 QMT server 需 `QMT_SERVER_ORDER_RISK_ENABLED=true`。该参数只拦截买入订单，卖出、清仓和降仓不受影响。

## 6. 一个判断原则

如果你现在只是想先跑通：

- 本地 QMT：只配 `DEFAULT_DATA_PROVIDER`、`DEFAULT_BROKER`、`QMT_DATA_PATH`、`QMT_ACCOUNT_ID`
- 远程 server：只配 `QMT_DATA_PATH`、`QMT_ACCOUNT_ID`、`QMT_SERVER_TOKEN`
- 远程客户端：只配 `DEFAULT_DATA_PROVIDER`、`DEFAULT_BROKER`、`QMT_SERVER_HOST`、`QMT_SERVER_PORT`、`QMT_SERVER_TOKEN`

其他先都不要写。
