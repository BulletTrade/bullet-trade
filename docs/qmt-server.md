# QMT server（远程服务端）

这页只讲一件事：在 Windows 上把 `bullet-trade server` 启起来，给远程策略提供行情和下单。

本页默认讲 MiniQMT/xtquant 后端，也就是通过 `QMT_DATA_PATH` 访问 `userdata_mini`。这套配置不能直接套到大 QMT。

如果券商不再提供 MiniQMT，需要用大 QMT 策略环境提供数据和交易网关，请先看 [大 QMT 服务向导](big-qmt-server.md)。那种模式下，大 QMT helper 监听 `127.0.0.1:9000`，`bullet-trade server --server-type big_qmt` 再对外提供 `qmt-remote` 服务。

概念上要分清三层：

- `qmt`：MiniQMT/xtquant 本地直连数据源和交易通道。
- `big_qmt`：大 QMT helper server 后端，不是本地直连 provider。
- `qmt-remote`：客户端远程协议，底层可以接 MiniQMT server，也可以接大 QMT server。

## 先记住两点

- `bullet-trade server` **不支持** `--data-path`，MiniQMT 数据目录要写到 `.env` 的 `QMT_DATA_PATH`。
- 单账户时，`--accounts` 不是必填；如果已经在 `.env` 里写了 `QMT_ACCOUNT_ID`，可以直接启动。

## 最小 `.env`

把下面 3 个变量写进 `.env`：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

说明：

- `QMT_DATA_PATH`：MiniQMT 的 `userdata_mini` 目录
- `QMT_ACCOUNT_ID`：QMT 账号
- `QMT_SERVER_TOKEN`：远程访问 token

如果是股票账户，**不用写** `QMT_ACCOUNT_TYPE`。默认就是 `stock`。  
只有期货账户才需要额外写：

```env
QMT_ACCOUNT_TYPE=future
```

## 最小启动命令

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

这条命令里真正需要你关心的只有：

- `--listen`
- `--port`

其他账号、数据目录、token 都从 `.env` 里读。

## 客户端最小 `.env`

远程策略所在机器只需要这几个变量：

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

## `:stock` 到底要不要写

如果你看到这样的写法：

```bash
--accounts default=123456:stock
```

这里的 `stock` 是账户类型。它的作用只有一个：告诉服务端这是股票账户还是期货账户。

但当前默认值就是 `stock`，所以单账户股票场景完全可以写成：

```bash
--accounts default=123456
```

更进一步，如果你已经在 `.env` 里写了 `QMT_ACCOUNT_ID=123456`，连 `--accounts` 都可以不写。

只有下面两种情况，才建议显式写账户类型：

- 你在配多账户
- 你不是股票账户，而是期货账户

例如：

```bash
--accounts main=123456,hedge=654321:future
```

## 常见问题

### 为什么我写了 `--data-path` 报错

因为当前版本没有这个 CLI 参数。请把数据目录写进 `.env`：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
```

### 单账户要不要写 `QMT_SERVER_ACCOUNT_KEY`

不用。  
只有多账户时，客户端才需要指定：

```env
QMT_SERVER_ACCOUNT_KEY=main
```

### 启动后没有行情/下单失败

先检查这几项：

- QMT 客户端是否已登录
- `QMT_DATA_PATH` 是否指向正确的 MiniQMT `userdata_mini`
- Windows 防火墙是否放行了监听端口
- `QMT_SERVER_TOKEN` 是否和客户端一致
