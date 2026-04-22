# 快速上手

这页保留作为旧入口。  
如果你是第一次使用 BulletTrade，推荐先看：

- [新手入门总览：先选方案，再看对应文档](beginner-guide.md)
- [环境准备：先安装 Python，再创建虚拟环境](python-setup.md)

那一页已经把下面这些问题合并讲清楚了：

- 我们支持哪两种方案
- 两种方案的结构分别是什么
- 怎么决策选 A 或 B
- 进入 A 方案或 B 方案的详细文档

## 如果你只想看最短路径

最短顺序仍然是：

1. 先安装
2. 先跑一个最小回测
3. 再打通本地 QMT 或远程 `qmt-remote`

## 1. 安装

```bash
python -m venv .venv
```

Windows：

```bash
.venv\Scripts\activate
```

安装：

```bash
pip install bullet-trade
```

## 2. 回测

```bash
bullet-trade backtest strategies/demo_strategy.py --start 2024-01-01 --end 2024-06-01
```

## 3. 本地 QMT

如果你还不知道 `.env` 是什么，先看：

- [什么是 `.env` 文件，怎么创建](python-setup.md#env-file)

下面这些代码块不是命令，而是要写进 `.env` 文件里的内容。

`.env` 最少：

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

## 4. 远程 server

Windows 上的 `.env` 最少：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

启动：

```bash
bullet-trade --env-file .env server --listen 0.0.0.0 --port 58620 --enable-data --enable-broker
```

## 5. 远程 qmt-remote 客户端

客户端 `.env` 最少：

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

## 6. 这几个参数先不用写

如果你只是想先跑通，下面这些先不要写：

- `LOG_DIR`
- `RUNTIME_DIR`
- `QMT_ACCOUNT_TYPE`
- `QMT_SESSION_ID`
- 风控相关参数

## 7. 常见问题

### `QMT_ACCOUNT_TYPE=stock` 要不要写

不用。  
默认就是 `stock`。  
只有期货账户才需要写 `QMT_ACCOUNT_TYPE=future`。

### `--data-path` 为什么报错

因为当前版本没有这个参数。  
请把数据目录写到 `.env`：

```env
QMT_DATA_PATH=C:\国金QMT交易端\userdata_mini
```

更多说明：

- [环境准备：先安装 Python，再创建虚拟环境](python-setup.md)
- [新手入门总览：先选方案，再看对应文档](beginner-guide.md)
- [方案 A：策略在 BulletTrade 本地直接运行](beginner-route-a.md)
- [方案 B：策略继续在聚宽侧运行，BulletTrade 负责接收信号并在本地 QMT 执行](beginner-route-b.md)
- [配置总览](config.md)
- [实盘引擎](live.md)
- [QMT server](qmt-server.md)
