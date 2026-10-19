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

## 独立账户身份查询

候选核心提供可选 `BrokerBase.get_account_identity()` 和远程只读
`broker.identity`，与原余额/持仓查询分开。旧适配器默认返回待核对，不要求实现新的
抽象方法，也不改变既有下单、撤单或账户同步行为。

远程请求仅接受父账户路由 `account_key`。调用必须经过原 Server 令牌握手，不能
携带客户端自填 `broker_id` / `account_id` 来生成认证结果，也不能拿远端虚拟子账号
认证实体父账户。接口不读取持仓或触发交易。

结果 `schema_version=1`，`state` 为 `verified` 或 `unverified`。完整结果包含
`identity.broker_id`、`identity.account_id` 和明确依据；待核对结果的 `identity` 为 null，
可以保留已查询到的 `observed_account_id`。结构校验不是来源真实性证明，接收方还须
与所选目标核对，不能只因字段齐全或请求成功就绑定另一账户。

当前 MiniQMT 从原 XtAsset 的 `account_id` 读取资金账号；大 QMT 从 helper 查询结果
的 `raw.m_strAccountID` 读取，不使用外层配置回显。缺原始字段、查询失败或账号不一致
均待核对。原生 QMT 账户查询没有可靠券商公司字段，因此适配器只提供真实账号观察，
不自行猜券商；原已认证Server可以按父账户使用自己的明确券商声明补齐依据。
`broker_type` / `m_nBrokerType` 表示账号类型，不能当作券商公司编号。
参见 [XtQuant 账号信息](https://dict.thinktrader.net/nativeApi/xttrader.html#账号信息xtaccountinfo)
和 [QMT 账户对象](https://dict.thinktrader.net/innerApi/data_structure.html#account-账户对象)。

账号保持字符串及前导零，IP、端口、别名和 QMT 类型不进入实体身份。旧 Server 没有
此接口时调用者应显示待核对，不退回原 `broker.account` 推断，也不通过试单认证。

### 配置服务端券商声明

声明是可选身份元数据，不是订单启停开关。不填写时保持旧交易行为，身份接口仍显示
券商待核对；查询不到实际资金账号时，即使已声明券商也不能生成完整实体。

单账户原 `.env` 可增加：

```env
QMT_SERVER_ACCOUNT_BROKERS=default=guojin
```

使用原账户别名逐项设置多个声明，也可由CLI明确覆盖环境配置：

```bash
bullet-trade server --accounts main=001234,other=009876 --account-brokers main=guojin,other=huatai
```

右侧是与客户端目标一致的规范券商标识，不是IP、QMT类型或账号尾号。未配置的账户
不继承另一账户的声明，未知别名、重复或非法标识拒绝。编程配置可在原
`AccountConfig`中填写可选`broker_id`。

只有原身份适配器独立查到的账号与目标账号一致，才返回
`broker_source=authenticated_server`。客户端请求不能携带自填券商/账号；子路由和
缺能力仍待核对。原查询已返回券商时保留查询依据，查询券商与声明不一致返回
`BROKER_MISMATCH`，不覆盖原查询值。不订阅行情、不读取持仓或发试单，声明本身也
不是厂商真实性、资产同步或工作台账户绑定验收。

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
