# 同花顺模拟交易服务

同花顺通过独立的 `ths` 券商适配器实验性接入 BulletTrade。当前只支持 **Windows 交互桌面中已登录的“模拟炒股”客户端 profile**。GUI actor 操作客户端，本机 API 返回持久快照和请求结果，BulletTrade Server 通过该 API 接入券商侧交易。行情与历史数据源需独立配置；此服务不提供行情订阅，也没有真实券商账户或无人值守运行的认证承诺。

## 安装与进程

发布支持此功能的版本后，在 Windows、Python 3.9 或更新版本的环境安装：

```powershell
python -m pip install "bullet-trade[ths]"
```

源码开发可使用 `python -m pip install -e ".[ths]"`。GUI actor 需要已登录、未锁屏的交互桌面；同一客户端只能有一个 GUI 操作者。新建私有 profile，填入实际客户端路径、模拟资金卡身份和 OCR 数字模板目录：

```json
{
  "account": "ths-paper",
  "client_exe": "C:/ths/client/xiadan.exe",
  "expected_card": "替换为实际模拟资金卡号",
  "account_mask": "模拟炒股-替换为客户端显示名称",
  "ocr_template_dir": "C:/ths/digit_templates",
  "exclusive_order_source": false
}
```

将 `THS_GUI_PROFILE` 设为该文件的绝对路径。`THS_SERVICE_TOKEN` 使用至少 24 字符的私有随机 ASCII 字符串；不要将 profile、模板或 token 提交到仓库。OCR 模板须来自已核实的当前客户端数字样本并覆盖 0～9。更多字段和操作证据要求见包内 `bullet_trade/integrations/ths/README.md`。

actor 与 API 使用**相同的账户和本机状态目录**，状态目录应位于本机磁盘。以下为只读启动示意；路径、账户和端口按自己的隔离环境替换：

```powershell
python -m bullet_trade.integrations.ths --actor --account ths-paper --state-dir C:/ths/state --driver-factory bullet_trade.integrations.ths.windows:make_driver --interval 20 --snapshot-max-age 180
python -m bullet_trade.integrations.ths --account ths-paper --state-dir C:/ths/state --port 18765 --max-age 180
```

两个进程默认只读。仅在已授权的模拟交易验收中，才同时给 actor 和 API 添加 `--enable-trading`；API 接单与 actor 执行分别受控。GUI 操作以分钟计也可能仍未完成，`--interval 20` 只表示尝试安排刷新，不保证 20 秒内完成全表。API 读持久快照，不等待 GUI，也不因频繁查询而增加 GUI 任务。

## 接入 BulletTrade Server

在运行 Server 的受信任环境设置 `THS_SERVICE_URL`、`THS_SERVICE_ACCOUNT`、`THS_SERVICE_TOKEN`、`QMT_SERVER_ACCOUNTS` 和独立的 `QMT_SERVER_TOKEN`。示例账户路由为 `QMT_SERVER_ACCOUNTS=paper=ths-paper:stock`，服务 URL 为 `http://127.0.0.1:18765`。模拟 GUI 写入较慢时可设置 `THS_REQUEST_TTL_SECONDS=300`；期限届满不代表未提交，也不会自动重发。

```powershell
bullet-trade server --server-type ths --disable-data --enable-broker --listen 127.0.0.1 --port 58630
```

Server 沿用通用认证与账户路由。`--disable-data` 表明这条接入只负责券商交易；策略所需的行情和历史数据应选择并验证独立的数据源。一个 THS 服务对应一个实体账户。

## 交易与查询边界

- 买入和卖出须提供证券、数量、**明确限价**；市价单、自动取价和行情订阅不支持。
- 自动撤单仅支持在**当前可撤全表中唯一匹配、且属于本服务原请求**的合同。接收撤单请求不等于最终已撤；应查询原合同终态。
- 客户端查询页中的记录消失、可撤表为空或冻结资金释放，都不能单独证明原委托已撤销或被拒绝。缺少明确终态时须保留原请求和合同映射，不以新请求替代核对。
- 资金、持仓、委托等 API 查询返回最近一次成功快照及采集时间、年龄和 `stale` 状态。旧快照不会冒充最新结果；过期或完整性无法证明时，查询可明确报不可用。
- 特定客户端的复制验证码输入可能失败。该查询失败时保留上一份成功快照及其采集时间，聚合健康接口的 `trading_ready` 会标为 false；它不是服务总熔断，不能据此推断每笔委托的最终结果。`POST /requests` 的 `202` 仅表示请求已排队，实际执行仍须通过原生提交前校验；调用方应核查所依赖快照的新鲜度。
- `POST /requests` 的 `202` 只表示请求已保存。超时或 `submit_unknown` 时按原账户、交易日和幂等键核查持久结果，**不得换键重放**或仅凭未查到委托判断拒单。人工恢复须按包内 `bullet_trade/integrations/ths/README.md` 留存证据。

已有一个 Windows 模拟 profile 的查询、限价受理和受限撤单局部验证，不能据此认定服务可稳定、开箱即用；实际成交及对应资金持仓变化、真实券商账户、自动登录和长期无人值守运行仍需分别验收。离线测试通过不能替代这些验收。
