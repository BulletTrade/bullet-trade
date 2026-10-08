# 同花顺交易服务（受限模拟版）

同花顺通过独立的 `ths` 券商适配器接入 BulletTrade。
本包提供本机查询服务、持久请求日志和 Windows GUI 驱动；订单须包含明确限价。

## 结构与性能

```text
BulletTrade Server / THS Adapter
          ↓ 本机 HTTP（鉴权、短超时）
SQLite 请求日志、结果、最新快照、快照历史
          ↑                    ↓
   单一 Windows GUI actor：交易优先，查询合并
          ↓
   已登录的同花顺模拟炒股客户端
```

- API 查询只读持久快照，不等待 GUI，不因调用频繁而生成查询任务。
- actor 默认每 20 秒尝试安排刷新；同类任务排队或执行中只保留一个。
  GUI 较慢时跳过错过的周期，不累积补跑。20 秒不是全表刷新完成承诺。
- 买、卖、撤单优先于尚未执行的刷新。已开始的 GUI 步骤在安全边界让出，
  不强行打断验证码、确认框或可能已提交的操作。
- API 与 actor 分进程。响应超时不撤销已接收的委托，也不触发自动重发。
- 请求、原始合同号、调用方标识、状态历史落盘；进程重启不丢映射。
  `submit_unknown` 停止新增写入，先核实原请求结果。
- SQLite 是本地数据库；状态目录放本机磁盘，不使用网络盘或同步盘。

## 当前能力边界

| 能力 | 状态 |
| --- | --- |
| 资金、持仓、委托查询 → 快照 → API → THS Adapter | 已在一个 Win11 模拟客户端串联验证 |
| 成交、可撤查询 | 当前客户端的空表已通过同一路径；非空成交须有可验证日期与市场来源 |
| 列表完整性 | 同次核验页面、未过滤、加载状态、剪贴板归属及前后原生行范围；只证明当前查询页 |
| 限价买入 | 一个 Win11 模拟 profile 已受理并按原请求键恢复；独占来源时使用精确确认与唯一新增合同关联 |
| 限价卖出 | 一个 Win11 模拟 profile 已受理并按原请求查询结果、撤销原合同；实际成交及对应资金持仓变化尚未验收 |
| 自动撤单 | 一个 Win11 模拟 profile 已完成原合同撤单、终态查询、API 结果恢复及可撤空表收尾；仅支持可撤全表唯一且属于本服务的原合同 |
| 高频快照查询、历史分页、交易优先、幂等恢复 | 已有定向回归和并发测试 |
| 市价单、自动价格、行情订阅 | unsupported；调用方须传证券、数量、明确限价 |
| 真实券商账户、自动登录/重启、无人值守长期运行 | 尚未交付；当前原生配置只接受“模拟炒股”身份 |

当前表格缺少完整性证明时，Broker API 会明确报查询不可用，保留上一份
成功快照及其时间，不伪装成零持仓、零委托或“最新”结果。
资金走原生文字控件；列表走复制，出现验证码则识别并提交。识别不确定即报错。
只安全取消身份已核实的复制验证码，不自动关闭未知交易确认或登录弹窗。
复制后验证码尚未出现时，客户端主窗口的短暂隐藏可在固定三秒窗口内只读复看；
仍核对原窗口、网格、账户和页面，不重新发送复制。验证码识别后、确认前的隐藏仍停止。
复制验证码确认成功后，仅原主窗仍存在且进程/绑定一致的短暂隐藏可在确认后三秒内只读复核；
恢复后重查原网格、账户、页面、弹窗及新剪贴板证据，不再次复制或确认。
客户端“当日”页可能在非交易日保留旧记录。只有时分秒的委托保留
`order_time_raw`，不拼接本机日期；本服务已受理合同的日期可由持久原请求逐行证明。
非空成交缺少日期或市场时，只有整表每一笔都能匹配本服务同日已受理原合同，
且证券、方向、价格及累计数量一致，才使用原请求证明这些字段。
混入无归属记录时整表不可用，不删除未知行，也不返回虚假的空成交。

## 安装与配置

GUI actor 需要 Windows、Python 3.9 或更新版本。发布支持此功能的版本后安装：

```powershell
python -m pip install "bullet-trade[ths]"
```

从源码开发可使用 `python -m pip install -e ".[ths]"`。

新建本机 `profile.private.json`，填写实际独立资金卡号和客户端显示名称：

```json
{
  "account": "ths-paper",
  "client_exe": "C:/ExampleTradingClient/xiadan.exe",
  "expected_card": "替换为实际资金卡号",
  "account_mask": "模拟炒股-替换为实际显示名称",
  "ocr_template_dir": "C:/private/ths/digit_templates",
  "exclusive_order_source": false
}
```

账号和 OCR 模板不随开源包分发。模板来自当前客户端的已核实数字样本，
需包含 0～9；文件可命名 `real_0_1.png`，或放在 `0`～`9` 子目录。
运行环境保持已有交互登录会话；SSH/Windows 服务的非交互桌面不能直接操作界面。
连接与断开 RDP、锁屏、分辨率变化需分别验收，不能互相替代。

`exclusive_order_source` 默认关闭。仅当账户全部下单来源都经过本 actor、
期间没有人工或其它终端下单时才设为 `true`。这允许在精确确认后未弹成功回报时，
用完整前后委托表中的唯一新增合同及严格条款、时间窗口关联原请求。
多新增、旧合同丢失、条款不符或来源不独占都不确认受理，也不重复提交。

两个进程使用相同账户、状态目录。设置 `THS_SERVICE_TOKEN` 为私有随机 ASCII
字符串（至少 24 字符），以及 `THS_GUI_PROFILE` 为配置文件绝对路径。
不要把 token 放进公开命令、Git 或日志。

在已登录的交互桌面启动 actor：

```powershell
python -m bullet_trade.integrations.ths --actor --account ths-paper --state-dir C:/private/ths/state --driver-factory bullet_trade.integrations.ths.windows:make_driver --interval 20 --snapshot-max-age 180
```

独立启动本机 API：

```powershell
python -m bullet_trade.integrations.ths --account ths-paper --state-dir C:/private/ths/state --port 18765 --max-age 180
```

默认均为只读。模拟交易验收时，**两个进程**均需添加 `--enable-trading`。
API 接收权限和 actor 执行权限分开，避免只开接口就意外下单。
同一客户端只能有一个 GUI 操作者；本包使用状态目录锁和 Windows 会话互斥锁。
人工操作及旧探针也必须遵守该单执行者约定。
当前原生列表读取较慢，五类查询循环可能超过一分钟。actor 的
`--snapshot-max-age` 默认 120 秒，API 的 `--max-age` 默认 60 秒；以上示例
把两者都设为 180 秒，以较严格条件判定就绪。这些参数不改变采集时间；
调用方应检查 `age_seconds` 和 `stale`。需要更严格的新鲜度时可降低上限，
过期结果会明确报不可用。

BulletTrade Server 配置：

```text
THS_SERVICE_URL=http://127.0.0.1:18765
THS_SERVICE_ACCOUNT=ths-paper
THS_SERVICE_TOKEN=<与本机 API 相同的私有 token>
THS_REQUEST_TTL_SECONDS=300
QMT_SERVER_ACCOUNTS=paper=ths-paper:stock
QMT_SERVER_TOKEN=<通用 Server 的独立私有 token>
```

```powershell
bullet-trade server --server-type ths --disable-data --enable-broker --listen 127.0.0.1 --port 58630
```

沿用通用 Server 的认证和账户路由。一个 THS 服务对应一个实体账户。
`THS_WAIT_SECONDS` 默认 2 秒（最多 10 秒）；`THS_REQUEST_TTL_SECONDS`
默认 120 秒（最多 300 秒）。等待上限不是 GUI 执行速度承诺。
当前模拟 GUI 写入较慢，上例显式使用 300 秒请求期限。到期不会自动重发；
可能已经提交的请求仍须按原键核实结果。

## 本机 API

所有接口使用 `Authorization: Bearer <token>`。仅监听 loopback。
共享 token 仅用于受信任调用方，不能充当不同调用方之间的访问隔离。

| 接口 | 含义 |
| --- | --- |
| `GET /health` | actor 心跳、必需快照的新鲜度与错误、接单状态、未决请求；不是客户端登录或柜台连接认证 |
| `GET /snapshots/{kind}` | 最新成功快照、版本、采集时间、年龄、完整性、上次错误 |
| `GET /snapshots/{kind}/history?limit=50&before_version=123` | 本服务采集的历史版本，非券商全部历史交易 |
| `GET /requests/{request_id}` | 持久请求结果 |
| `GET /requests/{request_id}/events?after_id=0&limit=50` | 请求状态变化历史 |
| `GET /requests?account=ths-paper&idempotency_key=...&trade_day=YYYY-MM-DD` | 按原始键只读恢复；日期省略时仅接受跨日唯一记录 |
| `POST /requests` | 启用写入后接收请求；202 仅表示已保存，不代表券商接受 |

`kind` 为 `account/positions/orders/trades/cancelable`。
成功快照默认每类最多 1000 版、最多 30 天；历史只反映实际采集结果。
错误不覆盖上一次成功数据，过期数据必须结合 `stale`/采集时间解释。

请求包含 `account, trade_day, idempotency_key, kind, params, expires_at, origin`。
限价单参数为 `security, quantity, price`（价格为 Decimal 字符串）；撤单参数
仅含 `broker_contract_no`。正常使用 THS Adapter，由其生成稳定键并保存来源。
请求 UUID 不是券商合同号。HTTP 超时后查询原键，不能换键再发。
同键不同参数/来源/期限返回冲突；跨日同键歧义也停止写入。

`origin` 保存虚拟账户、策略、调用方订单标识。委托归属按实体账户、交易日、
原始合同字符串匹配；保留前导零。手动委托、未映射委托不猜测归属。
收到撤单请求不等于最终已撤；实际终态须由原合同查询证据确认。

## 未知提交恢复

`trading_ready` 只有在接单开启、actor 心跳新鲜、驱动声明就绪、全部必需业务快照
完整且无当前错误/过期、并且无未决提交时才为真。单个查询恢复不会清除其它查询
的错误；历史错误仍保留。actor 按 `--snapshot-max-age`（默认120秒）检查快照年龄，
API 按 `--max-age` 复核，以较严格条件为准。该字段是健康声明，不能替代
每笔订单的原生提交前校验。
驱动会精确核验已知主站认证提示的所属窗口、错误原文和控件结构，单次点击“确定”，
确认关窗后继续账户身份及新查询校验。失败或未知弹窗仍报错，不连续点击；
同一窗口不重复发出点击，新窗口也受短暂冷却限制。成功的新查询可恢复快照，
关闭窗口本身不证明连接或订单结果；此流程不会重新发送交易请求。

Adapter 的 `resolve_submission` 优先读取本服务持久结果，不依赖客户端备注，
不因为只查到一张“相似委托”就当作原请求已成功。仅 THS 显式启用这条路径，
既有 QMT/其他券商保持原行为。

通过 Server 恢复时，`request_payload` 必须保留首次写入的完整载荷，
包括 `account_key`、证券、方向、数量、价格及来源标识。仅重构部分参数
会触发幂等指纹冲突；恢复查询不会替调用方补全或重发原订单。

确需人工核实原生回报时：先停 actor，检查原请求、原合同及证据文件，生成
`recovery.py` 规定的完整清单（身份、参数、来源、预期状态、结论、证据 SHA256），再运行：

```powershell
python -m bullet_trade.integrations.ths.recovery --state-dir C:/private/ths/state --evidence-file C:/private/ths/recovery.json --operator-reviewed
```

该工具留存审计记录，不打开 GUI、不重新下单。允许 `submit_unknown` 核实为
`accepted/rejected`，或核实尚未提交的 `preparing` 为 `local_aborted`。
不得仅因超时或未查到委托就认定拒单。

只有撤单请求在首次选行点击前发生已核实的固定错误，才可另用
`resolution=not_submitted`、`evidence_source=operator_reviewed_pre_click_failure`
进行显式审核恢复，结果为 `local_aborted`，不会冒充券商拒单。
清单须绑定完整原请求、实际执行版本源码、对应异常记录及各文件 SHA256，
同时证明错误点位于首次选行、提交、确认点击之前；仅错误码或当前源码不够。
选行后失败、普通超时、进程退出或缺少执行版本证据均不得使用该分支。
原请求键保持终结，后续操作须创建新请求；恢复工具不会自行重放。

## 回归

```sh
python -m pytest tests/unit/integrations/ths tests/unit/test_ths_adapter_registration.py tests/server/test_process_memory_idempotency.py -q
```

离线测试验证持久性、优先级、并发查询、超时、跨日恢复、归属、错误停写及原适配器
兼容；现场委托、成交、断线和登录验收必须分别记录，不能由单测替代。
