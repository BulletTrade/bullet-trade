# 大 QMT 除权事实增强版：仿真加载说明

构建号：`20260908_dividend_facts_v1`。事件协议：`big-qmt-dividend-events/v1`。

本版只增强 helper 的事件事实、复权参数解释及版本可观测性。复权计算仍在 QMT 外，未接入现有行情主路径；交易、撤单、竞价、成交量换算和 HTTP 生命周期不变。生产不在本次范围。

## 本版改变什么

- `/data/split_dividend` 保留旧六字段，并增加每股/份现金、红股、转增、配股数、配股价、股改标识、QMT 原生 `dr`、原始字段名和值及源时间戳。
- 七项顺序依据[大 QMT 官方接口](https://dict.thinktrader.net/innerApi/data_function.html)和[讯投字段字典](https://dict.thinktrader.net/nativeApi/xtdata.html)：`interest / stockBonus / stockGift / allotNum / allotPrice / gugai / dr`。其中 `gift=stockBonus`，`transfer=stockGift`，不按英文字面交换。
- 旧 `security_type/per_base/bonus_pre_tax` 保留原兼容约定；新 `cash_per_share` 直接保留 QMT 每原股/份现金，不再按旧 `per_base` 重复放大。
- 缺项、非有限数、非法日期及窗口内重复事件返回 `SPLIT_DIVIDEND_INVALID_DATA`。API 异常返回 `SPLIT_DIVIDEND_FAILED`，缺少接口返回 `QMT_API_NOT_READY`；不再把 `None` 或短数组当成零事件。
- 空字典可以成功返回空列表，但 `history_completeness_verified` 始终为 false，不能证明上市起事件已完整下载。`event_fields_complete` 只说明非空返回行具备本版完整字段。
- 负送转保持原值（总股本比例必须为正），不据此宣称自算已支持基金拆并；股改只识别明确0/1，未知标识明确报错。`qmt_dr` 仅用于原生算法对照，不作为聚宽累计因子使用。
- `/data/history` 中显式 `fq` 优先于 `dividend_type`。`fq: null` 或 `"none"` 明确传入 QMT `none`；`pre/qfq` 对应 `front_ratio`，`post/hfq` 对应 `back_ratio`。两参数均省略才保留旧 `follow` 默认。空字符串和未知模式在下载/读取前报 `BAD_REQUEST`。
- `/health` 返回新的 `gateway_build_id` 和 `dividend_event_schema`。配套取数探针保留这两个字段，不记录账户或认证信息。

## 用户在仿真 QMT 中操作

1. 确认目标是仿真客户端，在没有仿真策略执行的时段进行。备份当前 helper 文件和本地配置，记录旧构建号。
2. 仿真核心仓从已推送的 `bullet-trade/dev` 做 Git 快进，使用本次交付记录给出的精确提交。加载该版本的 `helpers/big_qmt_gateway_strategy_sample.py`，不要使用旧的11111诊断文件。
3. 文件声明和实际字节均为 **GBK**。通过文件导入/加载时保持 GBK，不另存为 UTF-8。逐项保留仿真当前的账号、端口、认证、交易配置及生命周期参数；共享样例中的 `change_me...` 是占位值，不可覆盖现有配置。不要整块粘贴旧配置区：其中的 `GATEWAY_BUILD_ID` 必须保留新版值，新增 `DIVIDEND_EVENT_SCHEMA` 也必须保留，不能用旧内容覆盖或删除。

   本地配置仅保留在 QMT 实际运行副本中，不把认证信息写回共享 Git 样例；不要在仿真仓库手改程序逻辑。
4. 让旧 helper 实例先退出，再加载并运行新版本；不要同时启动第二份 HTTP 服务。现有默认 `STOP_HTTP_ON_QMT_STOP=False`，因此仅点击“停止策略”未必释放旧服务。若旧实例仍在，由用户关闭并重新打开**这一个仿真 QMT**后加载新 helper；不要重启所有 QMT，不要修改该参数来绕过既有生命周期。
5. 加载会让仿真 helper 接口暂时不可用。重新运行成功后告知代理，由代理检查接口和版本；不要仅凭编译成功、端口监听或旧界面的日志判断升级成功。

此说明不是 QMT 客户端软件升级，也不要求新建账户、服务或交易开关。外部 server、node-agent、V2 和信号进程本阶段无需为本 helper 构建单独重启。

## 加载后的检查由代理完成

- 通过既有仿真连接核对 `/health`：`gateway_build_id=20260908_dividend_facts_v1`、`dividend_event_schema=big-qmt-dividend-events/v1`，并检查上下文就绪。
- 重新查询000001/510500事件，检查原始七字段、旧字段单位、窗口、来源及错误语义；将原始结果保存到本地，不触发交易/写信号/通知。
- 核对行情 `none/pre/post` 显式参数回归，确认既有历史接口结构没有变化。
- 记录实际加载文件、源提交、本地配置差异和验收结果。当前交付仅代表代码准备就绪，用户尚未加载的新版本不得记录为仿真已升级。

ETF九格残差、后复权固定原点、基础分钟规范化、自然周月和实际信号回放仍属后续待验收范围。
