# 掘金独立数据层适配

`GmDataProvider` 已加入标准数据源工厂，支持 `gm`、`goldminer`、`掘金`。
**运行数据只有 GM 一个来源。聚宽仅是验收测试的对比标准，不是运行依赖。**
基础安装不加载 GM SDK；第一次读取才启动短时只读 worker，访问本机可用的 GM SDK/终端。
账户 ID 和策略 ID 不参与数据查询；本地 GM Broker 候选版见[交易文档](gm-trading.md)，远程 GM Server 尚未实现。

## 更正之前的双来源实现

2026-10-03 的实现误将聚宽基准因子和分钟金额输入 Provider，并报告 52/52。
该结果不满足独立掘金要求，**撤回其作为本方案验收依据的结论**。
2026-10-04 已删除生产包中的 RPC 校准客户端与 worker，移除模板的聚宽配置，所有字段只从 GM 产生。
旧 `GM_ALIGNMENT_MODE=joinquant_rpc` 会报错；应从私密配置移除这个误加的变量。
不能通过第二个来源、按证券硬编码系数或放宽阈值消除测试失败。
历史双来源报告保留为审计材料，不作为当前独立实现的通过证据。

## 使用方式

复制 `env.gm.example` 为私密 `.env.gm`，只配置自己的 `GM_TOKEN`、`GM_SERV_ADDR`。
SDK 已装在本机另一解释器时，可指定 `GM_PYTHON_EXECUTABLE`；它不是 SSH 设置。
不需要 JQ 账号、RPC 地址、认证密钥或聚宽 env 文件。

```python
from bullet_trade.data.api import set_data_provider, get_price
from bullet_trade.utils.env_loader import load_env

load_env(".env.gm")
set_data_provider("gm")
prices = get_price(
    "510300.XSHG", start_date="2026-09-28", end_date="2026-09-30",
    fields=["open", "high", "low", "close", "volume", "money"], fq="pre",
)
```

## 数据变换

- 价格、成交量、成交金额取 GM 未复权行情。
- 股票/基金因子取 GM 免费历史证券状态 `adj_factor`；前复权除以固定参考日因子，后复权使用 GM 累计因子。
- OHLC 乘因子并按报价精度舍入；成交量除因子并按股取整；日线金额保持 GM 原值，分钟金额按整数元截断后聚合。
- 指数使用单位因子，不查询缺失的股票复权因子。
- 多分钟线从 GM 1m 行连续分组，包含请求精确起点，保留末尾不足一组。
- 整日停牌按官方历史状态补行；`fill_paused=False` 保留全空值行。
- 历史盘中缺行仅在本源全天分钟量额与日线守恒时按零成交补齐，价格沿用此前真实收盘。请求截止必须已到该日收盘，且不能是当天；无法证明的缺口报错。`skip_paused` 只跳过整日停牌，不删除盘中零成交分钟。

`attrs` 记录 `data_source=gm`、`alignment_mode=native`、全部 `field_sources=gm`。
回测 `use_real_price=True` 时，宽表和多证券长表均传递回测当天参考日。
绕过当前版本只调整价格的通用动态复权缓存，由 GM Provider 同时调整量价。

## 已实现接口

| API | 范围 |
| --- | --- |
| `get_price` | 单/多证券，日线及 1/5/15/30/60 分钟线，范围/count，None/pre/post，字段、宽表/长表 |
| `get_trade_days` | 交易日历、范围及向前 count |
| `get_security_info` / `get_all_securities` | 股票、ETF、LOF、货币 ETF 分类及上市/退市过滤；名称为当前名称 |
| `get_index_stocks` | 指定历史交易日成份，假日取此前交易日 |
| `get_split_dividend` | GM 免费现金/送转事件及单位转换；配股认购款显式拒绝 |
| `get_bars` | include_now、固定复权参考日、DF/records、多证券 |
| `get_ticks` / `get_current_tick` | 历史/当前 tick、五档、重复量价过滤后再 count |
| `get_live_current` | 最新报价和新鲜度拒绝逻辑；假期未验收盘中新鲜报价 |

历史请求分片低于 SDK 上限，重复、乱序、证券错配和无效数值拒绝。
基本面、行业、概念、基金净值、周/月/多日、订阅和远程 Server 尚未实现；GM Broker 已有本地候选版。

## 验收与限制

测试工具 `scripts/probe_gm_adapter.py` 分别读取独立 Provider 输出和聚宽 RPC 输出，
只在两份结果均返回后比较。Provider 没有基准客户端参数，也不读取测试的 RPC 环境。
SSH 桥仅用于在 Windows 内存执行正式 SDK worker，不是已部署的远程 Server。

```bash
# 以下聚宽参数仅属于开发验收脚本，实际策略不用这些参数。
python scripts/probe_gm_adapter.py \
  --rpc-env-file /path/to/test-benchmark.env \
  --rpc-module /path/to/test-rpc_proxy.py \
  --gm-ssh-host YOUR_TEST_HOST \
  --gm-python '%USERPROFILE%\.conda\envs\bullettrade\python.exe' \
  --output-dir artifacts/gm-validation/independent
```

2026-10-04 已按用户确认允许小误差的要求增加 `gm_practical_v1` 实用验收：

| 字段 | 接受标准 |
| --- | --- |
| 价格（含前收、涨跌停、均价） | max(一个报价刻度, 基准绝对值 × 0.1%) |
| 成交量 | max(1 股, 基准绝对值 × 0.1%) |
| 成交金额 | max(1 元, 基准绝对值 × 0.0001%) |
| 因子 | max(1e-8, 基准绝对值 × 0.1%) |
| 停牌状态 | 完全一致，停牌时量额必须为零 |

后复权以两份结果的首个共同交易日为参考，各自除以本数据源该日累计因子，成交量同步
乘该因子。GM 因子来自已采集 GM 官方历史状态；基准因子是已采集聚宽输出，仅参与测试。
**不从价格比例拟合倍数，不把聚宽因子输入 Provider。**因子来源缺失、不唯一、无效时失败。

重复、乱序、错日期、缺行、缺列、非有限数、负数、量纲错误仍不得接受。元信息、日历、
成份和 tick 检查沿用严格契约；数据读取错误不会被小误差规则放行。证券错配与未来数据
继续由适配器及数据 API 现有回归检查拦截。此容差用于目前的常规量价验收，不等同于精细
价差策略的信号/成交验证。

对已采集的独立实机结果重新执行验收：**实用 52/52 通过；相关离线回归 612 项通过**。
原严格诊断仍是 **32 通过、20 项数值差异**，原 `report.json` 不改写。
实用报告和测试日志见 `artifacts/gm-validation/20261004-practical/`；
原始实机证据见 `artifacts/gm-validation/20261004-independent-final/`。
本次是已保存实测数据的离线重验，没有重新查询远端，也没有修改生产数据算法。

```bash
python scripts/review_gm_acceptance.py \
  --source-dir artifacts/gm-validation/20261004-independent-final \
  --benchmark-factors artifacts/gm-validation/rpc-factor-chains.json \
  --output artifacts/gm-validation/20261004-practical/acceptance-report.json

pytest tests/e2e/data/test_gm_adapter_acceptance.py -m requires_network --no-cov \
  --gm-adapter-report artifacts/gm-validation/20261004-practical/acceptance-report.json \
  --gm-acceptance-profile practical
```

`strict` profile 保留原数值诊断，20 项差异仍会标记失败；不是重新采集或部署失败。
实用报告的 52 项是完整既有矩阵，不是把其中小差异用例改为跳过。独立接入及实用验收通过，
不等同于两家数值逐字一致，也不覆盖所有证券、全历史及实际策略回放。
聚宽也说明各厂商自行计算复权，分钟行情有各自 tick 采集和合成差异，见
[聚宽官方数据常见疑问](https://test.demo.joinquant.com/community/post/detailMobile?postId=21247)。
这一说明用于定位来源，不能免除当前失败或自动放宽标准。

分红窗口行情核对与 Finance 分红事件表核对是两件事；后者仍因基准服务端
`Interruptingcow` 线程错误受阻，尚未验收。没有修改或重启该服务。


## 2026-10-08 覆盖补齐

完整矩阵、实机重验结果及未覆盖边界见 [GM 数据接口回归报告](gm-data-tests.md)。
本轮新增 202 项测试；44 个相关测试文件共 933 项通过，5 项显式联网用例未运行。
本轮 GM 采集并重验的共享停牌契约 19/19 通过；旧 52 项实用验收报告重验通过，
后者并非本轮重新联网对齐。所有运行行情仍只来自 GM。
