# RQData 数据源 Beta

`RQDataProvider` 位于 `bullet_trade/data/providers/rqdata.py`，通过 `DEFAULT_DATA_PROVIDER=rqdata` 或 `set_data_provider("rqdata")` 启用。

## 安装与配置

RQData 是可选依赖，默认安装 BulletTrade 不会安装 `rqdatac`：

```bash
pip install "bullet-trade[rqdata]"
```

`.env` 示例：

```env
DEFAULT_DATA_PROVIDER=rqdata

# 二选一：license 优先
RQDATA_LICENSE=your_license

# 或使用账号密码
RQDATA_USERNAME=your_username
RQDATA_PASSWORD=your_password
```

## 当前状态

该 provider 当前作为 Beta 接入：

- 已完成 fake `rqdatac` 单元测试，覆盖日线、分钟线、`count`、多证券 shape、`factor`、`paused`、`avg`、涨跌停和 `get_live_current()`。
- 尚未完成真实 RQData 账号 smoke 和与 JQData 的派息窗口对账，因为当前没有可用 API key。
- 合入后不会影响默认 `jqdata`、`miniqmt`、`tushare` 和 `remote_qmt`。

## 行情口径

- `get_price()` 接收聚宽格式代码，例如 `000001.XSHE`、`600519.XSHG`。
- `money` 映射到 rqdatac 的 `total_turnover`，返回口径为元。
- `high_limit` / `low_limit` 映射到 `limit_up` / `limit_down`。
- 分钟线如果不带涨跌停字段，会单独拉日线涨跌停并按日期合并。
- `panel=True` 多证券返回 `(field, code)` MultiIndex 列；`panel=False` 返回 `time/code` 长表。

## 动态真实价格

RQData provider 提供两条路径：

- `fq="none"` 返回未复权价格。
- `fields=["factor"]` 返回复权因子，供 BulletTrade 回测数据会话在 `set_option("use_real_price", True)` 时计算动态前复权。

当显式传入 `pre_factor_ref_date` 且 `fq="pre"` 时，provider 会用未复权价格和 factor 按 `factor / factor_ref` 锚定到指定参考日。

真实账号到位后，需要补充以下验收：

- 分红/拆分窗口的动态前复权对账。
- 与 JQData 对比 `fq=None`、`fq=pre`、`use_real_price=True`。
- 股票、ETF、指数样例的涨跌停和停牌字段对账。

## 已知限制

- 真实账号权限、历史深度和分钟线可用范围仍待验证。
- `get_all_securities()`、指数成分和指数权重依赖 rqdatac 版本和账号权限。
- 当前文档不宣称 RQData 已完成生产级真实数据验收。
