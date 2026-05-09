# MiniQMT 数据源封装说明

`MiniQMTProvider` 位于 `bullet_trade/data/providers/miniqmt.py`，用于在本地安装了 mini QMT/xtquant 环境的场景下直接复用行情数据。

## 依赖与配置
- 依赖官方 `xtquant` 包（通常随 miniQMT 安装提供），未安装时会抛出明确的 ImportError。  
- 支持的配置/环境变量：
  - `data_dir` / `QMT_DATA_PATH`：显式指定 xtquant 数据目录（`auth` 会调用 `xtdata.set_data_dir`）。`.env.example` 内提供了 Windows 默认安装路径 `C:\国金QMT交易端模拟\userdata_mini` 作为参考。  
  - `mode`：回测 `backtest`（默认）或实盘 `live`，用于控制自动下载等行为，可在初始化参数中传入。  
  - `auto_download` / `MINIQMT_AUTO_DOWNLOAD`：是否调用 `xtdata.download_history_data`；未指定时默认开启，实盘也会主动补齐缺失行情。  
  - `market` / `MINIQMT_MARKET`：交易日市场代码（默认 `SH`）。  
  - `cache_dir` / `DATA_CACHE_DIR`：磁盘缓存目录，若不显式传入则使用 `DATA_CACHE_DIR/miniqmt`；未设置则禁用缓存。  
  - `tushare_token` / `TUSHARE_TOKEN`：xtquant 缺失分红或指数成分时自动回退到 Tushare。

## 代码后缀兼容
- `MiniQMTProvider` 接受聚宽 (`000001.XSHE`) 与 QMT (`000001.SZ`) 两种证券后缀，内部统一转换并保持缓存命中。  
- 输出会尊重调用方传入的格式；例如 `get_price(["000001.XSHE"])` 会返回列索引 `000001.XSHE`，`get_all_securities`/`get_index_stocks` 默认输出聚宽风格，并在 `qmt_code` 列保留原始后缀。  
- 在策略切换数据源时无需手动替换证券代码后缀。

## 价格与复权实现
- 通过 `xtdata.get_local_data` 读取数据，`dividend_type` 分别传入 `none/front_ratio/back_ratio`。若官方接口缺少复权行情，会基于拆分/派现事件自行回溯生成动态前复权序列。  
- 为了支持 `pre_factor_ref_date`，会同时读取未复权行情，计算参考日缩放系数，使得 `fq='pre'` 时价格在参考日与真实成交价一致。  
- `skip_paused=True` 时以 `volume>0` 近似判定停牌。

## 分红/拆分
- 通过 `xtdata.get_divid_factors` 直接提取派现、送转、配股等信息，转换为回测引擎统一的事件格式（默认按“每 1 股”计价）。  
- 如果当前 xtquant 版本缺少该接口或返回空结果，并且提供了 `tushare_token`，则自动回退到 `TushareProvider` 继续补足事件，同时会将按 10 股计价的派现金额标准化为“每 1 股”。配置缺失时仍可依赖复权价格运行。

## 指数成分
- 优先尝试 `xtdata.get_index_stocks`（当版本支持时）。  
- 若函数不存在或返回空且配置了 `tushare_token`，则自动调用 Tushare 的 `index_weight` 作为后备数据。

## 常见问题
1. **数据目录权限**：`xtdata.download_history_data` 对安装路径具有写权限要求，建议改为运行在 QMT 安装用户下，或在配置中禁用自动下载并预先同步数据。  
2. **时区差异**：`xtdata` 返回的时间戳使用毫秒，需要转换为 pandas `datetime`；封装已统一为本地时区，无需额外处理。  
3. **事件缺失**：旧版 xtquant 若不支持 `get_divid_factors`，且未配置 Tushare Token，则拆分/派息仍无法广播，回测需依赖复权价格。  
4. **指数行情格式**：部分指数在 QMT 内以特殊板块维护，若 `get_all_securities(types='index')` 返回空，请确认 miniQMT 版本是否已同步指数板块数据。

## 真实数据对齐测试
- BulletTrade 在 `tests/e2e/data/test_provider_parity.py` 内提供了平安银行 (`000001.XSHE`) 两个派息窗口（2025-06-12、2025-10-15）的 miniQMT 与 JQData 真实数据对齐测试。  
- 运行前请确保：
  - `.env` 中已设置 `JQDATA_USERNAME`、`JQDATA_PASSWORD`；`QMT_DATA_PATH` 可留空，除非需要覆盖 xtquant 默认目录或为后续交易能力提前配置，若显式配置则需保证路径有效并同步行情；
  - 默认会自动下载补齐缺失行情；如果显式设置 `MINIQMT_AUTO_DOWNLOAD=false`，请先确认本地行情已经预先同步；
  - 已安装 `jqdatasdk` 与 `xtquant`（可通过 `pip install jqdatasdk`、`pip install xtquant` 或 `pip install bullet-trade[qmt]` 完成）。
- 执行示例：
  ```bash
  python -m pytest bullet-trade/tests/e2e/data/test_provider_parity.py::test_ping_an_bank_real_parity \
    -m "requires_jqdata and requires_network"
  ```
- 测试在缺失账号或依赖时会自动跳过，并打印补齐提示；若设置了 `QMT_DATA_PATH` 但路径无效同样会提醒。无需修改 `DEFAULT_DATA_PROVIDER`，用例内部会显式初始化所需的数据源实例。

如需进一步自定义（如远程 QMT Server、ClickHouse 接入），可参考 `reference/tmp/jqtrade/data/data_gates/` 中的示例实现。***
