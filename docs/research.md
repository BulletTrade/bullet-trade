# 研究环境（JupyterLab）

BulletTrade 提供 `bullet-trade lab`/`bullet-trade jupyterlab` 一键启动研究环境。默认根目录位于用户主目录下的 `bullet-trade`，集中存放 Notebook、`.env` 与输出。

首次运行要注意一下设置和目录：
![first-started](assets/jupyter-first-started.png)

## 默认路径与安全策略
- Notebook 根目录：macOS/Linux `~/bullet-trade`，Windows `~\\bullet-trade`
- 设置文件：`~/.bullet-trade/setting.json`，可调 `host`、`port`、`root_dir`、`env_path`、`open_browser`、`no_password`、`no_cert`、`token`
- `.env` 读取顺序：默认仅加载根目录下的 `.env`（不会读取当前工作目录的 `.env`）
- 默认监听 `127.0.0.1:8088`，开启 Jupyter token，未配置密码/证书；如需公网访问请务必配置密码或证书

运行后会自动打开浏览器：
![launcher](assets/jupyter-launcher.png)

## 首次运行会自动完成
1. 创建 `~/.bullet-trade/setting.json`、默认根目录与 `.env`。
2. 将安装目录的 `bullet_trade/notebook/` 样例复制到根目录（保留子目录结构，已存在同名文件跳过并在日志中提示）。
3. 写入 BulletTrade 代码片段设置，可在 Command Palette 搜索 “BulletTrade” 插入。
4. 打印当前使用的设置文件与 `.env` 路径，以及可修改的默认行为。

欢迎样例ipynb
![welcome](assets/jupyter-welcome.png)


## 常用命令
- 启动研究环境（默认打开浏览器）：  
  ```bash
  bullet-trade lab
  ```
- 仅做依赖/端口诊断：  
  ```bash
  bullet-trade lab --diagnose
  ```
- 自定义监听或根目录：编辑 `~/.bullet-trade/setting.json`（如修改 `root_dir`、`env_path`、`host`、`port`），或临时使用 `--ip/--port/--notebook-dir` 覆盖。

系统部分api与聚宽对齐，下面是接入miniqmt配置后的数据获取样例：
![data](assets/jupyter-data.png)

## 与回测/实盘的衔接
- 在根目录 `.env` 配置数据源与券商参数，Notebook 中 `import bullet_trade` 或聚宽兼容 API 即可直接使用。
- 复用根目录运行命令行：  
  ```bash
  bullet-trade backtest strategies/demo_strategy.py --start 2025-01-01 --end 2025-06-01
  bullet-trade live strategies/demo_strategy.py --broker qmt
  ```

## 常见问题
- **端口占用**：使用 `bullet-trade lab --diagnose --port 8088` 查看占用，或在 `setting.json` 修改端口。  
- **无法写入目录**：确保根目录可写，必要时调整 `root_dir`。  
- **安全提示**：当监听非 127.0.0.1 且未配置密码/证书时，CLI 会拒绝启动；请配置密码/证书或改为 loopback。
