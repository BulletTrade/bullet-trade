# QMT 失败重连资源累积修复

作者：BruceLee。日期：2026-10-05。

主生产服务器 `real-brucelee-server`，核心目录 `C:\bullet-trade-workspace\bullet-trade`。
升级前 `dev`：`356fc953c46b43017e6b05eecd092ce74df8d7b2`。
生产服务：华鑫 MiniQMT Server，实例 123，监听 `127.0.0.1:18087`。
泄漏进程：11924；托管 wrapper：18012。

## 问题与实现

只读诊断确认 11924 保留 272 个 `userdata_mini/down_queue_*` 映射，
映射提交总计 19.126 GiB，每个约 72 MiB；私有提交约 657 MiB。
同时有 822 个 TCP 项和 6007 个句柄。日志每约 303 秒出现一次
`xtquant connect() 失败，返回码: -1`。

旧代码每次失败创建新 XtQuantTrader，SDK stop() 后旧映射仍存活。
本次让账户复用已启动 SDK 做连接或订阅重试，停止服务时才结束生命周期。
连接未成功仍保持不可用，健康实例幂等连接；不修改交易、金额和订单幂等规则。
启动未完成的实例保留原有失败清理逻辑。

## 验证

macOS：`tests/unit/test_qmt_socket_guard.py`、`tests/test_qmt_cancel_wait.py`、
`tests/broker/test_qmt_remote_warning.py`、`tests/server/test_process_memory_idempotency.py`
合计 59 passed。包含 301 次失败仅创建一个实例、连接/订阅失败恢复、显式停止幂等。
首次受沙箱回环监听限制，改为允许本机监听后完整通过，未降低验证标准。

真实 Windows SDK 隔离验收使用受 Git 管理的
`scripts/diagnostics/probe_qmt_reconnect_resources.py`，必须使用独占空目录，
只验证连接失败，不调用下单/撤单。执行结果、生产升级目标 SHA、停止/启动命令、
加载路径、内存复测和回退点随后写入工作区受 Git 管理的发布记录。

## 发布与回退

代码必须先从 macOS 推送到 `bullet-trade/dev`，服务器工作树干净后
`git pull --ff-only`；只重启服务实例 123，其他进程无需重启。
回退点为上述升级前完整 SHA；如需回退，先在 macOS 发布对应 revert 到 dev，
生产继续通过快进拉取恢复，不在服务器修改 Git 历史或用文件拷贝覆盖。
