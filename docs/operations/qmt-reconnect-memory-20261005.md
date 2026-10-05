# QMT 失败重连资源累积修复

作者：BruceLee。日期：2026-10-05。

## 问题与实现

失败重连诊断发现，SDK 共享内存映射、TCP 连接与句柄会随重试累积。
连接日志持续报告失败，但调用 stop() 后部分底层资源仍未释放。

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
只验证连接失败，不调用下单/撤单。具体环境的连接配置、运行记录和部署回退信息应由使用者单独保管，
不要写入公开仓库。
