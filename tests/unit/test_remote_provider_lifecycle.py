"""作者：BruceLee。

职责：验证远程行情 Provider 关闭仅回收自己的传输，不执行任何券商写入。
输入：明确假服务地址及独立传输夹具；输出：创建/启动/关闭和零请求断言。
上下游：RemoteQmtProvider 与其自有 RemoteQmtConnection；无网络及真实账户。
约定：传输存活不代表行情或交易验收，本项只测试资源归属。
"""

from unittest.mock import Mock, patch

from bullet_trade.data.providers.remote_qmt import RemoteQmtProvider


def test_close_only_owns_its_connection():
    """无输入返回；关闭一个 Provider 两次不关闭另一个，不发远程下单、撤单或服务器终止请求。"""
    first, second = Mock(), Mock()
    with patch(
        "bullet_trade.data.providers.remote_qmt.RemoteQmtConnection", side_effect=[first, second]
    ):
        left = RemoteQmtProvider({"host": "fixture-a", "port": 1234, "token": "fixture-a"})
        right = RemoteQmtProvider({"host": "fixture-b", "port": 1235, "token": "fixture-b"})
        left.close()
        left.close()
        assert first.close.call_count == 2
        second.close.assert_not_called()
        first.request.assert_not_called()
        second.request.assert_not_called()
        right.close()
        assert second.close.call_count == 1
