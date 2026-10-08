"""远程拒绝与提交未知的离线协议回归。

作者：BruceLee
职责：从错误帧验证客户端副作用分类及同步异常保留。
输入：合成错误帧、下单/撤单 action；输出：异常类型、原因和发送次数断言。
上下游：真实 RemoteQmtConnection 与协议编解码；不连接网络或真实券商。
环境：pytest 与本机 asyncio；全部凭据和幂等键均为合成数据。
"""

import asyncio
import concurrent.futures

import pytest

from bullet_trade.remote.connection import (
    RemoteQmtConnection,
    RemoteServerError,
    RemoteSubmissionUnknownError,
)
from bullet_trade.server.protocol import encode_message


@pytest.mark.parametrize("action", ["broker.place_order", "broker.cancel_order"])
@pytest.mark.parametrize("code", ["REQUEST_FAILED", "REQUEST_TIMEOUT"])
@pytest.mark.parametrize("fact", [False, True, None, 0, "false", "missing"])
def test_error_frame_preserves_strict_broker_evidence(action, code, fact):
    """输入写动作、错误码和调用证据，无返回；明确拒绝与未知均不得重发。"""
    conn = RemoteQmtConnection("127.0.0.1", 0, "synthetic-token")
    conn._connected.set()
    sent = []

    async def send(message):
        """输入请求帧，返回None；将合成错误送入真实读取循环，不产生网络副作用。"""
        sent.append(message)
        error = dict(type="error", id=message["id"], code=code, message="合成拒绝原因")
        if fact != "missing":
            error["broker_called"] = fact
        conn._reader = asyncio.StreamReader()
        conn._reader.feed_data(encode_message(error))
        conn._reader.feed_eof()
        await conn._reader_loop()

    conn._send = send

    async def run():
        """无输入，返回客户端异常；执行单次写请求，不调用券商。"""
        expected = (
            RemoteServerError
            if fact is False and code == "REQUEST_FAILED"
            else RemoteSubmissionUnknownError
        )
        with pytest.raises(expected, match="合成拒绝原因") as raised:
            await conn._request_async(action, {"idempotency_key": "synthetic-once"})
        return raised.value

    error = asyncio.run(run())
    assert len(sent) == 1
    assert not conn._pending
    if fact is False and code == "REQUEST_FAILED":
        assert error.broker_called is False
        assert error.code == code
    else:
        assert error.idempotency_key == "synthetic-once"
        assert error.request_payload == sent[0]["payload"]


@pytest.mark.parametrize("action", ["broker.place_order", "broker.cancel_order"])
def test_sync_request_preserves_unknown_reason(monkeypatch, action):
    """输入pytest夹具和写动作，无返回；已完成的未知异常不能被改写成等待超时。"""
    conn = RemoteQmtConnection("127.0.0.1", 0, "synthetic-token")
    conn._loop = object()
    error = RemoteSubmissionUnknownError(action, "synthetic-once", {}, message="原始连接中断")
    future = concurrent.futures.Future()
    future.set_exception(error)

    def submit(coro, loop):
        """输入协程与占位循环，返回已失败Future；关闭协程，避免启动后台任务。"""
        coro.close()
        return future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    with pytest.raises(RemoteSubmissionUnknownError) as raised:
        conn.request(action, {})
    assert raised.value is error
    assert "client response timeout" not in str(raised.value)
