"""验证通用远程 server 的 TLS 配置真正进入 asyncio 监听。"""

from types import SimpleNamespace

import pytest

from bullet_trade.remote.connection import RemoteQmtConnection
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.app import ServerApplication, _build_tls_context
from bullet_trade.server.config import ServerConfig, TLSConfig, build_server_config
from bullet_trade.server.session import ClientSession


class _FakeServer:
    """提供 start/serve/shutdown 所需最小 asyncio server 合同。"""

    def __init__(self):
        """创建无 socket 的测试 server。

        Returns:
            None。
        """

        self.sockets = []
        self.closed = False

    async def serve_forever(self):
        """立即返回，允许应用进入 shutdown。

        Returns:
            None。
        """

        return None

    def close(self):
        """记录关闭动作。

        Returns:
            None。
        """

        self.closed = True

    async def wait_closed(self):
        """模拟异步等待关闭。

        Returns:
            None。
        """

        return None


class _LifecycleBroker:
    """记录 TLS 初始化失败前后的 broker 生命周期。"""

    def __init__(self):
        """初始化未启动、未停止状态。

        Returns:
            None。
        """

        self.started = False
        self.stopped = False

    async def start(self):
        """记录组件已经启动。

        Returns:
            None。
        """

        self.started = True

    async def stop(self):
        """记录组件已经清理。

        Returns:
            None。
        """

        self.stopped = True


def test_build_tls_context_loads_server_certificate(monkeypatch) -> None:
    """验证 TLS helper 使用服务端协议并加载证书链。"""

    calls = {}

    class _Context:
        """记录证书加载参数的 SSLContext 替身。"""

        def __init__(self, protocol):
            """记录协议。

            Args:
                protocol: SSL 服务端协议常量。

            Returns:
                None。
            """

            calls["protocol"] = protocol
            self.minimum_version = None

        def load_cert_chain(self, certfile, keyfile):
            """记录证书和私钥路径。

            Args:
                certfile: 证书路径。
                keyfile: 私钥路径。

            Returns:
                None。
            """

            calls["certfile"] = certfile
            calls["keyfile"] = keyfile

    monkeypatch.setattr("bullet_trade.server.app.ssl.SSLContext", _Context)
    config = ServerConfig(tls=TLSConfig(True, "cert.pem", "key.pem"))

    context = _build_tls_context(config)

    assert isinstance(context, _Context)
    assert calls["certfile"] == "cert.pem"
    assert calls["keyfile"] == "key.pem"


@pytest.mark.asyncio
async def test_server_passes_ssl_context_to_asyncio_start_server(monkeypatch) -> None:
    """验证应用 start 把已构造 SSLContext 传给监听 socket。"""

    sentinel = object()
    captured = {}
    fake_server = _FakeServer()

    async def _start_server(callback, host, port, **kwargs):
        """记录 asyncio.start_server 的关键参数。

        Args:
            callback: 客户连接回调。
            host: 监听地址。
            port: 监听端口。
            **kwargs: ssl 等关键字参数。

        Returns:
            _FakeServer: 测试 server。
        """

        captured.update(callback=callback, host=host, port=port, kwargs=kwargs)
        return fake_server

    monkeypatch.setattr("bullet_trade.server.app._build_tls_context", lambda config: sentinel)
    monkeypatch.setattr("bullet_trade.server.app.asyncio.start_server", _start_server)
    config = ServerConfig(listen="127.0.0.1", port=7000)
    app = ServerApplication(config, AccountRouter([]), AdapterBundle(None, None, False))

    await app.start()

    assert captured["kwargs"]["ssl"] is sentinel
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 7000
    assert fake_server.closed is True


def test_partial_tls_configuration_is_rejected(monkeypatch) -> None:
    """验证只有证书没有私钥时配置阶段立即失败。"""

    monkeypatch.setattr("bullet_trade.server.config.get_env", lambda name, default=None: default)
    args = SimpleNamespace(tls_cert="cert.pem", tls_key=None)

    with pytest.raises(ValueError, match="证书和私钥"):
        build_server_config(args)


@pytest.mark.asyncio
async def test_tls_initialization_failure_stops_started_broker(monkeypatch) -> None:
    """验证证书加载失败不会遗留已连接的真实 broker 组件。"""

    broker = _LifecycleBroker()
    config = ServerConfig(tls=TLSConfig(True, "missing.pem", "missing.key"))
    app = ServerApplication(
        config,
        AccountRouter([]),
        AdapterBundle(None, broker, False),
    )

    monkeypatch.setattr(
        "bullet_trade.server.app._build_tls_context",
        lambda _config: (_ for _ in ()).throw(ValueError("bad certificate")),
    )

    with pytest.raises(ValueError, match="bad certificate"):
        await app.start()

    assert broker.started is True
    assert broker.stopped is True


def test_remote_tls_cannot_fall_back_to_plaintext_without_ca() -> None:
    """验证显式 TLS 缺少 CA/证书时在联网前立即失败。"""

    with pytest.raises(ValueError, match="禁止回退明文"):
        RemoteQmtConnection("real.bullettrade.cn", 7000, "token", tls_enabled=True)


@pytest.mark.asyncio
async def test_session_token_uses_constant_time_comparison(monkeypatch) -> None:
    """验证握手 token 通过 compare_digest 比较而非普通相等运算。"""

    compared = []

    class _App:
        """提供握手所需的最小 server app 合同。"""

        config = SimpleNamespace(token="fixed-token")

        def register_session(self, session):
            """接受测试会话注册。

            Args:
                session: 当前 ClientSession。

            Returns:
                None。
            """

            return None

        def active_features(self):
            """返回空功能列表。

            Returns:
                list: 空列表。
            """

            return []

    class _Writer:
        """不执行真实 I/O 的 writer 替身。"""

        def is_closing(self):
            """返回 writer 未关闭。

            Returns:
                bool: False。
            """

            return False

    async def _read_message(_reader):
        """返回固定握手。

        Args:
            _reader: 未使用的 reader。

        Returns:
            dict: 正确 token 的握手消息。
        """

        return {"type": "handshake", "token": "fixed-token"}

    async def _write_message(_writer, _message):
        """吞掉握手确认。

        Args:
            _writer: 测试 writer。
            _message: 握手确认消息。

        Returns:
            None。
        """

        return None

    def _compare_digest(left, right):
        """记录常量时间比较的两个操作数。

        Args:
            left: 客户端 token。
            right: 服务端 token。

        Returns:
            bool: 两者相等时为 True。
        """

        compared.append((left, right))
        return left == right

    monkeypatch.setattr("bullet_trade.server.session.read_message", _read_message)
    monkeypatch.setattr("bullet_trade.server.session.write_message", _write_message)
    monkeypatch.setattr("bullet_trade.server.session.hmac.compare_digest", _compare_digest)
    session = ClientSession(_App(), object(), _Writer(), "127.0.0.1")

    await session._handshake()

    assert compared == [(b"fixed-token", b"fixed-token")]
