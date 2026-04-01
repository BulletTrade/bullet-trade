"""
远程 QMT server 运行时探针。

默认只读检查；如需实际下单 smoke，请显式加 `--trade-smoke`。
"""

from bullet_trade.server.runtime_probe import main


if __name__ == "__main__":
    raise SystemExit(main())
