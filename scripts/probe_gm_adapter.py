#!/usr/bin/env python
"""作者: BruceLee
职责: 验收正式 GmDataProvider 注册路径，Windows 上仅执行临时 SDK 只读 worker。
输入: 显式 SSH 主机/解释器、私密 RPC env、已有 RPC 模块；输出: 脱敏原始事实和比较报告。
GM SSH 客户端仅供验收桥接，不是已发布的远程数据服务；不写入远端、不访问账户/交易。
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import signal
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values
from probe_gm_parity import WINDOWS, remote_bootstrap

from bullet_trade.data.api import _create_provider
from bullet_trade.integrations.gm.data_client import GmDataError
from bullet_trade.integrations.gm.validation import (
    BAR_FIELDS,
    compare_bars,
    jq_tick_time,
)


class SshSdkReadClient:
    """仅验收：将正式 data_worker 源码送至 Windows 内存执行，缓存同一轮重复历史请求。"""

    def __init__(self, args):
        self.args = args
        self.cache = {}
        self.source = (
            Path(__file__).resolve().parents[1] / "bullet_trade/integrations/gm/data_worker.py"
        ).read_text()

    def auth(self):
        pass

    def query(self, method, **kwargs):
        key = json.dumps([method, kwargs], default=str, sort_keys=True)
        if key in self.cache:
            return self.cache[key]
        # 主启动器静态取唯一 token；源文件为正式生产 worker，无验收专用 SDK 兜底。
        worker = self.source.replace(
            "request = json.load(sys.stdin)",
            'request = json.load(sys.stdin)\n        request.update(request["cases"][0])',
        )
        code = remote_bootstrap(worker, [dict(method=method, kwargs=kwargs)], False).replace(
            "BT_GM_VALIDATION=", "BT_GM_DATA="
        )
        command = 'set PYTHONDONTWRITEBYTECODE=1&& "' + self.args.gm_python + '" -'
        proc = subprocess.run(
            [
                "ssh",
                "-4",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=12",
                self.args.gm_ssh_host,
                command,
            ],
            input=code.encode(),
            capture_output=True,
            timeout=55,
        )
        lines = [
            x
            for x in proc.stdout.decode("utf8", errors="replace").splitlines()
            if x.startswith("BT_GM_DATA=")
        ]
        if proc.returncode != 0 or not lines:
            raise GmDataError("GM 验收 SSH worker 失败")
        result = json.loads(lines[-1].split("=", 1)[1])
        if result.get("status") != "ok":
            raise GmDataError("GM 验收数据查询失败")
        self.cache[key] = result["data"]
        return result["data"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["rpc-env-file", "rpc-module", "gm-ssh-host", "gm-python", "output-dir"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--mode", choices=["native"], default="native")
    args = parser.parse_args()
    signal.signal(signal.SIGALRM, lambda *a: (_ for _ in ()).throw(TimeoutError("验收总超时")))
    signal.alarm(900)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    env = dotenv_values(args.rpc_env_file, interpolate=False)
    spec = importlib.util.spec_from_file_location("gm_adapter_rpc", args.rpc_module)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        rpc = module.RPCProxy(
            env["JQ_PROXY_HOST"],
            int(env["JQ_PROXY_PORT"]),
            env["JQ_PROXY_AUTHKEY"],
            recv_timeout=12,
            max_retries=1,
        )
    client = SshSdkReadClient(args)
    provider = _create_provider("gm", dict(client=client))
    report = dict(
        created_at=datetime.now().astimezone().isoformat(),
        provider_source_sha256=hashlib.sha256(
            (Path(__file__).resolve().parents[1] / "bullet_trade/data/providers/gm.py").read_bytes()
        ).hexdigest(),
        baseline="JoinQuant RPC",
        alignment_mode="native",
        runtime_data_sources=["gm"],
        benchmark_role="comparison_only_not_provider_input",
        runtime_path=(
            "data.api._create_provider -> GmDataProvider -> "
            "production data_worker -> Windows GM SDK"
        ),
        sdk_transport="temporary read-only SSH validation bridge",
        cases=[],
        not_covered=["broker/trading", "Finance dividend RPC blocked", "historical security names"],
    )

    def save():
        (out / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str)
        )
        (out / "sdk_reads.json").write_text(
            json.dumps(client.cache, ensure_ascii=False, default=str)
        )

    try:
        cases = []
        for name, security, first, last, freq in WINDOWS:
            for fq in [None, "pre", "post"]:
                cases.append(
                    dict(
                        id=name + "_" + str(fq),
                        security=security,
                        kwargs=dict(
                            start_date=first,
                            end_date=last,
                            frequency=freq,
                            fields=list(BAR_FIELDS),
                            skip_paused=True,
                            fq=fq,
                        ),
                    )
                )
        for name, kw in [
            (
                "paused_fill",
                dict(start_date="2016-06-27", end_date="2016-07-05", skip_paused=False),
            ),
            ("paused_skip", dict(start_date="2016-06-27", end_date="2016-07-05", skip_paused=True)),
            (
                "holiday_empty",
                dict(start_date="2026-10-01", end_date="2026-10-03", skip_paused=True),
            ),
            ("count_raw", dict(end_date="2026-09-30", count=5, skip_paused=True)),
        ]:
            cases.append(
                dict(
                    id=name,
                    security=(
                        "000002.XSHE"
                        if name.startswith("paused")
                        else "510300.XSHG" if name == "holiday_empty" else "601318.XSHG"
                    ),
                    kwargs=dict(fields=list(BAR_FIELDS), fq=None, **kw),
                )
            )
        for freq, first, last in [
            ("1m", "2026-09-30 09:31:00", "2026-09-30 09:35:00"),
            ("5m", "2026-09-30 13:05:00", "2026-09-30 13:30:00"),
        ]:
            cases.append(
                dict(
                    id="exact_start_" + freq,
                    security="000001.XSHE",
                    kwargs=dict(
                        start_date=first,
                        end_date=last,
                        frequency=freq,
                        fields=list(BAR_FIELDS),
                        fq=None,
                        skip_paused=True,
                    ),
                )
            )
        cases.append(
            dict(
                id="minute_halt",
                security="000002.XSHE",
                kwargs=dict(
                    start_date="2016-07-01 09:30:00",
                    end_date="2016-07-01 09:33:00",
                    frequency="1m",
                    fields=list(BAR_FIELDS),
                    fq=None,
                    skip_paused=False,
                ),
            )
        )
        for case in cases:
            entry = dict(id=case["id"])
            try:
                actual = provider.get_price(case["security"], **case["kwargs"])
                reference = rpc.get_price(case["security"], **case["kwargs"])
                decimals = (
                    provider._decimals(provider._info(case["security"])) if not actual.empty else 3
                )
                compared = compare_bars(
                    reference, actual, 10**-decimals, allow_empty=case["id"] == "holiday_empty"
                )
                entry.update(
                    ok=compared["ok"],
                    comparison=compared,
                    field_sources=actual.attrs.get("field_sources", {}),
                )
                actual.to_csv(out / (case["id"] + "_adapter.csv"))
                reference.to_csv(out / (case["id"] + "_jq.csv"))
            except Exception as exc:
                entry.update(ok=False, error_type=type(exc).__name__)
            report["cases"].append(entry)
            save()
            print(
                json.dumps(dict(case=entry["id"], ok=entry["ok"], completed=len(report["cases"]))),
                flush=True,
            )
        for skip in [False, True]:
            name = "tick_skip_" + str(skip)
            try:
                actual = provider.get_ticks(
                    "510300.XSHG",
                    "2026-09-30 15:00:00",
                    count=1,
                    skip=skip,
                    fields=["time", "current", "volume", "money"],
                    df=True,
                )
                reference = rpc.get_ticks(
                    "510300.XSHG",
                    end_dt="2026-09-30 15:00:00",
                    count=1,
                    skip=skip,
                    fields=["time", "current", "volume", "money"],
                    df=True,
                )
                a, b = actual.iloc[-1], reference.iloc[-1]
                delta = abs((jq_tick_time(a.time) - jq_tick_time(b.time)).total_seconds())
                ok = (
                    delta <= 1
                    and abs(a.current - b.current) <= 0.001
                    and abs(a.volume - b.volume) <= 1
                    and abs(a.money - b.money) <= max(0.01, abs(b.money) * 1e-8)
                )
                report["cases"].append(dict(id=name, ok=bool(ok), time_delta_seconds=delta))
            except Exception as exc:
                report["cases"].append(dict(id=name, ok=False, error_type=type(exc).__name__))
            save()
            print(json.dumps(report["cases"][-1]), flush=True)
        extended = [
            (
                "fields_" + str(fq),
                "601318.XSHG",
                dict(start_date="2024-07-24", end_date="2024-07-30", fq=fq, skip_paused=True),
            )
            for fq in [None, "pre", "post"]
        ]
        extended.append(
            (
                "minute_fields",
                "000001.XSHE",
                dict(
                    start_date="2026-09-30 13:01:00",
                    end_date="2026-09-30 13:05:00",
                    frequency="1m",
                    fq=None,
                    skip_paused=True,
                ),
            )
        )
        for name, security, kw in extended:
            fields = ["close", "pre_close", "high_limit", "low_limit", "avg", "paused", "factor"]
            try:
                actual = provider.get_price(security, fields=fields, **kw)
                reference = rpc.get_price(security, fields=fields, **kw)
                difference = {f: float(abs(actual[f] - reference[f]).max()) for f in fields}
                ok = actual.index.equals(reference.index) and all(
                    difference[f] <= (1e-8 if f in {"factor", "paused"} else 0.01 + 1e-12)
                    for f in fields
                )
                report["cases"].append(dict(id=name, ok=bool(ok), difference=difference))
                actual.to_csv(out / (name + "_adapter.csv"))
                reference.to_csv(out / (name + "_jq.csv"))
            except Exception as exc:
                report["cases"].append(dict(id=name, ok=False, error_type=type(exc).__name__))
            save()
        try:
            securities = ["510300.XSHG", "000001.XSHE"]
            actual = provider.get_price(
                securities,
                start_date="2026-09-28",
                end_date="2026-09-30",
                fields=list(BAR_FIELDS),
                fq=None,
                skip_paused=True,
                panel=False,
            )
            comparisons = []
            for security in securities:
                reference = rpc.get_price(
                    security,
                    start_date="2026-09-28",
                    end_date="2026-09-30",
                    fields=list(BAR_FIELDS),
                    fq=None,
                    skip_paused=True,
                )
                observed = actual.loc[actual.code == security].set_index("time")[list(BAR_FIELDS)]
                comparisons.append(
                    compare_bars(reference, observed, 0.001 if security.startswith("5") else 0.01)
                )
            report["cases"].append(
                dict(
                    id="multi_symbol", ok=all(c["ok"] for c in comparisons), comparisons=comparisons
                )
            )
        except Exception as exc:
            report["cases"].append(dict(id="multi_symbol", ok=False, error_type=type(exc).__name__))
        save()
        # 免费官方细类在本轮通过真实 SDK 验证，名称差异不冒充全部元信息对齐。
        table = rpc.get_all_securities(types=["stock", "etf", "fund"], date="2026-09-30")
        if not isinstance(table, pd.DataFrame):
            raise GmDataError("聚宽证券表读取失败")
        for security in ["601318.XSHG", "000001.XSHE", "510300.XSHG", "511880.XSHG"]:
            info = provider.get_security_info(security, date="2026-09-30")
            ref = table.loc[security]
            report["cases"].append(
                dict(
                    id="type_" + security,
                    ok=info["type"] == ref["type"]
                    and info["start_date"] == pd.Timestamp(ref["start_date"]).date(),
                    gm_type=info["type"],
                    jq_type=ref["type"],
                )
            )
        for name, call, benchmark in [
            (
                "calendar",
                lambda: provider.get_trade_days("2026-09-21", "2026-10-09"),
                lambda: rpc.get_trade_days(start_date="2026-09-21", end_date="2026-10-09"),
            ),
            (
                "index_constituents",
                lambda: provider.get_index_stocks("000300.XSHG", "2026-09-30"),
                lambda: rpc.get_index_stocks("000300.XSHG", date="2026-09-30"),
            ),
        ]:
            try:
                actual, reference = call(), benchmark()
                if name == "calendar":
                    actual = [pd.Timestamp(x).date().isoformat() for x in actual]
                    reference = [pd.Timestamp(x).date().isoformat() for x in reference]
                report["cases"].append(dict(id=name, ok=sorted(actual) == sorted(reference)))
            except Exception as exc:
                report["cases"].append(dict(id=name, ok=False, error_type=type(exc).__name__))
            save()
        for include, end in [(False, "2024-07-30 14:00:00"), (True, "2024-07-30 15:00:00")]:
            name = "bars_include_" + str(include)
            try:
                fields = ["date", "open", "high", "low", "close", "volume", "money"]
                kwargs = dict(
                    count=5,
                    unit="1d",
                    fields=fields,
                    end_dt=end,
                    include_now=include,
                    fq_ref_date=pd.Timestamp("2024-07-30").date(),
                    df=True,
                )
                actual = provider.get_bars("601318.XSHG", **kwargs).set_index("date")
                reference = rpc.get_bars("601318.XSHG", **kwargs).set_index("date")
                compared = compare_bars(reference, actual, 0.01)
                report["cases"].append(dict(id=name, ok=compared["ok"], comparison=compared))
                actual.to_csv(out / (name + "_adapter.csv"))
                reference.to_csv(out / (name + "_jq.csv"))
            except Exception as exc:
                report["cases"].append(dict(id=name, ok=False, error_type=type(exc).__name__))
            save()
        save()
    finally:
        rpc.close()
    report["passed"] = sum(x["ok"] for x in report["cases"])
    report["total"] = len(report["cases"])
    report["ok"] = all(x["ok"] for x in report["cases"])
    save()
    print(json.dumps({k: report[k] for k in ["passed", "total", "ok"]}), flush=True)
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
