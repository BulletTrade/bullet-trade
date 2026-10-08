#!/usr/bin/env python
"""掘金 Windows SDK 与聚宽 RPC 的有界只读验收。

显式指定已有 RPC 模块、私密 env、Windows SSH 主机及 Python；从终端项目
静态读取唯一 token，从日志选择唯一 RemoteV5 账户，不执行项目或修改终端。
证据写入 output-dir；任何差异/权限错误/缺覆盖返回非零，不把跳过视为通过。
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import signal
import subprocess
from datetime import datetime
from functools import partial
from operator import getitem
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import text

from bullet_trade.integrations.gm.validation import (
    BAR_FIELDS,
    compare_bars,
    compare_dividends,
    gm_bars,
    gm_symbol,
    jq_symbol,
    jq_tick_time,
    legacy_dividends,
    local_time,
    validate_account,
)

ANCHOR = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
# 固定样本与阈值在发请求前确定。包括股息、送转、基金年末分红及已知停牌。
WINDOWS = [
    ("recent_etf", "510300.XSHG", "2026-09-28", "2026-09-30", "daily"),
    ("recent_bank", "000001.XSHE", "2026-09-28", "2026-09-30", "daily"),
    ("stock_cash_july", "601318.XSHG", "2024-07-24", "2024-07-30", "daily"),
    ("stock_cash_october", "601318.XSHG", "2024-10-16", "2024-10-22", "daily"),
    ("stock_transfer", "300750.XSHE", "2023-04-24", "2023-04-28", "daily"),
    ("fund_year_end", "511880.XSHG", "2024-12-26", "2024-12-31", "daily"),
    ("etf_distribution", "510300.XSHG", "2024-12-16", "2024-12-20", "daily"),
    ("index", "000300.XSHG", "2026-09-28", "2026-09-30", "daily"),
    ("minute_etf", "510300.XSHG", "2026-09-30 09:30:00", "2026-09-30 10:00:00", "1m"),
    ("minute_bank", "000001.XSHE", "2026-09-30 13:00:00", "2026-09-30 13:30:00", "5m"),
]


def build_cases():
    """返回完整市场查询矩阵，不带凭据或账户 ID。"""
    result = []
    for name, security, first, last, freq in WINDOWS:
        for mode, fq, adjust in [("raw", None, 0), ("pre", "pre", 1), ("post", "post", 2)]:
            gm = dict(
                symbol=gm_symbol(security),
                frequency="1d" if freq == "daily" else str(int(freq[:-1]) * 60) + "s",
                start_time=first + " 00:00:00" if len(first) == 10 else first,
                end_time=last + " 23:59:59" if len(last) == 10 else last,
                fields="symbol,eob,open,high,low,close,volume,amount",
                skip_suspended=True,
                adjust=adjust,
                adjust_end_time=ANCHOR + " 23:59:59",
                df=False,
            )
            result.append(
                {
                    "id": name + "_" + mode,
                    "kind": "bars",
                    "security": security,
                    "fq": fq,
                    "gm": {"method": "history", "kwargs": gm},
                    "jq": dict(
                        start_date=first,
                        end_date=last,
                        frequency=freq,
                        fields=list(BAR_FIELDS),
                        fq=fq,
                        skip_paused=True,
                    ),
                }
            )
    for name, skip, fill in [("paused_skip", True, None), ("paused_fill", False, "Last")]:
        result.append(
            {
                "id": name,
                "kind": "bars",
                "security": "000002.XSHE",
                "fq": None,
                "allow_empty": skip,
                "gm": {
                    "method": "history",
                    "kwargs": dict(
                        symbol="SZSE.000002",
                        frequency="1d",
                        start_time="2016-06-27 00:00:00",
                        end_time="2016-07-05 23:59:59",
                        fields="symbol,eob,open,high,low,close,volume,amount",
                        skip_suspended=skip,
                        fill_missing=fill,
                        adjust=0,
                        df=False,
                    ),
                },
                "jq": dict(
                    start_date="2016-06-27",
                    end_date="2016-07-05",
                    frequency="daily",
                    fields=list(BAR_FIELDS),
                    fq=None,
                    skip_paused=skip,
                    fill_paused=True,
                ),
            }
        )
    result += [
        {
            "id": "holiday_empty",
            "kind": "bars",
            "security": "510300.XSHG",
            "fq": None,
            "allow_empty": True,
            "gm": {
                "method": "history",
                "kwargs": dict(
                    symbol="SHSE.510300",
                    frequency="1d",
                    start_time="2026-10-01 00:00:00",
                    end_time="2026-10-03 23:59:59",
                    fields="symbol,eob,open,high,low,close,volume,amount",
                    adjust=0,
                    df=False,
                ),
            },
            "jq": dict(
                start_date="2026-10-01",
                end_date="2026-10-03",
                frequency="daily",
                fields=list(BAR_FIELDS),
                fq=None,
                skip_paused=True,
            ),
        },
        {
            "id": "count_raw",
            "kind": "bars",
            "security": "601318.XSHG",
            "fq": None,
            "gm": {
                "method": "history_n",
                "kwargs": dict(
                    symbol="SHSE.601318",
                    frequency="1d",
                    count=5,
                    end_time="2026-09-30 23:59:59",
                    fields="symbol,eob,open,high,low,close,volume,amount",
                    adjust=0,
                    df=False,
                ),
            },
            "jq": dict(
                count=5,
                end_date="2026-09-30",
                frequency="daily",
                fields=list(BAR_FIELDS),
                fq=None,
                skip_paused=True,
            ),
        },
    ]
    extra = [
        (
            "calendar",
            "get_trading_dates",
            dict(exchange="SHSE", start_date="2026-09-21", end_date="2026-10-09"),
        ),
        (
            "instruments",
            "get_instrumentinfos",
            dict(symbols="SHSE.510300,SZSE.000001,SHSE.601318,SHSE.511880", df=False),
        ),
        (
            "index_constituents",
            "stk_get_index_constituents",
            dict(index="SHSE.000300", trade_date="2026-09-30"),
        ),
        (
            "stock_dividend",
            "get_dividend",
            dict(symbol="SHSE.601318", start_date="2024-07-01", end_date="2024-10-31", df=False),
        ),
        (
            "transfer_dividend",
            "get_dividend",
            dict(symbol="SZSE.300750", start_date="2023-04-01", end_date="2023-04-30", df=False),
        ),
        (
            "fund_dividend_legacy",
            "get_dividend",
            dict(symbol="SHSE.511880", start_date="2024-12-01", end_date="2024-12-31", df=False),
        ),
        (
            "fund_dividend",
            "fnd_get_dividend",
            dict(fund="SHSE.511880", start_date="2024-12-01", end_date="2024-12-31"),
        ),
        (
            "stock_factor",
            "stk_get_adj_factor",
            dict(
                symbol="SHSE.601318",
                start_date="2024-07-24",
                end_date="2024-07-30",
                base_date=ANCHOR,
            ),
        ),
        (
            "fund_factor",
            "fnd_get_adj_factor",
            dict(
                fund="SHSE.510300", start_date="2026-09-28", end_date="2026-09-30", base_date=ANCHOR
            ),
        ),
        (
            "current",
            "current",
            dict(symbols="SHSE.510300,SZSE.000001", fields="symbol,created_at,price"),
        ),
        (
            "historical_instruments",
            "get_history_instruments",
            dict(symbols="SHSE.601318", start_date="2026-09-28", end_date="2026-09-30", df=False),
        ),
        (
            "historical_tick",
            "history_n",
            dict(
                symbol="SHSE.510300",
                frequency="tick",
                count=1,
                end_time="2026-09-30 15:00:00",
                fields="symbol,created_at,price,cum_volume,cum_amount",
                df=False,
            ),
        ),
        (
            "multi_symbol_raw",
            "history",
            dict(
                symbol="SHSE.510300,SZSE.000001",
                frequency="1d",
                start_time="2026-09-28 00:00:00",
                end_time="2026-09-30 23:59:59",
                fields="symbol,eob,open,high,low,close,volume,amount",
                adjust=0,
                df=False,
            ),
        ),
        ("account", "account_readonly", {}),
    ]
    result += [
        {"id": name, "kind": "extra", "gm": {"method": method, "kwargs": kwargs}}
        for name, method, kwargs in extra
    ]
    tick_case = next(c for c in result if c["id"] == "historical_tick")
    result.append({"id": "historical_tick_all", "kind": "extra", "gm": tick_case["gm"]})
    return result


def remote_bootstrap(worker_source, cases, include_account):
    """静态解析唯一凭据；只在显式账户批次选择日志内唯一 RemoteV5 ID。"""
    return (
        """import pathlib,json,ast,re,subprocess,sys
root=pathlib.Path.home()/'.cfgm3';tokens=set();ids=set()
for p in (root/'projects').glob('*/main.py'):
 for n in ast.walk(ast.parse(p.read_text(encoding='utf-8-sig'))):
  if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='run':
   q={k.arg:k.value.value for k in n.keywords if isinstance(k.value,ast.Constant)}
   if q.get('token'):tokens.add(q['token'])
if INCLUDE_ACCOUNT:
 for log in root.glob('logs/*/gmserv.log'):
  for line in log.read_text(encoding='utf8',errors='replace').splitlines():
   try:row=json.loads(line)
   except Exception:continue
   m=re.search(r'build AccountChannelRemoteV5 for account:\\s*([^,\\s]+)',row.get('msg',''))
   if m:ids.add(m.group(1))
if len(tokens)!=1 or (INCLUDE_ACCOUNT and len(ids)!=1):
 print('BT_GM_VALIDATION='+json.dumps({'status':'selection_required'}),flush=True);sys.exit(0)
request={'token':next(iter(tokens)), 'serv_addr':'127.0.0.1:7001',
 'account_id':next(iter(ids)) if INCLUDE_ACCOUNT else '', 'cases':CASES}
try:
 p=subprocess.run([sys.executable,'-c',WORKER],input=json.dumps(request).encode(),capture_output=True,timeout=40)
 lines=p.stdout.decode('utf8',errors='replace').splitlines()
 out=[line for line in lines if line.startswith('BT_GM_VALIDATION=')]
 result=out[-1] if p.returncode==0 and out else 'BT_GM_VALIDATION='+json.dumps(
  {'status':'worker_no_result'})
 print(result,flush=True)
except subprocess.TimeoutExpired:
 print('BT_GM_VALIDATION='+json.dumps({'status':'timeout'}),flush=True)
""".replace("INCLUDE_ACCOUNT", repr(include_account))
        .replace("CASES", repr(cases))
        .replace("WORKER", repr(worker_source))
    )


def fetch_gm(args, cases):
    source = (
        Path(__file__).resolve().parents[1] / "bullet_trade/integrations/gm/validation_worker.py"
    ).read_text()
    results = {}
    for offset in range(0, len(cases), 10):
        batch = cases[offset : offset + 10]
        queries = [{"id": c["id"], **c["gm"]} for c in batch]
        code = remote_bootstrap(source, queries, any(c["id"] == "account" for c in batch))
        # 主机名与解释器由用户显式选择；凭据始终留在 Windows 内存。
        command = 'set PYTHONDONTWRITEBYTECODE=1&& "' + args.gm_python + '" -'
        proc = subprocess.run(
            [
                "ssh",
                "-4",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=12",
                args.gm_ssh_host,
                command,
            ],
            input=code.encode(),
            capture_output=True,
            timeout=55,
        )
        output = proc.stdout.decode("utf8", errors="replace")
        lines = [line for line in output.splitlines() if line.startswith("BT_GM_VALIDATION=")]
        if proc.returncode != 0 or not lines:
            raise RuntimeError("GM远端进程失败")
        data = json.loads(lines[-1].split("=", 1)[1])
        if "cases" not in data:
            raise RuntimeError("GM批次未完成")
        results.update(data["cases"])
        print(
            json.dumps(
                {"stage": "gm_batch", "completed": len(results), "sdk_version": data["sdk_version"]}
            ),
            flush=True,
        )
    return results


def finance_rows(rpc, table, code, date_field, start, end):
    """旧 RPC Finance 编译对象的有界兼容协议，仅支持固定分红表和受验证字面量。"""
    if table not in ("STK_XR_XD", "FUND_DIVIDEND") or date_field not in ("a_xr_date", "ex_date"):
        raise ValueError("非法分红表")
    gm_symbol(code if "." in code else code + ".XSHG")
    datetime.fromisoformat(start)
    datetime.fromisoformat(end)
    sql = (
        f"SELECT * FROM {table} WHERE code='{code}' "
        f"AND {date_field}>='{start}' AND {date_field}<='{end}' LIMIT 100"
    )
    compiled = SimpleNamespace(statement=text(sql), _limit=100, _offset=0)
    envelope = SimpleNamespace(
        statement=text(sql),
        _limit=100,
        _offset=0,
        limit=partial(getitem, {100: compiled, 3000: compiled, 5000: compiled}),
    )
    result = rpc.finance.run_query(envelope)
    if isinstance(result, Exception):
        raise result
    if not isinstance(result, pd.DataFrame) or not {"code", date_field}.issubset(result):
        raise TypeError("分红基准原表格式错误")
    if len(result) >= 100:
        raise ValueError("分红原表触及分页上限")
    if not result.code.eq(code).all():
        raise ValueError("分红原表证券不一致")
    return result


def run(args):
    """单个 RPC 连接顺序读取，逐案保存原始文件和脱敏报告。"""
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    cases = build_cases()
    if not args.account:
        cases = [c for c in cases if c["id"] != "account"]
    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "baseline": "JoinQuant RPC",
        "anchor": ANCHOR,
        "tolerances": {
            "price": "one exchange tick",
            "volume": 1.0,
            "money": "max(0.01,abs(reference)*1e-8)",
        },
        "cases": [],
        "not_covered": [
            "trading_writes",
            "nonempty_account_lifecycle",
            "full_fundamentals_industry_extras",
        ],
    }

    def save():
        (output / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf8"
        )

    rpc = None
    try:
        gm = fetch_gm(args, cases)
        (output / "gm_raw.json").write_text(
            json.dumps(gm, default=str, ensure_ascii=False, indent=2), encoding="utf8"
        )
        env = dotenv_values(args.rpc_env_file, interpolate=False)
        spec = importlib.util.spec_from_file_location("gm_parity_rpc", args.rpc_module)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            rpc = module.RPCProxy(
                env["JQ_PROXY_HOST"],
                int(env["JQ_PROXY_PORT"]),
                env["JQ_PROXY_AUTHKEY"],
                recv_timeout=12,
                max_retries=1,
            )
        for case in cases:
            name = case["id"]
            entry = {"id": name, "kind": case["kind"]}
            actual = gm[name]
            try:
                if actual["status"] != "ok":
                    entry.update(
                        ok=False,
                        status="gm_error",
                        error_type=actual.get("error_type"),
                        error_code=actual.get("error_code"),
                        reason=actual.get("reason"),
                    )
                elif case["kind"] == "bars":
                    with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(
                        io.StringIO()
                    ):
                        left = rpc.get_price(case["security"], **case["jq"])
                    if isinstance(left, Exception):
                        raise left
                    right = gm_bars(actual["data"], case["security"])
                    left.to_csv(output / (name + "_jq.csv"))
                    right.to_csv(output / (name + "_gm.csv"))
                    entry.update(
                        compare_bars(
                            left,
                            right,
                            0.001 if case["security"].startswith("51") else 0.01,
                            case.get("allow_empty", False),
                        )
                    )
                    entry.update(
                        security=case["security"], query=case["jq"], gm_query=case["gm"]["kwargs"]
                    )
                else:
                    with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(
                        io.StringIO()
                    ):
                        extra_result = compare_extra(name, actual["data"], rpc, output)
                    entry.update(extra_result)
            except Exception as exc:
                entry.update(
                    ok=False,
                    status=(
                        "baseline_blocked" if type(exc).__name__ == "StateException" else "error"
                    ),
                    error_type=type(exc).__name__,
                )
            report["cases"].append(entry)
            save()
            print(
                json.dumps(
                    {"id": name, "ok": entry["ok"], "status": entry.get("status", "compared")}
                ),
                flush=True,
            )
    except Exception as exc:
        report["fatal_error_type"] = type(exc).__name__
    finally:
        if rpc:
            rpc.close()
        report["passed"] = sum(c.get("ok", False) for c in report["cases"])
        report["total"] = len(cases)
        report["completed"] = len(report["cases"])
        report["ok"] = len(report["cases"]) == len(cases) and all(c["ok"] for c in report["cases"])
        save()
    return 0 if report["ok"] else 1


def compare_extra(name, data, rpc, output):
    if name == "calendar":
        reference = [
            str(pd.Timestamp(x).date())
            for x in rpc.get_trade_days(start_date="2026-09-21", end_date="2026-10-09")
        ]
        return {"ok": reference == data, "reference": reference, "observed": data}
    if name == "index_constituents":
        reference = rpc.get_index_stocks("000300.XSHG", date="2026-09-30")
        if isinstance(reference, Exception):
            raise reference
        observed = [jq_symbol(x["symbol"]) for x in data]
        return {
            "ok": len(observed) == len(set(observed)) and set(reference) == set(observed),
            "reference_rows": len(reference),
            "observed_rows": len(observed),
            "missing": sorted(set(reference) - set(observed)),
            "extra": sorted(set(observed) - set(reference)),
        }
    if name == "instruments":
        checks = []
        catalog = rpc.get_all_securities(
            types=["stock", "etf", "fund", "fja", "fjb"], date="2026-09-30"
        )
        if isinstance(catalog, Exception):
            raise catalog
        catalog.loc[catalog.index.isin([jq_symbol(x["symbol"]) for x in data])].to_csv(
            output / "instruments_jq.csv"
        )
        for x in data:
            security = jq_symbol(x["symbol"])
            info = catalog.loc[security]
            if isinstance(info, Exception):
                raise info
            checks.append(
                {
                    "security": security,
                    "start_date_matches": local_time(x["listed_date"]).date()
                    == pd.Timestamp(info["start_date"]).date(),
                    "name_matches": x["sec_name"] == info["display_name"],
                    "jq_type": info["type"],
                    "gm_type": x["sec_type"],
                }
            )
        # 厂商中文简称可以不同，保存差异；首次上市日期必须一致。
        return {
            "ok": len(data) == 4
            and len({x["symbol"] for x in data}) == 4
            and all(x["start_date_matches"] for x in checks),
            "checks": checks,
            "name_semantics": "reported_separately",
        }
    if name in ("stock_dividend", "transfer_dividend", "fund_dividend_legacy", "fund_dividend"):
        stock = name in ("stock_dividend", "transfer_dividend")
        sec = (
            "601318.XSHG" if name == "stock_dividend" else "300750.XSHE" if stock else "511880.XSHG"
        )
        first, last = (
            ("2024-07-01", "2024-10-31")
            if name == "stock_dividend"
            else ("2023-04-01", "2023-04-30") if stock else ("2024-12-01", "2024-12-31")
        )
        frame = finance_rows(
            rpc,
            "STK_XR_XD" if stock else "FUND_DIVIDEND",
            sec if stock else sec[:6],
            "a_xr_date" if stock else "ex_date",
            first,
            last,
        )
        frame.to_csv(output / (name + "_jq_finance.csv"), index=False)
        ref = []
        for row in frame.to_dict("records"):
            if stock:

                def ratio(field, number):
                    value = row.get(field)
                    if value is not None and pd.notna(value):
                        return float(value)
                    # 聚宽 number 常为送转股数，不能直接解释为每十股比例。
                    if row.get(number) not in (None, 0) and pd.notna(row.get(number)):
                        raise NotImplementedError("缺少送转比例字段")
                    return 0.0

                ref.append(
                    {
                        "date": local_time(row["a_xr_date"]).date().isoformat(),
                        "cash_per_share": float(row["bonus_ratio_rmb"]) / 10,
                        "scale_factor": 1
                        + (
                            ratio("dividend_ratio", "dividend_number")
                            + ratio("transfer_ratio", "transfer_number")
                        )
                        / 10,
                    }
                )
            else:
                ref.append(
                    {
                        "date": local_time(row["ex_date"]).date().isoformat(),
                        "cash_per_share": float(row["proportion"]),
                        "scale_factor": 1.0,
                    }
                )
        if name == "fund_dividend":
            return {"ok": False, "status": "fund_field_units_not_yet_accepted", "reference": ref}
        obs = legacy_dividends(data, sec)
        return {**compare_dividends(ref, obs), "reference": ref, "observed": obs}
    if name == "account":
        if data["status"] != "ok":
            return {"ok": False, "status": data["status"]}
        return validate_account(
            data["connection"], data["cash"], data["positions"], data["queries"]
        )
    if name == "current":
        checks = []
        for row in data:
            sec = jq_symbol(row["symbol"])
            ticks = rpc.get_ticks(
                sec, end_dt="2026-09-30 23:59:59", count=1, fields=["time", "current"]
            )
            if isinstance(ticks, Exception):
                raise ticks
            frame = pd.DataFrame(ticks)
            price = float(frame.current.iloc[-1])
            checks.append(
                {
                    "security": sec,
                    "gm_time": str(row["created_at"]),
                    "reference_price": price,
                    "observed_price": row["price"],
                    "ok": abs(price - row["price"]) <= 1e-8,
                }
            )
        return {
            "ok": len(checks) == 2 and all(x["ok"] for x in checks),
            "checks": checks,
            "freshness": "holiday_last_session_snapshot",
        }
    if name == "multi_symbol_raw":
        expected = {"510300.XSHG", "000001.XSHE"}
        symbols = {jq_symbol(row["symbol"]) for row in data}
        comparisons = {}
        for sec in sorted(expected):
            reference = rpc.get_price(
                sec,
                start_date="2026-09-28",
                end_date="2026-09-30",
                frequency="daily",
                fields=list(BAR_FIELDS),
                fq=None,
                skip_paused=True,
            )
            if isinstance(reference, Exception):
                raise reference
            observed = gm_bars([row for row in data if row["symbol"] == gm_symbol(sec)], sec)
            reference.to_csv(output / (name + "_" + sec + "_jq.csv"))
            observed.to_csv(output / (name + "_" + sec + "_gm.csv"))
            comparisons[sec] = compare_bars(
                reference, observed, 0.001 if sec.startswith("51") else 0.01
            )
        return {
            "ok": symbols == expected and all(v["ok"] for v in comparisons.values()),
            "comparisons": comparisons,
        }
    if name in ("historical_tick", "historical_tick_all"):
        reference = pd.DataFrame(
            rpc.get_ticks(
                "510300.XSHG",
                end_dt="2026-09-30 15:00:00",
                count=1,
                fields=["time", "current", "volume", "money"],
                skip=(name == "historical_tick"),
            )
        )
        reference.to_csv(output / (name + "_jq.csv"), index=False)
        if len(reference) != 1 or len(data) != 1 or data[0]["symbol"] != "SHSE.510300":
            return {"ok": False, "status": "tick_row_or_symbol_mismatch"}
        left = reference.iloc[0]
        right = data[0]
        checks = {
            "price": abs(float(left["current"]) - right["price"]) <= 0.001,
            "volume": abs(float(left["volume"]) - right["cum_volume"]) <= 1,
            "money": abs(float(left["money"]) - right["cum_amount"])
            <= max(0.01, abs(float(left["money"])) * 1e-8),
        }
        reference_time = jq_tick_time(left["time"])
        observed_time = local_time(right["created_at"])
        delta = abs((reference_time - observed_time).total_seconds())
        checks["time_precision_one_second"] = delta <= 1.0
        return {
            "ok": all(checks.values()),
            "checks": checks,
            "time_difference_seconds": delta,
            "reference_time": str(reference_time),
            "observed_time": str(observed_time),
            "reference_skip": name == "historical_tick",
        }
    if name == "stock_factor":
        reference = rpc.get_price(
            "601318.XSHG",
            start_date="2024-07-24",
            end_date="2024-07-30",
            frequency="daily",
            fields=["factor"],
            fq="pre",
        )
        if isinstance(reference, Exception):
            raise reference
        reference.to_csv(output / "stock_factor_jq.csv")
        observed = pd.DataFrame(data).set_index("trade_date")
        observed.index = pd.DatetimeIndex([local_time(x) for x in observed.index])
        good = reference.index.equals(observed.index) and not observed.index.has_duplicates
        diff = (
            (reference.factor - observed.adj_factor_fwd).abs() if good else pd.Series(dtype=float)
        )
        return {
            "ok": bool(good and len(diff) and np.isfinite(diff).all() and (diff <= 1e-8).all()),
            "max_abs_diff": float(diff.max()) if len(diff) else None,
        }
    if name == "historical_instruments":
        reference = rpc.get_price(
            "601318.XSHG",
            start_date="2026-09-28",
            end_date="2026-09-30",
            frequency="daily",
            fields=["high_limit", "low_limit", "paused"],
            fq=None,
        )
        if isinstance(reference, Exception):
            raise reference
        reference.to_csv(output / "historical_instruments_jq.csv")
        observed = pd.DataFrame(data).set_index("trade_date")
        observed.index = pd.DatetimeIndex([local_time(x) for x in observed.index])
        index_ok = reference.index.equals(observed.index) and not observed.index.has_duplicates
        checks = {
            j: bool(
                index_ok
                and np.isfinite(reference[j]).all()
                and np.isfinite(observed[g]).all()
                and ((reference[j] - observed[g]).abs() <= tol).all()
            )
            for j, g, tol in [
                ("high_limit", "upper_limit", 0.01),
                ("low_limit", "lower_limit", 0.01),
                ("paused", "is_suspended", 0),
            ]
        }
        return {"ok": all(checks.values()), "checks": checks}
    if name == "fund_factor":
        return {"ok": False, "status": "fund_factor_unaccepted", "rows": len(data)}
    return {"ok": False, "status": "missing_comparison"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-env-file", required=True)
    parser.add_argument("--rpc-module", required=True)
    parser.add_argument("--gm-ssh-host", required=True)
    parser.add_argument("--gm-python", required=True)
    parser.add_argument("--account", action="store_true", help="显式查询唯一 RemoteV5 仿真候选账户")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    # RPC 初始化握手也必须有上限，不能只依赖 recv_timeout。
    def timeout_handler(*_):
        raise TimeoutError("验收总时限")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(600)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
