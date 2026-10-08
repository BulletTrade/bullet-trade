#!/usr/bin/env python
"""按已授权的实用容差复核保存的 GM/JQ 结果；不联网、不修改原严格报告。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from bullet_trade.integrations.gm.acceptance import (
    PRACTICAL_RULES,
    compare_practical,
    normalize_post,
)
from bullet_trade.integrations.gm.validation import gm_symbol, local_time

WINDOW_SECURITIES = {
    "recent_etf": "510300.XSHG",
    "recent_bank": "000001.XSHE",
    "stock_cash_july": "601318.XSHG",
    "stock_cash_october": "601318.XSHG",
    "stock_transfer": "300750.XSHE",
    "fund_year_end": "511880.XSHG",
    "etf_distribution": "510300.XSHG",
    "index": "000300.XSHG",
    "minute_etf": "510300.XSHG",
    "minute_bank": "000001.XSHE",
    "fields": "601318.XSHG",
}


def sdk_anchor(sdk: dict, security: str, day: str) -> tuple[float, float]:
    """从已有 GM 官方元信息和历史状态读取刻度/因子，不从价格比例拟合。"""
    symbol = gm_symbol(security)
    ticks, factors = set(), set()
    index = False
    for key, rows in sdk.items():
        method, _ = json.loads(key)
        if method == "get_instrumentinfos":
            for row in rows:
                if row.get("symbol") == symbol:
                    ticks.add(float(row["price_tick"]))
                    index = int(row["sec_type"]) == 3
        elif method == "get_history_instruments":
            for row in rows:
                if (
                    row.get("symbol") == symbol
                    and local_time(row["trade_date"]).date().isoformat() == day
                ):
                    if row.get("adj_factor") is not None:
                        factors.add(float(row["adj_factor"]))
    if len(ticks) != 1 or (not index and len(factors) != 1):
        raise ValueError("缺少唯一 GM 原始刻度/参考因子")
    return ticks.pop(), 1.0 if index else factors.pop()


def review_report(source_dir: Path, factor_path: Path) -> dict:
    """原记录保持只读，所有小差异和共同基准归一化的依据写入新报告。"""
    source = source_dir / "report.json"
    raw = json.loads(source.read_text())
    if raw.get("runtime_data_sources") != ["gm"] or raw.get("alignment_mode") != "native":
        raise ValueError("只能验收独立 GM 原始报告")
    provider_source = Path(__file__).resolve().parents[1] / "bullet_trade/data/providers/gm.py"
    if (
        raw.get("provider_source_sha256")
        != hashlib.sha256(provider_source.read_bytes()).hexdigest()
    ):
        raise ValueError("采集时的 GM Provider 与当前源文件不一致")
    ids = [c["id"] for c in raw["cases"]]
    if not ids or len(set(ids)) != len(ids) or raw.get("total") != len(ids):
        raise ValueError("验收案例缺失或重复")
    if any(set(c.get("field_sources", {}).values()) - {"gm"} for c in raw["cases"]):
        raise ValueError("案例中存在 GM 之外的运行数据来源")
    sdk = json.loads((source_dir / "sdk_reads.json").read_text())
    factors = json.loads(factor_path.read_text())
    report = dict(raw)
    report.update(
        acceptance_policy="gm_practical_v1",
        policy_rules=PRACTICAL_RULES,
        evidence_scope="saved live captures reviewed offline; no new network collection",
        original_report_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        benchmark_factor_sha256=hashlib.sha256(factor_path.read_bytes()).hexdigest(),
        cases=[],
    )
    for case in raw["cases"]:
        entry = dict(case, strict_ok=case["ok"])
        name = case["id"]
        actual_path, reference_path = (
            source_dir / (name + "_adapter.csv"),
            source_dir / (name + "_jq.csv"),
        )
        numeric_case = "comparison" in case or "difference" in case
        if not numeric_case:
            # 元信息、日历、成份、tick 等沿用严格检查，不豁免任何失败。
            compared = {"ok": case["ok"], "rule": "original_strict_non_numeric_contract"}
        elif not actual_path.exists() or not reference_path.exists():
            compared = {"ok": False, "reason": "missing_captured_tables"}
        else:
            actual = pd.read_csv(actual_path, index_col=0)
            reference = pd.read_csv(reference_path, index_col=0)
            if name.startswith("fields") or name.startswith("bars_"):
                security = "601318.XSHG"
            elif name in {"paused_fill", "paused_skip", "minute_halt"}:
                security = "000002.XSHE"
            elif name == "holiday_empty":
                security = "510300.XSHG"
            elif name == "count_raw":
                security = "601318.XSHG"
            elif name.startswith("exact_start") or name == "minute_fields":
                security = "000001.XSHE"
            else:
                security = WINDOW_SECURITIES[name.rsplit("_", 1)[0]]
            try:
                if name.endswith("_post"):
                    if not len(actual) or not len(reference):
                        raise ValueError("后复权缺少共同参考日")
                    day = local_time(reference.index[0]).date().isoformat()
                    if local_time(actual.index[0]).date().isoformat() != day:
                        raise ValueError("后复权起点不一致")
                    tick, gm_factor = sdk_anchor(sdk, security, day)
                    if security == "000300.XSHG":
                        jq_factor = 1.0
                    else:
                        matches = [r["factor"] for r in factors[security] if r["date"] == day]
                        if len(matches) != 1:
                            raise ValueError("缺少唯一基准侧参考因子")
                        jq_factor = float(matches[0])
                    actual, reference = (
                        normalize_post(actual, gm_factor),
                        normalize_post(reference, jq_factor),
                    )
                    entry["common_basis"] = dict(
                        date=day,
                        gm_factor=gm_factor,
                        benchmark_factor=jq_factor,
                        gm_factor_source="captured GM get_history_instruments",
                        benchmark_factor_source="captured JoinQuant output; comparison only",
                        method=(
                            "divide each source's prices/factor; "
                            "multiply volume by its own factor"
                        ),
                    )
                else:
                    # 未复权/前复权不作尺度变换。
                    day = (
                        local_time(reference.index[0]).date().isoformat() if len(reference) else ""
                    )
                    symbol = gm_symbol(security)
                    ticks = {
                        float(r["price_tick"])
                        for k, rows in sdk.items()
                        if json.loads(k)[0] == "get_instrumentinfos"
                        for r in rows
                        if r.get("symbol") == symbol
                    }
                    if len(ticks) != 1:
                        raise ValueError("缺少唯一报价刻度")
                    tick = ticks.pop()
                compared = compare_practical(
                    reference, actual, tick, allow_empty=name == "holiday_empty"
                )
            except (KeyError, ValueError, TypeError):
                compared = {"ok": False, "reason": "invalid_or_missing_basis_evidence"}
        entry.update(practical_ok=compared["ok"], ok=compared["ok"], practical_comparison=compared)
        report["cases"].append(entry)
    report["strict_passed"] = sum(c["strict_ok"] for c in report["cases"])
    report["passed"] = sum(c["practical_ok"] for c in report["cases"])
    report["total"] = len(report["cases"])
    report["ok"] = report["passed"] == report["total"]
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--benchmark-factors", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.resolve() == (args.source_dir / "report.json").resolve():
        parser.error("不能覆盖原始严格报告")
    report = review_report(args.source_dir, args.benchmark_factors)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("passed", "strict_passed", "total", "ok")}))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
