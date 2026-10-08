"""验收明确选定的实测报告；先运行 probe_gm_parity.py，失败与阻塞均不得 skip。

本文件不启动第二组网络查询，不改变已有账号；保留报告原始采集时间，读取报告
通过不表示重新采集，也不把离线单元测试的绿色结果当作在线接口通过。
"""

import importlib.util
import json
from pathlib import Path

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.requires_network]
_spec = importlib.util.spec_from_file_location(
    "gm_acceptance_matrix", Path(__file__).resolve().parents[3] / "scripts/probe_gm_parity.py"
)
_matrix = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_matrix)
EXPECTED = [c["id"] for c in _matrix.build_cases()]


@pytest.fixture(scope="module")
def report(request):
    path = request.config.getoption("--gm-parity-report")
    if not path:
        pytest.fail("需要 --gm-parity-report；先运行明确授权的只读采集脚本")
    result = json.loads(Path(path).read_text(encoding="utf8"))
    assert result["baseline"] == "JoinQuant RPC"
    assert result["generated_at"]
    assert result["completed"] == len(EXPECTED)
    assert {c["id"] for c in result["cases"]} == set(EXPECTED)
    assert len(result["cases"]) == len(EXPECTED)
    return {c["id"]: c for c in result["cases"]}


@pytest.mark.parametrize("case_id", EXPECTED)
def test_gm_matches_rpc_contract(report, case_id):
    case = report[case_id]
    assert case["ok"], json.dumps(case, ensure_ascii=False)
