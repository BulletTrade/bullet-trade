<#
作者: BruceLee
文件职责: 在当前 Windows clone 内创建独立环境并安装 BulletTrade 与掘金 SDK。
主要输入: 已安装 Python、虚拟环境目录、gm 版本及是否显式检查 SDK 导入。
主要输出: .venv-gm 环境、pip 检查结果与 gm doctor 诊断。
上下游关系: 用户手动执行本脚本；调用 pip 和 BulletTrade CLI。
关键配置: 不安装或启动终端，不读取凭据，不连接账户或执行交易。
#>
[CmdletBinding()]
param(
    [string]$PythonExe = "py",
    [string]$VenvDirectory = ".venv-gm",
    [ValidatePattern('^\d+\.\d+\.\d+$')]
    [string]$SdkVersion = "3.0.187",
    [switch]$LoadSdk
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonArguments = @()
if ([IO.Path]::GetFileNameWithoutExtension($PythonExe) -eq "py") {
    $pythonArguments = @("-3.11")
}

# 先确认已有解释器；不修改系统 Python 或系统 PATH。
& $PythonExe @pythonArguments -c "import platform, struct, sys; print(sys.version); sys.exit(0 if platform.system() == 'Windows' and struct.calcsize('P') == 8 and (3,8) <= sys.version_info[:2] <= (3,14) else 2)"
if ($LASTEXITCODE -ne 0) {
    throw "需要 Windows x64 Python 3.8-3.14，推荐 3.11；可用 -PythonExe 指定解释器。"
}
if ([version]$SdkVersion -lt [version]"3.0.186" -or [version]$SdkVersion -ge [version]"3.1.0") {
    throw "当前基础接入支持 gm >=3.0.186,<3.1；券商指定的其他版本需要先核实。"
}

if ([IO.Path]::IsPathRooted($VenvDirectory)) {
    $venvPath = $VenvDirectory
} else {
    $venvPath = Join-Path $repoRoot $VenvDirectory
}
$venvPython = Join-Path $venvPath "Scripts\python.exe"
if (Test-Path $venvPath) {
    if (!(Test-Path $venvPython)) {
        throw "目标目录已存在且不是可用虚拟环境，请选择新的 -VenvDirectory。"
    }
} else {
    & $PythonExe @pythonArguments -m venv $venvPath
    if ($LASTEXITCODE -ne 0) { throw "创建虚拟环境失败。" }
}

# 同样检查复用的虚拟环境，避免把依赖装到不匹配的旧环境。
& $venvPython -c "import platform, struct, sys; sys.exit(0 if platform.system() == 'Windows' and struct.calcsize('P') == 8 and (3,8) <= sys.version_info[:2] <= (3,14) else 2)"
if ($LASTEXITCODE -ne 0) { throw "目标虚拟环境的 Python/架构不匹配。" }

& $venvPython -m pip install -e "${repoRoot}[gm]" "gm==$SdkVersion"
if ($LASTEXITCODE -ne 0) { throw "依赖安装失败，请检查 pip 输出。" }
& $venvPython -m pip check
if ($LASTEXITCODE -ne 0) { throw "依赖检查失败。" }
& $venvPython -m bullet_trade --version
if ($LASTEXITCODE -ne 0) { throw "BulletTrade CLI 不可用。" }

$doctorArguments = @("-m", "bullet_trade", "gm", "doctor")
if ($LoadSdk) { $doctorArguments += "--load-sdk" }
& $venvPython @doctorArguments
if ($LASTEXITCODE -ne 0) { throw "掘金环境诊断未通过，请查看诊断 JSON。" }
Write-Host "环境已准备：$venvPython"
Write-Host "终端登录、行情查询和交易账户连接尚未验证。"
