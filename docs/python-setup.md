# 环境准备：先安装 Python，再创建虚拟环境

这篇文档是两个方案共用的前置步骤。  
不管你最后走：

- [方案 A：策略在 BulletTrade 本地直接运行](beginner-route-a.md)
- [方案 B：策略继续在聚宽侧运行，BulletTrade 负责接收信号并在本地 QMT 执行](beginner-route-b.md)

都建议先把这一页做完。

> 先记住一句话：  
> **只有当 `python --version` 和 `pip --version` 都能正常输出时，才开始执行 `python -m venv .venv`。**

## 1. 先确认你要在哪台机器上装 Python

### 方案 A

Python 要装在运行 BulletTrade 的那台机器上。

- 如果策略和 QMT 在同一台 Windows 机器，就装在这台 Windows 机器
- 如果策略跑在另一台机器，通过 `qmt-remote` 连 QMT，那就装在策略所在机器

### 方案 B

Python 至少要装在 **QMT 所在的 Windows 机器** 上。  
因为这台机器要运行：

- `bullet-trade server`
- helper 联调时可能会用到的 Jupyter / Notebook

## 2. 去哪里下载 Python

推荐直接从 Python 官网下载，不建议新手第一步就混用多个 Python 发行版。

- Windows: [python.org/downloads/windows](https://www.python.org/downloads/windows/)
- macOS: [python.org/downloads/macos](https://www.python.org/downloads/macos/)
- Linux: [python.org/downloads/source](https://www.python.org/downloads/source/)  
  Linux 更常见的是直接用系统包管理器安装，但如果你不熟，先让熟悉 Linux 的同事协助。

### Windows 推荐版本

建议优先安装：

- Python 3.10
- 或 Python 3.11

对于新手，优先选择：

- `Windows installer (64-bit)`

## 3. Windows 安装时一定注意这一步

打开安装器后，先看底部有没有这项：

- `Add python.exe to PATH`

**一定要勾上。**

否则安装完以后，命令行里可能找不到 `python`。

然后再点：

- `Install Now`

## 4. 安装完成后，先不要急着建虚拟环境

先打开命令行验证。

Windows 可以用：

- `cmd`
- PowerShell
- Anaconda Prompt

先执行：

```bash
python --version
```

如果输出类似下面这样，就说明 Python 命令已经可用：

```bash
Python 3.11.9
```

再执行：

```bash
pip --version
```

如果也能正常输出版本号，就说明 `pip` 也可用。

## 5. 如果 `python` 不可用怎么办

先试这几个排查动作。

### 情况 1：`python` 提示“不是内部或外部命令”

优先检查：

- 安装时是不是没有勾 `Add python.exe to PATH`
- 是否需要重新打开一个新的命令行窗口

### 情况 2：`python` 不行，但 `py` 可以

Windows 上有时是这样：

```bash
py --version
```

如果 `py` 能用，也可以这样创建虚拟环境：

```bash
py -m venv .venv
```

### 情况 3：装了很多 Python，自己也不知道哪个在生效

这时候先不要继续装依赖。  
先把命令统一清楚，再继续。

Windows 可以检查：

```bash
where python
where pip
```

如果结果看起来很乱，建议先找一个最确定的终端重新开始，比如：

- 新开的 `cmd`
- 新开的 PowerShell

## 6. 验证通过后，再创建虚拟环境

进入你准备放项目的目录，再执行：

```bash
python -m venv .venv
```

如果你用的是 Windows 的 `py`：

```bash
py -m venv .venv
```

## 7. 激活虚拟环境

### Windows

```bash
.venv\Scripts\activate
```

激活成功后，命令行前面通常会出现：

```bash
(.venv)
```

### macOS / Linux

```bash
source .venv/bin/activate
```

## 8. 然后再安装 BulletTrade

建议先升级一下 `pip`：

```bash
python -m pip install --upgrade pip
```

然后按你的场景安装。

### 情况 1：只是先做回测、JQData 联调，或者只是作为 `qmt-remote` 客户端

这种情况先装基础版就够了：

```bash
pip install bullet-trade
```

### 情况 2：这台机器要直接连本地 QMT，或者这台机器要启动 `bullet-trade server`

这种情况这台机器需要 `xtquant` 支持。  
建议直接安装带 `qmt` 扩展的版本：

```bash
pip install "bullet-trade[qmt]"
```

你可以把它理解成：

- `pip install bullet-trade`：安装基础能力
- `pip install "bullet-trade[qmt]"`：额外把 QMT / `xtquant` 相关依赖也装上

最常见需要装 `qmt` 扩展的机器有两类：

- 装了 QMT、要本地直接下单的 Windows 机器
- 装了 QMT、要运行 `bullet-trade server` 的 Windows 机器

### 如果 `qmt` 扩展没装，会发生什么

在本地 QMT 或 QMT server 场景里，如果这里没有装 `qmt` 扩展，常见结果就是：

- 能安装 `bullet-trade`
- 但一到 QMT 取数或下单时，才报缺少 `xtquant`

如果你已经装了基础版，也不用重来，直接再补装一次即可：

```bash
pip install "bullet-trade[qmt]"
```

如果本机环境特殊，也可以单独安装：

```bash
pip install xtquant
```

但对新手来说，优先建议直接用：

```bash
pip install "bullet-trade[qmt]"
```

验证：

```bash
bullet-trade --help
bullet-trade --version
```

只要这两个命令都能正常输出，说明基础环境已经可以继续走后面的方案文档了。

<a id="env-file"></a>

## 9. 什么是 `.env` 文件，怎么创建

`.env` 不是什么特殊格式软件。  
对新手来说，你可以把它理解成：

- 一个普通的纯文本文件
- 文件名就叫 `.env`
- 通常放在你准备执行 `bullet-trade` 命令的当前目录
- 里面一行写一个配置，格式是 `变量名=值`

这里说的“当前目录”，就是你打开命令行后所在的文件夹。  
最简单的做法就是：**把 `.env` 放在你准备执行 `bullet-trade live ...` 或 `bullet-trade server ...` 命令的那个目录里。**

例如，下面这种内容就应该写进 `.env` 文件里，而不是直接粘到命令行里执行：

```env
QMT_ACCOUNT_ID=123456
QMT_SERVER_TOKEN=secret
```

### Windows 用记事本怎么创建 `.env`

1. 打开记事本。
2. 把文档里的配置内容粘进去。
3. 点击“文件 -> 另存为”。
4. “保存类型”不要选 `文本文档 (*.txt)`，要改成 `所有文件 (*.*)`。
5. “文件名”直接填写 `.env`。
6. 保存到你准备执行 `bullet-trade` 命令的那个文件夹。

### 如果你保存出来的是 `.env.txt`

这说明 Windows 还是把它当成了文本文件扩展名。  
请这样处理：

- 回到“另存为”，把“保存类型”改成 `所有文件 (*.*)`
- 文件名重新写成 `.env`
- 如果资源管理器里看不到扩展名，先打开“查看 -> 文件扩展名”

### 如果目录里已经有 `env.example`

更简单的做法是直接复制一份，再改名成 `.env`：

```bash
# macOS / Linux
cp env.example .env

# Windows cmd
copy env.example .env
```

## 10. 做完这一页后去哪里

- 如果你要本地直跑策略，看 [方案 A：策略在 BulletTrade 本地直接运行](beginner-route-a.md)
- 如果你要让策略继续在聚宽侧运行，看 [方案 B：策略继续在聚宽侧运行，BulletTrade 负责接收信号并在本地 QMT 执行](beginner-route-b.md)
