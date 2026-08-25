# 此脚本实现tushare接口数据持久化到duckdb
1. **tushare.initdb.sql** duckdb数据的初始化schema
- 用 SQL 初始化一个全新数据库
```bash
duckdb ./duckdb/tushare.duckdb < tushare.initdb.sql
```
或 Python
```bash
python -c "import duckdb; duckdb.connect('x.duckdb').execute(open('tushare.initdb.sql').read())"
```

2. **sync_table.py**、**tasks_sync.json**配置需要同步的接口及执行同步操作，执行前需要设置环境变量“TUSHARE_TOKEN”
```bash
TUSHARE_TOKEN=your_token python sync_table.py --tasks-file tasks_sync.json --duckdb-path ./duckdb/tushare.duckdb
```
3. **check_quality.py** 用于检查持久化的数据质量
4. **test_query_performance.py** 用于测试查询duckdb数据性能（日行情数据，1000次随机查询，约耗时3秒+）

5. 核心同步代码使用tushare_duckdb_sync_skills生成
https://github.com/shadowinlife/nano_quant_skills/tree/main/tushare_to_duckdb/tushare_duckdb_sync_skills