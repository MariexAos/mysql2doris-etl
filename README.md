# mysql2doris-etl

MySQL → Apache Doris 手动 ETL 工具集，纯 Shell + Python 3 实现，无需额外依赖。

## 工具列表

| 工具 | 说明 |
|------|------|
| `mysql2doris.py` | MySQL DDL → Doris DDL 批量转换 |
| `scripts/doris_load.sh` | Doris Stream Load 批量 CSV 导入 |
| `scripts/decompress.sh` | `.csv.gz` 批量解压 |

---

## mysql2doris.py

将 MySQL 的 `CREATE TABLE` DDL 批量转换为 Apache Doris 兼容语法。

### 快速开始

```bash
# 转换并输出到文件
python3 mysql2doris.py input.sql -o output.sql

# 转换并打印到终端
python3 mysql2doris.py input.sql
```

### 参数

| 参数 | 必填 | 说明 |
|------|------|------|
| `input` | 是 | 输入的 MySQL SQL 文件路径 |
| `-o, --output` | 否 | 输出文件路径，不指定则打印到 stdout |
| `-b, --buckets` | 否 | 默认分桶数（默认 8） |

### 转换规则

**数据模型**
- 所有表统一使用 `DUPLICATE KEY` 模型
- KEY 列取自原 MySQL `PRIMARY KEY`，无主键时取第一列
- KEY 列自动排到列定义最前面（Doris 要求）
- 分桶使用 `BUCKETS AUTO`

**类型映射**

| MySQL | Doris | 说明 |
|-------|-------|------|
| `INT(N)` / `BIGINT(N)` | `INT` / `BIGINT` | 去掉显示宽度 |
| `TINYINT(N)` / `SMALLINT(N)` | `TINYINT` / `SMALLINT` | 同上 |
| `MEDIUMINT(N)` | `INT` | Doris 无 MEDIUMINT |
| `TEXT` / `LONGTEXT` / `MEDIUMTEXT` | `STRING` | Doris 用 STRING 代替 |
| `CHAR(N)` | `VARCHAR(N)` | 避免 Doris CHAR 的定长差异 |
| `DECIMAL(P,S)` | `DECIMAL(P,S)` | 精度超 38 自动截断 |
| `DATE` / `DATETIME` | `DATE` / `DATETIME` | 直接兼容 |

**去除的 MySQL 语法**：`COLLATE`、`ENGINE=InnoDB`、`ROW_FORMAT`、`AUTO_INCREMENT`、`USING BTREE`、`UNIQUE INDEX`/`INDEX`（作为注释保留）

### 输出示例

```sql
CREATE TABLE IF NOT EXISTS `orders` (
    `order_id` BIGINT NOT NULL COMMENT '订单ID',
    `user_id`  BIGINT NOT NULL,
    `amount`   DECIMAL(18, 2) NULL DEFAULT NULL
)
DUPLICATE KEY (`order_id`)
DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO;
```

---

## scripts/doris_load.sh

通过 Doris HTTP Stream Load 接口将 CSV 文件批量导入 Doris。

### 快速开始

```bash
chmod +x scripts/doris_load.sh

# 最简（全用默认值）
./scripts/doris_load.sh

# 指定目录和库名
./scripts/doris_load.sh -d /data/csv -D mydb

# 全部指定，含 enclose/escape
./scripts/doris_load.sh \
    -H 10.0.0.1 -P 8030 -D mydb \
    -u admin -p 'MyPass!' \
    -d /data/csv -l /data/logs \
    -s '|+|' --skip-lines 1 \
    --enclose '"' --escape '\'
```

### 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-H, --host` | `127.0.0.1` | FE 地址 |
| `-P, --port` | `8030` | FE HTTP 端口 |
| `-D, --db` | `audit_db` | 目标数据库名 |
| `-u, --user` | `root` | 用户名 |
| `-p, --password` | `password` | 密码 |
| `-d, --csv-dir` | `/data/csv` | CSV 文件目录 |
| `-l, --log-dir` | `/data/logs` | 日志目录 |
| `-s, --separator` | `\|+\|` | 字段分隔符 |
| `--skip-lines` | `1` | 跳过首行数 |
| `--enclose` | _(不启用)_ | 字段包围符，如 `"` |
| `--escape` | _(不启用)_ | 转义字符，如 `\` |

### 分表合并导入

当数据库分表后导出为多个 CSV 时，脚本会自动识别 `表名_N.csv` 格式并将所有分片导入同一张目标表：

```
/data/csv/
  orders_1.csv   ┐
  orders_2.csv   ├─→  导入到 orders 表
  orders_3.csv   ┘
  users.csv      ──→  导入到 users 表
```

规则：文件名末尾若为 `_<纯数字>`，自动剥离后缀作为目标表名；否则文件名即表名。

每张目标表的所有分片日志追加写入同一个 `<table>.log` 文件，便于排查。

### 日志格式

```
--- [2025-01-01 12:00:00] 文件: orders_1.csv ---
{"TxnId":1001,"Status":"Success","NumberLoadedRows":50000,...}

--- [2025-01-01 12:00:05] 文件: orders_2.csv ---
{"TxnId":1002,"Status":"Success","NumberLoadedRows":48000,...}
```

---

## scripts/decompress.sh

批量解压 `.csv.gz` 文件。

```bash
chmod +x scripts/decompress.sh

# 解压到当前目录
./scripts/decompress.sh /data/compressed

# 解压到指定目录
./scripts/decompress.sh /data/compressed /data/csv
```

---

## 典型 ETL 流程

```bash
# 1. 转换 DDL（在 Doris 中建表）
python3 mysql2doris.py schema.sql -o doris_schema.sql
# 手动在 Doris 执行 doris_schema.sql

# 2. 解压数据文件（如有 .gz）
./scripts/decompress.sh /data/compressed /data/csv

# 3. 批量导入
./scripts/doris_load.sh \
    -H doris-fe.example.com \
    -D mydb \
    -u admin -p 'password' \
    -d /data/csv
```

---

## 环境要求

- Python 3.6+（仅标准库）
- Bash 4.0+
- curl
- Apache Doris ≥ 1.2.2（`BUCKETS AUTO` 支持）
