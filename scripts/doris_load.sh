#!/bin/bash
# ============================================================
# Doris Stream Load 批量导入
#
# 用法:
#   ./doris_load.sh --csv-dir /data/csv --db audit_db
#   ./doris_load.sh -d /data/csv -D audit_db -H 10.0.0.1 -P 8030
#   ./doris_load.sh -d /data/csv -D mydb --enclose '"' --escape '\'
#
# 分表合并支持:
#   文件名格式 tablename_1.csv, tablename_2.csv ... 会自动合并导入同一个表 tablename
#   纯数字后缀 (_N) 会被识别并剥离，其余文件名直接作为表名
# ============================================================

# ── 默认值 ──────────────────────────────────────────────────
FE_HOST="127.0.0.1"
FE_PORT="8030"
DB="audit_db"
USER="root"
PASSWORD="password"
CSV_DIR="/data/csv"
LOG_DIR="/data/logs"
SEPARATOR="|+|"
SKIP_LINES=1
ENCLOSE=""      # 可选：字段包围符，如 '"'
ESCAPE=""       # 可选：转义字符，如 '\'

# ── 帮助函数 ─────────────────────────────────────────────────
usage() {
    cat <<EOF
用法: $0 [选项]

连接选项:
  -H, --host        FE 地址        (默认: 127.0.0.1)
  -P, --port        FE HTTP 端口   (默认: 8030)
  -D, --db          目标数据库     (默认: audit_db)
  -u, --user        用户名         (默认: root)
  -p, --password    密码           (默认: password)

数据选项:
  -d, --csv-dir     CSV 文件目录   (默认: /data/csv)
  -l, --log-dir     日志目录       (默认: /data/logs)
  -s, --separator   字段分隔符     (默认: |+|)
      --skip-lines  跳过首行数     (默认: 1)
      --enclose     字段包围符     (可选，如 '"'，不传则不启用)
      --escape      转义字符       (可选，如 '\'，不传则不启用)

其他:
  -h, --help        显示此帮助

分表合并说明:
  CSV 文件名若以 _N 结尾（N 为纯数字），则自动去除后缀作为目标表名。
  例: orders_1.csv, orders_2.csv, orders_3.csv → 全部导入 orders 表
  普通文件: orders.csv → 导入 orders 表
EOF
    exit 0
}

# ── 解析命名参数 ──────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -H|--host)       FE_HOST="$2";    shift 2 ;;
        -P|--port)       FE_PORT="$2";    shift 2 ;;
        -D|--db)         DB="$2";         shift 2 ;;
        -u|--user)       USER="$2";       shift 2 ;;
        -p|--password)   PASSWORD="$2";   shift 2 ;;
        -d|--csv-dir)    CSV_DIR="$2";    shift 2 ;;
        -l|--log-dir)    LOG_DIR="$2";    shift 2 ;;
        -s|--separator)  SEPARATOR="$2";  shift 2 ;;
        --skip-lines)    SKIP_LINES="$2"; shift 2 ;;
        --enclose)       ENCLOSE="$2";    shift 2 ;;
        --escape)        ESCAPE="$2";     shift 2 ;;
        -h|--help)       usage ;;
        *)
            echo "❌ 未知参数: $1，用 -h 查看帮助"
            exit 1 ;;
    esac
done

# ── 校验 ─────────────────────────────────────────────────────
if [ ! -d "$CSV_DIR" ]; then
    echo "❌ 错误: CSV 目录 $CSV_DIR 不存在"
    exit 1
fi

mkdir -p "${LOG_DIR}"

# ── 打印配置摘要 ──────────────────────────────────────────────
echo "=========================================="
echo "  FE:       ${FE_HOST}:${FE_PORT}"
echo "  目标库:   ${DB}"
echo "  CSV 目录: ${CSV_DIR}"
echo "  分隔符:   ${SEPARATOR}"
echo "  skip_lines: ${SKIP_LINES}"
[ -n "$ENCLOSE" ] && echo "  enclose:  ${ENCLOSE}"
[ -n "$ESCAPE"  ] && echo "  escape:   ${ESCAPE}"
echo "=========================================="

# ── 提取表名：剥离分表数字后缀 ───────────────────────────────
# 支持两种分表命名格式：
#   带下划线分隔：orders_1 / orders_10  → orders
#   数字直连:     user_info1 / user_info2 → user_info
#   非数字后缀保持不变：orders_abc       → orders_abc
#   无后缀:       orders               → orders
get_table_name() {
    local filename="$1"          # 已去掉 .csv 扩展名的文件名
    echo "${filename}" | sed -E 's/_?[0-9]+$//'
}

# ── 主循环 ───────────────────────────────────────────────────
success=0
fail=0

# 检查是否有 csv 文件
shopt -s nullglob
csv_files=("${CSV_DIR}"/*.csv)
shopt -u nullglob

if [ ${#csv_files[@]} -eq 0 ]; then
    echo "❌ 未在 ${CSV_DIR} 找到任何 .csv 文件"
    exit 1
fi

for csv_file in "${csv_files[@]}"; do
    [ -f "$csv_file" ] || continue

    filename=$(basename "$csv_file" .csv)
    table_name=$(get_table_name "$filename")

    # 如果是分表文件，标注来源分片
    if [ "$filename" != "$table_name" ]; then
        shard_info=" [分片: ${filename}]"
    else
        shard_info=""
    fi

    echo -n "[$(date '+%H:%M:%S')] ${table_name}${shard_info} ... "

    # 读取首行作为列名（用原始分隔符替换为逗号）
    header=$(head -1 "$csv_file" | sed "s/${SEPARATOR}/,/g")
    # 如果有 enclose，将包围符从列名中移除
    if [ -n "$ENCLOSE" ]; then
        header=$(echo "$header" | sed "s/${ENCLOSE}//g")
    fi

    # ── 构造可选 Header ──────────────────────────────────────
    extra_headers=()
    [ -n "$ENCLOSE" ] && extra_headers+=(-H "enclose: ${ENCLOSE}")
    [ -n "$ESCAPE"  ] && extra_headers+=(-H "escape: ${ESCAPE}")

    # ── 执行 Stream Load ─────────────────────────────────────
    result=$(curl --location-trusted -s -w "\n%{http_code}" \
        -u "${USER}:${PASSWORD}" \
        -H "format: csv" \
        -H "column_separator: ${SEPARATOR}" \
        -H "skip_lines: ${SKIP_LINES}" \
        -H "columns: ${header}" \
        "${extra_headers[@]}" \
        -T "$csv_file" \
        "http://${FE_HOST}:${FE_PORT}/api/${DB}/${table_name}/_stream_load")

    # 记录日志（追加，支持多分片写入同一日志）
    {
        echo "--- [$(date '+%Y-%m-%d %H:%M:%S')] 文件: $(basename "$csv_file") ---"
        echo "$result"
        echo ""
    } >> "${LOG_DIR}/${table_name}.log"

    http_code=$(echo "$result" | tail -1)
    body=$(echo "$result" | head -n -1)

    if echo "$body" | grep -q '"Status": "Success"'; then
        rows=$(echo "$body" | grep -o '"NumberLoadedRows":[^,}]*' | grep -o '[0-9]*' || echo "?")
        echo "✅  (加载行数: ${rows})"
        ((success++))
    else
        err_msg=$(echo "$body" | grep -o '"Message":"[^"]*"' | head -1 || echo "")
        echo "❌  HTTP ${http_code} ${err_msg} -> ${LOG_DIR}/${table_name}.log"
        ((fail++))
    fi
done

echo ""
echo "=========================================="
echo "  完成: ✅ 成功 ${success}  ❌ 失败 ${fail}"
echo "=========================================="
[ "$fail" -gt 0 ] && exit 1 || exit 0
