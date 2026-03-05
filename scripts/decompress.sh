#!/bin/bash

# 用法: ./decompress_csv.sh <源文件夹> [目标文件夹]
# 不指定目标文件夹则解压到源文件夹

SRC_DIR="${1:-.}"
DST_DIR="${2:-$SRC_DIR}"

mkdir -p "$DST_DIR"

count=0
fail=0

for f in "$SRC_DIR"/*.csv.gz; do
    [ -f "$f" ] || { echo "未找到 .csv.gz 文件"; exit 1; }
    
    filename=$(basename "$f" .gz)
    echo -n "解压: $(basename "$f") -> ${filename} ... "
    
    if gunzip -c "$f" > "$DST_DIR/$filename"; then
        echo "✅"
        ((count++))
    else
        echo "❌"
        ((fail++))
    fi
done

echo "完成: 成功 ${count} 个, 失败 ${fail} 个"
