#!/usr/bin/env python3
"""
MySQL DDL → Apache Doris DDL 转换工具

转换规则：
1. 去除 COLLATE（带引号/不带引号）/ CHARACTER SET / CHARSET / ENGINE /
   ROW_FORMAT / AUTO_INCREMENT 等MySQL特有属性（关键字大小写不敏感）
2. 去除 ON UPDATE <expr>（如 ON UPDATE CURRENT_TIMESTAMP）
3. PRIMARY KEY / UNIQUE INDEX / INDEX 等索引语法转为 Doris KEY 模型
4. INT(N) / BIGINT(N) 去掉显示宽度
5. TEXT → STRING
6. CHAR → VARCHAR (Doris 的 CHAR 行为不同，统一用 VARCHAR 更安全)
7. 自动生成 DUPLICATE KEY(...) + DISTRIBUTED BY HASH(...) BUCKETS AUTO
8. 保留 COMMENT
9. 建表语句开头的行注释（-- ...）和块注释（/* ... */）会被自动忽略
"""

import re
import sys
from pathlib import Path


def convert_mysql_to_doris(sql_text: str, default_buckets: int = 8) -> str:
    """将整个SQL文件中的所有CREATE TABLE语句转换为Doris兼容语法"""

    # 统一换行符
    sql_text = sql_text.replace('\r\n', '\n').replace('\r', '\n')
    # 去除BOM
    sql_text = sql_text.lstrip('\ufeff')

    # 按分号拆分语句
    statements = re.split(r';\s*\n', sql_text)
    results = []

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        # 去除语句开头连续的行注释（-- ...）和块注释（/* ... */），再判断语句类型
        stmt_body = re.sub(r'^(\s*(--[^\n]*\n|/\*.*?\*/\s*))+', '', stmt,
                           flags=re.DOTALL).strip()
        if not stmt_body:
            # 纯注释块，原样保留（不加分号）
            results.append(stmt)
            continue
        if stmt_body.upper().startswith('CREATE TABLE'):
            converted = convert_single_table(stmt, default_buckets)
            results.append(converted)
        else:
            # 非建表语句原样保留
            results.append(stmt + ';')

    return '\n\n\n'.join(results)


def convert_single_table(stmt: str, default_buckets: int = 8) -> str:
    """转换单个 CREATE TABLE 语句"""

    # ========== 1. 提取表名 ==========
    # 先剥离语句头部的注释行，再匹配 CREATE TABLE
    stmt_for_parse = re.sub(r'^(\s*(--[^\n]*\n|/\*.*?\*/\s*))+', '', stmt,
                            flags=re.DOTALL).strip()
    m = re.match(r'CREATE\s+TABLE\s+`?(\w+)`?\s*\(', stmt_for_parse, re.IGNORECASE)
    if not m:
        return '-- [转换失败] ' + stmt
    table_name = m.group(1)

    # ========== 2. 提取列定义和约束部分 ==========
    # 找到最外层括号的内容（跳过单引号字符串内的括号）
    paren_start = stmt_for_parse.index('(')
    depth = 0
    paren_end = -1
    in_string = False
    for i in range(paren_start, len(stmt_for_parse)):
        ch = stmt_for_parse[i]
        if ch == "'" and not in_string:
            in_string = True
        elif ch == "'" and in_string:
            # 检查是否是转义的引号 ''
            if i + 1 < len(stmt_for_parse) and stmt_for_parse[i + 1] == "'":
                continue
            in_string = False
        elif not in_string:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    paren_end = i
                    break

    if paren_end == -1:
        return '-- [转换失败: 括号不匹配] ' + stmt

    body = stmt_for_parse[paren_start + 1:paren_end]
    tail = stmt_for_parse[paren_end + 1:].strip().rstrip(';').strip()

    # ========== 3. 提取表级COMMENT ==========
    table_comment = ''
    cm = re.search(r"COMMENT\s*=\s*'([^']*)'", tail, re.IGNORECASE)
    if cm:
        table_comment = cm.group(1)

    # ========== 4. 逐行解析列定义和约束 ==========
    lines = split_columns(body)

    columns = []        # (col_name, col_def_line)
    pk_cols = []        # PRIMARY KEY 列
    unique_indexes = [] # [(index_name, [cols])]
    normal_indexes = [] # [(index_name, [cols])]

    for line in lines:
        line = line.strip().rstrip(',').strip()
        if not line:
            continue

        # --- PRIMARY KEY ---
        pk_match = re.match(r'PRIMARY\s+KEY\s*\(([^)]+)\)', line, re.IGNORECASE)
        if pk_match:
            pk_cols = [c.strip().strip('`') for c in pk_match.group(1).split(',')]
            continue

        # --- UNIQUE INDEX / UNIQUE KEY ---
        ui_match = re.match(
            r'UNIQUE\s+(?:INDEX|KEY)\s+`?(\w+)`?\s*\(([^)]+)\)',
            line, re.IGNORECASE
        )
        if ui_match:
            idx_name = ui_match.group(1)
            idx_cols = [c.strip().strip('`') for c in ui_match.group(2).split(',')]
            unique_indexes.append((idx_name, idx_cols))
            continue

        # --- INDEX / KEY ---
        ni_match = re.match(
            r'(?:INDEX|KEY)\s+`?(\w+)`?\s*\(([^)]+)\)',
            line, re.IGNORECASE
        )
        if ni_match:
            idx_name = ni_match.group(1)
            idx_cols = [c.strip().strip('`') for c in ni_match.group(2).split(',')]
            normal_indexes.append((idx_name, idx_cols))
            continue

        # --- 普通列定义 ---
        col = parse_column(line)
        if col:
            columns.append(col)

    # ========== 5. 确定 Doris KEY 模型 ==========
    # 全部使用 DUPLICATE KEY 模型
    if pk_cols:
        key_cols = pk_cols
    else:
        key_cols = [columns[0][0]] if columns else []
    key_model = 'DUPLICATE KEY'

    # 分桶列：取 key 的第一列
    dist_col = key_cols[0] if key_cols else columns[0][0]

    # ========== 6. 对列排序：Doris 要求 KEY 列在前面 ==========
    key_col_set = set(key_cols)
    key_columns = []
    non_key_columns = []
    col_map = {name: defn for name, defn in columns}

    for kc in key_cols:
        if kc in col_map:
            key_columns.append((kc, col_map[kc]))

    for name, defn in columns:
        if name not in key_col_set:
            non_key_columns.append((name, defn))

    ordered_columns = key_columns + non_key_columns

    # ========== 7. 生成 Doris DDL ==========
    out_lines = []
    out_lines.append(f'CREATE TABLE IF NOT EXISTS `{table_name}` (')

    col_defs = []
    for col_name, col_def in ordered_columns:
        col_defs.append(f'    `{col_name}` {col_def}')
    out_lines.append(',\n'.join(col_defs))

    out_lines.append(')')

    # KEY 模型
    key_cols_str = ', '.join(f'`{c}`' for c in key_cols)
    out_lines.append(f'{key_model} ({key_cols_str})')

    # 表注释
    if table_comment:
        out_lines.append(f"COMMENT '{table_comment}'")

    # 分桶
    out_lines.append(f'DISTRIBUTED BY HASH(`{dist_col}`) BUCKETS AUTO;')

    result = '\n'.join(out_lines)

    # 如果有普通索引/唯一索引，作为注释附在后面供参考
    if unique_indexes or normal_indexes:
        result += '\n-- ====== 原始索引信息（Doris 不直接支持，仅供参考）======'
        for idx_name, idx_cols in unique_indexes:
            cols_str = ', '.join(idx_cols)
            result += f'\n-- UNIQUE INDEX {idx_name} ({cols_str})'
        for idx_name, idx_cols in normal_indexes:
            cols_str = ', '.join(idx_cols)
            result += f'\n-- INDEX {idx_name} ({cols_str})'

    return result


def parse_column(line: str) -> tuple:
    """
    解析单个列定义，返回 (col_name, doris_col_def)
    """
    # 去除尾部逗号
    line = line.strip().rstrip(',').strip()

    # 提取列名
    m = re.match(r'`?(\w+)`?\s+(.*)', line, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    col_name = m.group(1)
    rest = m.group(2).strip()

    # --- 去除 COLLATE（带引号和不带引号两种形式）---
    rest = re.sub(r"COLLATE\s+'[^']*'", '', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bCOLLATE\s+\w+', '', rest, flags=re.IGNORECASE)

    # --- 去除 CHARACTER SET / CHARSET ---
    rest = re.sub(r'\bCHARACTER\s+SET\s+\w+', '', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bCHARSET\s+\w+', '', rest, flags=re.IGNORECASE)

    # --- 去除 USING BTREE ---
    rest = re.sub(r'\s+USING\s+BTREE', '', rest, flags=re.IGNORECASE)

    # --- 去除 AUTO_INCREMENT ---
    rest = re.sub(r'\s*AUTO_INCREMENT', '', rest, flags=re.IGNORECASE)

    # --- 去除 ON UPDATE <expr>（如 ON UPDATE CURRENT_TIMESTAMP）---
    rest = re.sub(r'\bON\s+UPDATE\s+\S+(\([^)]*\))?', '', rest, flags=re.IGNORECASE)

    # --- 类型转换 ---

    # INT(N) / BIGINT(N) / DECIMAL(...) 等
    # INT(10) → INT, BIGINT(19) → BIGINT
    rest = re.sub(r'\bINT\(\d+\)', 'INT', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bBIGINT\(\d+\)', 'BIGINT', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bTINYINT\(\d+\)', 'TINYINT', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bSMALLINT\(\d+\)', 'SMALLINT', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bMEDIUMINT\(\d+\)', 'INT', rest, flags=re.IGNORECASE)

    # TEXT → STRING
    rest = re.sub(r'\bTEXT\b', 'STRING', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bLONGTEXT\b', 'STRING', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bMEDIUMTEXT\b', 'STRING', rest, flags=re.IGNORECASE)
    rest = re.sub(r'\bTINYTEXT\b', 'STRING', rest, flags=re.IGNORECASE)

    # CHAR(N) → VARCHAR(N)  (Doris CHAR 是定长且最大255，用VARCHAR更通用)
    rest = re.sub(r'\bCHAR\((\d+)\)', r'VARCHAR(\1)', rest, flags=re.IGNORECASE)

    # DECIMAL 精度检查：Doris 最大 DECIMAL(38, s)
    dec_match = re.search(r'DECIMAL\((\d+),\s*(\d+)\)', rest, re.IGNORECASE)
    if dec_match:
        p, s = int(dec_match.group(1)), int(dec_match.group(2))
        if p > 38:
            p = 38
        if s > p:
            s = p
        rest = re.sub(
            r'DECIMAL\(\d+,\s*\d+\)',
            f'DECIMAL({p}, {s})',
            rest,
            flags=re.IGNORECASE
        )

    # DATETIME → DATETIME (Doris 支持)
    # DATE → DATE (Doris 支持)

    # --- 处理 NULL / NOT NULL / DEFAULT ---
    # Doris UNIQUE KEY 列不能为 NULL，非KEY列默认可以为NULL
    # 这里先保留原始的 NULL/NOT NULL 定义，后续在排序KEY列时再处理

    # --- 去除多余空格 ---
    rest = re.sub(r'\s+', ' ', rest).strip()

    return (col_name, rest)


def split_columns(body: str) -> list:
    """
    按顶层逗号拆分列定义（处理括号嵌套和字符串内的特殊字符）
    """
    parts = []
    depth = 0
    in_string = False
    current = []

    for i, ch in enumerate(body):
        if ch == "'" and not in_string:
            in_string = True
            current.append(ch)
        elif ch == "'" and in_string:
            current.append(ch)
            # 检查转义引号 ''
            if i + 1 < len(body) and body[i + 1] == "'":
                continue
            in_string = False
        elif in_string:
            current.append(ch)
        elif ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(ch)

    if current:
        last = ''.join(current).strip()
        if last:
            parts.append(last)

    return parts


def main():
    import argparse
    parser = argparse.ArgumentParser(description='MySQL DDL → Doris DDL 转换工具')
    parser.add_argument('input', help='输入的MySQL SQL文件路径')
    parser.add_argument('-o', '--output', help='输出的Doris SQL文件路径（不指定则输出到stdout）')
    parser.add_argument('-b', '--buckets', type=int, default=8, help='默认分桶数（默认8）')
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f'错误: 文件不存在 {input_path}', file=sys.stderr)
        sys.exit(1)

    # 尝试多种编码读取
    for enc in ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin-1']:
        try:
            sql_text = input_path.read_text(encoding=enc)
            break
        except (UnicodeDecodeError, ValueError):
            continue
    else:
        print('错误: 无法识别文件编码', file=sys.stderr)
        sys.exit(1)

    result = convert_mysql_to_doris(sql_text, default_buckets=args.buckets)

    if args.output:
        out_path = Path(args.output)
        out_path.write_text(result, encoding='utf-8')
        print(f'✅ 转换完成，已写入: {out_path}')
    else:
        print(result)


if __name__ == '__main__':
    main()
