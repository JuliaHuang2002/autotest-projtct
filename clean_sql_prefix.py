

import pandas as pd
import re

# 输入输出路径
INPUT_FILE = "/Users/apple/Desktop/实习/autotest-project/input_excel/测试数据-0331.xlsx"
OUTPUT_FILE = "/Users/apple/Desktop/实习/autotest-project/input_excel/cleaned_sql_overwrite.xlsx"

# === 函数1：去掉库名前缀（FROM xxx.table → FROM table / JOIN xxx.table → JOIN table）===
def clean_sql_database_prefix(sql: str) -> str:
    sql = re.sub(r'FROM\s+[\w\d_]+\.', 'FROM ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'JOIN\s+[\w\d_]+\.', 'JOIN ', sql, flags=re.IGNORECASE)
    return sql

# === 函数2：去掉 AS 别名（SELECT col AS 名 → SELECT col）===
def remove_as_aliases(sql: str) -> str:
    match = re.search(r"(SELECT\s+)(.+?)(\s+FROM\s+)", sql, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return sql

    select_prefix, select_fields, from_suffix = match.groups()

    def remove_as(field):
        parts = re.split(r"\s+AS\s+", field, flags=re.IGNORECASE)
        return parts[0].strip()

    cleaned_fields = ", ".join(remove_as(f.strip()) for f in select_fields.split(","))
    return f"{select_prefix}{cleaned_fields}{from_suffix}" + sql[match.end():]

# === 合并清洗函数 ===
def full_clean_sql(sql: str) -> str:
    sql = clean_sql_database_prefix(sql)
    sql = remove_as_aliases(sql)
    return sql

# === 读取 Excel ===
df = pd.read_excel(INPUT_FILE)

# === 应用清洗逻辑 ===
df["GT_sql"] = df["GT_sql"].astype(str).apply(full_clean_sql)
df["模型输出sql列"] = df["模型输出sql列"].astype(str).apply(full_clean_sql)

# === 保存输出 ===
df.to_excel(OUTPUT_FILE, index=False)
print(f"✅ SQL 清洗完成：库名 + AS 别名 已处理。\n输出文件：{OUTPUT_FILE}")
