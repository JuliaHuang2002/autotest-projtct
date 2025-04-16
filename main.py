# main.py

import pandas as pd
import pymysql
from db_config import db_config

from sql_utils import execute_and_compare

# === 文件路径设置 ===
INPUT_FILE = "/Users/apple/Desktop/实习/autotest-project/input_excel/32B_instruct.xlsx"
OUTPUT_ALL = "/Users/apple/Desktop/实习/autotest-project/znws_compare_with_32B_instruct.xlsx"
OUTPUT_DIFF = "/Users/apple/Desktop/实习/autotest-project/znws_differences_only_32B_instruct.xlsx"

# === 读取 Excel 数据 ===
df = pd.read_excel(INPUT_FILE)

# 初始化比对结果列
labels = []
results = []
diffs = []
gt_results = []
model_results = []
structure_diffs = []


# 建立数据库连接
connection = pymysql.connect(**db_config)

# 执行每一组 SQL 对比
for _, row in df.iterrows():
    gt_sql = str(row.get("GT_sql", "")).strip()
    model_sql = str(row.get("模型输出sql列", "")).strip()

    if not gt_sql or not model_sql:
        labels.append(0)
        gt_results.append("")
        model_results.append("")
        diffs.append("SQL为空")
        structure_diffs.append("")
        continue

    label, gt_result, model_result, diff, structure_diff = execute_and_compare(gt_sql, model_sql, connection)

    labels.append(label)
    gt_results.append(gt_result)
    model_results.append(model_result)
    diffs.append(diff)
    structure_diffs.append(structure_diff)


connection.close()

# 写入结果
df["label"] = labels
df["GT查询结果"] = gt_results
df["模型查询结果"] = model_results
df["差异信息"] = diffs
df["语句比对"] = structure_diffs



# 导出完整结果
df.to_excel(OUTPUT_ALL, index=False)

# 导出差异项
df[df["label"] == 0].to_excel(OUTPUT_DIFF, index=False)

print("差异分析完成！\n- 所有结果: ", OUTPUT_ALL, "\n- 差异结果: ", OUTPUT_DIFF)
