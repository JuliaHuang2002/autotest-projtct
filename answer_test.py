import pymysql
import pandas as pd


df = pd.read_excel("QwQ-32B_20250317225032.xlsx")

db_config = {
    "host": "10.8.0.1",
    "port": 13307,
    "user": "admin",
    "password": "frog_admin_2023",
    "database": "wd_cloud",
    "charset": "utf8mb4"
}


def execute_and_compare(gt_sql, model_sql, connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(gt_sql)
            gt_result = cursor.fetchall()

        with connection.cursor() as cursor:
            cursor.execute(model_sql)
            model_result = cursor.fetchall()

        
        if sorted(gt_result) == sorted(model_result):  #按照默认规则排序
            return 1, str(gt_result), ""
        else:
            return 0, "", f"GT: {gt_result}\nModel: {model_result}"

    except Exception as e:
        return 0, "", f"执行错误：{e}"


connection = pymysql.connect(**db_config)
labels = []
match_results = []#一致的查询结果
diff_info = []#不一致的查询结果

for _, row in df.iterrows():
    gt_sql = str(row.get("GT_sql", "")).strip()
    model_sql = str(row.get("模型输出sql列", "")).strip()

    if not gt_sql or not model_sql:
        labels.append(0)
        match_results.append("")
        diff_info.append("SQL为空")
        continue

    label, result, diff = execute_and_compare(gt_sql, model_sql, connection)
    labels.append(label)
    match_results.append(result)
    diff_info.append(diff)

connection.close()

df["label"] = labels
df["查询结果"] = match_results
df["差异信息"] = diff_info

df.to_excel("znws_compare_with_diff.xlsx", index=False)
print("差异分析完成，结果已保存为 znws_compare_with_diff.xlsx")
