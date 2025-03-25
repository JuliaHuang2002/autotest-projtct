import pandas as pd
import os

def clean_sql(sql):
    """去除 SQL 语句中的所有换行和空格"""
    return str(sql).replace('\n', '').replace(' ', '')

# 使用完整路径
file_name = '/home/huangzhixuan/workfile/auto-test-project/QwQ-32B_20250317225032.xlsx'
df = pd.read_excel(file_name)


# 初始化统计变量
count_1 = 0
count_0 = 0
count_na = 0

# 遍历每一行
for index, row in df.iterrows():
    gt_raw = row['GT_sql']
    model_raw = row['模型输出sql列']
    
    # 如果 GT_sql 缺失，跳过并统计
    if pd.isna(gt_raw):
        count_na += 1
        continue

    gt_sql = clean_sql(gt_raw)
    model_sql = clean_sql(model_raw)

    # 比较处理后的 SQL
    if gt_sql == model_sql:
        df.at[index, 'label'] = "1"
        count_1 += 1
    else:
        df.at[index, 'label'] = "0"
        count_0 += 1

# 输出统计信息
total = len(df)
print("标注完成，统计如下：")
print(f"总数据条数：{total}")
print(f"标记为 1（匹配）：{count_1}")
print(f"标记为 0（不匹配）：{count_0}")
print(f"GT_sql 缺失未打标：{count_na}")

# 保存结果为新的 Excel 文件
output_name = '/home/huangzhixuan/workfile/auto-test-project/QwQ-32B_20250317225032_打标结果.xlsx'
df.to_excel(output_name, index=False)
print(f"结果已保存为：{output_name}")
s