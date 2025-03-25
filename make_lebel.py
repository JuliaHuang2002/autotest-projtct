import pandas as pd

def clean_sql(sql):
    """去除SQL语句中的所有换行和空格"""
    return str(sql).replace('\n', '').replace(' ', '')

# 读取Excel文件
file_path = '/data/liuzaizhu/db_chat_algor/data/sql/sql测试/测试结果/v1~v3全部无属性用户问数据结果（20250305）待打标.xlsx'
df = pd.read_excel(file_path)

# 遍历每一行
for index, row in df.iterrows():
    gt_sql = clean_sql(row['GT_sql'])
    model_output_sql = clean_sql(row['模型输出sql'])
    
    # 判断处理后的SQL是否一致
    if gt_sql == model_output_sql:
        df.at[index, 'label'] = "1"

# 这样只会更新SQL匹配的情况为"1"，其他情况保持原值不变

# 将结果保存回Excel文件
df.to_excel('/data/liuzaizhu/db_chat_algor/data/sql/sql测试/测试结果/v1~v3全部无属性用户问数据结果（20250305）部分打标.xlsx', index=False)