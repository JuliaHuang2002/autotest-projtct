main.py	主控脚本，读取 Excel、调用比对逻辑、写入结果
sql_utils.py	核心对比逻辑，包含 SQL 执行、容错判断、DataFrame 比较
sql_metrics.py	提供指标字段集合 all_metric_fields，用于维度缺失容忍
db_config.py	数据库配置
input_excel/	存放原始 SQL 对比任务的 Excel 文件
output_excel/	存放输出结果的 Excel 文件（完整+仅差异）