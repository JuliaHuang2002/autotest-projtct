import re

def extract_date_expressions(sql: str) -> list:
    """从 SQL 中提取所有包含 date_time 的表达式片段"""
    sql = sql.lower().replace("“", "\"").replace("”", "\"").replace("‘", "'").replace("’", "'")
    matches = re.findall(r"(date_time\s*(?:=|>=|<=|>|<|between|in).*?)(?:\s|and|group|order|limit|$)", sql)
    return [m.strip() for m in matches]

def compare_time_granularity(gt_sql: str, model_sql: str) -> str:
    """比较 GT 和模型 SQL 中的 date_time 表达是否一致"""
    gt_exprs = extract_date_expressions(gt_sql)
    model_exprs = extract_date_expressions(model_sql)

    # 排序后统一比较
    if sorted(gt_exprs) != sorted(model_exprs):
        return f"时间表达不同（GT: {gt_exprs}, Model: {model_exprs}）"
    return ""
