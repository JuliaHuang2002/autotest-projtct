import re

def extract_date_granularity(sql: str) -> str:
    """提取 SQL 中的时间粒度（year, month, day）"""
    if "DATE_FORMAT(date_time, '%Y-%m-%d')" in sql:
        return "day"
    elif "DATE_FORMAT(date_time, '%Y-%m')" in sql:
        return "month"
    elif "DATE_FORMAT(date_time, '%Y')" in sql:
        return "year"
    else:
        # 尝试通过 WHERE 子句中的范围推断
        match = re.search(r"date_time\s*(>=|BETWEEN)\s*'(\d{4})(?:[-/](\d{2}))?(?:[-/](\d{2}))?", sql)
        if match:
            if match.group(3) and match.group(4):
                return "day"
            elif match.group(3):
                return "month"
            else:
                return "year"
    return "unknown"

def compare_time_granularity(gt_sql: str, model_sql: str) -> str:
    """比较 GT 和模型 SQL 的时间粒度"""
    gt_granularity = extract_date_granularity(gt_sql)
    model_granularity = extract_date_granularity(model_sql)

    if gt_granularity != "unknown" and model_granularity != "unknown" and gt_granularity != model_granularity:
        return f"时间粒度不同（GT为{gt_granularity}，Model为{model_granularity}）"
    return ""
