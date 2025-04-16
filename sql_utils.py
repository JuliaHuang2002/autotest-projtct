import pandas as pd
from column_aliases import COLUMN_ALIASES, METRIC_FIELDS
from sql_structure import compare_time_granularity


def run_sql(sql, connection):
    """执行 SQL 查询并返回结果 DataFrame 或错误信息。"""
    try:
        df = pd.read_sql(sql, connection)
        df.columns = df.columns.astype(str)
        return True, df, None
    except Exception as e:
        return False, None, str(e)

def normalize_columns(columns):
    """将列名中的中文别名转换为标准英文名"""
    return [COLUMN_ALIASES.get(col, col) for col in columns]

def structured_dataframe_equal(df1, df2):
    """判断两个 DataFrame 在结构和内容上是否一致（容忍字段别名，忽略顺序）"""
    if df1 is None or df2 is None:
        return False
    if df1.empty and df2.empty:
        return True
    if df1.empty or df2.empty:
        return False

    # 别名统一
    df1.columns = normalize_columns(df1.columns)
    df2.columns = normalize_columns(df2.columns)

    # 列集合需一致
    if set(df1.columns) != set(df2.columns):
        return False

    # 排序并比较
    df1_sorted = df1[sorted(df1.columns)].reset_index(drop=True)
    df2_sorted = df2[sorted(df2.columns)].reset_index(drop=True)
    return df1_sorted.equals(df2_sorted)

def execute_and_compare(gt_sql, model_sql, connection):
    gt_ok, gt_df, gt_err = run_sql(gt_sql, connection)
    model_ok, model_df, model_err = run_sql(model_sql, connection)

    # === 构造输出字符串 ===
    gt_result_str = gt_df.to_string(index=False) if gt_ok and gt_df is not None else (f"[GT执行错误: {gt_err}]" if not gt_ok else "")
    model_result_str = model_df.to_string(index=False) if model_ok and model_df is not None else (f"[模型执行错误: {model_err}]" if not model_ok else "")

    # === 执行错误 ===
    if not gt_ok or not model_ok:
        error_info = []
        if not gt_ok:
            error_info.append("GT执行报错")
        if not model_ok:
            error_info.append("模型执行报错")
        return 0, gt_result_str, model_result_str, "\n".join(error_info), ""

    # === 空结果 ===
    if gt_df.empty and model_df.empty:
        return 0, gt_result_str, model_result_str, "查询结果皆为空", ""
    elif gt_df.empty:
        return 0, gt_result_str, model_result_str, "GT结果为空", ""
    elif model_df.empty:
        return 0, gt_result_str, model_result_str, "模型SQL结果为空", ""

    # === 标准化字段名 ===
    gt_df.columns = normalize_columns(gt_df.columns)
    model_df.columns = normalize_columns(model_df.columns)

    # === 完全一致 ===
    if structured_dataframe_equal(gt_df, model_df):
        return 1, gt_result_str, model_result_str, "", ""
    

    # === 容忍判断：只有 date_time 缺失，其他字段和值都一致（label=1，说明能容忍）
    if "date_time" in gt_df.columns and "date_time" not in model_df.columns:
        gt_cols_minus_time = [col for col in gt_df.columns if col != "date_time"]
        # 判断是否字段名一致
        if set(gt_cols_minus_time) == set(model_df.columns):
            gt_trimmed = gt_df[gt_cols_minus_time].sort_values(by=gt_cols_minus_time).reset_index(drop=True)
            model_sorted = model_df[gt_cols_minus_time].sort_values(by=gt_cols_minus_time).reset_index(drop=True)
            if gt_trimmed.equals(model_sorted) and len(gt_df) == len(model_df):
                return 1, gt_result_str, model_result_str, "时间缺失，其他一致", ""


    
    

    # 字段类型不一致但值一致
    if set(gt_df.columns) == set(model_df.columns):
        try:
            gt_sorted = gt_df[sorted(gt_df.columns)].reset_index(drop=True).astype(str)
            model_sorted = model_df[sorted(model_df.columns)].reset_index(drop=True).astype(str)
            if gt_sorted.equals(model_sorted):
                return 1, gt_result_str, model_result_str, "字段类型不一致但值一致", ""
        except Exception:
            pass

    # === 指标一致 + 维度缺失容忍 ===
    shared_fields = set(gt_df.columns).intersection(set(model_df.columns))
    if all(col in METRIC_FIELDS for col in shared_fields):
        if len(gt_df) == len(model_df):
            shared_gt = gt_df[sorted(shared_fields)].reset_index(drop=True)
            shared_model = model_df[sorted(shared_fields)].reset_index(drop=True)
            if shared_gt.equals(shared_model):
                return 0, gt_result_str, model_result_str, "指标一致，维度缺失", ""
    
    # === fallback 开始 ===
    if set(gt_df.columns) != set(model_df.columns):
        structure_diff = compare_time_granularity(gt_sql, model_sql)
        return 0, gt_result_str, model_result_str, f"字段不一致: GT列={list(gt_df.columns)}, Model列={list(model_df.columns)}", structure_diff

    if len(gt_df) != len(model_df):
        structure_diff = compare_time_granularity(gt_sql, model_sql)
        return 0, gt_result_str, model_result_str, f"行数不一致: GT行数={len(gt_df)}, Model行数={len(model_df)}", structure_diff

    # fallback 最后一步
    structure_diff = compare_time_granularity(gt_sql, model_sql)
    return 0, gt_result_str, model_result_str, "数据内容或顺序不一致", structure_diff


    