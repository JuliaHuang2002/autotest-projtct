import pandas as pd

def run_sql(sql, connection):
    try:
        df = pd.read_sql(sql, connection)
        return True, df, None
    except Exception as e:
        return False, None, str(e)

def structured_dataframe_equal(df1, df2):
    if df1 is None or df2 is None:
        return False
    if set(df1.columns) != set(df2.columns):
        return False
    df1 = df1[sorted(df1.columns)].reset_index(drop=True)
    df2 = df2[sorted(df2.columns)].reset_index(drop=True)
    return df1.equals(df2)

def execute_and_compare(gt_sql, model_sql, connection):
    gt_ok, gt_df, gt_err = run_sql(gt_sql, connection)
    model_ok, model_df, model_err = run_sql(model_sql, connection)

    gt_result_str = gt_df.to_string(index=False) if gt_ok else ""
    model_result_str = model_df.to_string(index=False) if model_ok else ""

    if gt_ok and model_ok:
        # 完全一致判断
        if structured_dataframe_equal(gt_df, model_df):
            return 1, gt_result_str, model_result_str, ""
        
        # 时间缺失判断
        if "date_time" in gt_df.columns and "date_time" not in model_df.columns:
            if gt_df["date_time"].nunique() == 1:
                return 0, gt_result_str, model_result_str, "时间缺失"
        
        # 字段缺失判断（字段不一致但值一致）
        gt_cols = set(gt_df.columns)
        model_cols = set(model_df.columns)
        if gt_cols != model_cols:
            missing_cols = list(gt_cols - model_cols)
            try:
                shared_cols = [col for col in gt_df.columns if col in model_df.columns]
                gt_sub = gt_df[shared_cols].reset_index(drop=True)
                model_sub = model_df[shared_cols].reset_index(drop=True)
                if gt_sub.equals(model_sub):
                    return 0, gt_result_str, model_result_str, f"字段缺失: {missing_cols}"
            except Exception:
                pass
            return 0, gt_result_str, model_result_str, f"字段不一致: GT: {list(gt_df.columns)}, Model: {list(model_df.columns)}"

        # 值不一致字段
        diffs = [col for col in gt_df.columns if col in model_df.columns and not gt_df[col].equals(model_df[col])]
        return 0, gt_result_str, model_result_str, f"数据不一致字段: {diffs}"

    else:
        error_info = ""
        if not gt_ok:
            error_info += f"GT执行错误: {gt_err}\n"
        if not model_ok:
            error_info += f"模型执行错误: {model_err}"
        return 0, gt_result_str, model_result_str, error_info
