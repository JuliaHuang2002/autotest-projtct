import pymysql
import time
from db_config import db_config

table_name = 'znws'
batch_size = 1000

def convert_to_2025(date_str: str) -> str:
    """把 2024 开头的时间字符串替换为 2025"""
    return date_str.replace('2024', '2025', 1)

def main():
    start_time = time.time()
    connection = pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # 获取所有 2024 年数据（支持所有粒度）
            print("📥 正在加载 2024 年数据...")
            cursor.execute(f"SELECT * FROM {table_name} WHERE date_time LIKE '2024%'")
            all_rows = cursor.fetchall()
            total = len(all_rows)
            print(f"✅ 共加载 {total} 条记录，准备分批插入")

            if total == 0:
                return

            inserted = 0
            batch = []
            columns = []

            for i, row in enumerate(all_rows):
                if not columns:
                    columns = [k for k in row.keys() if k != 'id']  # 移除主键
                row_data = {k: v for k, v in row.items() if k != 'id'}

                if isinstance(row_data.get('date_time'), str):
                    row_data['date_time'] = convert_to_2025(row_data['date_time'])

                batch.append(tuple(row_data[col] for col in columns))

                # 到达一批就执行插入
                if len(batch) == batch_size or i == total - 1:
                    with connection.cursor() as insert_cursor:
                        placeholders = ', '.join(['%s'] * len(columns))
                        col_names = ', '.join(f"`{col}`" for col in columns)
                        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
                        insert_cursor.executemany(sql, batch)
                        connection.commit()

                    inserted += len(batch)
                    print(f"🚀 已插入 {inserted} / {total} 条")
                    batch = []

            duration = time.time() - start_time
            print(f"🎉 全部插入完成，共插入 {inserted} 条，用时 {duration:.2f} 秒")

    finally:
        connection.close()
        print("🔚 数据库连接已关闭")

if __name__ == "__main__":
    main()
