
import pandas as pd
from edinet_utils import engine, TABLE_NAME, upload_submission

def collect_date_to_load():
    """
    直近10年間の日付のうち，テーブルに存在しない日付をyyyy-mm-dd形式の文字列のリストで取得する関数
    """
    with engine.connect() as conn:
        query = f"SELECT DISTINCT format(dateFile, 'yyyy-MM-dd') as dateFile FROM {TABLE_NAME}"
        existing_dates = pd.read_sql(query, conn)['dateFile'].tolist()
        print(f"Existing dates in the table: {len(existing_dates)}")

    date_range = pd.date_range(end=pd.Timestamp.today(), periods=365*10, freq='D')
    date_list = [date.strftime('%Y-%m-%d') for date in date_range]
    new_dates = [date for date in date_list if date not in existing_dates]
    
    return new_dates


if __name__ == "__main__":
    new_dates = collect_date_to_load()
    print(f"Collecting data for {len(new_dates)} new dates.")
    # upload_submission("2025-07-16")  # Example date to upload data for
    for date in new_dates:
        upload_submission(date)
