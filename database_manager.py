import pandas as pd
from sqlalchemy import create_engine, select, table, column
from config import CONNECTION_STRING, SUBMISSION_TABLE_NAME

# アプリケーション全体で共有するデータベースエンジンを作成
engine = create_engine(CONNECTION_STRING)

def save_submission_list(df: pd.DataFrame, date_str: str):
    """提出書類一覧のDataFrameをDBに保存する"""
    if df.empty:
        print(f"Info: No new records to upload for {date_str}.")
        return

    try:
        df.to_sql(SUBMISSION_TABLE_NAME, con=engine, if_exists='append', index=False)
        print(f"Success: Uploaded {len(df)} records for {date_str}.")
    except Exception as e:
        print(f"Error: An unexpected error occurred during DB upload for {date_str}: {e}")

def get_existing_dates() -> list[str]:
    """データベースに保存されている日付の一覧を取得する"""
    try:
        with engine.connect() as connection:
            query = f"SELECT DISTINCT dateFile FROM {SUBMISSION_TABLE_NAME}"
            df = pd.read_sql(query, connection)
            # 日付オブジェクトを'YYYY-MM-DD'形式の文字列に変換
            if not df.empty:
                return pd.to_datetime(df['dateFile']).dt.strftime('%Y-%m-%d').tolist()
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve existing dates: {e}")
        return []

def get_documents_by_date(target_date: str) -> list[tuple[str, str]]:
    """
    指定された日付の書類の(docID, formCode)のリストを取得する
    """
    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME,
                column('docID'),
                column('formCode'),
                column('dateFile'),
                column('csvFlag')
            )

            stmt = select(
                submission_table.c.docID,
                submission_table.c.formCode
            ).where(
                submission_table.c.dateFile == target_date
            ).where(
                submission_table.c.csvFlag == 1
            )

            df = pd.read_sql(stmt, connection)

            if not df.empty:
                # (docID, formCode) のタプルのリストを返す
                return list(df.itertuples(index=False, name=None))
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve documents for date {target_date}: {e}")
        return []

if __name__ == "__main__":
    target_date = '2025-06-30' # テスト用に固定
    print(f"Processing documents for date: {target_date}")
    documents_to_process = get_documents_by_date(target_date)
    for doc_id, form_code in documents_to_process:
        print(doc_id, form_code) # form_codeにNoneがあることに注意