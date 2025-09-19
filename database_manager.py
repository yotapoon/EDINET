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

def get_doc_ids_by_date(target_date: str) -> list[str]:
    """
    指定された日付の有価証券報告書・四半期報告書のdocIDを取得する
    """
    # 対象とする書類コード (formCode)
    # 030000: 有価証券報告書
    # 043000: 四半期報告書
    target_form_codes = ['030000', '043000']

    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME, 
                column('docID'), 
                column('dateFile'), 
                column('formCode')
            )
            
            stmt = select(submission_table.c.docID).where(
                submission_table.c.dateFile == target_date,
                submission_table.c.formCode.in_(target_form_codes)
            )
            
            df = pd.read_sql(stmt, connection)

            if not df.empty:
                return df['docID'].tolist()
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve docIDs for date {target_date}: {e}")
        return []
