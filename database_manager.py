import pandas as pd
from sqlalchemy import create_engine, select, table, column, desc
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

def get_documents_by_date(target_date: str) -> list[tuple[str, str, str]]:
    """
    指定された日付の書類の(docID, formCode, ordinanceCodeShort)のリストを取得する
    """
    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME,
                column('docID'),
                column('formCode'),
                column('ordinanceCode'),
                column('dateFile'),
                column('csvFlag'),
            )
            document_form_master_table = table(
                'DocumentFormMaster',
                column('formCode'),
                column('ordinanceCode'),
                column('ordinanceCodeShort'),
            )

            stmt = select(
                submission_table.c.docID,
                submission_table.c.formCode,
                document_form_master_table.c.ordinanceCodeShort,
            ).join(
                document_form_master_table,
                (submission_table.c.formCode == document_form_master_table.c.formCode) &
                (submission_table.c.ordinanceCode == document_form_master_table.c.ordinanceCode)
            ).where(
                submission_table.c.dateFile == target_date
            ).where(
                submission_table.c.csvFlag == 1
            )

            df = pd.read_sql(stmt, connection)

            if not df.empty:
                # (docID, formCode, ordinanceCodeShort) のタプルのリストを返す
                return list(df.itertuples(index=False, name=None))
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve documents for date {target_date}: {e}")
        return []

def get_documents_by_form_code(target_form_code: str) -> list[tuple[str, str, str, int]]:
    """
    指定されたformCodeの書類の(dateFile, docID, ordinanceCodeShort, seqNumber)のリストを取得する
    日付が新しい順にソートされる。
    """
    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME,
                column('dateFile'),
                column('docID'),
                column('formCode'),
                column('ordinanceCode'),
                column('csvFlag'),
                column('seqNumber'),
            )
            document_form_master_table = table(
                'DocumentFormMaster',
                column('formCode'),
                column('ordinanceCode'),
                column('ordinanceCodeShort'),
            )

            stmt = select(
                submission_table.c.dateFile,
                submission_table.c.docID,
                document_form_master_table.c.ordinanceCodeShort,
                submission_table.c.seqNumber,
            ).join(
                document_form_master_table,
                (submission_table.c.formCode == document_form_master_table.c.formCode) &
                (submission_table.c.ordinanceCode == document_form_master_table.c.ordinanceCode)
            ).where(
                submission_table.c.formCode == target_form_code
            ).where(
                submission_table.c.csvFlag == 1
            ).order_by(
                desc(submission_table.c.dateFile)
            )

            df = pd.read_sql(stmt, connection)

            if not df.empty:
                # (dateFile, docID, ordinanceCodeShort, seqNumber) のタプルのリストを返す
                return list(df.itertuples(index=False, name=None))
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve documents for formCode {target_form_code}: {e}")
        return []

def save_buyback_status_report(df: pd.DataFrame):
    """自己株券買付状況報告書のDataFrameをDBに保存する"""
    if df.empty:
        print("Info: No new records to upload for BuybackStatusReport.")
        return

    try:
        # テーブル名は大文字小文字を区別する可能性があるため、SQLで定義した通りに指定
        df.to_sql("BuybackStatusReport", con=engine, if_exists='append', index=False)
        print(f"Success: Uploaded {len(df)} records to BuybackStatusReport.")
    except Exception as e:
        print(f"Error: An unexpected error occurred during DB upload to BuybackStatusReport: {e}")


if __name__ == "__main__":
    # get_documents_by_date のテスト
    target_date = '2025-06-30' # テスト用に固定
    print(f"Processing documents for date: {target_date}")
    documents_to_process_by_date = get_documents_by_date(target_date)
    for doc_id, form_code, ordinanceCodeShort in documents_to_process_by_date:
        print(doc_id, form_code, ordinanceCodeShort)

    print("-" * 20)

    # get_documents_by_form_code のテスト
    target_form_code = '030000'  # 有価証券報告書
    print(f"Processing documents for formCode: {target_form_code}")
    documents_to_process_by_form_code = get_documents_by_form_code(target_form_code)
    for date_file, doc_id, ordinanceCodeShort, seq_number in documents_to_process_by_form_code:
        print(date_file, doc_id, ordinanceCodeShort, seq_number)