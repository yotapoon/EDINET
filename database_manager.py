import re
import pandas as pd
from sqlalchemy import create_engine, select, table, column, desc
from config import CONNECTION_STRING, SUBMISSION_TABLE_NAME

# アプリケーション全体で共有するデータベースエンジンを作成
engine = create_engine(CONNECTION_STRING)

# データタイプ名をテーブル名にマッピングする。異なる場合のみ定義。
TABLE_NAME_MAP = {
    "Officer": "OfficerInformation"
}

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

def save_data(df: pd.DataFrame, data_type_name: str):
    """共通のデータ保存ロジック"""
    table_name = TABLE_NAME_MAP.get(data_type_name, data_type_name)

    if df.empty:
        print(f"Info: No new records to upload for {table_name}.")
        return

    try:
        # docIDとdateFile/SubmissionDateの組み合わせで既存レコードを削除
        # docIDは必ず存在すると仮定
        # 日付カラムは 'dateFile' or 'SubmissionDate' or 'ReportObligationDate'
        date_col = None
        if 'dateFile' in df.columns:
            date_col = 'dateFile'
        elif 'SubmissionDate' in df.columns:
            date_col = 'SubmissionDate'
        elif 'ReportObligationDate' in df.columns:
            date_col = 'ReportObligationDate'

        # docIDと日付で既存データを削除し、冪等性を担保
        if 'docID' in df.columns and date_col:
            unique_docs = df[['docID', date_col]].drop_duplicates()
            with engine.begin() as connection: # トランザクションを開始
                for _, row in unique_docs.iterrows():
                    doc_id = row['docID']
                    date_val = row[date_col]
                    # SQLインジェクション対策のため、テーブル名とカラム名は安全なもののみを許可
                    if not re.match(r'^[a-zA-Z0-9_]+$', table_name) or not re.match(r'^[a-zA-Z0-9_]+$', date_col):
                         raise ValueError("Invalid table or column name.")
                    
                    # テーブル名とカラム名を安全にクエリに組み込む
                    delete_stmt = f'DELETE FROM "{table_name}" WHERE "docID" = ? AND "{date_col}" = ?'
                    connection.execute(delete_stmt, (doc_id, date_val))

        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Success: Uploaded {len(df)} records to {table_name}.")
    except ValueError as ve:
        print(f"Error during DB upload to {table_name}: {ve}")
    except Exception as e:
        print(f"Error: An unexpected error occurred during DB upload to {table_name}: {e}")


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
