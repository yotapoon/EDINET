import re
import pandas as pd
import traceback
from sqlalchemy import create_engine, select, table, column, desc, or_, and_, Table, MetaData, text
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

def get_documents_by_codes(codes: list[tuple[str, str]]) -> list[tuple[str, str, str, str, str, int]]:
    """
    指定された(formCode, ordinanceCode)のタプルリストに一致する書類のリストを取得する。
    (dateFile, docID, formCode, ordinanceCode, ordinanceCodeShort, seqNumber)
    日付が新しい順にソートされる。
    """
    if not codes:
        return []
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

            # SQL Serverと互換性のあるWHERE句を構築
            conditions = []
            for form_code, ordinance_code in codes:
                conditions.append(
                    and_(
                        submission_table.c.formCode == form_code,
                        submission_table.c.ordinanceCode == ordinance_code
                    )
                )

            stmt = select(
                submission_table.c.dateFile,
                submission_table.c.docID,
                submission_table.c.formCode,
                submission_table.c.ordinanceCode,
                document_form_master_table.c.ordinanceCodeShort,
                submission_table.c.seqNumber,
            ).join(
                document_form_master_table,
                (submission_table.c.formCode == document_form_master_table.c.formCode) &
                (submission_table.c.ordinanceCode == document_form_master_table.c.ordinanceCode)
            ).where(
                or_(*conditions)  # (formCode = c1 AND ordinanceCode = o1) OR (...)
            ).where(
                submission_table.c.csvFlag == 1
            ).order_by(
                desc(submission_table.c.dateFile)
            )

            df = pd.read_sql(stmt, connection)

            if not df.empty:
                return list(df.itertuples(index=False, name=None))
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve documents for codes {codes}: {e}")
        return []

def get_documents_by_form_code(target_form_code: str) -> list[tuple[str, str, str, str, int]]:
    """
    指定されたformCodeの書類の(dateFile, docID, ordinanceCode, ordinanceCodeShort, seqNumber)のリストを取得する
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
                submission_table.c.ordinanceCode,
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
                # (dateFile, docID, ordinanceCode, ordinanceCodeShort, seqNumber) のタプルのリストを返す
                return list(df.itertuples(index=False, name=None))
            return []
    except Exception as e:
        print(f"Error: Failed to retrieve documents for formCode {target_form_code}: {e}")
        return []

def save_data(df: pd.DataFrame, data_type_name: str):
    """共通のデータ保存ロジック。冪等性を担保する。"""
    table_name = TABLE_NAME_MAP.get(data_type_name, data_type_name)

    if df.empty:
        print(f"Info: No new records to upload for {table_name}.")
        return

    try:
        with engine.begin() as connection: # トランザクションを開始
            # テーブルが存在する場合のみ、既存レコードの削除を試みる
            if engine.dialect.has_table(connection, table_name):
                meta = MetaData()
                tbl = Table(table_name, meta, autoload_with=connection)
                primary_key_cols = [c.name for c in tbl.primary_key.columns]

                if primary_key_cols and all(col in df.columns for col in primary_key_cols):
                    # 既存のレコードを主キーに基づいて削除
                    unique_keys = df[primary_key_cols].drop_duplicates()
                    
                    # SQLAlchemyのtextと名前付きパラメータを使用して、より安全なDELETE文を構築
                    conditions = [f'[{col}] = :{col}' for col in primary_key_cols]
                    delete_stmt_str = f'DELETE FROM [{table_name}] WHERE {" AND ".join(conditions)}'
                    delete_stmt = text(delete_stmt_str)

                    # to_dict('records') を使って、行のリストを辞書のリストに変換し、一括で実行
                    keys_to_delete = unique_keys.to_dict('records')
                    if keys_to_delete:
                        connection.execute(delete_stmt, keys_to_delete)
            
            # DataFrameをDBに書き込み (if_exists='append' なので、テーブルがなければ作成される)
            df.to_sql(table_name, con=connection, if_exists='append', index=False)
            print(f"Success: Upserted {len(df)} records to {table_name}.")

    except Exception as e:
        print(f"Error: An unexpected error occurred during DB upload to {table_name}: {e}")
        traceback.print_exc()


def get_name_code_master_data() -> pd.DataFrame:
    """
    名寄せマスターの元データとなる、(filerName, edinetCode, secCode) のリストをDBから取得する。
    edinetCodeがNULLでない、法人・団体の提出者のみを対象とする。
    """
    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME,
                column('filerName'),
                column('edinetCode'),
                column('secCode'),
            )
            
            stmt = select(
                submission_table.c.filerName,
                submission_table.c.edinetCode,
                submission_table.c.secCode,
            ).where(
                submission_table.c.edinetCode.is_not(None)
            ).distinct()

            df = pd.read_sql(stmt, connection)
            print(f"Successfully fetched {len(df)} records for name master.")
            return df
    except Exception as e:
        print(f"Error: Failed to retrieve name master data: {e}")
        return pd.DataFrame()


def get_data_for_enrichment(table_name: str, column_name: str) -> pd.DataFrame:
    """
    指定されたテーブルから、名寄せ対象となるカラムを含む全てのデータを取得する。
    """
    try:
        with engine.connect() as connection:
            # テーブル名を直接埋め込むことで、カラムを自動認識させる
            query = f'SELECT * FROM "{table_name}"'
            df = pd.read_sql(query, connection)
            print(f"Successfully fetched {len(df)} records from {table_name} for enrichment.")
            return df
    except Exception as e:
        print(f"Error: Failed to retrieve data from {table_name}: {e}")
        return pd.DataFrame()

def get_enriched_keys(table_name: str) -> set:
    """指定されたEnrichedテーブルから、既に処理済みのキーのセットを取得する。"""
    keys = set()
    try:
        with engine.connect() as connection:
            # テーブルが存在しない場合を考慮
            if not engine.dialect.has_table(connection, table_name):
                print(f"Info: Enriched table '{table_name}' does not exist yet. Returning empty set.")
                return keys

            meta = MetaData()
            enriched_table = Table(table_name, meta, autoload_with=connection)
            primary_key_cols = [c.name for c in enriched_table.primary_key.columns]
            if not primary_key_cols:
                print(f"Warning: No primary key found for {table_name}. Cannot check for existing records.")
                return keys

            stmt = select(*[column(c) for c in primary_key_cols])
            df = pd.read_sql(stmt, connection)
            
            if not df.empty:
                keys = set(df.itertuples(index=False, name=None))
            print(f"Found {len(keys)} existing keys in {table_name}.")
            return keys
    except Exception as e:
        print(f"Error: Failed to retrieve existing keys from {table_name}: {e}")
        return keys

def get_document_details_by_id(doc_id: str) -> tuple | None:
    """
    doc_idに一致する書類の詳細情報をデータベースから取得する。

    Args:
        doc_id (str): 取得したい書類のdocId。

    Returns:
        tuple | None: (form_code, ordinance_code, ordinance_code_short) のタプル、見つからない場合はNone。
    """
    try:
        with engine.connect() as connection:
            submission_table = table(
                SUBMISSION_TABLE_NAME,
                column('docID'),
                column('formCode'),
                column('ordinanceCode'),
            )
            document_form_master_table = table(
                'DocumentFormMaster',
                column('formCode'),
                column('ordinanceCode'),
                column('ordinanceCodeShort'),
            )

            stmt = select(
                submission_table.c.formCode,
                submission_table.c.ordinanceCode,
                document_form_master_table.c.ordinanceCodeShort,
            ).join(
                document_form_master_table,
                (submission_table.c.formCode == document_form_master_table.c.formCode) &
                (submission_table.c.ordinanceCode == document_form_master_table.c.ordinanceCode)
            ).where(
                submission_table.c.docID == doc_id
            )

            result = connection.execute(stmt).fetchone()
            return result if result else None

    except Exception as e:
        print(f"Error: Failed to retrieve document details for docID {doc_id}: {e}")
        return None


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
