import itertools
from operator import itemgetter

import database_manager
import document_processor
import pandas as pd

def process_documents_by_form_code(target_form_code):
    """
    指定された書類コードのドキュメントを処理します。
    """
    print(f"Processing documents for formCode: {target_form_code}")

    # ステップ1: 対象とすべき書類(dateFile, docID, ordinanceCodeShort)をDBから日付順で抽出
    documents_to_process = database_manager.get_documents_by_form_code(target_form_code)

    if not documents_to_process:
        print(f"No target documents found for the specified formCode.")
        return

    print(f"Found {len(documents_to_process)} total documents to process.")

    # ステップ2: 書類を日付ごとにグループ化して処理
    for date_file, group in itertools.groupby(documents_to_process, key=itemgetter(0)):
        
        print(f"\n--- Processing date: {date_file} ---")
        docs_on_date = list(group)
        print(f"Found {len(docs_on_date)} documents for this date.")

        # ステップ2a: 同じ日付の各書類について処理を実行
        for _, doc_id, ordinanceCodeShort, seq_number in docs_on_date:
            print(f"  - Processing docID: {doc_id}")
            
            # 1. 書類をダウンロードしてファイルパスを取得
            csv_path = document_processor.fetch_and_save_document(doc_id, ordinanceCodeShort)
            
            if not csv_path:
                print(f"    Skipping docID {doc_id} due to download/save failure.")
                continue
                
            # 2. ファイルを解析して複数のデータタイプを抽出
            extracted_data_map = document_processor.parse_document_file(csv_path, target_form_code, ordinanceCodeShort)
            
            # 3. 抽出された各データタイプをDBに保存
            if not extracted_data_map:
                print(f"    No data extracted for docID: {doc_id}")
                continue

            for data_type_name, df in extracted_data_map.items():
                if df.empty:
                    continue

                # 共通のメタデータをDataFrameに追加
                df['docID'] = doc_id
                if 'seqNumber' not in df.columns:
                    df['seqNumber'] = seq_number
                if 'dateFile' not in df.columns:
                    df['dateFile'] = date_file

                # 汎用保存関数を直接呼び出す
                print(f"    -> Saving {data_type_name} data...")
                database_manager.save_data(df, data_type_name)

    print(f"\n--- Finished processing for formCode: {target_form_code} ---")

if __name__ == "__main__":
    # 処理対象の書類コードリスト
    target_form_codes = [
        # '030000',  # 有価証券報告書
        # '050210',  # 大量保有報告書
        # '050220',  # 大量保有報告書（変更報告書）
        '170000',  # 自己株券買付状況報告書
        '170001',  # 自己株券買付状況報告書（訂正）
        '253000'   # 自己株券買付状況報告書（訂正，特定有価証券）
    ]

    for form_code in target_form_codes:
        process_documents_by_form_code(form_code)
    
    print("\n--- All processing finished. ---")