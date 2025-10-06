import itertools
from operator import itemgetter

import database_manager
import document_processor
import pandas as pd

def main():
    """
    メイン処理
    """
    # target_form_code = '170000' # 自己株券買付状況報告書
    target_form_code = '170001' # 自己株券買付状況報告書（訂正）
    target_form_code = '253000' # 自己株券買付状況報告書（訂正，特定有価証券）
    print(f"Processing documents for formCode: {target_form_code}")

    # ステップ1: 対象とすべき書類(dateFile, docID, ordinanceCodeShort)をDBから日付順で抽出
    documents_to_process = database_manager.get_documents_by_form_code(target_form_code)

    if not documents_to_process:
        print("No target documents found for the specified formCode.")
        return

    print(f"Found {len(documents_to_process)} total documents to process.")

    # ステップ2: 書類を日付ごとにグループ化して処理
    for date_file, group in itertools.groupby(documents_to_process, key=itemgetter(0)):
        
        print(f"\n--- Processing date: {date_file} ---")
        daily_reports = []
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
                
            # 2. ファイルを解析してデータを抽出
            extracted_for_doc = document_processor.parse_document_file(csv_path, target_form_code, ordinanceCodeShort)
            
            # 3. 抽出データに必要な情報を追加してリストに格納
            if "BuybackStatusReport" in extracted_for_doc and not extracted_for_doc["BuybackStatusReport"].empty:
                report_df = extracted_for_doc["BuybackStatusReport"]
                report_df['docID'] = doc_id
                report_df['seqNumber'] = seq_number # DBから取得した値を使用
                daily_reports.append(report_df)

        # ステップ2b: 日付ごとの処理結果をDBに保存
        if daily_reports:
            final_df_for_date = pd.concat(daily_reports, ignore_index=True)
            
            # カラムの順序をSQL定義に合わせる
            final_df_for_date = final_df_for_date[[
                'docID', 'dateFile', 'seqNumber', 'secCode', 'ordinanceCode',
                'formCode', 'acquisitionStatus', 'disposalStatus', 'holdingStatus'
            ]]

            print(f"  Uploading {len(final_df_for_date)} records for {date_file} to the database.")
            database_manager.save_buyback_status_report(final_df_for_date)
        else:
            print(f"No data extracted for date: {date_file}")

    print("\n--- All processing finished. ---")

if __name__ == "__main__":
    main()