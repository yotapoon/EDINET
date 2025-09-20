import datetime
import database_manager
import document_processor
import pandas as pd

def main():
    """
    メイン処理
    """
    # 処理対象の日付を指定
    # target_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    target_date = '2025-06-30' # テスト用に固定
    print(f"Processing documents for date: {target_date}")

    # ステップ1: 対象とすべき書類(docID, formCode)をDBから抽出する
    documents_to_process = database_manager.get_documents_by_date(target_date)

    if not documents_to_process:
        print("No target documents found for the specified date.")
        return

    print(f"Found {len(documents_to_process)} target documents.")

    # ステップ2: 各書類について処理を実行し、結果を収集
    all_extracted_data = {}

    for doc_id, form_code in documents_to_process:
        print(f"--- Processing docID: {doc_id}, formCode: {form_code} ---")
        
        # 1. 書類をダウンロードしてファイルパスを取得
        csv_path = document_processor.fetch_and_save_document(doc_id)
        
        if not csv_path:
            print(f"Skipping docID {doc_id} due to download/save failure.")
            continue
            
        # 2. ファイルを解析してデータを抽出
        extracted_for_doc = document_processor.parse_document_file(csv_path, form_code)
        
        # 抽出されたデータタイプごとに結果を結合
        for data_type, df_result in extracted_for_doc.items():
            if data_type not in all_extracted_data:
                all_extracted_data[data_type] = []
            all_extracted_data[data_type].append(df_result)
    
    # 収集したデータを最終的なDataFrameに結合して表示
    if all_extracted_data:
        print("\n--- Consolidated Processed Data ---")
        for data_type, list_of_dfs in all_extracted_data.items():
            if list_of_dfs:
                final_df_for_type = pd.concat(list_of_dfs, ignore_index=True)
                print(f"\n--- {data_type} ---")
                print(final_df_for_type)
                print("---------------------------------")
    else:
        print("No data processed from documents.")

    # ステップ3: (今後の実装) 取得したデータをDBに保存する
    # if all_extracted_data:
    #     for data_type, list_of_dfs in all_extracted_data.items():
    #         if list_of_dfs:
    #             final_df_for_type = pd.concat(list_of_dfs, ignore_index=True)
    #             database_manager.save_processed_data(data_type, final_df_for_type) # 仮の関数



if __name__ == "__main__":
    main()